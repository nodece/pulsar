/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pulsar.broker.delayed.bucket.journal;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import lombok.CustomLog;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.api.DigestType;
import org.apache.bookkeeper.client.api.LedgerEntry;
import org.apache.bookkeeper.client.api.ReadHandle;
import org.apache.bookkeeper.client.api.WriteHandle;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;

/**
 * Append-only journal ledgers for the {@link JournalingBucketDelayedDeliveryTracker}: every
 * delayed entry is copied, framed and appended into the ledger of its delivery time window, so a
 * due delivery reads only the current window's ledger sequentially. Within a window the arrival
 * order is preserved; due delivery drains the ledger in that order and holds back at the first
 * record that is not due yet, so the disorder a consumer observes is bounded by the window width.
 */
@CustomLog
class JournalStorage {

    /** Framed journal record: deliverAt | dataLedgerId | dataEntryId | entryData (no length header —
     * the entry length is the BK entry length). */
    static final int HEADER_SIZE = 8 + 8 + 8;

    /** Delivery time windows, in milliseconds. */
    static final long WINDOW_MS = 60_000;

    private static final int MAX_ENTRIES_PER_LEDGER = 2_000_000;

    private final BookKeeper bookKeeper;
    private final int ensembleSize;
    private final int writeQuorum;
    private final int ackQuorum;

    /** Window start (ms) → the ledger that receives the copies of that window. */
    private final Map<Long, WriteHandle> windowLedgers = new ConcurrentHashMap<>();

    /** Ledger id → next entry id to read (the drain position of that ledger). */
    private final Map<Long, Long> drainOffsets = new ConcurrentHashMap<>();

    /** Entry count per ledger, to roll a window ledger that outgrows the limit. */
    private final Map<Long, Long> ledgerEntryCounts = new ConcurrentHashMap<>();

    JournalStorage(PulsarService pulsarService) throws Exception {
        ServiceConfiguration config = pulsarService.getConfig();
        this.bookKeeper = pulsarService.getBookKeeperClientFactory().create(
                config, pulsarService.getLocalMetadataStore(), pulsarService.getIoEventLoopGroup(),
                java.util.Optional.empty(), null).get(30, TimeUnit.SECONDS);
        this.ensembleSize = config.getManagedLedgerDefaultEnsembleSize();
        this.writeQuorum = config.getManagedLedgerDefaultWriteQuorum();
        this.ackQuorum = config.getManagedLedgerDefaultAckQuorum();
    }

    CompletableFuture<Long> start() {
        // No ledger is created eagerly: the first append of a window creates its ledger.
        return CompletableFuture.completedFuture(0L);
    }

    /** Returns the window a deliverAt timestamp falls into. */
    static long windowOf(long deliverAt) {
        return Math.floorDiv(deliverAt, WINDOW_MS) * WINDOW_MS;
    }

    /** Appends a framed copy of the entry into the ledger of the entry's delivery window. */
    synchronized CompletableFuture<Long> append(long deliverAt, long dataLedgerId, long dataEntryId,
            ByteBuf entryData) {
        long window = windowOf(deliverAt);
        WriteHandle ledger = windowLedgers.get(window);
        if (ledger != null) {
            return appendTo(ledger, deliverAt, dataLedgerId, dataEntryId, entryData);
        }
        synchronized (this) {
            ledger = windowLedgers.get(window);
            if (ledger != null) {
                return appendTo(ledger, deliverAt, dataLedgerId, dataEntryId, entryData);
            }
            return newLedger().thenCompose(newHandle -> {
                WriteHandle existing = windowLedgers.putIfAbsent(window, newHandle);
                if (existing != null) {
                    newHandle.closeAsync();
                    return appendTo(existing, deliverAt, dataLedgerId, dataEntryId, entryData);
                }
                return appendTo(newHandle, deliverAt, dataLedgerId, dataEntryId, entryData);
            });
        }
    }

    private CompletableFuture<Long> appendTo(WriteHandle ledger, long deliverAt, long dataLedgerId,
            long dataEntryId, ByteBuf entryData) {
        ByteBuf framed = Unpooled.buffer(HEADER_SIZE + entryData.readableBytes());
        framed.writeLong(deliverAt);
        framed.writeLong(dataLedgerId);
        framed.writeLong(dataEntryId);
        framed.writeBytes(entryData, entryData.readerIndex(), entryData.readableBytes());

        return ledger.appendAsync(framed).whenComplete((entryId, ex) -> framed.release())
                .thenApply(entryId -> entryId);
    }

    /**
     * Reads the next batch of the window ledgers due at or before {@code cutoffTime}. Ledgers are
     * drained in window order; a ledger is drained only from its saved offset, so the reads are
     * sequential within each window. Returns the framed records that are due, in journal order.
     */
    synchronized CompletableFuture<List<JournalRecord>> readDue(long cutoffTime, int maxRecords) {
        List<CompletableFuture<List<JournalRecord>>> drains = new ArrayList<>();
        List<Long> expiredLedgers = new ArrayList<>();
        // Window order: earlier windows first, then the current one.
        new TreeMap<>(windowLedgers).forEach((window, ledger) -> {
            if (window > cutoffTime) {
                return; // a future window: nothing in it can be due
            }
            Long offset = drainOffsets.computeIfAbsent(ledger.getId(), id -> 0L);
            boolean fullyDrained = offset > ledger.getLastAddConfirmed();
            boolean windowExpired = window + WINDOW_MS <= cutoffTime;
            if (fullyDrained) {
                if (windowExpired) {
                    // The window has passed and the drain is past its end: no future read can
                    // come from this ledger. Delete it to reclaim the disk space.
                    expiredLedgers.add(ledger.getId());
                }
                return;
            }
            long last = Math.min(offset + maxRecords - 1, ledger.getLastAddConfirmed());
            drains.add(readHandle(ledger.getId()).thenCompose(rh -> rh.readAsync(offset, last))
                    .thenApply(entries -> {
                        List<JournalRecord> due = new ArrayList<>();
                        long nextOffset = offset;
                        for (LedgerEntry e : entries) {
                            ByteBuf buf = e.getEntryBuffer();
                            JournalRecord record;
                            try {
                                record = JournalRecord.parse(e.getLedgerId(), e.getEntryId(), buf);
                            } finally {
                                e.close();
                            }
                            if (record.deliverAt <= cutoffTime) {
                                due.add(record);
                                nextOffset = e.getEntryId() + 1;
                            } else {
                                // Not due yet: hold the drain here; the arrival order within
                                // the window means a later record can carry an earlier deliverAt,
                                // so the offset only advances past records that are due.
                                break;
                            }
                        }
                        drainOffsets.put(ledger.getId(), nextOffset);
                        return due;
                    }));
        });
        for (Long ledgerId : expiredLedgers) {
            removeWindowLedger(ledgerId);
        }
        if (drains.isEmpty()) {
            return CompletableFuture.completedFuture(List.of());
        }
        return CompletableFuture.allOf(drains.toArray(new CompletableFuture[0]))
                .thenApply(__ -> {
                    List<JournalRecord> all = new ArrayList<>();
                    drains.forEach(d -> all.addAll(d.join()));
                    return all;
                });
    }

    /** Removes a fully-drained, expired window ledger from the maps and deletes it from BK. */
    private void removeWindowLedger(long ledgerId) {
        windowLedgers.entrySet().removeIf(e -> e.getValue().getId() == ledgerId);
        drainOffsets.remove(ledgerId);
        ledgerEntryCounts.remove(ledgerId);
        bookKeeper.newDeleteLedgerOp().withLedgerId(ledgerId).execute()
                .whenComplete((__, ex) -> {
                    if (ex != null) {
                        log.warn().attr("ledgerId", ledgerId).exceptionMessage(ex)
                                .log("Failed to delete the drained journal ledger;"
                                        + " it will be retried after the next restart");
                    }
                });
    }

    /** Deletes every journal ledger, used by clear and close. */
    CompletableFuture<Void> deleteAll() {
        List<Long> ledgerIds = new ArrayList<>(windowLedgers.values().stream().map(WriteHandle::getId).toList());
        windowLedgers.clear();
        drainOffsets.clear();
        ledgerEntryCounts.clear();
        List<CompletableFuture<Void>> deletes = new ArrayList<>();
        for (Long ledgerId : ledgerIds) {
            deletes.add(bookKeeper.newDeleteLedgerOp().withLedgerId(ledgerId).execute()
                    .thenApply(__ -> null));
        }
        return CompletableFuture.allOf(deletes.toArray(new CompletableFuture[0]));
    }

    private CompletableFuture<WriteHandle> newLedger() {
        return bookKeeper.newCreateLedgerOp()
                .withEnsembleSize(ensembleSize).withWriteQuorumSize(writeQuorum)
                .withAckQuorumSize(ackQuorum)
                .withDigestType(DigestType.CRC32C)
                .withPassword(new byte[0])
                .execute();
    }

    private CompletableFuture<ReadHandle> readHandle(long ledgerId) {
        return bookKeeper.newOpenLedgerOp().withLedgerId(ledgerId)
                .withDigestType(DigestType.CRC32C).withPassword(new byte[0]).execute();
    }

    /** A parsed journal record: where the copy lives and where the original entry lives. */
    record JournalRecord(long journalLedgerId, long journalEntryId, long deliverAt, long dataLedgerId,
            long dataEntryId, ByteBuf entryData) {

        static JournalRecord parse(long journalLedgerId, long journalEntryId, ByteBuf framed) {
            int readerIndex = framed.readerIndex();
            long deliverAt = framed.readLong();
            long dataLedgerId = framed.readLong();
            long dataEntryId = framed.readLong();
            // Retain the payload: the ledger entry that owns the buffer is closed inside readDue,
            // while this record outlives it until the dispatcher sends the entry.
            ByteBuf data = framed.retainedSlice();
            framed.readerIndex(readerIndex);
            return new JournalRecord(journalLedgerId, journalEntryId, deliverAt, dataLedgerId,
                    dataEntryId, data);
        }
    }

    List<Long> journalLedgerIds() {
        return new ArrayList<>(windowLedgers.keySet().stream()
                .map(windowLedgers::get).map(WriteHandle::getId).toList());
    }

    CompletableFuture<Void> close() {
        return deleteAll().whenComplete((__, ex) -> {
            if (ex != null) {
                log.warn().exceptionMessage(ex).log("Failed to delete the journal ledgers on close");
            }
            try {
                bookKeeper.close();
            } catch (Exception e) {
                log.warn().exceptionMessage(e).log("Failed to close the journal BookKeeper client");
            }
        });
    }
}
