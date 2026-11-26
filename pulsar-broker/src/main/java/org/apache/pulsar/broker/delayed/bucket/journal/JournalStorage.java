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
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import lombok.CustomLog;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.api.DigestType;
import org.apache.bookkeeper.client.api.LedgerEntries;
import org.apache.bookkeeper.client.api.WriteHandle;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;

/**
 * Append-only journal ledgers for the {@link JournalingBucketDelayedDeliveryTracker}: every
 * delayed entry is copied, framed and appended in arrival order, so the write side is always
 * sequential. The ledgers roll over by entry count to bound a single ledger's size.
 */
@CustomLog
class JournalStorage {

    /** Framed journal record: deliverAt | dataLedgerId | dataEntryId | entryData (no length header —
     * the entry length is the BK entry length). */
    static final int HEADER_SIZE = 8 + 8 + 8;

    private static final int ROLLOVER_ENTRIES = 50_000;

    private final BookKeeper bookKeeper;
    private final int ensembleSize;
    private final int writeQuorum;
    private final int ackQuorum;

    private volatile WriteHandle activeLedger;

    /** Ledgers whose appends are complete; new writes go to {@link #activeLedger}. */
    private final List<Long> sealedLedgers = new CopyOnWriteArrayList<>();

    private long entriesInActiveLedger = 0L;

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
        return createWriteHandle().thenApply(lh -> {
            activeLedger = lh;
            entriesInActiveLedger = 0;
            return lh.getId();
        });
    }

    /** Appends a framed copy of the entry, rolling over to a fresh ledger past the entry limit. */
    synchronized CompletableFuture<Long> append(long deliverAt, long dataLedgerId, long dataEntryId,
            ByteBuf entryData) {
        WriteHandle ledger = activeLedger;
        if (ledger == null) {
            return CompletableFuture.failedFuture(
                    new IllegalStateException("Journal storage not started or already closed"));
        }

        ByteBuf framed = Unpooled.buffer(HEADER_SIZE + entryData.readableBytes());
        framed.writeLong(deliverAt);
        framed.writeLong(dataLedgerId);
        framed.writeLong(dataEntryId);
        framed.writeBytes(entryData, entryData.readerIndex(), entryData.readableBytes());

        long ledgerIdAtAppend = ledger.getId();
        boolean rollover = ++entriesInActiveLedger >= ROLLOVER_ENTRIES;
        return ledger.appendAsync(framed).thenApply(entryId -> {
            framed.release();
            return entryId;
        }).whenComplete((__, ex) -> {
            if (rollover && ex == null) {
                rollover(ledger);
            }
        }).thenApply(__ -> ledgerIdAtAppend);
    }

    private void rollover(WriteHandle previous) {
        sealedLedgers.add(previous.getId());
        previous.closeAsync().whenComplete((__, ex) -> {
            if (ex != null) {
                log.warn().attr("ledgerId", previous.getId()).exceptionMessage(ex)
                        .log("Failed to close the rolled-over journal ledger");
            }
        });
        createWriteHandle().thenAccept(newLedger -> {
            synchronized (this) {
                if (activeLedger == previous) {
                    activeLedger = newLedger;
                    entriesInActiveLedger = 0;
                } else {
                    // another rollover won the race or the storage closed
                    newLedger.closeAsync();
                }
            }
        }).exceptionally(ex -> {
            log.warn().exceptionMessage(ex).log("Failed to roll over the journal ledger;"
                    + " further appends keep going to the previous ledger");
            return null;
        });
    }

    private CompletableFuture<WriteHandle> createWriteHandle() {
        return bookKeeper.newCreateLedgerOp()
                .withEnsembleSize(ensembleSize).withWriteQuorumSize(writeQuorum)
                .withAckQuorumSize(ackQuorum)
                .withDigestType(DigestType.CRC32C)
                .withPassword(new byte[0])
                .execute();
    }

    CompletableFuture<LedgerEntries> read(long ledgerId, long firstEntry, long lastEntry) {
        return bookKeeper.newOpenLedgerOp().withLedgerId(ledgerId)
                .withDigestType(DigestType.CRC32C).withPassword(new byte[0]).execute()
                .thenCompose(lh -> lh.readAsync(firstEntry, lastEntry));
    }

    List<Long> journalLedgerIds() {
        List<Long> ids = new java.util.ArrayList<>(sealedLedgers);
        WriteHandle ledger = activeLedger;
        if (ledger != null) {
            ids.add(ledger.getId());
        }
        return ids;
    }

    CompletableFuture<Void> close() {
        WriteHandle ledger = activeLedger;
        activeLedger = null;
        CompletableFuture<Void> closed = ledger == null ? CompletableFuture.completedFuture(null)
                : ledger.closeAsync();
        return closed.whenComplete((__, ex) -> {
            try {
                bookKeeper.close();
            } catch (Exception e) {
                log.warn().exceptionMessage(e).log("Failed to close the journal BookKeeper client");
            }
        });
    }
}
