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
package org.apache.pulsar.broker.delayed.routing;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.LongAdder;
import lombok.CustomLog;
import org.apache.bookkeeper.client.api.DigestType;
import org.apache.bookkeeper.client.api.LedgerEntry;
import org.apache.bookkeeper.client.api.ReadHandle;
import org.apache.bookkeeper.client.api.WriteHandle;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;

/**
 * Routes delayed messages to a time-windowed delay storage instead of the data ledger, and writes
 * them back to the topic's tail when due — the consumer always reads sequentially, with no holes.
 *
 * <p>The write path intercepts {@code PersistentTopic.publishMessage}: a message with
 * {@code deliverAtTime} in the future goes to the delay storage (a BK ledger per 60-second
 * window, shared by all subscriptions of the topic); the producer gets the delay-storage position
 * as its messageId. A timer drains the due entries and rewrites each one to the data ledger's
 * tail via the topic's normal publish path; the consumer sees the rewritten entry at its tail
 * position. The delay-storage window ledgers are deleted after they expire and drain.
 */
@CustomLog
public final class TopicDelayRouter implements AutoCloseable {

    /** Delivery time windows, in milliseconds. */
    static final long WINDOW_MS = 60_000;

    /** How many journal records one drain round reads at most. */
    static final int DRAIN_BATCH = 500;

    /** Framed record header: deliverAt(8B). The body follows as the rest of the BK entry. */
    static final int HEADER_SIZE = 8;

    private final PersistentTopic topic;
    private final org.apache.bookkeeper.client.BookKeeper bookKeeper;
    private final int ensembleSize;
    private final int writeQuorum;
    private final int ackQuorum;
    private final long tickTimeMillis;

    /** Window start (ms) → the ledger that receives the copies of that window. */
    private final Map<Long, WriteHandle> windowLedgers = new ConcurrentHashMap<>();

    /** Ledger id → next entry id to drain. */
    private final Map<Long, Long> drainOffsets = new ConcurrentHashMap<>();

    private final AtomicBoolean closed = new AtomicBoolean(false);

    private final LongAdder routedEntries = new LongAdder();
    private final LongAdder rewrittenEntries = new LongAdder();
    private final LongAdder failedEntries = new LongAdder();

    public TopicDelayRouter(PersistentTopic topic, PulsarService pulsar) throws Exception {
        this.topic = topic;
        ServiceConfiguration config = pulsar.getConfig();
        this.bookKeeper = pulsar.getBookKeeperClientFactory().create(
                config, pulsar.getLocalMetadataStore(), pulsar.getIoEventLoopGroup(),
                java.util.Optional.empty(), null).get(30, TimeUnit.SECONDS);
        this.ensembleSize = config.getManagedLedgerDefaultEnsembleSize();
        this.writeQuorum = config.getManagedLedgerDefaultWriteQuorum();
        this.ackQuorum = config.getManagedLedgerDefaultAckQuorum();
        this.tickTimeMillis = Math.max(100, config.getDelayedDeliveryTickTimeMillis());
        this.executor = pulsar.getExecutor();
    }

    private final java.util.concurrent.ScheduledExecutorService executor;

    /** Starts the due-rewrite timer. */
    public void start() {
        scheduleDrain();
    }

    private void scheduleDrain() {
        if (closed.get()) {
            return;
        }
        executor.schedule(this::drainOnce, tickTimeMillis, TimeUnit.MILLISECONDS);
    }

    private void drainOnce() {
        if (closed.get()) {
            return;
        }
        rewriteDue().whenComplete((__, ex) -> {
            if (ex != null) {
                log.warn()
                        .attr("topic", topic.getName())
                        .exceptionMessage(ex)
                        .log("Failed to rewrite due delayed entries; retried on the next tick");
            }
            scheduleDrain();
        });
    }

    /**
     * Routes a delayed message to the delay storage. Returns the delay-storage position as the
     * producer-visible messageId.
     */
    public CompletableFuture<long[]> append(long deliverAt, ByteBuf headersAndPayload) {
        long window = windowOf(deliverAt);
        WriteHandle ledger = windowLedgers.get(window);
        if (ledger != null) {
            return appendTo(ledger, deliverAt, headersAndPayload);
        }
        return newLedger().thenCompose(newHandle -> {
            WriteHandle existing = windowLedgers.putIfAbsent(window, newHandle);
            if (existing != null) {
                newHandle.closeAsync();
                return appendTo(existing, deliverAt, headersAndPayload);
            }
            return appendTo(newHandle, deliverAt, headersAndPayload);
        });
    }

    private CompletableFuture<long[]> appendTo(WriteHandle ledger, long deliverAt,
            ByteBuf headersAndPayload) {
        ByteBuf framed = Unpooled.buffer(HEADER_SIZE + headersAndPayload.readableBytes());
        framed.writeLong(deliverAt);
        framed.writeBytes(headersAndPayload, headersAndPayload.readerIndex(),
                headersAndPayload.readableBytes());
        return ledger.appendAsync(framed).whenComplete((entryId, ex) -> framed.release())
                .thenApply(entryId -> {
                    routedEntries.increment();
                    return new long[]{ledger.getId(), entryId};
                });
    }

    /**
     * Drains the due entries and rewrites each one to the data ledger's tail. The drain reads
     * window ledgers sequentially and holds at the first record that is not due yet.
     */
    public synchronized CompletableFuture<Void> rewriteDue() {
        if (closed.get()) {
            return CompletableFuture.completedFuture(null);
        }
        long cutoff = System.currentTimeMillis();
        List<CompletableFuture<Void>> rewrites = new ArrayList<>();
        List<Long> expiredLedgers = new ArrayList<>();

        new TreeMap<>(windowLedgers).forEach((window, ledger) -> {
            if (window > cutoff) {
                return; // a future window: nothing due
            }
            Long offset = drainOffsets.computeIfAbsent(ledger.getId(), id -> 0L);
            if (offset > ledger.getLastAddConfirmed()) {
                if (window + WINDOW_MS <= cutoff) {
                    expiredLedgers.add(ledger.getId());
                }
                return;
            }
            long last = Math.min(offset + DRAIN_BATCH - 1, ledger.getLastAddConfirmed());
            rewrites.add(readHandle(ledger.getId()).thenCompose(rh -> rh.readAsync(offset, last))
                    .thenAccept(entries -> {
                        long nextOffset = offset;
                        for (LedgerEntry e : entries) {
                            ByteBuf buf = e.getEntryBuffer();
                            ByteBuf body;
                            long deliverAt;
                            try {
                                deliverAt = buf.readLong();
                                body = buf.retainedSlice();
                            } finally {
                                e.close();
                            }
                            if (deliverAt <= cutoff) {
                                nextOffset = e.getEntryId() + 1;
                                // Write the original headers+payload to the data ledger's tail;
                                // the consumer reads this entry as a normal message.
                                topic.publishMessage(body.duplicate(), new ReroutePublishContext());
                                body.release();
                                rewrittenEntries.increment();
                            } else {
                                body.release();
                                break; // not due yet: hold the drain here
                            }
                        }
                        drainOffsets.put(ledger.getId(), nextOffset);
                    }));
        });

        for (Long ledgerId : expiredLedgers) {
            removeWindowLedger(ledgerId);
        }

        if (rewrites.isEmpty()) {
            return CompletableFuture.completedFuture(null);
        }
        return CompletableFuture.allOf(rewrites.toArray(new CompletableFuture[0]));
    }

    private void removeWindowLedger(long ledgerId) {
        windowLedgers.entrySet().removeIf(e -> e.getValue().getId() == ledgerId);
        drainOffsets.remove(ledgerId);
        bookKeeper.newDeleteLedgerOp().withLedgerId(ledgerId).execute()
                .whenComplete((__, ex) -> {
                    if (ex != null) {
                        log.warn().attr("ledgerId", ledgerId).exceptionMessage(ex)
                                .log("Failed to delete the drained delay-storage ledger");
                    }
                });
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

    static long windowOf(long deliverAt) {
        return Math.floorDiv(deliverAt, WINDOW_MS) * WINDOW_MS;
    }

    long routedEntries() {
        return routedEntries.sum();
    }

    long rewrittenEntries() {
        return rewrittenEntries.sum();
    }

    long failedEntries() {
        return failedEntries.sum();
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {

            List<Long> ledgerIds = new ArrayList<>(
                    windowLedgers.values().stream().map(WriteHandle::getId).toList());
            windowLedgers.clear();
            drainOffsets.clear();
            for (Long ledgerId : ledgerIds) {
                bookKeeper.newDeleteLedgerOp().withLedgerId(ledgerId).execute()
                        .whenComplete((__, ex) -> {
                            if (ex != null) {
                                log.warn().attr("ledgerId", ledgerId).exceptionMessage(ex)
                                        .log("Failed to delete the delay-storage ledger on close");
                            }
                        });
            }
            try {
                bookKeeper.close();
            } catch (Exception e) {
                log.warn().exceptionMessage(e).log("Failed to close the delay-storage BookKeeper client");
            }
        }
    }

    /** A no-op publish context for the rewrite path: stats are counted by the router itself. */
    private class ReroutePublishContext implements Topic.PublishContext {
        @Override
        public void completed(Exception exception, long ledgerId, long entryId) {
            if (exception != null) {
                failedEntries.increment();
                log.warn()
                        .attr("topic", topic.getName())
                        .exceptionMessage(exception)
                        .log("Failed to rewrite a delayed entry to the data ledger");
            }
        }

        @Override
        public String getProducerName() {
            return "delay-router";
        }

        @Override
        public long getSequenceId() {
            return -1;
        }
    }
}
