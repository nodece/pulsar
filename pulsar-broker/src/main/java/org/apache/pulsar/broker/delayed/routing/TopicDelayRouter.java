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
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
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
 *
 * <p>The drain is driven by an exact-due timer modeled on the tracker timer
 * ({@code AbstractDelayedDeliveryTracker.updateTimer()}): the earliest known {@code deliverAt}
 * arms a one-shot timeout, an append that moves it earlier re-arms the timeout, and a completed
 * round recomputes it from what the drain saw (the held-back record, the future windows, the
 * batch truncation) — there is no fixed-rate polling. The firing rate is floored at one
 * {@code tickTimeMillis} per round, so a backlog that cannot drain in a single round retries on
 * successive ticks instead of hot-looping. A hashed timing wheel (Varghese &amp; Lauck, SOSP'87)
 * earns its complexity with many concurrent timeouts; this router holds one per topic, where a
 * precise one-shot schedule is both simpler and more accurate than a wheel rounded to its tick
 * duration.
 *
 * <p>Within a window the drain follows arrival order and holds at the first record that is not
 * due yet, so a not-due record that arrived before an already-due one delays that delivery
 * until it becomes due — the lateness is bounded by the window width. Skipping past the head
 * would need a per-entry rewrite index and re-reads of the held prefix every round, which is
 * the very index this write-routing design exists to avoid.
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

    private final ScheduledExecutorService executor;

    /**
     * Earliest known {@code deliverAt} among routed-but-unrewritten entries;
     * {@link Long#MAX_VALUE} when nothing is pending. Lowered at append time (before the BK
     * write completes — delivery must not wait for the ack) and recomputed at the end of every
     * drain round; see {@link #recomputeNextDeliverAt(long, long, long)}.
     */
    private final AtomicLong nextDeliverAt = new AtomicLong(Long.MAX_VALUE);

    /**
     * Bumped when a routed append becomes visible to drains (its BK write completes); lets a
     * drain round tell whether its recompute raced an append.
     */
    private final AtomicLong appendVersion = new AtomicLong();

    /** Serializes drain rounds so one round's callbacks cannot interleave another's. */
    private final AtomicBoolean drainInFlight = new AtomicBoolean(false);

    /**
     * Guards the timer state below. A leaf lock: no router method is invoked while holding it,
     * mirroring {@code AbstractDelayedDeliveryTracker}'s timeoutLock.
     */
    private final Object timerLock = new Object();

    private ScheduledFuture<?> armedTimer;        // Guarded by timerLock
    private long currentTarget = Long.MAX_VALUE;  // Guarded by timerLock
    private long lastFireAt;                      // Guarded by timerLock
    private long timerGeneration;                 // Guarded by timerLock

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

    /** Arms the timer if any routed entry is already due. */
    public void start() {
        updateTimer();
    }

    /**
     * Arms the timer to fire when {@link #nextDeliverAt} becomes due. Mirrors
     * {@code AbstractDelayedDeliveryTracker.updateTimer()}: the timer is armed for the earliest
     * known target, only re-armed when the target moves earlier, and never fires faster than one
     * {@link #tickTimeMillis} after the last round.
     */
    private void updateTimer() {
        long target = nextDeliverAt.get();
        synchronized (timerLock) {
            if (closed.get()) {
                return;
            }
            if (armedTimer != null) {
                if (target == currentTarget || currentTarget < target) {
                    // Already armed at this target, or at an earlier one — an early round is a
                    // harmless no-op followed by a recompute.
                    return;
                }
                armedTimer.cancel(false);
            }
            if (target == Long.MAX_VALUE) {
                currentTarget = Long.MAX_VALUE;
                armedTimer = null;
                return;
            }
            long now = System.currentTimeMillis();
            long dueDelay = Math.max(0, target - now);
            long tickFloor = Math.max(0, lastFireAt + tickTimeMillis - now);
            currentTarget = target;
            long generation = ++timerGeneration;
            armedTimer = executor.schedule(() -> onTimer(generation),
                    Math.max(dueDelay, tickFloor), TimeUnit.MILLISECONDS);
        }
    }

    /**
     * A timer firing. Only the live generation proceeds: a timeout that raced its cancellation
     * may still fire and must not clear the newer timer's state (as in upstream #25996).
     */
    private void onTimer(long generation) {
        synchronized (timerLock) {
            if (closed.get() || generation != timerGeneration) {
                return;
            }
            armedTimer = null;
            currentTarget = Long.MAX_VALUE;
            lastFireAt = System.currentTimeMillis();
        }
        runDrain();
    }

    private void runDrain() {
        if (closed.get() || !drainInFlight.compareAndSet(false, true)) {
            // A round is already running; its completion re-arms the timer from a floor that is
            // never later than this firing's target.
            return;
        }
        CompletableFuture<Void> round;
        try {
            round = rewriteDue();
        } catch (Throwable t) {
            // A synchronous failure would otherwise leave the in-flight flag stuck and stall
            // every later round.
            drainInFlight.set(false);
            log.warn()
                    .attr("topic", topic.getName())
                    .exceptionMessage(t)
                    .log("Failed to start a rewrite round; retried on the next tick");
            updateTimer();
            return;
        }
        round.whenComplete((__, ex) -> {
            drainInFlight.set(false);
            if (ex != null) {
                log.warn()
                        .attr("topic", topic.getName())
                        .exceptionMessage(ex)
                        .log("Failed to rewrite due delayed entries; retried on the next tick");
            }
            // On failure the round floored the next due at its cutoff, so this arms the retry;
            // on success it arms the recomputed target.
            updateTimer();
        });
    }

    /**
     * Routes a delayed message to the delay storage. Returns the delay-storage position as the
     * producer-visible messageId.
     */
    public CompletableFuture<long[]> append(long deliverAt, ByteBuf headersAndPayload) {
        // Track the due time before the BK write: the timer must not wait for the ack, or the
        // write's visibility gap would delay delivery.
        nextDeliverAt.accumulateAndGet(deliverAt, Math::min);
        updateTimer();
        return route(deliverAt, headersAndPayload).whenComplete((position, ex) -> {
            // The record is now visible to drains (or the write failed). Re-assert its due time —
            // a round that completed during the write may have raised the floor past it — and
            // publish the visibility version the recompute relies on. On failure this only buys
            // an early no-op round; the next recompute raises the floor again.
            nextDeliverAt.accumulateAndGet(deliverAt, Math::min);
            appendVersion.incrementAndGet();
            updateTimer();
        });
    }

    private CompletableFuture<long[]> route(long deliverAt, ByteBuf headersAndPayload) {
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
     *
     * <p>The round also recomputes the timer's next-due floor from what it saw: the held-back
     * record's {@code deliverAt} (exact), the start of a future window (a lower bound for every
     * record in it), and {@code cutoff + 1} when a window's drain was truncated by
     * {@link #DRAIN_BATCH} with everything read already due.
     */
    public synchronized CompletableFuture<Void> rewriteDue() {
        if (closed.get()) {
            return CompletableFuture.completedFuture(null);
        }
        long version = appendVersion.get();
        long cutoff = System.currentTimeMillis();
        AtomicLong nextKnown = new AtomicLong(Long.MAX_VALUE);
        List<CompletableFuture<Void>> rewrites = new ArrayList<>();
        List<Long> expiredLedgers = new ArrayList<>();

        new TreeMap<>(windowLedgers).forEach((window, ledger) -> {
            if (window > cutoff) {
                // A future window: its start lower-bounds every record in it.
                nextKnown.accumulateAndGet(window, Math::min);
                return;
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
                        boolean heldBack = false;
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
                                // Not due yet: this is the window's exact next due time, and
                                // the arrival order within the window holds the drain here.
                                nextKnown.accumulateAndGet(deliverAt, Math::min);
                                heldBack = true;
                                break;
                            }
                        }
                        if (!heldBack && nextOffset <= ledger.getLastAddConfirmed()) {
                            // The batch was truncated but everything read was due: more due
                            // records may remain in this ledger; fire again as soon as allowed.
                            nextKnown.accumulateAndGet(cutoff + 1, Math::min);
                        }
                        drainOffsets.put(ledger.getId(), nextOffset);
                    }));
        });

        for (Long ledgerId : expiredLedgers) {
            removeWindowLedger(ledgerId);
        }

        CompletableFuture<Void> done = rewrites.isEmpty()
                ? CompletableFuture.completedFuture(null)
                : CompletableFuture.allOf(rewrites.toArray(new CompletableFuture[0]));
        return done.whenComplete((__, ex) -> {
            if (ex != null) {
                // A failed round read an unknown subset of the windows: fire again as soon as
                // the tick floor allows.
                nextDeliverAt.accumulateAndGet(cutoff + 1, Math::min);
            } else {
                recomputeNextDeliverAt(version, cutoff, nextKnown.get());
            }
        });
    }

    /**
     * Raises the next-due floor to what a completed round knows. When an append became visible
     * during the round (version changed), the floor may only be lowered to {@code cutoff + 1}:
     * a racing append's {@code deliverAt} is past the round's cutoff — routing only appends
     * future deliveries — so the timer fires no later than that append's due time, at worst one
     * early no-op round earlier.
     */
    private void recomputeNextDeliverAt(long version, long cutoff, long drainedFloor) {
        for (;;) {
            long current = nextDeliverAt.get();
            long target = appendVersion.get() == version
                    ? drainedFloor
                    : Math.min(Math.max(current, cutoff + 1), drainedFloor);
            if (current == target || nextDeliverAt.compareAndSet(current, target)) {
                return;
            }
        }
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
            synchronized (timerLock) {
                if (armedTimer != null) {
                    armedTimer.cancel(false);
                    armedTimer = null;
                }
                currentTarget = Long.MAX_VALUE;
            }

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
