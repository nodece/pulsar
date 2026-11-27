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

import com.google.common.annotations.VisibleForTesting;
import io.netty.buffer.ByteBuf;
import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import java.util.NavigableSet;
import java.util.concurrent.atomic.LongAdder;
import lombok.CustomLog;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.PositionFactory;
import org.apache.bookkeeper.mledger.impl.EntryImpl;
import org.apache.pulsar.broker.delayed.DelayedDeliveryContext;
import org.apache.pulsar.broker.delayed.bucket.BucketDelayedDeliveryTracker;
import org.apache.pulsar.broker.delayed.bucket.BucketSnapshotStorage;
import org.apache.pulsar.broker.delayed.bucket.RecoverDelayedDeliveryTrackerException;

/**
 * A bucket tracker that also copies every delayed entry into time-window journal ledgers and
 * serves due delivery from those journals sequentially, so the due path performs no random read on
 * the data ledgers. The copy is taken at the only point where the broker holds the entry buffer
 * ({@code trackDelayedDelivery}) and routed to the ledger of the entry's delivery window.
 *
 * <p>Due delivery is journal-driven: the drain reads the current windows' ledgers in journal
 * order, and a record is served only when its position is still tracked by the inherited index
 * ({@code removeIndexBit}) and not acknowledged — the index bitmap is the single source of
 * truth for what is pending, so journal and regular delivery cannot double-deliver. Whatever the
 * index still reports as due after the drain (a failed or still-in-flight journal copy) is
 * replayed from the data ledgers through the regular cursor path and merged into the same
 * completion.
 *
 * <p>Entries are wrapped with their original positions, so acknowledgements, cursors and the
 * redelivery bookkeeping are unchanged.
 *
 * <p>Not durable across restarts: the journal ledgers and drain offsets are not recorded in
 * cursor properties, so a restart falls back to the inherited delivery entirely (the journal
 * copies are abandoned). This is an experiment, not a production feature.
 */
@CustomLog
public class JournalingBucketDelayedDeliveryTracker extends BucketDelayedDeliveryTracker
        implements org.apache.pulsar.broker.delayed.DueEntryReader {

    /** How many journal records one dispatch round reads at most. */
    static final int DRAIN_BATCH = 500;

    private static final int MAX_RECORDED_FAILURES = 100;

    private final JournalStorage journalStorage;

    /** The subscription's cursor, captured from the context at construction. */
    private final ManagedCursor cursor;

    private final LongAdder journaledEntries = new LongAdder();
    private final LongAdder journaledBytes = new LongAdder();
    private final LongAdder journalFailures = new LongAdder();
    private final LongAdder journalDelivered = new LongAdder();
    private final LongAdder journalFallbackDelivered = new LongAdder();

    JournalingBucketDelayedDeliveryTracker(DelayedDeliveryContext context,
            io.netty.util.Timer timer, long tickTimeMillis, Clock clock,
            boolean isDelayedDeliveryDeliverAtTimeStrict,
            BucketSnapshotStorage bucketSnapshotStorage,
            long minIndexCountPerBucket, long timeStepPerBucketSnapshotSegmentInMillis,
            int maxIndexesPerBucketSnapshotSegment, int maxNumBuckets,
            JournalStorage journalStorage) throws RecoverDelayedDeliveryTrackerException {
        super(context, timer, tickTimeMillis, clock, isDelayedDeliveryDeliverAtTimeStrict,
                bucketSnapshotStorage, minIndexCountPerBucket, timeStepPerBucketSnapshotSegmentInMillis,
                maxIndexesPerBucketSnapshotSegment, maxNumBuckets);
        this.journalStorage = journalStorage;
        this.cursor = context.getCursor();
    }

    /**
     * Copies the serialized entry into its delivery window's journal. The buffer belongs to the
     * caller and is only read here; the copy is framed and released inside the journal storage.
     */
    @Override
    public void addEntry(long ledgerId, long entryId, long deliveryAt, ByteBuf entryData) {
        JournalStorage storage = journalStorage;
        if (storage == null) {
            // The journal was never started (or already closed): count it and move on, the
            // inherited tracking already accepted the message; its delivery uses the fallback.
            journalFailures.increment();
            return;
        }
        int readableBytes = entryData.readableBytes();
        storage.append(deliveryAt, ledgerId, entryId, entryData)
                .whenComplete((journalLedgerId, ex) -> {
                    if (ex != null) {
                        journalFailures.increment();
                        if (journalFailures.sum() <= MAX_RECORDED_FAILURES) {
                            log.warn()
                                    .attr("ledgerId", ledgerId).attr("entryId", entryId)
                                    .exceptionMessage(ex)
                                    .log("Failed to journal the delayed entry;"
                                            + " its delivery falls back to the data-ledger replay");
                        }
                    } else {
                        journaledEntries.increment();
                        journaledBytes.add(readableBytes);
                    }
                });
    }

    /**
     * Serves one dispatch round. The journal drain runs first: every drained record whose
     * position the index still tracks and the cursor has not acknowledged becomes an entry,
     * wrapped with its original position. Then the inherited {@link #getScheduledMessages(int)}
     * reports what is still due after the drain — the copies the journal does not have — and the
     * regular cursor replay reads them from the data ledgers. Both sources merge into a single
     * {@code readEntriesComplete} on the dispatcher's callback; a journal read failure falls the
     * whole round back to the regular replay.
     */
    @Override
    public void readDueEntries(AsyncCallbacks.ReadEntriesCallback callback, Object ctx, int maxMessages) {
        JournalStorage storage = journalStorage;
        if (storage == null) {
            replayFromDataLedgers(callback, ctx, maxMessages);
            return;
        }

        long cutoff = clock.millis();
        storage.readDue(cutoff, DRAIN_BATCH).whenComplete((records, ex) -> {
            if (ex != null) {
                log.warn().exceptionMessage(ex)
                        .log("Failed to read the delayed-delivery journal;"
                                + " the round falls back to the data-ledger replay");
                replayFromDataLedgers(callback, ctx, maxMessages);
                return;
            }

            List<Entry> journalEntries = new ArrayList<>();
            for (JournalStorage.JournalRecord record : records) {
                Position position = PositionFactory.create(record.dataLedgerId(), record.dataEntryId());
                if (cursor.isMessageDeleted(position)) {
                    // The original was acknowledged (or the subscription reset): drop the copy.
                    continue;
                }
                // The index is the single source of pending truth: serving the copy and untracking
                // the position are one step, so neither delivery path can serve it twice.
                if (removeIndexBit(position.getLedgerId(), position.getEntryId())) {
                    journalEntries.add(EntryImpl.create(position, record.entryData(), 0));
                    journalDelivered.increment();
                }
                // else: already untracked (delivered by an earlier round or never journaled
                // through this tracker) — skip.
            }

            // What the index still reports as due is exactly what the drain could not serve.
            NavigableSet<Position> remaining = getScheduledMessages(maxMessages);
            if (remaining.isEmpty()) {
                complete(journalEntries, callback, ctx);
                return;
            }

            cursor.asyncReplayEntries(remaining, new AsyncCallbacks.ReadEntriesCallback() {
                @Override
                public void readEntriesComplete(List<Entry> entries, Object replayCtx) {
                    List<Entry> merged = new ArrayList<>(journalEntries.size() + entries.size());
                    merged.addAll(journalEntries);
                    merged.addAll(entries);
                    journalFallbackDelivered.add(entries.size());
                    merged.sort((a, b) -> a.getPosition().compareTo(b.getPosition()));
                    callback.readEntriesComplete(merged, replayCtx);
                }

                @Override
                public void readEntriesFailed(ManagedLedgerException exception, Object replayCtx) {
                    // The journal part succeeded but the fallback did not: release the copies and
                    // fail the round; the dispatcher retries the whole round on the regular path.
                    journalEntries.forEach(Entry::release);
                    callback.readEntriesFailed(exception, replayCtx);
                }
            }, ctx, true);
        });
    }

    /** The regular path: everything due comes from the data ledgers, as in the bucket tracker. */
    private void replayFromDataLedgers(AsyncCallbacks.ReadEntriesCallback callback,
            Object ctx, int maxMessages) {
        NavigableSet<Position> due = getScheduledMessages(maxMessages);
        if (due.isEmpty()) {
            complete(List.of(), callback, ctx);
            return;
        }
        cursor.asyncReplayEntries(due, callback, ctx, true);
    }

    private static void complete(List<Entry> entries, AsyncCallbacks.ReadEntriesCallback callback,
            Object ctx) {
        callback.readEntriesComplete(entries, ctx);
    }

    long journaledEntries() {
        return journaledEntries.sum();
    }

    long journaledBytes() {
        return journaledBytes.sum();
    }

    long journalFailures() {
        return journalFailures.sum();
    }

    long journalDelivered() {
        return journalDelivered.sum();
    }

    long journalFallbackDelivered() {
        return journalFallbackDelivered.sum();
    }

    @VisibleForTesting
    JournalStorage journalStorageForTest() {
        return journalStorage;
    }

    static long headerSize() {
        return JournalStorage.HEADER_SIZE;
    }

    static long windowOf(long deliverAt) {
        return JournalStorage.windowOf(deliverAt);
    }
}
