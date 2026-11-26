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
import java.time.Clock;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.LongAdder;
import lombok.CustomLog;
import org.apache.pulsar.broker.delayed.DelayedDeliveryContext;
import org.apache.pulsar.broker.delayed.bucket.BucketDelayedDeliveryTracker;
import org.apache.pulsar.broker.delayed.bucket.BucketSnapshotStorage;
import org.apache.pulsar.broker.delayed.bucket.RecoverDelayedDeliveryTrackerException;

/**
 * A bucket tracker that additionally copies every delayed entry into an append-only journal, in
 * arrival order, at the only point where the broker holds the entry buffer
 * ({@code trackDelayedDelivery}). The journal is the write-side experiment of the body-copy
 * design: due delivery still runs on the inherited bucket machinery (index + replay from the data
 * ledgers), so the extra sequential writes are measurable on their own before the read side
 * switches over.
 *
 * <p>Not durable across restarts: the journal ledgers are not recorded in cursor properties and
 * the read side does not use them yet, so a restart leaves them behind. This is an experiment
 * scaffold, not a production feature.
 */
@CustomLog
public class JournalingBucketDelayedDeliveryTracker extends BucketDelayedDeliveryTracker {

    private final JournalStorage journalStorage;

    private final LongAdder journaledEntries = new LongAdder();
    private final LongAdder journaledBytes = new LongAdder();
    private final LongAdder journalFailures = new LongAdder();

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
    }

    /**
     * Copies the serialized entry into the journal. The buffer belongs to the caller and is only
     * read here; the copy is framed and released inside the journal storage.
     */
    @Override
    public void addEntry(long ledgerId, long entryId, long deliveryAt, ByteBuf entryData) {
        JournalStorage storage = journalStorage;
        if (storage == null) {
            // The journal was never started (or already closed): count it and move on, the
            // inherited tracking already accepted the message and delivery is unaffected.
            journalFailures.increment();
            return;
        }
        int readableBytes = entryData.readableBytes();
        storage.append(deliveryAt, ledgerId, entryId, entryData)
                .whenComplete((journalLedgerId, ex) -> {
                    if (ex != null) {
                        journalFailures.increment();
                        log.warn()
                                .attr("ledgerId", ledgerId).attr("entryId", entryId)
                                .exceptionMessage(ex)
                                .log("Failed to journal the delayed entry;"
                                        + " delivery is unaffected");
                    } else {
                        journaledEntries.increment();
                        journaledBytes.add(readableBytes);
                    }
                });
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

    List<Long> journalLedgerIds() {
        return journalStorage.journalLedgerIds();
    }

    CompletableFuture<org.apache.bookkeeper.client.api.LedgerEntries> readJournal(
            long ledgerId, long firstEntry, long lastEntry) {
        return journalStorage.read(ledgerId, firstEntry, lastEntry);
    }

    static long headerSize() {
        return JournalStorage.HEADER_SIZE;
    }
}
