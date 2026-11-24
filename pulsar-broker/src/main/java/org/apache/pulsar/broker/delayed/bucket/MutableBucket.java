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
package org.apache.pulsar.broker.delayed.bucket;

import static com.google.common.base.Preconditions.checkArgument;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import lombok.CustomLog;
import org.apache.pulsar.broker.delayed.proto.DelayedIndex;
import org.apache.pulsar.broker.delayed.proto.SnapshotMetadata;
import org.apache.pulsar.broker.delayed.proto.SnapshotSegment;
import org.apache.pulsar.broker.delayed.proto.SnapshotSegmentMetadata;
import org.apache.pulsar.common.util.collections.LongBitmap;
import org.apache.pulsar.common.util.collections.LongBitmaps;
import org.apache.pulsar.common.util.collections.TripleLongPriorityQueue;

@CustomLog
class MutableBucket implements AutoCloseable {

    private final BucketContext ctx;

    private final TripleLongPriorityQueue priorityQueue;

    long startLedgerId = -1L;
    long endLedgerId = -1L;

    MutableBucket(BucketContext ctx) {
        this.ctx = ctx;
        this.priorityQueue = new TripleLongPriorityQueue();
    }

    ImmutableBucket sealBucketAndAsyncPersistent(
            long timeStepPerBucketSnapshotSegment,
            int maxIndexesPerBucketSnapshotSegment) {
        return createImmutableBucketAndAsyncPersistent(timeStepPerBucketSnapshotSegment,
                maxIndexesPerBucketSnapshotSegment,
                TripleLongPriorityDelayedIndexQueue.wrap(priorityQueue), startLedgerId, endLedgerId);
    }

    ImmutableBucket createImmutableBucketAndAsyncPersistent(
            final long timeStepPerBucketSnapshotSegment, final int maxIndexesPerBucketSnapshotSegment,
            DelayedIndexQueue delayedIndexQueue, final long startLedgerId,
            final long endLedgerId) {
            log.debug()
                    .attr("dispatcher", ctx.dispatcherName())
                    .attr("startLedgerId", startLedgerId)
                    .attr("endLedgerId", endLedgerId)
                    .log("Creating bucket snapshot");
                if (delayedIndexQueue.isEmpty()) {
            return null;
        }
        long numMessages = 0;

        List<SnapshotSegment> bucketSnapshotSegments = new ArrayList<>();
        List<SnapshotSegmentMetadata> segmentMetadataList = new ArrayList<>();
        Map<Long, LongBitmap> immutableBucketBitMap = new HashMap<>();

        Map<Long, LongBitmap> bitMap = new HashMap<>();
        SnapshotSegment snapshotSegment = new SnapshotSegment();
        SnapshotSegmentMetadata segmentMetadata = new SnapshotSegmentMetadata();

        List<Long> firstScheduleTimestamps = new ArrayList<>();
        long currentTimestampUpperLimit = 0;
        long currentFirstTimestamp = 0L;
        while (!delayedIndexQueue.isEmpty()) {
            final long timestamp = delayedIndexQueue.peekTimestamp();
            if (currentTimestampUpperLimit == 0) {
                currentFirstTimestamp = timestamp;
                firstScheduleTimestamps.add(currentFirstTimestamp);
                currentTimestampUpperLimit = timestamp + timeStepPerBucketSnapshotSegment - 1;
            }

            DelayedIndex delayedIndex = snapshotSegment.addIndexe();
            delayedIndexQueue.popToObject(delayedIndex);

            final long ledgerId = delayedIndex.getLedgerId();
            final long entryId = delayedIndex.getEntryId();

            checkArgument(ledgerId >= startLedgerId && ledgerId <= endLedgerId);

            bitMap.computeIfAbsent(ledgerId, k -> LongBitmaps.create()).add(entryId);

            numMessages++;

            if (delayedIndexQueue.isEmpty() || delayedIndexQueue.peekTimestamp() > currentTimestampUpperLimit
                    || (maxIndexesPerBucketSnapshotSegment != -1
                    && snapshotSegment.getIndexesCount() >= maxIndexesPerBucketSnapshotSegment)) {
                segmentMetadata.setMaxScheduleTimestamp(timestamp);
                segmentMetadata.setMinScheduleTimestamp(currentFirstTimestamp);
                currentTimestampUpperLimit = 0;

                Iterator<Map.Entry<Long, LongBitmap>> iterator = bitMap.entrySet().iterator();
                while (iterator.hasNext()) {
                    final var entry = iterator.next();
                    final var lId = entry.getKey();
                    final var bm = entry.getValue();
                    segmentMetadata.putDelayedIndexBitMap(lId, bm.serialize());
                    immutableBucketBitMap.compute(lId, (__, bm0) -> {
                        if (bm0 == null) {
                            return bm;
                        }
                        bm0.or(bm);
                        return bm0;
                    });
                    iterator.remove();
                }

                segmentMetadataList.add(segmentMetadata);
                segmentMetadata = new SnapshotSegmentMetadata();

                bucketSnapshotSegments.add(snapshotSegment);
                snapshotSegment = new SnapshotSegment();
            }
        }

        SnapshotMetadata bucketSnapshotMetadata = new SnapshotMetadata();
        for (SnapshotSegmentMetadata sm : segmentMetadataList) {
            bucketSnapshotMetadata.addMetadata().copyFrom(sm);
        }

        final int lastSegmentEntryId = segmentMetadataList.size();

        ImmutableBucket bucket = new ImmutableBucket(ctx, startLedgerId, endLedgerId);
        // Segments are loaded lazily: 0 means no segment is resident, so the next load fetches
        // segment 1 whose earliest timestamp is firstScheduleTimestamps.get(0).
        bucket.setCurrentSegmentEntryId(0);
        bucket.setNumberBucketDelayedMessages(numMessages);
        bucket.setLastSegmentEntryId(lastSegmentEntryId);
        bucket.setFirstScheduleTimestamps(firstScheduleTimestamps);
        bucket.setDelayedIndexBitMap(immutableBucketBitMap);

        // Keep every segment resident until the create future completes: on failure the tracker
        // downgrades the bucket to memory mode by re-queuing all of them.
        bucket.setSnapshotSegments(bucketSnapshotSegments);

        CompletableFuture<Long> future = bucket.asyncSaveBucketSnapshot(
                bucketSnapshotMetadata, bucketSnapshotSegments);
        bucket.setSnapshotCreateFuture(future);

        return bucket;
    }

    void moveScheduledMessageToSharedQueue(long cutoffTime, TripleLongPriorityQueue sharedBucketPriorityQueue) {
        while (!priorityQueue.isEmpty()) {
            long timestamp = priorityQueue.peekN1();
            if (timestamp > cutoffTime) {
                break;
            }

            long ledgerId = priorityQueue.peekN2();
            long entryId = priorityQueue.peekN3();
            sharedBucketPriorityQueue.add(timestamp, ledgerId, entryId);

            priorityQueue.pop();
        }
    }

    void resetLastMutableBucketRange() {
        this.startLedgerId = -1L;
        this.endLedgerId = -1L;
    }

    void clear() {
        this.resetLastMutableBucketRange();
        this.priorityQueue.clear();
    }

    public void close() {
        priorityQueue.close();
    }

    long getBufferMemoryUsage() {
        return priorityQueue.bytesCapacity();
    }

    boolean isEmpty() {
        return priorityQueue.isEmpty();
    }

    long nextDeliveryTime() {
        return priorityQueue.peekN1();
    }

    long size() {
        return priorityQueue.size();
    }

    void addMessage(long ledgerId, long entryId, long deliverAt) {
        priorityQueue.add(deliverAt, ledgerId, entryId);
        if (startLedgerId == -1L) {
            this.startLedgerId = ledgerId;
        }
        this.endLedgerId = ledgerId;
    }
}
