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

import static org.apache.bookkeeper.mledger.util.Futures.executeWithRetry;
import static org.apache.pulsar.broker.delayed.bucket.BucketDelayedDeliveryTracker.DELAYED_BUCKET_KEY_PREFIX;
import static org.apache.pulsar.broker.delayed.bucket.BucketDelayedDeliveryTracker.NULL_LONG_PROMISE;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;
import lombok.CustomLog;
import lombok.Getter;
import lombok.Setter;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.mutable.MutableLong;
import org.apache.pulsar.broker.delayed.proto.DelayedIndex;
import org.apache.pulsar.broker.delayed.proto.SnapshotMetadata;
import org.apache.pulsar.broker.delayed.proto.SnapshotSegment;
import org.apache.pulsar.common.util.Codec;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.common.util.collections.LongBitmap;
import org.apache.pulsar.common.util.collections.LongBitmaps;

@CustomLog
class ImmutableBucket {

    static final String DELIMITER = "_";
    static final int MaxRetryTimes = 3;

    private final BucketContext ctx;

    @Getter
    private final long startLedgerId;

    @Getter
    private final long endLedgerId;

    @Getter
    @Setter
    private Map<Long, LongBitmap> delayedIndexBitMap = new Long2ObjectOpenHashMap<>();

    // Volatile: written by the snapshot-create completion callback, read by the tracker's
    // loadDueSegments to serve segments while the create is still in flight.
    @Setter
    private volatile List<SnapshotSegment> snapshotSegments;

    boolean merging = false;

    // Set as soon as a snapshot delete claims this bucket. A create that fails afterwards must
    // not re-queue its segments: the delete owns the cleanup, and re-queuing would resurrect
    // entries it is about to remove.
    private volatile boolean deleteStarted;

    @Setter
    @Getter
    List<Long> firstScheduleTimestamps = new ArrayList<>();

    @Getter
    @Setter
    private long numberBucketDelayedMessages;

    @Getter
    @Setter
    private int lastSegmentEntryId;

    @Getter
    @Setter
    private volatile int currentSegmentEntryId;

    @Getter
    @Setter
    private volatile long snapshotLength;

    @Setter
    private volatile Long bucketId;

    @Setter
    private volatile CompletableFuture<Long> snapshotCreateFuture;

    ImmutableBucket(BucketContext ctx, long startLedgerId, long endLedgerId) {
        this.ctx = ctx;
        this.startLedgerId = startLedgerId;
        this.endLedgerId = endLedgerId;
    }

    String bucketKey() {
        return String.join(DELIMITER, DELAYED_BUCKET_KEY_PREFIX, String.valueOf(startLedgerId),
                String.valueOf(endLedgerId));
    }

    boolean isDeleteStarted() {
        return deleteStarted;
    }

    Optional<CompletableFuture<Long>> getSnapshotCreateFuture() {
        return Optional.ofNullable(snapshotCreateFuture);
    }

    Optional<Long> getBucketId() {
        return Optional.ofNullable(bucketId);
    }

    long getAndUpdateBucketId() {
        Optional<Long> bucketIdOptional = getBucketId();
        if (bucketIdOptional.isPresent()) {
            return bucketIdOptional.get();
        }

        String bucketIdStr = ctx.cursor().getCursorProperties().get(bucketKey());
        long bucketId = Long.parseLong(bucketIdStr);
        setBucketId(bucketId);
        return bucketId;
    }

    CompletableFuture<Long> asyncSaveBucketSnapshot(
            SnapshotMetadata snapshotMetadata, List<SnapshotSegment> bucketSnapshotSegments) {
        final String bucketKey = bucketKey();
        final String cursorName = Codec.decode(ctx.cursor().getName());
        final String dispatcher = ctx.dispatcherName();
        final String topicName = dispatcher.substring(0, dispatcher.lastIndexOf(" / " + cursorName));
        return executeWithRetry(
                () -> ctx.bucketSnapshotStorage().createBucketSnapshot(snapshotMetadata, bucketSnapshotSegments,
                                bucketKey, topicName, cursorName)
                        .whenComplete((__, ex) -> {
                            if (ex != null) {
                                log.warn()
                                        .attr("dispatcher", dispatcher)
                                        .attr("bucketKey", bucketKey)
                                        .exception(ex)
                                        .log("Failed to create bucket snapshot");
                            }
                        }), BucketSnapshotPersistenceException.class, MaxRetryTimes).thenCompose(newBucketId -> {
                    setBucketId(newBucketId);

                    return putBucketKeyId(bucketKey, newBucketId).exceptionally(ex -> {
                        log.warn()
                                .attr("dispatcher", dispatcher)
                                .attr("bucketKey", bucketKey)
                                .attr("bucketId", newBucketId)
                                .exception(ex)
                                .log("Failed to record bucketId to cursor property");
                        return null;
                    }).thenApply(__ -> newBucketId);
                });
    }

    private CompletableFuture<Void> putBucketKeyId(String bucketKey, Long bucketId) {
        if (bucketId == null) {
            return FutureUtil.failedFuture(new NullPointerException("Expected bucketId should not be null"));
        }
        return ctx.sequencer().sequential(() ->
                executeWithRetry(() -> ctx.cursor().putCursorProperty(bucketKey, String.valueOf(bucketId)),
                        ManagedLedgerException.BadVersionException.class, MaxRetryTimes));
    }

    CompletableFuture<Void> removeBucketCursorProperty(String bucketKey) {
        return ctx.sequencer().sequential(() ->
                executeWithRetry(() -> ctx.cursor().removeCursorProperty(bucketKey),
                        ManagedLedgerException.BadVersionException.class, MaxRetryTimes));
    }

    public Optional<List<SnapshotSegment>> getSnapshotSegments() {
        return Optional.ofNullable(snapshotSegments);
    }

    CompletableFuture<List<DelayedIndex>> asyncLoadNextBucketSnapshotEntry() {
        final long bucketId = getAndUpdateBucketId();
        final int nextSegmentEntryId = currentSegmentEntryId + 1;
        if (nextSegmentEntryId > lastSegmentEntryId) {
            return CompletableFuture.completedFuture(null);
        }

        return executeWithRetry(
                () -> ctx.bucketSnapshotStorage().getBucketSnapshotSegment(bucketId, nextSegmentEntryId,
                        nextSegmentEntryId).whenComplete((___, ex) -> {
                    if (ex != null) {
                        log.warn()
                                .attr("dispatcher", ctx.dispatcherName())
                                .attr("bucketKey", bucketKey())
                                .attr("bucketId", bucketId)
                                .attr("segmentEntryId", nextSegmentEntryId)
                                .exception(ex)
                                .log("Failed to get bucket snapshot segment");
                    }
                }), BucketSnapshotPersistenceException.class, MaxRetryTimes)
                .thenCompose(bucketSnapshotSegments -> {
                    if (CollectionUtils.isEmpty(bucketSnapshotSegments)) {
                        return CompletableFuture.completedFuture(Collections.emptyList());
                    }

                    SnapshotSegment snapshotSegment =
                            bucketSnapshotSegments.get(0);
                    List<DelayedIndex> indexList = snapshotSegment.getIndexesList();
                    this.setCurrentSegmentEntryId(nextSegmentEntryId);
                    return CompletableFuture.completedFuture(indexList);
                });
    }

    /**
     * Recovers the bucket from its snapshot metadata only: segments are loaded lazily when they
     * become due, so recovery must not read any segment entry. Segments already past the cutoff
     * time are skipped exactly as before — their messages are not tracked and are delivered by
     * the normal read path.
     *
     * @return {@code true} if the bucket still holds segments that are not due yet (the bucket is
     *         kept and lazily armed), {@code false} if every segment is already due (the snapshot
     *         is obsolete and the caller should delete it).
     */
    CompletableFuture<Boolean> asyncRecoverBucketSnapshotEntry(Supplier<Long> cutoffTimeSupplier) {
        final long bucketId = getAndUpdateBucketId();
        final long cutoffTime = cutoffTimeSupplier.get();
        final String bucketKey = bucketKey();
        return executeWithRetry(() -> ctx.bucketSnapshotStorage().getBucketSnapshotMetadata(bucketId)
                        .whenComplete((___, ex) -> {
                            if (ex != null) {
                                log.warn()
                                        .attr("dispatcher", ctx.dispatcherName())
                                        .attr("bucketKey", bucketKey)
                                        .attr("bucketId", bucketId)
                                        .exception(ex)
                                        .log("Failed to get bucket snapshot metadata");
                            }
                        }), BucketSnapshotPersistenceException.class, MaxRetryTimes)
                .thenApply(snapshotMetadata -> {
                    int metadataListSize = snapshotMetadata.getMetadataListCount();

                    // Skip all already reach schedule time snapshot segments
                    int nextSnapshotEntryIndex = 0;
                    while (nextSnapshotEntryIndex < metadataListSize
                            && snapshotMetadata.getMetadataAt(nextSnapshotEntryIndex)
                                    .getMaxScheduleTimestamp() <= cutoffTime) {
                        nextSnapshotEntryIndex++;
                    }

                    this.setLastSegmentEntryId(metadataListSize);
                    this.recoverDelayedIndexBitMapAndNumber(nextSnapshotEntryIndex, snapshotMetadata);
                    List<Long> firstScheduleTimestamps = new ArrayList<>();
                    for (int i = 0; i < metadataListSize; i++) {
                        firstScheduleTimestamps.add(
                                snapshotMetadata.getMetadataAt(i).getMinScheduleTimestamp());
                    }
                    this.setFirstScheduleTimestamps(firstScheduleTimestamps);
                    // No segment is resident after recovery; the next lazy load fetches segment
                    // nextSnapshotEntryIndex + 1, the first one that is not due yet.
                    this.setCurrentSegmentEntryId(nextSnapshotEntryIndex);

                    return nextSnapshotEntryIndex < metadataListSize;
                })
                .thenCompose(hasFutureSegments -> hasFutureSegments
                        ? this.asyncUpdateSnapshotLength()
                                .thenAccept(this::setSnapshotLength)
                                .thenApply(__ -> true)
                        : CompletableFuture.completedFuture(false));
    }

    /**
     * Earliest delivery timestamp of the next unloaded segment, or -1 when every segment has
     * been loaded. {@code firstScheduleTimestamps} is zero-based while segment ids start at 1,
     * so index {@code currentSegmentEntryId} describes segment {@code currentSegmentEntryId + 1}.
     */
    long getNextUnloadedSegmentTimestamp() {
        if (currentSegmentEntryId >= lastSegmentEntryId) {
            return -1;
        }
        return firstScheduleTimestamps.get(currentSegmentEntryId);
    }

    /**
     * Segments considered loaded for merge eligibility. A lazy bucket (no segment resident yet,
     * {@code currentSegmentEntryId == 0}) counts as one loaded segment, so a single-segment
     * bucket stays unmergeable exactly as with eager loading. After recovery with {@code k}
     * skipped due segments the bucket is eligible one segment earlier than the eager baseline
     * (the first not-due segment is not pre-loaded); this is safe because merging reads every
     * segment from storage anyway.
     */
    int mergeEligibilityLoadedSegments() {
        return Math.max(currentSegmentEntryId, 1);
    }

    /**
     * Recover delayed index bit map and message numbers.
     */
    private void recoverDelayedIndexBitMapAndNumber(int startSnapshotIndex,
                                                    SnapshotMetadata snapshotMetadata) {
        delayedIndexBitMap.clear(); // cleanup dirty bm
        final var numberMessages = new MutableLong(0);
        for (int i = startSnapshotIndex; i < snapshotMetadata.getMetadataListCount(); i++) {
            snapshotMetadata.getMetadataAt(i).forEachDelayedIndexBitMap((ledgerId, bs) -> {
                final ByteBuf buf = Unpooled.wrappedBuffer(bs);
                try {
                    final LongBitmap sbm = LongBitmaps.deserialize(buf);
                    numberMessages.add(sbm.cardinality());
                    delayedIndexBitMap.compute(ledgerId, (lId, bm) -> {
                        if (bm == null) {
                            return sbm;
                        }
                        bm.or(sbm);
                        return bm;
                    });
                } finally {
                    buf.release();
                }
            });
        }
        setNumberBucketDelayedMessages(numberMessages.longValue());
    }

    CompletableFuture<List<SnapshotSegment>> getAllSnapshotSegments() {
        if (lastSegmentEntryId < 1) {
            return CompletableFuture.completedFuture(Collections.emptyList());
        }
        return executeWithRetry(() -> {
            return ctx.bucketSnapshotStorage().getBucketSnapshotSegment(getAndUpdateBucketId(), 1,
                    lastSegmentEntryId).whenComplete((__, ex) -> {
                if (ex != null) {
                    log.warn()
                            .attr("dispatcher", ctx.dispatcherName())
                            .attr("bucketKey", bucketKey())
                            .attr("lastSegmentEntryId", lastSegmentEntryId)
                            .exception(ex)
                            .log("Failed to get all bucket snapshot segments for merge");
                }
            });
        }, BucketSnapshotPersistenceException.class, MaxRetryTimes);
    }

    CompletableFuture<Void> asyncDeleteBucketSnapshot(BucketDelayedMessageIndexStats stats) {
        deleteStarted = true;
        long deleteStartTime = System.currentTimeMillis();
        stats.recordTriggerEvent(BucketDelayedMessageIndexStats.Type.delete);
        String bucketKey = bucketKey();
        long bucketId = getAndUpdateBucketId();

        return executeWithRetry(() -> ctx.bucketSnapshotStorage().deleteBucketSnapshot(bucketId),
                BucketSnapshotPersistenceException.class, MaxRetryTimes)
                .whenComplete((__, ex) -> {
                    if (ex != null) {
                        log.error()
                                .attr("dispatcher", ctx.dispatcherName())
                                .attr("bucketId", bucketId)
                                .attr("bucketKey", bucketKey)
                                .exception(ex)
                                .log("Failed to delete bucket snapshot");

                        stats.recordFailEvent(BucketDelayedMessageIndexStats.Type.delete);
                    } else {
                        log.info()
                                .attr("dispatcher", ctx.dispatcherName())
                                .attr("bucketId", bucketId)
                                .attr("bucketKey", bucketKey)
                                .log("Delete bucket snapshot finish");

                        stats.recordSuccessEvent(BucketDelayedMessageIndexStats.Type.delete,
                                System.currentTimeMillis() - deleteStartTime);
                    }
                })
                .thenCompose(__ -> removeBucketCursorProperty(bucketKey));
    }

    CompletableFuture<Void> clear(BucketDelayedMessageIndexStats stats) {
        delayedIndexBitMap.clear();
        return getSnapshotCreateFuture().orElse(NULL_LONG_PROMISE).exceptionally(e -> null)
                .thenCompose(__ -> asyncDeleteBucketSnapshot(stats));
    }

    protected CompletableFuture<Long> asyncUpdateSnapshotLength() {
        long bucketId = getAndUpdateBucketId();
        return ctx.bucketSnapshotStorage().getBucketSnapshotLength(bucketId).whenComplete((length, ex) -> {
            if (ex != null) {
                log.error()
                        .attr("dispatcher", ctx.dispatcherName())
                        .attr("bucketId", bucketId)
                        .attr("bucketKey", bucketKey())
                        .exception(ex)
                        .log("Failed to get snapshot length");
            }
        });
    }
}
