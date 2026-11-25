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
import static org.apache.bookkeeper.mledger.ManagedCursor.CURSOR_INTERNAL_PROPERTY_PREFIX;
import static org.apache.pulsar.broker.delayed.bucket.ImmutableBucket.DELIMITER;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Range;
import com.google.common.collect.RangeMap;
import com.google.common.collect.TreeRangeMap;
import io.github.merlimat.slog.Logger;
import io.netty.util.Timeout;
import io.netty.util.Timer;
import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import javax.annotation.concurrent.ThreadSafe;
import lombok.Getter;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.PositionFactory;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.broker.delayed.AbstractDelayedDeliveryTracker;
import org.apache.pulsar.broker.delayed.DelayedDeliveryContext;
import org.apache.pulsar.broker.delayed.DispatcherDelayedDeliveryContext;
import org.apache.pulsar.broker.delayed.proto.DelayedIndex;
import org.apache.pulsar.broker.delayed.proto.SnapshotSegment;
import org.apache.pulsar.broker.service.persistent.AbstractPersistentDispatcherMultipleConsumers;
import org.apache.pulsar.common.policies.data.stats.TopicMetricBean;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.common.util.collections.LongBitmap;
import org.apache.pulsar.common.util.collections.TripleLongPriorityQueue;

@ThreadSafe
public class BucketDelayedDeliveryTracker extends AbstractDelayedDeliveryTracker {

    private static final Logger LOG = Logger.get(BucketDelayedDeliveryTracker.class);
    protected final Logger log;

    public static final String DELAYED_BUCKET_KEY_PREFIX = CURSOR_INTERNAL_PROPERTY_PREFIX + "delayed.bucket";

    static final CompletableFuture<Long> NULL_LONG_PROMISE = CompletableFuture.completedFuture(null);

    static final int AsyncOperationTimeoutSeconds = 60;

    private static final Long INVALID_BUCKET_ID = -1L;

    private static final int MAX_MERGE_NUM = 4;

    private final long minIndexCountPerBucket;

    private final long timeStepPerBucketSnapshotSegmentInMillis;

    private final int maxIndexesPerBucketSnapshotSegment;

    private final int maxNumBuckets;

    @Getter
    @VisibleForTesting
    private final BucketContext ctx;

    @Getter
    @VisibleForTesting
    private final MutableBucket lastMutableBucket;

    @Getter
    @VisibleForTesting
    private final TripleLongPriorityQueue sharedBucketPriorityQueue;

    @Getter
    @VisibleForTesting
    private final RangeMap<Long, ImmutableBucket> immutableBuckets;

    @Getter
    @VisibleForTesting
    private final BucketDelayedMessageIndex index = new BucketDelayedMessageIndex();

    @Getter
    @VisibleForTesting
    private final AtomicLong bucketsCount = new AtomicLong(0);

    @Getter
    @VisibleForTesting
    private final AtomicLong totalSnapshotLengthBytes = new AtomicLong(0);

    private final BucketDelayedMessageIndexStats stats;

    // Segment loads armed by loadDueSegments that have not settled yet. Delivery waits for the
    // whole batch (pruned as futures complete), so a round only returns positions that were all
    // resident before it started popping. Mutated under the tracker lock.
    private final Set<CompletableFuture<Void>> pendingLoads = new HashSet<>();

    // Drained buckets whose snapshot delete is in flight; guards against stacking concurrent
    // deletes for the same bucket across dispatch rounds. Entries remove themselves on completion.
    private final Set<ImmutableBucket> pendingDrainedDeletes = ConcurrentHashMap.newKeySet();

    private final Set<CompletableFuture<Void>> pendingDeletes = ConcurrentHashMap.newKeySet();

    // Buckets no longer in immutableBuckets whose snapshot is still stored: a failed delete, or the source
    // buckets of a failed merge. clear() deletes them.
    private final Set<ImmutableBucket> uncleanedBuckets = new HashSet<>();

    private volatile CompletableFuture<Void> trimFuture;

    public BucketDelayedDeliveryTracker(AbstractPersistentDispatcherMultipleConsumers dispatcher,
                                        Timer timer, long tickTimeMillis,
                                        boolean isDelayedDeliveryDeliverAtTimeStrict,
                                        BucketSnapshotStorage bucketSnapshotStorage,
                                        long minIndexCountPerBucket, long timeStepPerBucketSnapshotSegmentInMillis,
                                        int maxIndexesPerBucketSnapshotSegment, int maxNumBuckets)
            throws RecoverDelayedDeliveryTrackerException {
        this(new DispatcherDelayedDeliveryContext(dispatcher), timer, tickTimeMillis, Clock.systemUTC(),
                isDelayedDeliveryDeliverAtTimeStrict, bucketSnapshotStorage, minIndexCountPerBucket,
                timeStepPerBucketSnapshotSegmentInMillis, maxIndexesPerBucketSnapshotSegment, maxNumBuckets);
    }

    public BucketDelayedDeliveryTracker(AbstractPersistentDispatcherMultipleConsumers dispatcher,
                                        Timer timer, long tickTimeMillis, Clock clock,
                                        boolean isDelayedDeliveryDeliverAtTimeStrict,
                                        BucketSnapshotStorage bucketSnapshotStorage,
                                        long minIndexCountPerBucket, long timeStepPerBucketSnapshotSegmentInMillis,
                                        int maxIndexesPerBucketSnapshotSegment, int maxNumBuckets)
            throws RecoverDelayedDeliveryTrackerException {
        this(new DispatcherDelayedDeliveryContext(dispatcher), timer, tickTimeMillis, clock,
                isDelayedDeliveryDeliverAtTimeStrict, bucketSnapshotStorage, minIndexCountPerBucket,
                timeStepPerBucketSnapshotSegmentInMillis, maxIndexesPerBucketSnapshotSegment, maxNumBuckets);
    }

    @VisibleForTesting
    public BucketDelayedDeliveryTracker(DelayedDeliveryContext context,
                                        Timer timer, long tickTimeMillis, Clock clock,
                                        boolean isDelayedDeliveryDeliverAtTimeStrict,
                                        BucketSnapshotStorage bucketSnapshotStorage,
                                        long minIndexCountPerBucket, long timeStepPerBucketSnapshotSegmentInMillis,
                                        int maxIndexesPerBucketSnapshotSegment, int maxNumBuckets)
            throws RecoverDelayedDeliveryTrackerException {
        this(context, timer, tickTimeMillis, clock, isDelayedDeliveryDeliverAtTimeStrict, bucketSnapshotStorage,
                minIndexCountPerBucket, timeStepPerBucketSnapshotSegmentInMillis, maxIndexesPerBucketSnapshotSegment,
                maxNumBuckets, true);
    }

    public BucketDelayedDeliveryTracker(DelayedDeliveryContext context,
                                        Timer timer, long tickTimeMillis, Clock clock,
                                        boolean isDelayedDeliveryDeliverAtTimeStrict,
                                        BucketSnapshotStorage bucketSnapshotStorage,
                                        long minIndexCountPerBucket, long timeStepPerBucketSnapshotSegmentInMillis,
                                        int maxIndexesPerBucketSnapshotSegment, int maxNumBuckets,
                                        boolean recoverBucketSnapshot)
            throws RecoverDelayedDeliveryTrackerException {
        super(context, timer, tickTimeMillis, clock, isDelayedDeliveryDeliverAtTimeStrict);
        this.log = LOG.with().ctx(super.log).build();
        this.minIndexCountPerBucket = minIndexCountPerBucket;
        this.timeStepPerBucketSnapshotSegmentInMillis = timeStepPerBucketSnapshotSegmentInMillis;
        this.maxIndexesPerBucketSnapshotSegment = maxIndexesPerBucketSnapshotSegment;
        this.maxNumBuckets = maxNumBuckets;
        this.sharedBucketPriorityQueue = new TripleLongPriorityQueue();
        this.immutableBuckets = TreeRangeMap.create();
        this.ctx = new BucketContext(context.getName(), context.getCursor(), FutureUtil.Sequencer.create(),
                bucketSnapshotStorage);
        this.lastMutableBucket = new MutableBucket(ctx);
        this.stats = new BucketDelayedMessageIndexStats();

        if (recoverBucketSnapshot) {
            recoverBucketSnapshot();
        }
    }

    /**
     * Recover the bucket snapshots without blocking the caller. On failure the tracker is closed.
     */
    public CompletableFuture<BucketDelayedDeliveryTracker> recoverBucketSnapshotAsync(
            ScheduledExecutorService timeoutExecutor) {
        return recoverBucketSnapshotAsync(timeoutExecutor, new CompletableFuture<>());
    }

    public CompletableFuture<BucketDelayedDeliveryTracker> recoverBucketSnapshotAsync(
            ScheduledExecutorService timeoutExecutor, CompletableFuture<Void> skipRecovery) {
        try {
            CompletableFuture<Long> recoverFuture = recoverBucketSnapshot0(skipRecovery);
            FutureUtil.addTimeoutHandling(recoverFuture, Duration.ofSeconds(AsyncOperationTimeoutSeconds * 5L),
                    timeoutExecutor, () -> FutureUtil.createTimeoutException(
                            "Timeout", getClass(), "recoverBucketSnapshotAsync(...)"));
            return recoverFuture.thenApply(__ -> this).whenComplete((__, ex) -> {
                if (ex != null) {
                    log.warn()
                            .attr("timeoutSeconds", AsyncOperationTimeoutSeconds * 5)
                            .exception(FutureUtil.unwrapCompletionException(ex))
                            .log("Failed to recover delayed message index bucket snapshot asynchronously, "
                                    + "closing tracker");
                    closeAsync();
                }
            });
        } catch (Throwable t) {
            log.warn()
                    .exception(t)
                    .log("Failed to start delayed message index bucket snapshot recovery asynchronously, "
                            + "closing tracker");
            closeAsync();
            return FutureUtil.failedFuture(t);
        }
    }

    public void recoverBucketSnapshot() throws RecoverDelayedDeliveryTrackerException {
        try {
            recoverBucketSnapshot0().get(AsyncOperationTimeoutSeconds * 5, TimeUnit.SECONDS);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            log.error()
                    .exception(e)
                    .log("Failed to recover delayed message index bucket snapshot.");
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            close();
            throw new RecoverDelayedDeliveryTrackerException(e);
        }
    }

    private CompletableFuture<Long> recoverBucketSnapshot0() {
        return recoverBucketSnapshot0(new CompletableFuture<>());
    }

    private synchronized CompletableFuture<Long> recoverBucketSnapshot0(CompletableFuture<Void> skipRecovery) {
        ManagedCursor cursor = ctx.cursor();
        Map<String, String> cursorProperties = cursor.getCursorProperties();
        if (MapUtils.isEmpty(cursorProperties)) {
            log.info("Recover delayed message index bucket snapshot finish, don't find bucket snapshot");
            return CompletableFuture.completedFuture(0L);
        }
        Map<Range<Long>, ImmutableBucket> toBeDeletedBucketMap = new HashMap<>();
        List<ImmutableBucket> recoveryBuckets = new ArrayList<>();
        cursorProperties.keySet().forEach(key -> {
            if (key.startsWith(DELAYED_BUCKET_KEY_PREFIX)) {
                String[] keys = key.split(DELIMITER);
                checkArgument(keys.length == 3);
                ImmutableBucket immutableBucket =
                        new ImmutableBucket(ctx, Long.parseLong(keys[1]), Long.parseLong(keys[2]));
                recoveryBuckets.add(immutableBucket);
                putAndCleanOverlapRange(Range.closed(immutableBucket.getStartLedgerId(),
                        immutableBucket.getEndLedgerId()), immutableBucket, toBeDeletedBucketMap);
            }
        });

        Map<Range<Long>, ImmutableBucket> immutableBucketMap = immutableBuckets.asMapOfRanges();
        if (immutableBucketMap.isEmpty()) {
            log.info("Recover delayed message index bucket snapshot finish, don't find bucket snapshot");
            return CompletableFuture.completedFuture(0L);
        }

        if (skipRecovery.isDone()) {
            return CompletableFuture.completedFuture(prepareRecoveryBucketsForClear(recoveryBuckets));
        }

        Map<Range<Long>, CompletableFuture<Boolean>>
                futures = new HashMap<>(immutableBucketMap.size());
        for (Map.Entry<Range<Long>, ImmutableBucket> entry : immutableBucketMap.entrySet()) {
            Range<Long> key = entry.getKey();
            futures.put(key, handleRecoverBucketSnapshotEntry(entry.getValue()));
        }

        // Only one continuation can publish recovered state. Completing the skip signal also prevents
        // reads that finish later from restoring indexes or deleting a replacement snapshot's property.
        return FutureUtil.waitForAll(futures.values()).applyToEither(skipRecovery, result -> result).thenApply(__ -> {
            synchronized (this) {
                if (skipRecovery.isDone()) {
                    return prepareRecoveryBucketsForClear(recoveryBuckets);
                }
                for (Map.Entry<Range<Long>, CompletableFuture<Boolean>> entry : futures.entrySet()) {
                    // the future is completed since waitForAll completed normally
                    boolean hasFutureSegments = entry.getValue().getNow(false);
                    if (!hasFutureSegments) {
                        // Every segment is already due: the messages are delivered by the normal read
                        // path, so the snapshot is obsolete. This also covers the not-exist case.
                        toBeDeletedBucketMap.put(entry.getKey(), immutableBucketMap.get(entry.getKey()));
                    }
                }

                for (Map.Entry<Range<Long>, ImmutableBucket> mapEntry : toBeDeletedBucketMap.entrySet()) {
                    Range<Long> key = mapEntry.getKey();
                    ImmutableBucket immutableBucket = mapEntry.getValue();
                    removeBucket(key);
                    trackDelete(immutableBucket, immutableBucket.asyncDeleteBucketSnapshot(stats));
                }

                long totalLength = 0;
                for (ImmutableBucket bucket : immutableBucketMap.values()) {
                    index.restore(bucket.getDelayedIndexBitMap());
                    totalLength += bucket.getSnapshotLength();
                }
                totalSnapshotLengthBytes.set(totalLength);
                bucketsCount.set(immutableBuckets.asMapOfRanges().size());
                // Recovery sets each bucket's currentSegmentEntryId on the storage executor after
                // the bucket was put, so the cached minimum cannot be trusted from the put alone.
                invalidateMinUnloadedSegmentTimestamp();

                log.info()
                        .attr("buckets", immutableBucketMap.size())
                        .attr("numberDelayedMessages", index.size())
                        .log("Recover delayed message index bucket snapshot finish");

                return index.size();
            }
        });
    }

    private long prepareRecoveryBucketsForClear(List<ImmutableBucket> recoveryBuckets) {
        // Pending reads may still mutate their private buckets. Detach those buckets without clearing
        // their bitmaps, and retain every snapshot (including overlaps) for clear() to delete and retry.
        uncleanedBuckets.addAll(recoveryBuckets);
        immutableBuckets.clear();
        bucketsCount.set(0);
        totalSnapshotLengthBytes.set(0);
        return 0L;
    }

    /**
     * Handle the BucketNotExistException when recover bucket snapshot entry.
     * A bucket that does not exist completes with {@code false}, so it is added to
     * `toBeDeletedBucketMap` and deleted from `immutableBuckets` in the next step.
     */
    private CompletableFuture<Boolean> handleRecoverBucketSnapshotEntry(ImmutableBucket bucket) {
        CompletableFuture<Boolean> f = new CompletableFuture<>();
        bucket.asyncRecoverBucketSnapshotEntry(this::getCutoffTime)
                .whenComplete((v, e) -> {
                    if (e == null) {
                        f.complete(v);
                    } else {
                        // Dependent stages wrap checked exceptions in CompletionException, so unwrap
                        // before matching the not-exist case.
                        if (FutureUtil.unwrapCompletionException(e) instanceof BucketNotExistException) {
                            // If the bucket does not exist, complete with false: the bucket will be
                            // deleted from `immutableBuckets` in the next step.
                            f.complete(false);
                        } else {
                            f.completeExceptionally(e);
                        }
                    }
                });
        return f;
    }

    private synchronized void putAndCleanOverlapRange(Range<Long> range, ImmutableBucket immutableBucket,
                                                      Map<Range<Long>, ImmutableBucket> toBeDeletedBucketMap) {
        Map<Range<Long>, ImmutableBucket> subRangeMap = immutableBuckets.subRangeMap(range).asMapOfRanges();
        boolean canPut = false;
        if (!subRangeMap.isEmpty()) {
            for (Map.Entry<Range<Long>, ImmutableBucket> rangeEntry : subRangeMap.entrySet()) {
                // Use original key instead of truncated key for encloses check
                ImmutableBucket bucket = rangeEntry.getValue();
                Range<Long> originalKey = Range.closed(bucket.getStartLedgerId(), bucket.getEndLedgerId());

                if (range.encloses(originalKey)) {
                    toBeDeletedBucketMap.put(originalKey, bucket);
                    canPut = true;
                }
            }
        } else {
            canPut = true;
        }

        if (canPut) {
            putBucket(range, immutableBucket);
        }
    }

    @Override
    public void run(Timeout timeout) throws Exception {
        synchronized (this) {
            if (timeout == null || timeout.isCancelled()) {
                return;
            }
            lastMutableBucket.moveScheduledMessageToSharedQueue(getCutoffTime(), sharedBucketPriorityQueue);
        }
        super.run(timeout);
    }

    private ImmutableBucket findImmutableBucket(long ledgerId) {
        return immutableBuckets.get(ledgerId);
    }

    private void afterCreateImmutableBucket(ImmutableBucket immutableBucket, long startTime) {
        if (immutableBucket != null) {
            putBucket(Range.closed(immutableBucket.getStartLedgerId(), immutableBucket.getEndLedgerId()),
                    immutableBucket);

            immutableBucket.getSnapshotCreateFuture().ifPresent(createFuture -> {
                CompletableFuture<Long> future = createFuture.handle((bucketId, ex) -> {
                    if (ex == null) {
                        synchronized (BucketDelayedDeliveryTracker.this) {
                            // Publish the drop of the resident segment list under the tracker lock:
                            // loadDueSegments reads it to serve segments before the create completes.
                            immutableBucket.setSnapshotSegments(null);
                        }
                        immutableBucket.asyncUpdateSnapshotLength()
                                .thenAccept(newLength -> {
                                    synchronized (BucketDelayedDeliveryTracker.this) {
                                        updateBucketSnapshotLength(immutableBucket, newLength);
                                    }
                                });
                        log.info()
                                .attr("bucketKey", immutableBucket.bucketKey())
                                .log("Create bucket snapshot finish, bucketKey");

                        stats.recordSuccessEvent(BucketDelayedMessageIndexStats.Type.create,
                                System.currentTimeMillis() - startTime);

                        return bucketId;
                    }

                    log.error()
                            .attr("bucketKey", immutableBucket.bucketKey())
                            .exception(ex)
                            .log("Failed to create bucket snapshot");
                    stats.recordFailEvent(BucketDelayedMessageIndexStats.Type.create);

                    // Put indexes back into the shared queue and downgrade to memory mode
                    synchronized (BucketDelayedDeliveryTracker.this) {
                        immutableBucket.getSnapshotSegments().ifPresent(snapshotSegments -> {
                            for (SnapshotSegment snapshotSegment : snapshotSegments) {
                                for (DelayedIndex delayedIndex : snapshotSegment.getIndexesList()) {
                                    sharedBucketPriorityQueue.add(delayedIndex.getTimestamp(),
                                            delayedIndex.getLedgerId(), delayedIndex.getEntryId());
                                }
                            }
                            immutableBucket.setSnapshotSegments(null);
                        });

                        immutableBucket.setCurrentSegmentEntryId(immutableBucket.getLastSegmentEntryId());
                        removeBucket(
                                Range.closed(immutableBucket.getStartLedgerId(), immutableBucket.getEndLedgerId()));
                    }
                    return INVALID_BUCKET_ID;
                });
                immutableBucket.setSnapshotCreateFuture(future);
            });
        }
    }

    @Override
    public synchronized boolean addMessage(long ledgerId, long entryId, long deliverAt) {
        if (deliverAt < 0 || deliverAt <= getCutoffTime()) {
            removeIndexBit(ledgerId, entryId);
            return false;
        }

        if (containsMessage(ledgerId, entryId)) {
            return true;
        }

        boolean existBucket = findImmutableBucket(ledgerId) != null;

        // Create bucket snapshot
        if (!existBucket && ledgerId > lastMutableBucket.endLedgerId
                && lastMutableBucket.size() >= minIndexCountPerBucket
                && !lastMutableBucket.isEmpty()) {
            long createStartTime = System.currentTimeMillis();
            stats.recordTriggerEvent(BucketDelayedMessageIndexStats.Type.create);
            ImmutableBucket immutableBucket = lastMutableBucket.sealBucketAndAsyncPersistent(
                    this.timeStepPerBucketSnapshotSegmentInMillis,
                    this.maxIndexesPerBucketSnapshotSegment);
            afterCreateImmutableBucket(immutableBucket, createStartTime);
            lastMutableBucket.resetLastMutableBucketRange();

            if (maxNumBuckets > 0 && bucketsCount.get() > maxNumBuckets
                    && (trimFuture == null || trimFuture.isDone())) {
                trimFuture = asyncTrimImmutableBuckets()
                        .thenCompose(ignore -> asyncMergeBucketSnapshot())
                        .whenComplete((ignore, t) -> {
                            if (t != null) {
                                log.warn().exception(t).log("Failed to trim or merge bucket snapshots");
                            }
                        });
            }
        }

        if (ledgerId >= lastMutableBucket.endLedgerId && !existBucket) {
            lastMutableBucket.addMessage(ledgerId, entryId, deliverAt);
        } else {
            // Message index belongs to previous bucket range or the current mutable bucket range,
            // enter sharedBucketPriorityQueue directly
            sharedBucketPriorityQueue.add(deliverAt, ledgerId, entryId);
        }
        index.track(ledgerId, entryId);
            log.debug()
                    .attr("ledgerId", ledgerId)
                    .attr("entryId", entryId)
                    .attr("deliveryInMs", deliverAt - clock.millis())
                    .log("Add message");
                updateTimer();

        return true;
    }

    @VisibleForTesting
    synchronized List<ImmutableBucket> selectMergedBuckets(final List<ImmutableBucket> values, int mergeNum) {
        if (values.size() < 2 || mergeNum < 2) {
            return Collections.emptyList();
        }
        int actualMergeNum = Math.min(mergeNum, values.size());
        long minNumberMessages = Long.MAX_VALUE;
        long minScheduleTimestamp = Long.MAX_VALUE;
        int minIndex = -1;
        for (int i = 0; i + (actualMergeNum - 1) < values.size(); i++) {
            List<ImmutableBucket> immutableBuckets = values.subList(i, i + actualMergeNum);
            if (immutableBuckets.stream().allMatch(bucket -> {
                // We should skip the bucket which last segment already been load to memory,
                // avoid record replicated index. A lazy bucket (no segment loaded yet,
                // currentSegmentEntryId == 0) counts as if its first segment were loaded, so a
                // single-segment bucket stays unmergeable exactly as with eager loading.
                int loadedSegments = Math.max(bucket.getCurrentSegmentEntryId(), 1);
                return bucket.getLastSegmentEntryId() > loadedSegments && !bucket.merging;
            })) {
                long numberMessages = immutableBuckets.stream()
                        .mapToLong(bucket -> bucket.getNumberBucketDelayedMessages())
                        .sum();
                if (numberMessages <= minNumberMessages) {
                    minNumberMessages = numberMessages;
                    // Snapshot segment IDs start at 1 while the timestamp list is zero-based.
                    // The next unloaded segment is currentSegmentEntryId + 1, at list index
                    // currentSegmentEntryId.
                    long scheduleTimestamp = immutableBuckets.stream()
                            .mapToLong(bucket -> bucket.getFirstScheduleTimestamps()
                                       .get(bucket.getCurrentSegmentEntryId()))
                            .min().getAsLong();
                    if (scheduleTimestamp < minScheduleTimestamp) {
                        minScheduleTimestamp = scheduleTimestamp;
                        minIndex = i;
                    }
                }
            }
        }

        if (minIndex >= 0) {
            return values.subList(minIndex, minIndex + actualMergeNum);
        } else if (actualMergeNum > 2) {
            return selectMergedBuckets(values, actualMergeNum - 1);
        } else {
            return Collections.emptyList();
        }
    }

    private synchronized CompletableFuture<Void> asyncMergeBucketSnapshot() {
        if (maxNumBuckets <= 0 || bucketsCount.get() <= maxNumBuckets) {
            return CompletableFuture.completedFuture(null);
        }
        List<ImmutableBucket> immutableBucketList = immutableBuckets.asMapOfRanges().values().stream().toList();
        List<ImmutableBucket> toBeMergeImmutableBuckets = selectMergedBuckets(immutableBucketList, MAX_MERGE_NUM);

        if (toBeMergeImmutableBuckets.isEmpty()) {
            log.warn("Can't find able merged buckets");
            return CompletableFuture.completedFuture(null);
        }

        final String bucketsStr = toBeMergeImmutableBuckets.stream().map(ImmutableBucket::bucketKey).collect(
                Collectors.joining(",")).replaceAll(DELAYED_BUCKET_KEY_PREFIX + "_", "");
            log.info()
                    .attr("bucketKeys", bucketsStr)
                    .log("Merging bucket snapshot, bucketKeys");
                for (ImmutableBucket immutableBucket : toBeMergeImmutableBuckets) {
            immutableBucket.merging = true;
        }

        long mergeStartTime = System.currentTimeMillis();
        stats.recordTriggerEvent(BucketDelayedMessageIndexStats.Type.merge);
        return asyncMergeBucketSnapshot(toBeMergeImmutableBuckets).whenComplete((__, ex) -> {
            synchronized (this) {
                for (ImmutableBucket immutableBucket : toBeMergeImmutableBuckets) {
                    immutableBucket.merging = false;
                }
            }
            if (ex != null) {
                log.error()
                        .attr("bucketKeys", bucketsStr)
                        .exception(ex)
                        .log("Failed to merge bucket snapshot, bucketKeys");

                stats.recordFailEvent(BucketDelayedMessageIndexStats.Type.merge);
            } else {
                log.info()
                        .attr("bucketKeys", bucketsStr)
                        .attr("bucketNum", bucketsCount.get())
                        .log("Merge bucket snapshot finish");

                stats.recordSuccessEvent(BucketDelayedMessageIndexStats.Type.merge,
                        System.currentTimeMillis() - mergeStartTime);
            }
        });
    }

    private synchronized CompletableFuture<Void> asyncMergeBucketSnapshot(List<ImmutableBucket> buckets) {
        List<CompletableFuture<Long>> createFutures =
                buckets.stream().map(bucket -> bucket.getSnapshotCreateFuture().orElse(NULL_LONG_PROMISE))
                        .toList();

        return FutureUtil.waitForAll(createFutures).thenCompose(bucketId -> {
            if (createFutures.stream().anyMatch(future -> INVALID_BUCKET_ID.equals(future.join()))) {
                return FutureUtil.failedFuture(new RuntimeException("Can't merge buckets due to bucket create failed"));
            }

            List<CompletableFuture<List<SnapshotSegment>>> getAllSnapshotFutures =
                    buckets.stream().map(ImmutableBucket::getAllSnapshotSegments).toList();

            return FutureUtil.waitForAll(getAllSnapshotFutures)
                    .thenApply(__ -> {
                        return CombinedSegmentDelayedIndexQueue.wrap(
                                getAllSnapshotFutures.stream().map(CompletableFuture::join).toList());
                    })
                    .thenCompose(combinedDelayedIndexQueue -> {
                        synchronized (BucketDelayedDeliveryTracker.this) {
                            long createStartTime = System.currentTimeMillis();
                            stats.recordTriggerEvent(BucketDelayedMessageIndexStats.Type.create);
                            ImmutableBucket mergedBucket = lastMutableBucket.createImmutableBucketAndAsyncPersistent(
                                    timeStepPerBucketSnapshotSegmentInMillis,
                                    maxIndexesPerBucketSnapshotSegment, combinedDelayedIndexQueue,
                                    buckets.get(0).getStartLedgerId(),
                                    buckets.get(buckets.size() - 1).getEndLedgerId());

                            // Merge bit map to new bucket
                            Long2ObjectMap<LongBitmap> delayedIndexBitMap =
                                    new Long2ObjectOpenHashMap<>(buckets.get(0).getDelayedIndexBitMap());
                            for (int i = 1; i < buckets.size(); i++) {
                                buckets.get(i).getDelayedIndexBitMap().forEach((ledgerId, bitMapB) -> {
                                    delayedIndexBitMap.compute(ledgerId, (k, bitMap) -> {
                                        if (bitMap == null) {
                                            return bitMapB;
                                        }

                                        bitMap.or(bitMapB);
                                        return bitMap;
                                    });
                                });
                            }

                            mergedBucket.setDelayedIndexBitMap(delayedIndexBitMap);

                            afterCreateImmutableBucket(mergedBucket, createStartTime);

                            for (ImmutableBucket bucket : buckets) {
                                removeBucket(Range.closed(bucket.getStartLedgerId(), bucket.getEndLedgerId()));
                            }

                            // A failed merged-bucket creation completes with INVALID_BUCKET_ID: keep
                            // the source snapshots so their delayed indexes stay recoverable, and let
                            // clear() delete them.
                            return mergedBucket.getSnapshotCreateFuture()
                                    .orElse(NULL_LONG_PROMISE)
                                    .thenCompose(mergedBucketId -> INVALID_BUCKET_ID.equals(mergedBucketId)
                                            ? keepUncleaned(buckets)
                                            : FutureUtil.waitForAll(
                                            buckets.stream()
                                                    .map(bucket -> trackDelete(bucket,
                                                            bucket.asyncDeleteBucketSnapshot(stats)))
                                                    .toList()));
                        }
                    });
        });
    }

    @Override
    public synchronized boolean hasMessageAvailable() {
        long cutoffTime = getCutoffTime();

        boolean hasMessageAvailable = getNumberOfDelayedMessages() > 0 && nextDeliveryTime() <= cutoffTime;
        if (!hasMessageAvailable) {
            updateTimer();
        }
        return hasMessageAvailable;
    }

    @Override
    protected synchronized long nextDeliveryTime() {
        long timestamp = Long.MAX_VALUE;
        if (!lastMutableBucket.isEmpty()) {
            timestamp = lastMutableBucket.nextDeliveryTime();
        }
        if (!sharedBucketPriorityQueue.isEmpty()) {
            timestamp = Math.min(timestamp, sharedBucketPriorityQueue.peekN1());
        }
        // Segments that are not loaded yet keep only their earliest delivery timestamp in memory;
        // it must take part in the timer target, otherwise the load would never be triggered.
        // The minimum is cached: addMessage calls this via updateTimer on the hot write path, and
        // scanning every bucket does not scale to thousands of sealed buckets. All invalidation
        // points (bucket added/removed, segment loaded) run under this same lock.
        long minUnloaded = minUnloadedSegmentTimestamp();
        if (minUnloaded >= 0) {
            timestamp = Math.min(timestamp, minUnloaded);
        }
        // numberDelayedMessages can be > 0 while no source holds a timestamp (e.g. remaining
        // messages live in segments of a bucket whose creation is still in flight). Returning
        // Long.MAX_VALUE signals "no imminent delivery" without throwing on the empty queues.
        return timestamp;
    }

    /** Sentinel for {@link #cachedMinUnloadedSegmentTimestamp}: must be recomputed. */
    private static final long MIN_UNLOADED_DIRTY = -2;

    private long cachedMinUnloadedSegmentTimestamp = MIN_UNLOADED_DIRTY;

    private long minUnloadedSegmentTimestamp() {
        if (cachedMinUnloadedSegmentTimestamp == MIN_UNLOADED_DIRTY) {
            long min = -1;
            for (ImmutableBucket bucket : immutableBuckets.asMapOfRanges().values()) {
                long nextUnloadedSegmentTimestamp = bucket.getNextUnloadedSegmentTimestamp();
                if (nextUnloadedSegmentTimestamp >= 0
                        && (min < 0 || nextUnloadedSegmentTimestamp < min)) {
                    min = nextUnloadedSegmentTimestamp;
                }
            }
            cachedMinUnloadedSegmentTimestamp = min;
        }
        return cachedMinUnloadedSegmentTimestamp;
    }

    private void invalidateMinUnloadedSegmentTimestamp() {
        cachedMinUnloadedSegmentTimestamp = MIN_UNLOADED_DIRTY;
    }

    @Override
    public long getNumberOfDelayedMessages() {
        return index.size();
    }

    @Override
    public long getBufferMemoryUsage() {
        return this.lastMutableBucket.getBufferMemoryUsage() + sharedBucketPriorityQueue.bytesCapacity();
    }

    @Override
    public synchronized NavigableSet<Position> getScheduledMessages(int maxMessages) {
        if (!checkPendingLoadDone()) {
            log.debug("Skip getScheduledMessages to wait for bucket snapshot load finish");
            return new TreeSet<>();
        }

        long cutoffTime = getCutoffTime();
        Long firstLiveLedgerId = firstActiveLedgerId();

        lastMutableBucket.moveScheduledMessageToSharedQueue(cutoffTime, sharedBucketPriorityQueue);

        // Only loaded segments are deliverable, so arm the loads for every segment that is due
        // now. If any load is (still) in flight, deliver nothing this round: once the batch
        // settles, the re-triggered round sees every source due at this cutoff in the shared
        // queue and returns them position-ordered.
        loadDueSegments(cutoffTime);
        if (!checkPendingLoadDone()) {
            updateTimer();
            return new TreeSet<>();
        }

        NavigableSet<Position> positions = new TreeSet<>();
        int n = maxMessages;

        while (n > 0 && !sharedBucketPriorityQueue.isEmpty()) {
            long timestamp = sharedBucketPriorityQueue.peekN1();
            if (timestamp > cutoffTime) {
                break;
            }

            long ledgerId = sharedBucketPriorityQueue.peekN2();
            long entryId = sharedBucketPriorityQueue.peekN3();

            boolean orphaned = firstLiveLedgerId != null && ledgerId < firstLiveLedgerId;

            sharedBucketPriorityQueue.pop();

            if (orphaned) {
                removeIndexBit(ledgerId, entryId);
                continue;
            }

            // Dedup: queue may carry the same position twice (resident load + failed-create requeue,
            // or seal + merge); only the first delivery of each position decrements the counter via
            // removeIndexBit.
            if (removeIndexBit(ledgerId, entryId)) {
                positions.add(PositionFactory.create(ledgerId, entryId));
                --n;
            }
        }

        updateTimer();

        return positions;
    }

    /**
     * Loads the next segment of every bucket whose earliest unloaded timestamp is due. All due
     * loads are armed together and {@link #getScheduledMessages} only delivers once the whole
     * batch settles: every source due at the same cutoff is then present in the shared queue, so
     * the positions a dispatch round returns keep their order even when some of them had to be
     * fetched from BookKeeper first. Segments of a bucket whose snapshot creation has not
     * completed yet are served from the resident in-memory segment list synchronously — there is
     * no durable copy to read yet, and delivery must not wait for the persistence to finish.
     *
     * <p>Buckets whose segments are all loaded are checked for being drained; a drained bucket's
     * snapshot is deleted here (replacing the former "terminal segment load" cleanup, which no
     * longer exists with lazy loading).
     */
    private void loadDueSegments(long cutoffTime) {
        // Fast path: the cached minimum says the earliest unloaded segment is not due yet, so no
        // bucket can need a load and the per-bucket scan is skipped entirely. A negative minimum
        // means every segment of every bucket is loaded — the scan must still run, because it is
        // also where drained buckets are detected and deleted.
        long minUnloaded = minUnloadedSegmentTimestamp();
        if (minUnloaded >= 0 && minUnloaded > cutoffTime) {
            return;
        }
        // Copy: segment loads may complete inline against the mock storage and delete a drained
        // bucket from the range map during the iteration.
        for (ImmutableBucket bucket : new ArrayList<>(immutableBuckets.asMapOfRanges().values())) {
            long nextUnloadedSegmentTimestamp = bucket.getNextUnloadedSegmentTimestamp();
            if (nextUnloadedSegmentTimestamp < 0) {
                maybeDeleteDrainedBucket(bucket);
                continue;
            }
            if (nextUnloadedSegmentTimestamp > cutoffTime) {
                continue;
            }
            if (bucket.merging) {
                // Retried once the merge settles: the merged bucket owns the segment chain.
                continue;
            }

            List<SnapshotSegment> residentSegments = bucket.getSnapshotSegments().orElse(null);
            if (residentSegments != null) {
                // Snapshot creation still in flight: the resident list still holds every segment.
                int nextSegmentId = bucket.getCurrentSegmentEntryId() + 1;
                SnapshotSegment segment = residentSegments.get(nextSegmentId - 1);
                bucket.setCurrentSegmentEntryId(nextSegmentId);
                invalidateMinUnloadedSegmentTimestamp();
                for (DelayedIndex delayedIndex : segment.getIndexesList()) {
                    sharedBucketPriorityQueue.add(delayedIndex.getTimestamp(), delayedIndex.getLedgerId(),
                            delayedIndex.getEntryId());
                }
                continue;
            }

            if (!bucket.getSnapshotCreateFuture().orElse(NULL_LONG_PROMISE).isDone()) {
                log.info()
                        .attr("bucketKey", bucket.bucketKey())
                        .log("Skip load to wait for bucket snapshot create finish");
                continue;
            }

            long loadStartTime = System.currentTimeMillis();
            stats.recordTriggerEvent(BucketDelayedMessageIndexStats.Type.load);
            final int segmentEntryId = bucket.getCurrentSegmentEntryId() + 1;
            CompletableFuture<Void> loadFuture = bucket.asyncLoadNextBucketSnapshotEntry()
                    .thenAccept(indexList -> {
                        synchronized (BucketDelayedDeliveryTracker.this) {
                            if (CollectionUtils.isEmpty(indexList)) {
                                removeBucket(Range.closed(bucket.getStartLedgerId(), bucket.getEndLedgerId()));
                                // Don't hold delivery on the delete; clear() waits for it.
                                trackDelete(bucket, bucket.asyncDeleteBucketSnapshot(stats));
                                return;
                            }
                            for (DelayedIndex index : indexList) {
                                sharedBucketPriorityQueue.add(index.getTimestamp(), index.getLedgerId(),
                                        index.getEntryId());
                            }
                            invalidateMinUnloadedSegmentTimestamp();
                        }
                    }).whenComplete((__, ex) -> {
                        if (ex != null) {
                            log.error()
                                    .attr("bucketKey", bucket.bucketKey())
                                    .attr("segmentEntryId", segmentEntryId)
                                    .exception(ex)
                                    .log("Failed to load bucket snapshot segment");

                            stats.recordFailEvent(BucketDelayedMessageIndexStats.Type.load);
                        } else {
                            log.info()
                                    .attr("bucketKey", bucket.bucketKey())
                                    .attr("segmentEntryId", segmentEntryId)
                                    .log("Load next bucket snapshot segment finish");

                            stats.recordSuccessEvent(BucketDelayedMessageIndexStats.Type.load,
                                    System.currentTimeMillis() - loadStartTime);
                        }
                    });
            pendingLoads.add(loadFuture);
            // Once the load is done, start the next dispatch round: the loaded messages can
            // already be due, and the timer would only run at its next tick. Not from the
            // callback above, as the round could then run before the load completes, and skip.
            // A failed load is retried at the timer's next tick.
            loadFuture.whenComplete((__, ex) -> {
                if (ex != null) {
                    rescheduleTimer(0);
                } else {
                    context.triggerReadMoreEntries();
                }
            });
        }
    }

    /**
     * Deletes the snapshot of a bucket whose segments are all loaded once no tracked position of
     * the bucket remains: every message it indexed has been delivered (or orphan-skipped), so the
     * snapshot is obsolete. A failed delete leaves the bucket in {@code immutableBuckets}, so the
     * next dispatch round re-detects the drained bucket and retries.
     */
    private void maybeDeleteDrainedBucket(ImmutableBucket bucket) {
        if (pendingDrainedDeletes.contains(bucket)) {
            return;
        }
        for (long ledgerId : bucket.getDelayedIndexBitMap().keySet()) {
            if (index.containsLedger(ledgerId)) {
                return;
            }
        }
        if (!pendingDrainedDeletes.add(bucket)) {
            return;
        }
        ManagedLedger managedLedger = context.getCursor().getManagedLedger();
        String ledgerName = managedLedger != null ? managedLedger.getName() : null;
        deleteBucketSnapshot(ledgerName, Range.closed(bucket.getStartLedgerId(), bucket.getEndLedgerId()), bucket)
                .whenComplete((__, ex) -> pendingDrainedDeletes.remove(bucket));
    }

    private synchronized boolean checkPendingLoadDone() {
        pendingLoads.removeIf(CompletableFuture::isDone);
        return pendingLoads.isEmpty();
    }

    private synchronized CompletableFuture<Void> keepUncleaned(List<ImmutableBucket> buckets) {
        uncleanedBuckets.addAll(buckets);
        return CompletableFuture.completedFuture(null);
    }

    /**
     * Track the snapshot delete of a bucket that is no longer in {@code immutableBuckets}, so that
     * {@link #clear()} waits for it and retries it if it failed.
     */
    private CompletableFuture<Void> trackDelete(ImmutableBucket bucket, CompletableFuture<Void> deleteFuture) {
        // Record the failure in the returned stage itself, so that it is visible to whoever waits on it.
        CompletableFuture<Void> tracked = deleteFuture.whenComplete((__, ex) -> {
            synchronized (BucketDelayedDeliveryTracker.this) {
                if (ex != null) {
                    uncleanedBuckets.add(bucket);
                }
            }
        });
        synchronized (this) {
            pendingDeletes.add(tracked);
        }
        tracked.whenComplete((__, ex) -> pendingDeletes.remove(tracked));
        return tracked;
    }

    @Override
    public boolean shouldPauseAllDeliveries() {
        return false;
    }

    @Override
    public synchronized CompletableFuture<Void> clear() {
        // Wait for in-flight trim/merge and pending load/delete work before resetting state.
        CompletableFuture<Void> before = trimFuture != null && !trimFuture.isDone()
                ? trimFuture : CompletableFuture.completedFuture(null);
        trimFuture = before
                .exceptionally(t -> {
                    log.warn().exception(t).log("Trim/merge buckets failed, but still clear");
                    return null;
                })
                .thenCompose(__ -> {
                    List<CompletableFuture<?>> pending = new ArrayList<>();
                    synchronized (BucketDelayedDeliveryTracker.this) {
                        if (!pendingLoads.isEmpty()) {
                            pending.addAll(pendingLoads);
                        }
                        pending.addAll(pendingDeletes);
                    }
                    // A failed delete is retried below, so only wait for the pending work to settle.
                    return FutureUtil.waitForAll(pending)
                            .exceptionally(t -> {
                                log.warn().exception(t)
                                        .log("Pending delayed delivery work failed, but still clear");
                                return null;
                            })
                            .thenCompose(ignore -> {
                                synchronized (BucketDelayedDeliveryTracker.this) {
                                    CompletableFuture<Void> future = cleanImmutableBuckets();
                                    sharedBucketPriorityQueue.clear();
                                    index.clear();
                                    lastMutableBucket.clear();
                                    return future;
                                }
                            });
                });
        return trimFuture;
    }

    @Override
    public void close() {
        // Block for AutoCloseable / synchronous callers; asynchronous callers should use closeAsync().
        closeAsync().join();
    }

    @Override
    public CompletableFuture<Void> closeAsync() {
        List<CompletableFuture<Long>> completableFutures;
        synchronized (this) {
            super.close();
            lastMutableBucket.close();
            sharedBucketPriorityQueue.close();
            completableFutures = immutableBuckets.asMapOfRanges().values().stream()
                    .map(bucket -> bucket.getSnapshotCreateFuture().orElse(NULL_LONG_PROMISE)).toList();
        }
        return FutureUtil.waitForAll(completableFutures)
                .exceptionally(e -> {
                    log.warn().exception(e).log("Failed wait to snapshot generate");
                    return null;
                });
    }

    private CompletableFuture<Void> cleanImmutableBuckets() {
        Map<Range<Long>, ImmutableBucket> bucketsToDelete =
                new HashMap<>(immutableBuckets.asMapOfRanges());

        List<CompletableFuture<Void>> futures = new ArrayList<>();
        bucketsToDelete.forEach((range, bucket) -> {
            removeBucket(range);
            futures.add(trackDelete(bucket, bucket.clear(stats)));
        });

        // Delete the snapshots of buckets that are no longer tracked. A bucket whose delete fails again is
        // kept for the next clear(), and clear() fails so that the caller does not assume the old delayed
        // state is gone.
        List<ImmutableBucket> retries = new ArrayList<>(uncleanedBuckets);
        uncleanedBuckets.clear();
        for (ImmutableBucket bucket : retries) {
            futures.add(trackDelete(bucket, bucket.asyncDeleteBucketSnapshot(stats)));
        }

        return FutureUtil.waitForAll(futures);
    }

    private boolean removeIndexBit(long ledgerId, long entryId) {
        return index.untrack(ledgerId, entryId);
    }

    public synchronized boolean containsMessage(long ledgerId, long entryId) {
        return index.contains(ledgerId, entryId);
    }

    public Map<String, TopicMetricBean> genTopicMetricMap() {
        stats.recordNumOfBuckets((int) (bucketsCount.get() + 1));
        stats.recordDelayedMessageIndexLoaded(this.sharedBucketPriorityQueue.size() + this.lastMutableBucket.size());
        stats.recordBucketSnapshotSizeBytes(totalSnapshotLengthBytes.get());
        return stats.genTopicMetricMap();
    }

    /**
     * Delete orphaned bucket snapshots whose ledger range lies entirely before the earliest
     * surviving ledger. Buckets are deleted sequentially; the chain stops on first failure
     * to avoid wasted work when storage is unavailable.
     */
    private synchronized CompletableFuture<Void> asyncTrimImmutableBuckets() {
        Long firstLedgerId = firstActiveLedgerId();
        if (null == firstLedgerId) {
            return CompletableFuture.completedFuture(null);
        }
        ManagedLedger ledger = context.getCursor().getManagedLedger();

        Map<Range<Long>, ImmutableBucket> toBeDeletedBuckets = new HashMap<>();
        // subRangeMap returns clipped intersection ranges. Snapshot deletion must use the original
        // bucket range, so only select buckets whose complete range precedes the first live ledger.
        immutableBuckets.asMapOfRanges().forEach((range, bucket) -> {
            if (range.upperEndpoint() < firstLedgerId) {
                toBeDeletedBuckets.put(range, bucket);
            }
        });

        if (toBeDeletedBuckets.isEmpty()) {
            return CompletableFuture.completedFuture(null);
        }

        String ledgerName = ledger.getName();
        CompletableFuture<Void> chain = CompletableFuture.completedFuture(null);
        for (Map.Entry<Range<Long>, ImmutableBucket> entry : toBeDeletedBuckets.entrySet()) {
            chain = chain.thenCompose(__ ->
                    deleteBucketSnapshot(ledgerName, entry.getKey(), entry.getValue()));
        }
        return chain;
    }

    private CompletableFuture<Void> deleteBucketSnapshot(String ledgerName,
                                                          Range<Long> range, ImmutableBucket bucket) {
        // The bucket id is only known once the snapshot creation completes, so wait for an in-flight
        // creation before deleting. When the creation failed the bucket has already been removed and
        // downgraded to memory mode, so there is no snapshot left to delete.
        return bucket.getSnapshotCreateFuture().orElse(NULL_LONG_PROMISE)
                .thenCompose(bucketId -> INVALID_BUCKET_ID.equals(bucketId)
                        ? CompletableFuture.<Void>completedFuture(null)
                        : doDeleteBucketSnapshot(ledgerName, range, bucket));
    }

    private CompletableFuture<Void> doDeleteBucketSnapshot(String ledgerName,
                                                           Range<Long> range, ImmutableBucket bucket) {
        return bucket.asyncDeleteBucketSnapshot(stats)
                .handle((__, t) -> {
                    if (t != null) {
                        log.warn().attr("LedgerName", ledgerName)
                                .attr("BucketKey", bucket.bucketKey())
                                .log("Failed to delete bucket snapshot");
                        throw new CompletionException(t);
                    }
                    synchronized (this) {
                        removeBucket(range);
                        bucket.getDelayedIndexBitMap().forEach((ledgerId, bitmap) ->
                                bitmap.forEachLong(entryId -> index.untrack(ledgerId, entryId)));
                    }
                    return null;
                });
    }

    private Long firstActiveLedgerId() {
        ManagedCursor cursor = context.getCursor();
        Position mdp = cursor.getMarkDeletedPosition();
        return mdp == null ? null : mdp.getLedgerId();
    }

    private void putBucket(Range<Long> range, ImmutableBucket bucket) {
        long removedLength = immutableBuckets.subRangeMap(range).asMapOfRanges().values().stream()
                .mapToLong(ImmutableBucket::getSnapshotLength)
                .sum();

        immutableBuckets.put(range, bucket);
        bucketsCount.set(immutableBuckets.asMapOfRanges().size());
        totalSnapshotLengthBytes.addAndGet(bucket.getSnapshotLength() - removedLength);
        invalidateMinUnloadedSegmentTimestamp();
    }

    private void removeBucket(Range<Long> range) {
        // Use exact key matching - all callers should provide exact keys
        ImmutableBucket bucket = immutableBuckets.asMapOfRanges().get(range);

        if (bucket != null) {
            // Remove even if snapshot length is 0 (for newly created buckets)
            immutableBuckets.asMapOfRanges().remove(range);
            bucketsCount.set(immutableBuckets.asMapOfRanges().size());
            totalSnapshotLengthBytes.addAndGet(-bucket.getSnapshotLength());
            invalidateMinUnloadedSegmentTimestamp();
        }
    }

    private void updateBucketSnapshotLength(ImmutableBucket bucket, long newLength) {
        if (!immutableBuckets.asMapOfRanges().containsValue(bucket)) {
            return;
        }
        long oldLength = bucket.getSnapshotLength();
        bucket.setSnapshotLength(newLength);
        totalSnapshotLengthBytes.addAndGet(newLength - oldLength);
    }
}
