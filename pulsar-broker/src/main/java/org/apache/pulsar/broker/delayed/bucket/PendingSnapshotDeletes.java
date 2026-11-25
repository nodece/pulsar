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

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.pulsar.common.util.FutureUtil;

/**
 * The snapshot deletions of one tracker that are not done yet: in flight, or failed and
 * waiting for a retry. Bucket snapshots are deleted asynchronously (drained buckets, trimmed
 * buckets, merge sources, recovery leftovers) and the tracker must remember that work:
 *
 * <ul>
 *   <li>an in-flight delete guards against stacking a second delete for the same bucket
 *   <li>a failed delete keeps the bucket for a later retry, so a snapshot is never abandoned
 *       silently ({@code clear()} retries the remembered buckets)
 * </ul>
 */
final class PendingSnapshotDeletes {

    /** Snapshot deletes that are still in flight, keyed by the bucket they belong to. */
    private final Set<ImmutableBucket> deletesInFlight = ConcurrentHashMap.newKeySet();

    /** Buckets no longer owned by the tracker whose snapshot delete failed and needs a retry. */
    private final Set<ImmutableBucket> bucketsPendingRetry = ConcurrentHashMap.newKeySet();

    /** Futures of the in-flight deletes, so {@link #awaitAllSettled()} can cover them all. */
    private final Set<CompletableFuture<Void>> pendingDeleteFutures = ConcurrentHashMap.newKeySet();

    /**
     * Tracks a snapshot delete: the bucket counts as in-flight until the delete settles, and a
     * failed delete is remembered for a retry. The returned future is the tracked one — it
     * completes with the outcome of {@code deleteFuture}.
     */
    CompletableFuture<Void> track(ImmutableBucket bucket, CompletableFuture<Void> deleteFuture) {
        deletesInFlight.add(bucket);
        pendingDeleteFutures.add(deleteFuture);
        return deleteFuture.whenComplete((__, ex) -> {
            deletesInFlight.remove(bucket);
            pendingDeleteFutures.remove(deleteFuture);
            if (ex != null) {
                bucketsPendingRetry.add(bucket);
            }
        });
    }

    /** Checks whether the bucket's snapshot delete is still in flight.
     *
     * @return true while a delete is running for this bucket
     */
    boolean isDeleteInFlight(ImmutableBucket bucket) {
        return deletesInFlight.contains(bucket);
    }

    /**
     * Remembers a bucket whose snapshot must be deleted by a later {@code clear()} without
     * starting a delete now — the sources of a failed merge, whose snapshots must stay
     * recoverable until then.
     */
    void rememberForRetry(ImmutableBucket bucket) {
        bucketsPendingRetry.add(bucket);
    }

    /** Takes the buckets whose snapshot delete failed, removing them from the retry set.
     *
     * @return the buckets whose delete must be retried
     */
    List<ImmutableBucket> takeBucketsPendingRetry() {
        if (bucketsPendingRetry.isEmpty()) {
            return List.of();
        }
        List<ImmutableBucket> buckets = new ArrayList<>(bucketsPendingRetry);
        bucketsPendingRetry.clear();
        return buckets;
    }

    /**
     * Waits for every in-flight delete to settle.
     *
     * @return a future completing when nothing is in flight, succeeding even when some deletes
     *         failed (the failed buckets are kept for a retry instead)
     */
    CompletableFuture<Void> awaitAllSettled() {
        if (pendingDeleteFutures.isEmpty()) {
            return CompletableFuture.completedFuture(null);
        }
        return FutureUtil.waitForAll(List.copyOf(pendingDeleteFutures)).exceptionally(ex -> null);
    }
}
