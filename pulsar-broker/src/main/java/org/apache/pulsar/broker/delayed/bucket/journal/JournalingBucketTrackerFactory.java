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

import java.util.concurrent.TimeUnit;
import lombok.CustomLog;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.delayed.BucketDelayedDeliveryTrackerFactory;
import org.apache.pulsar.broker.delayed.DispatcherDelayedDeliveryContext;
import org.apache.pulsar.broker.delayed.bucket.BucketDelayedDeliveryTracker;
import org.apache.pulsar.broker.delayed.bucket.RecoverDelayedDeliveryTrackerException;
import org.apache.pulsar.broker.service.persistent.AbstractPersistentDispatcherMultipleConsumers;

/**
 * Selects, per topic, between the regular bucket tracker and the
 * {@link JournalingBucketDelayedDeliveryTracker}: topics whose name ends with
 * {@value #JOURNALING_TOPIC_SUFFIX} get the journaling tracker, everything else keeps the regular
 * bucket tracker unchanged. A single broker can therefore run both implementations side by side,
 * which is the point of this factory — the body-copy experiment is measured by producing the same
 * traffic on {@code mytopic} and {@code mytopic-dj} and comparing.
 *
 * <p>Select with
 * {@code delayedDeliveryTrackerFactoryClassName=
 * org.apache.pulsar.broker.delayed.bucket.journal.JournalingBucketTrackerFactory}.
 */
@CustomLog
public class JournalingBucketTrackerFactory extends BucketDelayedDeliveryTrackerFactory {

    /** Topics ending with this suffix get the journaling tracker. */
    public static final String JOURNALING_TOPIC_SUFFIX = "-dj";

    private volatile JournalStorage journalStorage;

    @Override
    public void initialize(PulsarService pulsarService) throws Exception {
        super.initialize(pulsarService);
        JournalStorage storage = new JournalStorage(pulsarService);
        storage.start().get(60, TimeUnit.SECONDS);
        this.journalStorage = storage;
        log.info().attr("suffix", JOURNALING_TOPIC_SUFFIX)
                .log("Journaling bucket tracker factory initialized");
    }

    @Override
    protected BucketDelayedDeliveryTracker newTracker0(AbstractPersistentDispatcherMultipleConsumers dispatcher)
            throws RecoverDelayedDeliveryTrackerException {
        if (!usesJournalingTracker(dispatcher)) {
            return super.newTracker0(dispatcher);
        }
        DispatcherDelayedDeliveryContext context = new DispatcherDelayedDeliveryContext(dispatcher);
        return new JournalingBucketDelayedDeliveryTracker(context, timer, tickTimeMillis,
                java.time.Clock.systemUTC(), isDelayedDeliveryDeliverAtTimeStrict,
                bucketSnapshotStorage, delayedDeliveryMinIndexCountPerBucket,
                TimeUnit.SECONDS.toMillis(delayedDeliveryMaxTimeStepPerBucketSnapshotSegmentSeconds),
                delayedDeliveryMaxIndexesPerBucketSnapshotSegment, delayedDeliveryMaxNumBuckets,
                journalStorage);
    }

    private boolean usesJournalingTracker(AbstractPersistentDispatcherMultipleConsumers dispatcher) {
        String topicName = dispatcher.getTopic().getName();
        return topicName != null && topicName.endsWith(JOURNALING_TOPIC_SUFFIX);
    }

    @Override
    public void close() throws Exception {
        JournalStorage storage = journalStorage;
        journalStorage = null;
        if (storage != null) {
            storage.close().whenComplete((__, ex) -> {
                if (ex != null) {
                    log.warn().exceptionMessage(ex).log("Failed to close the journal storage");
                }
            });
        }
        super.close();
    }
}
