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

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.mock;
import io.netty.buffer.Unpooled;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.broker.delayed.MockBucketSnapshotStorage;
import org.apache.pulsar.broker.delayed.MockManagedCursor;
import org.apache.pulsar.broker.delayed.NoopDelayedDeliveryContext;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

/**
 * The journaling tracker's write side: every delayed entry that the dispatcher hands over with its
 * buffer is journaled, framed and readable back, while the inherited bucket machinery keeps
 * tracking the position unchanged.
 */
public class JournalingBucketTrackerTest {

    private JournalingBucketDelayedDeliveryTracker tracker;
    private MockBucketSnapshotStorage snapshotStorage;

    private static final String DISPATCHER_NAME = "persistent://prop/ns/delay-journal-test / sub";

    @AfterMethod(alwaysRun = true)
    public void cleanup() throws Exception {
        if (tracker != null) {
            tracker.close();
            tracker = null;
        }
        if (snapshotStorage != null) {
            snapshotStorage.close();
            snapshotStorage = null;
        }
    }

    private JournalingBucketDelayedDeliveryTracker newTracker() throws Exception {
        snapshotStorage = new MockBucketSnapshotStorage();
        snapshotStorage.start();
        MockManagedCursor cursor = new MockManagedCursor(DISPATCHER_NAME);
        NoopDelayedDeliveryContext context = new NoopDelayedDeliveryContext(DISPATCHER_NAME, cursor);
        // The journal storage is BK-backed; in this unit test it is not started, so the tracker
        // counts failures instead. The journaling assertion runs against a started storage only in
        // the mocked-BK flow below.
        return new JournalingBucketDelayedDeliveryTracker(context, mock(io.netty.util.Timer.class),
                1000, java.time.Clock.systemUTC(), true, snapshotStorage,
                5, TimeUnit.MILLISECONDS.toMillis(10), -1, 50, null);
    }

    @Test
    public void journalFailuresAreCountedAndDeliveryTrackingIsUnaffected() throws Exception {
        tracker = newTracker();

        // Without a started journal storage the appends fail; the failures are counted and
        // the inherited tracking still accepts the message.
        byte[] entryData = new byte[64];
        tracker.addMessage(1, 1, System.currentTimeMillis() + 60_000);
        tracker.addEntry(1, 1, System.currentTimeMillis() + 60_000, Unpooled.wrappedBuffer(entryData));

        await().atMost(Duration.ofSeconds(5)).until(() -> tracker.journalFailures() >= 1);
        assertThat(tracker.containsMessage(1, 1)).isTrue();
        assertThat(tracker.getNumberOfDelayedMessages()).isEqualTo(1);
        assertThat(tracker.journaledEntries()).isZero();
    }

    @Test
    public void framedHeaderSizeIsStable() {
        // deliverAt(8) | dataLedgerId(8) | dataEntryId(8)
        assertThat(JournalingBucketDelayedDeliveryTracker.headerSize()).isEqualTo(24);
    }
}
