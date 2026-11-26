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
package org.apache.pulsar.broker.service.persistent;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.pulsar.common.protocol.Commands.serializeMetadataAndPayload;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.util.concurrent.ImmediateEventExecutor;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.impl.ManagedCursorImpl;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.apache.bookkeeper.test.MockedBookKeeperTestCase;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.service.Consumer;
import org.apache.pulsar.broker.service.Dispatcher;
import org.apache.pulsar.broker.service.EntryBatchIndexesAcks;
import org.apache.pulsar.broker.service.EntryBatchSizes;
import org.apache.pulsar.broker.service.TransportCnx;
import org.apache.pulsar.broker.testcontext.PulsarTestContext;
import org.apache.pulsar.common.api.proto.CommandSubscribe.SubType;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.apache.pulsar.common.policies.data.stats.ConsumerStatsImpl;
import org.apache.pulsar.common.protocol.Commands;
import org.awaitility.Awaitility;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

/**
 * Replays (redeliveries, due delayed messages) go before normal reads; a redelivery storm must not
 * starve the entries waiting to be read for longer than {@code dispatcherMaxNormalReadDelayInMillis}.
 */
public class PersistentDispatcherMultipleConsumersReadFairnessTest extends MockedBookKeeperTestCase {

    private static final String TOPIC = "persistent://prop/ns/shared-read-fairness";
    private static final long TIMEOUT_SECONDS = 10;
    private static final long BOUND_MILLIS = 100;

    private PulsarTestContext pulsarTestContext;
    private ManagedLedgerImpl ledger;
    private ManagedCursorImpl cursor;
    private PersistentDispatcherMultipleConsumers dispatcher;
    private Consumer consumer;

    private final List<Delivery> deliveries = Collections.synchronizedList(new ArrayList<>());
    private final AtomicLong msgCounter = new AtomicLong();
    // The dispatcher's clock: the test advances it to cross the postponement bound deterministically.
    private final AtomicLong clockNanos = new AtomicLong();
    // Normal reads issued on the cursor.
    private final AtomicInteger normalReads = new AtomicInteger();
    // Redelivery rounds left in the storm: every delivery is nacked while it is positive.
    private final AtomicInteger stormRoundsRemaining = new AtomicInteger();

    @Override
    protected ManagedLedgerConfig initManagedLedgerConfig(ManagedLedgerConfig config) {
        super.initManagedLedgerConfig(config);
        config.setNewEntriesCheckDelayInMillis(0);
        config.setMaxEntriesPerLedger(1_000_000);
        config.setRetentionTime(1, TimeUnit.HOURS);
        config.setRetentionSizeInMB(-1);
        return config;
    }

    private void buildFixture(String ledgerName, int maxNormalReadDelayMillis) throws Exception {
        ServiceConfiguration svcConfig = new ServiceConfiguration();
        svcConfig.setBrokerShutdownTimeoutMs(0L);
        svcConfig.setLoadBalancerOverrideBrokerNicSpeedGbps(Optional.of(1.0d));
        svcConfig.setClusterName("test");
        svcConfig.setDelayedDeliveryEnabled(false);
        svcConfig.setSystemTopicEnabled(false);
        svcConfig.setTopicLevelPoliciesEnabled(false);
        svcConfig.setDispatcherReadFailureBackoffInitialTimeInMs(1);
        svcConfig.setDispatcherMaxNormalReadDelayInMillis(maxNormalReadDelayMillis);

        pulsarTestContext = PulsarTestContext.builderForNonStartableContext()
                .config(svcConfig)
                .managedLedgerClients(bkc, factory)
                .build();

        ledger = (ManagedLedgerImpl) factory.open(ledgerName, initManagedLedgerConfig(new ManagedLedgerConfig()));
        PersistentTopic topic = new PersistentTopic(TOPIC, ledger, pulsarTestContext.getBrokerService());
        cursor = spy((ManagedCursorImpl) ledger.openCursor("sub"));
        doAnswer(inv -> {
            normalReads.incrementAndGet();
            return inv.callRealMethod();
        }).when(cursor).asyncReadEntriesWithSkipOrWait(anyInt(), anyLong(), any(), any(), any(), any());
        PersistentSubscription subscription = new PersistentSubscription(topic, "sub", cursor, true) {
            @Override
            protected Dispatcher reuseOrCreateDispatcher(Dispatcher existingDispatcher, Consumer newConsumer) {
                return PersistentDispatcherMultipleConsumersReadFairnessTest.this.dispatcher;
            }
        };
        dispatcher = spy(new PersistentDispatcherMultipleConsumers(topic, cursor, subscription) {
            @Override
            protected long currentTimeNanos() {
                return clockNanos.get();
            }
        });
        deliveries.clear();
        normalReads.set(0);
        clockNanos.set(0);
        stormRoundsRemaining.set(0);

        consumer = newNackingConsumer();
    }

    /** A consumer with one permit that records deliveries and nacks them while the storm is on. */
    private Consumer newNackingConsumer() {
        Consumer mockConsumer = mock(Consumer.class);
        TransportCnx cnx = mock(TransportCnx.class);
        doReturn(true).when(cnx).isActive();
        doReturn(cnx).when(mockConsumer).cnx();
        doReturn(1).when(mockConsumer).getAvailablePermits();
        doReturn(1).when(mockConsumer).getAvgMessagesPerEntry();
        doReturn(true).when(mockConsumer).isWritable();
        doReturn(false).when(mockConsumer).readCompacted();
        doReturn(false).when(mockConsumer).isPreciseDispatcherFlowControl();
        doReturn(false).when(mockConsumer).isBlocked();
        doReturn("c1").when(mockConsumer).consumerName();
        doReturn(0).when(mockConsumer).getPriorityLevel();
        doReturn(0L).when(mockConsumer).getConsumerEpoch();
        doReturn(SubType.Shared).when(mockConsumer).subType();
        doReturn(new ConsumerStatsImpl()).when(mockConsumer).getStats();
        doAnswer(inv -> {
            List<Entry> entries = inv.getArgument(0);
            List<Position> positions = new ArrayList<>(entries.size());
            for (Entry entry : entries) {
                positions.add(entry.getPosition());
                deliveries.add(Delivery.of(entry.getPosition()));
                entry.release();
            }
            EntryBatchSizes batchSizes = inv.getArgument(1);
            if (batchSizes != null) {
                batchSizes.recyle();
            }
            EntryBatchIndexesAcks batchIndexesAcks = inv.getArgument(2);
            if (batchIndexesAcks != null) {
                batchIndexesAcks.recycle();
            }
            if (stormRoundsRemaining.getAndDecrement() > 0) {
                for (Position position : positions) {
                    dispatcher.redeliveryMessages.add(position.getLedgerId(), position.getEntryId());
                }
            }
            return ImmediateEventExecutor.INSTANCE.newSucceededFuture(null);
        }).when(mockConsumer).sendMessages(any(), any(), any(), anyInt(), anyLong(), anyLong(), any());
        return mockConsumer;
    }

    @AfterMethod(alwaysRun = true)
    public void afterMethod() {
        try {
            if (cursor != null && !cursor.isClosed()) {
                cursor.close();
            }
        } catch (Exception ignore) {
            // best effort
        }
        try {
            if (ledger != null) {
                ledger.close();
            }
        } catch (Exception ignore) {
            // best effort
        }
        try {
            if (pulsarTestContext != null) {
                pulsarTestContext.close();
            }
        } catch (Exception ignore) {
            // best effort
        }
    }

    /**
     * A redelivery storm postpones the normal read, but no longer than the bound: the entries
     * waiting behind it are read once the clock passes the bound, while the storm goes on.
     */
    @Test
    public void testNormalReadIsBoundedUnderRedeliveryStorm() throws Exception {
        buildFixture("shared-read-fairness-bound", (int) BOUND_MILLIS);
        Position p1 = publish();
        Position p2 = publish();
        Position p3 = publish();
        stormRoundsRemaining.set(Integer.MAX_VALUE);

        subscribeAndRead();
        awaitDeliveriesOf(p1, 3);
        assertFalse(containsDelivery(p2),
                "the normal read is postponed while the bound is not reached: " + deliveries);

        // One nanosecond below the bound: still postponed.
        clockNanos.set(TimeUnit.MILLISECONDS.toNanos(BOUND_MILLIS) - 1);
        int p1Before = frequency(p1);
        awaitDeliveriesOf(p1, p1Before + 2);
        assertFalse(containsDelivery(p2),
                "the normal read is still postponed one nanosecond below the bound: " + deliveries);

        // At the bound: the next scheduling round reads from the cursor, the storm goes on after it.
        clockNanos.set(TimeUnit.MILLISECONDS.toNanos(BOUND_MILLIS));
        Awaitility.await("the postponed entries are read once the bound is reached")
                .atMost(Duration.ofSeconds(TIMEOUT_SECONDS))
                .until(() -> containsDelivery(p2));
        int p1AtYield = frequency(p1);
        awaitDeliveriesOf(p1, p1AtYield + 2);
        assertFalse(containsDelivery(p3),
                "one postponement episode buys one normal read; the clock is frozen at the bound: " + deliveries);

        // The postponement restarts after the normal read: another bound of waiting lets the next
        // entry in, and the storm still goes on after it.
        clockNanos.addAndGet(TimeUnit.MILLISECONDS.toNanos(BOUND_MILLIS));
        Awaitility.await("the next postponement episode lets the next entry in")
                .atMost(Duration.ofSeconds(TIMEOUT_SECONDS))
                .until(() -> containsDelivery(p3));
        int p1AtSecondYield = frequency(p1);
        awaitDeliveriesOf(p1, p1AtSecondYield + 2);

        // Over: the storm drains and no replay round is left.
        stormRoundsRemaining.set(0);
        Awaitility.await("the storm ends")
                .atMost(Duration.ofSeconds(TIMEOUT_SECONDS))
                .until(() -> dispatcher.redeliveryMessages.isEmpty() && !dispatcher.havePendingReplayRead);
    }

    /** With the bound disabled, replays keep the strict priority: the entries wait for the storm to end. */
    @Test
    public void testReplaysKeepStrictPriorityWhenBoundDisabled() throws Exception {
        buildFixture("shared-read-fairness-disabled", 0);
        Position p1 = publish();
        Position p2 = publish();
        Position p3 = publish();
        stormRoundsRemaining.set(5);

        subscribeAndRead();
        Awaitility.await("the storm ends and the backlog is read")
                .atMost(Duration.ofSeconds(TIMEOUT_SECONDS))
                .until(() -> deliveries.contains(Delivery.of(p3)));

        // The clock is far past what the bound would be: it had no effect.
        clockNanos.set(TimeUnit.HOURS.toNanos(1));
        assertEquals(frequency(p1), 6, "one initial delivery and five redeliveries: " + deliveries);
        assertTrue(lastIndexOf(p1) < indexOf(p2),
                "the entries waiting behind the storm are read only after it ends: " + deliveries);
        assertEquals(frequency(p1) + frequency(p2) + frequency(p3), 8);
    }

    /**
     * The postponement is only counted while the cursor has entries to read: with nothing to read,
     * a normal read would only park a waiting read, so the storm must not be interrupted.
     */
    @Test
    public void testNoYieldWithoutEntriesToRead() throws Exception {
        buildFixture("shared-read-fairness-nothing-to-read", (int) BOUND_MILLIS);
        Position p1 = publish();
        stormRoundsRemaining.set(Integer.MAX_VALUE);

        subscribeAndRead();
        awaitDeliveriesOf(p1, 3);
        clockNanos.set(TimeUnit.HOURS.toNanos(1));
        int p1Before = frequency(p1);
        awaitDeliveriesOf(p1, p1Before + 3);
        assertEquals(normalReads.get(), 1,
                "only the initial read: nothing else is readable, so the storm is never interrupted");

        stormRoundsRemaining.set(0);
        Awaitility.await("the storm ends")
                .atMost(Duration.ofSeconds(TIMEOUT_SECONDS))
                .until(() -> dispatcher.redeliveryMessages.isEmpty() && !dispatcher.havePendingReplayRead);
    }

    private void subscribeAndRead() throws Exception {
        dispatcher.addConsumer(consumer).get();
        dispatcher.readMoreEntriesAsync();
    }

    private void awaitDeliveriesOf(Position position, int expectedCount) {
        Awaitility.await("deliveries of " + position + " to reach " + expectedCount)
                .atMost(Duration.ofSeconds(TIMEOUT_SECONDS))
                .untilAsserted(() -> assertTrue(frequency(position) >= expectedCount,
                        "deliveries=" + frequency(position) + " of " + position));
    }

    private int frequency(Position position) {
        synchronized (deliveries) {
            return Collections.frequency(deliveries, Delivery.of(position));
        }
    }

    private int indexOf(Position position) {
        synchronized (deliveries) {
            return deliveries.indexOf(Delivery.of(position));
        }
    }

    private int lastIndexOf(Position position) {
        synchronized (deliveries) {
            return deliveries.lastIndexOf(Delivery.of(position));
        }
    }

    private boolean containsDelivery(Position position) {
        return frequency(position) > 0;
    }

    private Position publish() throws Exception {
        long sequenceId = msgCounter.incrementAndGet();
        MessageMetadata messageMetadata = new MessageMetadata()
                .setSequenceId(sequenceId)
                .setProducerName("testProducer")
                .setPartitionKeyB64Encoded(false)
                .setPublishTime(System.currentTimeMillis());
        ByteBuf message = serializeMetadataAndPayload(Commands.ChecksumType.Crc32c, messageMetadata,
                Unpooled.copiedBuffer(("m" + sequenceId).getBytes(UTF_8)));
        byte[] bytes = new byte[message.readableBytes()];
        message.readBytes(bytes);
        message.release();
        return ledger.addEntry(bytes);
    }

    private record Delivery(long ledgerId, long entryId) {

        static Delivery of(Position position) {
            return new Delivery(position.getLedgerId(), position.getEntryId());
        }

        @Override
        public String toString() {
            return ledgerId + ":" + entryId;
        }
    }
}
