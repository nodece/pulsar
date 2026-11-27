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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Answers.RETURNS_SELF;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.api.CreateBuilder;
import org.apache.bookkeeper.client.api.DeleteBuilder;
import org.apache.bookkeeper.client.api.LedgerEntries;
import org.apache.bookkeeper.client.api.LedgerEntry;
import org.apache.bookkeeper.client.api.OpenBuilder;
import org.apache.bookkeeper.client.api.ReadHandle;
import org.apache.bookkeeper.client.api.WriteHandle;
import org.apache.pulsar.broker.BookKeeperClientFactory;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Exercises {@link TopicDelayRouter} against an in-memory BookKeeper double: the producer-visible
 * position, the due-time behavior of the exact-due timer (no rewrite before the entry is due,
 * held-back records re-arming the timer at their own {@code deliverAt}, an earlier append moving
 * the firing earlier), and close cancelling the timer and deleting the storage.
 */
public class TopicDelayRouterTest {

    private static final String TOPIC_NAME = "persistent://tns/ns/topic";

    /** Stands in for BookKeeper storage: ledgerId → framed records, in append order. */
    private final Map<Long, List<byte[]>> ledgers = new ConcurrentHashMap<>();
    private final AtomicLong nextLedgerId = new AtomicLong();
    private final List<Long> deletedLedgers = new CopyOnWriteArrayList<>();

    private ScheduledExecutorService executor;
    private PersistentTopic topic;
    private TopicDelayRouter router;

    @BeforeMethod
    void setUp() throws Exception {
        ServiceConfiguration config = new ServiceConfiguration();
        config.setManagedLedgerDefaultEnsembleSize(1);
        config.setManagedLedgerDefaultWriteQuorum(1);
        config.setManagedLedgerDefaultAckQuorum(1);
        config.setDelayedDeliveryTickTimeMillis(100);
        executor = Executors.newSingleThreadScheduledExecutor();

        BookKeeper bookKeeper = mock(BookKeeper.class);
        CreateBuilder createBuilder = mock(CreateBuilder.class, RETURNS_SELF);
        when(createBuilder.execute()).thenAnswer(inv -> newWriteHandle());
        when(bookKeeper.newCreateLedgerOp()).thenReturn(createBuilder);

        AtomicLong openLedgerId = new AtomicLong(-1);
        OpenBuilder openBuilder = mock(OpenBuilder.class);
        when(openBuilder.withLedgerId(anyLong())).thenAnswer(inv -> {
            openLedgerId.set(inv.getArgument(0));
            return openBuilder;
        });
        when(openBuilder.withDigestType(any())).thenReturn(openBuilder);
        when(openBuilder.withPassword(any())).thenReturn(openBuilder);
        when(openBuilder.execute())
                .thenAnswer(inv -> CompletableFuture.completedFuture(newReadHandle(openLedgerId.get())));
        when(bookKeeper.newOpenLedgerOp()).thenReturn(openBuilder);

        when(bookKeeper.newDeleteLedgerOp()).thenAnswer(inv -> {
            AtomicLong deleteLedgerId = new AtomicLong(-1);
            DeleteBuilder deleteBuilder = mock(DeleteBuilder.class);
            when(deleteBuilder.withLedgerId(anyLong())).thenAnswer(deleteInv -> {
                deleteLedgerId.set(deleteInv.getArgument(0));
                return deleteBuilder;
            });
            when(deleteBuilder.execute()).thenAnswer(deleteInv -> {
                deletedLedgers.add(deleteLedgerId.get());
                ledgers.remove(deleteLedgerId.get());
                return CompletableFuture.completedFuture(null);
            });
            return deleteBuilder;
        });

        BookKeeperClientFactory factory = mock(BookKeeperClientFactory.class);
        when(factory.create(any(), any(), any(), any(), any()))
                .thenReturn(CompletableFuture.completedFuture(bookKeeper));
        PulsarService pulsar = mock(PulsarService.class);
        when(pulsar.getConfig()).thenReturn(config);
        when(pulsar.getBookKeeperClientFactory()).thenReturn(factory);
        when(pulsar.getLocalMetadataStore()).thenReturn(null);
        when(pulsar.getIoEventLoopGroup()).thenReturn(null);
        when(pulsar.getExecutor()).thenReturn(executor);

        topic = mock(PersistentTopic.class);
        when(topic.getName()).thenReturn(TOPIC_NAME);
        router = new TopicDelayRouter(topic, pulsar);
        router.start();
    }

    @AfterMethod
    void tearDown() {
        router.close();
        executor.shutdownNow();
    }

    @Test
    public void appendReturnsTheDelayStoragePosition() throws Exception {
        CompletableFuture<long[]> position =
                router.append(System.currentTimeMillis() + 60_000, payload("hello"));

        long[] messageId = position.get(5, TimeUnit.SECONDS);
        assertThat(messageId).containsExactly(1L, 0L);
        assertThat(router.routedEntries()).isEqualTo(1);
        assertThat(ledgers).containsKey(1L);
    }

    @Test
    public void rewritesWhenDue() {
        router.append(System.currentTimeMillis() + 300, payload("hello"));

        ArgumentCaptor<ByteBuf> body = ArgumentCaptor.forClass(ByteBuf.class);
        verify(topic, timeout(8000)).publishMessage(body.capture(), any());
        assertThat(bodyAsString(body.getValue())).isEqualTo("hello");
        assertThat(router.rewrittenEntries()).isEqualTo(1);
    }

    @Test
    public void holdsBackUntilDueThenDelivers() throws Exception {
        router.append(System.currentTimeMillis() + 1500, payload("later"));

        // Well past several ticks while nothing is due: no rewrite may happen early.
        Thread.sleep(700);
        verify(topic, never()).publishMessage(any(), any());

        // Once due, delivery happens with no further producer action.
        verify(topic, timeout(8000)).publishMessage(any(), any());
    }

    @Test
    public void heldBackRecordRearmsTheTimerAtItsOwnDeliverAt() {
        long now = System.currentTimeMillis();
        router.append(now + 200, payload("first"));
        router.append(now + 2500, payload("second"));

        ArgumentCaptor<ByteBuf> first = ArgumentCaptor.forClass(ByteBuf.class);
        verify(topic, timeout(5000)).publishMessage(first.capture(), any());
        assertThat(bodyAsString(first.getValue())).isEqualTo("first");

        // The second record is held back by the first round; the timer must re-arm at its
        // deliverAt — not stay parked until an unrelated append wakes the router.
        ArgumentCaptor<ByteBuf> all = ArgumentCaptor.forClass(ByteBuf.class);
        verify(topic, timeout(10000).times(2)).publishMessage(all.capture(), any());
        assertThat(all.getAllValues())
                .extracting(this::bodyAsString)
                .containsExactly("first", "second");
    }

    @Test
    public void earlierAppendMovesTheFiringEarlier() {
        long now = System.currentTimeMillis();
        router.append(now + 250, payload("early"));
        router.append(now + 5000, payload("late"));

        ArgumentCaptor<ByteBuf> bodies = ArgumentCaptor.forClass(ByteBuf.class);
        verify(topic, timeout(5000)).publishMessage(bodies.capture(), any());
        assertThat(bodyAsString(bodies.getValue())).isEqualTo("early");
    }

    /**
     * Within a window the drain follows arrival order, so a not-due record at the head holds
     * back a later-arriving record that is already due — its delivery waits for the head and the
     * lateness is bounded by the window width. The alternative, skipping past the head, needs a
     * per-entry rewrite index and re-reads of the held prefix every round, which is the very
     * index the write-routing design exists to avoid.
     */
    @Test
    public void notDueHeadHoldsBackALaterDueRecordWithinTheWindow() throws Exception {
        long now = System.currentTimeMillis();
        router.append(now + 2000, payload("lateArrival"));
        router.append(now + 100, payload("earlyArrival"));

        Thread.sleep(700);
        verify(topic, never()).publishMessage(any(), any());

        ArgumentCaptor<ByteBuf> all = ArgumentCaptor.forClass(ByteBuf.class);
        verify(topic, timeout(10000).times(2)).publishMessage(all.capture(), any());
        assertThat(all.getAllValues())
                .extracting(this::bodyAsString)
                .containsExactly("lateArrival", "earlyArrival");
    }

    @Test
    public void windowsAreSixtySecondFloors() {
        assertThat(TopicDelayRouter.windowOf(119_999)).isEqualTo(60_000);
        assertThat(TopicDelayRouter.windowOf(120_000)).isEqualTo(120_000);
        assertThat(TopicDelayRouter.windowOf(-1)).isEqualTo(-60_000);
    }

    @Test
    public void closeCancelsTheTimerAndDeletesTheStorage() throws Exception {
        router.append(System.currentTimeMillis() + 300, payload("doomed"));

        router.close();

        Thread.sleep(1000);
        verify(topic, never()).publishMessage(any(), any());
        assertThat(deletedLedgers).contains(1L);
    }

    private CompletableFuture<WriteHandle> newWriteHandle() {
        long ledgerId = nextLedgerId.incrementAndGet();
        ledgers.put(ledgerId, new CopyOnWriteArrayList<>());
        WriteHandle handle = mock(WriteHandle.class);
        AtomicLong lastAddConfirmed = new AtomicLong(-1);
        when(handle.getId()).thenReturn(ledgerId);
        when(handle.getLastAddConfirmed()).thenAnswer(inv -> lastAddConfirmed.get());
        when(handle.appendAsync(any(ByteBuf.class))).thenAnswer(inv -> {
            ByteBuf framed = inv.getArgument(0);
            byte[] copy = new byte[framed.readableBytes()];
            framed.getBytes(framed.readerIndex(), copy);
            List<byte[]> store = ledgers.get(ledgerId);
            long entryId = store.size();
            store.add(copy);
            lastAddConfirmed.set(entryId);
            return CompletableFuture.completedFuture(entryId);
        });
        when(handle.closeAsync()).thenReturn(CompletableFuture.completedFuture(null));
        return CompletableFuture.completedFuture(handle);
    }

    private ReadHandle newReadHandle(long ledgerId) {
        ReadHandle readHandle = mock(ReadHandle.class);
        when(readHandle.readAsync(anyLong(), anyLong())).thenAnswer(inv -> {
            long from = inv.getArgument(0);
            long to = inv.getArgument(1);
            List<byte[]> store = ledgers.getOrDefault(ledgerId, List.of());
            List<LedgerEntry> entries = new ArrayList<>();
            for (long entryId = from; entryId <= to && entryId < store.size(); entryId++) {
                entries.add(new FakeLedgerEntry(ledgerId, entryId, store.get((int) entryId)));
            }
            return CompletableFuture.completedFuture(new FakeLedgerEntries(entries));
        });
        return readHandle;
    }

    private static ByteBuf payload(String value) {
        return Unpooled.wrappedBuffer(value.getBytes(StandardCharsets.UTF_8));
    }

    private String bodyAsString(ByteBuf body) {
        byte[] bytes = new byte[body.readableBytes()];
        body.getBytes(body.readerIndex(), bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }

    private static final class FakeLedgerEntry implements LedgerEntry {
        private final long ledgerId;
        private final long entryId;
        private final byte[] framed;

        FakeLedgerEntry(long ledgerId, long entryId, byte[] framed) {
            this.ledgerId = ledgerId;
            this.entryId = entryId;
            this.framed = framed;
        }

        @Override
        public long getLedgerId() {
            return ledgerId;
        }

        @Override
        public long getEntryId() {
            return entryId;
        }

        @Override
        public long getLength() {
            return framed.length;
        }

        @Override
        public byte[] getEntryBytes() {
            return framed.clone();
        }

        @Override
        public java.nio.ByteBuffer getEntryNioBuffer() {
            return java.nio.ByteBuffer.wrap(framed);
        }

        @Override
        public ByteBuf getEntryBuffer() {
            return Unpooled.wrappedBuffer(framed);
        }

        @Override
        public LedgerEntry duplicate() {
            return new FakeLedgerEntry(ledgerId, entryId, framed);
        }

        @Override
        public void close() {
            // No-op: the buffer is an independent copy, not a pooled BK buffer.
        }
    }

    private record FakeLedgerEntries(List<LedgerEntry> entries) implements LedgerEntries {

        @Override
        public LedgerEntry getEntry(long entryId) {
            return entries.stream()
                    .filter(e -> e.getEntryId() == entryId)
                    .findFirst().orElse(null);
        }

        @Override
        public java.util.Iterator<LedgerEntry> iterator() {
            return entries.iterator();
        }

        @Override
        public void close() {
            // No-op: see FakeLedgerEntry.close.
        }
    }
}
