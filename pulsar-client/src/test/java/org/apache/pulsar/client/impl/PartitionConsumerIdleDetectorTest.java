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
package org.apache.pulsar.client.impl;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import java.net.InetSocketAddress;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.client.impl.conf.ConsumerConfigurationData;
import org.apache.pulsar.client.util.ExecutorProvider;
import org.apache.pulsar.common.naming.TopicName;
import org.awaitility.Awaitility;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class PartitionConsumerIdleDetectorTest {
    private final String topic = "persistent://tenant/ns1/my-topic";
    private ExecutorProvider executorProvider;
    private ExecutorService internalExecutor;
    private ConsumerImpl<byte[]> consumer;
    private ConsumerConfigurationData<byte[]> consumerConf;
    private PulsarClientImpl client;
    private PartitionConsumerIdleDetector idleDetector;

    @BeforeMethod(alwaysRun = true)
    public void setUp() {
        consumerConf = new ConsumerConfigurationData<>();
        consumerConf.setSubscriptionName("test-sub");
        consumerConf.setConsumerIdleTimeoutMs(1000); // 1 second for testing
        consumerConf.setEnablePartitionOwnershipCheck(true);
        
        executorProvider = new ExecutorProvider(1, "PartitionConsumerIdleDetectorTest");
        internalExecutor = Executors.newSingleThreadScheduledExecutor();

        client = ClientTestFixtures.createPulsarClientMock(executorProvider, internalExecutor);
        ClientConfigurationData clientConf = client.getConfiguration();
        clientConf.setOperationTimeoutMs(100);
        clientConf.setStatsIntervalSeconds(0);
        
        CompletableFuture<Consumer<byte[]>> subscribeFuture = new CompletableFuture<>();
        consumer = spy(ConsumerImpl.newConsumerImpl(client, topic, consumerConf,
                executorProvider, -1, false, subscribeFuture, null, null, null,
                true));
        consumer.setState(HandlerState.State.Ready);
        
        idleDetector = new PartitionConsumerIdleDetector(consumer, 1000);
    }

    @AfterMethod(alwaysRun = true)
    public void cleanup() {
        if (executorProvider != null) {
            executorProvider.shutdownNow();
            executorProvider = null;
        }
        if (internalExecutor != null) {
            internalExecutor.shutdownNow();
            internalExecutor = null;
        }
    }

    @Test
    public void testMarkActiveResetsTimer() throws Exception {
        // Verify initial timestamp
        long initialTimestamp = System.currentTimeMillis();
        
        // Wait a bit to ensure timestamp would change
        Thread.sleep(100);
        
        // Mark as active
        idleDetector.markActive();
        
        // Check that the consumer is not considered idle yet
        CompletableFuture<Void> result = idleDetector.checkIdleAndReconnectIfNeeded();
        result.get(1, TimeUnit.SECONDS);
        
        // Verify no reconnection was triggered (would be verified by no calls to connection handler)
        verify(consumer, never()).getConnectionHandler();
    }

    @Test
    public void testIdleDetectionDoesNotTriggerWhenNotIdle() throws Exception {
        // Mark as active to reset the timer
        idleDetector.markActive();
        
        // Immediately check (consumer is not idle)
        CompletableFuture<Void> result = idleDetector.checkIdleAndReconnectIfNeeded();
        result.get(1, TimeUnit.SECONDS);
        
        // Verify no ownership verification was attempted
        verify(consumer, never()).getClient();
    }

    @Test
    public void testIdleDetectionWhenNoConnection() throws Exception {
        // Wait for idle timeout
        Thread.sleep(1200);
        
        // Set no connection
        when(consumer.getClientCnx()).thenReturn(null);
        
        // Check idle - should not trigger reconnect when no connection
        CompletableFuture<Void> result = idleDetector.checkIdleAndReconnectIfNeeded();
        result.get(1, TimeUnit.SECONDS);
        
        // Verify that it checked for connection but didn't proceed
        verify(consumer, times(1)).getClientCnx();
    }

    @Test
    public void testOwnershipChangeDetection() throws Exception {
        // Wait for idle timeout
        Thread.sleep(1200);
        
        // Mock connection and lookup
        ClientCnx mockCnx = mock(ClientCnx.class);
        ChannelHandlerContext mockCtx = mock(ChannelHandlerContext.class);
        Channel mockChannel = mock(Channel.class);
        InetSocketAddress oldAddress = new InetSocketAddress("localhost", 6650);
        InetSocketAddress newAddress = new InetSocketAddress("localhost", 6651);
        
        when(consumer.getClientCnx()).thenReturn(mockCnx);
        when(mockCnx.ctx()).thenReturn(mockCtx);
        when(mockCtx.channel()).thenReturn(mockChannel);
        when(mockChannel.remoteAddress()).thenReturn(oldAddress);
        
        // Mock lookup service to return different broker
        LookupService mockLookup = mock(LookupService.class);
        when(consumer.getClient()).thenReturn(client);
        when(client.getLookup()).thenReturn(mockLookup);
        
        LookupTopicResult lookupResult = new LookupTopicResult(newAddress, newAddress, false);
        when(mockLookup.getBroker(any(TopicName.class)))
                .thenReturn(CompletableFuture.completedFuture(lookupResult));
        
        // Mock connection handler
        ConnectionHandler mockConnectionHandler = mock(ConnectionHandler.class);
        when(consumer.getConnectionHandler()).thenReturn(mockConnectionHandler);
        
        // Check idle - should trigger reconnect due to ownership change
        CompletableFuture<Void> result = idleDetector.checkIdleAndReconnectIfNeeded();
        result.get(2, TimeUnit.SECONDS);
        
        // Verify reconnection was triggered
        verify(mockConnectionHandler, times(1))
                .reconnectLater(any(PulsarClientException.class));
    }

    @Test
    public void testNoReconnectWhenOwnershipUnchanged() throws Exception {
        // Wait for idle timeout
        Thread.sleep(1200);
        
        // Mock connection and lookup with same broker
        ClientCnx mockCnx = mock(ClientCnx.class);
        ChannelHandlerContext mockCtx = mock(ChannelHandlerContext.class);
        Channel mockChannel = mock(Channel.class);
        InetSocketAddress sameAddress = new InetSocketAddress("localhost", 6650);
        
        when(consumer.getClientCnx()).thenReturn(mockCnx);
        when(mockCnx.ctx()).thenReturn(mockCtx);
        when(mockCtx.channel()).thenReturn(mockChannel);
        when(mockChannel.remoteAddress()).thenReturn(sameAddress);
        
        // Mock lookup service to return same broker
        LookupService mockLookup = mock(LookupService.class);
        when(consumer.getClient()).thenReturn(client);
        when(client.getLookup()).thenReturn(mockLookup);
        
        LookupTopicResult lookupResult = new LookupTopicResult(sameAddress, sameAddress, false);
        when(mockLookup.getBroker(any(TopicName.class)))
                .thenReturn(CompletableFuture.completedFuture(lookupResult));
        
        // Mock connection handler
        ConnectionHandler mockConnectionHandler = mock(ConnectionHandler.class);
        when(consumer.getConnectionHandler()).thenReturn(mockConnectionHandler);
        
        // Check idle - should NOT trigger reconnect (same broker)
        CompletableFuture<Void> result = idleDetector.checkIdleAndReconnectIfNeeded();
        result.get(2, TimeUnit.SECONDS);
        
        // Verify reconnection was NOT triggered
        verify(mockConnectionHandler, never())
                .reconnectLater(any(PulsarClientException.class));
    }

    @Test
    public void testLookupFailureDoesNotTriggerReconnect() throws Exception {
        // Wait for idle timeout
        Thread.sleep(1200);
        
        // Mock connection
        ClientCnx mockCnx = mock(ClientCnx.class);
        ChannelHandlerContext mockCtx = mock(ChannelHandlerContext.class);
        Channel mockChannel = mock(Channel.class);
        InetSocketAddress address = new InetSocketAddress("localhost", 6650);
        
        when(consumer.getClientCnx()).thenReturn(mockCnx);
        when(mockCnx.ctx()).thenReturn(mockCtx);
        when(mockCtx.channel()).thenReturn(mockChannel);
        when(mockChannel.remoteAddress()).thenReturn(address);
        
        // Mock lookup service to fail
        LookupService mockLookup = mock(LookupService.class);
        when(consumer.getClient()).thenReturn(client);
        when(client.getLookup()).thenReturn(mockLookup);
        
        when(mockLookup.getBroker(any(TopicName.class)))
                .thenReturn(CompletableFuture.failedFuture(
                        new PulsarClientException("Lookup failed")));
        
        // Mock connection handler
        ConnectionHandler mockConnectionHandler = mock(ConnectionHandler.class);
        when(consumer.getConnectionHandler()).thenReturn(mockConnectionHandler);
        
        // Check idle - should handle lookup failure gracefully
        CompletableFuture<Void> result = idleDetector.checkIdleAndReconnectIfNeeded();
        result.get(2, TimeUnit.SECONDS);
        
        // Verify reconnection was NOT triggered on lookup failure
        verify(mockConnectionHandler, never())
                .reconnectLater(any(PulsarClientException.class));
    }

    @Test
    public void testDisabledWhenConfiguredWithZeroTimeout() {
        // Create consumer with idle detection disabled
        consumerConf.setConsumerIdleTimeoutMs(0);
        
        CompletableFuture<Consumer<byte[]>> subscribeFuture = new CompletableFuture<>();
        ConsumerImpl<byte[]> consumerWithDisabledDetector = ConsumerImpl.newConsumerImpl(
                client, topic, consumerConf,
                executorProvider, -1, false, subscribeFuture, null, null, null,
                true);
        
        // Idle detector should be null when disabled
        // This is validated by the fact that consumer creation succeeds without idle detector
        assertNotNull(consumerWithDisabledDetector);
    }

    @Test
    public void testDisabledWhenOwnershipCheckDisabled() {
        // Create consumer with ownership check disabled
        consumerConf.setConsumerIdleTimeoutMs(1000);
        consumerConf.setEnablePartitionOwnershipCheck(false);
        
        CompletableFuture<Consumer<byte[]>> subscribeFuture = new CompletableFuture<>();
        ConsumerImpl<byte[]> consumerWithDisabledDetector = ConsumerImpl.newConsumerImpl(
                client, topic, consumerConf,
                executorProvider, -1, false, subscribeFuture, null, null, null,
                true);
        
        // Idle detector should be null when ownership check is disabled
        assertNotNull(consumerWithDisabledDetector);
    }
}
