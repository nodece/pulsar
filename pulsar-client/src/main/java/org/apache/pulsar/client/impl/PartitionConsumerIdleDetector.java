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

import java.net.InetSocketAddress;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.PulsarClientException;

/**
 * Detects when a partition consumer becomes idle and triggers reconnection if topic ownership changed.
 * 
 * This helps prevent permanent message loss when:
 * - Topic ownership transfers to a different broker
 * - The old broker fails to send CommandCloseConsumer notification
 * - The consumer remains connected to a broker that no longer owns the topic
 */
@Slf4j
class PartitionConsumerIdleDetector {
    private final ConsumerImpl<?> consumer;
    private final long idleTimeoutMs;
    private final AtomicLong lastActivityTimestamp;

    PartitionConsumerIdleDetector(ConsumerImpl<?> consumer, long idleTimeoutMs) {
        this.consumer = consumer;
        this.idleTimeoutMs = idleTimeoutMs;
        this.lastActivityTimestamp = new AtomicLong(System.currentTimeMillis());
    }

    /**
     * Mark the consumer as active (message received or acknowledged).
     */
    void markActive() {
        lastActivityTimestamp.set(System.currentTimeMillis());
    }

    /**
     * Check if the consumer is idle and reconnect if topic ownership changed.
     * 
     * @return CompletableFuture that completes when the check is done
     */
    CompletableFuture<Void> checkIdleAndReconnectIfNeeded() {
        long idleDuration = System.currentTimeMillis() - lastActivityTimestamp.get();
        
        if (idleDuration < idleTimeoutMs) {
            // Not idle yet
            return CompletableFuture.completedFuture(null);
        }

        if (log.isDebugEnabled()) {
            log.debug("[{}][{}] Consumer idle for {}ms, verifying topic ownership",
                    consumer.getTopic(), consumer.getSubscription(), idleDuration);
        }

        return verifyTopicOwnership()
                .thenCompose(ownershipChanged -> {
                    if (ownershipChanged) {
                        log.info("[{}][{}] Topic ownership changed, reconnecting consumer",
                                consumer.getTopic(), consumer.getSubscription());
                        return reconnectWithCleanup();
                    } else {
                        if (log.isDebugEnabled()) {
                            log.debug("[{}][{}] Topic ownership unchanged, no reconnection needed",
                                    consumer.getTopic(), consumer.getSubscription());
                        }
                        return CompletableFuture.completedFuture(null);
                    }
                });
    }

    /**
     * Verify if topic ownership has changed by performing a lookup.
     * 
     * @return CompletableFuture<Boolean> true if ownership changed, false otherwise
     */
    private CompletableFuture<Boolean> verifyTopicOwnership() {
        // Get current broker address
        ClientCnx currentCnx = consumer.getClientCnx();
        if (currentCnx == null) {
            // No current connection, no need to check
            return CompletableFuture.completedFuture(false);
        }

        // Safely get the remote address and verify it's an InetSocketAddress
        if (!(currentCnx.ctx().channel().remoteAddress() instanceof InetSocketAddress)) {
            log.warn("[{}][{}] Remote address is not an InetSocketAddress, skipping ownership check",
                    consumer.getTopic(), consumer.getSubscription());
            return CompletableFuture.completedFuture(false);
        }

        InetSocketAddress currentBroker = (InetSocketAddress) currentCnx.ctx().channel().remoteAddress();

        // Perform topic lookup to find the current owner
        return consumer.getClient().getLookup().getBroker(consumer.getTopicName())
                .thenApply(lookupResult -> {
                    InetSocketAddress newBroker = lookupResult.getLogicalAddress();
                    
                    // Check for null to prevent NPE
                    if (newBroker == null) {
                        log.warn("[{}][{}] Lookup returned null logical address",
                                consumer.getTopic(), consumer.getSubscription());
                        return false;
                    }
                    
                    boolean changed = !currentBroker.equals(newBroker);
                    
                    if (changed) {
                        log.info("[{}][{}] Topic ownership changed: {} -> {}",
                                consumer.getTopic(), consumer.getSubscription(),
                                currentBroker, newBroker);
                    }
                    
                    return changed;
                })
                .exceptionally(ex -> {
                    log.warn("[{}][{}] Failed to verify topic ownership",
                            consumer.getTopic(), consumer.getSubscription(), ex);
                    // On lookup failure, don't trigger reconnection
                    return false;
                });
    }

    /**
     * Reconnect the consumer with comprehensive cleanup.
     * 
     * @return CompletableFuture that completes when reconnection is initiated
     */
    private CompletableFuture<Void> reconnectWithCleanup() {
        return CompletableFuture.runAsync(() -> {
            cleanupConsumerState();
        }, consumer.getInternalPinnedExecutor())
        .thenCompose(__ -> {
            // Trigger reconnection by calling reconnectLater on connection handler
            consumer.getConnectionHandler().reconnectLater(
                    new PulsarClientException("Topic ownership changed, reconnecting"));
            return CompletableFuture.completedFuture(null);
        });
    }

    /**
     * Clean up consumer state before reconnection.
     */
    private void cleanupConsumerState() {
        try {
            // 1. Clear unacked message tracker
            consumer.getUnAckedMessageTracker().clear();
            
            // 2. Clear pending ack queues in acknowledgment tracker
            consumer.getAcknowledgmentsGroupingTracker().flush();
            
            // 3. Clear batch message ack tracker (if exists)
            // The batch ack tracker is internal to acknowledgment tracker
            
            // 4. Increment consumer epoch to reject old acks
            consumer.incrementConsumerEpoch();
            
            log.info("[{}][{}] Cleaned up consumer state before reconnection",
                    consumer.getTopic(), consumer.getSubscription());
            
        } catch (Exception e) {
            log.error("[{}][{}] Error during cleanup before reconnection",
                    consumer.getTopic(), consumer.getSubscription(), e);
        }
    }
}
