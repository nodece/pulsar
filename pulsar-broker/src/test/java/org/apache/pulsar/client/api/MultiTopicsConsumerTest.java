/**
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

package org.apache.pulsar.client.api;

import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import com.google.common.collect.Lists;
import static org.testng.Assert.fail;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import lombok.Cleanup;
import org.apache.pulsar.broker.BrokerTestUtil;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.impl.ClientBuilderImpl;
import org.apache.pulsar.client.impl.MultiTopicsConsumerImpl;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.util.FutureUtil;
import org.mockito.AdditionalAnswers;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class MultiTopicsConsumerTest extends ProducerConsumerBase {
    private static final Logger log = LoggerFactory.getLogger(MultiTopicsConsumerTest.class);
    private ScheduledExecutorService internalExecutorServiceDelegate;

    @BeforeMethod(alwaysRun = true)
    @Override
    protected void setup() throws Exception {
        super.internalSetup();
        super.producerBaseSetup();
    }

    @AfterMethod(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Override
    protected void customizeNewPulsarClientBuilder(ClientBuilder clientBuilder) {
       clientBuilder.ioThreads(4).connectionsPerBroker(4);
    }

    // test that reproduces the issue https://github.com/apache/pulsar/issues/12024
    // where closing the consumer leads to an endless receive loop
    @Test
    public void testMultiTopicsConsumerCloses() throws Exception {
        String topicNameBase = "persistent://my-property/my-ns/my-topic-consumer-closes-";

        ClientConfigurationData conf = ((ClientBuilderImpl) PulsarClient.builder().serviceUrl(lookupUrl.toString()))
                .getClientConfigurationData();

        @Cleanup
        PulsarClientImpl client = new PulsarClientImpl(conf) {
            {
                ScheduledExecutorService internalExecutorService =
                        (ScheduledExecutorService) super.getScheduledExecutorProvider().getExecutor();
                internalExecutorServiceDelegate = mock(ScheduledExecutorService.class,
                        // a spy isn't used since that doesn't work for private classes, instead
                        // the mock delegatesTo an existing instance. A delegate is sufficient for verifying
                        // method calls on the interface.
                        Mockito.withSettings().defaultAnswer(AdditionalAnswers.delegatesTo(internalExecutorService)));
            }

            @Override
            public ExecutorService getInternalExecutorService() {
                return internalExecutorServiceDelegate;
            }
        };
        @Cleanup
        Producer<byte[]> producer1 = client.newProducer()
                .topic(topicNameBase + "1")
                .enableBatching(false)
                .create();
        @Cleanup
        Producer<byte[]> producer2 = client.newProducer()
                .topic(topicNameBase + "2")
                .enableBatching(false)
                .create();
        @Cleanup
        Producer<byte[]> producer3 = client.newProducer()
                .topic(topicNameBase + "3")
                .enableBatching(false)
                .create();

        Consumer<byte[]> consumer = client
                .newConsumer()
                .topics(Lists.newArrayList(topicNameBase + "1", topicNameBase + "2", topicNameBase + "3"))
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .receiverQueueSize(1)
                .subscriptionName(methodName)
                .subscribe();

        // wait for background tasks to start
        Thread.sleep(1000L);

        // when consumer is closed
        consumer.close();
        // give time for background tasks to execute
        Thread.sleep(1000L);

        // then verify that no scheduling operation has happened
        verify(internalExecutorServiceDelegate, times(0))
                .schedule(any(Runnable.class), anyLong(), any());
    }

    // test that reproduces the issue that PR https://github.com/apache/pulsar/pull/12456 fixes
    // where MultiTopicsConsumerImpl has a data race that causes out-of-order delivery of messages
    @Test
    public void testShouldMaintainOrderForIndividualTopicInMultiTopicsConsumer()
            throws PulsarAdminException, PulsarClientException, ExecutionException, InterruptedException,
            TimeoutException {
        String topicName = newTopicName();
        int numPartitions = 2;
        int numMessages = 100000;
        admin.topics().createPartitionedTopic(topicName, numPartitions);

        Producer<Long>[] producers = new Producer[numPartitions];

        for (int i = 0; i < numPartitions; i++) {
            producers[i] = pulsarClient.newProducer(Schema.INT64)
                    // produce to each partition directly so that order can be maintained in sending
                    .topic(topicName + "-partition-" + i)
                    .enableBatching(true)
                    .maxPendingMessages(30000)
                    .maxPendingMessagesAcrossPartitions(60000)
                    .batchingMaxMessages(10000)
                    .batchingMaxPublishDelay(5, TimeUnit.SECONDS)
                    .batchingMaxBytes(4 * 1024 * 1024)
                    .blockIfQueueFull(true)
                    .create();
        }

        @Cleanup
        Consumer<Long> consumer = pulsarClient
                .newConsumer(Schema.INT64)
                // consume on the partitioned topic
                .topic(topicName)
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .receiverQueueSize(numMessages)
                .subscriptionName(methodName)
                .subscribe();

        // produce sequence numbers to each partition topic
        long sequenceNumber = 1L;
        for (int i = 0; i < numMessages; i++) {
            for (Producer<Long> producer : producers) {
                producer.newMessage()
                        .value(sequenceNumber)
                        .sendAsync();
            }
            sequenceNumber++;
        }
        for (Producer<Long> producer : producers) {
            producer.close();
        }

        // receive and validate sequences in the partitioned topic
        Map<String, AtomicLong> receivedSequences = new HashMap<>();
        int receivedCount = 0;
        while (receivedCount < numPartitions * numMessages) {
            Message<Long> message = consumer.receiveAsync().get(5, TimeUnit.SECONDS);
            consumer.acknowledge(message);
            receivedCount++;
            AtomicLong receivedSequenceCounter =
                    receivedSequences.computeIfAbsent(message.getTopicName(), k -> new AtomicLong(1L));
            Assert.assertEquals(message.getValue().longValue(), receivedSequenceCounter.getAndIncrement());
        }
        Assert.assertEquals(numPartitions * numMessages, receivedCount);
    }

    @Test(invocationCount = 10, timeOut = 30000)
    public void testMultipleIOThreads() throws PulsarAdminException, PulsarClientException {
        final String topic = TopicName.get(newTopicName()).toString();
        final int numPartitions = 100;
        admin.topics().createPartitionedTopic(topic, numPartitions);
        for (int i = 0; i < 100; i++) {
            admin.topics().createNonPartitionedTopic(topic + "-" + i);
        }
        @Cleanup
        final Consumer<Integer> consumer = pulsarClient.newConsumer(Schema.INT32).topicsPattern(topic + ".*")
                .subscriptionName("sub").subscribe();
        assertTrue(consumer instanceof MultiTopicsConsumerImpl);
        assertTrue(consumer.isConnected());
    }

    @Test
    public void testSameTopics() throws Exception {
        final String topic1 = BrokerTestUtil.newUniqueName("public/default/tp");
        final String topic2 = "persistent://" + topic1;
        admin.topics().createNonPartitionedTopic(topic2);
        // Create consumer with two same topics.
        try {
            pulsarClient.newConsumer(Schema.INT32).topics(Arrays.asList(topic1, topic2))
                    .subscriptionName("s1").subscribe();
            fail("Do not allow use two same topics.");
        } catch (Exception e) {
            if (e instanceof PulsarClientException && e.getCause() != null) {
                e = (Exception) e.getCause();
            }
            Throwable unwrapEx = FutureUtil.unwrapCompletionException(e);
            assertTrue(unwrapEx instanceof IllegalArgumentException);
            assertTrue(e.getMessage().contains( "Subscription topics include duplicate items"
                    + " or invalid names"));
        }
        // cleanup.
        admin.topics().delete(topic2);
    }
}
