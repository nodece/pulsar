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
package org.apache.pulsar.broker.service;

import static org.awaitility.Awaitility.await;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import com.google.common.collect.Sets;
import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.BrokerTestUtil;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.common.naming.TopicName;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Slf4j
@Test(groups = "broker")
public class OneWayReplicatorWithSeparateZKTest extends OneWayReplicatorTestBase {

    @Override
    @BeforeMethod(alwaysRun = true, timeOut = 300000)
    public void setup() throws Exception {
        usingGlobalZK = false;
        super.setup();
    }

    @Override
    @AfterMethod(alwaysRun = true, timeOut = 300000)
    public void cleanup() throws Exception {
        super.cleanup();
    }

    @Test
    public void testStartReplicatorWhenTopicPartitionsIsDifferent() throws Exception {
        final String topicName = BrokerTestUtil.newUniqueName("persistent://" + replicatedNamespace + "/tp_");

        // Disable replicator
        admin1.namespaces().setNamespaceReplicationClusters(replicatedNamespace, Collections.singleton(cluster1));
        admin2.namespaces().setNamespaceReplicationClusters(replicatedNamespace, Collections.singleton(cluster2));

        admin2.topics().createPartitionedTopic(topicName, 2);
        admin1.topics().createPartitionedTopic(topicName, 1);

        // Enable replicator
        admin1.namespaces().setNamespaceReplicationClusters(replicatedNamespace, Sets.newHashSet(cluster1, cluster2));

        String string = TopicName.get(topicName).getPartition(0).toString();
        Optional<Topic> topic1Optional = pulsar1.getBrokerService().getTopic(string, false).join();
        assertTrue(topic1Optional.isPresent());
        PersistentTopic topic1 = (PersistentTopic) topic1Optional.get();
        await().pollDelay(3, TimeUnit.SECONDS).untilAsserted(() -> {
            assertFalse(topic1.getReplicators().containsKey(cluster2));
        });
    }
}
