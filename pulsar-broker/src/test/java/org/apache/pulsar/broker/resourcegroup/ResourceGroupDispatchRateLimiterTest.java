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
package org.apache.pulsar.broker.resourcegroup;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.pulsar.broker.qos.AsyncTokenBucket;
import org.apache.pulsar.broker.resourcegroup.ResourceGroup.BytesAndMessagesCount;
import org.apache.pulsar.broker.service.BrokerTestBase;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.common.naming.NamespaceName;
import org.awaitility.Awaitility;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class ResourceGroupDispatchRateLimiterTest extends BrokerTestBase {

    @BeforeClass
    @Override
    protected void setup() throws Exception {
        super.baseSetup();
    }

    @AfterClass(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test
    public void testEmptyResourceDispatchRateLimiter() {
        org.apache.pulsar.common.policies.data.ResourceGroup emptyResourceGroup =
                new org.apache.pulsar.common.policies.data.ResourceGroup();
        ResourceGroupDispatchLimiter resourceGroupDispatchLimiter = new ResourceGroupDispatchLimiter(emptyResourceGroup);
        assertFalse(resourceGroupDispatchLimiter.isDispatchRateLimitingEnabled());

        assertEquals(resourceGroupDispatchLimiter.getAvailableDispatchRateLimitOnMsg(), -1L);
        assertEquals(resourceGroupDispatchLimiter.getDispatchRateOnMsg(), -1L);

        assertEquals(resourceGroupDispatchLimiter.getAvailableDispatchRateLimitOnByte(), -1L);
        assertEquals(resourceGroupDispatchLimiter.getDispatchRateOnByte(), -1L);
    }

    @Test
    public void testResourceDispatchRateLimiterOnMsgs() {
        org.apache.pulsar.common.policies.data.ResourceGroup resourceGroup =
                new org.apache.pulsar.common.policies.data.ResourceGroup();
        resourceGroup.setDispatchRateInMsgs(10);
        ResourceGroupDispatchLimiter resourceGroupDispatchLimiter = new ResourceGroupDispatchLimiter(resourceGroup);
        assertTrue(resourceGroupDispatchLimiter.isDispatchRateLimitingEnabled());

        assertEquals(resourceGroupDispatchLimiter.getAvailableDispatchRateLimitOnMsg(), (long) resourceGroup.getDispatchRateInMsgs());
        assertEquals(resourceGroupDispatchLimiter.getDispatchRateOnMsg(), (long) resourceGroup.getDispatchRateInMsgs());

        assertEquals(resourceGroupDispatchLimiter.getAvailableDispatchRateLimitOnByte(), -1L);
        assertEquals(resourceGroupDispatchLimiter.getDispatchRateOnByte(), -1L);
    }

    @Test
    public void testResourceDispatchRateLimiterOnBytes() {
        org.apache.pulsar.common.policies.data.ResourceGroup resourceGroup =
                new org.apache.pulsar.common.policies.data.ResourceGroup();
        resourceGroup.setDispatchRateInBytes(20L);
        ResourceGroupDispatchLimiter resourceGroupDispatchLimiter = new ResourceGroupDispatchLimiter(resourceGroup);
        assertTrue(resourceGroupDispatchLimiter.isDispatchRateLimitingEnabled());

        assertEquals(resourceGroupDispatchLimiter.getAvailableDispatchRateLimitOnMsg(), -1);
        assertEquals(resourceGroupDispatchLimiter.getDispatchRateOnMsg(), -1);

        assertEquals(resourceGroupDispatchLimiter.getAvailableDispatchRateLimitOnByte(), resourceGroup.getDispatchRateInBytes());
        assertEquals(resourceGroupDispatchLimiter.getDispatchRateOnByte(), resourceGroup.getDispatchRateInBytes());
    }

    @Test
    public void testUpdateResourceGroupDispatchRateLimiter() {
        org.apache.pulsar.common.policies.data.ResourceGroup resourceGroup =
                new org.apache.pulsar.common.policies.data.ResourceGroup();
        resourceGroup.setDispatchRateInMsgs(10);
        resourceGroup.setDispatchRateInBytes(100L);

        ResourceGroupDispatchLimiter resourceGroupDispatchLimiter = new ResourceGroupDispatchLimiter(resourceGroup);
        AsyncTokenBucket dispatchRateLimiterOnByte = resourceGroupDispatchLimiter.getDispatchRateLimiterOnByte();
        assertNotNull(dispatchRateLimiterOnByte);
        AsyncTokenBucket dispatchRateLimiterOnMessage = resourceGroupDispatchLimiter.getDispatchRateLimiterOnMessage();
        assertNotNull(dispatchRateLimiterOnMessage);

        resourceGroupDispatchLimiter.update(resourceGroup);
        assertEquals(resourceGroupDispatchLimiter.getDispatchRateLimiterOnByte(), dispatchRateLimiterOnByte);
        assertEquals(resourceGroupDispatchLimiter.getDispatchRateLimiterOnMessage(), dispatchRateLimiterOnMessage);

        BytesAndMessagesCount bytesAndMessagesCount = new BytesAndMessagesCount();
        bytesAndMessagesCount.messages = 20;
        bytesAndMessagesCount.bytes = 200;
        resourceGroupDispatchLimiter.update(bytesAndMessagesCount);

        assertEquals(resourceGroupDispatchLimiter.getAvailableDispatchRateLimitOnByte(), bytesAndMessagesCount.bytes);
        assertEquals(resourceGroupDispatchLimiter.getDispatchRateOnByte(), bytesAndMessagesCount.bytes);
        assertEquals(resourceGroupDispatchLimiter.getAvailableDispatchRateLimitOnMsg(), bytesAndMessagesCount.messages);
        assertEquals(resourceGroupDispatchLimiter.getDispatchRateOnMsg(), bytesAndMessagesCount.messages);
    }

    @Test
    public void testCreateResourceGroupDispatchRateLimiterByAdmin() throws PulsarAdminException {
        org.apache.pulsar.common.policies.data.ResourceGroup rgData =
                new org.apache.pulsar.common.policies.data.ResourceGroup();
        rgData.setDispatchRateInMsgs(10);
        rgData.setDispatchRateInBytes(100L);

        String rgn = "rg-1";
        admin.resourcegroups().createResourceGroup(rgn, rgData);

        Awaitility.await().untilAsserted(() -> {
            assertNotNull(admin.resourcegroups().getResourceGroup(rgn));
            ResourceGroup rg = pulsar.getResourceGroupServiceManager().resourceGroupGet(rgn);
            assertNotNull(rg);
            ResourceGroupDispatchLimiter resourceGroupDispatchLimiter = rg.getResourceGroupDispatchLimiter();
            assertNotNull(resourceGroupDispatchLimiter);
            assertEquals(resourceGroupDispatchLimiter.getAvailableDispatchRateLimitOnMsg(), (long) rgData.getDispatchRateInMsgs());
            assertEquals(resourceGroupDispatchLimiter.getDispatchRateOnMsg(), (long) rgData.getDispatchRateInMsgs());
            assertEquals(resourceGroupDispatchLimiter.getAvailableDispatchRateLimitOnByte(), rgData.getDispatchRateInBytes());
            assertEquals(resourceGroupDispatchLimiter.getDispatchRateOnByte(), rgData.getDispatchRateInBytes());
        });
    }
}
