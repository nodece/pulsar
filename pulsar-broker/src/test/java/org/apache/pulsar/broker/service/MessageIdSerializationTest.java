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
package org.apache.pulsar.broker.service;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

import java.io.IOException;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.apache.pulsar.client.impl.TopicMessageIdImpl;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class MessageIdSerializationTest {

    @Test
    public void testProtobufSerialization1() throws Exception {
        MessageId id = new MessageIdImpl(1, 2, 3);
        byte[] serializedId = id.toByteArray();
        assertEquals(MessageId.fromByteArray(serializedId), id);
    }

    @Test
    public void testProtobufSerialization2() throws Exception {
        MessageId id = new MessageIdImpl(1, 2, -1);
        byte[] serializedId = id.toByteArray();
        assertEquals(MessageId.fromByteArray(serializedId), id);
    }

    @Test
    public void testProtobufSerializationWithTopic() throws Exception {
        MessageId id = new MessageIdImpl(1, 2, -1);
        byte[] serializedId = id.toByteArray();
        MessageId actualMessageId = MessageId.fromByteArrayWithTopic(serializedId, "topic-1");
        if (!(actualMessageId instanceof TopicMessageIdImpl)) {
            fail("actualMessageId should be " + TopicMessageIdImpl.class);
        }
        assertEquals(actualMessageId, id);
    }

    @Test
    public void testProtobufSerializationWithPartitionTopic() throws Exception {
        MessageId id = new MessageIdImpl(1, 2, -1);
        byte[] serializedId = id.toByteArray();
        MessageId actualMessageId = MessageId.fromByteArrayWithTopic(serializedId, "topic-partition-1");
        if (!(actualMessageId instanceof TopicMessageIdImpl)) {
            fail("actualMessageId should be " + TopicMessageIdImpl.class);
        }
        assertEquals(actualMessageId, id);
    }

    @Test(expectedExceptions = NullPointerException.class)
    public void testProtobufSerializationNull() throws Exception {
        MessageId.fromByteArray(null);
    }

    @Test(expectedExceptions = IOException.class)
    void testProtobufSerializationEmpty() throws Exception {
        MessageId.fromByteArray(new byte[0]);
    }
}
