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
package org.apache.pulsar.broker.delayed;

import org.apache.bookkeeper.mledger.AsyncCallbacks;

/**
 * A delayed delivery tracker that reads the due entries — the serialized message bodies — itself,
 * instead of reporting positions for the dispatcher to replay from the data ledgers. A tracker
 * implements this interface when it holds a copy of the delayed entries (e.g. in a journal) and
 * can serve them with better IO patterns than position-based replay.
 *
 * <p>The dispatcher uses this capability as its preferred dispatch path: when the tracker
 * implements this interface, the dispatcher calls {@link #readDueEntries} instead of
 * {@link DelayedDeliveryTracker#getScheduledMessages} followed by a cursor replay. Entries are
 * served wrapped in their original positions, so acknowledgements, cursors and redelivery
 * bookkeeping are unchanged.
 */
public interface DueEntryReader {

    /**
     * Serves one dispatch round of due entries, completing the callback exactly once with the
     * entries in position order. A reader that cannot serve some due entries itself (a failed or
     * still-in-flight copy) should replay them from the data ledgers through the cursor and merge
     * the results into the same completion; a read failure should fail the callback so the
     * dispatcher retries the round.
     *
     * @param callback    the dispatcher's read callback
     * @param ctx         the caller's context, passed through to the callback
     * @param maxMessages the maximum number of entries to serve in this round
     */
    void readDueEntries(AsyncCallbacks.ReadEntriesCallback callback, Object ctx, int maxMessages);
}
