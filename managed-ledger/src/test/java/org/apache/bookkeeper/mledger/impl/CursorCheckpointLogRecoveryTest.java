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
package org.apache.bookkeeper.mledger.impl;

import static org.assertj.core.api.Assertions.assertThat;
import java.util.concurrent.TimeUnit;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.proto.CursorCheckpoint;
import org.apache.bookkeeper.mledger.proto.CursorCheckpointChunk;
import org.apache.bookkeeper.mledger.proto.CursorLogEntry;
import org.apache.bookkeeper.mledger.proto.PositionInfo;
import org.apache.bookkeeper.test.MockedBookKeeperTestCase;
import org.testng.annotations.Test;

public class CursorCheckpointLogRecoveryTest extends MockedBookKeeperTestCase {

    private LedgerHandle createLedger() throws Exception {
        return bkc.createLedger(BookKeeper.DigestType.MAC, new byte[0]);
    }

    private long appendRaw(LedgerHandle lh, byte[] data) throws Exception {
        return lh.append(data);
    }

    private byte[] wrapCheckpoint(CursorCheckpoint cp) {
        CursorLogEntry env = new CursorLogEntry();
        env.setCheckpoint().parseFrom(cp.toByteArray());
        return env.toByteArray();
    }

    private byte[] wrapChunkPart(int partIndex, int partCount, byte[] payload) {
        CursorLogEntry env = new CursorLogEntry();
        CursorCheckpointChunk chunk = env.setCheckpointChunk();
        chunk.setPartIndex(partIndex).setPartCount(partCount).setCheckpointBytes(payload);
        return env.toByteArray();
    }

    private byte[] wrapLegacyPositionInfo(long ledgerId, long entryId) {
        PositionInfo pi = new PositionInfo().setLedgerId(ledgerId).setEntryId(entryId);
        return pi.toByteArray();
    }

    private CursorCheckpoint makeCheckpoint(long mdLedgerId, long mdEntryId) {
        CursorCheckpoint cp = new CursorCheckpoint()
                .setMarkDeleteLedgerId(mdLedgerId)
                .setMarkDeleteEntryId(mdEntryId);
        cp.addAckState().setMsgLedgerId(mdLedgerId);
        return cp;
    }

    /**
     * Scenario: checkpoint → legacy → chunk(complete) → chunk(incomplete) → chunk(incomplete)
     *
     * Recovery reads last (incomplete) → scanBack → finds chunk(complete) → assembles.
     */
    @Test(timeOut = 30000)
    public void testScanBackFindsCompleteChunk() throws Exception {
        LedgerHandle lh = createLedger();
        CursorCheckpointLog writer = new CursorCheckpointLog(5 * 1024 * 1024);

        appendRaw(lh, wrapCheckpoint(makeCheckpoint(10, 0)));
        appendRaw(lh, wrapLegacyPositionInfo(10, 1));
        // Write a complete 2-part chunk
        byte[] cpBytes = makeCheckpoint(30, 5).toByteArray();
        int half = cpBytes.length / 2;
        byte[] part0 = new byte[half];
        byte[] part1 = new byte[cpBytes.length - half];
        System.arraycopy(cpBytes, 0, part0, 0, half);
        System.arraycopy(cpBytes, half, part1, 0, part1.length);
        appendRaw(lh, wrapChunkPart(0, 2, part0));
        appendRaw(lh, wrapChunkPart(1, 2, part1));
        // Two incomplete chunks after
        appendRaw(lh, wrapChunkPart(0, 3, new byte[100]));
        appendRaw(lh, wrapChunkPart(1, 3, new byte[100]));

        CursorCheckpointLog.RecoveredState state = writer.readLatest(lh).get(5, TimeUnit.SECONDS);
        assertThat(state.isLegacy()).isFalse();
        assertThat(state.checkpoint.getMarkDeleteLedgerId()).isEqualTo(30L);
    }

    /**
     * Scenario: checkpoint → legacy → chunk(incomplete)
     *
     * Recovery reads last (incomplete) → scanBack → finds legacy → returns legacy.
     */
    @Test(timeOut = 30000)
    public void testScanBackFallsBackToLegacy() throws Exception {
        LedgerHandle lh = createLedger();
        CursorCheckpointLog writer = new CursorCheckpointLog(5 * 1024 * 1024);

        appendRaw(lh, wrapCheckpoint(makeCheckpoint(10, 0)));
        appendRaw(lh, wrapLegacyPositionInfo(20, 5));
        // Incomplete chunk
        appendRaw(lh, wrapChunkPart(0, 3, new byte[100]));

        CursorCheckpointLog.RecoveredState state = writer.readLatest(lh).get(5, TimeUnit.SECONDS);
        assertThat(state.isLegacy()).isTrue();
        PositionInfo pi = new PositionInfo();
        pi.parseFrom(state.legacyBytes);
        assertThat(pi.getLedgerId()).isEqualTo(20L);
        assertThat(pi.getEntryId()).isEqualTo(5L);
    }

    /**
     * Scenario: only incomplete chunks → scanBack exhausts → error
     */
    @Test(timeOut = 30000)
    public void testScanBackExhausts() throws Exception {
        LedgerHandle lh = createLedger();
        CursorCheckpointLog writer = new CursorCheckpointLog(5 * 1024 * 1024);

        appendRaw(lh, wrapChunkPart(0, 3, new byte[100]));
        appendRaw(lh, wrapChunkPart(1, 3, new byte[100]));

        try {
            writer.readLatest(lh).get(5, TimeUnit.SECONDS);
            assertThat(false).as("Should have failed").isTrue();
        } catch (Exception e) {
            assertThat(e).hasCauseInstanceOf(ManagedLedgerException.class);
        }
    }

    /**
     * Scenario: last entry is legacy → returns directly (no scanBack).
     */
    @Test(timeOut = 30000)
    public void testLegacyAsLastEntry() throws Exception {
        LedgerHandle lh = createLedger();
        CursorCheckpointLog writer = new CursorCheckpointLog(5 * 1024 * 1024);

        appendRaw(lh, wrapCheckpoint(makeCheckpoint(10, 0)));
        appendRaw(lh, wrapLegacyPositionInfo(20, 5));

        CursorCheckpointLog.RecoveredState state = writer.readLatest(lh).get(5, TimeUnit.SECONDS);
        assertThat(state.isLegacy()).isTrue();
    }

    /**
     * Scenario: last entry is a valid checkpoint → returns directly.
     */
    @Test(timeOut = 30000)
    public void testCheckpointAsLastEntry() throws Exception {
        LedgerHandle lh = createLedger();
        CursorCheckpointLog writer = new CursorCheckpointLog(5 * 1024 * 1024);

        appendRaw(lh, wrapLegacyPositionInfo(10, 0));
        appendRaw(lh, wrapCheckpoint(makeCheckpoint(20, 5)));

        CursorCheckpointLog.RecoveredState state = writer.readLatest(lh).get(5, TimeUnit.SECONDS);
        assertThat(state.isLegacy()).isFalse();
        assertThat(state.checkpoint.getMarkDeleteLedgerId()).isEqualTo(20L);
    }

    /**
     * Scenario: old, chunk(complete as last entry).
     */
    @Test(timeOut = 30000)
    public void testCompleteChunkAsLastEntry() throws Exception {
        LedgerHandle lh = createLedger();
        CursorCheckpointLog writer = new CursorCheckpointLog(5 * 1024 * 1024);

        appendRaw(lh, wrapLegacyPositionInfo(10, 0));
        byte[] cpBytes = makeCheckpoint(40, 7).toByteArray();
        int half = cpBytes.length / 2;
        byte[] part0 = new byte[half];
        byte[] part1 = new byte[cpBytes.length - half];
        System.arraycopy(cpBytes, 0, part0, 0, half);
        System.arraycopy(cpBytes, half, part1, 0, part1.length);
        appendRaw(lh, wrapChunkPart(0, 2, part0));
        appendRaw(lh, wrapChunkPart(1, 2, part1));

        CursorCheckpointLog.RecoveredState state = writer.readLatest(lh).get(5, TimeUnit.SECONDS);
        assertThat(state.isLegacy()).isFalse();
        assertThat(state.checkpoint.getMarkDeleteLedgerId()).isEqualTo(40L);
    }

    /**
     * Scenario: chunk(complete), chunk(incomplete).
     */
    @Test(timeOut = 30000)
    public void testCompleteThenIncompleteChunkFallsBackToPreviousCompleteChunk() throws Exception {
        LedgerHandle lh = createLedger();
        CursorCheckpointLog writer = new CursorCheckpointLog(5 * 1024 * 1024);

        byte[] cpBytes = makeCheckpoint(50, 9).toByteArray();
        int half = cpBytes.length / 2;
        byte[] part0 = new byte[half];
        byte[] part1 = new byte[cpBytes.length - half];
        System.arraycopy(cpBytes, 0, part0, 0, half);
        System.arraycopy(cpBytes, half, part1, 0, part1.length);
        appendRaw(lh, wrapChunkPart(0, 2, part0));
        appendRaw(lh, wrapChunkPart(1, 2, part1));
        appendRaw(lh, wrapChunkPart(0, 3, new byte[64]));

        CursorCheckpointLog.RecoveredState state = writer.readLatest(lh).get(5, TimeUnit.SECONDS);
        assertThat(state.isLegacy()).isFalse();
        assertThat(state.checkpoint.getMarkDeleteLedgerId()).isEqualTo(50L);
    }

    /**
     * Scenario: new, chunk(incomplete).
     */
    @Test(timeOut = 30000)
    public void testNewThenIncompleteChunkFallsBackToNewCheckpoint() throws Exception {
        LedgerHandle lh = createLedger();
        CursorCheckpointLog writer = new CursorCheckpointLog(5 * 1024 * 1024);

        appendRaw(lh, wrapCheckpoint(makeCheckpoint(60, 11)));
        appendRaw(lh, wrapChunkPart(0, 3, new byte[64]));

        CursorCheckpointLog.RecoveredState state = writer.readLatest(lh).get(5, TimeUnit.SECONDS);
        assertThat(state.isLegacy()).isFalse();
        assertThat(state.checkpoint.getMarkDeleteLedgerId()).isEqualTo(60L);
    }

    /**
     * Boundary: recovery keeps scanning back, but only within MAX_SCAN_BACK (1000).
     */
    @Test(timeOut = 30000)
    public void testScanBackLimitBoundary() throws Exception {
        LedgerHandle lh = createLedger();
        CursorCheckpointLog writer = new CursorCheckpointLog(5 * 1024 * 1024);

        appendRaw(lh, wrapCheckpoint(makeCheckpoint(70, 13)));
        for (int i = 0; i < 1000; i++) {
            appendRaw(lh, wrapChunkPart(0, 3, new byte[16]));
        }
        CursorCheckpointLog.RecoveredState withinLimit = writer.readLatest(lh).get(5, TimeUnit.SECONDS);
        assertThat(withinLimit.isLegacy()).isFalse();
        assertThat(withinLimit.checkpoint.getMarkDeleteLedgerId()).isEqualTo(70L);

        appendRaw(lh, wrapChunkPart(0, 3, new byte[16]));
        try {
            writer.readLatest(lh).get(5, TimeUnit.SECONDS);
            assertThat(false).as("Should fail after exceeding scan-back limit").isTrue();
        } catch (Exception e) {
            assertThat(e).hasCauseInstanceOf(ManagedLedgerException.class);
        }
    }

    /**
     * Scenario: entry parses as CursorLogEntry but contains no known payload.
     *
     * Recovery should fail fast because this is a data error.
     */
    @Test(timeOut = 30000)
    public void testInvalidEnvelopeFailsRecovery() throws Exception {
        LedgerHandle lh = createLedger();
        CursorCheckpointLog writer = new CursorCheckpointLog(5 * 1024 * 1024);

        appendRaw(lh, new CursorLogEntry().toByteArray());

        try {
            writer.readLatest(lh).get(5, TimeUnit.SECONDS);
            assertThat(false).as("Should have failed").isTrue();
        } catch (Exception e) {
            assertThat(e).hasCauseInstanceOf(ManagedLedgerException.class);
        }
    }

    /**
     * Scenario: bytes cannot be parsed as CursorLogEntry or PositionInfo.
     *
     * Recovery should fail rather than treating corrupted data as legacy.
     */
    @Test(timeOut = 30000)
    public void testCorruptedBytesFailRecovery() throws Exception {
        LedgerHandle lh = createLedger();
        CursorCheckpointLog writer = new CursorCheckpointLog(5 * 1024 * 1024);

        appendRaw(lh, new byte[]{(byte) 0x80});

        try {
            writer.readLatest(lh).get(5, TimeUnit.SECONDS);
            assertThat(false).as("Should have failed").isTrue();
        } catch (Exception e) {
            assertThat(e).hasCauseInstanceOf(ManagedLedgerException.class);
        }
    }

    /**
     * Scenario: latest chunk looks complete, but assembly fails.
     *
     * Recovery should continue scan-back and return the previous valid checkpoint.
     */
    @Test(timeOut = 30000)
    public void testBrokenCompleteChunkFallsBackToPreviousCheckpoint() throws Exception {
        LedgerHandle lh = createLedger();
        CursorCheckpointLog writer = new CursorCheckpointLog(5 * 1024 * 1024);

        appendRaw(lh, wrapCheckpoint(makeCheckpoint(80, 15)));
        appendRaw(lh, new byte[]{0x01, 0x02, 0x03}); // not a valid chunk part
        appendRaw(lh, wrapChunkPart(1, 2, new byte[]{0x11, 0x22}));

        CursorCheckpointLog.RecoveredState state = writer.readLatest(lh).get(5, TimeUnit.SECONDS);
        assertThat(state.isLegacy()).isFalse();
        assertThat(state.checkpoint.getMarkDeleteLedgerId()).isEqualTo(80L);
    }
}
