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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.function.BiConsumer;
import lombok.CustomLog;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.PositionFactory;
import org.apache.bookkeeper.mledger.proto.AckState;
import org.apache.bookkeeper.mledger.proto.AckStateRef;
import org.apache.bookkeeper.mledger.proto.BatchedEntryDeletionIndexInfo;
import org.apache.bookkeeper.mledger.proto.CursorCheckpoint;
import org.apache.bookkeeper.mledger.proto.CursorLogEntry;

@CustomLog
public class CursorCheckpointPersistence {

    private final CursorCheckpointLog writer;
    private final ReadWriteLock lock;
    private final BiConsumer<Long, Long> checkpointAckedCallback;
    private final BookKeeper bookKeeper;
    private final BookKeeper.DigestType digestType;
    private final byte[] password;
    private final boolean perLedgerEntryPersistEnabled;

    private final Map<Long, Position> lastAppendedPos = new HashMap<>();
    private volatile CompletableFuture<CursorCheckpointLog.AppendResult> lastPersist =
            CompletableFuture.completedFuture(null);

    public CursorCheckpointPersistence(CursorCheckpointLog writer, ReadWriteLock lock,
                                         BiConsumer<Long, Long> checkpointAckedCallback,
                                         BookKeeper bookKeeper,
                                         BookKeeper.DigestType digestType, byte[] password,
                                         boolean perLedgerEntryPersistEnabled) {
        this.writer = writer;
        this.lock = lock;
        this.checkpointAckedCallback = checkpointAckedCallback;
        this.bookKeeper = bookKeeper;
        this.digestType = digestType;
        this.password = password;
        this.perLedgerEntryPersistEnabled = perLedgerEntryPersistEnabled;
    }

    public boolean isPerLedgerEntryPersistEnabled() {
        return perLedgerEntryPersistEnabled;
    }

    public void setZkCheckpointHint(long cursorLedgerId, long entryId) {
        writer.setZkCheckpointHint(cursorLedgerId, entryId);
    }

    public synchronized CompletableFuture<CursorCheckpointLog.AppendResult> persist(
            LedgerHandle lh, Position mdPos, Map<String, Long> properties,
            ManagedCursorImpl cursor) {
        lastPersist = lastPersist
                .exceptionally(e -> null)
                .thenCompose(ignored -> doPersist(lh, mdPos, properties, cursor));
        return lastPersist;
    }

    private CompletableFuture<CursorCheckpointLog.AppendResult> doPersist(
            LedgerHandle lh, Position mdPos, Map<String, Long> properties,
            ManagedCursorImpl cursor) {

        final long mdLedgerId = mdPos.getLedgerId();
        final Set<Long> dirtyLedgers;
        final Map<Long, byte[]> dirtyBitmaps = new HashMap<>();
        final Map<Long, List<BatchedEntryDeletionIndexInfo>> dirtyBatchAcks = new HashMap<>();
        final byte[] mdBitmap;
        final List<BatchedEntryDeletionIndexInfo> mdBatchAcks;

        lock.readLock().lock();
        try {
            dirtyLedgers = cursor.individualDeletedMessages.snapshotAndClearDirtyLedgers();
            dirtyLedgers.remove(mdLedgerId);
            for (long id : dirtyLedgers) {
                dirtyBitmaps.put(id, cursor.individualDeletedMessages.bitmapOf(id));
                dirtyBatchAcks.put(id, filterBatchAcksForLedger(cursor, id));
            }
            mdBitmap = cursor.individualDeletedMessages.bitmapOf(mdLedgerId);
            mdBatchAcks = filterBatchAcksForLedger(cursor, mdLedgerId);
        } finally {
            lock.readLock().unlock();
        }

        List<Long> dirtyOrder = new ArrayList<>(dirtyLedgers);
        Collections.sort(dirtyOrder);
        List<Long> writeOrder = new ArrayList<>(dirtyOrder);
        writeOrder.add(mdLedgerId);

        final Map<Long, Position> prevPositions = new HashMap<>();
        CompletableFuture<CursorCheckpointLog.AppendResult> chain =
                CompletableFuture.completedFuture(null);
        for (long ledgerId : writeOrder) {
            chain = chain.thenCompose(ignored -> {
                byte[] bitmap = ledgerId == mdLedgerId ? mdBitmap : dirtyBitmaps.get(ledgerId);
                List<BatchedEntryDeletionIndexInfo> batchAcks =
                        ledgerId == mdLedgerId ? mdBatchAcks : dirtyBatchAcks.get(ledgerId);
                CursorCheckpoint checkpoint = buildCheckpoint(ledgerId, bitmap, batchAcks,
                        mdPos, properties, cursor, dirtyLedgers);
                return writer.appendCheckpoint(lh, checkpoint).thenApply(result -> {
                    lock.writeLock().lock();
                    try {
                        Position persistedPos = PositionFactory.create(lh.getId(), result.commitEntryId());
                        Position prev = lastAppendedPos.put(ledgerId, persistedPos);
                        if (prev != null && !prevPositions.containsKey(ledgerId)) {
                            prevPositions.put(ledgerId, prev);
                        }
                    } finally {
                        lock.writeLock().unlock();
                    }
                    return result;
                });
            });
        }

        return chain.thenApply(result -> {
            if (checkpointAckedCallback != null) {
                checkpointAckedCallback.accept(lh.getId(), result.commitEntryId());
            }
            return result;
        }).exceptionally(error -> {
            lock.writeLock().lock();
            try {
                cursor.individualDeletedMessages.restoreDirtyLedgers(dirtyLedgers);
                for (long ledgerId : writeOrder) {
                    Position prev = prevPositions.get(ledgerId);
                    if (prev != null) {
                        lastAppendedPos.put(ledgerId, prev);
                    } else {
                        lastAppendedPos.remove(ledgerId);
                    }
                }
            } finally {
                lock.writeLock().unlock();
            }
            throw new CompletionException(error);
        });
    }

    public CompletableFuture<RecoveredCheckpoint> recover(LedgerHandle lh) {
        return writer.readLatest(lh).thenCompose(state -> {
            if (state.isLegacy()) {
                return CompletableFuture.completedFuture(RecoveredCheckpoint.legacy(state.legacyBytes));
            }
            CursorCheckpoint cp = state.checkpoint;
            int expectedCount = 1 + cp.getAckStateRefsCount();
            return fetchAckStateRefs(cp).thenApply(fetched -> {
                if (fetched.size() != expectedCount) {
                    throw new RuntimeException(new ManagedLedgerException(
                            "Recovery inconsistency: expected " + expectedCount + " ack states, got "
                                    + fetched.size()));
                }
                return RecoveredCheckpoint.of(cp, fetched);
            });
        });
    }

    public CompletableFuture<RecoveredCheckpoint> recoverWithHint(
            LedgerHandle lh, long hintCursorLedgerId, long hintEntryId) {
        if (hintCursorLedgerId >= 0 && hintEntryId >= 0) {
            setZkCheckpointHint(hintCursorLedgerId, hintEntryId);
        }
        return recover(lh);
    }

    private CompletableFuture<Map<Long, AckStateData>> fetchAckStateRefs(CursorCheckpoint cp) {
        Map<Long, AckStateData> result = new HashMap<>();
        if (cp.getAckStatesCount() != 1) {
            throw new RuntimeException(new ManagedLedgerException(
                    "CursorCheckpoint must contain exactly one inline AckState, got "
                            + cp.getAckStatesCount()));
        }
        AckState primary = cp.getAckStateAt(0);
        result.put(primary.getMsgLedgerId(),
                new AckStateData(extractBitmap(primary),
                        primary.getBatchAcksList() != null
                                ? primary.getBatchAcksList() : Collections.emptyList()));

        List<AckStateRef> refs = cp.getAckStateRefsList();
        if (refs.isEmpty()) {
            return CompletableFuture.completedFuture(result);
        }

        List<CompletableFuture<Void>> fetches = new ArrayList<>();
        for (AckStateRef ref : refs) {
            fetches.add(fetchAckState(ref).thenAccept(ackData -> {
                synchronized (result) {
                    result.put(ref.getMsgLedgerId(), ackData);
                }
            }));
        }
        return CompletableFuture.allOf(fetches.toArray(new CompletableFuture[0]))
                .thenApply(ignored -> result);
    }

    private CompletableFuture<AckStateData> fetchAckState(AckStateRef ref) {
        return readEntryFromLedger(ref.getCursorLedgerId(), ref.getEntryId()).thenApply(bytes -> {
            CursorLogEntry env = new CursorLogEntry();
            try {
                env.parseFrom(bytes);
            } catch (Exception e) {
                throw new RuntimeException(new ManagedLedgerException(
                        "AckStateRef parse failure for " + ref, e));
            }
            if (!env.hasCheckpoint()) {
                throw new RuntimeException(new ManagedLedgerException(
                        "AckStateRef target " + ref + " is not a checkpoint entry"));
            }
            CursorCheckpoint cp = env.getCheckpoint();
            if (cp.getAckStatesCount() != 1) {
                throw new RuntimeException(new ManagedLedgerException(
                        "AckStateRef target " + ref + " is not a single-ack checkpoint"));
            }
            AckState ackState = cp.getAckStateAt(0);
            if (ackState.getMsgLedgerId() == ref.getMsgLedgerId()) {
                return new AckStateData(extractBitmap(ackState),
                        ackState.getBatchAcksList() != null
                                ? ackState.getBatchAcksList() : Collections.emptyList());
            }
            throw new RuntimeException(new ManagedLedgerException(
                    "AckStateRef target " + ref + " does not contain msgLedgerId "
                            + ref.getMsgLedgerId()));
        });
    }

    private CompletableFuture<byte[]> readEntryFromLedger(long ledgerId, long entryId) {
        CompletableFuture<byte[]> future = new CompletableFuture<>();
        bookKeeper.asyncOpenLedgerNoRecovery(ledgerId, digestType, password, (rc, handle, ctx) -> {
            if (rc != BKException.Code.OK) {
                future.completeExceptionally(BKException.create(rc));
                return;
            }
            handle.asyncReadEntries(entryId, entryId, (rc2, lh, entries, ctx2) -> {
                lh.asyncClose((closeRc, closeHandle, closeCtx) -> {}, null);
                if (rc2 != BKException.Code.OK) {
                    future.completeExceptionally(BKException.create(rc2));
                    return;
                }
                if (!entries.hasMoreElements()) {
                    future.completeExceptionally(new ManagedLedgerException(
                            "Empty read for entry " + entryId + " in ledger " + ledgerId));
                    return;
                }
                future.complete(entries.nextElement().getEntry());
            }, null);
        }, null);
        return future;
    }

    private static List<BatchedEntryDeletionIndexInfo> filterBatchAcksForLedger(
            ManagedCursorImpl cursor, long msgLedgerId) {
        if (cursor.batchDeletedIndexes == null || cursor.batchDeletedIndexes.isEmpty()) {
            return Collections.emptyList();
        }
        List<BatchedEntryDeletionIndexInfo> result = new ArrayList<>();
        cursor.batchDeletedIndexes.forEach((position, bitSet) -> {
            if (position.getLedgerId() == msgLedgerId) {
                BatchedEntryDeletionIndexInfo info = new BatchedEntryDeletionIndexInfo();
                info.setPosition().setLedgerId(position.getLedgerId())
                        .setEntryId(position.getEntryId());
                bitSet.stream().forEach(info::addDeleteSet);
                result.add(info);
            }
        });
        return result;
    }

    private CursorCheckpoint buildCheckpoint(long msgLedgerId, byte[] bitmap,
                                             List<BatchedEntryDeletionIndexInfo> batchAcks,
                                             Position mdPos, Map<String, Long> properties,
                                             ManagedCursorImpl cursor, Set<Long> dirtyLedgers) {
        CursorCheckpoint cp = new CursorCheckpoint()
                .setMarkDeleteLedgerId(mdPos.getLedgerId())
                .setMarkDeleteEntryId(mdPos.getEntryId());
        if (properties != null) {
            properties.forEach((name, value) -> {
                org.apache.bookkeeper.mledger.proto.LongProperty prop = cp.addProperty();
                prop.setName(name).setValue(value);
            });
        }
        addAckState(cp, msgLedgerId, bitmap, batchAcks);
        lock.readLock().lock();
        try {
            cursor.individualDeletedMessages.forEachActiveLedger(id -> {
                if (id != msgLedgerId) {
                    Position pos = lastAppendedPos.get(id);
                    if (pos == null) {
                        if (dirtyLedgers.contains(id)) {
                            return;
                        }
                        throw new IllegalStateException(
                                "Missing lastAppendedPos for active msgLedgerId " + id);
                    }
                    AckStateRef ref = cp.addAckStateRef();
                    ref.setMsgLedgerId(id)
                            .setCursorLedgerId(pos.getLedgerId())
                            .setEntryId(pos.getEntryId());
                }
            });
        } finally {
            lock.readLock().unlock();
        }
        return cp;
    }

    private static void addAckState(CursorCheckpoint cp, long msgLedgerId, byte[] bitmap,
                                    List<BatchedEntryDeletionIndexInfo> batchAcks) {
        AckState ackState = cp.addAckState().setMsgLedgerId(msgLedgerId);
        if (bitmap != null && bitmap.length > 0) {
            ackState.setAckBitmap(bitmap);
        }
        if (batchAcks != null && !batchAcks.isEmpty()) {
            ackState.addAllBatchAcks(batchAcks);
        }
    }

    private static byte[] extractBitmap(AckState ackState) {
        return ackState.hasAckBitmap() ? ackState.getAckBitmap() : null;
    }

    record AckStateData(byte[] ackBitmap, List<BatchedEntryDeletionIndexInfo> batchAcks) {
    }

    static final class RecoveredCheckpoint {
        final CursorCheckpoint checkpoint;
        final Map<Long, AckStateData> ackData;
        final byte[] legacyBytes;

        private RecoveredCheckpoint(CursorCheckpoint checkpoint, Map<Long, AckStateData> ackData) {
            this.checkpoint = checkpoint;
            this.ackData = ackData;
            this.legacyBytes = null;
        }

        private RecoveredCheckpoint(byte[] legacyBytes) {
            this.checkpoint = null;
            this.ackData = null;
            this.legacyBytes = legacyBytes;
        }

        static RecoveredCheckpoint of(CursorCheckpoint cp, Map<Long, AckStateData> ackData) {
            return new RecoveredCheckpoint(cp, ackData);
        }

        static RecoveredCheckpoint legacy(byte[] bytes) {
            return new RecoveredCheckpoint(bytes);
        }

        boolean isLegacy() {
            return legacyBytes != null;
        }
    }
}
