/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.table.distributed;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.function.Function.identity;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.apache.ignite.internal.util.StringUtils.toHexString;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.BinaryTuple;
import org.apache.ignite.internal.schema.BinaryTuplePrefix;
import org.apache.ignite.internal.schema.ColumnsExtractor;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.storage.StorageDestroyedException;
import org.apache.ignite.internal.storage.index.IndexRow;
import org.apache.ignite.internal.storage.index.PeekCursor;
import org.apache.ignite.internal.storage.index.SortedIndexStorage;
import org.apache.ignite.internal.tx.Lock;
import org.apache.ignite.internal.tx.LockKey;
import org.apache.ignite.internal.tx.LockManager;
import org.apache.ignite.internal.tx.LockMode;
import org.apache.ignite.internal.util.Cursor;
import org.jetbrains.annotations.Nullable;

/**
 * Locker for a sorted indexes.
 *
 * <p>Simply acquires lock on a given row for lookup and remove, acquires lock on a next key for insert.
 */
public class SortedIndexLocker implements IndexLocker {

    private static final IgniteLogger LOG = Loggers.forClass(SortedIndexLocker.class);

    /** Index INF+ value object. */
    private final Object positiveInf;

    private final int indexId;

    private final LockManager lockManager;

    private final boolean unique;
    /** Index storage. */
    private final SortedIndexStorage storage;

    private final ColumnsExtractor indexRowResolver;

    /**
     * Constructs the object.
     *
     * @param indexId An identifier of the index this locker is created for.
     * @param partId Partition number.
     * @param lockManager A lock manager to acquire locks in.
     * @param storage A storage of an index this locker is created for.
     * @param indexRowResolver A convertor which derives an index key from given table row.
     * @param unique A flag indicating whether the given index unique or not.
     */
    public SortedIndexLocker(
            int indexId,
            int partId,
            LockManager lockManager,
            SortedIndexStorage storage,
            ColumnsExtractor indexRowResolver,
            boolean unique
    ) {
        this.indexId = indexId;
        this.lockManager = lockManager;
        this.storage = storage;
        this.indexRowResolver = indexRowResolver;
        this.positiveInf = partId;
        this.unique = unique;
    }

    @Override
    public int id() {
        return indexId;
    }

    @Override
    public CompletableFuture<Void> locksForLookupByKey(UUID txId, BinaryTuple key) {
        return lockManager.acquire(txId, new LockKey(indexId, key.byteBuffer()), LockMode.S).thenApply(lock -> null);
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Void> locksForLookup(UUID txId, BinaryRow tableRow) {
        BinaryTuple key = indexRowResolver.extractColumns(tableRow);

        return locksForLookupByKey(txId, key);
    }

    /**
     * Acquires the lock for scan operation.
     *
     * @param txId An identifier of the transaction in which the row is read.
     * @param cursor Cursor, which next key is to be locked.
     * @return A future representing a next index row or {@code null} if the cursor has no rows more.
     */
    public CompletableFuture<IndexRow> locksForScan(UUID txId, Cursor<IndexRow> cursor) {
        assert cursor instanceof PeekCursor : "Cursor has incorrect type [type=" + cursor.getClass().getSimpleName() + ']';

        PeekCursor<IndexRow> peekCursor = (PeekCursor<IndexRow>) cursor;

        LOG.info("SortedIndexLocker scan lock start [indexId={}, txId={}]", indexId, txId);

        return acquireLockNextKey(txId, peekCursor).thenApply(indexRow -> {
            if (indexRow == null) {
                LOG.info("SortedIndexLocker scan lock reached end [indexId={}, txId={}]", indexId, txId);
                return null;
            }

            IndexRow nextRow = peekCursor.next();

            LOG.info("SortedIndexLocker scan lock row ready [indexId={}, txId={}, row={}]", indexId, txId, rowToString(nextRow));

            return nextRow;
        });
    }

    /**
     * Acquires a lock on the next key without moving a cursor pointer ahead.
     *
     * @param txId An identifier of the transaction in which the row is read.
     * @param peekCursor Cursor, which next key is to be locked.
     * @return A future representing a locked index row or {@code null} if the cursor has no rows more.
     */
    private CompletableFuture<IndexRow> acquireLockNextKey(UUID txId, PeekCursor<IndexRow> peekCursor) {
        IndexRow peekedRow = peekCursor.peek();

        LockKey lockKey = new LockKey(indexId, indexKey(peekedRow));

        LOG.info(
                "SortedIndexLocker scan acquire next-key start [indexId={}, txId={}, row={}, lockKey={}]",
                indexId,
                txId,
                rowToString(peekedRow),
                lockKeyDetails(lockKey)
        );

        return lockManager.acquire(txId, lockKey, LockMode.S)
                .thenCompose(ignore -> {
                    IndexRow peekedRowAfterLock = peekCursor.peek();

                    if (!rowsMatches(peekedRow, peekedRowAfterLock)) {
                        LOG.info(
                                "SortedIndexLocker scan stale boundary, retry [indexId={}, txId={}, oldRow={}, newRow={}, lockKey={}]",
                                indexId,
                                txId,
                                rowToString(peekedRow),
                                rowToString(peekedRowAfterLock),
                                lockKeyDetails(lockKey)
                        );

                        lockManager.release(txId, lockKey, LockMode.S);

                        return acquireLockNextKey(txId, peekCursor);
                    }

                    IndexRow lastLocked = peekCursor.getLastLocked();

                    if (lastLocked != null) {
                    IndexRow rescannedRow = getNextRow(lastLocked);

                        if (!rowsMatches(peekedRowAfterLock, rescannedRow)) {
                            LOG.error(
                                    "SortedIndexLocker scan peek/rescan mismatch [indexId={}, txId={}, peekedRow={}, lastLocked={}, rescannedRow={}, "
                                            + "peekedRowAfterLock={}, lockKey={}]",
                                    indexId,
                                    txId,
                                    rowToString(peekedRow),
                                    rowToString(lastLocked),
                                    rowToString(rescannedRow),
                                    rowToString(peekedRowAfterLock),
                                    lockKeyDetails(lockKey)
                            );

                        }
                    }

                    LOG.info(
                            "SortedIndexLocker scan acquire next-key success [indexId={}, txId={}, row={}, lockKey={}]",
                            indexId,
                            txId,
                            rowToString(peekedRow),
                            lockKeyDetails(lockKey)
                    );

                    peekCursor.locked(peekedRow);

                    return completedFuture(peekedRow);
                });
    }

    private static boolean rowsMatches(@Nullable IndexRow row0, @Nullable IndexRow row1) {
        if (row0 == null ^ row1 == null) {
            return false;
        }

        return row0 == row1 || row0.rowId().equals(row1.rowId())
                && row0.indexColumns().byteBuffer().equals(row1.indexColumns().byteBuffer());
    }

    private Object indexKey(@Nullable IndexRow indexRow) {
        return (indexRow == null) ? positiveInf : indexRow.indexColumns().byteBuffer();
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<@Nullable Lock> locksForInsert(UUID txId, BinaryRow tableRow, RowId rowId) {
        BinaryTuple key = indexRowResolver.extractColumns(tableRow);

        BinaryTuplePrefix prefix = BinaryTuplePrefix.fromBinaryTuple(key);

        LOG.info(
                "SortedIndexLocker insert lock start [indexId={}, txId={}, rowIdUUID={}, key={}, prefix={}]",
                indexId,
                txId,
                rowId.uuid(),
                toHexString(key.byteBuffer()),
                toHexString(prefix.byteBuffer())
        );

        try {
            return lockForInsert(getNextRowForInsertV2(prefix, rowId), txId, prefix, rowId, new LockKey(indexId, key.byteBuffer()));
        } catch (StorageDestroyedException e) {
            LOG.info("SortedIndexLocker insert lock skipped, storage destroyed [indexId={}, txId={}]", indexId, txId);
            return nullCompletedFuture();
        }
    }

    private CompletableFuture<@Nullable Lock> lockForInsert(
            @Nullable IndexRow nextRow0,
            UUID txId,
            BinaryTuplePrefix prefix,
            RowId rowId,
            LockKey currentLockKey
    ) {
        var nextLockKey = new LockKey(indexId, indexKey(nextRow0));

        LOG.info(
                "SortedIndexLocker insert acquire start [indexId={}, txId={}, nextRow={}, nextLockKey={}, currentLockKey={}]",
                indexId,
                txId,
                rowToString(nextRow0),
                lockKeyDetails(nextLockKey),
                lockKeyDetails(currentLockKey)
        );

        return lockManager.acquire(txId, nextLockKey, LockMode.IX).thenCompose(shortLock ->
                lockManager.acquire(txId, currentLockKey, currentKeyLockMode(shortLock.lockMode()))
                        .thenCompose(currentLock -> {
                            try {
                                IndexRow nextRow1 = getNextRowForInsertV2(prefix, rowId);

                                if (!rowsMatches(nextRow0, nextRow1)) { // ?
                                    LOG.info(
                                            "SortedIndexLocker insert boundary changed, retry [indexId={}, txId={}, oldNextRow={}, newNextRow={}]",
                                            indexId,
                                            txId,
                                            rowToString(nextRow0),
                                            rowToString(nextRow1)
                                    );

                                    lockManager.release(txId, currentLock.lockKey(), currentLock.lockMode());
                                    lockManager.release(txId, shortLock.lockKey(), shortLock.lockMode());

                                    return lockForInsert(nextRow1, txId, prefix, rowId, currentLockKey);
                                }
                            } catch (StorageDestroyedException ignored) {
                                // The index storage is already destroyed, so no one can scan it. This means we can just omit taking insertion locks
                                // on it: we are not going to write to it (as we can't) anyway.
                                LOG.info("SortedIndexLocker insert lock skipped during retry, storage destroyed [indexId={}, txId={}]", indexId, txId);
                                return nullCompletedFuture();
                            }

                            LOG.info(
                                    "SortedIndexLocker insert acquire success [indexId={}, txId={}, nextLock={}, currentLockMode={}]",
                                    indexId,
                                    txId,
                                    lockKeyDetails(shortLock.lockKey()),
                                    currentLock.lockMode()
                            );

                            return completedFuture(new Lock(shortLock.lockKey(), shortLock.lockMode(), txId));
                        })
                        .thenApply(identity())
        );
    }

    private @Nullable IndexRow getNextRow(@Nullable IndexRow lastLocked) {
        if (lastLocked == null) {
            return getNextRow((BinaryTuplePrefix) null);
        }

        BinaryTuplePrefix prefix = BinaryTuplePrefix.fromBinaryTuple(lastLocked.indexColumns());

        LOG.info(
                "SortedIndexLocker rescan prefix [indexId={}, lastLocked={}, key={}, prefix={}]",
                indexId,
                lastLocked.rowId(),
                toHexString(lastLocked.indexColumns().byteBuffer()),
                toHexString(prefix.byteBuffer())
        );

        return getNextRowForInsertV2(prefix, lastLocked.rowId());
    }

    private @Nullable IndexRow getNextRow(@Nullable BinaryTuplePrefix prefix) {
        try (PeekCursor<IndexRow> cursor = storage.tolerantScan(prefix, null, SortedIndexStorage.GREATER)) {
            if (cursor.peek() != null) {
                return cursor.next();
            }
        }

        return null;
    }

    private static BinaryTuplePrefix copyPrefix(BinaryTuplePrefix src) {
        ByteBuffer in = src.byteBuffer().duplicate();

        byte[] bytes = new byte[in.remaining()];
        in.get(bytes);

        return new BinaryTuplePrefix(src.elementCount(), bytes);
    }

    private @Nullable IndexRow getNextRowForInsertV2(BinaryTuplePrefix keyPrefix, RowId rowId) {
        try (PeekCursor<IndexRow> cursor = storage.tolerantScan(
                keyPrefix,
                null,
                SortedIndexStorage.GREATER
        )) {
            if (cursor.peek() != null) {
                return cursor.next();
            }
        }
        return null;
    }
//TODO fixes problem with different next elements between scans/inserts
    private @Nullable IndexRow getNextRowForInsert(BinaryTuplePrefix keyPrefix, RowId rowId) {
        LOG.info(
                "SortedIndexLocker insert same-key scan start [indexId={}, rowId={}, keyPrefix={}]",
                indexId,
                rowId,
                toHexString(keyPrefix.byteBuffer())
        );

        List<IndexRow> sameKeyRows = new ArrayList<>();

        // Keep insert boundary in sorted-index order (key, rowId): first row with the same key and greater RowId.
        try (PeekCursor<IndexRow> sameKeyCursor = storage.tolerantScan(
                copyPrefix(keyPrefix),
                copyPrefix(keyPrefix),
                SortedIndexStorage.GREATER_OR_EQUAL | SortedIndexStorage.LESS_OR_EQUAL
        )) {
            while (sameKeyCursor.peek() != null) {
                sameKeyRows.add(sameKeyCursor.next());
            }
        }

        LOG.info(
                "SortedIndexLocker insert same-key candidates [indexId={}, rowId={}, keyPrefix={}, candidates={}]",
                indexId,
                rowId,
                toHexString(keyPrefix.byteBuffer()),
                rowsToString(sameKeyRows)
        );

        for (int i = 0; i < sameKeyRows.size(); i++) {
            IndexRow row = sameKeyRows.get(i);

            if (row.rowId().compareTo(rowId) > 0) { // TODO will work only for test storage, it is fine for a moment
                LOG.info(
                        "SortedIndexLocker insert same-key selected [indexId={}, rowId={}, selected={}, candidateIndex={}]",
                        indexId,
                        rowId,
                        rowToString(row),
                        i + 1
                );

                return row;
            }
        }

        // No rows for this key after the insert position, move to the next key.
        IndexRow nextKeyRow = getNextRow(keyPrefix);

        LOG.info(
                "SortedIndexLocker insert same-key fallback-next-key [indexId={}, rowId={}, candidates={}, nextKeyRow={}]",
                indexId,
                rowId,
                sameKeyRows.size(),
                rowToString(nextKeyRow)
        );

        return nextKeyRow;
    }

    private static String rowsToString(List<IndexRow> rows) {
        if (rows.isEmpty()) {
            return "[]";
        }

        StringBuilder sb = new StringBuilder("[");

        for (int i = 0; i < rows.size(); i++) {
            if (i > 0) {
                sb.append(", ");
            }

            sb.append(rowToString(rows.get(i)));
        }

        sb.append(']');

        return sb.toString();
    }

    private static String boundary(@Nullable IndexRow row) {
        if (row == null) {
            return "INF+";
        }

        return toHexString(row.indexColumns().byteBuffer());
    }

    private static String lockKeyDetails(LockKey lockKey) {
        Object key = lockKey.key();

        if (key instanceof ByteBuffer) {
            ByteBuffer buffer = (ByteBuffer) key;

            return "keyBytes=" + toHexString(buffer);
        }

        return "n/a";
    }

    private LockMode currentKeyLockMode(LockMode nextKeyLockMode) {
        if (unique) {
            return LockMode.X;
        }
        if (nextKeyLockMode == LockMode.S
                || nextKeyLockMode == LockMode.X
                || nextKeyLockMode == LockMode.SIX) {
            return LockMode.X;
        }
        return LockMode.IX;
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Void> locksForRemove(UUID txId, BinaryRow tableRow, RowId rowId) {
        BinaryTuple key = indexRowResolver.extractColumns(tableRow);

        return lockManager.acquire(txId, new LockKey(indexId, key.byteBuffer()), LockMode.IX).thenApply(lock -> null);
    }

    private static String rowToString(@Nullable IndexRow row) {
        if (row == null) {
            return "INF+";
        }

        return "rowId=" + row.rowId() + ", key=" + toHexString(row.indexColumns().byteBuffer());
    }
}
