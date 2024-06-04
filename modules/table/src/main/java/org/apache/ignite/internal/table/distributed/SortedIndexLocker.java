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

import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
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
    /** Index INF+ value object. */
    private final Object positiveInf;

    private final int indexId;
    private final LockManager lockManager;


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
     */
    public SortedIndexLocker(int indexId, int partId, LockManager lockManager, SortedIndexStorage storage,
            ColumnsExtractor indexRowResolver) {
        this.indexId = indexId;
        this.lockManager = lockManager;
        this.storage = storage;
        this.indexRowResolver = indexRowResolver;
        this.positiveInf = partId;
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

        return acquireLockNextKey(txId, peekCursor).thenApply(indexRow -> {
            if (indexRow == null) {
                return null;
            }

            return peekCursor.next();
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

        return lockManager.acquire(txId, lockKey, LockMode.S)
                .thenCompose(ignore -> {
                    IndexRow peekedRowAfterLock = peekCursor.peek();

                    if (!rowIdMatches(peekedRow, peekedRowAfterLock)) {
                        lockManager.release(txId, lockKey, LockMode.S);

                        return acquireLockNextKey(txId, peekCursor);
                    }

                    return CompletableFuture.completedFuture(peekedRow);
                });
    }

    private static boolean rowIdMatches(@Nullable IndexRow row0, @Nullable IndexRow row1) {
        if (row0 == null ^ row1 == null) {
            return false;
        }

        return row0 == row1 || row0.rowId().equals(row1.rowId());
    }

    private Object indexKey(@Nullable IndexRow indexRow) {
        return (indexRow == null) ? positiveInf : indexRow.indexColumns().byteBuffer();
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<@Nullable Lock> locksForInsert(UUID txId, BinaryRow tableRow, RowId rowId) {
        BinaryTuple key = indexRowResolver.extractColumns(tableRow);

        BinaryTuplePrefix prefix = BinaryTuplePrefix.fromBinaryTuple(key);

        IndexRow nextRow = null;

        // Find next key.
        try (Cursor<IndexRow> cursor = storage.tolerantScan(prefix, null, SortedIndexStorage.GREATER)) {
            if (cursor.hasNext()) {
                nextRow = cursor.next();
            }
        } catch (StorageDestroyedException ignored) {
            // The index storage is already destroyed, so no one can scan it. This means we can just omit taking insertion locks
            // on it: we are not going to write to it (as we can't) anyway.
            return nullCompletedFuture();
        }

        var nextLockKey = new LockKey(indexId, indexKey(nextRow));

        return lockManager.acquire(txId, nextLockKey, LockMode.IX).thenCompose(shortLock ->
                lockManager.acquire(txId, new LockKey(indexId, key.byteBuffer()), LockMode.X).thenApply((lock) ->
                        new Lock(nextLockKey, LockMode.IX, txId)
                ));
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Void> locksForRemove(UUID txId, BinaryRow tableRow, RowId rowId) {
        BinaryTuple key = indexRowResolver.extractColumns(tableRow);

        return lockManager.acquire(txId, new LockKey(indexId, key.byteBuffer()), LockMode.IX).thenApply(lock -> null);
    }
}
