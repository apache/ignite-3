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

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import org.apache.ignite.internal.binarytuple.BinaryTupleBuilder;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.BinaryTuple;
import org.apache.ignite.internal.schema.BinaryTuplePrefix;
import org.apache.ignite.internal.schema.BinaryTupleSchema;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.NativeTypes;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.storage.RowId;
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

    private static final SchemaDescriptor INFINITY_TUPLE_SCHEMA = new SchemaDescriptor(
            1,
            new Column[]{
                    new Column("indexId", NativeTypes.UUID, false),
                    new Column("partId", NativeTypes.INT32, false)
            },
            new Column[0]
    );

    /** Index INF+ value object. */
    private final BinaryTuple positiveInf;

    private final UUID indexId;
    private final LockManager lockManager;


    /** Index storage. */
    private final SortedIndexStorage storage;

    private final Function<BinaryRow, BinaryTuple> indexRowResolver;

    /**
     * Constructs the object.
     *
     * @param indexId An identifier of the index this locker is created for.
     * @param partId Partition number.
     * @param lockManager A lock manager to acquire locks in.
     * @param storage A storage of an index this locker is created for.
     * @param indexRowResolver A convertor which derives an index key from given table row.
     */
    public SortedIndexLocker(UUID indexId, int partId, LockManager lockManager, SortedIndexStorage storage,
            Function<BinaryRow, BinaryTuple> indexRowResolver) {
        this.indexId = indexId;
        this.lockManager = lockManager;
        this.storage = storage;
        this.indexRowResolver = indexRowResolver;

        this.positiveInf = createInfiniteBoundary(partId, indexId);
    }

    /**
     * Creates a tuple for positive infinity boundary.
     *
     * @param partId Partition id.
     * @param indexId Index id.
     * @return Infinity binary tuple.
     */
    private static BinaryTuple createInfiniteBoundary(int partId, UUID indexId) {
        var binarySchema = BinaryTupleSchema.createSchema(INFINITY_TUPLE_SCHEMA, new int[]{
                INFINITY_TUPLE_SCHEMA.column("indexId").schemaIndex(),
                INFINITY_TUPLE_SCHEMA.column("partId").schemaIndex()
        });

        return new BinaryTuple(
                binarySchema,
                new BinaryTupleBuilder(binarySchema.elementCount(), false).appendUuid(indexId).appendInt(partId).build()
        );
    }

    /** {@inheritDoc} */
    @Override
    public UUID id() {
        return indexId;
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Void> locksForLookup(UUID txId, BinaryRow tableRow) {
        BinaryTuple key = indexRowResolver.apply(tableRow);

        return lockManager.acquire(txId, new LockKey(indexId, key.byteBuffer()), LockMode.S).thenApply(lock -> null);
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

        LockKey lockKey = new LockKey(indexId, indexKey(peekedRow).byteBuffer());

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

    private BinaryTuple indexKey(@Nullable IndexRow indexRow) {
        return (indexRow == null) ? positiveInf : indexRow.indexColumns();
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Lock> locksForInsert(UUID txId, BinaryRow tableRow, RowId rowId) {
        BinaryTuple key = indexRowResolver.apply(tableRow);

        BinaryTuplePrefix prefix = BinaryTuplePrefix.fromBinaryTuple(key);

        // Find next key.
        Cursor<IndexRow> cursor = storage.scan(prefix, null, SortedIndexStorage.GREATER);

        BinaryTuple nextKey;
        if (cursor.hasNext()) {
            nextKey = cursor.next().indexColumns();
        } else { // Otherwise INF.
            nextKey = positiveInf;
        }

        var nextLockKey = new LockKey(indexId, nextKey.byteBuffer());

        return lockManager.acquire(txId, nextLockKey, LockMode.IX).thenCompose(shortLock ->
                lockManager.acquire(txId, new LockKey(indexId, key.byteBuffer()), LockMode.X).thenApply((lock) ->
                        new Lock(nextLockKey, LockMode.IX, txId)
                ));
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Void> locksForRemove(UUID txId, BinaryRow tableRow, RowId rowId) {
        BinaryTuple key = indexRowResolver.apply(tableRow);

        return lockManager.acquire(txId, new LockKey(indexId, key.byteBuffer()), LockMode.IX).thenApply(lock -> null);
    }
}
