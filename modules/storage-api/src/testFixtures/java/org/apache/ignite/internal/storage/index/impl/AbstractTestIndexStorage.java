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

package org.apache.ignite.internal.storage.index.impl;

import java.util.Iterator;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.internal.schema.BinaryTuple;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.storage.StorageClosedException;
import org.apache.ignite.internal.storage.StorageRebalanceException;
import org.apache.ignite.internal.storage.index.IndexStorage;
import org.apache.ignite.internal.util.Cursor;
import org.jetbrains.annotations.Nullable;

/**
 * Test-only abstract index storage class.
 */
abstract class AbstractTestIndexStorage implements IndexStorage {
    private volatile boolean closed;

    private volatile boolean rebalance;

    private volatile @Nullable RowId nextRowIdToBuild;

    /** Amount of cursors that opened and still do not close. */
    protected final AtomicInteger pendingCursors = new AtomicInteger();

    AbstractTestIndexStorage(int partitionId) {
        nextRowIdToBuild = RowId.lowestRowId(partitionId);
    }

    /**
     * Gets amount of pending cursors.
     *
     * @return Amount of pending cursors.
     */
    public int pendingCursors() {
        return pendingCursors.get();
    }

    @Override
    public Cursor<RowId> get(BinaryTuple key) {
        checkStorageClosedOrInProcessOfRebalance();

        Iterator<RowId> iterator = getRowIdIteratorForGetByBinaryTuple(key);

        pendingCursors.incrementAndGet();

        return new Cursor<>() {
            @Override
            public void close() {
                pendingCursors.decrementAndGet();
            }

            @Override
            public boolean hasNext() {
                checkStorageClosedOrInProcessOfRebalance();

                return iterator.hasNext();
            }

            @Override
            public RowId next() {
                checkStorageClosedOrInProcessOfRebalance();

                return iterator.next();
            }
        };
    }

    @Override
    public @Nullable RowId getNextRowIdToBuild() {
        checkStorageClosedOrInProcessOfRebalance();

        return nextRowIdToBuild;
    }

    @Override
    public void setNextRowIdToBuild(@Nullable RowId rowId) {
        checkStorageClosedOrInProcessOfRebalance();

        nextRowIdToBuild = rowId;
    }

    /**
     * Removes all index data.
     */
    public void clear() {
        checkStorageClosedOrInProcessOfRebalance();

        clear0();
    }

    public void destroy() {
        closed = true;

        clear0();
    }

    abstract Iterator<RowId> getRowIdIteratorForGetByBinaryTuple(BinaryTuple key);

    abstract void clear0();

    /**
     * Starts rebalancing of the storage.
     */
    public void startRebalance() {
        checkStorageClosed();

        rebalance = true;

        clear0();
    }

    /**
     * Aborts rebalance of the storage.
     */
    public void abortRebalance() {
        checkStorageClosed();

        if (!rebalance) {
            return;
        }

        rebalance = false;

        clear0();
    }

    /**
     * Completes rebalance of the storage.
     */
    public void finishRebalance() {
        checkStorageClosed();

        assert rebalance;

        rebalance = false;
    }

    void checkStorageClosed() {
        if (closed) {
            throw new StorageClosedException();
        }
    }

    void checkStorageClosedOrInProcessOfRebalance() {
        checkStorageClosed();

        if (rebalance) {
            throw new StorageRebalanceException("Storage in the process of rebalancing");
        }
    }
}
