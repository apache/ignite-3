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

package org.apache.ignite.internal.storage.pagememory.index.hash;

import static org.apache.ignite.internal.storage.util.StorageUtils.throwExceptionDependingOnStorageState;
import static org.apache.ignite.internal.storage.util.StorageUtils.throwExceptionDependingOnStorageStateOnRebalance;
import static org.apache.ignite.internal.storage.util.StorageUtils.throwExceptionIfStorageInProgressOfRebalance;
import static org.apache.ignite.internal.storage.util.StorageUtils.throwExceptionIfStorageNotInProgressOfRebalance;

import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import org.apache.ignite.internal.schema.BinaryTuple;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.storage.StorageException;
import org.apache.ignite.internal.storage.StorageRebalanceException;
import org.apache.ignite.internal.storage.index.HashIndexDescriptor;
import org.apache.ignite.internal.storage.index.HashIndexStorage;
import org.apache.ignite.internal.storage.index.IndexRow;
import org.apache.ignite.internal.storage.pagememory.index.freelist.IndexColumns;
import org.apache.ignite.internal.storage.pagememory.index.freelist.IndexColumnsFreeList;
import org.apache.ignite.internal.storage.util.StorageState;
import org.apache.ignite.internal.util.Cursor;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.lang.IgniteInternalCheckedException;
import org.apache.ignite.lang.IgniteStringFormatter;

/**
 * Implementation of Hash index storage using Page Memory.
 */
public class PageMemoryHashIndexStorage implements HashIndexStorage {
    /** Index descriptor. */
    private final HashIndexDescriptor descriptor;

    /** Free list to store index columns. */
    private volatile IndexColumnsFreeList freeList;

    /** Hash index tree instance. */
    private volatile HashIndexTree hashIndexTree;

    /** Partition id. */
    private final int partitionId;

    /** Lowest possible RowId according to signed long ordering. */
    private final RowId lowestRowId;

    /** Highest possible RowId according to signed long ordering. */
    private final RowId highestRowId;

    /** Busy lock. */
    private final IgniteSpinBusyLock busyLock = new IgniteSpinBusyLock();

    /** Current state of the storage. */
    private final AtomicReference<StorageState> state = new AtomicReference<>(StorageState.RUNNABLE);

    /**
     * Constructor.
     *
     * @param descriptor Hash index descriptor.
     * @param freeList Free list to store index columns.
     * @param hashIndexTree Hash index tree instance.
     */
    public PageMemoryHashIndexStorage(HashIndexDescriptor descriptor, IndexColumnsFreeList freeList, HashIndexTree hashIndexTree) {
        this.descriptor = descriptor;
        this.freeList = freeList;
        this.hashIndexTree = hashIndexTree;

        partitionId = hashIndexTree.partitionId();

        lowestRowId = RowId.lowestRowId(partitionId);

        highestRowId = RowId.highestRowId(partitionId);
    }

    @Override
    public HashIndexDescriptor indexDescriptor() {
        return descriptor;
    }

    @Override
    public Cursor<RowId> get(BinaryTuple key) throws StorageException {
        return busy(() -> getBusy(key));
    }

    private Cursor<RowId> getBusy(BinaryTuple key) throws StorageException {
        throwExceptionIfStorageInProgressOfRebalance(state.get(), this::createStorageInfo);

        try {
            IndexColumns indexColumns = new IndexColumns(partitionId, key.byteBuffer());

            HashIndexRow lowerBound = new HashIndexRow(indexColumns, lowestRowId);
            HashIndexRow upperBound = new HashIndexRow(indexColumns, highestRowId);

            Cursor<HashIndexRow> cursor = hashIndexTree.find(lowerBound, upperBound);

            return new Cursor<>() {
                @Override
                public void close() {
                    cursor.close();
                }

                @Override
                public boolean hasNext() {
                    return busy(() -> {
                        throwExceptionIfStorageInProgressOfRebalance(state.get(), PageMemoryHashIndexStorage.this::createStorageInfo);

                        return cursor.hasNext();
                    });
                }

                @Override
                public RowId next() {
                    return busy(() -> {
                        throwExceptionIfStorageInProgressOfRebalance(state.get(), PageMemoryHashIndexStorage.this::createStorageInfo);

                        return cursor.next().rowId();
                    });
                }
            };
        } catch (IgniteInternalCheckedException e) {
            throw new StorageException("Failed to create scan cursor", e);
        }
    }

    @Override
    public void put(IndexRow row) throws StorageException {
        busy(() -> {
            putBusy(row);

            return null;
        });
    }

    private void putBusy(IndexRow row) throws StorageException {
        try {
            IndexColumns indexColumns = new IndexColumns(partitionId, row.indexColumns().byteBuffer());

            HashIndexRow hashIndexRow = new HashIndexRow(indexColumns, row.rowId());

            var insert = new InsertHashIndexRowInvokeClosure(hashIndexRow, freeList, hashIndexTree.inlineSize());

            hashIndexTree.invoke(hashIndexRow, null, insert);
        } catch (IgniteInternalCheckedException e) {
            throw new StorageException("Failed to put value into index", e);
        }
    }

    @Override
    public void remove(IndexRow row) throws StorageException {
        busy(() -> {
            removeBusy(row);

            return null;
        });
    }

    private void removeBusy(IndexRow row) throws StorageException {
        throwExceptionIfStorageInProgressOfRebalance(state.get(), this::createStorageInfo);

        try {
            IndexColumns indexColumns = new IndexColumns(partitionId, row.indexColumns().byteBuffer());

            HashIndexRow hashIndexRow = new HashIndexRow(indexColumns, row.rowId());

            var remove = new RemoveHashIndexRowInvokeClosure(hashIndexRow, freeList);

            hashIndexTree.invoke(hashIndexRow, null, remove);

            // Performs actual deletion from freeList if necessary.
            remove.afterCompletion();
        } catch (IgniteInternalCheckedException e) {
            throw new StorageException("Failed to remove value from index", e);
        }
    }

    @Override
    public void destroy() throws StorageException {
        // TODO: IGNITE-17626 Remove it
        throw new UnsupportedOperationException();
    }

    /**
     * Closes the hash index storage.
     */
    public void close() {
        if (!state.compareAndSet(StorageState.RUNNABLE, StorageState.CLOSED)) {
            StorageState state = this.state.get();

            assert state == StorageState.CLOSED : state;

            return;
        }

        busyLock.block();

        hashIndexTree.close();
    }

    /**
     * Prepares storage for rebalancing.
     *
     * @throws StorageRebalanceException If there was an error when starting the rebalance.
     */
    public void startRebalance() {
        if (!state.compareAndSet(StorageState.RUNNABLE, StorageState.REBALANCE)) {
            throwExceptionDependingOnStorageStateOnRebalance(state.get(), createStorageInfo());
        }

        // Changed storage states and expect all storage operations to stop soon.
        busyLock.block();

        try {
            this.hashIndexTree.close();
        } finally {
            busyLock.unblock();
        }
    }

    /**
     * Completes the rebalancing of the storage.
     *
     * @throws StorageRebalanceException If there is an error while completing the storage rebalance.
     */
    public void completeRebalance() {
        if (!state.compareAndSet(StorageState.REBALANCE, StorageState.RUNNABLE)) {
            throwExceptionDependingOnStorageStateOnRebalance(state.get(), createStorageInfo());
        }
    }

    /**
     * Updates the internal data structures of the storage on rebalance.
     *
     * @param freeList Free list to store index columns.
     * @param hashIndexTree Hash index tree instance.
     * @throws StorageRebalanceException If the storage is not in the process of rebalancing.
     */
    public void updateDataStructuresOnRebalance(IndexColumnsFreeList freeList, HashIndexTree hashIndexTree) {
        throwExceptionIfStorageNotInProgressOfRebalance(state.get(), this::createStorageInfo);

        this.freeList = freeList;
        this.hashIndexTree = hashIndexTree;
    }

    private String createStorageInfo() {
        return IgniteStringFormatter.format("indexId={}, partitionId={}", descriptor.id(), partitionId);
    }

    private <V> V busy(Supplier<V> supplier) {
        if (!busyLock.enterBusy()) {
            throwExceptionDependingOnStorageState(state.get(), createStorageInfo());
        }

        try {
            return supplier.get();
        } finally {
            busyLock.leaveBusy();
        }
    }
}
