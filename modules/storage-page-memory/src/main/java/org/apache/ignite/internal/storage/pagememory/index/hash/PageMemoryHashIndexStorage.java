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

import static org.apache.ignite.internal.storage.pagememory.PageMemoryStorageUtils.inBusyLock;
import static org.apache.ignite.internal.storage.pagememory.PageMemoryStorageUtils.throwExceptionDependingOnStorageStateOnRebalance;
import static org.apache.ignite.internal.storage.pagememory.PageMemoryStorageUtils.throwExceptionIfStorageInProgressOfRebalance;
import static org.apache.ignite.internal.storage.pagememory.PageMemoryStorageUtils.throwExceptionIfStorageNotInProgressOfRebalance;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.function.Supplier;
import org.apache.ignite.internal.schema.BinaryTuple;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.storage.StorageException;
import org.apache.ignite.internal.storage.StorageRebalanceException;
import org.apache.ignite.internal.storage.index.HashIndexDescriptor;
import org.apache.ignite.internal.storage.index.HashIndexStorage;
import org.apache.ignite.internal.storage.index.IndexRow;
import org.apache.ignite.internal.storage.pagememory.StorageState;
import org.apache.ignite.internal.storage.pagememory.index.freelist.IndexColumns;
import org.apache.ignite.internal.storage.pagememory.index.freelist.IndexColumnsFreeList;
import org.apache.ignite.internal.util.Cursor;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.lang.IgniteInternalCheckedException;
import org.apache.ignite.lang.IgniteStringFormatter;

/**
 * Implementation of Hash index storage using Page Memory.
 */
public class PageMemoryHashIndexStorage implements HashIndexStorage {
    private static final VarHandle STATE;

    static {
        try {
            STATE = MethodHandles.lookup().findVarHandle(PageMemoryHashIndexStorage.class, "state", StorageState.class);
        } catch (ReflectiveOperationException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

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
    private volatile StorageState state = StorageState.RUNNABLE;

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
        return busy(() -> {
            throwExceptionIfStorageInProgressOfRebalance(state, this::createStorageInfo);

            try {
                IndexColumns indexColumns = new IndexColumns(partitionId, key.byteBuffer());

                HashIndexRow lowerBound = new HashIndexRow(indexColumns, lowestRowId);
                HashIndexRow upperBound = new HashIndexRow(indexColumns, highestRowId);

                Cursor<HashIndexRow> cursor = hashIndexTree.find(lowerBound, upperBound);

                return new Cursor<RowId>() {
                    @Override
                    public void close() {
                        cursor.close();
                    }

                    @Override
                    public boolean hasNext() {
                        return busy(() -> {
                            throwExceptionIfStorageInProgressOfRebalance(state, PageMemoryHashIndexStorage.this::createStorageInfo);

                            return cursor.hasNext();
                        });
                    }

                    @Override
                    public RowId next() {
                        return busy(() -> {
                            throwExceptionIfStorageInProgressOfRebalance(state, PageMemoryHashIndexStorage.this::createStorageInfo);

                            return cursor.next().rowId();
                        });
                    }
                };
            } catch (Throwable e) {
                throw new StorageException("Failed to create scan cursor", e);
            }
        });
    }

    @Override
    public void put(IndexRow row) throws StorageException {
        busy(() -> {
            try {
                IndexColumns indexColumns = new IndexColumns(partitionId, row.indexColumns().byteBuffer());

                HashIndexRow hashIndexRow = new HashIndexRow(indexColumns, row.rowId());

                var insert = new InsertHashIndexRowInvokeClosure(hashIndexRow, freeList, hashIndexTree.inlineSize());

                hashIndexTree.invoke(hashIndexRow, null, insert);

                return null;
            } catch (IgniteInternalCheckedException e) {
                throw new StorageException("Failed to put value into index", e);
            }
        });
    }

    @Override
    public void remove(IndexRow row) throws StorageException {
        busy(() -> {
            throwExceptionIfStorageInProgressOfRebalance(state, this::createStorageInfo);

            try {
                IndexColumns indexColumns = new IndexColumns(partitionId, row.indexColumns().byteBuffer());

                HashIndexRow hashIndexRow = new HashIndexRow(indexColumns, row.rowId());

                var remove = new RemoveHashIndexRowInvokeClosure(hashIndexRow, freeList);

                hashIndexTree.invoke(hashIndexRow, null, remove);

                // Performs actual deletion from freeList if necessary.
                remove.afterCompletion();

                return null;
            } catch (IgniteInternalCheckedException e) {
                throw new StorageException("Failed to remove value from index", e);
            }
        });
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
        if (!STATE.compareAndSet(this, StorageState.RUNNABLE, StorageState.CLOSED)) {
            StorageState state = this.state;

            assert state == StorageState.CLOSED : state;

            return;
        }

        busyLock.block();

        hashIndexTree.close();
    }

    /**
     * Prepares storage for rebalancing.
     *
     * <p>Stops ongoing index operations.
     *
     * @throws StorageRebalanceException If there was an error when starting the rebalance.
     */
    public void startRebalance() {
        if (!STATE.compareAndSet(this, StorageState.RUNNABLE, StorageState.REBALANCE)) {
            throwExceptionDependingOnStorageStateOnRebalance(state, createStorageInfo());
        }

        // Stops ongoing operations on the storage.
        busyLock.block();
        busyLock.unblock();
    }

    /**
     * Completes the rebalancing of the storage.
     *
     * @throws StorageRebalanceException If there is an error while completing the storage rebalance.
     */
    public void completeRebalance() {
        if (!STATE.compareAndSet(this, StorageState.REBALANCE, StorageState.RUNNABLE)) {
            throwExceptionDependingOnStorageStateOnRebalance(state, createStorageInfo());
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
        throwExceptionIfStorageNotInProgressOfRebalance(state, this::createStorageInfo);

        this.freeList = freeList;

        this.hashIndexTree.close();
        this.hashIndexTree = hashIndexTree;
    }

    private String createStorageInfo() {
        return IgniteStringFormatter.format("indexId={}, partitionId={}", descriptor.id(), partitionId);
    }

    private <V> V busy(Supplier<V> supplier) {
        return inBusyLock(busyLock, supplier, () -> state, this::createStorageInfo);
    }
}
