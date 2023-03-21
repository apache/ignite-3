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
import static org.apache.ignite.internal.storage.util.StorageUtils.throwExceptionIfStorageNotInCleanupOrRebalancedState;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.pagememory.util.GradualTaskExecutor;
import org.apache.ignite.internal.pagememory.util.PageIdUtils;
import org.apache.ignite.internal.schema.BinaryTuple;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.storage.StorageException;
import org.apache.ignite.internal.storage.StorageRebalanceException;
import org.apache.ignite.internal.storage.index.HashIndexDescriptor;
import org.apache.ignite.internal.storage.index.HashIndexStorage;
import org.apache.ignite.internal.storage.index.IndexRow;
import org.apache.ignite.internal.storage.pagememory.index.freelist.IndexColumns;
import org.apache.ignite.internal.storage.pagememory.index.freelist.IndexColumnsFreeList;
import org.apache.ignite.internal.storage.pagememory.index.meta.IndexMetaKey;
import org.apache.ignite.internal.storage.pagememory.index.meta.IndexMetaTree;
import org.apache.ignite.internal.storage.pagememory.index.meta.UpdateLastBuildRowIdInvokeClosure;
import org.apache.ignite.internal.storage.util.StorageState;
import org.apache.ignite.internal.util.Cursor;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.lang.IgniteInternalCheckedException;
import org.apache.ignite.lang.IgniteStringFormatter;
import org.jetbrains.annotations.Nullable;

/**
 * Implementation of Hash index storage using Page Memory.
 */
public class PageMemoryHashIndexStorage implements HashIndexStorage {
    private static final IgniteLogger LOG = Loggers.forClass(PageMemoryHashIndexStorage.class);

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

    /** Index meta tree instance. */
    private final IndexMetaTree indexMetaTree;

    /**
     * Constructor.
     *
     * @param descriptor Hash index descriptor.
     * @param freeList Free list to store index columns.
     * @param hashIndexTree Hash index tree instance.
     * @param indexMetaTree Index meta tree instance.
     */
    public PageMemoryHashIndexStorage(
            HashIndexDescriptor descriptor,
            IndexColumnsFreeList freeList,
            HashIndexTree hashIndexTree,
            IndexMetaTree indexMetaTree
    ) {
        this.descriptor = descriptor;
        this.freeList = freeList;
        this.hashIndexTree = hashIndexTree;
        this.indexMetaTree = indexMetaTree;

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
            throwExceptionIfStorageInProgressOfRebalance(state.get(), this::createStorageInfo);

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
            throwExceptionIfStorageInProgressOfRebalance(state.get(), this::createStorageInfo);

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
     * Starts destruction of the data stored by this index partition.
     *
     * @param executor {@link GradualTaskExecutor} on which to destroy.
     * @throws StorageException If something goes wrong.
     */
    public void startDestructionOn(GradualTaskExecutor executor) throws StorageException {
        try {
            executor.execute(
                    hashIndexTree.startGradualDestruction(rowKey -> removeIndexColumns((HashIndexRow) rowKey), false)
            ).whenComplete((res, ex) -> {
                if (ex != null) {
                    LOG.error("Hash index " + descriptor.id() + " destruction has failed", ex);
                }
            });
        } catch (IgniteInternalCheckedException e) {
            throw new StorageException("Cannot destroy hash index " + indexDescriptor().id(), e);
        }
    }

    private void removeIndexColumns(HashIndexRow indexRow) {
        if (indexRow.indexColumns().link() != PageIdUtils.NULL_LINK) {
            try {
                freeList.removeDataRowByLink(indexRow.indexColumns().link());
            } catch (IgniteInternalCheckedException e) {
                throw new StorageException("Cannot destroy hash index " + indexDescriptor().id(), e);
            }

            indexRow.indexColumns().link(PageIdUtils.NULL_LINK);
        }
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
     * Updates the internal data structures of the storage on rebalance or cleanup.
     *
     * @param freeList Free list to store index columns.
     * @param hashIndexTree Hash index tree instance.
     * @throws StorageException If failed.
     */
    public void updateDataStructures(IndexColumnsFreeList freeList, HashIndexTree hashIndexTree) {
        throwExceptionIfStorageNotInCleanupOrRebalancedState(state.get(), this::createStorageInfo);

        this.freeList = freeList;
        this.hashIndexTree = hashIndexTree;
    }

    private String createStorageInfo() {
        return IgniteStringFormatter.format("indexId={}, partitionId={}", descriptor.id(), partitionId);
    }

    /**
     * Prepares the storage for cleanup.
     *
     * <p>After cleanup (successful or not), method {@link #finishCleanup()} must be called.
     */
    public void startCleanup() {
        if (!state.compareAndSet(StorageState.RUNNABLE, StorageState.CLEANUP)) {
            throwExceptionDependingOnStorageState(state.get(), createStorageInfo());
        }

        // Changed storage states and expect all storage operations to stop soon.
        busyLock.block();

        try {
            hashIndexTree.close();
        } finally {
            busyLock.unblock();
        }
    }

    /**
     * Finishes cleanup up the storage.
     */
    public void finishCleanup() {
        state.compareAndSet(StorageState.CLEANUP, StorageState.RUNNABLE);
    }

    @Override
    public @Nullable RowId getLastBuildRowId() {
        return busy(() -> {
            throwExceptionIfStorageInProgressOfRebalance(state.get(), this::createStorageInfo);

            try {
                UUID lastBuildRowIdUuid = indexMetaTree.findOne(new IndexMetaKey(indexDescriptor().id())).lastBuildRowIdUuid();

                return lastBuildRowIdUuid == null ? null : new RowId(partitionId, lastBuildRowIdUuid);
            } catch (IgniteInternalCheckedException e) {
                throw new StorageException("Error getting last build row ID: [{}]", e, createStorageInfo());
            }
        });
    }

    @Override
    public void setLastBuildRowId(@Nullable RowId rowId) {
        busy(() -> {
            throwExceptionIfStorageInProgressOfRebalance(state.get(), this::createStorageInfo);

            try {
                indexMetaTree.invoke(new IndexMetaKey(indexDescriptor().id()), null, new UpdateLastBuildRowIdInvokeClosure(rowId));
            } catch (IgniteInternalCheckedException e) {
                throw new StorageException("Error setting last build row ID: [{}, rowId={}]", e, createStorageInfo(), rowId);
            }

            return null;
        });
    }
}
