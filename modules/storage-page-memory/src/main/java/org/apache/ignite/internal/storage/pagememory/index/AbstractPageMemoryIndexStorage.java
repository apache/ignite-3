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


package org.apache.ignite.internal.storage.pagememory.index;

import static org.apache.ignite.internal.storage.util.StorageUtils.throwExceptionDependingOnStorageState;
import static org.apache.ignite.internal.storage.util.StorageUtils.throwExceptionDependingOnStorageStateOnRebalance;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.storage.StorageRebalanceException;
import org.apache.ignite.internal.storage.index.IndexStorage;
import org.apache.ignite.internal.storage.pagememory.index.freelist.IndexColumnsFreeList;
import org.apache.ignite.internal.storage.util.StorageState;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.lang.IgniteStringFormatter;
import org.jetbrains.annotations.Nullable;

/**
 * Abstract index storage based on Page Memory.
 */
public abstract class AbstractPageMemoryIndexStorage implements IndexStorage {
    /** Index ID. */
    protected final UUID indexId;

    /** Partition id. */
    protected final int partitionId;

    /** Lowest possible RowId according to signed long ordering. */
    protected final RowId lowestRowId;

    /** Highest possible RowId according to signed long ordering. */
    protected final RowId highestRowId;

    /** Free list to store index columns. */
    protected volatile IndexColumnsFreeList freeList;

    /** Busy lock. */
    private final IgniteSpinBusyLock busyLock = new IgniteSpinBusyLock();

    /** Current state of the storage. */
    protected final AtomicReference<StorageState> state = new AtomicReference<>(StorageState.RUNNABLE);

    protected AbstractPageMemoryIndexStorage(UUID indexId, int partitionId, IndexColumnsFreeList freeList) {
        this.indexId = indexId;
        this.partitionId = partitionId;
        this.freeList = freeList;

        lowestRowId = RowId.lowestRowId(partitionId);

        highestRowId = RowId.highestRowId(partitionId);
    }

    @Override
    public @Nullable RowId getNextRowIdToBuild() {
        // TODO: IGNITE-19119 реализовать
        return null;
    }

    @Override
    public void setNextRowIdToBuild(@Nullable RowId rowId) {
        // TODO: IGNITE-19119 реализовать
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

        close0();
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
            close0();
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
            close0();
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

    protected <V> V busy(Supplier<V> supplier) {
        if (!busyLock.enterBusy()) {
            throwExceptionDependingOnStorageState(state.get(), createStorageInfo());
        }

        try {
            return supplier.get();
        } finally {
            busyLock.leaveBusy();
        }
    }

    protected String createStorageInfo() {
        return IgniteStringFormatter.format("indexId={}, partitionId={}", indexId, partitionId);
    }

    /**
     * Closes internal structures.
     */
    protected abstract void close0();
}
