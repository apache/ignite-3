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

package org.apache.ignite.internal.storage.engine;

import static org.apache.ignite.internal.worker.ThreadAssertions.assertThreadAllowsToWrite;

import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.storage.MvPartitionStorage;
import org.apache.ignite.internal.storage.StorageException;
import org.apache.ignite.internal.storage.ThreadAssertingMvPartitionStorage;
import org.apache.ignite.internal.storage.index.HashIndexStorage;
import org.apache.ignite.internal.storage.index.IndexStorage;
import org.apache.ignite.internal.storage.index.SortedIndexStorage;
import org.apache.ignite.internal.storage.index.StorageHashIndexDescriptor;
import org.apache.ignite.internal.storage.index.StorageSortedIndexDescriptor;
import org.apache.ignite.internal.storage.index.ThreadAssertingHashIndexStorage;
import org.apache.ignite.internal.storage.index.ThreadAssertingSortedIndexStorage;
import org.apache.ignite.internal.worker.ThreadAssertions;
import org.jetbrains.annotations.Nullable;

/**
 * {@link MvTableStorage} that performs thread assertions when doing read/write operations and wraps substorages it
 * creates to guarantee the same behavior for them.
 *
 * @see ThreadAssertions
 */
public class ThreadAssertingMvTableStorage implements MvTableStorage {
    private final MvTableStorage tableStorage;

    /** Constructor. */
    public ThreadAssertingMvTableStorage(MvTableStorage tableStorage) {
        this.tableStorage = tableStorage;
    }

    @Override
    public void close() throws Exception {
        tableStorage.close();
    }

    @Override
    public CompletableFuture<MvPartitionStorage> createMvPartition(int partitionId) {
        assertThreadAllowsToWrite();

        return tableStorage.createMvPartition(partitionId)
                .thenApply(ThreadAssertingMvPartitionStorage::new);
    }

    @Override
    public @Nullable MvPartitionStorage getMvPartition(int partitionId) {
        MvPartitionStorage partition = tableStorage.getMvPartition(partitionId);
        return partition == null ? null : new ThreadAssertingMvPartitionStorage(partition);
    }

    @Override
    public CompletableFuture<Void> destroyPartition(int partitionId) throws StorageException {
        assertThreadAllowsToWrite();

        return tableStorage.destroyPartition(partitionId);
    }

    @Override
    public SortedIndexStorage getOrCreateSortedIndex(int partitionId, StorageSortedIndexDescriptor indexDescriptor) {
        assertThreadAllowsToWrite();

        SortedIndexStorage indexStorage = tableStorage.getOrCreateSortedIndex(partitionId, indexDescriptor);
        return new ThreadAssertingSortedIndexStorage(indexStorage);
    }

    @Override
    public HashIndexStorage getOrCreateHashIndex(int partitionId, StorageHashIndexDescriptor indexDescriptor) {
        assertThreadAllowsToWrite();

        HashIndexStorage indexStorage = tableStorage.getOrCreateHashIndex(partitionId, indexDescriptor);
        return new ThreadAssertingHashIndexStorage(indexStorage);
    }

    @Override
    public CompletableFuture<Void> destroyIndex(int indexId) {
        assertThreadAllowsToWrite();

        return tableStorage.destroyIndex(indexId);
    }

    @Override
    public boolean isVolatile() {
        return tableStorage.isVolatile();
    }

    @Override
    public CompletableFuture<Void> destroy() {
        assertThreadAllowsToWrite();

        return tableStorage.destroy();
    }

    @Override
    public CompletableFuture<Void> startRebalancePartition(int partitionId) {
        assertThreadAllowsToWrite();

        return tableStorage.startRebalancePartition(partitionId);
    }

    @Override
    public CompletableFuture<Void> abortRebalancePartition(int partitionId) {
        assertThreadAllowsToWrite();

        return tableStorage.abortRebalancePartition(partitionId);
    }

    @Override
    public CompletableFuture<Void> finishRebalancePartition(
            int partitionId,
            long lastAppliedIndex,
            long lastAppliedTerm,
            byte[] groupConfig
    ) {
        assertThreadAllowsToWrite();

        return tableStorage.finishRebalancePartition(partitionId, lastAppliedIndex, lastAppliedTerm, groupConfig);
    }

    @Override
    public CompletableFuture<Void> clearPartition(int partitionId) {
        assertThreadAllowsToWrite();

        return tableStorage.clearPartition(partitionId);
    }

    @Override
    public @Nullable IndexStorage getIndex(int partitionId, int indexId) {
        return tableStorage.getIndex(partitionId, indexId);
    }

    @Override
    public StorageTableDescriptor getTableDescriptor() {
        return tableStorage.getTableDescriptor();
    }
}
