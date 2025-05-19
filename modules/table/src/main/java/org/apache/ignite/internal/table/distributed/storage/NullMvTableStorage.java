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

package org.apache.ignite.internal.table.distributed.storage;

import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.storage.MvPartitionStorage;
import org.apache.ignite.internal.storage.StorageException;
import org.apache.ignite.internal.storage.engine.MvPartitionMeta;
import org.apache.ignite.internal.storage.engine.MvTableStorage;
import org.apache.ignite.internal.storage.engine.StorageTableDescriptor;
import org.apache.ignite.internal.storage.index.IndexStorage;
import org.apache.ignite.internal.storage.index.StorageHashIndexDescriptor;
import org.apache.ignite.internal.storage.index.StorageSortedIndexDescriptor;
import org.jetbrains.annotations.Nullable;

/**
 * {@link MvTableStorage} implementation used when the node currently does not have the storage profile declared for the table
 * and/or the corresponding storage engine. Such an object is used as a placeholder to have ability to create a table object,
 * but any storage functionality will be absent (exceptions will be thrown).
 *
 * <p>Before a node starts getting assignments for partitions of such a table, both storage engine and storage profile must be created
 * on the node and this placeholder object must be replaced with a 'real' storage object.
 */
public class NullMvTableStorage implements MvTableStorage {
    private final StorageTableDescriptor tableDescriptor;

    /** Constructor. */
    public NullMvTableStorage(StorageTableDescriptor tableDescriptor) {
        this.tableDescriptor = tableDescriptor;
    }

    @Override
    public CompletableFuture<MvPartitionStorage> createMvPartition(int partitionId) {
        return throwNoPartitionsException();
    }

    @Override
    public @Nullable MvPartitionStorage getMvPartition(int partitionId) {
        return throwNoPartitionsException();
    }

    @Override
    public CompletableFuture<Void> destroyPartition(int partitionId) throws StorageException {
        return throwNoPartitionsException();
    }

    @Override
    public void createSortedIndex(int partitionId, StorageSortedIndexDescriptor indexDescriptor) {
        throwNoPartitionsException();
    }

    @Override
    public void createHashIndex(int partitionId, StorageHashIndexDescriptor indexDescriptor) {
        throwNoPartitionsException();
    }

    @Override
    public CompletableFuture<Void> destroyIndex(int indexId) {
        return throwNoStorageEngineException("Table uses an unknown storage profile or engine, so current node either should not receive "
                + "any index destruction events, or storage profile addition is not handled properly");
    }

    @Override
    public boolean isVolatile() {
        return throwNoPartitionsException();
    }

    @Override
    public CompletableFuture<Void> destroy() {
        return throwNoStorageEngineException("Table uses an unknown storage profile or engine, so current node either should not receive "
                + "any table destruction events, or storage profile addition is not handled properly");
    }

    @Override
    public CompletableFuture<Void> startRebalancePartition(int partitionId) {
        return throwNoPartitionsException();
    }

    @Override
    public CompletableFuture<Void> abortRebalancePartition(int partitionId) {
        return throwNoPartitionsException();
    }

    @Override
    public CompletableFuture<Void> finishRebalancePartition(int partitionId, MvPartitionMeta partitionMeta) {
        return throwNoPartitionsException();
    }

    @Override
    public CompletableFuture<Void> clearPartition(int partitionId) {
        return throwNoPartitionsException();
    }

    @Override
    public @Nullable IndexStorage getIndex(int partitionId, int indexId) {
        return throwNoPartitionsException();
    }

    @Override
    public StorageTableDescriptor getTableDescriptor() {
        return tableDescriptor;
    }

    @Override
    public void close() throws Exception {
        // No-op.
    }

    private <T> T throwNoPartitionsException() {
        return throwNoStorageEngineException("Table uses an unknown storage profile or engine, so current node either should not receive "
                + "any assignments, or storage profile addition is not handled properly");
    }

    private <T> T throwNoStorageEngineException(String errorMessage) {
        throw new StorageException(errorMessage + " [tableId=" + tableDescriptor.getId() + "]");
    }
}
