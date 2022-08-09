/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.storage.chm;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.ignite.configuration.schemas.table.TableConfiguration;
import org.apache.ignite.internal.storage.MvPartitionStorage;
import org.apache.ignite.internal.storage.StorageException;
import org.apache.ignite.internal.storage.engine.MvTableStorage;
import org.apache.ignite.internal.storage.index.SortedIndexDescriptor;
import org.apache.ignite.internal.storage.index.SortedIndexStorage;
import org.apache.ignite.internal.storage.index.impl.TestSortedIndexStorage;
import org.jetbrains.annotations.Nullable;

/**
 * Test table storage implementation.
 */
public class TestConcurrentHashMapMvTableStorage implements MvTableStorage {
    private final TableConfiguration tableConfig;

    private final Map<Integer, MvPartitionStorage> partitions = new ConcurrentHashMap<>();

    private final Map<String, Indices> sortedIndicesByName = new ConcurrentHashMap<>();

    /**
     * Class for storing Sorted Indices for a particular partition.
     */
    private static class Indices {
        private final SortedIndexDescriptor descriptor;

        final Map<Integer, SortedIndexStorage> storageByPartitionId = new ConcurrentHashMap<>();

        Indices(SortedIndexDescriptor descriptor) {
            this.descriptor = descriptor;
        }

        SortedIndexStorage getOrCreateStorage(Integer partitionId) {
            return storageByPartitionId.computeIfAbsent(partitionId, id -> new TestSortedIndexStorage(descriptor));
        }
    }

    public TestConcurrentHashMapMvTableStorage(TableConfiguration tableCfg) {
        this.tableConfig = tableCfg;
    }

    @Override
    public MvPartitionStorage getOrCreateMvPartition(int partitionId) throws StorageException {
        return partitions.computeIfAbsent(partitionId, TestConcurrentHashMapMvPartitionStorage::new);
    }

    @Override
    @Nullable
    public MvPartitionStorage getMvPartition(int partitionId) {
        return partitions.get(partitionId);
    }

    @Override
    public CompletableFuture<Void> destroyPartition(int partitionId) throws StorageException {
        Integer boxedPartitionId = partitionId;

        partitions.remove(boxedPartitionId);

        sortedIndicesByName.values().forEach(indices -> indices.storageByPartitionId.remove(boxedPartitionId));

        return CompletableFuture.completedFuture(null);
    }

    @Override
    public void createIndex(String indexName) {
        sortedIndicesByName.computeIfAbsent(indexName, name -> {
            var descriptor = new SortedIndexDescriptor(name, tableConfig.value());

            return new Indices(descriptor);
        });
    }

    @Override
    @Nullable
    public SortedIndexStorage getSortedIndex(int partitionId, String indexName) {
        Indices indices = sortedIndicesByName.get(indexName);

        if (indices == null || !partitions.containsKey(partitionId)) {
            return null;
        }

        return indices.getOrCreateStorage(partitionId);
    }

    @Override
    public CompletableFuture<Void> destroyIndex(String indexName) {
        sortedIndicesByName.remove(indexName);

        return CompletableFuture.completedFuture(null);
    }

    @Override
    public boolean isVolatile() {
        return true;
    }

    @Override
    public TableConfiguration configuration() {
        return tableConfig;
    }

    @Override
    public void start() throws StorageException {
    }

    @Override
    public void stop() throws StorageException {
    }

    @Override
    public void destroy() throws StorageException {
    }
}
