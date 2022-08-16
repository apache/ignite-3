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

import static org.apache.ignite.configuration.schemas.table.TableIndexConfigurationSchema.HASH_INDEX_TYPE;
import static org.apache.ignite.configuration.schemas.table.TableIndexConfigurationSchema.SORTED_INDEX_TYPE;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.ignite.configuration.schemas.table.TableConfiguration;
import org.apache.ignite.configuration.schemas.table.TableIndexView;
import org.apache.ignite.internal.storage.MvPartitionStorage;
import org.apache.ignite.internal.storage.StorageException;
import org.apache.ignite.internal.storage.engine.MvTableStorage;
import org.apache.ignite.internal.storage.index.HashIndexDescriptor;
import org.apache.ignite.internal.storage.index.HashIndexStorage;
import org.apache.ignite.internal.storage.index.SortedIndexDescriptor;
import org.apache.ignite.internal.storage.index.SortedIndexStorage;
import org.apache.ignite.internal.storage.index.impl.TestHashIndexStorage;
import org.apache.ignite.internal.storage.index.impl.TestSortedIndexStorage;
import org.jetbrains.annotations.Nullable;

/**
 * Test table storage implementation.
 */
public class TestConcurrentHashMapMvTableStorage implements MvTableStorage {
    private final TableConfiguration tableConfig;

    private final Map<Integer, MvPartitionStorage> partitions = new ConcurrentHashMap<>();

    private final Map<String, SortedIndices> sortedIndicesByName = new ConcurrentHashMap<>();

    private final Map<String, HashIndices> hashIndicesByName = new ConcurrentHashMap<>();

    /**
     * Class for storing Sorted Indices for a particular partition.
     */
    private static class SortedIndices {
        private final SortedIndexDescriptor descriptor;

        final Map<Integer, SortedIndexStorage> storageByPartitionId = new ConcurrentHashMap<>();

        SortedIndices(SortedIndexDescriptor descriptor) {
            this.descriptor = descriptor;
        }

        SortedIndexStorage getOrCreateStorage(Integer partitionId) {
            return storageByPartitionId.computeIfAbsent(partitionId, id -> new TestSortedIndexStorage(descriptor));
        }
    }

    /**
     * Class for storing Hash Indices for a particular partition.
     */
    private static class HashIndices {
        private final HashIndexDescriptor descriptor;

        final Map<Integer, HashIndexStorage> storageByPartitionId = new ConcurrentHashMap<>();

        HashIndices(HashIndexDescriptor descriptor) {
            this.descriptor = descriptor;
        }

        HashIndexStorage getOrCreateStorage(Integer partitionId) {
            return storageByPartitionId.computeIfAbsent(partitionId, id -> new TestHashIndexStorage(descriptor));
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
        hashIndicesByName.values().forEach(indices -> indices.storageByPartitionId.remove(boxedPartitionId));

        return CompletableFuture.completedFuture(null);
    }

    @Override
    public void createIndex(String indexName) {
        TableIndexView indexConfig = tableConfig.value().indices().get(indexName);

        switch (indexConfig.type()) {
            case HASH_INDEX_TYPE:
                hashIndicesByName.computeIfAbsent(
                        indexName,
                        name -> new HashIndices(new HashIndexDescriptor(name, tableConfig.value()))
                );

                break;
            case SORTED_INDEX_TYPE:
                sortedIndicesByName.computeIfAbsent(
                        indexName,
                        name -> new SortedIndices(new SortedIndexDescriptor(name, tableConfig.value()))
                );

                break;
            default:
                throw new StorageException("Unknown index type: " + indexConfig.type());
        }
    }

    @Override
    @Nullable
    public SortedIndexStorage getSortedIndex(int partitionId, String indexName) {
        SortedIndices sortedIndices = sortedIndicesByName.get(indexName);

        if (sortedIndices == null || !partitions.containsKey(partitionId)) {
            return null;
        }

        return sortedIndices.getOrCreateStorage(partitionId);
    }

    @Override
    @Nullable
    public HashIndexStorage getHashIndex(int partitionId, String indexName) {
        HashIndices hashIndices = hashIndicesByName.get(indexName);

        if (hashIndices == null || !partitions.containsKey(partitionId)) {
            return null;
        }

        return hashIndices.getOrCreateStorage(partitionId);
    }

    @Override
    public CompletableFuture<Void> destroyIndex(String indexName) {
        sortedIndicesByName.remove(indexName);
        hashIndicesByName.remove(indexName);

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
