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

package org.apache.ignite.internal.storage.engine;

import java.util.concurrent.CompletableFuture;
import org.apache.ignite.configuration.schemas.table.TableConfiguration;
import org.apache.ignite.internal.storage.MvPartitionStorage;
import org.apache.ignite.internal.storage.StorageException;
import org.apache.ignite.internal.storage.index.HashIndexStorage;
import org.apache.ignite.internal.storage.index.SortedIndexStorage;
import org.jetbrains.annotations.Nullable;

/**
 * Table storage that contains meta, partitions and SQL indexes.
 */
public interface MvTableStorage {
    /**
     * Retrieves or creates a partition for the current table. Not expected to be called concurrently with the same partition id.
     *
     * @param partitionId Partition id.
     * @return Partition storage.
     * @throws IllegalArgumentException If partition id is out of configured bounds.
     * @throws StorageException If an error has occurred during the partition creation.
     */
    MvPartitionStorage getOrCreateMvPartition(int partitionId) throws StorageException;

    /**
     * Returns the partition storage or {@code null} if the requested storage doesn't exist.
     *
     * @param partitionId Partition id.
     * @return Partition storage or {@code null} if it does not exist.
     * @throws IllegalArgumentException If partition id is out of configured bounds.
     */
    @Nullable MvPartitionStorage getMvPartition(int partitionId);

    /**
     * Destroys a partition and all associated indices.
     *
     * @param partitionId Partition id.
     * @throws IllegalArgumentException If partition id is out of bounds.
     * @throws StorageException If an error has occurred during the partition destruction.
     */
    CompletableFuture<Void> destroyPartition(int partitionId) throws StorageException;

    /**
     * Returns an already created Sorted Index with the given name or creates a new one if it does not exist.
     *
     * <p>In order for an index to be created, it should be already configured under the same name in the Table Configuration
     * (see {@link #configuration()}).
     *
     * @param partitionId Partition ID for which this index has been configured.
     * @param indexName Index name.
     * @return Sorted Index storage.
     * @throws StorageException If the given partition does not exist, or if the given index does not exist or is not configured as
     *         a sorted index.
     */
    SortedIndexStorage getOrCreateSortedIndex(int partitionId, String indexName);

    /**
     * Returns an already created Hash Index with the given name or creates a new one if it does not exist.
     *
     * <p>In order for an index to be created, it should be already configured under the same name in the Table Configuration
     * (see {@link #configuration()}).
     *
     * @param partitionId Partition ID for which this index has been configured.
     * @param indexName Index name.
     * @return Hash Index storage.
     * @throws StorageException If the given partition does not exist, or the given index does not exist or is not configured as a
     *         hash index.
     */
    HashIndexStorage getOrCreateHashIndex(int partitionId, String indexName);

    /**
     * Destroys the index under the given name and all data in it.
     *
     * <p>This method is a no-op if the index under the given name does not exist.
     *
     * @param indexName Index name.
     */
    CompletableFuture<Void> destroyIndex(String indexName);

    /**
     * Returns {@code true} if this storage is volatile (i.e. stores its data in memory), or {@code false} if it's persistent.
     */
    boolean isVolatile();

    /**
     * Returns the table configuration.
     */
    TableConfiguration configuration();

    /**
     * Starts the storage.
     *
     * @throws StorageException If an error has occurred during the start of the storage.
     */
    void start() throws StorageException;

    /**
     * Stops the storage.
     *
     * @throws StorageException If an error has occurred during the stop of the storage.
     */
    void stop() throws StorageException;

    /**
     * Stops and destroys the storage and cleans all allocated resources.
     *
     * @throws StorageException If an error has occurred during the destruction of the storage.
     */
    void destroy() throws StorageException;
}
