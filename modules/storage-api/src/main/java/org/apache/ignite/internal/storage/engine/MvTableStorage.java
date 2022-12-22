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

import static org.apache.ignite.internal.schema.configuration.index.TableIndexConfigurationSchema.HASH_INDEX_TYPE;
import static org.apache.ignite.internal.schema.configuration.index.TableIndexConfigurationSchema.SORTED_INDEX_TYPE;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.close.ManuallyCloseable;
import org.apache.ignite.internal.configuration.util.ConfigurationUtil;
import org.apache.ignite.internal.schema.configuration.TableConfiguration;
import org.apache.ignite.internal.schema.configuration.TablesConfiguration;
import org.apache.ignite.internal.schema.configuration.index.TableIndexConfiguration;
import org.apache.ignite.internal.storage.MvPartitionStorage;
import org.apache.ignite.internal.storage.StorageException;
import org.apache.ignite.internal.storage.StorageFullRebalanceException;
import org.apache.ignite.internal.storage.index.HashIndexStorage;
import org.apache.ignite.internal.storage.index.IndexStorage;
import org.apache.ignite.internal.storage.index.SortedIndexStorage;
import org.apache.ignite.internal.util.Cursor;
import org.jetbrains.annotations.Nullable;

/**
 * Table storage that contains meta, partitions and SQL indexes.
 */
public interface MvTableStorage extends ManuallyCloseable {
    /**
     * Retrieves or creates a partition for the current table. Not expected to be called concurrently with the same Partition ID.
     *
     * @param partitionId Partition ID.
     * @return Partition storage.
     * @throws IllegalArgumentException If Partition ID is out of configured bounds.
     * @throws StorageException If an error has occurred during the partition creation.
     */
    MvPartitionStorage getOrCreateMvPartition(int partitionId) throws StorageException;

    /**
     * Returns the partition storage or {@code null} if the requested storage doesn't exist.
     *
     * @param partitionId Partition ID.
     * @return Partition storage or {@code null} if it does not exist.
     * @throws IllegalArgumentException If Partition ID is out of configured bounds.
     */
    @Nullable MvPartitionStorage getMvPartition(int partitionId);

    /**
     * Destroys a partition and all associated indices.
     *
     * @param partitionId Partition ID.
     * @return Future that will complete when the destroy of the partition is completed.
     * @throws IllegalArgumentException If Partition ID is out of bounds.
     */
    CompletableFuture<Void> destroyPartition(int partitionId) throws StorageException;

    /**
     * Returns an already created Index (either Sorted or Hash) with the given name or creates a new one if it does not exist.
     *
     * @param partitionId Partition ID.
     * @param indexId Index ID.
     * @return Index Storage.
     * @throws StorageException If the given partition does not exist, or if the given index does not exist.
     */
    default IndexStorage getOrCreateIndex(int partitionId, UUID indexId) {
        TableIndexConfiguration indexConfig = ConfigurationUtil.getByInternalId(tablesConfiguration().indexes(), indexId);

        if (indexConfig == null) {
            throw new StorageException(String.format("Index configuration for \"%s\" could not be found", indexId));
        }

        switch (indexConfig.type().value()) {
            case HASH_INDEX_TYPE:
                return getOrCreateHashIndex(partitionId, indexId);
            case SORTED_INDEX_TYPE:
                return getOrCreateSortedIndex(partitionId, indexId);
            default:
                throw new StorageException("Unknown index type: " + indexConfig.type().value());
        }
    }

    /**
     * Returns an already created Sorted Index with the given name or creates a new one if it does not exist.
     *
     * <p>In order for an index to be created, it should be already configured under the same name in the Table Configuration
     * (see {@link #configuration()}).
     *
     * @param partitionId Partition ID for which this index has been configured.
     * @param indexId Index ID.
     * @return Sorted Index storage.
     * @throws StorageException If the given partition does not exist, or if the given index does not exist or is not configured as
     *         a sorted index.
     */
    SortedIndexStorage getOrCreateSortedIndex(int partitionId, UUID indexId);

    /**
     * Returns an already created Hash Index with the given name or creates a new one if it does not exist.
     *
     * <p>In order for an index to be created, it should be already configured under the same name in the Table Configuration
     * (see {@link #configuration()}).
     *
     * @param partitionId Partition ID for which this index has been configured.
     * @param indexId Index ID.
     * @return Hash Index storage.
     * @throws StorageException If the given partition does not exist, or the given index does not exist or is not configured as a
     *         hash index.
     */
    HashIndexStorage getOrCreateHashIndex(int partitionId, UUID indexId);

    /**
     * Destroys the index under the given name and all data in it.
     *
     * <p>This method is a no-op if the index under the given name does not exist.
     *
     * @param indexId Index ID.
     */
    CompletableFuture<Void> destroyIndex(UUID indexId);

    /**
     * Returns {@code true} if this storage is volatile (i.e. stores its data in memory), or {@code false} if it's persistent.
     */
    boolean isVolatile();

    /**
     * Returns the table configuration.
     */
    TableConfiguration configuration();

    /**
     * Returns configuration for all tables and indices.
     */
    TablesConfiguration tablesConfiguration();

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
     * @return Future that will complete when the table destruction is complete.
     */
    CompletableFuture<Void> destroy();

    /**
     * Prepares a partition for a full rebalance.
     * <ul>
     *     <li>Cleans up the {@link MvPartitionStorage multi-version partition storage} and its associated indexes ({@link HashIndexStorage}
     *     and {@link SortedIndexStorage});</li>
     *     <li>Sets {@link MvPartitionStorage#lastAppliedIndex()} and {@link MvPartitionStorage#lastAppliedTerm()} to
     *     {@link MvPartitionStorage#FULL_REBALANCE_IN_PROGRESS};</li>
     *     <li>Stops the cursors of a multi-version partition storage and its indexes, subsequent calls to {@link Cursor#hasNext()} and
     *     {@link Cursor#next()} will throw {@link StorageFullRebalanceException};</li>
     *     <li>For a multi-version partition storage and its indexes, only write methods will be available, while read and
     *     {@link MvPartitionStorage#lastApplied(long, long)} methods will throw {@link StorageFullRebalanceException}.</li>
     * </ul>
     *
     * <p>This method must be called before every full rebalance of a multi-version partition storage and its indexes and ends with a call
     * to one of the methods:
     * <ul>
     *     <li>{@link #abortFullRebalancePartition(int)} ()} - in case of errors or cancellation of a full rebalance;</li>
     *     <li>{@link #finishFullRebalancePartition(int, long, long)} - in case of successful completion of a full rebalance.</li>
     * </ul>
     *
     * <p>If the {@link MvPartitionStorage#lastAppliedIndex()} is {@link MvPartitionStorage#FULL_REBALANCE_IN_PROGRESS} after a node restart
     * , then a multi-version partition storage and its indexes needs to be cleared before they start.
     *
     * @param partitionId Partition ID.
     * @return Future of the start a full rebalance for a multi-version partition storage and its indexes.
     * @throws StorageFullRebalanceException If there is an error when starting a full rebalance.
     */
    CompletableFuture<Void> startFullRebalancePartition(int partitionId);

    /**
     * Aborts full rebalance for a partition.
     * <ul>
     *     <li>Cleans up the {@link MvPartitionStorage multi-version partition storage} and its associated indexes ({@link HashIndexStorage}
     *     and {@link SortedIndexStorage});</li>
     *     <li>Sets {@link MvPartitionStorage#lastAppliedIndex()} and {@link MvPartitionStorage#lastAppliedTerm()} to {@code 0};</li>
     *     <li>For a multi-version partition storage and its indexes, methods for writing and reading will be available.</li>
     * </ul>
     *
     * <p>If a full rebalance has not started, then nothing will happen.
     *
     * @return Future of the abort a full rebalance for a multi-version partition storage and its indexes.
     */
    CompletableFuture<Void> abortFullRebalancePartition(int partitionId);

    /**
     * Completes a full rebalance for a partition.
     * <ul>
     *     <li>Updates {@link MvPartitionStorage#lastAppliedIndex()} and {@link MvPartitionStorage#lastAppliedTerm()};</li>
     *     <li>For a multi-version partition storage and its indexes, methods for writing and reading will be available.</li>
     * </ul>
     *
     * <p>If a full rebalance has not started, then {@link StorageFullRebalanceException} will be thrown.
     *
     * @param lastAppliedIndex Last applied index.
     * @param lastAppliedTerm Last applied term.
     * @return Future of the finish a full rebalance for a multi-version partition storage and its indexes.
     * @throws StorageFullRebalanceException If there is an error when completing a full rebalance.
     */
    CompletableFuture<Void> finishFullRebalancePartition(int partitionId, long lastAppliedIndex, long lastAppliedTerm);
}
