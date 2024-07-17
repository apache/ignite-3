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

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.close.ManuallyCloseable;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.storage.MvPartitionStorage;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.storage.StorageClosedException;
import org.apache.ignite.internal.storage.StorageException;
import org.apache.ignite.internal.storage.StorageRebalanceException;
import org.apache.ignite.internal.storage.index.HashIndexStorage;
import org.apache.ignite.internal.storage.index.IndexRow;
import org.apache.ignite.internal.storage.index.IndexStorage;
import org.apache.ignite.internal.storage.index.SortedIndexStorage;
import org.apache.ignite.internal.storage.index.StorageHashIndexDescriptor;
import org.apache.ignite.internal.storage.index.StorageIndexDescriptor;
import org.apache.ignite.internal.storage.index.StorageSortedIndexDescriptor;
import org.apache.ignite.internal.util.Cursor;
import org.jetbrains.annotations.Nullable;

/**
 * Table storage that contains meta, partitions and SQL indexes.
 */
public interface MvTableStorage extends ManuallyCloseable {
    /**
     * Creates a partition for the current table.
     *
     * <p>If the partition has already been created or is in the process of being created, a {@link StorageException} will be thrown.
     *
     * <p>If the partition is in the process of being destroyed, it will be re-created after the destruction is completed.
     *
     * @param partitionId Partition ID.
     * @return Future that will be completed when the partition creation completes.
     * @throws IllegalArgumentException If Partition ID is out of configured bounds.
     * @throws StorageException If an error has occurred during the partition creation.
     */
    CompletableFuture<MvPartitionStorage> createMvPartition(int partitionId);

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
     * <p>REQUIRED: For background tasks for partition, such as rebalancing, to be completed by the time the method is called.
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
     * @param indexDescriptor Index descriptor.
     * @return Index Storage.
     * @throws StorageException If the given partition does not exist, or if the given index does not exist.
     */
    default IndexStorage getOrCreateIndex(int partitionId, StorageIndexDescriptor indexDescriptor) {
        if (indexDescriptor instanceof StorageHashIndexDescriptor) {
            return getOrCreateHashIndex(partitionId, (StorageHashIndexDescriptor) indexDescriptor);
        } else if (indexDescriptor instanceof StorageSortedIndexDescriptor) {
            return getOrCreateSortedIndex(partitionId, (StorageSortedIndexDescriptor) indexDescriptor);
        } else {
            throw new StorageException("Unknown index type: " + indexDescriptor);
        }
    }

    /**
     * Returns an already created Sorted Index with the given name or creates a new one if it does not exist.
     *
     * @param partitionId Partition ID for which this index has been configured.
     * @param indexDescriptor Index descriptor.
     * @return Sorted Index storage.
     * @throws StorageException If the given partition does not exist, or if the given index does not exist or is not configured as
     *         a sorted index.
     */
    SortedIndexStorage getOrCreateSortedIndex(int partitionId, StorageSortedIndexDescriptor indexDescriptor);

    /**
     * Returns an already created Hash Index with the given name or creates a new one if it does not exist.
     *
     * @param partitionId Partition ID for which this index has been configured.
     * @param indexDescriptor Index descriptor.
     * @return Hash Index storage.
     * @throws StorageException If the given partition does not exist, or the given index does not exist or is not configured as a
     *         hash index.
     */
    HashIndexStorage getOrCreateHashIndex(int partitionId, StorageHashIndexDescriptor indexDescriptor);

    /**
     * Destroys the index with the given ID and all data in it.
     *
     * <p>This method is a no-op if the index with the given ID does not exist or if the table storage is closed or under destruction.
     *
     * @param indexId Index ID.
     */
    CompletableFuture<Void> destroyIndex(int indexId);

    /**
     * Returns {@code true} if this storage is volatile (i.e. stores its data in memory), or {@code false} if it's persistent.
     */
    boolean isVolatile();

    /**
     * Stops and destroys the storage and cleans all allocated resources.
     *
     * @return Future that will complete when the table destruction is complete.
     */
    CompletableFuture<Void> destroy();

    /**
     * Prepares a partition for rebalance.
     * <ul>
     *     <li>Cleans up the {@link MvPartitionStorage multi-version partition storage} and its associated indexes ({@link HashIndexStorage}
     *     and {@link SortedIndexStorage});</li>
     *     <li>Sets {@link MvPartitionStorage#lastAppliedIndex()}, {@link MvPartitionStorage#lastAppliedTerm()} to
     *     {@link MvPartitionStorage#REBALANCE_IN_PROGRESS} and {@link MvPartitionStorage#committedGroupConfiguration()} to {@code null};
     *     </li>
     *     <li>Stops the cursors of a multi-version partition storage and its indexes, subsequent calls to {@link Cursor#hasNext()} and
     *     {@link Cursor#next()} will throw {@link StorageRebalanceException};</li>
     *     <li>For a multi-version partition storage and its indexes, methods for reading and writing data will throw
     *     {@link StorageRebalanceException} except:<ul>
     *         <li>{@link MvPartitionStorage#addWrite(RowId, BinaryRow, UUID, int, int)};</li>
     *         <li>{@link MvPartitionStorage#commitWrite(RowId, HybridTimestamp)};</li>
     *         <li>{@link MvPartitionStorage#addWriteCommitted(RowId, BinaryRow, HybridTimestamp)};</li>
     *         <li>{@link MvPartitionStorage#lastAppliedIndex()};</li>
     *         <li>{@link MvPartitionStorage#lastAppliedTerm()};</li>
     *         <li>{@link MvPartitionStorage#committedGroupConfiguration()};</li>
     *         <li>{@link HashIndexStorage#put(IndexRow)};</li>
     *         <li>{@link SortedIndexStorage#put(IndexRow)};</li>
     *     </ul></li>
     * </ul>
     *
     * <p>This method must be called before every rebalance of a multi-version partition storage and its indexes and ends with a call
     * to one of the methods:
     * <ul>
     *     <li>{@link #abortRebalancePartition(int)} ()} - in case of errors or cancellation of rebalance;</li>
     *     <li>{@link #finishRebalancePartition(int, long, long, byte[])} - in case of successful completion of rebalance.
     *     </li>
     * </ul>
     *
     * <p>If the {@link MvPartitionStorage#lastAppliedIndex()} is {@link MvPartitionStorage#REBALANCE_IN_PROGRESS} after a node restart
     * , then a multi-version partition storage and its indexes needs to be cleared before they start.
     *
     * <p>If the partition started to be destroyed or closed, then there will be an error when trying to start rebalancing.
     *
     * @param partitionId Partition ID.
     * @return Future of the start rebalance for a multi-version partition storage and its indexes.
     * @throws IllegalArgumentException If Partition ID is out of bounds.
     * @throws StorageRebalanceException If there is an error when starting rebalance.
     */
    CompletableFuture<Void> startRebalancePartition(int partitionId);

    /**
     * Aborts rebalance for a partition.
     * <ul>
     *     <li>Cleans up the {@link MvPartitionStorage multi-version partition storage} and its associated indexes ({@link HashIndexStorage}
     *     and {@link SortedIndexStorage});</li>
     *     <li>Sets {@link MvPartitionStorage#lastAppliedIndex()}, {@link MvPartitionStorage#lastAppliedTerm()} to {@code 0} and
     *     {@link MvPartitionStorage#committedGroupConfiguration()} to {@code null};</li>
     *     <li>For a multi-version partition storage and its indexes, methods for writing and reading will be available.</li>
     * </ul>
     *
     * <p>If rebalance has not started, then nothing will happen.
     *
     * @return Future of the abort rebalance for a multi-version partition storage and its indexes.
     * @throws IllegalArgumentException If Partition ID is out of bounds.
     */
    CompletableFuture<Void> abortRebalancePartition(int partitionId);

    /**
     * Completes rebalance for a partition.
     * <ul>
     *     <li>Updates {@link MvPartitionStorage#lastAppliedIndex()}, {@link MvPartitionStorage#lastAppliedTerm()} and
     *     {@link MvPartitionStorage#committedGroupConfiguration()};</li>
     *     <li>For a multi-version partition storage and its indexes, methods for writing and reading will be available.</li>
     * </ul>
     *
     * <p>If rebalance has not started, then {@link StorageRebalanceException} will be thrown.
     *
     * @param lastAppliedIndex Last applied index.
     * @param lastAppliedTerm Last applied term.
     * @param groupConfig Replication protocol group configuration (byte representation).
     * @return Future of the finish rebalance for a multi-version partition storage and its indexes.
     * @throws IllegalArgumentException If Partition ID is out of bounds.
     * @throws StorageRebalanceException If there is an error when completing rebalance.
     */
    CompletableFuture<Void> finishRebalancePartition(
            int partitionId,
            long lastAppliedIndex,
            long lastAppliedTerm,
            byte[] groupConfig
    );

    /**
     * Clears a partition and all associated indices. After the cleaning is completed, a partition and all associated indices will be fully
     * available.
     * <ul>
     *     <li>Cancels all current operations (including cursors) of a {@link MvPartitionStorage multi-version partition storage} and its
     *     associated indexes ({@link HashIndexStorage} and {@link SortedIndexStorage}) and waits for their completion;</li>
     *     <li>Does not allow operations on a multi-version partition storage and its indexes to be performed (exceptions will be thrown)
     *     until the cleaning is completed;</li>
     *     <li>Clears a multi-version partition storage and its indexes;</li>
     *     <li>Sets {@link MvPartitionStorage#lastAppliedIndex()}, {@link MvPartitionStorage#lastAppliedTerm()} to {@code 0}
     *     and {@link MvPartitionStorage#committedGroupConfiguration()} to {@code null};</li>
     *     <li>Once cleanup a multi-version partition storage and its indexes is complete (success or error), allows to perform all with a
     *     multi-version partition storage and its indexes.</li>
     * </ul>
     *
     * @return Future of cleanup of a multi-version partition storage and its indexes.
     * @throws IllegalArgumentException If Partition ID is out of bounds.
     * @throws StorageClosedException If a multi-version partition storage and its indexes is already closed or destroyed.
     * @throws StorageRebalanceException If a multi-version partition storage and its indexes are in process of rebalance.
     * @throws StorageException StorageException If a multi-version partition storage and its indexes are in progress of cleanup or failed
     *      for another reason.
     */
    CompletableFuture<Void> clearPartition(int partitionId);

    /**
     * Returns an already created Index (either Sorted or Hash) with the given name or {@code null} if it does not exist.
     *
     * @param partitionId Partition ID.
     * @param indexId Index ID.
     * @return Index Storage.
     * @throws StorageException If the given partition does not exist.
     */
    // TODO: IGNITE-19112 Change or get rid of
    @Nullable IndexStorage getIndex(int partitionId, int indexId);

    /**
     * Returns the table descriptor.
     */
    StorageTableDescriptor getTableDescriptor();
}
