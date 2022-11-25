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

package org.apache.ignite.internal.tx.storage.state;

import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.close.ManuallyCloseable;
import org.apache.ignite.internal.configuration.storage.StorageException;
import org.apache.ignite.internal.schema.configuration.TableConfiguration;
import org.jetbrains.annotations.Nullable;

/**
 * Transaction state storage for a table.
 */
public interface TxStateTableStorage extends ManuallyCloseable {
    /**
     * Returns or creates transaction state storage for partition.
     *
     * @param partitionId Partition id.
     * @throws StorageException In case when the operation has failed.
     */
    TxStateStorage getOrCreateTxStateStorage(int partitionId) throws StorageException;

    /**
     * Returns transaction state storage.
     *
     * @param partitionId Partition id.
     */
    @Nullable
    TxStateStorage getTxStateStorage(int partitionId);

    /**
     * Destroy transaction state storage.
     *
     * @param partitionId Partition id.
     * @throws StorageException In case when the operation has failed.
     */
    void destroyTxStateStorage(int partitionId) throws StorageException;

    /**
     * Returns table configuration.
     */
    TableConfiguration configuration();

    /**
     * Start the storage.
     *
     * @throws StorageException In case when the operation has failed.
     */
    void start() throws StorageException;

    /**
     * Stop the storage.
     *
     * @throws StorageException In case when the operation has failed.
     */
    void stop() throws StorageException;

    /**
     * Closes the storage.
     */
    @Override
    void close();

    /**
     * Removes all data from the storage and frees all resources.
     *
     * @throws StorageException In case when the operation has failed.
     */
    void destroy() throws StorageException;

    /**
     * Prepares the transaction state storage for rebalancing: makes a backup of the current transaction state storage and creates a new
     * storage.
     *
     * <p>This method must be called before every full rebalance of the transaction state storage, so that in case of errors or
     * cancellation of the full rebalance, we can restore the transaction state storage from the backup. A new full rebalance can only start
     * if the previous one has been completed.
     *
     * <p>Full rebalance will be completed when one of the methods is called:
     * <ol>
     *     <li>{@link #abortRebalance(int)} - in case of a full rebalance cancellation or failure, so that we can restore the transaction
     *     state storage from a backup;</li>
     *     <li>{@link #finishRebalance(int)} - in case of a successful full rebalance, to remove the backup of the transaction state
     *     storage.</li>
     * </ol>
     *
     * <p>During a full rebalance, data will be written to the new storage and read from the backup. If the full rebalance succeeds, then
     * all writes and reads and scans will occur in the new storage. All cursors that were open before the full rebalance was completed will
     * read from the old storage.. Also, at all stages, external storage replacement does not occur, i.e. the instance received from
     * {@link #getTxStateStorage(int)} or {@link #getOrCreateTxStateStorage(int)} will not change.
     *
     * @param partitionId Partition ID.
     * @return Future to indicate the completion of preparing the transaction state storage for full rebalance.
     * @throws StorageException If the given partition does not exist, or fail the start of rebalancing.
     */
    CompletableFuture<Void> startRebalance(int partitionId) throws StorageException;

    /**
     * Aborts rebalancing of the transaction state storage if it was started: restores the transaction state storage from a backup and
     * deletes the new storage.
     *
     * <p>If a full rebalance has not been {@link #startRebalance(int) started}, then nothing will happen.
     *
     * @param partitionId Partition ID.
     * @return Future to indicate the completion of the abort of a full rebalance for transaction state storage.
     * @throws StorageException If the given partition does not exist, or fail the abort of rebalancing.
     */
    CompletableFuture<Void> abortRebalance(int partitionId) throws StorageException;

    /**
     * Finishes a successful transaction state storage rebalance if it has been started: deletes the backup of the transaction state storage
     * and saves a new storage.
     *
     * <p>If a full rebalance has not been {@link #startRebalance(int) started}, then nothing will happen.
     *
     * @param partitionId Partition ID.
     * @return Future to indicate the completion of a full rebalance for transaction state storage.
     * @throws StorageException If the given partition does not exist, or fail the finish of rebalancing.
     */
    CompletableFuture<Void> finishRebalance(int partitionId) throws StorageException;
}
