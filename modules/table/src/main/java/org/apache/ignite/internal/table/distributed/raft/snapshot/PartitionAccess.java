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

package org.apache.ignite.internal.table.distributed.raft.snapshot;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.lang.IgniteBiTuple;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.storage.MvPartitionStorage;
import org.apache.ignite.internal.storage.ReadResult;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.storage.StorageException;
import org.apache.ignite.internal.storage.StorageRebalanceException;
import org.apache.ignite.internal.storage.TxIdMismatchException;
import org.apache.ignite.internal.table.distributed.raft.RaftGroupConfiguration;
import org.apache.ignite.internal.tx.TxMeta;
import org.apache.ignite.internal.util.Cursor;
import org.jetbrains.annotations.Nullable;

/**
 * Small abstractions for partition storages that includes only methods, mandatory for the snapshot storage.
 */
public interface PartitionAccess {
    /**
     * Returns the key that uniquely identifies the corresponding partition.
     */
    PartitionKey partitionKey();

    /**
     * Creates a cursor to scan all meta of transactions.
     *
     * <p>All metas that exist at the time the method is called will be returned, in transaction ID order as an unsigned 128 bit integer.
     */
    Cursor<IgniteBiTuple<UUID, TxMeta>> getAllTxMeta();

    /**
     * Adds transaction meta.
     *
     * @param txId Transaction ID.
     * @param txMeta Transaction meta.
     */
    void addTxMeta(UUID txId, TxMeta txMeta);

    /**
     * Returns an existing row ID that is greater than or equal to the lower bound, {@code null} if not found.
     *
     * @param lowerBound Lower bound.
     * @throws StorageException If failed to read data.
     */
    @Nullable RowId closestRowId(RowId lowerBound);

    /**
     * Scans all versions of a single row.
     *
     * <p>{@link ReadResult#newestCommitTimestamp()} is NOT filled by this method for intents having preceding committed
     * versions.
     *
     * @param rowId Row id.
     * @return List of results including both rows data and transaction-related context. The versions are ordered from newest to oldest.
     * @throws StorageException If failed to read data.
     */
    List<ReadResult> getAllRowVersions(RowId rowId) throws StorageException;

    /**
     * Returns committed RAFT group configuration corresponding to the write command with the highest applied index, {@code null} if it was
     * never saved.
     */
    @Nullable RaftGroupConfiguration committedGroupConfiguration();

    /**
     * Creates (or replaces) an uncommitted (aka pending) version, assigned to the given transaction ID. In details: - if there is no
     * uncommitted version, a new uncommitted version is added - if there is an uncommitted version belonging to the same transaction, it
     * gets replaced by the given version - if there is an uncommitted version belonging to a different transaction,
     * {@link TxIdMismatchException} is thrown
     *
     * @param rowId Row ID.
     * @param row Table row to update. Key only row means value removal.
     * @param txId Transaction ID.
     * @param commitTableId Commit table ID.
     * @param commitPartitionId Commit partitionId.
     * @param catalogVersion Catalog version of the incoming partition snapshot.
     * @throws TxIdMismatchException If there's another pending update associated with different transaction ID.
     * @throws StorageException If failed to write data.
     */
    void addWrite(
            RowId rowId,
            @Nullable BinaryRow row,
            UUID txId,
            int commitZoneId,
            int commitTableId,
            int commitPartitionId,
            int catalogVersion
    );

    /**
     * Creates a committed version. In details: - if there is no uncommitted version, a new committed version is added - if there is an
     * uncommitted version, this method may fail with a system exception (this method should not be called if there is already something
     * uncommitted for the given row).
     *
     * @param rowId Row ID.
     * @param row Table row to update. Key only row means value removal.
     * @param commitTimestamp Timestamp to associate with committed value.
     * @param catalogVersion Catalog version of the incoming partition snapshot.
     * @throws StorageException If failed to write data.
     */
    void addWriteCommitted(RowId rowId, @Nullable BinaryRow row, HybridTimestamp commitTimestamp, int catalogVersion);

    /**
     * Returns the minimum applied index of the partition storages.
     */
    long minLastAppliedIndex();

    /**
     * Returns the minimum applied term of the partition storages.
     */
    long minLastAppliedTerm();

    /**
     * Returns the maximum applied index of the partition storages.
     */
    long maxLastAppliedIndex();

    /**
     * Returns the maximum applied term of the partition storages.
     */
    long maxLastAppliedTerm();

    /**
     * Prepares partition storages for rebalancing.
     * <ul>
     *     <li>Cancels all current operations (including cursors) with storages and waits for their completion;</li>
     *     <li>Cleans up storages;</li>
     *     <li>Sets the last applied index and term to {@link MvPartitionStorage#REBALANCE_IN_PROGRESS} and the RAFT group configuration to
     *     {@code null};</li>
     *     <li>Only the following methods will be available:<ul>
     *         <li>{@link #partitionKey()};</li>
     *         <li>{@link #minLastAppliedIndex()};</li>
     *         <li>{@link #maxLastAppliedIndex()};</li>
     *         <li>{@link #minLastAppliedTerm()};</li>
     *         <li>{@link #maxLastAppliedTerm()};</li>
     *         <li>{@link #committedGroupConfiguration()};</li>
     *         <li>{@link #addTxMeta(UUID, TxMeta)};</li>
     *         <li>{@link #addWrite(RowId, BinaryRow, UUID, int, int, int, int)};</li>
     *         <li>{@link #addWriteCommitted(RowId, BinaryRow, HybridTimestamp, int)}.</li>
     *     </ul></li>
     * </ul>
     *
     * <p>This method must be called before every rebalance and ends with a call to one of the methods:
     * <ul>
     *     <li>{@link #abortRebalance()} - in case of errors or cancellation of rebalance;</li>
     *     <li>{@link #finishRebalance(long, long, RaftGroupConfiguration)} - in case of successful completion of rebalance.</li>
     * </ul>
     *
     * @return Future of the operation.
     * @throws StorageRebalanceException If there are errors when trying to start rebalancing.
     */
    CompletableFuture<Void> startRebalance();

    /**
     * Aborts rebalancing of the partition storages.
     * <ul>
     *     <li>Cleans up storages;</li>
     *     <li>Resets the last applied index, term, and RAFT group configuration;</li>
     *     <li>All methods will be available.</li>
     * </ul>
     *
     * <p>If rebalance has not started, then nothing will happen.
     *
     * @return Future of the operation.
     * @throws StorageRebalanceException If there are errors when trying to abort rebalancing.
     */
    CompletableFuture<Void> abortRebalance();

    /**
     * Completes rebalancing of the partition storages.
     * <ul>
     *     <li>Cleans up storages;</li>
     *     <li>Updates the last applied index, term, and RAFT group configuration;</li>
     *     <li>All methods will be available.</li>
     * </ul>
     *
     * <p>If rebalance has not started, then {@link StorageRebalanceException} will be thrown.
     *
     * @param lastAppliedIndex Last applied index.
     * @param lastAppliedTerm Last applied term.
     * @param raftGroupConfig RAFT group configuration.
     * @return Future of the operation.
     * @throws StorageRebalanceException If there are errors when trying to finish rebalancing.
     */
    CompletableFuture<Void> finishRebalance(long lastAppliedIndex, long lastAppliedTerm, RaftGroupConfiguration raftGroupConfig);

    /**
     * Returns the row ID for which the index needs to be built, {@code null} means that the index building has completed.
     *
     * @param indexId Index ID of interest.
     * @throws StorageException If failed to get the row ID.
     */
    @Nullable RowId getNextRowIdToBuildIndex(int indexId);

    /**
     * Sets the row ID for which the index needs to be built.
     *
     * @param nextRowIdToBuildByIndexId Mapping: Index ID -> row ID.
     * @throws StorageException If failed to set the row ID.
     */
    void setNextRowIdToBuildIndex(Map<Integer, RowId> nextRowIdToBuildByIndexId);

    /**
     * Updates the low watermark if it is higher than the current one.
     *
     * @param newLowWatermark Candidate for update.
     */
    void updateLowWatermark(HybridTimestamp newLowWatermark);
}
