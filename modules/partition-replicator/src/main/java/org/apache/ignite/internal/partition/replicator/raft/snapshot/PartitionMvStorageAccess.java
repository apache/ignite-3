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

package org.apache.ignite.internal.partition.replicator.raft.snapshot;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.raft.RaftGroupConfiguration;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.storage.MvPartitionStorage;
import org.apache.ignite.internal.storage.ReadResult;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.storage.StorageException;
import org.apache.ignite.internal.storage.StorageRebalanceException;
import org.apache.ignite.internal.storage.TxIdMismatchException;
import org.apache.ignite.internal.storage.engine.MvPartitionMeta;
import org.apache.ignite.internal.storage.lease.LeaseInfo;
import org.jetbrains.annotations.Nullable;

/**
 * Small abstractions for MV partition storages that includes only methods, mandatory for the snapshot storage.
 */
public interface PartitionMvStorageAccess {
    /** Table ID of the table that this partition storage is associated with. */
    int tableId();

    /** Partition ID of the partition that this partition storage is associated with. */
    int partitionId();

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

    // TODO: https://issues.apache.org/jira/browse/IGNITE-22522 - remove mentions of commit *table*.
    /**
     * Creates (or replaces) an uncommitted (aka pending) version, assigned to the given transaction ID.
     *
     * <p>In details:</p>
     * <ul>
     * <li>If there is no uncommitted version, a new uncommitted version is added.</li>
     * <li>If there is an uncommitted version belonging to the same transaction, it gets replaced by the given version.</li>
     * <li>If there is an uncommitted version belonging to a different transaction, {@link TxIdMismatchException} is thrown.</li>
     * </ul>
     *
     * @param rowId Row ID.
     * @param row Table row to update. Key only row means value removal.
     * @param txId Transaction ID.
     * @param commitTableOrZoneId Commit table/zone ID.
     * @param commitPartitionId Commit partition ID.
     * @param catalogVersion Catalog version of the incoming partition snapshot.
     * @throws StorageException If failed to write data.
     * @throws TxIdMismatchException If there's another pending update associated with different transaction ID.
     */
    void addWrite(RowId rowId, @Nullable BinaryRow row, UUID txId, int commitTableOrZoneId, int commitPartitionId, int catalogVersion);

    /**
     * Creates a committed version.
     *
     * <p>In details:</p>
     * <ul>
     * <li>If there is no uncommitted version, a new committed version is added.</li>
     * <li>f there is an uncommitted version, {@link StorageException} is thrown.</li>
     * </ul>
     *
     * @param rowId Row ID.
     * @param row Table row to update. Key only row means value removal.
     * @param commitTimestamp Timestamp to associate with committed value.
     * @param catalogVersion Catalog version of the incoming partition snapshot.
     * @throws StorageException If failed to write data to the storage.
     */
    void addWriteCommitted(RowId rowId, @Nullable BinaryRow row, HybridTimestamp commitTimestamp, int catalogVersion);

    /** Returns the last applied index of this storage. */
    long lastAppliedIndex();

    /** Returns the last applied term of this storage. */
    long lastAppliedTerm();

    /** Returns the saved lease information of this storage. */
    @Nullable LeaseInfo leaseInfo();

    /**
     * Prepares partition storages for rebalancing.
     * <ul>
     *     <li>Cancels all current operations (including cursors) with storages and waits for their completion;</li>
     *     <li>Cleans up storages;</li>
     *     <li>Sets the last applied index and term to {@link MvPartitionStorage#REBALANCE_IN_PROGRESS} and the RAFT group configuration to
     *     {@code null};</li>
     *     <li>Only the following methods will be available:<ul>
     *         <li>{@link #tableId};</li>
     *         <li>{@link #partitionId};</li>
     *         <li>{@link #lastAppliedIndex};</li>
     *         <li>{@link #lastAppliedTerm};</li>
     *         <li>{@link #committedGroupConfiguration};</li>
     *         <li>{@link #addWrite};</li>
     *         <li>{@link #addWriteCommitted}.</li>
     *     </ul></li>
     * </ul>
     *
     * <p>This method must be called before every rebalance and ends with a call to one of the methods:
     * <ul>
     *     <li>{@link #abortRebalance} - in case of errors or cancellation of rebalance;</li>
     *     <li>{@link #finishRebalance} - in case of successful completion of rebalance.</li>
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
     * @param partitionMeta Partition metadata.
     * @return Future of the operation.
     * @throws StorageRebalanceException If there are errors when trying to finish rebalancing.
     */
    CompletableFuture<Void> finishRebalance(MvPartitionMeta partitionMeta);

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

    /** Returns {@code true} if this storage is volatile (i.e. stores its data in memory), or {@code false} if it's persistent. */
    boolean isVolatile();
}
