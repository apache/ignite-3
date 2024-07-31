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

package org.apache.ignite.internal.table;

import java.util.BitSet;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Flow.Publisher;
import java.util.concurrent.ScheduledExecutorService;
import org.apache.ignite.internal.close.ManuallyCloseable;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.BinaryRowEx;
import org.apache.ignite.internal.schema.BinaryTuple;
import org.apache.ignite.internal.schema.BinaryTuplePrefix;
import org.apache.ignite.internal.storage.MvPartitionStorage;
import org.apache.ignite.internal.storage.engine.MvTableStorage;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.internal.tx.storage.state.TxStateTableStorage;
import org.apache.ignite.internal.util.PendingComparableValuesTracker;
import org.apache.ignite.internal.utils.PrimaryReplica;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.tx.TransactionException;
import org.jetbrains.annotations.Nullable;

/**
 * Internal table facade provides low-level methods for table operations. The facade hides TX/replication protocol over table storage
 * abstractions.
 */
public interface InternalTable extends ManuallyCloseable {
    /**
     * Gets a storage for the table.
     *
     * @return Table storage.
     */
    MvTableStorage storage();

    /**
     * Gets a table id.
     *
     * @return Table id.
     */
    int tableId();

    /**
     * Gets a name of the table.
     *
     * @return Table name.
     */
    String name();

    /**
     * Sets the name of the table.
     *
     * @param newName New name.
     */
    // TODO: revisit this approach, see https://issues.apache.org/jira/browse/IGNITE-21235.
    void name(String newName);

    /**
     * Extracts an identifier of a partition from a given row.
     *
     * @param row A row to extract partition from.
     * @return An identifier of a partition the row belongs to.
     */
    int partitionId(BinaryRowEx row);

    /**
     * Asynchronously gets a row with same key columns values as given one from the table.
     *
     * @param keyRow Row with key columns set.
     * @param tx     The transaction.
     * @return Future representing pending completion of the operation.
     */
    CompletableFuture<BinaryRow> get(BinaryRowEx keyRow, @Nullable InternalTransaction tx);

    /**
     * Asynchronously gets a row with same key columns values as given one from the table on a specific node for the proposed readTimestamp.
     *
     * @param keyRow        Row with key columns set.
     * @param readTimestamp Read timestamp.
     * @param recipientNode Cluster node that will handle given get request.
     * @return Future representing pending completion of the operation.
     */
    CompletableFuture<BinaryRow> get(
            BinaryRowEx keyRow,
            HybridTimestamp readTimestamp,
            ClusterNode recipientNode
    );

    /**
     * Asynchronously get rows from the table.
     *
     * @param keyRows Rows with key columns set.
     * @param tx      Transaction or {@code null} to auto-commit.
     * @return Future that will return rows with all columns filled from the table. The order of collection elements is
     *      guaranteed to be the same as the order of {@code keyRows}. If a record does not exist, the
     *      element at the corresponding index of the resulting collection is {@code null}.
     */
    CompletableFuture<List<BinaryRow>> getAll(Collection<BinaryRowEx> keyRows, @Nullable InternalTransaction tx);

    /**
     * Asynchronously get rows from the table for the proposed read timestamp.
     *
     * @param keyRows       Rows with key columns set.
     * @param readTimestamp Read timestamp.
     * @param recipientNode Cluster node that will handle given get request.
     * @return Future that will return rows with all columns filled from the table. The order of collection elements is
     *      guaranteed to be the same as the order of {@code keyRows}. If a record does not exist, the
     *      element at the corresponding index of the resulting collection is {@code null}.
     */
    CompletableFuture<List<BinaryRow>> getAll(
            Collection<BinaryRowEx> keyRows,
            HybridTimestamp readTimestamp,
            ClusterNode recipientNode
    );


    /**
     * Asynchronously inserts a row into the table if does not exist or replaces the existed one.
     *
     * @param row Row to insert into the table.
     * @param tx  The transaction.
     * @return Future representing pending completion of the operation.
     */
    CompletableFuture<Void> upsert(BinaryRowEx row, @Nullable InternalTransaction tx);

    /**
     * Asynchronously inserts records into a table, if they do not exist, or replaces the existing ones.
     *
     * @param rows Rows to insert into the table.
     * @param tx   The transaction.
     * @return Future representing pending completion of the operation.
     */
    CompletableFuture<Void> upsertAll(Collection<BinaryRowEx> rows, @Nullable InternalTransaction tx);

    /**
     * Asynchronously updates records in the table (insert, update, delete).
     *
     * @param rows Rows to update.
     * @param deleted Bit set indicating deleted rows (one bit per item in {@param rows}). When null, no rows are deleted.
     * @param partition Partition that the rows belong to.
     * @return Future representing pending completion of the operation.
     */
    CompletableFuture<Void> updateAll(Collection<BinaryRowEx> rows, @Nullable BitSet deleted, int partition);

    /**
     * Asynchronously inserts a row into the table or replaces if exists and return replaced previous row.
     *
     * @param row Row to insert into the table.
     * @param tx  The transaction.
     * @return Future representing pending completion of the operation.
     */
    CompletableFuture<BinaryRow> getAndUpsert(BinaryRowEx row, @Nullable InternalTransaction tx);

    /**
     * Asynchronously inserts a row into the table if not exists.
     *
     * @param row Row to insert into the table.
     * @param tx  The transaction.
     * @return Future representing pending completion of the operation.
     */
    CompletableFuture<Boolean> insert(BinaryRowEx row, @Nullable InternalTransaction tx);

    /**
     * Asynchronously insert rows into the table which do not exist, skipping existed ones.
     *
     * @param rows Rows to insert into the table.
     * @param tx The transaction.
     * @return Future represents the pending completion of the operation, with rejected rows for insertion in the result. The order of
     *         collection elements is guaranteed to be the same as the order of {@code rows}. If a record is inserted, the element will be
     *         excluded from the collection result.
     */
    CompletableFuture<List<BinaryRow>> insertAll(Collection<BinaryRowEx> rows, @Nullable InternalTransaction tx);

    /**
     * Asynchronously replaces an existed row associated with the same key columns values as the given one has.
     *
     * @param row Row to replace with.
     * @param tx  The transaction.
     * @return Future representing pending completion of the operation.
     */
    CompletableFuture<Boolean> replace(BinaryRowEx row, @Nullable InternalTransaction tx);

    /**
     * Asynchronously replaces an expected row in the table with the given new one.
     *
     * @param oldRow Row to replace.
     * @param newRow Row to replace with.
     * @param tx     The transaction.
     * @return Future representing pending completion of the operation.
     */
    CompletableFuture<Boolean> replace(BinaryRowEx oldRow, BinaryRowEx newRow, @Nullable InternalTransaction tx);

    /**
     * Asynchronously gets an existed row associated with the same key columns values as the given one has, then replaces with the given
     * one.
     *
     * @param row Row to replace with.
     * @param tx  The transaction.
     * @return Future representing pending completion of the operation.
     */
    CompletableFuture<BinaryRow> getAndReplace(BinaryRowEx row, @Nullable InternalTransaction tx);

    /**
     * Asynchronously deletes a row with the same key columns values as the given one from the table.
     *
     * @param keyRow Row with key columns set.
     * @param tx     The transaction.
     * @return Future representing pending completion of the operation.
     */
    CompletableFuture<Boolean> delete(BinaryRowEx keyRow, @Nullable InternalTransaction tx);

    /**
     * Asynchronously deletes given row from the table.
     *
     * @param oldRow Row to delete.
     * @param tx     The transaction.
     * @return Future representing pending completion of the operation.
     */
    CompletableFuture<Boolean> deleteExact(BinaryRowEx oldRow, @Nullable InternalTransaction tx);

    /**
     * Asynchronously gets then deletes a row with the same key columns values from the table.
     *
     * @param row Row with key columns set.
     * @param tx  The transaction.
     * @return Future representing pending completion of the operation.
     */
    CompletableFuture<BinaryRow> getAndDelete(BinaryRowEx row, @Nullable InternalTransaction tx);

    /**
     * Asynchronously remove rows with the same key columns values as the given one has from the table.
     *
     * @param rows Rows with key columns set.
     * @param tx The transaction.
     * @return Future represents the pending completion of the operation, with rejected rows for deletion in the result. The order of
     *         collection elements is guaranteed to be the same as the order of {@code rows}. If a record is deleted, the element will be
     *         excluded from the collection result.
     */
    CompletableFuture<List<BinaryRow>> deleteAll(Collection<BinaryRowEx> rows, @Nullable InternalTransaction tx);

    /**
     * Asynchronously remove given rows from the table.
     *
     * @param rows Rows to delete.
     * @param tx The transaction.
     * @return Future represents the pending completion of the operation, with rejected rows for deletion in the result. The order of
     *         collection elements is guaranteed to be the same as the order of {@code rows}. If a record is deleted, the element will be
     *         excluded from the collection result.
     */
    CompletableFuture<List<BinaryRow>> deleteAllExact(Collection<BinaryRowEx> rows, @Nullable InternalTransaction tx);

    /**
     * Returns a partition for a key.
     *
     * @param keyRow The key.
     * @return The partition.
     */
    int partition(BinaryRowEx keyRow);

    /**
     * Scans given partition, providing {@link Publisher} that reactively notifies about partition rows.
     *
     * @param partId The partition.
     * @param tx The transaction.
     * @return {@link Publisher} that reactively notifies about partition rows.
     * @throws IllegalArgumentException If proposed partition index {@code p} is out of bounds.
     */
    default Publisher<BinaryRow> scan(int partId, @Nullable InternalTransaction tx) {
        return scan(partId, tx, null, null, null, 0, null);
    }

    /**
     * Scans given partition with the proposed read timestamp, providing {@link Publisher} that reactively notifies about partition rows.
     *
     * @param partId The partition.
     * @param readTimestamp Read timestamp.
     * @param recipientNode Cluster node that will handle given get request.
     * @param txCoordinatorId Transaction coordinator inconsistent id.
     * @return {@link Publisher} that reactively notifies about partition rows.
     * @throws IllegalArgumentException If proposed partition index {@code p} is out of bounds.
     * @throws TransactionException If proposed {@code tx} is read-write. Transaction itself won't be automatically rolled back.
     */
    default Publisher<BinaryRow> scan(
            int partId,
            UUID txId,
            HybridTimestamp readTimestamp,
            ClusterNode recipientNode,
            String txCoordinatorId
    ) {
        return scan(partId, txId, readTimestamp, recipientNode, null, null, null, 0, null, txCoordinatorId);
    }

    /**
     * Lookup rows corresponding to the given key given partition index, providing {@link Publisher}
     * that reactively notifies about partition rows.
     *
     * @param partId The partition.
     * @param readTimestamp Read timestamp.
     * @param recipientNode Cluster node that will handle given get request.
     * @param indexId Index id.
     * @param lowerBound Lower search bound.
     * @param upperBound Upper search bound.
     * @param flags Control flags. See {@link org.apache.ignite.internal.storage.index.SortedIndexStorage} constants.
     * @param columnsToInclude Row projection.
     * @param txCoordinatorId Transaction coordinator inconsistent id.
     * @return {@link Publisher} that reactively notifies about partition rows.
     */
    Publisher<BinaryRow> scan(
            int partId,
            UUID txId,
            HybridTimestamp readTimestamp,
            ClusterNode recipientNode,
            @Nullable Integer indexId,
            @Nullable BinaryTuplePrefix lowerBound,
            @Nullable BinaryTuplePrefix upperBound,
            int flags,
            @Nullable BitSet columnsToInclude,
            String txCoordinatorId
    );

    /**
     * Scans given partition index, providing {@link Publisher} that reactively notifies about partition rows.
     *
     * @param partId The partition.
     * @param txId Transaction id.
     * @param commitPartition Commit partition id.
     * @param txCoordinatorId Transaction coordinator id.
     * @param recipient Primary replica that will handle given get request.
     * @param lowerBound Lower search bound.
     * @param upperBound Upper search bound.
     * @param flags Control flags. See {@link org.apache.ignite.internal.storage.index.SortedIndexStorage} constants.
     * @param columnsToInclude Row projection.
     * @return {@link Publisher} that reactively notifies about partition rows.
     */
    Publisher<BinaryRow> scan(
            int partId,
            UUID txId,
            TablePartitionId commitPartition,
            String txCoordinatorId,
            PrimaryReplica recipient,
            @Nullable Integer indexId,
            @Nullable BinaryTuplePrefix lowerBound,
            @Nullable BinaryTuplePrefix upperBound,
            int flags,
            @Nullable BitSet columnsToInclude
    );

    /**
     * Scans given partition index, providing {@link Publisher} that reactively notifies about partition rows.
     *
     * @param partId The partition.
     * @param tx The transaction.
     * @param indexId Index id.
     * @param lowerBound Lower search bound.
     * @param upperBound Upper search bound.
     * @param flags Control flags. See {@link org.apache.ignite.internal.storage.index.SortedIndexStorage} constants.
     * @param columnsToInclude Row projection.
     * @return {@link Publisher} that reactively notifies about partition rows.
     */
    Publisher<BinaryRow> scan(
            int partId,
            @Nullable InternalTransaction tx,
            @Nullable Integer indexId,
            @Nullable BinaryTuplePrefix lowerBound,
            @Nullable BinaryTuplePrefix upperBound,
            int flags,
            @Nullable BitSet columnsToInclude
    );

    /**
     * Scans given partition index, providing {@link Publisher} that reactively notifies about partition rows.
     *
     * @param partId The partition.
     * @param readTimestamp Read timestamp.
     * @param recipientNode Cluster node that will handle given get request.
     * @param indexId Index id.
     * @param key Key to search.
     * @param columnsToInclude Row projection.
     * @param txCoordinatorId Transaction coordinator id.
     * @return {@link Publisher} that reactively notifies about partition rows.
     */
    Publisher<BinaryRow> lookup(
            int partId,
            UUID txId,
            HybridTimestamp readTimestamp,
            ClusterNode recipientNode,
            int indexId,
            BinaryTuple key,
            @Nullable BitSet columnsToInclude,
            String txCoordinatorId
    );

    /**
     * Lookup rows corresponding to the given key given partition index, providing {@link Publisher}
     * that reactively notifies about partition rows.
     *
     * @param partId The partition.
     * @param txId Transaction id.
     * @param commitPartition Commit partition id.
     * @param txCoordinatorId Transaction coordinator id.
     * @param recipient Primary replica that will handle given get request.
     * @param indexId Index id.
     * @param key Key to search.
     * @param columnsToInclude Row projection.
     * @return {@link Publisher} that reactively notifies about partition rows.
     */
    Publisher<BinaryRow> lookup(
            int partId,
            UUID txId,
            TablePartitionId commitPartition,
            String txCoordinatorId,
            PrimaryReplica recipient,
            int indexId,
            BinaryTuple key,
            @Nullable BitSet columnsToInclude
    );

    /**
     * Gets a count of partitions of the table.
     *
     * @return Count of partitions.
     */
    int partitions();

    /**
     * Storage of transaction states for this table.
     *
     * @return Transaction states' storage.
     */
    TxStateTableStorage txStateStorage();

    /**
     * Raft service for this table.
     *
     * @return Table raft service.
     */
    TableRaftService tableRaftService();

    // TODO: IGNITE-14488. Add invoke() methods.

    /**
     * Closes the table.
     */
    @Override
    void close();

    /**
     * Returns the partition safe time tracker, {@code null} means not added.
     *
     * @param partitionId Partition ID.
     */
    @Nullable PendingComparableValuesTracker<HybridTimestamp, Void> getPartitionSafeTimeTracker(int partitionId);

    /**
     * Returns the partition storage index tracker, {@code null} means not added.
     *
     * @param partitionId Partition ID.
     */
    @Nullable PendingComparableValuesTracker<Long, Void> getPartitionStorageIndexTracker(int partitionId);

    /**
     * Gets the streamer flush executor service.
     *
     * @return Streamer flush executor.
     */
    ScheduledExecutorService streamerFlushExecutor();

    /**
     * Returns {@link ClusterNode} where primary replica of replication group is located.
     *
     * @param partitionId Replication group ID.
     * @return Cluster node with primary replica.
     */
    CompletableFuture<ClusterNode> partitionLocation(TablePartitionId partitionId);

    /**
     * Returns the <em>estimated size</em> of this table.
     *
     * <p>It is computed as a sum of estimated sizes of all partitions of this table.
     *
     * @return Estimated size of this table.
     *
     * @see MvPartitionStorage#estimatedSize
     */
    CompletableFuture<Long> estimatedSize();

    /**
     * Returns the streamer receiver runner.
     *
     * @return Streamer receiver runner.
     */
    StreamerReceiverRunner streamerReceiverRunner();
}
