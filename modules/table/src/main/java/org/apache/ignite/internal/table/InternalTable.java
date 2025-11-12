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
import org.apache.ignite.internal.network.InternalClusterNode;
import org.apache.ignite.internal.replicator.ReplicationGroupId;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.BinaryRowEx;
import org.apache.ignite.internal.storage.MvPartitionStorage;
import org.apache.ignite.internal.storage.engine.MvTableStorage;
import org.apache.ignite.internal.table.metrics.TableMetrics;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.internal.tx.storage.state.TxStateStorage;
import org.apache.ignite.internal.util.PendingComparableValuesTracker;
import org.apache.ignite.table.QualifiedName;
import org.apache.ignite.tx.TransactionException;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

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
     * Returns the zone ID that the table belongs to.
     *
     * @return Zone id.
     */
    int zoneId();

    /**
     * Gets a name of the table.
     *
     * @return Table name.
     */
    QualifiedName name();

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

    // TODO: remove get() methods which do not accept InternalTransaction, see IGNITE-24120.
    /**
     * Asynchronously gets a row with same key columns values as given one from the table on a specific node for the proposed readTimestamp.
     *
     * @param keyRow        Row with key columns set.
     * @param readTimestamp Read timestamp.
     * @param recipientNode Cluster node that will handle given get request.
     * @return Future representing pending completion of the operation.
     */
    default CompletableFuture<BinaryRow> get(
            BinaryRowEx keyRow,
            HybridTimestamp readTimestamp,
            InternalClusterNode recipientNode
    ) {
        return get(keyRow, readTimestamp, null, null, recipientNode);
    }

    /**
     * Asynchronously gets a row with same key columns values as given one from the table on a specific node for the proposed readTimestamp.
     *
     * @param keyRow        Row with key columns set.
     * @param readTimestamp Read timestamp.
     * @param transactionId Transaction ID (might be {@code null}).
     * @param coordinatorId Ephemeral ID of the transaction coordinator.
     * @param recipientNode Cluster node that will handle given get request.
     * @return Future representing pending completion of the operation.
     */
    CompletableFuture<BinaryRow> get(
            BinaryRowEx keyRow,
            HybridTimestamp readTimestamp,
            @Nullable UUID transactionId,
            @Nullable UUID coordinatorId,
            InternalClusterNode recipientNode
    );

    /**
     * Asynchronously get rows from the table.
     *
     * @param keyRows Rows with key columns set.
     * @param tx      Transaction or {@code null} for implicit transaction.
     * @return Future that will return rows with all columns filled from the table. The order of collection elements is
     *      guaranteed to be the same as the order of {@code keyRows}. If a record does not exist, the
     *      element at the corresponding index of the resulting collection is {@code null}.
     */
    CompletableFuture<List<BinaryRow>> getAll(Collection<BinaryRowEx> keyRows, @Nullable InternalTransaction tx);

    // TODO: remove getAll() methods which do not accept InternalTransaction, see IGNITE-24120.
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
    default CompletableFuture<List<BinaryRow>> getAll(
            Collection<BinaryRowEx> keyRows,
            HybridTimestamp readTimestamp,
            InternalClusterNode recipientNode
    ) {
        return getAll(keyRows, readTimestamp, null, null, recipientNode);
    }

    /**
     * Asynchronously get rows from the table for the proposed read timestamp.
     *
     * @param keyRows       Rows with key columns set.
     * @param readTimestamp Read timestamp.
     * @param transactionId Transaction ID (might be {@code null}).
     * @param coordinatorId Ephemeral ID of the transaction coordinator.
     * @param recipientNode Cluster node that will handle given getAll request. In case if given node is {@code null} then for each
     *      partition inside of the method recipient node will be calculated separately.
     * @return Future that will return rows with all columns filled from the table. The order of collection elements is
     *      guaranteed to be the same as the order of {@code keyRows}. If a record does not exist, the
     *      element at the corresponding index of the resulting collection is {@code null}.
     */
    CompletableFuture<List<BinaryRow>> getAll(
            Collection<BinaryRowEx> keyRows,
            HybridTimestamp readTimestamp,
            @Nullable UUID transactionId,
            @Nullable UUID coordinatorId,
            @Nullable InternalClusterNode recipientNode
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
     * @return Future represents the pending completion of the operation, with rejected rows for insertion in the result.
     *         If a record is inserted, the element will be excluded from the collection result.
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
     * @return Future represents the pending completion of the operation, with rejected rows for deletion in the result.
     *         If a record is deleted, the element will be excluded from the collection result.
     */
    CompletableFuture<List<BinaryRow>> deleteAll(Collection<BinaryRowEx> rows, @Nullable InternalTransaction tx);

    /**
     * Asynchronously remove given rows from the table.
     *
     * @param rows Rows to delete.
     * @param tx The transaction.
     * @return Future represents the pending completion of the operation, with rejected rows for deletion in the result.
     *         If a record is deleted, the element will be excluded from the collection result.
     */
    CompletableFuture<List<BinaryRow>> deleteAllExact(Collection<BinaryRowEx> rows, @Nullable InternalTransaction tx);

    /**
     * Scans given partition, providing {@link Publisher} that reactively notifies about partition rows.
     *
     * @param partId The partition.
     * @param tx The transaction.
     * @return {@link Publisher} that reactively notifies about partition rows.
     * @throws IllegalArgumentException If proposed partition index {@code p} is out of bounds.
     */
    // TODO: https://issues.apache.org/jira/browse/IGNITE-26846 Drop the method.
    @TestOnly
    @Deprecated(forRemoval = true)
    Publisher<BinaryRow> scan(int partId, @Nullable InternalTransaction tx);

    /**
     * Scans given partition, providing {@link Publisher} that reactively notifies about partition rows.
     *
     * @param partId The partition.
     * @param recipientNode Cluster node that will handle given get request.
     * @param operationContext Operation context.
     * @return {@link Publisher} that reactively notifies about partition rows.
     * @throws IllegalArgumentException If proposed partition index {@code p} is out of bounds.
     * @throws TransactionException If proposed {@code tx} is read-write. Transaction itself won't be automatically rolled back.
     */
    Publisher<BinaryRow> scan(
            int partId,
            InternalClusterNode recipientNode,
            OperationContext operationContext
    );

    /**
     * Scans given partition index, providing {@link Publisher}
     * that reactively notifies about partition rows.
     *
     * @param partId The partition.
     * @param recipientNode Cluster node that will handle given get request.
     * @param indexId Index id.
     * @param criteria Index scan criteria.
     * @param operationContext Operation context.
     * @return {@link Publisher} that reactively notifies about partition rows.
     */
    Publisher<BinaryRow> scan(
            int partId,
            InternalClusterNode recipientNode,
            int indexId,
            IndexScanCriteria criteria,
            OperationContext operationContext
    );

    /**
     * Scans given partition index, providing {@link Publisher} that reactively notifies about partition rows.
     *
     * @param partId The partition.
     * @param tx The transaction.
     * @param indexId Index id.
     * @param criteria Index scan criteria.
     * @return {@link Publisher} that reactively notifies about partition rows.
     */
    // TODO: https://issues.apache.org/jira/browse/IGNITE-26846 Drop the method.
    @TestOnly
    @Deprecated(forRemoval = true)
    Publisher<BinaryRow> scan(
            int partId,
            @Nullable InternalTransaction tx,
            int indexId,
            IndexScanCriteria.Range criteria
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
    // TODO: remove this method as a part of https://issues.apache.org/jira/browse/IGNITE-22522.
    TxStateStorage txStateStorage();

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
     * Returns {@link InternalClusterNode} where primary replica of replication group is located.
     *
     * @param partitionIndex Partition index.
     * @return Cluster node with primary replica.
     */
    CompletableFuture<InternalClusterNode> partitionLocation(int partitionIndex);

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

    /**
     * Gets a replication group id for this table.
     *
     * @param partId Partition id.
     * @return The id.
     */
    ReplicationGroupId targetReplicationGroupId(int partId);

    /**
     * Returns a metric for this table.
     *
     * @return Table metrics.
     */
    TableMetrics metrics();
}
