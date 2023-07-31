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

package org.apache.ignite.internal.table.distributed.storage;

import static it.unimi.dsi.fastutil.ints.Int2ObjectMaps.emptyMap;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.CompletableFuture.failedFuture;
import static org.apache.ignite.internal.table.distributed.storage.RowBatch.allResultFutures;
import static org.apache.ignite.internal.util.ExceptionUtils.withCause;
import static org.apache.ignite.lang.ErrorGroups.Common.INTERNAL_ERR;
import static org.apache.ignite.lang.ErrorGroups.Replicator.REPLICA_UNAVAILABLE_ERR;
import static org.apache.ignite.lang.ErrorGroups.Transactions.ACQUIRE_LOCK_ERR;
import static org.apache.ignite.lang.ErrorGroups.Transactions.TX_FAILED_READ_WRITE_OPERATION_ERR;
import static org.apache.ignite.lang.ErrorGroups.Transactions.TX_REPLICA_UNAVAILABLE_ERR;

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap.Entry;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import java.net.ConnectException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Flow.Publisher;
import java.util.concurrent.Flow.Subscriber;
import java.util.concurrent.Flow.Subscription;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.ignite.internal.binarytuple.BinaryTupleReader;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.raft.Peer;
import org.apache.ignite.internal.raft.service.LeaderWithTerm;
import org.apache.ignite.internal.raft.service.RaftGroupService;
import org.apache.ignite.internal.replicator.ReplicaService;
import org.apache.ignite.internal.replicator.ReplicationGroupId;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.replicator.exception.PrimaryReplicaMissException;
import org.apache.ignite.internal.replicator.exception.ReplicationException;
import org.apache.ignite.internal.replicator.message.ReplicaRequest;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.BinaryRowEx;
import org.apache.ignite.internal.schema.BinaryTuple;
import org.apache.ignite.internal.schema.BinaryTuplePrefix;
import org.apache.ignite.internal.storage.engine.MvTableStorage;
import org.apache.ignite.internal.table.InternalTable;
import org.apache.ignite.internal.table.distributed.TableMessagesFactory;
import org.apache.ignite.internal.table.distributed.replication.request.BinaryTupleMessage;
import org.apache.ignite.internal.table.distributed.replication.request.ReadOnlyScanRetrieveBatchReplicaRequest;
import org.apache.ignite.internal.table.distributed.replication.request.ReadWriteScanRetrieveBatchReplicaRequest;
import org.apache.ignite.internal.table.distributed.replication.request.ReadWriteScanRetrieveBatchReplicaRequestBuilder;
import org.apache.ignite.internal.table.distributed.replicator.action.RequestType;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.internal.tx.LockException;
import org.apache.ignite.internal.tx.TxManager;
import org.apache.ignite.internal.tx.TxState;
import org.apache.ignite.internal.tx.storage.state.TxStateTableStorage;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.util.PendingComparableValuesTracker;
import org.apache.ignite.internal.utils.PrimaryReplica;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.lang.IgniteFiveFunction;
import org.apache.ignite.lang.IgniteInternalException;
import org.apache.ignite.lang.IgniteStringFormatter;
import org.apache.ignite.lang.IgniteTetraFunction;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.tx.TransactionException;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

/**
 * Storage of table rows.
 */
public class InternalTableImpl implements InternalTable {
    /** Cursor id generator. */
    private static final AtomicLong CURSOR_ID_GENERATOR = new AtomicLong();

    /** Number of attempts. */
    private static final int ATTEMPTS_TO_ENLIST_PARTITION = 5;

    /** Map update guarded by {@link #updatePartitionMapsMux}. */
    protected volatile Int2ObjectMap<RaftGroupService> raftGroupServiceByPartitionId;

    /** Partitions. */
    private final int partitions;

    /** Table name. */
    private final String tableName;

    /** Table identifier. */
    private final int tableId;

    /** Resolver that resolves a node consistent ID to cluster node. */
    private final Function<String, ClusterNode> clusterNodeResolver;

    /** Transactional manager. */
    protected final TxManager txManager;

    /** Storage for table data. */
    private final MvTableStorage tableStorage;

    /** Storage for transaction states. */
    private final TxStateTableStorage txStateStorage;

    /** Replica service. */
    private final ReplicaService replicaSvc;

    /** Mutex for the partition maps update. */
    private final Object updatePartitionMapsMux = new Object();

    /** Table messages factory. */
    private final TableMessagesFactory tableMessagesFactory;

    /** A hybrid logical clock. */
    private final HybridClock clock;

    /** Map update guarded by {@link #updatePartitionMapsMux}. */
    private volatile Int2ObjectMap<PendingComparableValuesTracker<HybridTimestamp, Void>> safeTimeTrackerByPartitionId = emptyMap();

    /** Map update guarded by {@link #updatePartitionMapsMux}. */
    private volatile Int2ObjectMap<PendingComparableValuesTracker<Long, Void>> storageIndexTrackerByPartitionId = emptyMap();

    /**
     * Constructor.
     *
     * @param tableName Table name.
     * @param tableId Table id.
     * @param partMap Map partition id to raft group.
     * @param partitions Partitions.
     * @param txManager Transaction manager.
     * @param tableStorage Table storage.
     * @param txStateStorage Transaction state storage.
     * @param replicaSvc Replica service.
     * @param clock A hybrid logical clock.
     */
    public InternalTableImpl(
            String tableName,
            int tableId,
            Int2ObjectMap<RaftGroupService> partMap,
            int partitions,
            Function<String, ClusterNode> clusterNodeResolver,
            TxManager txManager,
            MvTableStorage tableStorage,
            TxStateTableStorage txStateStorage,
            ReplicaService replicaSvc,
            HybridClock clock
    ) {
        this.tableName = tableName;
        this.tableId = tableId;
        this.raftGroupServiceByPartitionId = partMap;
        this.partitions = partitions;
        this.clusterNodeResolver = clusterNodeResolver;
        this.txManager = txManager;
        this.tableStorage = tableStorage;
        this.txStateStorage = txStateStorage;
        this.replicaSvc = replicaSvc;
        this.tableMessagesFactory = new TableMessagesFactory();
        this.clock = clock;
    }

    /** {@inheritDoc} */
    @Override
    public MvTableStorage storage() {
        return tableStorage;
    }

    /** {@inheritDoc} */
    @Override
    public int partitions() {
        return partitions;
    }

    /** {@inheritDoc} */
    @Override
    public int tableId() {
        return tableId;
    }

    /** {@inheritDoc} */
    @Override
    public String name() {
        return tableName;
    }

    /**
     * Enlists a single row into a transaction.
     *
     * @param row The row.
     * @param tx The transaction.
     * @param op Replica requests factory.
     * @return The future.
     */
    private <R> CompletableFuture<R> enlistInTx(
            BinaryRowEx row,
            @Nullable InternalTransaction tx,
            IgniteTetraFunction<TablePartitionId, InternalTransaction, ReplicationGroupId, Long, ReplicaRequest> op
    ) {
        // Check whether proposed tx is read-only. Complete future exceptionally if true.
        // Attempting to enlist a read-only in a read-write transaction does not corrupt the transaction itself, thus read-write transaction
        // won't be rolled back automatically - it's up to the user or outer engine.
        if (tx != null && tx.isReadOnly()) {
            return failedFuture(
                    new TransactionException(
                            TX_FAILED_READ_WRITE_OPERATION_ERR,
                            "Failed to enlist read-write operation into read-only transaction txId={" + tx.id() + '}'
                    )
            );
        }

        final boolean implicit = tx == null;

        final InternalTransaction tx0 = implicit ? txManager.begin() : tx;

        int partId = partitionId(row);

        TablePartitionId partGroupId = new TablePartitionId(tableId, partId);

        IgniteBiTuple<ClusterNode, Long> primaryReplicaAndTerm = tx0.enlistedNodeAndTerm(partGroupId);

        CompletableFuture<R> fut;

        if (primaryReplicaAndTerm != null) {
            TablePartitionId commitPart = tx.commitPartition();

            ReplicaRequest request = op.apply(commitPart, tx0, partGroupId, primaryReplicaAndTerm.get2());

            try {
                fut = replicaSvc.invoke(primaryReplicaAndTerm.get1(), request);
            } catch (PrimaryReplicaMissException e) {
                throw new TransactionException(e);
            } catch (Throwable e) {
                throw new TransactionException("Failed to invoke the replica request.");
            }
        } else {
            fut = enlistWithRetry(
                    tx0,
                    partId,
                    (commitPart, term) -> op.apply(commitPart, tx0, partGroupId, term),
                    ATTEMPTS_TO_ENLIST_PARTITION
            );
        }

        return postEnlist(fut, implicit, tx0);
    }

    /**
     * Enlists a single row into a transaction.
     *
     * @param keyRows Rows.
     * @param tx The transaction.
     * @param op Replica requests factory.
     * @param reducer Transform reducer.
     * @return The future.
     */
    private <T> CompletableFuture<T> enlistInTx(
            Collection<BinaryRowEx> keyRows,
            @Nullable InternalTransaction tx,
            IgniteFiveFunction<TablePartitionId, Collection<BinaryRow>, InternalTransaction, ReplicationGroupId, Long, ReplicaRequest> op,
            Function<Collection<RowBatch>, CompletableFuture<T>> reducer
    ) {
        // Check whether proposed tx is read-only. Complete future exceptionally if true.
        // Attempting to enlist a read-only in a read-write transaction does not corrupt the transaction itself, thus read-write transaction
        // won't be rolled back automatically - it's up to the user or outer engine.
        if (tx != null && tx.isReadOnly()) {
            return failedFuture(
                    new TransactionException(
                            TX_FAILED_READ_WRITE_OPERATION_ERR,
                            "Failed to enlist read-write operation into read-only transaction txId={" + tx.id() + '}'
                    )
            );
        }

        final boolean implicit = tx == null;

        // It's possible to have null txState if transaction isn't started yet.
        if (!implicit && !(tx.state() == TxState.PENDING || tx.state() == null)) {
            return failedFuture(new TransactionException(
                    "The operation is attempted for completed transaction"));
        }

        final InternalTransaction tx0 = implicit ? txManager.begin() : tx;

        Int2ObjectMap<RowBatch> rowBatchByPartitionId = toRowBatchByPartitionId(keyRows);

        for (Int2ObjectMap.Entry<RowBatch> partitionRowBatch : rowBatchByPartitionId.int2ObjectEntrySet()) {
            int partitionId = partitionRowBatch.getIntKey();
            RowBatch rowBatch = partitionRowBatch.getValue();

            TablePartitionId partGroupId = new TablePartitionId(tableId, partitionId);

            IgniteBiTuple<ClusterNode, Long> primaryReplicaAndTerm = tx0.enlistedNodeAndTerm(partGroupId);

            CompletableFuture<Object> fut;

            if (primaryReplicaAndTerm != null) {
                TablePartitionId commitPart = tx.commitPartition();

                ReplicaRequest request = op.apply(commitPart, rowBatch.requestedRows, tx0, partGroupId, primaryReplicaAndTerm.get2());

                try {
                    fut = replicaSvc.invoke(primaryReplicaAndTerm.get1(), request);
                } catch (PrimaryReplicaMissException e) {
                    throw new TransactionException(e);
                } catch (Throwable e) {
                    throw new TransactionException("Failed to invoke the replica request.");
                }
            } else {
                fut = enlistWithRetry(
                        tx0,
                        partitionId,
                        (commitPart, term) -> op.apply(commitPart, rowBatch.requestedRows, tx0, partGroupId, term),
                        ATTEMPTS_TO_ENLIST_PARTITION
                );
            }

            rowBatch.resultFuture = fut;
        }

        CompletableFuture<T> fut = reducer.apply(rowBatchByPartitionId.values());

        return postEnlist(fut, implicit, tx0);
    }

    /**
     * Retrieves a batch of rows from replication storage.
     *
     * @param tx Internal transaction.
     * @param partId Partition number.
     * @param scanId Scan id.
     * @param batchSize Size of batch.
     * @param indexId Optional index id.
     * @param lowerBound Lower search bound.
     * @param upperBound Upper search bound.
     * @param flags Control flags. See {@link org.apache.ignite.internal.storage.index.SortedIndexStorage} constants.
     * @param columnsToInclude Row projection.
     * @return Batch of retrieved rows.
     */
    private CompletableFuture<Collection<BinaryRow>> enlistCursorInTx(
            InternalTransaction tx,
            int partId,
            long scanId,
            int batchSize,
            @Nullable Integer indexId,
            @Nullable BinaryTuple exactKey,
            @Nullable BinaryTuplePrefix lowerBound,
            @Nullable BinaryTuplePrefix upperBound,
            int flags,
            @Nullable BitSet columnsToInclude
    ) {
        TablePartitionId partGroupId = new TablePartitionId(tableId, partId);

        IgniteBiTuple<ClusterNode, Long> primaryReplicaAndTerm = tx.enlistedNodeAndTerm(partGroupId);

        CompletableFuture<Collection<BinaryRow>> fut;

        ReadWriteScanRetrieveBatchReplicaRequestBuilder requestBuilder = tableMessagesFactory.readWriteScanRetrieveBatchReplicaRequest()
                .groupId(partGroupId)
                .timestampLong(clock.nowLong())
                .transactionId(tx.id())
                .scanId(scanId)
                .indexToUse(indexId)
                .exactKey(binaryTupleMessage(exactKey))
                .lowerBoundPrefix(binaryTupleMessage(lowerBound))
                .upperBoundPrefix(binaryTupleMessage(upperBound))
                .flags(flags)
                .columnsToInclude(columnsToInclude)
                .batchSize(batchSize);

        if (primaryReplicaAndTerm != null) {
            ReadWriteScanRetrieveBatchReplicaRequest request = requestBuilder.term(primaryReplicaAndTerm.get2()).build();

            try {
                fut = replicaSvc.invoke(primaryReplicaAndTerm.get1(), request);
            } catch (PrimaryReplicaMissException e) {
                throw new TransactionException(e);
            } catch (Throwable e) {
                throw new TransactionException("Failed to invoke the replica request.");
            }
        } else {
            fut = enlistWithRetry(tx, partId, (commitPart, term) -> requestBuilder.term(term).build(), ATTEMPTS_TO_ENLIST_PARTITION);
        }

        return postEnlist(fut, false, tx);
    }

    private @Nullable BinaryTupleMessage binaryTupleMessage(@Nullable BinaryTupleReader binaryTuple) {
        if (binaryTuple == null) {
            return null;
        }

        return tableMessagesFactory.binaryTupleMessage()
                .tuple(binaryTuple.byteBuffer())
                .elementCount(binaryTuple.elementCount())
                .build();
    }

    /**
     * Partition enlisting with retrying.
     *
     * @param tx Internal transaction.
     * @param partId Partition number.
     * @param requestFunction Function to create replica request with new raft term.
     * @param attempts Number of attempts.
     * @return The future.
     */
    private <R> CompletableFuture<R> enlistWithRetry(
            InternalTransaction tx,
            int partId,
            BiFunction<TablePartitionId, Long, ReplicaRequest> requestFunction,
            int attempts
    ) {
        CompletableFuture<R> result = new CompletableFuture<>();

        enlist(partId, tx).<R>thenCompose(
                        primaryReplicaAndTerm -> {
                            try {
                                return replicaSvc.invoke(
                                        primaryReplicaAndTerm.get1(),
                                        requestFunction.apply(tx.commitPartition(), primaryReplicaAndTerm.get2())
                                );
                            } catch (PrimaryReplicaMissException e) {
                                throw new TransactionException(e);
                            } catch (Throwable e) {
                                throw new TransactionException(
                                        INTERNAL_ERR,
                                        IgniteStringFormatter.format(
                                                "Failed to enlist partition [tableName={}, partId={}] into a transaction",
                                                tableName,
                                                partId
                                        ),
                                        e
                                );
                            }
                        })
                .handle((res0, e) -> {
                    if (e != null) {
                        if (e.getCause() instanceof PrimaryReplicaMissException && attempts > 0) {
                            return enlistWithRetry(tx, partId, requestFunction, attempts - 1).handle((r2, e2) -> {
                                if (e2 != null) {
                                    return result.completeExceptionally(e2);
                                } else {
                                    return result.complete((R) r2);
                                }
                            });
                        }

                        return result.completeExceptionally(e);
                    }

                    return result.complete(res0);
                });

        return result;
    }

    /**
     * Performs post enlist operation.
     *
     * @param fut The future.
     * @param implicit {@code true} for implicit tx.
     * @param tx0 The transaction.
     * @param <T> Operation return type.
     * @return The future.
     */
    private <T> CompletableFuture<T> postEnlist(CompletableFuture<T> fut, boolean implicit, InternalTransaction tx0) {
        return fut.handle((BiFunction<T, Throwable, CompletableFuture<T>>) (r, e) -> {
            if (e != null) {
                RuntimeException e0 = wrapReplicationException(e);

                return tx0.rollbackAsync().handle((ignored, err) -> {

                    if (err != null) {
                        e0.addSuppressed(err);
                    }
                    throw e0;
                }); // Preserve failed state.
            } else {
                tx0.enlistResultFuture(fut);

                if (implicit) {
                    return tx0.commitAsync()
                            .exceptionally(ex -> {
                                throw wrapReplicationException(ex);
                            })
                            .thenApply(ignored -> r);
                } else {
                    return completedFuture(r);
                }
            }
        }).thenCompose(x -> x);
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<BinaryRow> get(BinaryRowEx keyRow, InternalTransaction tx) {
        if (tx != null && tx.isReadOnly()) {
            return evaluateReadOnlyRecipientNode(partitionId(keyRow))
                    .thenCompose(recipientNode -> get(keyRow, tx.readTimestamp(), recipientNode));
        } else {
            return enlistInTx(
                    keyRow,
                    tx,
                    (commitPart, txo, groupId, term) -> tableMessagesFactory.readWriteSingleRowReplicaRequest()
                            .groupId(groupId)
                            .binaryRowBytes(keyRow.byteBuffer())
                            .commitPartitionId(commitPart)
                            .transactionId(txo.id())
                            .term(term)
                            .requestType(RequestType.RW_GET)
                            .timestampLong(clock.nowLong())
                            .build()
            );
        }
    }

    @Override
    public CompletableFuture<BinaryRow> get(
            BinaryRowEx keyRow,
            HybridTimestamp readTimestamp,
            ClusterNode recipientNode
    ) {
        int partId = partitionId(keyRow);
        ReplicationGroupId partGroupId = raftGroupServiceByPartitionId.get(partId).groupId();

        return replicaSvc.invoke(recipientNode, tableMessagesFactory.readOnlySingleRowReplicaRequest()
                .groupId(partGroupId)
                .binaryRowBytes(keyRow.byteBuffer())
                .requestType(RequestType.RO_GET)
                .readTimestampLong(readTimestamp.longValue())
                .build()
        );
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<List<BinaryRow>> getAll(Collection<BinaryRowEx> keyRows, InternalTransaction tx) {
        if (tx != null && tx.isReadOnly()) {
            BinaryRowEx firstRow = keyRows.iterator().next();

            if (firstRow == null) {
                return completedFuture(Collections.emptyList());
            } else {
                return evaluateReadOnlyRecipientNode(partitionId(firstRow))
                        .thenCompose(recipientNode -> getAll(keyRows, tx.readTimestamp(), recipientNode));
            }
        } else {
            return enlistInTx(
                    keyRows,
                    tx,
                    (commitPart, keyRows0, txo, groupId, term) -> tableMessagesFactory.readWriteMultiRowReplicaRequest()
                            .groupId(groupId)
                            .binaryRowsBytes(serializeBinaryRows(keyRows0))
                            .commitPartitionId(commitPart)
                            .transactionId(txo.id())
                            .term(term)
                            .requestType(RequestType.RW_GET_ALL)
                            .timestampLong(clock.nowLong())
                            .build(),
                    InternalTableImpl::collectMultiRowsResponsesWithRestoreOrder
            );
        }
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<List<BinaryRow>> getAll(
            Collection<BinaryRowEx> keyRows,
            HybridTimestamp readTimestamp,
            ClusterNode recipientNode
    ) {
        Int2ObjectMap<RowBatch> rowBatchByPartitionId = toRowBatchByPartitionId(keyRows);

        for (Int2ObjectMap.Entry<RowBatch> partitionRowBatch : rowBatchByPartitionId.int2ObjectEntrySet()) {
            ReplicationGroupId partGroupId = raftGroupServiceByPartitionId.get(partitionRowBatch.getIntKey()).groupId();

            CompletableFuture<Object> fut = replicaSvc.invoke(recipientNode, tableMessagesFactory.readOnlyMultiRowReplicaRequest()
                    .groupId(partGroupId)
                    .binaryRowsBytes(serializeBinaryRows(partitionRowBatch.getValue().requestedRows))
                    .requestType(RequestType.RO_GET_ALL)
                    .readTimestampLong(readTimestamp.longValue())
                    .build()
            );

            partitionRowBatch.getValue().resultFuture = fut;
        }

        return collectMultiRowsResponsesWithRestoreOrder(rowBatchByPartitionId.values());
    }

    private static List<ByteBuffer> serializeBinaryRows(Collection<? extends BinaryRow> rows) {
        var result = new ArrayList<ByteBuffer>(rows.size());

        for (BinaryRow row : rows) {
            result.add(row.byteBuffer());
        }

        return result;
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Void> upsert(BinaryRowEx row, InternalTransaction tx) {
        return enlistInTx(
                row,
                tx,
                (commitPart, txo, groupId, term) -> tableMessagesFactory.readWriteSingleRowReplicaRequest()
                        .groupId(groupId)
                        .commitPartitionId(commitPart)
                        .binaryRowBytes(row.byteBuffer())
                        .transactionId(txo.id())
                        .term(term)
                        .requestType(RequestType.RW_UPSERT)
                        .timestampLong(clock.nowLong())
                        .build());
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Void> upsertAll(Collection<BinaryRowEx> rows, InternalTransaction tx) {
        return enlistInTx(
                rows,
                tx,
                this::upsertAllInternal,
                RowBatch::allResultFutures
        );
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Void> upsertAll(Collection<BinaryRowEx> rows, int partition) {
        InternalTransaction tx = txManager.begin();
        TablePartitionId partGroupId = new TablePartitionId(tableId, partition);

        CompletableFuture<Void> fut = enlistWithRetry(
                tx,
                partition,
                (commitPart, term) -> upsertAllInternal(commitPart, rows, tx, partGroupId, term),
                ATTEMPTS_TO_ENLIST_PARTITION
        );

        return postEnlist(fut, true, tx);
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<BinaryRow> getAndUpsert(BinaryRowEx row, InternalTransaction tx) {
        return enlistInTx(
                row,
                tx,
                (commitPart, txo, groupId, term) -> tableMessagesFactory.readWriteSingleRowReplicaRequest()
                        .groupId(groupId)
                        .commitPartitionId(commitPart)
                        .binaryRowBytes(row.byteBuffer())
                        .transactionId(txo.id())
                        .term(term)
                        .requestType(RequestType.RW_GET_AND_UPSERT)
                        .timestampLong(clock.nowLong())
                        .build()
        );
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Boolean> insert(BinaryRowEx row, InternalTransaction tx) {
        return enlistInTx(
                row,
                tx,
                (commitPart, txo, groupId, term) -> tableMessagesFactory.readWriteSingleRowReplicaRequest()
                        .groupId(groupId)
                        .commitPartitionId(commitPart)
                        .binaryRowBytes(row.byteBuffer())
                        .transactionId(txo.id())
                        .term(term)
                        .requestType(RequestType.RW_INSERT)
                        .timestampLong(clock.nowLong())
                        .build()
        );
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Collection<BinaryRow>> insertAll(Collection<BinaryRowEx> rows, InternalTransaction tx) {
        return enlistInTx(
                rows,
                tx,
                (commitPart, keyRows0, txo, groupId, term) -> tableMessagesFactory.readWriteMultiRowReplicaRequest()
                        .groupId(groupId)
                        .commitPartitionId(commitPart)
                        .binaryRowsBytes(serializeBinaryRows(keyRows0))
                        .transactionId(txo.id())
                        .term(term)
                        .requestType(RequestType.RW_INSERT_ALL)
                        .timestampLong(clock.nowLong())
                        .build(),
                InternalTableImpl::collectMultiRowsResponsesWithoutRestoreOrder
        );
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Boolean> replace(BinaryRowEx row, InternalTransaction tx) {
        return enlistInTx(
                row,
                tx,
                (commitPart, txo, groupId, term) -> tableMessagesFactory.readWriteSingleRowReplicaRequest()
                        .groupId(groupId)
                        .commitPartitionId(commitPart)
                        .binaryRowBytes(row.byteBuffer())
                        .transactionId(txo.id())
                        .term(term)
                        .requestType(RequestType.RW_REPLACE_IF_EXIST)
                        .timestampLong(clock.nowLong())
                        .build()
        );
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Boolean> replace(BinaryRowEx oldRow, BinaryRowEx newRow, InternalTransaction tx) {
        return enlistInTx(
                newRow,
                tx,
                (commitPart, txo, groupId, term) -> tableMessagesFactory.readWriteSwapRowReplicaRequest()
                        .groupId(groupId)
                        .commitPartitionId(commitPart)
                        .oldBinaryRowBytes(oldRow.byteBuffer())
                        .binaryRowBytes(newRow.byteBuffer())
                        .transactionId(txo.id())
                        .term(term)
                        .requestType(RequestType.RW_REPLACE)
                        .timestampLong(clock.nowLong())
                        .build()
        );
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<BinaryRow> getAndReplace(BinaryRowEx row, InternalTransaction tx) {
        return enlistInTx(
                row,
                tx,
                (commitPart, txo, groupId, term) -> tableMessagesFactory.readWriteSingleRowReplicaRequest()
                        .groupId(groupId)
                        .commitPartitionId(commitPart)
                        .binaryRowBytes(row.byteBuffer())
                        .transactionId(txo.id())
                        .term(term)
                        .requestType(RequestType.RW_GET_AND_REPLACE)
                        .timestampLong(clock.nowLong())
                        .build()
        );
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Boolean> delete(BinaryRowEx keyRow, InternalTransaction tx) {
        return enlistInTx(
                keyRow,
                tx,
                (commitPart, txo, groupId, term) -> tableMessagesFactory.readWriteSingleRowReplicaRequest()
                        .groupId(groupId)
                        .commitPartitionId(commitPart)
                        .binaryRowBytes(keyRow.byteBuffer())
                        .transactionId(txo.id())
                        .term(term)
                        .requestType(RequestType.RW_DELETE)
                        .timestampLong(clock.nowLong())
                        .build()
        );
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Boolean> deleteExact(BinaryRowEx oldRow, InternalTransaction tx) {
        return enlistInTx(
                oldRow,
                tx,
                (commitPart, txo, groupId, term) -> tableMessagesFactory.readWriteSingleRowReplicaRequest()
                        .groupId(groupId)
                        .commitPartitionId(commitPart)
                        .binaryRowBytes(oldRow.byteBuffer())
                        .transactionId(txo.id())
                        .term(term)
                        .requestType(RequestType.RW_DELETE_EXACT)
                        .timestampLong(clock.nowLong())
                        .build()
        );
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<BinaryRow> getAndDelete(BinaryRowEx row, InternalTransaction tx) {
        return enlistInTx(
                row,
                tx,
                (commitPart, txo, groupId, term) -> tableMessagesFactory.readWriteSingleRowReplicaRequest()
                        .groupId(groupId)
                        .commitPartitionId(commitPart)
                        .binaryRowBytes(row.byteBuffer())
                        .transactionId(txo.id())
                        .term(term)
                        .requestType(RequestType.RW_GET_AND_DELETE)
                        .timestampLong(clock.nowLong())
                        .build()
        );
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Collection<BinaryRow>> deleteAll(Collection<BinaryRowEx> rows, InternalTransaction tx) {
        return enlistInTx(
                rows,
                tx,
                (commitPart, keyRows0, txo, groupId, term) -> tableMessagesFactory.readWriteMultiRowReplicaRequest()
                        .groupId(groupId)
                        .commitPartitionId(commitPart)
                        .binaryRowsBytes(serializeBinaryRows(keyRows0))
                        .transactionId(txo.id())
                        .term(term)
                        .requestType(RequestType.RW_DELETE_ALL)
                        .timestampLong(clock.nowLong())
                        .build(),
                InternalTableImpl::collectMultiRowsResponsesWithoutRestoreOrder
        );
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Collection<BinaryRow>> deleteAllExact(
            Collection<BinaryRowEx> rows,
            InternalTransaction tx
    ) {
        return enlistInTx(
                rows,
                tx,
                (commitPart, keyRows0, txo, groupId, term) -> tableMessagesFactory.readWriteMultiRowReplicaRequest()
                        .groupId(groupId)
                        .commitPartitionId(commitPart)
                        .binaryRowsBytes(serializeBinaryRows(keyRows0))
                        .transactionId(txo.id())
                        .term(term)
                        .requestType(RequestType.RW_DELETE_EXACT_ALL)
                        .timestampLong(clock.nowLong())
                        .build(),
                InternalTableImpl::collectMultiRowsResponsesWithoutRestoreOrder
        );
    }

    @Override
    public Publisher<BinaryRow> lookup(
            int partId,
            HybridTimestamp readTimestamp,
            ClusterNode recipientNode,
            int indexId,
            BinaryTuple key,
            @Nullable BitSet columnsToInclude
    ) {
        return scan(partId, readTimestamp, recipientNode, indexId, key, null, null, 0, columnsToInclude);
    }

    @Override
    public Publisher<BinaryRow> lookup(
            int partId,
            UUID txId,
            PrimaryReplica recipient,
            int indexId,
            BinaryTuple key,
            @Nullable BitSet columnsToInclude
    ) {
        return scan(partId, txId, recipient, indexId, key, null, null, 0, columnsToInclude);
    }

    @Override
    public Publisher<BinaryRow> scan(
            int partId,
            HybridTimestamp readTimestamp,
            ClusterNode recipientNode,
            @Nullable Integer indexId,
            @Nullable BinaryTuplePrefix lowerBound,
            @Nullable BinaryTuplePrefix upperBound,
            int flags,
            @Nullable BitSet columnsToInclude
    ) {
        return scan(partId, readTimestamp, recipientNode, indexId, null, lowerBound, upperBound, flags, columnsToInclude);
    }

    private Publisher<BinaryRow> scan(
            int partId,
            HybridTimestamp readTimestamp,
            ClusterNode recipientNode,
            @Nullable Integer indexId,
            @Nullable BinaryTuple exactKey,
            @Nullable BinaryTuplePrefix lowerBound,
            @Nullable BinaryTuplePrefix upperBound,
            int flags,
            @Nullable BitSet columnsToInclude
    ) {
        validatePartitionIndex(partId);

        UUID txId = UUID.randomUUID();

        return new PartitionScanPublisher(
                (scanId, batchSize) -> {
                    ReplicationGroupId partGroupId = raftGroupServiceByPartitionId.get(partId).groupId();

                    ReadOnlyScanRetrieveBatchReplicaRequest request = tableMessagesFactory.readOnlyScanRetrieveBatchReplicaRequest()
                            .groupId(partGroupId)
                            .readTimestampLong(readTimestamp.longValue())
                            // TODO: IGNITE-17666 Close cursor tx finish.
                            .transactionId(txId)
                            .scanId(scanId)
                            .batchSize(batchSize)
                            .indexToUse(indexId)
                            .exactKey(binaryTupleMessage(exactKey))
                            .lowerBoundPrefix(binaryTupleMessage(lowerBound))
                            .upperBoundPrefix(binaryTupleMessage(upperBound))
                            .flags(flags)
                            .columnsToInclude(columnsToInclude)
                            .build();

                    return replicaSvc.invoke(recipientNode, request);
                },
                // TODO: IGNITE-17666 Close cursor tx finish.
                Function.identity());
    }

    @Override
    public Publisher<BinaryRow> scan(
            int partId,
            @Nullable InternalTransaction tx,
            @Nullable Integer indexId,
            @Nullable BinaryTuplePrefix lowerBound,
            @Nullable BinaryTuplePrefix upperBound,
            int flags,
            @Nullable BitSet columnsToInclude
    ) {
        return scan(partId, tx, indexId, null, lowerBound, upperBound, flags, columnsToInclude);
    }

    private Publisher<BinaryRow> scan(
            int partId,
            @Nullable InternalTransaction tx,
            @Nullable Integer indexId,
            @Nullable BinaryTuple exactKey,
            @Nullable BinaryTuplePrefix lowerBound,
            @Nullable BinaryTuplePrefix upperBound,
            int flags,
            @Nullable BitSet columnsToInclude
    ) {
        // Check whether proposed tx is read-only. Complete future exceptionally if true.
        // Attempting to enlist a read-only in a read-write transaction does not corrupt the transaction itself, thus read-write transaction
        // won't be rolled back automatically - it's up to the user or outer engine.
        if (tx != null && tx.isReadOnly()) {
            throw new TransactionException(
                    new TransactionException(
                            TX_FAILED_READ_WRITE_OPERATION_ERR,
                            "Failed to enlist read-write operation into read-only transaction txId={" + tx.id() + '}'
                    )
            );
        }

        validatePartitionIndex(partId);

        final boolean implicit = tx == null;

        final InternalTransaction tx0 = implicit ? txManager.begin() : tx;

        return new PartitionScanPublisher(
                (scanId, batchSize) -> enlistCursorInTx(
                        tx0,
                        partId,
                        scanId,
                        batchSize,
                        indexId,
                        exactKey,
                        lowerBound,
                        upperBound,
                        flags,
                        columnsToInclude
                ),
                fut -> postEnlist(fut, implicit, tx0)
        );
    }


    @Override
    public Publisher<BinaryRow> scan(
            int partId,
            UUID txId,
            PrimaryReplica recipient,
            @Nullable Integer indexId,
            @Nullable BinaryTuplePrefix lowerBound,
            @Nullable BinaryTuplePrefix upperBound,
            int flags,
            @Nullable BitSet columnsToInclude
    ) {
        return scan(partId, txId, recipient, indexId, null, lowerBound, upperBound, flags, columnsToInclude);
    }

    private Publisher<BinaryRow> scan(
            int partId,
            UUID txId,
            PrimaryReplica recipient,
            @Nullable Integer indexId,
            @Nullable BinaryTuple exactKey,
            @Nullable BinaryTuplePrefix lowerBound,
            @Nullable BinaryTuplePrefix upperBound,
            int flags,
            @Nullable BitSet columnsToInclude
    ) {
        return new PartitionScanPublisher(
                (scanId, batchSize) -> {
                    ReplicationGroupId partGroupId = raftGroupServiceByPartitionId.get(partId).groupId();

                    ReadWriteScanRetrieveBatchReplicaRequest request = tableMessagesFactory.readWriteScanRetrieveBatchReplicaRequest()
                            .groupId(partGroupId)
                            .timestampLong(clock.nowLong())
                            .transactionId(txId)
                            .scanId(scanId)
                            .indexToUse(indexId)
                            .exactKey(binaryTupleMessage(exactKey))
                            .lowerBoundPrefix(binaryTupleMessage(lowerBound))
                            .upperBoundPrefix(binaryTupleMessage(upperBound))
                            .flags(flags)
                            .columnsToInclude(columnsToInclude)
                            .batchSize(batchSize)
                            .term(recipient.term())
                            .build();

                    return replicaSvc.invoke(recipient.node(), request);
                },
                // TODO: IGNITE-17666 Close cursor tx finish.
                Function.identity());
    }

    /**
     * Validates partition index.
     *
     * @param p Partition index.
     * @throws IllegalArgumentException If proposed partition is out of bounds.
     */
    private void validatePartitionIndex(int p) {
        if (p < 0 || p >= partitions) {
            throw new IllegalArgumentException(
                    IgniteStringFormatter.format(
                            "Invalid partition [partition={}, minValue={}, maxValue={}].",
                            p,
                            0,
                            partitions - 1
                    )
            );
        }
    }

    /**
     * Creates batches of rows for processing, grouped by partition ID.
     *
     * @param rows Rows.
     */
    Int2ObjectMap<RowBatch> toRowBatchByPartitionId(Collection<BinaryRowEx> rows) {
        Int2ObjectMap<RowBatch> rowBatchByPartitionId = new Int2ObjectOpenHashMap<>();

        int i = 0;

        for (BinaryRowEx row : rows) {
            rowBatchByPartitionId.computeIfAbsent(partitionId(row), partitionId -> new RowBatch()).add(row, i++);
        }

        return rowBatchByPartitionId;
    }

    /** {@inheritDoc} */
    // TODO: IGNITE-17256 Use a placement driver for getting a primary replica.
    @Override
    public List<String> assignments() {
        awaitLeaderInitialization();

        return raftGroupServiceByPartitionId.int2ObjectEntrySet().stream()
                .sorted(Comparator.comparingInt(Int2ObjectOpenHashMap.Entry::getIntKey))
                .map(Map.Entry::getValue)
                .map(service -> service.leader().consistentId())
                .collect(Collectors.toList());
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<List<PrimaryReplica>> primaryReplicas() {
        List<Entry<RaftGroupService>> entries = new ArrayList<>(raftGroupServiceByPartitionId.int2ObjectEntrySet());
        List<CompletableFuture<PrimaryReplica>> result = new ArrayList<>();

        entries.sort(Comparator.comparingInt(Entry::getIntKey));

        for (Entry<RaftGroupService> e : entries) {
            CompletableFuture<LeaderWithTerm> f = e.getValue().refreshAndGetLeaderWithTerm();

            result.add(f.thenApply(lt -> {
                ClusterNode node = clusterNodeResolver.apply(lt.leader().consistentId());
                return new PrimaryReplica(node, lt.term());
            }));
        }

        CompletableFuture<Void> all = CompletableFuture.allOf(result.toArray(new CompletableFuture[0]));

        return all.thenApply(v -> result.stream()
                .map(CompletableFuture::join)
                .collect(Collectors.toList())
        );
    }

    @Override
    public ClusterNode leaderAssignment(int partition) {
        awaitLeaderInitialization();

        RaftGroupService raftGroupService = raftGroupServiceByPartitionId.get(partition);
        if (raftGroupService == null) {
            throw new IgniteInternalException("No such partition " + partition + " in table " + tableName);
        }

        return clusterNodeResolver.apply(raftGroupService.leader().consistentId());
    }

    /** {@inheritDoc} */
    @Override
    public RaftGroupService partitionRaftGroupService(int partition) {
        RaftGroupService raftGroupService = raftGroupServiceByPartitionId.get(partition);
        if (raftGroupService == null) {
            throw new IgniteInternalException("No such partition " + partition + " in table " + tableName);
        }

        if (raftGroupService.leader() == null) {
            raftGroupService.refreshLeader().join();
        }

        return raftGroupService;
    }

    /** {@inheritDoc} */
    @Override
    public TxStateTableStorage txStateStorage() {
        return txStateStorage;
    }

    private void awaitLeaderInitialization() {
        List<CompletableFuture<Void>> futs = new ArrayList<>();

        for (RaftGroupService raftSvc : raftGroupServiceByPartitionId.values()) {
            if (raftSvc.leader() == null) {
                futs.add(raftSvc.refreshLeader());
            }
        }

        CompletableFuture.allOf(futs.toArray(CompletableFuture[]::new)).join();
    }

    /** {@inheritDoc} */
    @TestOnly
    @Override
    public int partition(BinaryRowEx keyRow) {
        return partitionId(keyRow);
    }

    /**
     * Returns map of partition -> list of peers and learners of that partition.
     */
    @TestOnly
    public Map<Integer, List<String>> peersAndLearners() {
        awaitLeaderInitialization();

        return raftGroupServiceByPartitionId.int2ObjectEntrySet().stream()
                .collect(Collectors.toMap(Entry::getIntKey, e -> {
                    RaftGroupService service = e.getValue();
                    return Stream.of(service.peers(), service.learners())
                            .filter(Objects::nonNull)
                            .flatMap(Collection::stream)
                            .map(Peer::consistentId)
                            .collect(Collectors.toList());
                }));
    }

    /** {@inheritDoc} */
    @Override
    public int partitionId(BinaryRowEx row) {
        return IgniteUtils.safeAbs(row.colocationHash()) % partitions;
    }

    /**
     * Gathers the result of batch processing into a single resulting collection of rows.
     *
     * @param rowBatches Row batches.
     * @return Future of collecting results.
     */
    static CompletableFuture<Collection<BinaryRow>> collectMultiRowsResponsesWithoutRestoreOrder(Collection<RowBatch> rowBatches) {
        return allResultFutures(rowBatches)
                .thenApply(response -> {
                    var result = new ArrayList<BinaryRow>(rowBatches.size());

                    for (RowBatch rowBatch : rowBatches) {
                        Collection<BinaryRow> batchResult = (Collection<BinaryRow>) rowBatch.getCompletedResult();

                        if (batchResult == null) {
                            continue;
                        }

                        result.addAll(batchResult);
                    }

                    return result;
                });
    }

    /**
     * Gathers the result of batch processing into a single resulting collection of rows, restoring order as in the requested collection of
     * rows.
     *
     * @param rowBatches Row batches by partition ID.
     * @return Future of collecting results.
     */
    static CompletableFuture<List<BinaryRow>> collectMultiRowsResponsesWithRestoreOrder(Collection<RowBatch> rowBatches) {
        return allResultFutures(rowBatches)
                .thenApply(response -> {
                    var result = new BinaryRow[RowBatch.getTotalRequestedRowSize(rowBatches)];

                    for (RowBatch rowBatch : rowBatches) {
                        Collection<BinaryRow> batchResult = (Collection<BinaryRow>) rowBatch.getCompletedResult();

                        assert batchResult != null;

                        assert batchResult.size() == rowBatch.requestedRows.size() :
                                "batchResult=" + batchResult.size() + ", requestedRows=" + rowBatch.requestedRows.size();

                        int i = 0;

                        for (BinaryRow resultRow : batchResult) {
                            result[rowBatch.getOriginalRowIndex(i++)] = resultRow;
                        }
                    }

                    // Use Arrays#asList to avoid copying the array.
                    return Arrays.asList(result);
                });
    }

    /**
     * Updates internal table raft group service for given partition.
     *
     * @param p Partition.
     * @param raftGrpSvc Raft group service.
     */
    public void updateInternalTableRaftGroupService(int p, RaftGroupService raftGrpSvc) {
        RaftGroupService oldSrvc;

        synchronized (updatePartitionMapsMux) {
            Int2ObjectMap<RaftGroupService> newPartitionMap = new Int2ObjectOpenHashMap<>(partitions);

            newPartitionMap.putAll(raftGroupServiceByPartitionId);

            oldSrvc = newPartitionMap.put(p, raftGrpSvc);

            raftGroupServiceByPartitionId = newPartitionMap;
        }

        if (oldSrvc != null) {
            oldSrvc.shutdown();
        }
    }

    /**
     * Enlists a partition.
     *
     * @param partId Partition id.
     * @param tx The transaction.
     * @return The enlist future (then will a leader become known).
     */
    protected CompletableFuture<IgniteBiTuple<ClusterNode, Long>> enlist(int partId, InternalTransaction tx) {
        RaftGroupService svc = raftGroupServiceByPartitionId.get(partId);
        assert svc != null : "No raft group service for partition " + partId;

        tx.assignCommitPartition(new TablePartitionId(tableId, partId));

        // TODO: IGNITE-17256 Use a placement driver for getting a primary replica.
        CompletableFuture<LeaderWithTerm> fut0 = svc.refreshAndGetLeaderWithTerm();

        // TODO asch IGNITE-15091 fixme need to map to the same leaseholder.
        // TODO asch a leader race is possible when enlisting different keys from the same partition.
        return fut0.handle((primaryPeerAndTerm, e) -> {
            if (e != null) {
                throw withCause(TransactionException::new, REPLICA_UNAVAILABLE_ERR, "Failed to get the primary replica.", e);
            }
            if (primaryPeerAndTerm.leader() == null) {
                throw new TransactionException(REPLICA_UNAVAILABLE_ERR, "Failed to get the primary replica.");
            }

            String consistentId = primaryPeerAndTerm.leader().consistentId();

            ClusterNode node = clusterNodeResolver.apply(consistentId);

            if (node == null) {
                throw new TransactionException(REPLICA_UNAVAILABLE_ERR, "Failed to resolve the primary replica node [consistentId="
                        + consistentId + ']');
            }

            TablePartitionId partGroupId = new TablePartitionId(tableId, partId);

            return tx.enlist(partGroupId, new IgniteBiTuple<>(node, primaryPeerAndTerm.term()));
        });
    }

    /**
     * Partition scan publisher.
     */
    private static class PartitionScanPublisher implements Publisher<BinaryRow> {
        /** The closure enlists a partition, that is scanned, to the transaction context and retrieves a batch rows. */
        private final BiFunction<Long, Integer, CompletableFuture<Collection<BinaryRow>>> retrieveBatch;

        /** The closure will be invoked before the cursor closed. */
        Function<CompletableFuture<Void>, CompletableFuture<Void>> onClose;

        /** True when the publisher has a subscriber, false otherwise. */
        private AtomicBoolean subscribed;

        /**
         * The constructor.
         *
         * @param retrieveBatch Closure that gets a new batch from the remote replica.
         * @param onClose The closure will be applied when {@link Subscription#cancel} is invoked directly or the cursor is
         *         finished.
         */
        PartitionScanPublisher(
                BiFunction<Long, Integer, CompletableFuture<Collection<BinaryRow>>> retrieveBatch,
                Function<CompletableFuture<Void>, CompletableFuture<Void>> onClose
        ) {
            this.retrieveBatch = retrieveBatch;
            this.onClose = onClose;

            this.subscribed = new AtomicBoolean(false);
        }

        /** {@inheritDoc} */
        @Override
        public void subscribe(Subscriber<? super BinaryRow> subscriber) {
            if (subscriber == null) {
                throw new NullPointerException("Subscriber is null");
            }

            if (!subscribed.compareAndSet(false, true)) {
                subscriber.onError(new IllegalStateException("Scan publisher does not support multiple subscriptions."));
            }

            PartitionScanSubscription subscription = new PartitionScanSubscription(subscriber);

            subscriber.onSubscribe(subscription);
        }

        /**
         * Partition Scan Subscription.
         */
        private class PartitionScanSubscription implements Subscription {
            private final Subscriber<? super BinaryRow> subscriber;

            private final AtomicBoolean canceled;

            /**
             * Scan id to uniquely identify it on server side.
             */
            private final Long scanId;

            private final AtomicLong requestedItemsCnt;

            private static final int INTERNAL_BATCH_SIZE = 10_000;

            /**
             * The constructor.
             * TODO: IGNITE-15544 Close partition scans on node left.
             *
             * @param subscriber The subscriber.
             */
            private PartitionScanSubscription(Subscriber<? super BinaryRow> subscriber) {
                this.subscriber = subscriber;
                this.canceled = new AtomicBoolean(false);
                this.scanId = CURSOR_ID_GENERATOR.getAndIncrement();
                this.requestedItemsCnt = new AtomicLong(0);
            }

            /** {@inheritDoc} */
            @Override
            public void request(long n) {
                if (n <= 0) {
                    cancel();

                    subscriber.onError(new IllegalArgumentException(IgniteStringFormatter
                            .format("Invalid requested amount of items [requested={}, minValue=1]", n))
                    );
                }

                if (canceled.get()) {
                    return;
                }

                long prevVal = requestedItemsCnt.getAndUpdate(origin -> {
                    try {
                        return Math.addExact(origin, n);
                    } catch (ArithmeticException e) {
                        return Long.MAX_VALUE;
                    }
                });

                if (prevVal == 0) {
                    scanBatch((int) Math.min(n, INTERNAL_BATCH_SIZE));
                }
            }

            /** {@inheritDoc} */
            @Override
            public void cancel() {
                cancel(null);
            }

            /**
             * After the method is called, a subscriber won't be received updates from the publisher.
             *
             * @param t An exception which was thrown when entries were retrieving from the cursor.
             */
            public void cancel(Throwable t) {
                if (!canceled.compareAndSet(false, true)) {
                    return;
                }

                onClose.apply(t == null ? completedFuture(null) : failedFuture(t)).handle((ignore, th) -> {
                    if (th != null) {
                        subscriber.onError(th);
                    } else {
                        subscriber.onComplete();
                    }

                    return null;
                });
            }

            /**
             * Requests and processes n requested elements where n is an integer.
             *
             * @param n Amount of items to request and process.
             */
            private void scanBatch(int n) {
                if (canceled.get()) {
                    return;
                }

                retrieveBatch.apply(scanId, n).thenAccept(binaryRows -> {
                    if (binaryRows == null) {
                        cancel();

                        return;
                    } else {
                        assert binaryRows.size() <= n : "Rows more then requested " + binaryRows.size() + " " + n;

                        binaryRows.forEach(subscriber::onNext);
                    }

                    if (binaryRows.size() < n) {
                        cancel();
                    } else {
                        long remaining = requestedItemsCnt.addAndGet(Math.negateExact(binaryRows.size()));

                        if (remaining > 0) {
                            scanBatch((int) Math.min(remaining, INTERNAL_BATCH_SIZE));
                        }
                    }
                }).exceptionally(t -> {
                    cancel(t);

                    return null;
                });
            }
        }
    }

    /** {@inheritDoc} */
    @Override
    public void close() {
        for (RaftGroupService srv : raftGroupServiceByPartitionId.values()) {
            srv.shutdown();
        }
    }

    // TODO: IGNITE-17963 Use smarter logic for recipient node evaluation.

    /**
     * Evaluated cluster node for read-only request processing.
     *
     * @param partId Partition id.
     * @return Cluster node to evalute read-only request.
     */
    protected CompletableFuture<ClusterNode> evaluateReadOnlyRecipientNode(int partId) {
        RaftGroupService svc = raftGroupServiceByPartitionId.get(partId);

        return svc.refreshAndGetLeaderWithTerm().handle((res, e) -> {
            if (e != null) {
                throw withCause(TransactionException::new, REPLICA_UNAVAILABLE_ERR, e);
            } else {
                if (res == null || res.leader() == null) {
                    throw withCause(TransactionException::new, REPLICA_UNAVAILABLE_ERR, e);
                } else {
                    return clusterNodeResolver.apply(res.leader().consistentId());
                }
            }
        });
    }

    /**
     * Casts any exception type to a client exception, wherein {@link ReplicationException} and {@link LockException} are wrapped to
     * {@link TransactionException}, but another exceptions are wrapped to a common exception. The method does not wrap an exception if the
     * exception already inherits type of {@link RuntimeException}.
     *
     * @param e An instance exception to cast to client side one.
     * @return {@link IgniteException} An instance of client side exception.
     */
    private RuntimeException wrapReplicationException(Throwable e) {
        if (e instanceof CompletionException) {
            e = e.getCause();
        }

        RuntimeException e0;

        if (e instanceof ReplicationException || e instanceof ConnectException || e instanceof TimeoutException) {
            e0 = withCause(TransactionException::new, TX_REPLICA_UNAVAILABLE_ERR, e);
        } else if (e instanceof LockException) {
            e0 = withCause(TransactionException::new, ACQUIRE_LOCK_ERR, e);
        } else if (!(e instanceof RuntimeException)) {
            e0 = withCause(IgniteException::new, INTERNAL_ERR, e);
        } else {
            e0 = (RuntimeException) e;
        }

        return e0;
    }

    @Override
    public @Nullable PendingComparableValuesTracker<HybridTimestamp, Void> getPartitionSafeTimeTracker(int partitionId) {
        return safeTimeTrackerByPartitionId.get(partitionId);
    }

    @Override
    public @Nullable PendingComparableValuesTracker<Long, Void> getPartitionStorageIndexTracker(int partitionId) {
        return storageIndexTrackerByPartitionId.get(partitionId);
    }

    /**
     * Updates the partition trackers, if there were previous ones, it closes them.
     *
     * @param partitionId Partition ID.
     * @param newSafeTimeTracker New partition safe time tracker.
     * @param newStorageIndexTracker New partition storage index tracker.
     */
    public void updatePartitionTrackers(
            int partitionId,
            PendingComparableValuesTracker<HybridTimestamp, Void> newSafeTimeTracker,
            PendingComparableValuesTracker<Long, Void> newStorageIndexTracker
    ) {
        PendingComparableValuesTracker<HybridTimestamp, Void> previousSafeTimeTracker;
        PendingComparableValuesTracker<Long, Void> previousStorageIndexTracker;

        synchronized (updatePartitionMapsMux) {
            Int2ObjectMap<PendingComparableValuesTracker<HybridTimestamp, Void>> newSafeTimeTrackerMap =
                    new Int2ObjectOpenHashMap<>(partitions);
            Int2ObjectMap<PendingComparableValuesTracker<Long, Void>> newStorageIndexTrackerMap = new Int2ObjectOpenHashMap<>(partitions);

            newSafeTimeTrackerMap.putAll(safeTimeTrackerByPartitionId);
            newStorageIndexTrackerMap.putAll(storageIndexTrackerByPartitionId);

            previousSafeTimeTracker = newSafeTimeTrackerMap.put(partitionId, newSafeTimeTracker);
            previousStorageIndexTracker = newStorageIndexTrackerMap.put(partitionId, newStorageIndexTracker);

            safeTimeTrackerByPartitionId = newSafeTimeTrackerMap;
            storageIndexTrackerByPartitionId = newStorageIndexTrackerMap;
        }

        if (previousSafeTimeTracker != null) {
            previousSafeTimeTracker.close();
        }

        if (previousStorageIndexTracker != null) {
            previousStorageIndexTracker.close();
        }
    }

    private ReplicaRequest upsertAllInternal(
            TablePartitionId commitPart,
            Collection<? extends BinaryRow> keyRows0,
            InternalTransaction txo,
            ReplicationGroupId groupId,
            Long term) {
        return tableMessagesFactory.readWriteMultiRowReplicaRequest()
                .groupId(groupId)
                .commitPartitionId(commitPart)
                .binaryRowsBytes(serializeBinaryRows(keyRows0))
                .transactionId(txo.id())
                .term(term)
                .requestType(RequestType.RW_UPSERT_ALL)
                .timestampLong(clock.nowLong())
                .build();
    }
}
