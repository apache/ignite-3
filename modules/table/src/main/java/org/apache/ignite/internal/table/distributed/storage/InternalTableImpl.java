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
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;
import static org.apache.ignite.internal.table.distributed.replicator.action.RequestType.RW_DELETE_ALL;
import static org.apache.ignite.internal.table.distributed.replicator.action.RequestType.RW_GET;
import static org.apache.ignite.internal.table.distributed.replicator.action.RequestType.RW_GET_ALL;
import static org.apache.ignite.internal.table.distributed.storage.RowBatch.allResultFutures;
import static org.apache.ignite.internal.util.ExceptionUtils.withCause;
import static org.apache.ignite.lang.ErrorGroups.Common.INTERNAL_ERR;
import static org.apache.ignite.lang.ErrorGroups.Replicator.REPLICA_UNAVAILABLE_ERR;
import static org.apache.ignite.lang.ErrorGroups.Transactions.ACQUIRE_LOCK_ERR;
import static org.apache.ignite.lang.ErrorGroups.Transactions.TX_FAILED_READ_WRITE_OPERATION_ERR;
import static org.apache.ignite.lang.ErrorGroups.Transactions.TX_REPLICA_UNAVAILABLE_ERR;
import static org.apache.ignite.lang.ErrorGroups.Transactions.TX_UNEXPECTED_STATE_ERR;

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap.Entry;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import java.net.ConnectException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Flow.Publisher;
import java.util.concurrent.Flow.Subscriber;
import java.util.concurrent.Flow.Subscription;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiFunction;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.ignite.internal.binarytuple.BinaryTupleReader;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.lang.IgniteBiTuple;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.internal.lang.IgnitePentaFunction;
import org.apache.ignite.internal.lang.IgniteTriFunction;
import org.apache.ignite.internal.placementdriver.PlacementDriver;
import org.apache.ignite.internal.placementdriver.ReplicaMeta;
import org.apache.ignite.internal.raft.Peer;
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
import org.apache.ignite.internal.table.distributed.command.TablePartitionIdMessage;
import org.apache.ignite.internal.table.distributed.replication.request.BinaryTupleMessage;
import org.apache.ignite.internal.table.distributed.replication.request.MultipleRowPkReplicaRequest;
import org.apache.ignite.internal.table.distributed.replication.request.MultipleRowReplicaRequest;
import org.apache.ignite.internal.table.distributed.replication.request.ReadOnlyMultiRowPkReplicaRequest;
import org.apache.ignite.internal.table.distributed.replication.request.ReadOnlyScanRetrieveBatchReplicaRequest;
import org.apache.ignite.internal.table.distributed.replication.request.ReadWriteMultiRowPkReplicaRequest;
import org.apache.ignite.internal.table.distributed.replication.request.ReadWriteMultiRowReplicaRequest;
import org.apache.ignite.internal.table.distributed.replication.request.ReadWriteScanRetrieveBatchReplicaRequest;
import org.apache.ignite.internal.table.distributed.replication.request.ReadWriteScanRetrieveBatchReplicaRequestBuilder;
import org.apache.ignite.internal.table.distributed.replication.request.SingleRowPkReplicaRequest;
import org.apache.ignite.internal.table.distributed.replication.request.SingleRowReplicaRequest;
import org.apache.ignite.internal.table.distributed.replication.request.SwapRowReplicaRequest;
import org.apache.ignite.internal.table.distributed.replicator.action.RequestType;
import org.apache.ignite.internal.tx.HybridTimestampTracker;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.internal.tx.LockException;
import org.apache.ignite.internal.tx.TxManager;
import org.apache.ignite.internal.tx.storage.state.TxStateTableStorage;
import org.apache.ignite.internal.util.CollectionUtils;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.util.PendingComparableValuesTracker;
import org.apache.ignite.internal.utils.PrimaryReplica;
import org.apache.ignite.lang.IgniteException;
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

    private static final int AWAIT_PRIMARY_REPLICA_TIMEOUT = 30;

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

    /** Observable timestamp tracker. */
    private final HybridTimestampTracker observableTimestampTracker;

    /** Placement driver. */
    private final PlacementDriver placementDriver;

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
     * @param placementDriver Placement driver.
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
            HybridClock clock,
            HybridTimestampTracker observableTimestampTracker,
            PlacementDriver placementDriver
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
        this.observableTimestampTracker = observableTimestampTracker;
        this.placementDriver = placementDriver;
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
     * @param tx The transaction, not null if explicit.
     * @param fac Replica requests factory.
     * @param noWriteChecker Used to handle operations producing no updates.
     * @return The future.
     */
    private <R> CompletableFuture<R> enlistInTx(
            BinaryRowEx row,
            @Nullable InternalTransaction tx,
            IgniteTriFunction<InternalTransaction, ReplicationGroupId, Long, ReplicaRequest> fac,
            BiPredicate<R, ReplicaRequest> noWriteChecker
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

        boolean implicit = tx == null;
        InternalTransaction actualTx = startImplicitRwTxIfNeeded(tx);

        int partId = partitionId(row);

        TablePartitionId partGroupId = new TablePartitionId(tableId, partId);

        IgniteBiTuple<ClusterNode, Long> primaryReplicaAndTerm = actualTx.enlistedNodeAndTerm(partGroupId);

        CompletableFuture<R> fut;

        if (primaryReplicaAndTerm != null) {
            assert !implicit;

            fut = trackingInvoke(actualTx, partId, term -> fac.apply(actualTx, partGroupId, term), false, primaryReplicaAndTerm,
                    noWriteChecker);
        } else {
            fut = enlistWithRetry(actualTx, partId, term -> fac.apply(actualTx, partGroupId, term), ATTEMPTS_TO_ENLIST_PARTITION,
                    implicit, noWriteChecker);
        }

        return postEnlist(fut, false, actualTx, implicit);
    }

    /**
     * Enlists a single row into a transaction.
     *
     * @param keyRows Rows.
     * @param tx The transaction.
     * @param fac Replica requests factory.
     * @param reducer Transform reducer.
     * @param noOpChecker Used to handle no-op operations (producing no updates).
     * @return The future.
     */
    private <T> CompletableFuture<T> enlistInTx(
            Collection<BinaryRowEx> keyRows,
            @Nullable InternalTransaction tx,
            IgnitePentaFunction<
                    Collection<? extends BinaryRow>, InternalTransaction, ReplicationGroupId, Long, Boolean, ReplicaRequest
            > fac,
            Function<Collection<RowBatch>, CompletableFuture<T>> reducer,
            BiPredicate<T, ReplicaRequest> noOpChecker
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

        boolean implicit = tx == null;
        InternalTransaction actualTx = startImplicitRwTxIfNeeded(tx);

        Int2ObjectMap<RowBatch> rowBatchByPartitionId = toRowBatchByPartitionId(keyRows);

        boolean singlePart = rowBatchByPartitionId.size() == 1;
        boolean full = implicit && singlePart;

        for (Int2ObjectMap.Entry<RowBatch> partitionRowBatch : rowBatchByPartitionId.int2ObjectEntrySet()) {
            int partitionId = partitionRowBatch.getIntKey();
            RowBatch rowBatch = partitionRowBatch.getValue();

            TablePartitionId partGroupId = new TablePartitionId(tableId, partitionId);

            IgniteBiTuple<ClusterNode, Long> primaryReplicaAndTerm = actualTx.enlistedNodeAndTerm(partGroupId);

            CompletableFuture<T> fut;

            if (primaryReplicaAndTerm != null) {
                assert !implicit;

                fut = trackingInvoke(actualTx, partitionId, term -> fac.apply(rowBatch.requestedRows, actualTx, partGroupId, term, false),
                        false, primaryReplicaAndTerm, noOpChecker);
            } else {
                fut = enlistWithRetry(
                        actualTx,
                        partitionId,
                        term -> fac.apply(rowBatch.requestedRows, actualTx, partGroupId, term, full),
                        ATTEMPTS_TO_ENLIST_PARTITION,
                        full,
                        noOpChecker
                );
            }

            rowBatch.resultFuture = fut;
        }

        CompletableFuture<T> fut = reducer.apply(rowBatchByPartitionId.values());

        return postEnlist(fut, implicit && !singlePart, actualTx, full);
    }

    private InternalTransaction startImplicitRwTxIfNeeded(@Nullable InternalTransaction tx) {
        return tx == null ? txManager.begin(observableTimestampTracker) : tx;
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
     * @param implicit {@code True} if the implicit txn.
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
            @Nullable BitSet columnsToInclude,
            boolean implicit
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
                .full(implicit) // Intent for one phase commit.
                .batchSize(batchSize);

        if (primaryReplicaAndTerm != null) {
            ReadWriteScanRetrieveBatchReplicaRequest request = requestBuilder.term(primaryReplicaAndTerm.get2()).build();
            fut = replicaSvc.invoke(primaryReplicaAndTerm.get1(), request);
        } else {
            fut = enlistWithRetry(tx, partId, term -> requestBuilder.term(term).build(), ATTEMPTS_TO_ENLIST_PARTITION, false, null);
        }

        return postEnlist(fut, false, tx, false);
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
     * @param mapFunc Function to create replica request with new raft term.
     * @param attempts Number of attempts.
     * @param full {@code True} if is a full transaction.
     * @param noWriteChecker Used to handle operations producing no updates.
     * @return The future.
     */
    private <R> CompletableFuture<R> enlistWithRetry(
            InternalTransaction tx,
            int partId,
            Function<Long, ReplicaRequest> mapFunc,
            int attempts,
            boolean full,
            @Nullable BiPredicate<R, ReplicaRequest> noWriteChecker
    ) {
        CompletableFuture<R> result = new CompletableFuture<>();

        enlist(partId, tx).thenCompose(
                        primaryReplicaAndTerm -> trackingInvoke(tx, partId, mapFunc, full, primaryReplicaAndTerm, noWriteChecker))
                .handle((res0, e) -> {
                    if (e != null) {
                        if (e.getCause() instanceof PrimaryReplicaMissException && attempts > 0) {
                            return enlistWithRetry(tx, partId, mapFunc, attempts - 1, full, noWriteChecker).handle((r2, e2) -> {
                                if (e2 != null) {
                                    return result.completeExceptionally(e2);
                                } else {
                                    return result.complete(r2);
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
     * Invoke replica with additional tracking for writes.
     *
     * @param tx The transaction.
     * @param partId Partition id.
     * @param mapFunc Request factory.
     * @param full {@code True} for a full transaction.
     * @param primaryReplicaAndTerm Replica and term.
     * @param noWriteChecker Used to handle operations producing no updates.
     * @return The future.
     */
    private <R> CompletableFuture<R> trackingInvoke(
            InternalTransaction tx,
            int partId,
            Function<Long, ReplicaRequest> mapFunc,
            boolean full,
            IgniteBiTuple<ClusterNode, Long> primaryReplicaAndTerm,
            @Nullable BiPredicate<R, ReplicaRequest> noWriteChecker
    ) {
        ReplicaRequest request = mapFunc.apply(primaryReplicaAndTerm.get2());

        boolean write = request instanceof SingleRowReplicaRequest && ((SingleRowReplicaRequest) request).requestType() != RW_GET
                || request instanceof MultipleRowReplicaRequest && ((MultipleRowReplicaRequest) request).requestType() != RW_GET_ALL
                || request instanceof SingleRowPkReplicaRequest && ((SingleRowPkReplicaRequest) request).requestType() != RW_GET
                || request instanceof MultipleRowPkReplicaRequest && ((MultipleRowPkReplicaRequest) request).requestType() != RW_GET_ALL
                || request instanceof SwapRowReplicaRequest;

        if (write && !full) {
            // Track only write requests from explicit transactions.
            if (!txManager.addInflight(tx.id())) {
                return failedFuture(
                        new TransactionException(TX_UNEXPECTED_STATE_ERR, format(
                                "Failed to enlist a write operation into a transaction, tx is locked for updates "
                                        + "[tableName={}, partId={}, txState={}].",
                                tableName,
                                partId,
                                tx.state()
                        )));
            }

            return replicaSvc.<R>invoke(primaryReplicaAndTerm.get1(), request).thenApply(res -> {
                assert noWriteChecker != null;

                // Remove inflight if no replication was scheduled, otherwise inflight will be removed by delayed response.
                if (noWriteChecker.test(res, request)) {
                    txManager.removeInflight(tx.id());
                }

                return res;
            });
        } else {
            return replicaSvc.invoke(primaryReplicaAndTerm.get1(), request);
        }
    }

    /**
     * Performs post enlist operation.
     *
     * @param fut The future.
     * @param autoCommit {@code True} for auto commit.
     * @param tx0 The transaction.
     * @param full {@code True} if this is a full transaction.
     * @param <T> Operation return type.
     * @return The future.
     */
    private <T> CompletableFuture<T> postEnlist(CompletableFuture<T> fut, boolean autoCommit, InternalTransaction tx0, boolean full) {
        assert !(autoCommit && full) : "Invalid combination of flags";

        return fut.handle((BiFunction<T, Throwable, CompletableFuture<T>>) (r, e) -> {
            if (full) { // Full txn is already finished remotely. Just update local state.
                txManager.finishFull(observableTimestampTracker, tx0.id(), e == null);

                return e != null ? failedFuture(wrapReplicationException(e)) : completedFuture(r);
            }

            if (e != null) {
                RuntimeException e0 = wrapReplicationException(e);

                return tx0.rollbackAsync().handle((ignored, err) -> {
                    if (err != null) {
                        e0.addSuppressed(err);
                    }
                    throw e0;
                }); // Preserve failed state.
            } else {
                if (autoCommit) {
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

    /**
     * Evaluates the single-row request to the cluster for a read-only single-partition transaction.
     *
     * @param row Binary row.
     * @param op Operation.
     * @param <R> Result type.
     * @return The future.
     */
    private <R> CompletableFuture<R> evaluateReadOnlyPrimaryNode(
            BinaryRowEx row,
            BiFunction<ReplicationGroupId, Long, ReplicaRequest> op
    ) {
        InternalTransaction tx = txManager.begin(observableTimestampTracker, true);

        int partId = partitionId(row);

        TablePartitionId tablePartitionId = new TablePartitionId(tableId, partId);

        CompletableFuture<ReplicaMeta> primaryReplicaFuture = placementDriver.awaitPrimaryReplica(
                tablePartitionId,
                tx.startTimestamp(),
                AWAIT_PRIMARY_REPLICA_TIMEOUT,
                TimeUnit.SECONDS
        );

        CompletableFuture<R>  fut = primaryReplicaFuture.thenCompose(primaryReplica -> {
            try {
                ClusterNode node = clusterNodeResolver.apply(primaryReplica.getLeaseholder());

                if (node == null) {
                    throw new TransactionException(REPLICA_UNAVAILABLE_ERR, "Failed to resolve the primary replica node [consistentId="
                            + primaryReplica.getLeaseholder() + ']');
                }

                return replicaSvc.invoke(node, op.apply(tablePartitionId, primaryReplica.getStartTime().longValue()));
            } catch (Throwable e) {
                throw new TransactionException(
                        INTERNAL_ERR,
                        format(
                                "Failed to invoke the replica request [tableName={}, partId={}].",
                                tableName,
                                partId
                        ),
                        e
                );
            }
        });

        return postEvaluate(fut, tx);
    }

    /**
     * Evaluates the multi-row request to the cluster for a read-only single-partition transaction.
     *
     * @param rows Rows.
     * @param op Replica requests factory.
     * @param <R> Result type.
     * @return The future.
     */
    private <R> CompletableFuture<R> evaluateReadOnlyPrimaryNode(
            Collection<BinaryRowEx> rows,
            BiFunction<ReplicationGroupId, Long, ReplicaRequest> op
    ) {
        InternalTransaction tx = txManager.begin(observableTimestampTracker, true);

        int partId = partitionId(rows.iterator().next());

        TablePartitionId tablePartitionId = new TablePartitionId(tableId, partId);

        CompletableFuture<ReplicaMeta> primaryReplicaFuture = placementDriver.awaitPrimaryReplica(
                tablePartitionId,
                tx.startTimestamp(),
                AWAIT_PRIMARY_REPLICA_TIMEOUT,
                TimeUnit.SECONDS
        );

        CompletableFuture<R>  fut = primaryReplicaFuture.thenCompose(primaryReplica -> {
            try {
                ClusterNode node = clusterNodeResolver.apply(primaryReplica.getLeaseholder());

                if (node == null) {
                    throw new TransactionException(REPLICA_UNAVAILABLE_ERR, "Failed to resolve the primary replica node [consistentId="
                            + primaryReplica.getLeaseholder() + ']');
                }

                return replicaSvc.invoke(node, op.apply(tablePartitionId, primaryReplica.getStartTime().longValue()));
            } catch (Throwable e) {
                throw new TransactionException(
                        INTERNAL_ERR,
                        format(
                                "Failed to invoke the replica request [tableName={}, partId={}].",
                                tableName,
                                partId
                        ),
                        e
                );
            }
        });

        return postEvaluate(fut, tx);
    }

    /**
     * Performs post evaluate operation.
     *
     * @param fut The future.
     * @param tx The transaction.
     * @param <R> Operation return type.
     * @return The future.
     */
    private <R> CompletableFuture<R> postEvaluate(CompletableFuture<R> fut, InternalTransaction tx) {
        return fut.handle((BiFunction<R, Throwable, CompletableFuture<R>>) (r, e) -> {
            if (e != null) {
                RuntimeException e0 = wrapReplicationException(e);

                return tx.finish(false, clock.now())
                        .handle((ignored, err) -> {

                            if (err != null) {
                                e0.addSuppressed(err);
                            }
                            throw e0;
                        }); // Preserve failed state.
            }

            return tx.finish(true, clock.now())
                    .exceptionally(ex -> {
                        throw wrapReplicationException(ex);
                    })
                    .thenApply(ignored -> r);
        }).thenCompose(x -> x);
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<BinaryRow> get(BinaryRowEx keyRow, InternalTransaction tx) {
        if (tx == null) {
            return evaluateReadOnlyPrimaryNode(
                    keyRow,
                    (groupId, consistencyToken) -> tableMessagesFactory.readOnlyDirectSingleRowReplicaRequest()
                            .groupId(groupId)
                            .enlistmentConsistencyToken(consistencyToken)
                            .schemaVersion(keyRow.schemaVersion())
                            .primaryKey(keyRow.tupleSlice())
                            .requestType(RequestType.RO_GET)
                            .build()
            );
        }

        if (tx.isReadOnly()) {
            return evaluateReadOnlyRecipientNode(partitionId(keyRow))
                    .thenCompose(recipientNode -> get(keyRow, tx.readTimestamp(), recipientNode));
        }

        return enlistInTx(
                keyRow,
                tx,
                (txo, groupId, term) -> tableMessagesFactory.readWriteSingleRowPkReplicaRequest()
                        .groupId(groupId)
                        .schemaVersion(keyRow.schemaVersion())
                        .primaryKey(keyRow.tupleSlice())
                        .commitPartitionId(serializeTablePartitionId(txo.commitPartition()))
                        .transactionId(txo.id())
                        .term(term)
                        .requestType(RW_GET)
                        .timestampLong(clock.nowLong())
                        .full(tx == null)
                        .build(),
                (res, req) -> false
        );
    }

    @Override
    public CompletableFuture<BinaryRow> get(
            BinaryRowEx keyRow,
            HybridTimestamp readTimestamp,
            ClusterNode recipientNode
    ) {
        int partId = partitionId(keyRow);
        ReplicationGroupId partGroupId = raftGroupServiceByPartitionId.get(partId).groupId();

        return replicaSvc.invoke(recipientNode, tableMessagesFactory.readOnlySingleRowPkReplicaRequest()
                .groupId(partGroupId)
                .schemaVersion(keyRow.schemaVersion())
                .primaryKey(keyRow.tupleSlice())
                .requestType(RequestType.RO_GET)
                .readTimestampLong(readTimestamp.longValue())
                .build()
        );
    }

    /**
     * Checks that the batch of rows belongs to a single partition.
     *
     * @param rows Batch of rows.
     * @return If all rows belong to one partition, the method returns true; otherwise, it returns false.
     */
    private boolean isSinglePartitionBatch(Collection<BinaryRowEx> rows) {
        Iterator<BinaryRowEx> rowIterator = rows.iterator();

        int partId = partitionId(rowIterator.next());

        while (rowIterator.hasNext()) {
            BinaryRowEx row = rowIterator.next();

            if (partId != partitionId(row)) {
                return false;
            }
        }

        return true;
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<List<BinaryRow>> getAll(Collection<BinaryRowEx> keyRows, InternalTransaction tx) {
        if (CollectionUtils.nullOrEmpty(keyRows)) {
            return completedFuture(Collections.emptyList());
        }

        if (tx == null && isSinglePartitionBatch(keyRows)) {
            return evaluateReadOnlyPrimaryNode(
                    keyRows,
                    (groupId, consistencyToken) -> tableMessagesFactory.readOnlyDirectMultiRowReplicaRequest()
                            .groupId(groupId)
                            .enlistmentConsistencyToken(consistencyToken)
                            .schemaVersion(keyRows.iterator().next().schemaVersion())
                            .primaryKeys(serializeBinaryTuples(keyRows))
                            .requestType(RequestType.RO_GET_ALL)
                            .build()
            );
        }

        if (tx != null && tx.isReadOnly()) {
            BinaryRowEx firstRow = keyRows.iterator().next();

            return evaluateReadOnlyRecipientNode(partitionId(firstRow))
                        .thenCompose(recipientNode -> getAll(keyRows, tx.readTimestamp(), recipientNode));
        }

        return enlistInTx(
                keyRows,
                tx,
                    (keyRows0, txo, groupId, term, full) -> {
                        return readWriteMultiRowPkReplicaRequest(RW_GET_ALL, keyRows0, txo, groupId, term, full);
                    },
                InternalTableImpl::collectMultiRowsResponsesWithRestoreOrder,
                (res, req) -> false
        );
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

            ReadOnlyMultiRowPkReplicaRequest request = tableMessagesFactory.readOnlyMultiRowPkReplicaRequest()
                    .groupId(partGroupId)
                    .schemaVersion(partitionRowBatch.getValue().requestedRows.get(0).schemaVersion())
                    .primaryKeys(serializeBinaryTuples(partitionRowBatch.getValue().requestedRows))
                    .requestType(RequestType.RO_GET_ALL)
                    .readTimestampLong(readTimestamp.longValue())
                    .build();

            partitionRowBatch.getValue().resultFuture = replicaSvc.invoke(recipientNode, request);
        }

        return collectMultiRowsResponsesWithRestoreOrder(rowBatchByPartitionId.values());
    }

    private ReadWriteMultiRowPkReplicaRequest readWriteMultiRowPkReplicaRequest(
            RequestType requestType,
            Collection<? extends BinaryRow> rows,
            InternalTransaction tx,
            ReplicationGroupId groupId,
            Long term,
            boolean full
    ) {
        assert allSchemaVersionsSame(rows) : "Different schema versions encountered: " + uniqueSchemaVersions(rows);

        return tableMessagesFactory.readWriteMultiRowPkReplicaRequest()
                .groupId(groupId)
                .commitPartitionId(serializeTablePartitionId(tx.commitPartition()))
                .schemaVersion(rows.iterator().next().schemaVersion())
                .primaryKeys(serializeBinaryTuples(rows))
                .transactionId(tx.id())
                .term(term)
                .requestType(requestType)
                .timestampLong(clock.nowLong())
                .full(full)
                .build();
    }

    private static boolean allSchemaVersionsSame(Collection<? extends BinaryRow> rows) {
        int schemaVersion = -1;
        boolean first = true;

        for (BinaryRow row : rows) {
            if (first) {
                schemaVersion = row.schemaVersion();
                first = false;

                continue;
            }

            if (row.schemaVersion() != schemaVersion) {
                return false;
            }
        }

        return true;
    }

    private static Set<Integer> uniqueSchemaVersions(Collection<? extends BinaryRow> rows) {
        Set<Integer> set = new HashSet<>();

        for (BinaryRow row : rows) {
            set.add(row.schemaVersion());
        }

        return set;
    }

    private static List<ByteBuffer> serializeBinaryTuples(Collection<? extends BinaryRow> keys) {
        var result = new ArrayList<ByteBuffer>(keys.size());

        for (BinaryRow row : keys) {
            result.add(row.tupleSlice());
        }

        return result;
    }

    private TablePartitionIdMessage serializeTablePartitionId(TablePartitionId id) {
        return tableMessagesFactory.tablePartitionIdMessage()
                .partitionId(id.partitionId())
                .tableId(id.tableId())
                .build();
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Void> upsert(BinaryRowEx row, InternalTransaction tx) {
        return enlistInTx(
                row,
                tx,
                (txo, groupId, term) -> tableMessagesFactory.readWriteSingleRowReplicaRequest()
                        .groupId(groupId)
                        .commitPartitionId(serializeTablePartitionId(txo.commitPartition()))
                        .schemaVersion(row.schemaVersion())
                        .binaryTuple(row.tupleSlice())
                        .transactionId(txo.id())
                        .term(term)
                        .requestType(RequestType.RW_UPSERT)
                        .timestampLong(clock.nowLong())
                        .full(tx == null)
                        .build(),
                (res, req) -> false
        );
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Void> upsertAll(Collection<BinaryRowEx> rows, InternalTransaction tx) {
        return enlistInTx(
                rows,
                tx,
                this::upsertAllInternal,
                RowBatch::allResultFutures,
                (res, req) -> false
        );
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Void> upsertAll(Collection<BinaryRowEx> rows, int partition) {
        InternalTransaction tx = txManager.begin(observableTimestampTracker);
        TablePartitionId partGroupId = new TablePartitionId(tableId, partition);

        CompletableFuture<Void> fut = enlistWithRetry(
                tx,
                partition,
                term -> upsertAllInternal(rows, tx, partGroupId, term, true),
                ATTEMPTS_TO_ENLIST_PARTITION,
                true,
                null
        );

        return postEnlist(fut, false, tx, true); // Will be committed in one RTT.
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<BinaryRow> getAndUpsert(BinaryRowEx row, InternalTransaction tx) {
        return enlistInTx(
                row,
                tx,
                (txo, groupId, term) -> tableMessagesFactory.readWriteSingleRowReplicaRequest()
                        .groupId(groupId)
                        .commitPartitionId(serializeTablePartitionId(txo.commitPartition()))
                        .schemaVersion(row.schemaVersion())
                        .binaryTuple(row.tupleSlice())
                        .transactionId(txo.id())
                        .term(term)
                        .requestType(RequestType.RW_GET_AND_UPSERT)
                        .timestampLong(clock.nowLong())
                        .full(tx == null)
                        .build(),
                (res, req) -> false
        );
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Boolean> insert(BinaryRowEx row, InternalTransaction tx) {
        return enlistInTx(
                row,
                tx,
                (txo, groupId, term) -> tableMessagesFactory.readWriteSingleRowReplicaRequest()
                        .groupId(groupId)
                        .commitPartitionId(serializeTablePartitionId(txo.commitPartition()))
                        .schemaVersion(row.schemaVersion())
                        .binaryTuple(row.tupleSlice())
                        .transactionId(txo.id())
                        .term(term)
                        .requestType(RequestType.RW_INSERT)
                        .timestampLong(clock.nowLong())
                        .full(tx == null)
                        .build(),
                (res, req) -> !res
        );
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<List<BinaryRow>> insertAll(Collection<BinaryRowEx> rows, InternalTransaction tx) {
        return enlistInTx(
                rows,
                tx,
                (keyRows0, txo, groupId, term, full) -> {
                    return readWriteMultiRowReplicaRequest(RequestType.RW_INSERT_ALL, keyRows0, txo, groupId, term, full);
                },
                InternalTableImpl::collectRejectedRowsResponsesWithRestoreOrder,
                (res, req) -> {
                    for (BinaryRow row : res) {
                        if (row != null) {
                            return false;
                        }
                    }

                    // All values are null, this means nothing was deleted.
                    return true;
                }
        );
    }

    private ReadWriteMultiRowReplicaRequest readWriteMultiRowReplicaRequest(
            RequestType requestType,
            Collection<? extends BinaryRow> rows,
            InternalTransaction tx,
            ReplicationGroupId groupId,
            Long term,
            boolean full
    ) {
        assert allSchemaVersionsSame(rows) : "Different schema versions encountered: " + uniqueSchemaVersions(rows);

        return tableMessagesFactory.readWriteMultiRowReplicaRequest()
                .groupId(groupId)
                .commitPartitionId(serializeTablePartitionId(tx.commitPartition()))
                .schemaVersion(rows.iterator().next().schemaVersion())
                .binaryTuples(serializeBinaryTuples(rows))
                .transactionId(tx.id())
                .term(term)
                .requestType(requestType)
                .timestampLong(clock.nowLong())
                .full(full)
                .build();
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Boolean> replace(BinaryRowEx row, InternalTransaction tx) {
        return enlistInTx(
                row,
                tx,
                (txo, groupId, term) -> tableMessagesFactory.readWriteSingleRowReplicaRequest()
                        .groupId(groupId)
                        .commitPartitionId(serializeTablePartitionId(txo.commitPartition()))
                        .schemaVersion(row.schemaVersion())
                        .binaryTuple(row.tupleSlice())
                        .transactionId(txo.id())
                        .term(term)
                        .requestType(RequestType.RW_REPLACE_IF_EXIST)
                        .timestampLong(clock.nowLong())
                        .full(tx == null)
                        .build(),
                (res, req) -> !res
        );
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Boolean> replace(BinaryRowEx oldRow, BinaryRowEx newRow, InternalTransaction tx) {
        assert oldRow.schemaVersion() == newRow.schemaVersion()
                : "Mismatching schema versions: old " + oldRow.schemaVersion() + ", new " + newRow.schemaVersion();

        return enlistInTx(
                newRow,
                tx,
                (txo, groupId, term) -> tableMessagesFactory.readWriteSwapRowReplicaRequest()
                        .groupId(groupId)
                        .commitPartitionId(serializeTablePartitionId(txo.commitPartition()))
                        .schemaVersion(oldRow.schemaVersion())
                        .oldBinaryTuple(oldRow.tupleSlice())
                        .newBinaryTuple(newRow.tupleSlice())
                        .transactionId(txo.id())
                        .term(term)
                        .requestType(RequestType.RW_REPLACE)
                        .timestampLong(clock.nowLong())
                        .full(tx == null)
                        .build(),
                (res, req) -> !res
        );
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<BinaryRow> getAndReplace(BinaryRowEx row, InternalTransaction tx) {
        return enlistInTx(
                row,
                tx,
                (txo, groupId, term) -> tableMessagesFactory.readWriteSingleRowReplicaRequest()
                        .groupId(groupId)
                        .commitPartitionId(serializeTablePartitionId(txo.commitPartition()))
                        .schemaVersion(row.schemaVersion())
                        .binaryTuple(row.tupleSlice())
                        .transactionId(txo.id())
                        .term(term)
                        .requestType(RequestType.RW_GET_AND_REPLACE)
                        .timestampLong(clock.nowLong())
                        .full(tx == null)
                        .build(),
                (res, req) -> res == null
        );
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Boolean> delete(BinaryRowEx keyRow, InternalTransaction tx) {
        return enlistInTx(
                keyRow,
                tx,
                (txo, groupId, term) -> tableMessagesFactory.readWriteSingleRowPkReplicaRequest()
                        .groupId(groupId)
                        .commitPartitionId(serializeTablePartitionId(txo.commitPartition()))
                        .schemaVersion(keyRow.schemaVersion())
                        .primaryKey(keyRow.tupleSlice())
                        .transactionId(txo.id())
                        .term(term)
                        .requestType(RequestType.RW_DELETE)
                        .timestampLong(clock.nowLong())
                        .full(tx == null)
                        .build(),
                (res, req) -> !res
        );
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Boolean> deleteExact(BinaryRowEx oldRow, InternalTransaction tx) {
        return enlistInTx(
                oldRow,
                tx,
                (txo, groupId, term) -> tableMessagesFactory.readWriteSingleRowReplicaRequest()
                        .groupId(groupId)
                        .commitPartitionId(serializeTablePartitionId(txo.commitPartition()))
                        .schemaVersion(oldRow.schemaVersion())
                        .binaryTuple(oldRow.tupleSlice())
                        .transactionId(txo.id())
                        .term(term)
                        .requestType(RequestType.RW_DELETE_EXACT)
                        .timestampLong(clock.nowLong())
                        .full(tx == null)
                        .build(),
                (res, req) -> !res
        );
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<BinaryRow> getAndDelete(BinaryRowEx row, InternalTransaction tx) {
        return enlistInTx(
                row,
                tx,
                (txo, groupId, term) -> tableMessagesFactory.readWriteSingleRowPkReplicaRequest()
                        .groupId(groupId)
                        .commitPartitionId(serializeTablePartitionId(txo.commitPartition()))
                        .schemaVersion(row.schemaVersion())
                        .primaryKey(row.tupleSlice())
                        .transactionId(txo.id())
                        .term(term)
                        .requestType(RequestType.RW_GET_AND_DELETE)
                        .timestampLong(clock.nowLong())
                        .full(tx == null)
                        .build(),
                (res, req) -> res == null
        );
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<List<BinaryRow>> deleteAll(Collection<BinaryRowEx> rows, InternalTransaction tx) {
        return enlistInTx(
                rows,
                tx,
                (keyRows0, txo, groupId, term, full) -> {
                    return readWriteMultiRowPkReplicaRequest(RW_DELETE_ALL, keyRows0, txo, groupId, term, full);
                },
                InternalTableImpl::collectRejectedRowsResponsesWithRestoreOrder,
                (res, req) -> {
                    for (BinaryRow row : res) {
                        if (row != null) {
                            return false;
                        }
                    }

                    // All values are null, this means nothing was deleted.
                    return true;
                }
        );
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<List<BinaryRow>> deleteAllExact(
            Collection<BinaryRowEx> rows,
            InternalTransaction tx
    ) {
        return enlistInTx(
                rows,
                tx,
                (keyRows0, txo, groupId, term, full) -> {
                    return readWriteMultiRowReplicaRequest(RequestType.RW_DELETE_EXACT_ALL, keyRows0, txo, groupId, term, full);
                },
                InternalTableImpl::collectRejectedRowsResponsesWithRestoreOrder,
                (res, req) -> {
                    for (BinaryRow row : res) {
                        if (row != null) {
                            return false;
                        }
                    }

                    // All values are null, this means nothing was deleted.
                    return true;
                }
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
                (unused, fut) -> fut);
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
                    TX_FAILED_READ_WRITE_OPERATION_ERR,
                    "Failed to enlist read-write operation into read-only transaction txId={" + tx.id() + '}'
            );
        }

        validatePartitionIndex(partId);

        boolean implicit = tx == null;
        InternalTransaction actualTx = startImplicitRwTxIfNeeded(tx);

        return new PartitionScanPublisher(
                (scanId, batchSize) -> enlistCursorInTx(
                        actualTx,
                        partId,
                        scanId,
                        batchSize,
                        indexId,
                        exactKey,
                        lowerBound,
                        upperBound,
                        flags,
                        columnsToInclude,
                        implicit
                ),
                (commit, fut) -> postEnlist(fut, commit, actualTx, implicit && !commit)
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
                            .full(false) // Set explicitly.
                            .build();

                    return replicaSvc.invoke(recipient.node(), request);
                },
                // TODO: IGNITE-17666 Close cursor tx finish.
                (unused, fut) -> fut);
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
                    format(
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
    // TODO: https://issues.apache.org/jira/browse/IGNITE-19619 The method should be removed, SQL engine should use placementDriver directly
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
    // TODO: https://issues.apache.org/jira/browse/IGNITE-19619 The method should be removed, SQL engine should use placementDriver directly
    public CompletableFuture<List<PrimaryReplica>> primaryReplicas() {
        List<Entry<RaftGroupService>> entries = new ArrayList<>(raftGroupServiceByPartitionId.int2ObjectEntrySet());
        List<CompletableFuture<PrimaryReplica>> result = new ArrayList<>();

        entries.sort(Comparator.comparingInt(Entry::getIntKey));

        for (Entry<RaftGroupService> e : entries) {
            CompletableFuture<ReplicaMeta> f = placementDriver.awaitPrimaryReplica(
                    e.getValue().groupId(),
                    clock.now(),
                    AWAIT_PRIMARY_REPLICA_TIMEOUT,
                    SECONDS
            );

            result.add(f.thenApply(primaryReplica -> {
                ClusterNode node = clusterNodeResolver.apply(primaryReplica.getLeaseholder());
                return new PrimaryReplica(node, primaryReplica.getStartTime().longValue());
            }));
        }

        CompletableFuture<Void> all = CompletableFuture.allOf(result.toArray(new CompletableFuture[0]));

        return all.thenApply(v -> result.stream()
                .map(CompletableFuture::join)
                .collect(Collectors.toList())
        );
    }

    @Override
    public CompletableFuture<PrimaryReplica> primaryReplica(int partition) {
        var raftGroupService = raftGroupServiceByPartitionId.get(partition);
        assert raftGroupService != null : "No such partition " + partition + " in table " + tableName;

        return placementDriver
                .awaitPrimaryReplica(raftGroupService.groupId(), clock.now(), AWAIT_PRIMARY_REPLICA_TIMEOUT, SECONDS)
                .thenApply(primaryReplica -> {
                    ClusterNode node = clusterNodeResolver.apply(primaryReplica.getLeaseholder());
                    return new PrimaryReplica(node, primaryReplica.getStartTime().longValue());
                });
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
     * Gets a batch result.
     *
     * @param rowBatches Row batches.
     * @return Future of collecting results.
     */
    public static CompletableFuture<List<BinaryRow>> collectRejectedRowsResponsesWithRestoreOrder(Collection<RowBatch> rowBatches) {
        return collectMultiRowsResponsesWithRestoreOrder(
                rowBatches,
                batch -> {
                    List<BinaryRow> result = new ArrayList<>();
                    List<BinaryRow> response = (List<BinaryRow>) batch.getCompletedResult();

                    assert batch.requestedRows.size() == response.size() :
                            "Replication response does not fit to request [requestRows=" + batch.requestedRows.size()
                                    + "responseRows=" + response.size() + ']';

                    for (int i = 0; i < response.size(); i++) {
                        result.add(response.get(i) != null ? null : batch.requestedRows.get(i));
                    }

                    return result;
                },
                true
        );
    }

    /**
     * Gets a batch result.
     *
     * @param rowBatches Row batches.
     * @return Future of collecting results.
     */
    static CompletableFuture<List<BinaryRow>> collectMultiRowsResponsesWithRestoreOrder(Collection<RowBatch> rowBatches) {
        return collectMultiRowsResponsesWithRestoreOrder(
                rowBatches,
                batch -> (Collection<BinaryRow>) batch.getCompletedResult(),
                false
        );
    }

    /**
     * Gathers the result of batch processing into a single resulting collection of rows, restoring order as in the requested collection of
     * rows.
     *
     * @param rowBatches Row batches.
     * @param bathResultMapper Map a batch to the result collection of binary rows.
     * @param skipNull True to skip the null in result collection, false otherwise.
     * @return Future of collecting results.
     */
    private static CompletableFuture<List<BinaryRow>> collectMultiRowsResponsesWithRestoreOrder(
            Collection<RowBatch> rowBatches,
            Function<RowBatch, Collection<BinaryRow>> bathResultMapper,
            boolean skipNull
    ) {
        return allResultFutures(rowBatches)
                .thenApply(response -> {
                    var result = new BinaryRow[RowBatch.getTotalRequestedRowSize(rowBatches)];

                    for (RowBatch rowBatch : rowBatches) {
                        Collection<BinaryRow> batchResult = bathResultMapper.apply(rowBatch);

                        assert batchResult != null;

                        assert batchResult.size() == rowBatch.requestedRows.size() :
                                "batchResult=" + batchResult.size() + ", requestedRows=" + rowBatch.requestedRows.size();

                        int i = 0;

                        for (BinaryRow resultRow : batchResult) {
                            result[rowBatch.getOriginalRowIndex(i++)] = resultRow;
                        }
                    }

                    ArrayList<BinaryRow> resultToReturn = new ArrayList<>();

                    for (BinaryRow row : result) {
                        if (!skipNull || row != null) {
                            resultToReturn.add(row);
                        }
                    }

                    return resultToReturn;
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
        TablePartitionId tablePartitionId = new TablePartitionId(tableId, partId);
        tx.assignCommitPartition(tablePartitionId);

        HybridTimestamp now = clock.now();

        CompletableFuture<ReplicaMeta> primaryReplicaFuture = placementDriver.awaitPrimaryReplica(
                tablePartitionId,
                now,
                AWAIT_PRIMARY_REPLICA_TIMEOUT,
                SECONDS
        );

        return primaryReplicaFuture.handle((primaryReplica, e) -> {
            if (e != null) {
                throw withCause(TransactionException::new, REPLICA_UNAVAILABLE_ERR, "Failed to get the primary replica"
                        + " [tablePartitionId=" + tablePartitionId + ", awaitTimestamp=" + now + ']', e);
            }

            ClusterNode node = clusterNodeResolver.apply(primaryReplica.getLeaseholder());

            if (node == null) {
                throw new TransactionException(REPLICA_UNAVAILABLE_ERR, "Failed to resolve the primary replica node [consistentId="
                        + primaryReplica.getLeaseholder() + ']');
            }

            TablePartitionId partGroupId = new TablePartitionId(tableId, partId);

            return tx.enlist(partGroupId, new IgniteBiTuple<>(node, primaryReplica.getStartTime().longValue()));
        });
    }

    /**
     * Partition scan publisher.
     */
    private static class PartitionScanPublisher implements Publisher<BinaryRow> {
        /** The closure enlists a partition, that is scanned, to the transaction context and retrieves a batch rows. */
        private final BiFunction<Long, Integer, CompletableFuture<Collection<BinaryRow>>> retrieveBatch;

        /** The closure will be invoked before the cursor closed. */
        BiFunction<Boolean, CompletableFuture<Void>, CompletableFuture<Void>> onClose;

        /** True when the publisher has a subscriber, false otherwise. */
        private final AtomicBoolean subscribed;

        /**
         * The constructor.
         *
         * @param retrieveBatch Closure that gets a new batch from the remote replica.
         * @param onClose The closure will be applied when {@link Subscription#cancel} is invoked directly or the cursor is
         *         finished.
         */
        PartitionScanPublisher(
                BiFunction<Long, Integer, CompletableFuture<Collection<BinaryRow>>> retrieveBatch,
                BiFunction<Boolean, CompletableFuture<Void>, CompletableFuture<Void>> onClose
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
                    cancel(null, true);

                    subscriber.onError(new IllegalArgumentException(
                            format("Invalid requested amount of items [requested={}, minValue=1].", n)));
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
                cancel(null, true); // Explicit cancel.
            }

            /**
             * After the method is called, a subscriber won't be received updates from the publisher.
             *
             * @param t An exception which was thrown when entries were retrieving from the cursor.
             * @param commit {@code True} to commit.
             */
            private void cancel(Throwable t, boolean commit) {
                if (!canceled.compareAndSet(false, true)) {
                    return;
                }

                onClose.apply(commit, t == null ? completedFuture(null) : failedFuture(t)).handle((ignore, th) -> {
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
                    assert binaryRows != null;
                    assert binaryRows.size() <= n : "Rows more then requested " + binaryRows.size() + " " + n;

                    binaryRows.forEach(subscriber::onNext);

                    if (binaryRows.size() < n) {
                        cancel(null, false);
                    } else {
                        long remaining = requestedItemsCnt.addAndGet(Math.negateExact(binaryRows.size()));

                        if (remaining > 0) {
                            scanBatch((int) Math.min(remaining, INTERNAL_BATCH_SIZE));
                        }
                    }
                }).exceptionally(t -> {
                    cancel(t, false);

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
        TablePartitionId tablePartitionId = new TablePartitionId(tableId, partId);

        return placementDriver.awaitPrimaryReplica(tablePartitionId, clock.now(), AWAIT_PRIMARY_REPLICA_TIMEOUT, SECONDS)
                .handle((res, e) -> {
                    if (e != null) {
                        throw withCause(TransactionException::new, REPLICA_UNAVAILABLE_ERR, e);
                    } else {
                        if (res == null) {
                            throw withCause(TransactionException::new, REPLICA_UNAVAILABLE_ERR, e);
                        } else {
                            return clusterNodeResolver.apply(res.getLeaseholder());
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
            Collection<? extends BinaryRow> keyRows0,
            InternalTransaction txo,
            ReplicationGroupId groupId,
            Long term,
            boolean full
    ) {
        assert serializeTablePartitionId(txo.commitPartition()) != null;

        return readWriteMultiRowReplicaRequest(RequestType.RW_UPSERT_ALL, keyRows0, txo, groupId, term, full);
    }

    @Override
    public Function<String, ClusterNode> getClusterNodeResolver() {
        return clusterNodeResolver;
    }
}
