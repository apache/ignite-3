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
import static java.util.function.Function.identity;
import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;
import static org.apache.ignite.internal.table.distributed.replicator.action.RequestType.RW_DELETE_ALL;
import static org.apache.ignite.internal.table.distributed.replicator.action.RequestType.RW_GET;
import static org.apache.ignite.internal.table.distributed.replicator.action.RequestType.RW_GET_ALL;
import static org.apache.ignite.internal.table.distributed.storage.RowBatch.allResultFutures;
import static org.apache.ignite.internal.tracing.TracingManager.asyncSpan;
import static org.apache.ignite.internal.tracing.TracingManager.span;
import static org.apache.ignite.internal.util.CompletableFutures.completedOrFailedFuture;
import static org.apache.ignite.internal.util.CompletableFutures.emptyListCompletedFuture;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.apache.ignite.internal.util.ExceptionUtils.matchAny;
import static org.apache.ignite.internal.util.ExceptionUtils.sneakyThrow;
import static org.apache.ignite.internal.util.ExceptionUtils.unwrapCause;
import static org.apache.ignite.internal.util.ExceptionUtils.withCause;
import static org.apache.ignite.internal.util.FastTimestamps.coarseCurrentTimeMillis;
import static org.apache.ignite.lang.ErrorGroups.Common.INTERNAL_ERR;
import static org.apache.ignite.lang.ErrorGroups.Replicator.REPLICA_MISS_ERR;
import static org.apache.ignite.lang.ErrorGroups.Replicator.REPLICA_UNAVAILABLE_ERR;
import static org.apache.ignite.lang.ErrorGroups.Transactions.ACQUIRE_LOCK_ERR;
import static org.apache.ignite.lang.ErrorGroups.Transactions.TX_ALREADY_FINISHED_ERR;
import static org.apache.ignite.lang.ErrorGroups.Transactions.TX_FAILED_READ_WRITE_OPERATION_ERR;

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Flow.Publisher;
import java.util.concurrent.Flow.Subscriber;
import java.util.concurrent.Flow.Subscription;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiFunction;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.ignite.internal.binarytuple.BinaryTupleReader;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.lang.IgniteBiTuple;
import org.apache.ignite.internal.lang.IgnitePentaFunction;
import org.apache.ignite.internal.lang.IgniteTriFunction;
import org.apache.ignite.internal.placementdriver.PlacementDriver;
import org.apache.ignite.internal.placementdriver.ReplicaMeta;
import org.apache.ignite.internal.replicator.ReplicaService;
import org.apache.ignite.internal.replicator.ReplicationGroupId;
import org.apache.ignite.internal.replicator.TablePartitionId;
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
import org.apache.ignite.internal.table.distributed.replication.request.ScanCloseReplicaRequest;
import org.apache.ignite.internal.table.distributed.replication.request.SingleRowPkReplicaRequest;
import org.apache.ignite.internal.table.distributed.replication.request.SingleRowReplicaRequest;
import org.apache.ignite.internal.table.distributed.replication.request.SwapRowReplicaRequest;
import org.apache.ignite.internal.table.distributed.replicator.action.RequestType;
import org.apache.ignite.internal.tracing.TraceSpan;
import org.apache.ignite.internal.tx.HybridTimestampTracker;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.internal.tx.TxManager;
import org.apache.ignite.internal.tx.impl.TransactionInflights;
import org.apache.ignite.internal.tx.storage.state.TxStateTableStorage;
import org.apache.ignite.internal.util.CollectionUtils;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.util.PendingComparableValuesTracker;
import org.apache.ignite.internal.utils.PrimaryReplica;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.ClusterNodeResolver;
import org.apache.ignite.tx.TransactionException;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

/**
 * Storage of table rows.
 */
public class InternalTableImpl implements InternalTable {
    /** Cursor id generator. */
    private static final AtomicLong CURSOR_ID_GENERATOR = new AtomicLong();

    /** Primary replica await timeout. */
    public static final int AWAIT_PRIMARY_REPLICA_TIMEOUT = 30;

    /** Default no-op implementation to avoid unnecessary allocations. */
    private static final ReadWriteInflightBatchRequestTracker READ_WRITE_INFLIGHT_BATCH_REQUEST_TRACKER =
            new ReadWriteInflightBatchRequestTracker();

    /** Partitions. */
    private final int partitions;

    private final Supplier<ScheduledExecutorService> streamerFlushExecutor;

    /** Table name. */
    private volatile String tableName;

    /** Table identifier. */
    private final int tableId;

    /** Resolver that resolves a node consistent ID to cluster node. */
    private final ClusterNodeResolver clusterNodeResolver;

    /** Transactional manager. */
    protected final TxManager txManager;

    private final TransactionInflights transactionInflights;

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

    /** Table raft service. */
    private final TableRaftServiceImpl tableRaftService;

    /** Implicit transaction timeout. */
    private final long implicitTransactionTimeout;

    /** Attempts to take lock. */
    private final int attemptsObtainLock;

    /**
     * Constructor.
     *
     * @param tableName Table name.
     * @param tableId Table id.
     * @param partitions Partitions.
     * @param clusterNodeResolver Cluster node resolver.
     * @param txManager Transaction manager.
     * @param tableStorage Table storage.
     * @param txStateStorage Transaction state storage.
     * @param replicaSvc Replica service.
     * @param clock A hybrid logical clock.
     * @param placementDriver Placement driver.
     * @param tableRaftService Table raft service.
     * @param transactionInflights Transaction inflights.
     * @param implicitTransactionTimeout Implicit transaction timeout.
     * @param attemptsObtainLock Attempts to take lock.
     */
    public InternalTableImpl(
            String tableName,
            int tableId,
            int partitions,
            ClusterNodeResolver clusterNodeResolver,
            TxManager txManager,
            MvTableStorage tableStorage,
            TxStateTableStorage txStateStorage,
            ReplicaService replicaSvc,
            HybridClock clock,
            HybridTimestampTracker observableTimestampTracker,
            PlacementDriver placementDriver,
            TableRaftServiceImpl tableRaftService,
            TransactionInflights transactionInflights,
            long implicitTransactionTimeout,
            int attemptsObtainLock,
            Supplier<ScheduledExecutorService> streamerFlushExecutor
    ) {
        this.tableName = tableName;
        this.tableId = tableId;
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
        this.tableRaftService = tableRaftService;
        this.transactionInflights = transactionInflights;
        this.implicitTransactionTimeout = implicitTransactionTimeout;
        this.attemptsObtainLock = attemptsObtainLock;
        this.streamerFlushExecutor = streamerFlushExecutor;
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

    @Override
    public void name(String newName) {
        tableRaftService.name(newName);

        this.tableName = newName;
    }

    @Override
    public TableRaftServiceImpl tableRaftService() {
        return tableRaftService;
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
        return enlistInTx(row, tx, fac, noWriteChecker, null);
    }

    /**
     * Enlists a single row into a transaction.
     *
     * @param row The row.
     * @param tx The transaction, not null if explicit.
     * @param fac Replica requests factory.
     * @param noWriteChecker Used to handle operations producing no updates.
     * @param txStartTs Transaction start time or {@code null}. This parameter is used only for retry.
     * @return The future.
     */
    private <R> CompletableFuture<R> enlistInTx(
            BinaryRowEx row,
            @Nullable InternalTransaction tx,
            IgniteTriFunction<InternalTransaction, ReplicationGroupId, Long, ReplicaRequest> fac,
            BiPredicate<R, ReplicaRequest> noWriteChecker,
            @Nullable Long txStartTs
    ) {
        return span("InternalTableImpl.enlistInTx", (span) -> {
            // Check whether proposed tx is read-only. Complete future exceptionally if true.
            // Attempting to enlist a read-only in a read-write transaction does not corrupt the transaction itself,
            // thus read-write transaction won't be rolled back automatically - it's up to the user or outer engine.
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

            IgniteBiTuple<ClusterNode, Long> primaryReplicaAndConsistencyToken = actualTx.enlistedNodeAndConsistencyToken(partGroupId);

            CompletableFuture<R> fut;

            if (primaryReplicaAndConsistencyToken != null) {
                assert !implicit;

                fut = trackingInvoke(actualTx, partId,
                        enlistmentConsistencyToken -> fac.apply(actualTx, partGroupId, enlistmentConsistencyToken),
                        false,
                        primaryReplicaAndConsistencyToken,
                        noWriteChecker, attemptsObtainLock
                );
            } else {
                fut = enlistAndInvoke(actualTx, partId,
                        enlistmentConsistencyToken -> fac.apply(actualTx, partGroupId, enlistmentConsistencyToken), implicit,
                        noWriteChecker
                );
            }

            return postEnlist(fut, false, actualTx, implicit).handle((r, e) -> {
                if (e != null) {
                    if (implicit) {
                        long ts = (txStartTs == null) ? actualTx.startTimestamp().getPhysical() : txStartTs;

                        if (exceptionAllowsImplicitTxRetry(e) && coarseCurrentTimeMillis() - ts < implicitTransactionTimeout) {
                            return enlistInTx(row, null, fac, noWriteChecker, ts);
                        }
                    }

                    sneakyThrow(e);
                }

                return completedFuture(r);
            }).thenCompose(identity());
        });
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
        return enlistInTx(keyRows, tx, fac, reducer, noOpChecker, null);
    }

    /**
     * Enlists a single row into a transaction.
     *
     * @param keyRows Rows.
     * @param tx The transaction.
     * @param fac Replica requests factory.
     * @param reducer Transform reducer.
     * @param noOpChecker Used to handle no-op operations (producing no updates).
     * @param txStartTs Transaction start time or {@code null}. This parameter is used only for retry.
     * @return The future.
     */
    private <T> CompletableFuture<T> enlistInTx(
            Collection<BinaryRowEx> keyRows,
            @Nullable InternalTransaction tx,
            IgnitePentaFunction<
                    Collection<? extends BinaryRow>, InternalTransaction, ReplicationGroupId, Long, Boolean, ReplicaRequest
                    > fac,
            Function<Collection<RowBatch>, CompletableFuture<T>> reducer,
            BiPredicate<T, ReplicaRequest> noOpChecker,
            @Nullable Long txStartTs
    ) {
        return span("InternalTableImpl.enlistInTx", (span) -> {
            // Check whether proposed tx is read-only. Complete future exceptionally if true.
            // Attempting to enlist a read-only in a read-write transaction does not corrupt the transaction itself,
            // thus read-write transaction won't be rolled back automatically - it's up to the user or outer engine.
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

                IgniteBiTuple<ClusterNode, Long> primaryReplicaAndConsistencyToken = actualTx.enlistedNodeAndConsistencyToken(partGroupId);

                CompletableFuture<T> fut;

                if (primaryReplicaAndConsistencyToken != null) {
                    assert !implicit;

                    fut = trackingInvoke(actualTx, partitionId, enlistmentConsistencyToken ->
                                    fac.apply(rowBatch.requestedRows, actualTx, partGroupId, enlistmentConsistencyToken, false),
                            false,
                            primaryReplicaAndConsistencyToken, noOpChecker, attemptsObtainLock);
                } else {
                    fut = enlistAndInvoke(
                            actualTx,
                            partitionId,
                            enlistmentConsistencyToken ->
                                    fac.apply(rowBatch.requestedRows, actualTx, partGroupId, enlistmentConsistencyToken, full),
                            full,
                            noOpChecker

                    );
                }

                rowBatch.resultFuture = fut;
            }

            CompletableFuture<T> fut = reducer.apply(rowBatchByPartitionId.values());

            return postEnlist(fut, implicit && !singlePart, actualTx, full).handle((r, e) -> {
                if (e != null) {
                    if (implicit) {
                        long ts = (txStartTs == null) ? actualTx.startTimestamp().getPhysical() : txStartTs;

                        if (exceptionAllowsImplicitTxRetry(e) && coarseCurrentTimeMillis() - ts < implicitTransactionTimeout) {
                            return enlistInTx(keyRows, null, fac, reducer, noOpChecker, ts);
                        }
                    }

                    sneakyThrow(e);
                }

                return completedFuture(r);
            }).thenCompose(identity());
        });
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
        return span("InternalTableImpl.enlistCursorInTx", (span) -> {
            TablePartitionId partGroupId = new TablePartitionId(tableId, partId);

            IgniteBiTuple<ClusterNode, Long> primaryReplicaAndConsistencyToken = tx.enlistedNodeAndConsistencyToken(partGroupId);

            CompletableFuture<Collection<BinaryRow>> fut;

            Function<Long, ReplicaRequest> mapFunc =
                    (enlistmentConsistencyToken) -> tableMessagesFactory.readWriteScanRetrieveBatchReplicaRequest()
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
                            .batchSize(batchSize).enlistmentConsistencyToken(enlistmentConsistencyToken)
                            .commitPartitionId(serializeTablePartitionId(tx.commitPartition()))
                            .coordinatorId(tx.coordinatorId())
                            .build();

            if (primaryReplicaAndConsistencyToken != null) {
                fut = replicaSvc.invoke(primaryReplicaAndConsistencyToken.get1(), mapFunc.apply(primaryReplicaAndConsistencyToken.get2()));
            } else {
                fut = enlistAndInvoke(tx, partId, mapFunc, false, null);
            }

            return postEnlist(fut, false, tx, false);
        });
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
     * Enlists a partition and invokes the replica.
     *
     * @param tx Internal transaction.
     * @param partId Partition number.
     * @param mapFunc Function to create replica request with new enlistment consistency token.
     * @param full {@code True} if is a full transaction.
     * @param noWriteChecker Used to handle operations producing no updates.
     * @return The future.
     */
    private <R> CompletableFuture<R> enlistAndInvoke(
            InternalTransaction tx,
            int partId,
            Function<Long, ReplicaRequest> mapFunc,
            boolean full,
            @Nullable BiPredicate<R, ReplicaRequest> noWriteChecker
    ) {
        return span("InternalTableImpl.enlistWithRetry", (span) -> {
            span.addAttribute("partId", () -> Objects.toString(partId));
            span.addAttribute("full", () -> Objects.toString(full));

            return  enlist(partId, tx)
                    .thenCompose(primaryReplicaAndConsistencyToken ->
                        trackingInvoke(tx, partId, mapFunc, full, primaryReplicaAndConsistencyToken, noWriteChecker,
                            attemptsObtainLock));
        });
    }

    /**
     * Invoke replica with additional tracking for writes.
     *
     * @param tx The transaction.
     * @param partId Partition id.
     * @param mapFunc Request factory.
     * @param full {@code True} for a full transaction.
     * @param primaryReplicaAndConsistencyToken Replica and enlistment consistency token.
     * @param noWriteChecker Used to handle operations producing no updates.
     * @param retryOnLockConflict {@code True} to retry on lock conflics.
     * @return The future.
     */
    private <R> CompletableFuture<R> trackingInvoke(
            InternalTransaction tx,
            int partId,
            Function<Long, ReplicaRequest> mapFunc,
            boolean full,
            IgniteBiTuple<ClusterNode, Long> primaryReplicaAndConsistencyToken,
            @Nullable BiPredicate<R, ReplicaRequest> noWriteChecker,
            int retryOnLockConflict
    ) {
        assert !tx.isReadOnly() : format("Tracking invoke is available only for read-write transactions [tx={}].", tx);

        return span("InternalTableImpl.trackingInvoke", (span) -> {
            ReplicaRequest request = mapFunc.apply(primaryReplicaAndConsistencyToken.get2());

            boolean write = request instanceof SingleRowReplicaRequest && ((SingleRowReplicaRequest) request).requestType() != RW_GET
                    || request instanceof MultipleRowReplicaRequest && ((MultipleRowReplicaRequest) request).requestType() != RW_GET_ALL
                    || request instanceof SingleRowPkReplicaRequest && ((SingleRowPkReplicaRequest) request).requestType() != RW_GET
                    || request instanceof MultipleRowPkReplicaRequest && ((MultipleRowPkReplicaRequest) request).requestType() != RW_GET_ALL
                    || request instanceof SwapRowReplicaRequest;

            if (full) { // Full transaction retries are handled in postEnlist.
                return replicaSvc.invoke(primaryReplicaAndConsistencyToken.get1(), request);
            } else {
                if (write) { // Track only write requests from explicit transactions.
                    if (!transactionInflights.addInflight(tx.id(), false)) {
                        return failedFuture(
                                new TransactionException(TX_ALREADY_FINISHED_ERR, format(
                                        "Transaction is already finished [tableName={}, partId={}, txState={}].",
                                        tableName,
                                        partId,
                                        tx.state()
                                )));
                    }

                    return replicaSvc.<R>invoke(primaryReplicaAndConsistencyToken.get1(), request).thenApply(res -> {
                        assert noWriteChecker != null;

                        // Remove inflight if no replication was scheduled, otherwise inflight will be removed by delayed response.
                        if (noWriteChecker.test(res, request)) {
                            transactionInflights.removeInflight(tx.id());
                        }

                        return res;
                    }).handle((r, e) -> {
                        if (e != null) {
                            if (retryOnLockConflict > 0 && matchAny(unwrapCause(e), ACQUIRE_LOCK_ERR)) {
                                transactionInflights.removeInflight(tx.id()); // Will be retried.

                                return trackingInvoke(
                                        tx,
                                        partId,
                                        ignored -> request,
                                        false,
                                        primaryReplicaAndConsistencyToken,
                                        noWriteChecker,
                                        retryOnLockConflict - 1
                                );
                            }

                            sneakyThrow(e);
                        }

                        return completedFuture(r);
                    }).thenCompose(identity());
                } else { // Explicit reads should be retried too.
                    return replicaSvc.<R>invoke(primaryReplicaAndConsistencyToken.get1(), request).handle((r, e) -> {
                        if (e != null) {
                            if (retryOnLockConflict > 0 && matchAny(unwrapCause(e), ACQUIRE_LOCK_ERR)) {
                                return trackingInvoke(
                                        tx,
                                        partId,
                                        ignored -> request,
                                        false,
                                        primaryReplicaAndConsistencyToken,
                                        noWriteChecker,
                                        retryOnLockConflict - 1
                                );
                            }

                            sneakyThrow(e);
                        }

                        return completedFuture(r);
                    }).thenCompose(identity());
                }
            }
        });
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
        return span("InternalTableImpl.postEnlist", (span) -> {
            return fut.handle((BiFunction<T, Throwable, CompletableFuture<T>>) (r, e) -> {
                if (full) { // Full txn is already finished remotely. Just update local state.
                    txManager.finishFull(observableTimestampTracker, tx0.id(), e == null);
                    tx0.parentSpan().end();

                    return e != null ? failedFuture(e) : completedFuture(r);
                }

                if (e != null) {
                    return tx0.rollbackAsync().handle((ignored, err) -> {
                        if (err != null) {
                            e.addSuppressed(err);
                        }
                        sneakyThrow(e);
                        return null;
                    }); // Preserve failed state.
                } else {
                    if (autoCommit) {
                        return tx0.commitAsync().thenApply(ignored -> r);
                    } else {
                        return completedFuture(r);
                    }
                }
            }).thenCompose(identity());
        });
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
        try (TraceSpan ignored = asyncSpan("tx operation")) {
            InternalTransaction tx = txManager.begin(observableTimestampTracker, true);

            int partId = partitionId(row);

            TablePartitionId tablePartitionId = new TablePartitionId(tableId, partId);

            CompletableFuture<ReplicaMeta> primaryReplicaFuture = placementDriver.awaitPrimaryReplica(
                    tablePartitionId,
                    tx.startTimestamp(),
                    AWAIT_PRIMARY_REPLICA_TIMEOUT,
                    SECONDS
            );

            CompletableFuture<R> fut = primaryReplicaFuture.thenCompose(primaryReplica -> {
                try {
                    ClusterNode node = getClusterNode(primaryReplica.getLeaseholder());

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
                SECONDS
        );

        CompletableFuture<R> fut = primaryReplicaFuture.thenCompose(primaryReplica -> {
            try {
                ClusterNode node = getClusterNode(primaryReplica.getLeaseholder());

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
        return fut.handle((BiFunction<R, Throwable, CompletableFuture<R>>) (r, e) ->
                span("postEvaluate", (Function<TraceSpan, ? extends CompletableFuture<R>>) (span) -> {
                    if (e != null) {
                        return tx.finish(false, clock.now())
                                .handle((ignored, err) -> {
                                    if (err != null) {
                                        e.addSuppressed(err);
                                    }

                                    sneakyThrow(e);
                                    return null;
                                }); // Preserve failed state.
                    }

                    return tx.finish(true, clock.now()).thenApply(ignored -> r);
                })
        ).thenCompose(identity());
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<BinaryRow> get(BinaryRowEx keyRow, @Nullable InternalTransaction tx) {
        return span("InternalTableImpl.get", (span) -> {
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
                    (txo, groupId, enlistmentConsistencyToken) -> tableMessagesFactory.readWriteSingleRowPkReplicaRequest()
                            .groupId(groupId)
                            .schemaVersion(keyRow.schemaVersion())
                            .primaryKey(keyRow.tupleSlice())
                            .commitPartitionId(serializeTablePartitionId(txo.commitPartition()))
                            .transactionId(txo.id())
                            .enlistmentConsistencyToken(enlistmentConsistencyToken)
                            .requestType(RW_GET)
                            .timestampLong(clock.nowLong())
                            .full(false)
                            .coordinatorId(txo.coordinatorId())
                            .build(),
                    (res, req) -> false

            );
        });
    }

    @Override
    public CompletableFuture<BinaryRow> get(
            BinaryRowEx keyRow,
            HybridTimestamp readTimestamp,
            ClusterNode recipientNode
    ) {
        return span("InternalTableImpl.get", (span) -> {
            int partId = partitionId(keyRow);
            TablePartitionId tablePartitionId = new TablePartitionId(tableId, partId);

            return replicaSvc.invoke(recipientNode, tableMessagesFactory.readOnlySingleRowPkReplicaRequest()
                    .groupId(tablePartitionId)
                    .schemaVersion(keyRow.schemaVersion())
                    .primaryKey(keyRow.tupleSlice())
                    .requestType(RequestType.RO_GET)
                    .readTimestampLong(readTimestamp.longValue())
                    .build()
            );
        });
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
        return span("InternalTableImpl.getAll", (span) -> {
            if (CollectionUtils.nullOrEmpty(keyRows)) {
                return emptyListCompletedFuture();
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
                    (keyRows0, txo, groupId, enlistmentConsistencyToken, full) ->
                        readWriteMultiRowPkReplicaRequest(RW_GET_ALL, keyRows0, txo, groupId, enlistmentConsistencyToken, full),
                    InternalTableImpl::collectMultiRowsResponsesWithRestoreOrder,
                    (res, req) -> false
            );
        });
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<List<BinaryRow>> getAll(
            Collection<BinaryRowEx> keyRows,
            HybridTimestamp readTimestamp,
            ClusterNode recipientNode
    ) {
        return span("InternalTableImpl.getAll", (span) -> {
            Int2ObjectMap<RowBatch> rowBatchByPartitionId = toRowBatchByPartitionId(keyRows);

            for (Int2ObjectMap.Entry<RowBatch> partitionRowBatch : rowBatchByPartitionId.int2ObjectEntrySet()) {
                TablePartitionId tablePartitionId = new TablePartitionId(tableId, partitionRowBatch.getIntKey());

                ReadOnlyMultiRowPkReplicaRequest request = tableMessagesFactory.readOnlyMultiRowPkReplicaRequest()
                        .groupId(tablePartitionId)
                        .schemaVersion(partitionRowBatch.getValue().requestedRows.get(0).schemaVersion())
                        .primaryKeys(serializeBinaryTuples(partitionRowBatch.getValue().requestedRows))
                        .requestType(RequestType.RO_GET_ALL)
                        .readTimestampLong(readTimestamp.longValue())
                        .build();

                partitionRowBatch.getValue().resultFuture = replicaSvc.invoke(recipientNode, request);
            }

            return collectMultiRowsResponsesWithRestoreOrder(rowBatchByPartitionId.values());
        });
    }

    private ReadWriteMultiRowPkReplicaRequest readWriteMultiRowPkReplicaRequest(
            RequestType requestType,
            Collection<? extends BinaryRow> rows,
            InternalTransaction tx,
            ReplicationGroupId groupId,
            Long enlistmentConsistencyToken,
            boolean full
    ) {
        assert allSchemaVersionsSame(rows) : "Different schema versions encountered: " + uniqueSchemaVersions(rows);

        return tableMessagesFactory.readWriteMultiRowPkReplicaRequest()
                .groupId(groupId)
                .commitPartitionId(serializeTablePartitionId(tx.commitPartition()))
                .schemaVersion(rows.iterator().next().schemaVersion())
                .primaryKeys(serializeBinaryTuples(rows))
                .transactionId(tx.id())
                .enlistmentConsistencyToken(enlistmentConsistencyToken)
                .requestType(requestType)
                .timestampLong(clock.nowLong())
                .full(full)
                .coordinatorId(tx.coordinatorId())
                .build();
    }

    private static boolean allSchemaVersionsSame(Collection<? extends BinaryRow> rows) {
        int schemaVersion = -1;
        boolean first = true;

        for (BinaryRow row : rows) {
            if (row == null) {
                continue;
            }

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
    public CompletableFuture<Void> upsert(BinaryRowEx row, @Nullable InternalTransaction tx) {
        return span("InternalTableImpl.upsert", (span) -> {
            return enlistInTx(
                    row,
                    tx,
                    (txo, groupId, enlistmentConsistencyToken) -> tableMessagesFactory.readWriteSingleRowReplicaRequest()
                            .groupId(groupId)
                            .commitPartitionId(serializeTablePartitionId(txo.commitPartition()))
                            .schemaVersion(row.schemaVersion())
                            .binaryTuple(row.tupleSlice())
                            .transactionId(txo.id())
                            .enlistmentConsistencyToken(enlistmentConsistencyToken)
                            .requestType(RequestType.RW_UPSERT)
                            .timestampLong(clock.nowLong())
                            .full(tx == null)
                            .coordinatorId(txo.coordinatorId())
                            .build(),
                    (res, req) -> false

            );
        });
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Void> upsertAll(Collection<BinaryRowEx> rows, @Nullable InternalTransaction tx) {
        return span("InternalTableImpl.upsertAll", (span) -> {
            return enlistInTx(
                    rows,
                    tx,
                    this::upsertAllInternal,
                    RowBatch::allResultFutures,
                    (res, req) -> false
            );
        });
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Void> updateAll(Collection<BinaryRowEx> rows, @Nullable BitSet deleted, int partition) {
        return updateAllWithRetry(rows, deleted, partition, null);
    }

    /**
     * Update all with retry.
     *
     * @param rows Rows.
     * @param deleted Deleted.
     * @param partition The partition.
     * @param txStartTs Start timestamp.
     * @return The future.
     */
    private CompletableFuture<Void> updateAllWithRetry(
            Collection<BinaryRowEx> rows,
            @Nullable BitSet deleted,
            int partition,
            @Nullable Long txStartTs
    ) {
        return span("InternalTableImpl.upsertAll", (span) -> {
            InternalTransaction tx = txManager.begin(observableTimestampTracker);
            TablePartitionId partGroupId = new TablePartitionId(tableId, partition);

            assert rows.stream().allMatch(row -> partitionId(row) == partition) : "Invalid batch for partition " + partition;

            CompletableFuture<Void> fut = enlistAndInvoke(
                    tx,
                    partition,
                    enlistmentConsistencyToken -> upsertAllInternal(rows, deleted, tx, partGroupId, enlistmentConsistencyToken, true),
                    true,
                    null
            );

            // Will be finished in one RTT.
            return postEnlist(fut, false, tx, true).handle((r, e) -> {
                if (e != null) {
                    long ts = (txStartTs == null) ? tx.startTimestamp().getPhysical() : txStartTs;

                    if (exceptionAllowsImplicitTxRetry(e) && coarseCurrentTimeMillis() - ts < implicitTransactionTimeout) {
                        return updateAllWithRetry(rows, deleted, partition, ts);
                    }

                    sneakyThrow(e);
                }

                return completedFuture(r);
            }).thenCompose(identity());
        });
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<BinaryRow> getAndUpsert(BinaryRowEx row, InternalTransaction tx) {
        return span("InternalTableImpl.getAndUpsert", (span) -> {
            return enlistInTx(
                    row,
                    tx,
                    (txo, groupId, enlistmentConsistencyToken) -> tableMessagesFactory.readWriteSingleRowReplicaRequest()
                            .groupId(groupId)
                            .commitPartitionId(serializeTablePartitionId(txo.commitPartition()))
                            .schemaVersion(row.schemaVersion())
                            .binaryTuple(row.tupleSlice())
                            .transactionId(txo.id())
                            .enlistmentConsistencyToken(enlistmentConsistencyToken)
                            .requestType(RequestType.RW_GET_AND_UPSERT)
                            .timestampLong(clock.nowLong())
                            .full(tx == null)
                        .coordinatorId(txo.coordinatorId())
                            .build(),
                    (res, req) -> false

            );
        });
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Boolean> insert(BinaryRowEx row, InternalTransaction tx) {
        return span("InternalTableImpl.insert", (span) -> {
            return enlistInTx(
                    row,
                    tx,
                    (txo, groupId, enlistmentConsistencyToken) -> tableMessagesFactory.readWriteSingleRowReplicaRequest()
                            .groupId(groupId)
                            .commitPartitionId(serializeTablePartitionId(txo.commitPartition()))
                            .schemaVersion(row.schemaVersion())
                            .binaryTuple(row.tupleSlice())
                            .transactionId(txo.id())
                            .enlistmentConsistencyToken(enlistmentConsistencyToken)
                            .requestType(RequestType.RW_INSERT)
                            .timestampLong(clock.nowLong())
                            .full(tx == null)
                        .coordinatorId(txo.coordinatorId())
                            .build(),
                    (res, req) -> !res

            );
        });
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<List<BinaryRow>> insertAll(Collection<BinaryRowEx> rows, InternalTransaction tx) {
        return span("InternalTableImpl.insertAll", (span) -> {
            return enlistInTx(
                    rows,
                    tx,
                    (keyRows, txo, groupId, enlistmentConsistencyToken, full) ->
                         readWriteMultiRowReplicaRequest(
                                RequestType.RW_INSERT_ALL,
                                keyRows,
                                null,
                                txo,
                                groupId,
                                enlistmentConsistencyToken,
                    full),
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
        });
    }

    private ReadWriteMultiRowReplicaRequest readWriteMultiRowReplicaRequest(
            RequestType requestType,
            Collection<? extends BinaryRow> rows,
            @Nullable BitSet deleted,
            InternalTransaction tx,
            ReplicationGroupId groupId,
            Long enlistmentConsistencyToken,
            boolean full
    ) {
        assert allSchemaVersionsSame(rows) : "Different schema versions encountered: " + uniqueSchemaVersions(rows);

        return tableMessagesFactory.readWriteMultiRowReplicaRequest()
                .groupId(groupId)
                .commitPartitionId(serializeTablePartitionId(tx.commitPartition()))
                .schemaVersion(rows.iterator().next().schemaVersion())
                .binaryTuples(serializeBinaryTuples(rows))
                .deleted(deleted)
                .transactionId(tx.id())
                .enlistmentConsistencyToken(enlistmentConsistencyToken)
                .requestType(requestType)
                .timestampLong(clock.nowLong())
                .full(full)
                .coordinatorId(tx.coordinatorId())
                .build();
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Boolean> replace(BinaryRowEx row, InternalTransaction tx) {
        return span("InternalTableImpl.replace", (span) -> {
            return enlistInTx(
                    row,
                    tx,
                    (txo, groupId, enlistmentConsistencyToken) -> tableMessagesFactory.readWriteSingleRowReplicaRequest()
                            .groupId(groupId)
                            .commitPartitionId(serializeTablePartitionId(txo.commitPartition()))
                            .schemaVersion(row.schemaVersion())
                            .binaryTuple(row.tupleSlice())
                            .transactionId(txo.id())
                            .enlistmentConsistencyToken(enlistmentConsistencyToken)
                            .requestType(RequestType.RW_REPLACE_IF_EXIST)
                            .timestampLong(clock.nowLong())
                            .full(tx == null)
                        .coordinatorId(txo.coordinatorId())
                            .build(),
                    (res, req) -> !res

            );
        });
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Boolean> replace(BinaryRowEx oldRow, BinaryRowEx newRow, InternalTransaction tx) {
        assert oldRow.schemaVersion() == newRow.schemaVersion()
                : "Mismatching schema versions: old " + oldRow.schemaVersion() + ", new " + newRow.schemaVersion();

        return span("InternalTableImpl.replace", (span) -> {
            return enlistInTx(
                    newRow,
                    tx,
                    (txo, groupId, enlistmentConsistencyToken) -> tableMessagesFactory.readWriteSwapRowReplicaRequest()
                            .groupId(groupId)
                            .commitPartitionId(serializeTablePartitionId(txo.commitPartition()))
                            .schemaVersion(oldRow.schemaVersion())
                            .oldBinaryTuple(oldRow.tupleSlice())
                            .newBinaryTuple(newRow.tupleSlice())
                            .transactionId(txo.id())
                            .enlistmentConsistencyToken(enlistmentConsistencyToken)
                            .requestType(RequestType.RW_REPLACE)
                            .timestampLong(clock.nowLong())
                            .full(tx == null)
                        .coordinatorId(txo.coordinatorId())
                            .build(),
                    (res, req) -> !res

            );
        });
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<BinaryRow> getAndReplace(BinaryRowEx row, InternalTransaction tx) {
        return span("InternalTableImpl.getAndReplace", (span) -> {
            return enlistInTx(
                    row,
                    tx,
                    (txo, groupId, enlistmentConsistencyToken) -> tableMessagesFactory.readWriteSingleRowReplicaRequest()
                            .groupId(groupId)
                            .commitPartitionId(serializeTablePartitionId(txo.commitPartition()))
                            .schemaVersion(row.schemaVersion())
                            .binaryTuple(row.tupleSlice())
                            .transactionId(txo.id())
                            .enlistmentConsistencyToken(enlistmentConsistencyToken)
                            .requestType(RequestType.RW_GET_AND_REPLACE)
                            .timestampLong(clock.nowLong())
                            .full(tx == null)
                        .coordinatorId(txo.coordinatorId())
                            .build(),
                    (res, req) -> res == null

            );
        });
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Boolean> delete(BinaryRowEx keyRow, InternalTransaction tx) {
        return span("InternalTableImpl.delete", (span) -> {
            return enlistInTx(
                    keyRow,
                    tx,
                    (txo, groupId, enlistmentConsistencyToken) -> tableMessagesFactory.readWriteSingleRowPkReplicaRequest()
                            .groupId(groupId)
                            .commitPartitionId(serializeTablePartitionId(txo.commitPartition()))
                            .schemaVersion(keyRow.schemaVersion())
                            .primaryKey(keyRow.tupleSlice())
                            .transactionId(txo.id())
                            .enlistmentConsistencyToken(enlistmentConsistencyToken)
                            .requestType(RequestType.RW_DELETE)
                            .timestampLong(clock.nowLong())
                            .full(tx == null)
                        .coordinatorId(txo.coordinatorId())
                            .build(),
                    (res, req) -> !res

            );
        });
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Boolean> deleteExact(BinaryRowEx oldRow, InternalTransaction tx) {
        return span("InternalTableImpl.deleteExact", (span) -> {
            return enlistInTx(
                    oldRow,
                    tx,
                    (txo, groupId, enlistmentConsistencyToken) -> tableMessagesFactory.readWriteSingleRowReplicaRequest()
                            .groupId(groupId)
                            .commitPartitionId(serializeTablePartitionId(txo.commitPartition()))
                            .schemaVersion(oldRow.schemaVersion())
                            .binaryTuple(oldRow.tupleSlice())
                            .transactionId(txo.id())
                            .enlistmentConsistencyToken(enlistmentConsistencyToken)
                            .requestType(RequestType.RW_DELETE_EXACT)
                            .timestampLong(clock.nowLong())
                            .full(tx == null)
                        .coordinatorId(txo.coordinatorId())
                            .build(),
                    (res, req) -> !res

            );
        });
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<BinaryRow> getAndDelete(BinaryRowEx row, InternalTransaction tx) {
        return span("InternalTableImpl.getAndDelete", (span) -> {
            return enlistInTx(
                    row,
                    tx,
                    (txo, groupId, enlistmentConsistencyToken) -> tableMessagesFactory.readWriteSingleRowPkReplicaRequest()
                            .groupId(groupId)
                            .commitPartitionId(serializeTablePartitionId(txo.commitPartition()))
                            .schemaVersion(row.schemaVersion())
                            .primaryKey(row.tupleSlice())
                            .transactionId(txo.id())
                            .enlistmentConsistencyToken(enlistmentConsistencyToken)
                            .requestType(RequestType.RW_GET_AND_DELETE)
                            .timestampLong(clock.nowLong())
                            .full(tx == null)
                        .coordinatorId(txo.coordinatorId())
                            .build(),
                    (res, req) -> res == null

            );
        });
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<List<BinaryRow>> deleteAll(Collection<BinaryRowEx> rows, InternalTransaction tx) {
        return span("InternalTableImpl.deleteAll", (span) -> {
            return enlistInTx(
                    rows,
                    tx,
                    (keyRows0, txo, groupId, enlistmentConsistencyToken, full) ->
                            readWriteMultiRowPkReplicaRequest(RW_DELETE_ALL, keyRows0, txo, groupId, enlistmentConsistencyToken, full),
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
        });
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<List<BinaryRow>> deleteAllExact(
            Collection<BinaryRowEx> rows,
            InternalTransaction tx
    ) {
        return span("InternalTableImpl.deleteAllExact", (span) -> {
            return enlistInTx(
                    rows,
                    tx,
                    (keyRows0, txo, groupId, enlistmentConsistencyToken, full) ->
                         readWriteMultiRowReplicaRequest(
                                RequestType.RW_DELETE_EXACT_ALL,
                                keyRows0,
                                null,
                                txo,
                                groupId,
                                enlistmentConsistencyToken,
                                full
                    ),
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
        });
    }

    @Override
    public Publisher<BinaryRow> lookup(
            int partId,
            UUID txId,
            HybridTimestamp readTimestamp,
            ClusterNode recipientNode,
            int indexId,
            BinaryTuple key,
            @Nullable BitSet columnsToInclude,
            String txCoordinatorId
    ) {
        return readOnlyScan(partId, txId, readTimestamp, recipientNode, indexId, key, null, null, 0, columnsToInclude, txCoordinatorId);
    }

    @Override
    public Publisher<BinaryRow> lookup(
            int partId,
            UUID txId,
            TablePartitionId commitPartition,
            String coordinatorId,
            PrimaryReplica recipient,
            int indexId,
            BinaryTuple key,
            @Nullable BitSet columnsToInclude
    ) {
        return readWriteScan(
                partId,
                txId,
                commitPartition,
                coordinatorId,
                recipient,
                indexId,
                key,
                null,
                null,
                0,
                columnsToInclude
        );
    }

    @Override
    public Publisher<BinaryRow> scan(
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
    ) {
        return readOnlyScan(
                partId,
                txId,
                readTimestamp,
                recipientNode,
                indexId,
                null,
                lowerBound,
                upperBound,
                flags,
                columnsToInclude,
                txCoordinatorId
        );
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
        return readWriteScan(partId, tx, indexId, null, lowerBound, upperBound, flags, columnsToInclude);
    }

    @Override
    public Publisher<BinaryRow> scan(
            int partId,
            UUID txId,
            TablePartitionId commitPartition,
            String coordinatorId,
            PrimaryReplica recipient,
            @Nullable Integer indexId,
            @Nullable BinaryTuplePrefix lowerBound,
            @Nullable BinaryTuplePrefix upperBound,
            int flags,
            @Nullable BitSet columnsToInclude
    ) {
        return readWriteScan(
                partId,
                txId,
                commitPartition,
                coordinatorId,
                recipient,
                indexId,
                null,
                lowerBound,
                upperBound,
                flags,
                columnsToInclude
        );
    }

    private Publisher<BinaryRow> readOnlyScan(
            int partId,
            UUID txId,
            HybridTimestamp readTimestamp,
            ClusterNode recipientNode,
            @Nullable Integer indexId,
            @Nullable BinaryTuple exactKey,
            @Nullable BinaryTuplePrefix lowerBound,
            @Nullable BinaryTuplePrefix upperBound,
            int flags,
            @Nullable BitSet columnsToInclude,
            String txCoordinatorId
    ) {
        validatePartitionIndex(partId);

        TablePartitionId tablePartitionId = new TablePartitionId(tableId, partId);

        return new PartitionScanPublisher(
                (scanId, batchSize) -> {
                    ReadOnlyScanRetrieveBatchReplicaRequest request = tableMessagesFactory.readOnlyScanRetrieveBatchReplicaRequest()
                            .groupId(tablePartitionId)
                            .readTimestampLong(readTimestamp.longValue())
                            .transactionId(txId)
                            .scanId(scanId)
                            .batchSize(batchSize)
                            .indexToUse(indexId)
                            .exactKey(binaryTupleMessage(exactKey))
                            .lowerBoundPrefix(binaryTupleMessage(lowerBound))
                            .upperBoundPrefix(binaryTupleMessage(upperBound))
                            .flags(flags)
                            .columnsToInclude(columnsToInclude)
                            .coordinatorId(txCoordinatorId)
                            .build();

                    return replicaSvc.invoke(recipientNode, request);
                },
                (intentionallyClose, scanId, th) -> completeScan(
                        txId,
                        tablePartitionId,
                        scanId,
                        th,
                        recipientNode,
                        intentionallyClose || th != null
                ),
                new ReadOnlyInflightBatchRequestTracker(transactionInflights, txId)
        );
    }

    private Publisher<BinaryRow> readWriteScan(
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
                (intentionallyClose, scanId, th) -> {
                    CompletableFuture<Void> opFut;

                    if (implicit) {
                        opFut = completedOrFailedFuture(null, th);
                    } else {
                        var replicationGrpId = new TablePartitionId(tableId, partId);

                        opFut = tx.enlistedNodeAndConsistencyToken(replicationGrpId) != null ? completeScan(
                                tx.id(),
                                replicationGrpId,
                                scanId,
                                th,
                                tx.enlistedNodeAndConsistencyToken(replicationGrpId).get1(),
                                intentionallyClose
                        ) : completedOrFailedFuture(null, th);
                    }

                    return postEnlist(opFut, intentionallyClose, actualTx, implicit && !intentionallyClose);
                },
                READ_WRITE_INFLIGHT_BATCH_REQUEST_TRACKER
        );
    }

    private Publisher<BinaryRow> readWriteScan(
            int partId,
            UUID txId,
            TablePartitionId commitPartition,
            String coordinatorId,
            PrimaryReplica recipient,
            @Nullable Integer indexId,
            @Nullable BinaryTuple exactKey,
            @Nullable BinaryTuplePrefix lowerBound,
            @Nullable BinaryTuplePrefix upperBound,
            int flags,
            @Nullable BitSet columnsToInclude
    ) {
        TablePartitionId tablePartitionId = new TablePartitionId(tableId, partId);

        return new PartitionScanPublisher(
                (scanId, batchSize) -> {
                    ReadWriteScanRetrieveBatchReplicaRequest request = tableMessagesFactory.readWriteScanRetrieveBatchReplicaRequest()
                            .groupId(tablePartitionId)
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
                            .enlistmentConsistencyToken(recipient.enlistmentConsistencyToken())
                            .full(false) // Set explicitly.
                            .commitPartitionId(serializeTablePartitionId(commitPartition))
                            .coordinatorId(coordinatorId)
                            .build();

                    return replicaSvc.invoke(recipient.node(), request);
                },
                (intentionallyClose, scanId, th) -> completeScan(txId, tablePartitionId, scanId, th, recipient.node(), intentionallyClose),
                READ_WRITE_INFLIGHT_BATCH_REQUEST_TRACKER
        );
    }

    /**
     * Closes the cursor on server side.
     *
     * @param txId Transaction id.
     * @param replicaGrpId Replication group id.
     * @param scanId Scan id.
     * @param th An exception that may occur in the scan procedure or {@code null} when the procedure passes without an exception.
     * @param recipientNode Server node where the scan was started.
     * @param explicitCloseCursor True when the cursor should be closed explicitly.
     * @return The future.
     */
    private CompletableFuture<Void> completeScan(
            UUID txId,
            ReplicationGroupId replicaGrpId,
            Long scanId,
            Throwable th,
            ClusterNode recipientNode,
            boolean explicitCloseCursor
    ) {
        CompletableFuture<Void> closeFut = nullCompletedFuture();

        if (explicitCloseCursor) {
            ScanCloseReplicaRequest scanCloseReplicaRequest = tableMessagesFactory.scanCloseReplicaRequest()
                    .groupId(replicaGrpId)
                    .transactionId(txId)
                    .scanId(scanId)
                    .build();

            closeFut = replicaSvc.invoke(recipientNode, scanCloseReplicaRequest);
        }

        return closeFut.handle((unused, throwable) -> {
            CompletableFuture<Void> fut = nullCompletedFuture();

            if (th != null) {
                if (throwable != null) {
                    th.addSuppressed(throwable);
                }

                fut = failedFuture(th);
            } else if (throwable != null) {
                fut = failedFuture(throwable);
            }

            return fut;
        }).thenCompose(identity());
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
    @Override
    public TxStateTableStorage txStateStorage() {
        return txStateStorage;
    }

    /** {@inheritDoc} */
    @TestOnly
    @Override
    public int partition(BinaryRowEx keyRow) {
        return partitionId(keyRow);
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
     * Enlists a partition.
     *
     * @param partId Partition id.
     * @param tx The transaction.
     * @return The enlist future (then will a leader become known).
     */
    protected CompletableFuture<IgniteBiTuple<ClusterNode, Long>> enlist(int partId, InternalTransaction tx) {
        return span("InternalTableImpl.enlist", (span) -> {
            TablePartitionId tablePartitionId = new TablePartitionId(tableId, partId);
            tx.assignCommitPartition(tablePartitionId);

            return partitionMeta(tablePartitionId).thenApply(meta -> {
                TablePartitionId partGroupId = new TablePartitionId(tableId, partId);

                return tx.enlist(partGroupId, new IgniteBiTuple<>(
                        getClusterNode(meta.getLeaseholder()),
                        meta.getStartTime().longValue())
                );
            });
        });
    }

    @Override
    public CompletableFuture<ClusterNode> partitionLocation(ReplicationGroupId tablePartitionId) {
        return partitionMeta(tablePartitionId).thenApply(meta -> getClusterNode(meta.getLeaseholder()));
    }

    private CompletableFuture<ReplicaMeta> partitionMeta(ReplicationGroupId tablePartitionId) {
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

            return primaryReplica;
        });
    }

    private ClusterNode getClusterNode(@Nullable String leaserHolder) {
        ClusterNode node = clusterNodeResolver.getByConsistentId(leaserHolder);

        if (node == null) {
            throw new TransactionException(REPLICA_UNAVAILABLE_ERR, "Failed to resolve the primary replica node [consistentId="
                    + leaserHolder + ']');
        }

        return node;
    }

    /**
     * Partition scan publisher.
     */
    private static class PartitionScanPublisher implements Publisher<BinaryRow> {
        /** The closure enlists a partition, that is scanned, to the transaction context and retrieves a batch rows. */
        private final BiFunction<Long, Integer, CompletableFuture<Collection<BinaryRow>>> retrieveBatch;

        /** The closure will be invoked before the cursor closed. */
        IgniteTriFunction<Boolean, Long, Throwable, CompletableFuture<Void>> onClose;

        /** True when the publisher has a subscriber, false otherwise. */
        private final AtomicBoolean subscribed;

        private final InflightBatchRequestTracker inflightBatchRequestTracker;

        /**
         * The constructor.
         *
         * @param retrieveBatch Closure that gets a new batch from the remote replica.
         * @param onClose The closure will be applied when {@link Subscription#cancel} is invoked directly or the cursor is
         *         finished.
         * @param inflightBatchRequestTracker {@link InflightBatchRequestTracker} to track betch requests completion.
         */
        PartitionScanPublisher(
                BiFunction<Long, Integer, CompletableFuture<Collection<BinaryRow>>> retrieveBatch,
                IgniteTriFunction<Boolean, Long, Throwable, CompletableFuture<Void>> onClose,
                InflightBatchRequestTracker inflightBatchRequestTracker
        ) {
            this.retrieveBatch = retrieveBatch;
            this.onClose = onClose;
            this.inflightBatchRequestTracker = inflightBatchRequestTracker;

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

            subscriber.onSubscribe(new PartitionScanSubscription(subscriber));
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
             * @param intentionallyClose True if the subscription is closed for the client side.
             * @return Future to complete.
             */
            private void cancel(Throwable t, boolean intentionallyClose) {
                if (!canceled.compareAndSet(false, true)) {
                    return;
                }

                onClose.apply(intentionallyClose, scanId, t).whenComplete((ignore, th) -> {
                    if (th != null) {
                        subscriber.onError(th);
                    } else {
                        subscriber.onComplete();
                    }
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

                inflightBatchRequestTracker.onRequestBegin();

                retrieveBatch.apply(scanId, n).thenAccept(binaryRows -> {
                    assert binaryRows != null;
                    assert binaryRows.size() <= n : "Rows more then requested " + binaryRows.size() + " " + n;

                    inflightBatchRequestTracker.onRequestEnd();

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
                    inflightBatchRequestTracker.onRequestEnd();

                    cancel(t, false);

                    return null;
                });
            }
        }
    }

    /**
     * Created for {@code PartitionScanSubscription} to track inflight batch requests.
     */
    private interface InflightBatchRequestTracker {
        void onRequestBegin();

        void onRequestEnd();
    }

    /**
     * This is, in fact, no-op {@code InflightBatchRequestTracker} because tracking batch requests for read-write transactions is not
     * needed.
     */
    private static class ReadWriteInflightBatchRequestTracker implements InflightBatchRequestTracker {
        @Override
        public void onRequestBegin() {
            // No-op.
        }

        @Override
        public void onRequestEnd() {
            // No-op.
        }
    }

    private static class ReadOnlyInflightBatchRequestTracker implements InflightBatchRequestTracker {
        private final TransactionInflights transactionInflights;

        private final UUID txId;

        ReadOnlyInflightBatchRequestTracker(TransactionInflights transactionInflights, UUID txId) {
            this.transactionInflights = transactionInflights;
            this.txId = txId;
        }

        @Override
        public void onRequestBegin() {
            // Track read only requests which are able to create cursors.
            if (!transactionInflights.addInflight(txId, true)) {
                throw new TransactionException(TX_ALREADY_FINISHED_ERR, format(
                        "Transaction is already finished () [txId={}, readOnly=true].",
                        txId
                ));
            }
        }

        @Override
        public void onRequestEnd() {
            transactionInflights.removeInflight(txId);
        }
    }

    /** {@inheritDoc} */
    @Override
    public void close() {
        tableRaftService.close();
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
                            return getClusterNode(res.getLeaseholder());
                        }
                    }
                });
    }

    @Override
    public @Nullable PendingComparableValuesTracker<HybridTimestamp, Void> getPartitionSafeTimeTracker(int partitionId) {
        return safeTimeTrackerByPartitionId.get(partitionId);
    }

    @Override
    public @Nullable PendingComparableValuesTracker<Long, Void> getPartitionStorageIndexTracker(int partitionId) {
        return storageIndexTrackerByPartitionId.get(partitionId);
    }

    @Override
    public ScheduledExecutorService streamerFlushExecutor() {
        return streamerFlushExecutor.get();
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
            Long enlistmentConsistencyToken,
            boolean full
    ) {
        assert serializeTablePartitionId(txo.commitPartition()) != null;

        return readWriteMultiRowReplicaRequest(RequestType.RW_UPSERT_ALL, keyRows0, null, txo, groupId, enlistmentConsistencyToken, full);
    }

    private ReplicaRequest upsertAllInternal(
            Collection<? extends BinaryRow> keyRows0,
            @Nullable BitSet deleted,
            InternalTransaction txo,
            ReplicationGroupId groupId,
            Long enlistmentConsistencyToken,
            boolean full
    ) {
        assert serializeTablePartitionId(txo.commitPartition()) != null;

        return readWriteMultiRowReplicaRequest(
                RequestType.RW_UPSERT_ALL, keyRows0, deleted, txo, groupId, enlistmentConsistencyToken, full);
    }

    /**
     * Ensure that the exception allows you to restart a transaction.
     *
     * @param e Exception to check.
     * @return True if retrying is possible, false otherwise.
     */
    private static boolean exceptionAllowsImplicitTxRetry(Throwable e) {
        return matchAny(unwrapCause(e), ACQUIRE_LOCK_ERR, REPLICA_MISS_ERR);
    }
}
