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

import static java.util.concurrent.CompletableFuture.allOf;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.CompletableFuture.failedFuture;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.function.Function.identity;
import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;
import static org.apache.ignite.internal.partition.replicator.network.replication.RequestType.RO_GET;
import static org.apache.ignite.internal.partition.replicator.network.replication.RequestType.RO_GET_ALL;
import static org.apache.ignite.internal.partition.replicator.network.replication.RequestType.RW_DELETE;
import static org.apache.ignite.internal.partition.replicator.network.replication.RequestType.RW_DELETE_ALL;
import static org.apache.ignite.internal.partition.replicator.network.replication.RequestType.RW_DELETE_EXACT;
import static org.apache.ignite.internal.partition.replicator.network.replication.RequestType.RW_DELETE_EXACT_ALL;
import static org.apache.ignite.internal.partition.replicator.network.replication.RequestType.RW_GET;
import static org.apache.ignite.internal.partition.replicator.network.replication.RequestType.RW_GET_ALL;
import static org.apache.ignite.internal.partition.replicator.network.replication.RequestType.RW_GET_AND_DELETE;
import static org.apache.ignite.internal.partition.replicator.network.replication.RequestType.RW_GET_AND_REPLACE;
import static org.apache.ignite.internal.partition.replicator.network.replication.RequestType.RW_GET_AND_UPSERT;
import static org.apache.ignite.internal.partition.replicator.network.replication.RequestType.RW_INSERT;
import static org.apache.ignite.internal.partition.replicator.network.replication.RequestType.RW_INSERT_ALL;
import static org.apache.ignite.internal.partition.replicator.network.replication.RequestType.RW_REPLACE;
import static org.apache.ignite.internal.partition.replicator.network.replication.RequestType.RW_REPLACE_IF_EXIST;
import static org.apache.ignite.internal.partition.replicator.network.replication.RequestType.RW_UPSERT;
import static org.apache.ignite.internal.partition.replicator.network.replication.RequestType.RW_UPSERT_ALL;
import static org.apache.ignite.internal.replicator.message.ReplicaMessageUtils.toZonePartitionIdMessage;
import static org.apache.ignite.internal.table.distributed.TableUtils.isDirectFlowApplicable;
import static org.apache.ignite.internal.table.distributed.storage.RowBatch.allResultFutures;
import static org.apache.ignite.internal.tx.TransactionLogUtils.formatTxInfo;
import static org.apache.ignite.internal.util.CompletableFutures.completedOrFailedFuture;
import static org.apache.ignite.internal.util.CompletableFutures.emptyListCompletedFuture;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.apache.ignite.internal.util.ExceptionUtils.matchAny;
import static org.apache.ignite.internal.util.ExceptionUtils.sneakyThrow;
import static org.apache.ignite.internal.util.ExceptionUtils.unwrapCause;
import static org.apache.ignite.internal.util.ExceptionUtils.withCause;
import static org.apache.ignite.internal.util.FastTimestamps.coarseCurrentTimeMillis;
import static org.apache.ignite.lang.ErrorGroups.Common.INTERNAL_ERR;
import static org.apache.ignite.lang.ErrorGroups.PlacementDriver.PRIMARY_REPLICA_AWAIT_ERR;
import static org.apache.ignite.lang.ErrorGroups.PlacementDriver.PRIMARY_REPLICA_AWAIT_TIMEOUT_ERR;
import static org.apache.ignite.lang.ErrorGroups.Replicator.GROUP_OVERLOADED_ERR;
import static org.apache.ignite.lang.ErrorGroups.Replicator.REPLICA_ABSENT_ERR;
import static org.apache.ignite.lang.ErrorGroups.Replicator.REPLICA_MISS_ERR;
import static org.apache.ignite.lang.ErrorGroups.Replicator.REPLICA_UNAVAILABLE_ERR;
import static org.apache.ignite.lang.ErrorGroups.Transactions.ACQUIRE_LOCK_ERR;
import static org.apache.ignite.lang.ErrorGroups.Transactions.TX_ALREADY_FINISHED_ERR;
import static org.apache.ignite.lang.ErrorGroups.Transactions.TX_ALREADY_FINISHED_WITH_TIMEOUT_ERR;
import static org.apache.ignite.lang.ErrorGroups.Transactions.TX_FAILED_READ_WRITE_OPERATION_ERR;

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Flow.Publisher;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.BiFunction;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.ignite.internal.binarytuple.BinaryTupleReader;
import org.apache.ignite.internal.hlc.ClockService;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.hlc.HybridTimestampTracker;
import org.apache.ignite.internal.lang.IgniteTriFunction;
import org.apache.ignite.internal.network.ClusterNodeResolver;
import org.apache.ignite.internal.network.InternalClusterNode;
import org.apache.ignite.internal.network.UnresolvableConsistentIdException;
import org.apache.ignite.internal.partition.replicator.network.PartitionReplicationMessagesFactory;
import org.apache.ignite.internal.partition.replicator.network.replication.BinaryTupleMessage;
import org.apache.ignite.internal.partition.replicator.network.replication.ReadOnlyMultiRowPkReplicaRequest;
import org.apache.ignite.internal.partition.replicator.network.replication.ReadOnlyScanRetrieveBatchReplicaRequest;
import org.apache.ignite.internal.partition.replicator.network.replication.ReadWriteMultiRowPkReplicaRequest;
import org.apache.ignite.internal.partition.replicator.network.replication.ReadWriteMultiRowReplicaRequest;
import org.apache.ignite.internal.partition.replicator.network.replication.ReadWriteReplicaRequest;
import org.apache.ignite.internal.partition.replicator.network.replication.ReadWriteScanRetrieveBatchReplicaRequest;
import org.apache.ignite.internal.partition.replicator.network.replication.RequestType;
import org.apache.ignite.internal.partition.replicator.network.replication.ScanCloseReplicaRequest;
import org.apache.ignite.internal.placementdriver.PlacementDriver;
import org.apache.ignite.internal.placementdriver.ReplicaMeta;
import org.apache.ignite.internal.replicator.ReplicaService;
import org.apache.ignite.internal.replicator.ZonePartitionId;
import org.apache.ignite.internal.replicator.exception.PrimaryReplicaMissException;
import org.apache.ignite.internal.replicator.exception.ReplicationException;
import org.apache.ignite.internal.replicator.exception.ReplicationTimeoutException;
import org.apache.ignite.internal.replicator.message.ReplicaMessagesFactory;
import org.apache.ignite.internal.replicator.message.ReplicaRequest;
import org.apache.ignite.internal.replicator.message.ReplicationGroupIdMessage;
import org.apache.ignite.internal.replicator.message.TimestampAware;
import org.apache.ignite.internal.replicator.message.ZonePartitionIdMessage;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.BinaryRowEx;
import org.apache.ignite.internal.schema.BinaryTuple;
import org.apache.ignite.internal.schema.BinaryTuplePrefix;
import org.apache.ignite.internal.storage.engine.MvTableStorage;
import org.apache.ignite.internal.table.IndexScanCriteria;
import org.apache.ignite.internal.table.InternalTable;
import org.apache.ignite.internal.table.OperationContext;
import org.apache.ignite.internal.table.StreamerReceiverRunner;
import org.apache.ignite.internal.table.TxContext;
import org.apache.ignite.internal.table.distributed.storage.PartitionScanPublisher.InflightBatchRequestTracker;
import org.apache.ignite.internal.table.metrics.ReadWriteMetricSource;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.internal.tx.PendingTxPartitionEnlistment;
import org.apache.ignite.internal.tx.TransactionIds;
import org.apache.ignite.internal.tx.TxManager;
import org.apache.ignite.internal.tx.TxStateMeta;
import org.apache.ignite.internal.tx.impl.TransactionInflights;
import org.apache.ignite.internal.util.CollectionUtils;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.table.QualifiedName;
import org.apache.ignite.table.QualifiedNameHelper;
import org.apache.ignite.tx.TransactionException;
import org.jetbrains.annotations.Nullable;

/**
 * Storage of table rows.
 */
public class InternalTableImpl implements InternalTable {
    /** Primary replica await timeout. */
    public static final int AWAIT_PRIMARY_REPLICA_TIMEOUT = 30;

    /** Default no-op implementation to avoid unnecessary allocations. */
    private static final ReadWriteInflightBatchRequestTracker READ_WRITE_INFLIGHT_BATCH_REQUEST_TRACKER =
            new ReadWriteInflightBatchRequestTracker();

    /** Table messages factory. */
    private static final PartitionReplicationMessagesFactory TABLE_MESSAGES_FACTORY = new PartitionReplicationMessagesFactory();

    /** Replica messages factory. */
    private static final ReplicaMessagesFactory REPLICA_MESSAGES_FACTORY = new ReplicaMessagesFactory();

    /** Partitions. */
    private final int partitions;

    private final Supplier<ScheduledExecutorService> streamerFlushExecutor;

    private final StreamerReceiverRunner streamerReceiverRunner;

    /** Table name. */
    private volatile QualifiedName tableName;

    /** Table identifier. */
    private final int tableId;

    /** Distribution zone identifier. */
    private final int zoneId;

    /** Resolver that resolves a node consistent ID to cluster node. */
    private final ClusterNodeResolver clusterNodeResolver;

    /** Transactional manager. */
    protected final TxManager txManager;

    private final TransactionInflights transactionInflights;

    /** Storage for table data. */
    private final MvTableStorage tableStorage;

    /** Replica service. */
    private final ReplicaService replicaSvc;

    /** A hybrid logical clock service. */
    private final ClockService clockService;

    /** Observable timestamp tracker. */
    private final HybridTimestampTracker observableTimestampTracker;

    /** Placement driver. */
    private final PlacementDriver placementDriver;

    /** Default read-write transaction timeout. */
    private final Supplier<Long> defaultRwTxTimeout;

    /** Default read-only transaction timeout. */
    private final Supplier<Long> defaultReadTxTimeout;

    private final ReadWriteMetricSource metrics;

    /**
     * Constructor.
     *
     * @param tableName Table name.
     * @param zoneId Distribution zone identifier.
     * @param tableId Table identifier.
     * @param partitions Number of partitions.
     * @param clusterNodeResolver Cluster node resolver.
     * @param txManager Transaction manager.
     * @param tableStorage Table storage.
     * @param replicaSvc Replica service.
     * @param clockService A hybrid logical clock service.
     * @param placementDriver Placement driver.
     * @param transactionInflights Transaction inflights.
     * @param streamerFlushExecutor Streamer flush executor.
     * @param streamerReceiverRunner Streamer receiver runner.
     * @param defaultRwTxTimeout Default read-write transaction timeout.
     * @param defaultReadTxTimeout Default read-only transaction timeout.
     */
    public InternalTableImpl(
            QualifiedName tableName,
            int zoneId,
            int tableId,
            int partitions,
            ClusterNodeResolver clusterNodeResolver,
            TxManager txManager,
            MvTableStorage tableStorage,
            ReplicaService replicaSvc,
            ClockService clockService,
            HybridTimestampTracker observableTimestampTracker,
            PlacementDriver placementDriver,
            TransactionInflights transactionInflights,
            Supplier<ScheduledExecutorService> streamerFlushExecutor,
            StreamerReceiverRunner streamerReceiverRunner,
            Supplier<Long> defaultRwTxTimeout,
            Supplier<Long> defaultReadTxTimeout,
            ReadWriteMetricSource metrics
    ) {
        this.tableName = tableName;
        this.zoneId = zoneId;
        this.tableId = tableId;
        this.partitions = partitions;
        this.clusterNodeResolver = clusterNodeResolver;
        this.txManager = txManager;
        this.tableStorage = tableStorage;
        this.replicaSvc = replicaSvc;
        this.clockService = clockService;
        this.observableTimestampTracker = observableTimestampTracker;
        this.placementDriver = placementDriver;
        this.transactionInflights = transactionInflights;
        this.streamerFlushExecutor = streamerFlushExecutor;
        this.streamerReceiverRunner = streamerReceiverRunner;
        this.defaultRwTxTimeout = defaultRwTxTimeout;
        this.defaultReadTxTimeout = defaultReadTxTimeout;
        this.metrics = metrics;
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

    @Override
    public int zoneId() {
        return zoneId;
    }

    /** {@inheritDoc} */
    @Override
    public QualifiedName name() {
        return tableName;
    }

    @Override
    public synchronized void name(String newName) {
        this.tableName = QualifiedNameHelper.fromNormalized(tableName.schemaName(), newName);
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
            IgniteTriFunction<InternalTransaction, ZonePartitionId, Long, ReplicaRequest> fac,
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
            IgniteTriFunction<InternalTransaction, ZonePartitionId, Long, ReplicaRequest> fac,
            BiPredicate<R, ReplicaRequest> noWriteChecker,
            @Nullable Long txStartTs
    ) {
        // Check whether proposed tx is read-only. Complete future exceptionally if true.
        // Attempting to enlist a read-only in a read-write transaction does not corrupt the transaction itself, thus read-write transaction
        // won't be rolled back automatically - it's up to the user or outer engine.
        if (tx != null && tx.isReadOnly()) {
            return failedFuture(
                    new TransactionException(
                            TX_FAILED_READ_WRITE_OPERATION_ERR,
                            failedReadWriteOperationMsg(tx)
                    )
            );
        }

        InternalTransaction actualTx = startImplicitRwTxIfNeeded(tx);

        int partId = partitionId(row);

        ZonePartitionId partGroupId = targetReplicationGroupId(partId);

        PendingTxPartitionEnlistment enlistment = actualTx.enlistedPartition(partGroupId);

        CompletableFuture<R> fut;

        if (enlistment != null) {
            assert !actualTx.implicit();

            fut = trackingInvoke(
                    actualTx,
                    partId,
                    enlistmentConsistencyToken -> fac.apply(actualTx, partGroupId, enlistmentConsistencyToken),
                    false,
                    enlistment,
                    noWriteChecker,
                    txManager.lockRetryCount()
            );
        } else {
            fut = enlistAndInvoke(
                    actualTx,
                    partId,
                    enlistmentConsistencyToken -> fac.apply(
                            actualTx, partGroupId, enlistmentConsistencyToken),
                    actualTx.implicit(),
                    noWriteChecker
            );
        }

        return postEnlist(fut, false, actualTx, actualTx.implicit()).handle((r, e) -> {
            if (e != null) {
                if (actualTx.implicit()) {
                    long timeout = actualTx.getTimeout();

                    long ts = (txStartTs == null) ? actualTx.schemaTimestamp().getPhysical() : txStartTs;

                    boolean canRetry = canRetry(e, ts, timeout);

                    if (canRetry) {
                        return enlistInTx(row, null, fac, noWriteChecker, ts);
                    }
                }

                sneakyThrow(e);
            }

            return completedFuture(r);
        }).thenCompose(identity());
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
            ReplicaRequestFactory fac,
            Function<Collection<RowBatch>, CompletableFuture<T>> reducer,
            BiPredicate<T, ReplicaRequest> noOpChecker
    ) {
        return enlistInTx(keyRows, tx, fac, reducer, noOpChecker, null);
    }

    /**
     * Enlists collection of rows into a transaction.
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
            ReplicaRequestFactory fac,
            Function<Collection<RowBatch>, CompletableFuture<T>> reducer,
            BiPredicate<T, ReplicaRequest> noOpChecker,
            @Nullable Long txStartTs
    ) {
        // Check whether proposed tx is read-only. Complete future exceptionally if true.
        // Attempting to enlist a read-only in a read-write transaction does not corrupt the transaction itself, thus read-write transaction
        // won't be rolled back automatically - it's up to the user or outer engine.
        if (tx != null && tx.isReadOnly()) {
            return failedFuture(
                    new TransactionException(
                            TX_FAILED_READ_WRITE_OPERATION_ERR,
                            failedReadWriteOperationMsg(tx)
                    )
            );
        }

        InternalTransaction actualTx = startImplicitRwTxIfNeeded(tx);

        Int2ObjectMap<RowBatch> rowBatchByPartitionId = toRowBatchByPartitionId(keyRows);

        boolean singlePart = rowBatchByPartitionId.size() == 1;
        boolean full = actualTx.implicit() && singlePart;

        for (Int2ObjectMap.Entry<RowBatch> partitionRowBatch : rowBatchByPartitionId.int2ObjectEntrySet()) {
            int partitionId = partitionRowBatch.getIntKey();
            RowBatch rowBatch = partitionRowBatch.getValue();

            ZonePartitionId replicationGroupId = targetReplicationGroupId(partitionId);

            PendingTxPartitionEnlistment enlistment = actualTx.enlistedPartition(replicationGroupId);

            CompletableFuture<T> fut;

            if (enlistment != null) {
                assert !actualTx.implicit();

                fut = trackingInvoke(
                        actualTx,
                        partitionId,
                        enlistmentConsistencyToken ->
                                fac.create(rowBatch.requestedRows, actualTx, replicationGroupId, enlistmentConsistencyToken, false),
                        false,
                        enlistment,
                        noOpChecker,
                        txManager.lockRetryCount()
                );
            } else {
                fut = enlistAndInvoke(
                        actualTx,
                        partitionId,
                        enlistmentConsistencyToken ->
                                fac.create(rowBatch.requestedRows, actualTx, replicationGroupId, enlistmentConsistencyToken, full),
                        full,
                        noOpChecker
                );
            }

            rowBatch.resultFuture = fut;
        }

        CompletableFuture<T> fut = reducer.apply(rowBatchByPartitionId.values());

        return postEnlist(fut, actualTx.implicit() && !singlePart, actualTx, full).handle((r, e) -> {
            if (e != null) {
                if (actualTx.implicit()) {
                    long timeout = actualTx.getTimeout();

                    long ts = (txStartTs == null) ? actualTx.schemaTimestamp().getPhysical() : txStartTs;

                    if (canRetry(e, ts, timeout)) {
                        return enlistInTx(keyRows, null, fac, reducer, noOpChecker, ts);
                    }
                }

                sneakyThrow(e);
            }

            return completedFuture(r);
        }).thenCompose(identity());
    }

    private InternalTransaction startImplicitRwTxIfNeeded(@Nullable InternalTransaction tx) {
        return tx == null ? txManager.beginImplicitRw(observableTimestampTracker) : tx;
    }

    private InternalTransaction startImplicitRoTxIfNeeded(@Nullable InternalTransaction tx) {
        return tx == null ? txManager.beginImplicitRo(observableTimestampTracker) : tx;
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
            int flags
    ) {
        ZonePartitionId replicationGroupId = targetReplicationGroupId(partId);

        PendingTxPartitionEnlistment enlistment = tx.enlistedPartition(replicationGroupId);

        CompletableFuture<Collection<BinaryRow>> fut;

        Function<Long, ReplicaRequest> mapFunc =
                (enlistmentConsistencyToken) -> TABLE_MESSAGES_FACTORY.readWriteScanRetrieveBatchReplicaRequest()
                        .groupId(serializeReplicationGroupId(replicationGroupId))
                        .tableId(tableId)
                        .timestamp(tx.schemaTimestamp())
                        .transactionId(tx.id())
                        .scanId(scanId)
                        .indexToUse(indexId)
                        .exactKey(binaryTupleMessage(exactKey))
                        .lowerBoundPrefix(binaryTupleMessage(lowerBound))
                        .upperBoundPrefix(binaryTupleMessage(upperBound))
                        .flags(flags)
                        .full(tx.implicit()) // Intent for one phase commit.
                        .batchSize(batchSize)
                        .enlistmentConsistencyToken(enlistmentConsistencyToken)
                        .commitPartitionId(serializeReplicationGroupId(tx.commitPartition()))
                        .coordinatorId(tx.coordinatorId())
                        .build();

        if (enlistment != null) {
            enlistment.addTableId(tableId);

            fut = replicaSvc.invoke(enlistment.primaryNodeConsistentId(), mapFunc.apply(enlistment.consistencyToken()));
        } else {
            fut = enlistAndInvoke(tx, partId, mapFunc, false, null);
        }

        return postEnlist(fut, false, tx, false);
    }

    private static @Nullable BinaryTupleMessage binaryTupleMessage(@Nullable BinaryTupleReader binaryTuple) {
        if (binaryTuple == null) {
            return null;
        }

        return TABLE_MESSAGES_FACTORY.binaryTupleMessage()
                .tuple(binaryTuple.byteBuffer())
                .elementCount(binaryTuple.elementCount())
                .build();
    }

    private static boolean canRetry(Throwable e, long ts, long timeout) {
        return exceptionAllowsImplicitTxRetry(e) && coarseCurrentTimeMillis() - ts < timeout;
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
        return enlist(partId, tx)
                .thenCompose(enlistment ->
                        trackingInvoke(tx, partId, mapFunc, full, enlistment, noWriteChecker, txManager.lockRetryCount()));
    }

    /**
     * Invoke replica with additional tracking for writes.
     *
     * @param tx The transaction.
     * @param partId Partition id.
     * @param mapFunc Request factory.
     * @param full {@code True} for a full transaction.
     * @param enlistment Enlisted partition.
     * @param noWriteChecker Used to handle operations producing no updates.
     * @param retryOnLockConflict {@code True} to retry on lock conflicts.
     * @return The future.
     */
    private <R> CompletableFuture<R> trackingInvoke(
            InternalTransaction tx,
            int partId,
            Function<Long, ReplicaRequest> mapFunc,
            boolean full,
            PendingTxPartitionEnlistment enlistment,
            @Nullable BiPredicate<R, ReplicaRequest> noWriteChecker,
            int retryOnLockConflict
    ) {
        assert !tx.isReadOnly() : format("Tracking invoke is available only for read-write transactions [tx={}].", tx);

        enlistment.addTableId(tableId);

        ReplicaRequest request = mapFunc.apply(enlistment.consistencyToken());

        if (full) { // Full transaction retries are handled in postEnlist.
            return replicaSvc.invokeRaw(enlistment.primaryNodeConsistentId(), request).handle((r, e) -> {
                boolean hasError = e != null;
                assert hasError || r instanceof TimestampAware;

                // Timestamp is set to commit timestamp for full transactions.
                tx.finish(!hasError, hasError ? null : ((TimestampAware) r).timestamp(), true, hasError ? e : null);

                if (e != null) {
                    sneakyThrow(e);
                }

                return (R) r.result();
            });
        } else {
            ReadWriteReplicaRequest req = (ReadWriteReplicaRequest) request;

            if (req.isWrite()) {
                // Track only write requests from explicit transactions.
                if (!tx.remote() && !transactionInflights.addInflight(tx.id())) {
                    int code = TX_ALREADY_FINISHED_ERR;
                    if (tx.isRolledBackWithTimeoutExceeded()) {
                        code = TX_ALREADY_FINISHED_WITH_TIMEOUT_ERR;
                    }

                    Throwable cause = lastException(tx.id());

                    return failedFuture(
                            new TransactionException(code, format(
                                    "Transaction is already finished or finishing"
                                            + " [tableName={}, partId={}, txState={}, timeoutExceeded={}].",
                                    tableName,
                                    partId,
                                    tx.state(),
                                    tx.isRolledBackWithTimeoutExceeded()
                            ), cause));
                }

                return replicaSvc.<R>invoke(enlistment.primaryNodeConsistentId(), request).thenApply(res -> {
                    assert noWriteChecker != null;

                    // Remove inflight if no replication was scheduled, otherwise inflight will be removed by delayed response.
                    if (noWriteChecker.test(res, request)) {
                        if (!tx.remote()) {
                            transactionInflights.removeInflight(tx.id());
                        } else {
                            tx.kill(); // Kill for remote txn has special meaning - no-op enlistment..
                        }
                    }

                    return res;
                }).handle((r, e) -> {
                    if (e != null) {
                        if (retryOnLockConflict > 0 && matchAny(unwrapCause(e), ACQUIRE_LOCK_ERR)) {
                            if (!tx.remote()) {
                                transactionInflights.removeInflight(tx.id()); // Will be retried.
                            }

                            return trackingInvoke(
                                    tx,
                                    partId,
                                    ignored -> request,
                                    false,
                                    enlistment,
                                    noWriteChecker,
                                    retryOnLockConflict - 1
                            );
                        }

                        sneakyThrow(e);
                    }

                    return completedFuture(r);
                }).thenCompose(identity());
            } else { // Explicit reads should be retried too.
                return replicaSvc.<R>invoke(enlistment.primaryNodeConsistentId(), request).handle((r, e) -> {
                    if (e != null) {
                        if (retryOnLockConflict > 0 && matchAny(unwrapCause(e), ACQUIRE_LOCK_ERR)) {
                            return trackingInvoke(
                                    tx,
                                    partId,
                                    ignored -> request,
                                    false,
                                    enlistment,
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
    private static <T> CompletableFuture<T> postEnlist(
            CompletableFuture<T> fut, boolean autoCommit, InternalTransaction tx0, boolean full
    ) {
        assert !(autoCommit && full) : "Invalid combination of flags";

        return fut.handle((BiFunction<T, Throwable, CompletableFuture<T>>) (r, e) -> {
            if (full || tx0.remote()) {
                return e != null ? failedFuture(e) : completedFuture(r);
            }

            if (e != null) {
                CompletableFuture<Void> rollbackFuture = tx0.rollbackWithExceptionAsync(e);

                return rollbackFuture.handle((ignored, err) -> {
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
    }

    /**
     * Evaluates the single-row request to the cluster for a read-only single-partition implicit transaction.
     *
     * @param tx Transaction or {@code null} if it is not started yet.
     * @param row Binary row.
     * @param op Operation.
     * @param <R> Result type.
     * @return The future.
     */
    private <R> CompletableFuture<R> evaluateReadOnlyPrimaryNode(
            @Nullable InternalTransaction tx,
            BinaryRowEx row,
            BiFunction<ZonePartitionId, Long, ReplicaRequest> op
    ) {
        InternalTransaction actualTx = startImplicitRoTxIfNeeded(tx);

        int partId = partitionId(row);

        ZonePartitionId replicationGroupId = targetReplicationGroupId(partId);

        return sendReadOnlyToPrimaryReplica(actualTx, replicationGroupId, op);
    }

    /**
     * Evaluates the multi-row request to the cluster for a read-only single-partition implicit transaction.
     *
     * @param rows Rows.
     * @param op Replica requests factory.
     * @param <R> Result type.
     * @return The future.
     */
    private <R> CompletableFuture<R> evaluateReadOnlyPrimaryNode(
            Collection<BinaryRowEx> rows,
            BiFunction<ZonePartitionId, Long, ReplicaRequest> op
    ) {
        InternalTransaction actualTx = txManager.beginImplicitRo(observableTimestampTracker);

        int partId = partitionId(rows.iterator().next());

        ZonePartitionId replicationGroupId = targetReplicationGroupId(partId);

        return sendReadOnlyToPrimaryReplica(actualTx, replicationGroupId, op);
    }

    /**
     * Sends a read-only transaction request to the primary replica for a replication grout specified.
     *
     * @param tx Transaction.
     * @param replicationGroupId Replication group id.
     * @param op Replica requests factory.
     * @param <R> The future.
     * @return The future.
     */
    private <R> CompletableFuture<R> sendReadOnlyToPrimaryReplica(
            InternalTransaction tx,
            ZonePartitionId replicationGroupId,
            BiFunction<ZonePartitionId, Long, ReplicaRequest> op
    ) {
        ReplicaMeta meta = placementDriver.getCurrentPrimaryReplica(replicationGroupId, tx.schemaTimestamp());

        Function<ReplicaMeta, CompletableFuture<R>> evaluateClo = primaryReplica -> {
            try {
                InternalClusterNode node = getClusterNode(primaryReplica);

                return replicaSvc.invoke(node, op.apply(replicationGroupId, enlistmentConsistencyToken(primaryReplica)));
            } catch (Throwable e) {
                String canonicalName = tableName.toCanonicalForm();
                throw new TransactionException(
                        INTERNAL_ERR,
                        format("Failed to invoke the replica request [tableName={}, grp={}].", canonicalName, replicationGroupId),
                        e
                );
            }
        };

        CompletableFuture<R> fut;

        if (meta != null && clusterNodeResolver.getById(meta.getLeaseholderId()) != null) {
            try {
                fut = evaluateClo.apply(meta);
            } catch (IgniteException e) {
                return failedFuture(e);
            }
        } else {
            fut = awaitPrimaryReplica(replicationGroupId, tx.schemaTimestamp())
                    .thenCompose(evaluateClo);
        }

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
                return tx.finish(false, clockService.current(), false, e)
                        .handle((ignored, err) -> {
                            if (err != null) {
                                // Preserve failed state.
                                e.addSuppressed(err);
                            }

                            sneakyThrow(e);
                            return null;
                        });
            }

            return tx.finish(true, clockService.current(), false, null).thenApply(ignored -> r);
        }).thenCompose(identity());
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<BinaryRow> get(BinaryRowEx keyRow, @Nullable InternalTransaction tx) {
        checkTransactionFinishStarted(tx);

        if (isDirectFlowApplicable(tx)) {
            return evaluateReadOnlyPrimaryNode(
                    tx,
                    keyRow,
                    (groupId, consistencyToken) -> TABLE_MESSAGES_FACTORY.readOnlyDirectSingleRowReplicaRequest()
                            .groupId(serializeReplicationGroupId(groupId))
                            .tableId(tableId)
                            .enlistmentConsistencyToken(consistencyToken)
                            .schemaVersion(keyRow.schemaVersion())
                            .primaryKey(keyRow.tupleSlice())
                            .requestType(RO_GET)
                            .build()
            );
        }

        if (tx.isReadOnly()) {
            return evaluateReadOnlyRecipientNode(partitionId(keyRow), tx.readTimestamp())
                    .thenCompose(recipientNode -> get(keyRow, tx.readTimestamp(), tx.id(), tx.coordinatorId(), recipientNode));
        }

        return enlistInTx(
                keyRow,
                tx,
                (txo, groupId, enlistmentConsistencyToken) -> TABLE_MESSAGES_FACTORY.readWriteSingleRowPkReplicaRequest()
                        .groupId(serializeReplicationGroupId(groupId))
                        .tableId(tableId)
                        .schemaVersion(keyRow.schemaVersion())
                        .primaryKey(keyRow.tupleSlice())
                        .commitPartitionId(serializeReplicationGroupId(txo.commitPartition()))
                        .transactionId(txo.id())
                        .enlistmentConsistencyToken(enlistmentConsistencyToken)
                        .requestType(RW_GET)
                        .timestamp(txo.schemaTimestamp())
                        .full(false)
                        .coordinatorId(txo.coordinatorId())
                        .build(),
                (res, req) -> false
        );
    }

    @Override
    public CompletableFuture<BinaryRow> get(
            BinaryRowEx keyRow,
            HybridTimestamp readTimestamp,
            @Nullable UUID transactionId,
            @Nullable UUID coordinatorId,
            InternalClusterNode recipientNode
    ) {
        int partId = partitionId(keyRow);
        ZonePartitionId replicationGroupId = targetReplicationGroupId(partId);

        return replicaSvc.invoke(recipientNode, TABLE_MESSAGES_FACTORY.readOnlySingleRowPkReplicaRequest()
                .groupId(serializeReplicationGroupId(replicationGroupId))
                .tableId(tableId)
                .schemaVersion(keyRow.schemaVersion())
                .primaryKey(keyRow.tupleSlice())
                .requestType(RO_GET)
                .readTimestamp(readTimestamp)
                .transactionId(transactionId)
                .coordinatorId(coordinatorId)
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
        checkTransactionFinishStarted(tx);

        if (CollectionUtils.nullOrEmpty(keyRows)) {
            return emptyListCompletedFuture();
        }

        if (tx == null && isSinglePartitionBatch(keyRows)) {
            return evaluateReadOnlyPrimaryNode(
                    keyRows,
                    (groupId, consistencyToken) -> TABLE_MESSAGES_FACTORY.readOnlyDirectMultiRowReplicaRequest()
                            .groupId(serializeReplicationGroupId(groupId))
                            .tableId(tableId)
                            .enlistmentConsistencyToken(consistencyToken)
                            .schemaVersion(keyRows.iterator().next().schemaVersion())
                            .primaryKeys(serializeBinaryTuples(keyRows))
                            .requestType(RO_GET_ALL)
                            .build()
            );
        }

        if (tx != null && tx.isReadOnly()) {
            assert !tx.implicit() : "implicit RO getAll not supported";

            return getAll(keyRows, tx.readTimestamp(), tx.id(), tx.coordinatorId(), null);
        }

        return enlistInTx(
                keyRows,
                tx,
                (keyRows0, txo, groupId, enlistmentConsistencyToken, full) ->
                        readWriteMultiRowPkReplicaRequest(RW_GET_ALL, keyRows0, txo, groupId, enlistmentConsistencyToken, full),
                InternalTableImpl::collectMultiRowsResponsesWithRestoreOrder,
                (res, req) -> false
        );
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<List<BinaryRow>> getAll(
            Collection<BinaryRowEx> keyRows,
            HybridTimestamp readTimestamp,
            @Nullable UUID transactionId,
            @Nullable UUID coordinatorId,
            @Nullable InternalClusterNode recipientNode
    ) {
        Int2ObjectMap<RowBatch> rowBatchByPartitionId = toRowBatchByPartitionId(keyRows);

        for (Int2ObjectMap.Entry<RowBatch> partitionRowBatch : rowBatchByPartitionId.int2ObjectEntrySet()) {
            int partitionId = partitionRowBatch.getIntKey();

            ZonePartitionId replicationGroupId = targetReplicationGroupId(partitionId);

            ReadOnlyMultiRowPkReplicaRequest request = TABLE_MESSAGES_FACTORY.readOnlyMultiRowPkReplicaRequest()
                    .groupId(serializeReplicationGroupId(replicationGroupId))
                    .tableId(tableId)
                    .schemaVersion(partitionRowBatch.getValue().requestedRows.get(0).schemaVersion())
                    .primaryKeys(serializeBinaryTuples(partitionRowBatch.getValue().requestedRows))
                    .requestType(RO_GET_ALL)
                    .readTimestamp(readTimestamp)
                    .transactionId(transactionId)
                    .coordinatorId(coordinatorId)
                    .build();

            partitionRowBatch.getValue().resultFuture = recipientNode != null
                    ? replicaSvc.invoke(recipientNode, request)
                    : evaluateReadOnlyRecipientNode(partitionId, readTimestamp)
                            .thenCompose(targetNode -> replicaSvc.invoke(targetNode, request));
        }

        return collectMultiRowsResponsesWithRestoreOrder(rowBatchByPartitionId.values());
    }

    private ReadWriteMultiRowPkReplicaRequest readWriteMultiRowPkReplicaRequest(
            RequestType requestType,
            Collection<? extends BinaryRow> rows,
            InternalTransaction tx,
            ZonePartitionId groupId,
            Long enlistmentConsistencyToken,
            boolean full
    ) {
        assert allSchemaVersionsSame(rows) : "Different schema versions encountered: " + uniqueSchemaVersions(rows);

        return TABLE_MESSAGES_FACTORY.readWriteMultiRowPkReplicaRequest()
                .groupId(serializeReplicationGroupId(groupId))
                .tableId(tableId)
                .commitPartitionId(serializeReplicationGroupId(tx.commitPartition()))
                .schemaVersion(rows.iterator().next().schemaVersion())
                .primaryKeys(serializeBinaryTuples(rows))
                .transactionId(tx.id())
                .enlistmentConsistencyToken(enlistmentConsistencyToken)
                .requestType(requestType)
                .timestamp(tx.schemaTimestamp())
                .full(full)
                .coordinatorId(tx.coordinatorId())
                .delayedAckProcessor(tx.remote() ? tx::processDelayedAck : null)
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

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Void> upsert(BinaryRowEx row, @Nullable InternalTransaction tx) {
        return enlistInTx(
                row,
                tx,
                (txo, groupId, enlistmentConsistencyToken) -> TABLE_MESSAGES_FACTORY.readWriteSingleRowReplicaRequest()
                        .groupId(serializeReplicationGroupId(groupId))
                        .tableId(tableId)
                        .commitPartitionId(serializeReplicationGroupId(txo.commitPartition()))
                        .schemaVersion(row.schemaVersion())
                        .binaryTuple(row.tupleSlice())
                        .transactionId(txo.id())
                        .enlistmentConsistencyToken(enlistmentConsistencyToken)
                        .requestType(RW_UPSERT)
                        .timestamp(txo.schemaTimestamp())
                        .full(txo.implicit())
                        .coordinatorId(txo.coordinatorId())
                        .delayedAckProcessor(txo.remote() ? txo::processDelayedAck : null)
                        .build(),
                (res, req) -> false
        );
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Void> upsertAll(Collection<BinaryRowEx> rows, @Nullable InternalTransaction tx) {
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
        InternalTransaction tx = txManager.beginImplicitRw(observableTimestampTracker);
        ZonePartitionId replicationGroupId = targetReplicationGroupId(partition);

        assert rows.stream().allMatch(row -> partitionId(row) == partition) : "Invalid batch for partition " + partition;

        CompletableFuture<Void> fut = enlistAndInvoke(
                tx,
                partition,
                enlistmentConsistencyToken ->
                        upsertAllInternal(rows, deleted, tx, replicationGroupId, enlistmentConsistencyToken, true),
                true,
                null
        );

        // Will be finished in one RTT.
        return postEnlist(fut, false, tx, true).handle((r, e) -> {
            if (e != null) {
                long timeout = tx.getTimeout();

                long ts = (txStartTs == null) ? tx.schemaTimestamp().getPhysical() : txStartTs;

                if (canRetry(e, ts, timeout)) {
                    return updateAllWithRetry(rows, deleted, partition, ts);
                }

                sneakyThrow(e);
            }

            return completedFuture(r);
        }).thenCompose(identity());
    }

    private long getDefaultTimeout(InternalTransaction tx) {
        return tx.isReadOnly() ? defaultReadTxTimeout.get() : defaultRwTxTimeout.get();
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<BinaryRow> getAndUpsert(BinaryRowEx row, InternalTransaction tx) {
        checkTransactionFinishStarted(tx);

        return enlistInTx(
                row,
                tx,
                (txo, groupId, enlistmentConsistencyToken) -> TABLE_MESSAGES_FACTORY.readWriteSingleRowReplicaRequest()
                        .groupId(serializeReplicationGroupId(groupId))
                        .tableId(tableId)
                        .commitPartitionId(serializeReplicationGroupId(txo.commitPartition()))
                        .schemaVersion(row.schemaVersion())
                        .binaryTuple(row.tupleSlice())
                        .transactionId(txo.id())
                        .enlistmentConsistencyToken(enlistmentConsistencyToken)
                        .requestType(RW_GET_AND_UPSERT)
                        .timestamp(txo.schemaTimestamp())
                        .full(txo.implicit())
                        .coordinatorId(txo.coordinatorId())
                        .delayedAckProcessor(txo.remote() ? txo::processDelayedAck : null)
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
                (txo, groupId, enlistmentConsistencyToken) -> TABLE_MESSAGES_FACTORY.readWriteSingleRowReplicaRequest()
                        .groupId(serializeReplicationGroupId(groupId))
                        .tableId(tableId)
                        .commitPartitionId(serializeReplicationGroupId(txo.commitPartition()))
                        .schemaVersion(row.schemaVersion())
                        .binaryTuple(row.tupleSlice())
                        .transactionId(txo.id())
                        .enlistmentConsistencyToken(enlistmentConsistencyToken)
                        .requestType(RW_INSERT)
                        .timestamp(txo.schemaTimestamp())
                        .full(txo.implicit())
                        .coordinatorId(txo.coordinatorId())
                        .delayedAckProcessor(txo.remote() ? txo::processDelayedAck : null)
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
                (keyRows, txo, groupId, enlistmentConsistencyToken, full) ->
                        readWriteMultiRowReplicaRequest(
                                RW_INSERT_ALL,
                                keyRows,
                                null,
                                txo,
                                groupId,
                                enlistmentConsistencyToken,
                                full),
                InternalTableImpl::collectRejectedRowsResponses,
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
            @Nullable BitSet deleted,
            InternalTransaction tx,
            ZonePartitionId groupId,
            Long enlistmentConsistencyToken,
            boolean full
    ) {
        assert allSchemaVersionsSame(rows) : "Different schema versions encountered: " + uniqueSchemaVersions(rows);

        return TABLE_MESSAGES_FACTORY.readWriteMultiRowReplicaRequest()
                .groupId(serializeReplicationGroupId(groupId))
                .tableId(tableId)
                .commitPartitionId(serializeReplicationGroupId(tx.commitPartition()))
                .schemaVersion(rows.iterator().next().schemaVersion())
                .binaryTuples(serializeBinaryTuples(rows))
                .deleted(deleted)
                .transactionId(tx.id())
                .enlistmentConsistencyToken(enlistmentConsistencyToken)
                .requestType(requestType)
                .timestamp(tx.schemaTimestamp())
                .full(full)
                .coordinatorId(tx.coordinatorId())
                .delayedAckProcessor(tx.remote() ? tx::processDelayedAck : null)
                .build();
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Boolean> replace(BinaryRowEx row, InternalTransaction tx) {
        return enlistInTx(
                row,
                tx,
                (txo, groupId, enlistmentConsistencyToken) -> TABLE_MESSAGES_FACTORY.readWriteSingleRowReplicaRequest()
                        .groupId(serializeReplicationGroupId(groupId))
                        .tableId(tableId)
                        .commitPartitionId(serializeReplicationGroupId(txo.commitPartition()))
                        .schemaVersion(row.schemaVersion())
                        .binaryTuple(row.tupleSlice())
                        .transactionId(txo.id())
                        .enlistmentConsistencyToken(enlistmentConsistencyToken)
                        .requestType(RW_REPLACE_IF_EXIST)
                        .timestamp(txo.schemaTimestamp())
                        .full(txo.implicit())
                        .coordinatorId(txo.coordinatorId())
                        .delayedAckProcessor(txo.remote() ? txo::processDelayedAck : null)
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
                (txo, groupId, enlistmentConsistencyToken) -> TABLE_MESSAGES_FACTORY.readWriteSwapRowReplicaRequest()
                        .groupId(serializeReplicationGroupId(groupId))
                        .tableId(tableId)
                        .commitPartitionId(serializeReplicationGroupId(txo.commitPartition()))
                        .schemaVersion(oldRow.schemaVersion())
                        .oldBinaryTuple(oldRow.tupleSlice())
                        .newBinaryTuple(newRow.tupleSlice())
                        .transactionId(txo.id())
                        .enlistmentConsistencyToken(enlistmentConsistencyToken)
                        .requestType(RW_REPLACE)
                        .timestamp(txo.schemaTimestamp())
                        .full(txo.implicit())
                        .coordinatorId(txo.coordinatorId())
                        .delayedAckProcessor(txo.remote() ? txo::processDelayedAck : null)
                        .build(),
                (res, req) -> !res
        );
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<BinaryRow> getAndReplace(BinaryRowEx row, InternalTransaction tx) {
        checkTransactionFinishStarted(tx);

        return enlistInTx(
                row,
                tx,
                (txo, groupId, enlistmentConsistencyToken) -> TABLE_MESSAGES_FACTORY.readWriteSingleRowReplicaRequest()
                        .groupId(serializeReplicationGroupId(groupId))
                        .tableId(tableId)
                        .commitPartitionId(serializeReplicationGroupId(txo.commitPartition()))
                        .schemaVersion(row.schemaVersion())
                        .binaryTuple(row.tupleSlice())
                        .transactionId(txo.id())
                        .enlistmentConsistencyToken(enlistmentConsistencyToken)
                        .requestType(RW_GET_AND_REPLACE)
                        .timestamp(txo.schemaTimestamp())
                        .full(txo.implicit())
                        .coordinatorId(txo.coordinatorId())
                        .delayedAckProcessor(txo.remote() ? txo::processDelayedAck : null)
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
                (txo, groupId, enlistmentConsistencyToken) -> TABLE_MESSAGES_FACTORY.readWriteSingleRowPkReplicaRequest()
                        .groupId(serializeReplicationGroupId(groupId))
                        .tableId(tableId)
                        .commitPartitionId(serializeReplicationGroupId(txo.commitPartition()))
                        .schemaVersion(keyRow.schemaVersion())
                        .primaryKey(keyRow.tupleSlice())
                        .transactionId(txo.id())
                        .enlistmentConsistencyToken(enlistmentConsistencyToken)
                        .requestType(RW_DELETE)
                        .timestamp(txo.schemaTimestamp())
                        .full(txo.implicit())
                        .coordinatorId(txo.coordinatorId())
                        .delayedAckProcessor(txo.remote() ? txo::processDelayedAck : null)
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
                (txo, groupId, enlistmentConsistencyToken) -> TABLE_MESSAGES_FACTORY.readWriteSingleRowReplicaRequest()
                        .groupId(serializeReplicationGroupId(groupId))
                        .tableId(tableId)
                        .commitPartitionId(serializeReplicationGroupId(txo.commitPartition()))
                        .schemaVersion(oldRow.schemaVersion())
                        .binaryTuple(oldRow.tupleSlice())
                        .transactionId(txo.id())
                        .enlistmentConsistencyToken(enlistmentConsistencyToken)
                        .requestType(RW_DELETE_EXACT)
                        .timestamp(txo.schemaTimestamp())
                        .full(txo.implicit())
                        .coordinatorId(txo.coordinatorId())
                        .delayedAckProcessor(txo.remote() ? txo::processDelayedAck : null)
                        .build(),
                (res, req) -> !res
        );
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<BinaryRow> getAndDelete(BinaryRowEx row, InternalTransaction tx) {
        checkTransactionFinishStarted(tx);

        return enlistInTx(
                row,
                tx,
                (txo, groupId, enlistmentConsistencyToken) -> TABLE_MESSAGES_FACTORY.readWriteSingleRowPkReplicaRequest()
                        .groupId(serializeReplicationGroupId(groupId))
                        .tableId(tableId)
                        .commitPartitionId(serializeReplicationGroupId(txo.commitPartition()))
                        .schemaVersion(row.schemaVersion())
                        .primaryKey(row.tupleSlice())
                        .transactionId(txo.id())
                        .enlistmentConsistencyToken(enlistmentConsistencyToken)
                        .requestType(RW_GET_AND_DELETE)
                        .timestamp(txo.schemaTimestamp())
                        .full(txo.implicit())
                        .coordinatorId(txo.coordinatorId())
                        .delayedAckProcessor(txo.remote() ? txo::processDelayedAck : null)
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
                (keyRows0, txo, groupId, enlistmentConsistencyToken, full) ->
                        readWriteMultiRowPkReplicaRequest(RW_DELETE_ALL, keyRows0, txo, groupId, enlistmentConsistencyToken, full),
                InternalTableImpl::collectRejectedRowsResponses,
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
                (keyRows0, txo, groupId, enlistmentConsistencyToken, full) ->
                        readWriteMultiRowReplicaRequest(
                                RW_DELETE_EXACT_ALL,
                                keyRows0,
                                null,
                                txo,
                                groupId,
                                enlistmentConsistencyToken,
                                full
                        ),
                InternalTableImpl::collectRejectedRowsResponses,
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
    public Publisher<BinaryRow> scan(
            int partId,
            InternalClusterNode recipientNode,
            OperationContext operationContext
    ) {
        validatePartitionIndex(partId);

        if (operationContext.txContext().isReadOnly()) {
            return readOnlyScan(
                    partId,
                    recipientNode,
                    null,
                    null,
                    operationContext
            );
        } else {
            return readWriteScan(
                    partId,
                    recipientNode,
                    null,
                    null,
                    operationContext
            );
        }
    }

    @Override
    public Publisher<BinaryRow> scan(
            int partId,
            InternalClusterNode recipientNode,
            int indexId,
            IndexScanCriteria criteria,
            OperationContext operationContext
    ) {
        validatePartitionIndex(partId);

        if (operationContext.txContext().isReadOnly()) {
            return readOnlyScan(
                    partId,
                    recipientNode,
                    indexId,
                    criteria,
                    operationContext
            );
        } else {
            return readWriteScan(
                    partId,
                    recipientNode,
                    indexId,
                    criteria,
                    operationContext
            );
        }
    }

    @Override
    public Publisher<BinaryRow> scan(
            int partId,
            @Nullable InternalTransaction tx
    ) {
        validatePartitionIndex(partId);

        InternalTransaction actualTx = startImplicitRwTxIfNeeded(tx);

        return readWriteScan(partId, actualTx, null, null);
    }

    @Override
    public Publisher<BinaryRow> scan(
            int partId,
            @Nullable InternalTransaction tx,
            int indexId,
            IndexScanCriteria.Range criteria
    ) {

        validatePartitionIndex(partId);

        InternalTransaction actualTx = startImplicitRwTxIfNeeded(tx);

        return readWriteScan(partId, actualTx, indexId, criteria);
    }

    private Publisher<BinaryRow> readOnlyScan(
            int partId,
            InternalClusterNode recipientNode,
            @Nullable Integer indexId,
            @Nullable IndexScanCriteria criteria,
            OperationContext opCtx
    ) {
        assert opCtx.txContext().isReadOnly();

        boolean rangeScan = criteria instanceof IndexScanCriteria.Range;
        boolean lookup = criteria instanceof IndexScanCriteria.Lookup;

        BinaryTuple exactKey = lookup ? ((IndexScanCriteria.Lookup) criteria).key() : null;
        BinaryTuplePrefix lowerBound = rangeScan ? ((IndexScanCriteria.Range) criteria).lowerBound() : null;
        BinaryTuplePrefix upperBound = rangeScan ? ((IndexScanCriteria.Range) criteria).upperBound() : null;
        int flags = rangeScan ? ((IndexScanCriteria.Range) criteria).flags() : 0;

        TxContext.ReadOnly txContext = (TxContext.ReadOnly) opCtx.txContext();

        ZonePartitionId replicationGroupId = targetReplicationGroupId(partId);

        return new PartitionScanPublisher<>(new ReadOnlyInflightBatchRequestTracker(transactionInflights, txContext.txId(), txManager)) {
            @Override
            protected CompletableFuture<Collection<BinaryRow>> retrieveBatch(long scanId, int batchSize) {
                ReadOnlyScanRetrieveBatchReplicaRequest request = TABLE_MESSAGES_FACTORY.readOnlyScanRetrieveBatchReplicaRequest()
                        .groupId(serializeReplicationGroupId(replicationGroupId))
                        .tableId(tableId)
                        .transactionId(txContext.txId())
                        .coordinatorId(txContext.coordinatorId())
                        .readTimestamp(txContext.readTimestamp())
                        .scanId(scanId)
                        .batchSize(batchSize)
                        .indexToUse(indexId)
                        .exactKey(binaryTupleMessage(exactKey))
                        .lowerBoundPrefix(binaryTupleMessage(lowerBound))
                        .upperBoundPrefix(binaryTupleMessage(upperBound))
                        .flags(flags)
                        .build();

                return replicaSvc.invoke(recipientNode, request);
            }

            @Override
            protected CompletableFuture<Void> onClose(boolean intentionallyClose, long scanId, @Nullable Throwable th) {
                return completeScan(
                        txContext.txId(),
                        replicationGroupId,
                        scanId,
                        th,
                        recipientNode.name(),
                        intentionallyClose || th != null
                );
            }
        };
    }

    private Publisher<BinaryRow> readWriteScan(
            int partId,
            InternalTransaction tx,
            @Nullable Integer indexId,
            @Nullable IndexScanCriteria.Range criteria
    ) {
        // Check whether proposed tx is read-only. Complete future exceptionally if true.
        // Attempting to enlist a read-only in a read-write transaction does not corrupt the transaction itself, thus read-write transaction
        // won't be rolled back automatically - it's up to the user or outer engine.
        if (tx != null && tx.isReadOnly()) {
            throw new TransactionException(
                    TX_FAILED_READ_WRITE_OPERATION_ERR,
                    failedReadWriteOperationMsg(tx)
            );
        }

        validatePartitionIndex(partId);

        assert indexId == null || criteria != null;

        BinaryTuplePrefix lowerBound = indexId == null ? null : criteria.lowerBound();
        BinaryTuplePrefix upperBound = indexId == null ? null : criteria.upperBound();
        int flags = indexId == null ? 0 : criteria.flags();

        return new PartitionScanPublisher<>(READ_WRITE_INFLIGHT_BATCH_REQUEST_TRACKER) {
            @Override
            protected CompletableFuture<Collection<BinaryRow>> retrieveBatch(long scanId, int batchSize) {
                return enlistCursorInTx(
                        tx,
                        partId,
                        scanId,
                        batchSize,
                        indexId,
                        null,
                        lowerBound,
                        upperBound,
                        flags
                );
            }

            @Override
            protected CompletableFuture<Void> onClose(boolean intentionallyClose, long scanId, @Nullable Throwable th) {
                CompletableFuture<Void> opFut;

                if (tx.implicit()) {
                    opFut = completedOrFailedFuture(null, th);
                } else {
                    ZonePartitionId replicationGrpId = targetReplicationGroupId(partId);

                    PendingTxPartitionEnlistment enlistment = tx.enlistedPartition(replicationGrpId);
                    opFut = enlistment != null ? completeScan(
                            tx.id(),
                            replicationGrpId,
                            scanId,
                            th,
                            enlistment.primaryNodeConsistentId(),
                            intentionallyClose
                    ) : completedOrFailedFuture(null, th);
                }

                return postEnlist(
                        opFut,
                        intentionallyClose,
                        tx,
                        tx.implicit() && !intentionallyClose
                );
            }
        };
    }

    private Publisher<BinaryRow> readWriteScan(
            int partId,
            InternalClusterNode recipient,
            @Nullable Integer indexId,
            @Nullable IndexScanCriteria criteria,
            OperationContext opCtx
    ) {
        assert !opCtx.txContext().isReadOnly();

        boolean rangeScan = criteria instanceof IndexScanCriteria.Range;
        boolean lookup = criteria instanceof IndexScanCriteria.Lookup;

        BinaryTuple exactKey = lookup ? ((IndexScanCriteria.Lookup) criteria).key() : null;
        BinaryTuplePrefix lowerBound = rangeScan ? ((IndexScanCriteria.Range) criteria).lowerBound() : null;
        BinaryTuplePrefix upperBound = rangeScan ? ((IndexScanCriteria.Range) criteria).upperBound() : null;
        int flags = rangeScan ? ((IndexScanCriteria.Range) criteria).flags() : 0;

        TxContext.ReadWrite txContext = (TxContext.ReadWrite) opCtx.txContext();

        ZonePartitionId replicationGroupId = targetReplicationGroupId(partId);

        return new PartitionScanPublisher<>(READ_WRITE_INFLIGHT_BATCH_REQUEST_TRACKER) {
            @Override
            protected CompletableFuture<Collection<BinaryRow>> retrieveBatch(long scanId, int batchSize) {
                ReadWriteScanRetrieveBatchReplicaRequest request = TABLE_MESSAGES_FACTORY.readWriteScanRetrieveBatchReplicaRequest()
                        .groupId(serializeReplicationGroupId(replicationGroupId))
                        .tableId(tableId)
                        .transactionId(txContext.txId())
                        .coordinatorId(txContext.coordinatorId())
                        .timestamp(txContext.beginTimestamp())
                        .commitPartitionId(serializeReplicationGroupId(txContext.commitPartition()))
                        .enlistmentConsistencyToken(txContext.enlistmentConsistencyToken())
                        .scanId(scanId)
                        .indexToUse(indexId)
                        .exactKey(binaryTupleMessage(exactKey))
                        .lowerBoundPrefix(binaryTupleMessage(lowerBound))
                        .upperBoundPrefix(binaryTupleMessage(upperBound))
                        .flags(flags)
                        .batchSize(batchSize)
                        .full(false) // Set explicitly.
                        .build();

                return replicaSvc.invoke(recipient, request);
            }

            @Override
            protected CompletableFuture<Void> onClose(boolean intentionallyClose, long scanId, @Nullable Throwable th) {
                return completeScan(txContext.txId(), replicationGroupId, scanId, th, recipient.name(), intentionallyClose);
            }
        };
    }

    /**
     * Returns message used to create {@code TransactionException} with {@code ErrorGroups.TX_FAILED_READ_WRITE_OPERATION_ERR}.
     *
     * @param tx Internal transaction
     * @return message
     */
    private String failedReadWriteOperationMsg(InternalTransaction tx) {
        return format("Failed to enlist read-write operation into read-only transaction {}.",
                formatTxInfo(tx.id(), txManager));
    }

    /**
     * Closes the cursor on server side.
     *
     * @param txId Transaction id.
     * @param replicaGrpId Replication group id.
     * @param scanId Scan id.
     * @param th An exception that may occur in the scan procedure or {@code null} when the procedure passes without an exception.
     * @param recipientConsistentId Consistent ID of the server node where the scan was started.
     * @param explicitCloseCursor True when the cursor should be closed explicitly.
     * @return The future.
     */
    private CompletableFuture<Void> completeScan(
            UUID txId,
            ZonePartitionId replicaGrpId,
            long scanId,
            Throwable th,
            String recipientConsistentId,
            boolean explicitCloseCursor
    ) {
        CompletableFuture<Void> closeFut = nullCompletedFuture();

        if (explicitCloseCursor) {
            ScanCloseReplicaRequest scanCloseReplicaRequest = TABLE_MESSAGES_FACTORY.scanCloseReplicaRequest()
                    .groupId(serializeReplicationGroupId(replicaGrpId))
                    .tableId(tableId)
                    .transactionId(txId)
                    .scanId(scanId)
                    .timestamp(TransactionIds.beginTimestamp(txId))
                    .build();

            closeFut = replicaSvc.invoke(recipientConsistentId, scanCloseReplicaRequest);
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
    public int partitionId(BinaryRowEx row) {
        return IgniteUtils.safeAbs(row.colocationHash()) % partitions;
    }

    /**
     * Gets a batch result.
     *
     * @param rowBatches Row batches.
     * @return Future of collecting results.
     */
    public static CompletableFuture<List<BinaryRow>> collectRejectedRowsResponses(Collection<RowBatch> rowBatches) {
        return allResultFutures(rowBatches).thenApply(ignored -> {
            List<BinaryRow> result = new ArrayList<>();

            for (RowBatch batch : rowBatches) {
                List<BinaryRow> response = (List<BinaryRow>) batch.getCompletedResult();

                assert batch.requestedRows.size() == response.size() :
                        "Replication response does not fit to request [requestRows=" + batch.requestedRows.size()
                                + "responseRows=" + response.size() + ']';

                for (int i = 0; i < response.size(); i++) {
                    if (response.get(i) != null) {
                        continue;
                    }

                    result.add(batch.requestedRows.get(i));
                }
            }

            return result;
        });
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
    protected CompletableFuture<PendingTxPartitionEnlistment> enlist(int partId, InternalTransaction tx) {
        HybridTimestamp now = tx.schemaTimestamp();

        ZonePartitionId replicationGroupId = targetReplicationGroupId(partId);
        tx.assignCommitPartition(replicationGroupId);

        ReplicaMeta meta = placementDriver.getCurrentPrimaryReplica(replicationGroupId, now);

        Function<ReplicaMeta, PendingTxPartitionEnlistment> enlistClo = replicaMeta -> {
            ZonePartitionId partGroupId = targetReplicationGroupId(partId);

            String leaseHolderNodeId = replicaMeta.getLeaseholder();

            assert leaseHolderNodeId != null;

            tx.enlist(partGroupId, tableId, leaseHolderNodeId, enlistmentConsistencyToken(replicaMeta));

            return tx.enlistedPartition(partGroupId);
        };

        if (meta != null && clusterNodeResolver.getById(meta.getLeaseholderId()) != null) {
            try {
                return completedFuture(enlistClo.apply(meta));
            } catch (IgniteException e) {
                return failedFuture(e);
            }
        }

        return partitionMeta(replicationGroupId, now).thenApply(enlistClo);
    }

    @Override
    public CompletableFuture<InternalClusterNode> partitionLocation(int partitionIndex) {
        return partitionMeta(targetReplicationGroupId(partitionIndex), clockService.current()).thenApply(this::getClusterNode);
    }

    private CompletableFuture<ReplicaMeta> partitionMeta(ZonePartitionId replicationGroupId, HybridTimestamp at) {
        return awaitPrimaryReplica(replicationGroupId, at)
                .exceptionally(e -> {
                    throw withCause(
                            TransactionException::new,
                            REPLICA_UNAVAILABLE_ERR,
                            "Failed to get the primary replica [replicationGroupId=" + replicationGroupId + ", awaitTimestamp=" + at + ']',
                            e
                    );
                });
    }

    private InternalClusterNode getClusterNode(ReplicaMeta replicaMeta) {
        UUID leaseHolderId = replicaMeta.getLeaseholderId();

        InternalClusterNode node = leaseHolderId == null ? null : clusterNodeResolver.getById(leaseHolderId);

        if (node == null) {
            throw new TransactionException(
                    REPLICA_UNAVAILABLE_ERR,
                    String.format("Failed to resolve the primary replica node [id=%s]", leaseHolderId)
            );
        }

        return node;
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

        private final TxManager txManager;

        ReadOnlyInflightBatchRequestTracker(TransactionInflights transactionInflights, UUID txId, TxManager txManager) {
            this.transactionInflights = transactionInflights;
            this.txId = txId;
            this.txManager = txManager;
        }

        @Override
        public void onRequestBegin() {
            // Track read only requests which are able to create cursors.
            if (!transactionInflights.addScanInflight(txId)) {
                throw new TransactionException(TX_ALREADY_FINISHED_ERR, format(
                        "Transaction is already finished or finishing [{}, readOnly=true].",
                        formatTxInfo(txId, txManager, false)
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
        // No-op
    }

    // TODO: IGNITE-17963 Use smarter logic for recipient node evaluation.

    /**
     * Evaluated cluster node for read-only request processing.
     *
     * @param partId Partition id.
     * @param readTimestamp Read timestamp.
     * @return Cluster node to evaluate read-only request.
     */
    protected CompletableFuture<InternalClusterNode> evaluateReadOnlyRecipientNode(int partId, HybridTimestamp readTimestamp) {
        ZonePartitionId replicationGroupId = targetReplicationGroupId(partId);

        return awaitPrimaryReplica(replicationGroupId, readTimestamp)
                .handle((res, e) -> {
                    if (e != null) {
                        throw withCause(TransactionException::new, REPLICA_UNAVAILABLE_ERR, e);
                    } else {
                        if (res == null) {
                            throw createFailedGetPrimaryReplicaTransactionException(replicationGroupId, readTimestamp);
                        } else {
                            return getClusterNode(res);
                        }
                    }
                });
    }

    private static TransactionException createFailedGetPrimaryReplicaTransactionException(
            ZonePartitionId replicationGroupId,
            HybridTimestamp readTimestamp
    ) {
        String errorMessage = format(
                "Failed to get the primary replica [replicationGroupId={}, awaitTimestamp={}",
                replicationGroupId,
                readTimestamp
        );

        return new TransactionException(REPLICA_UNAVAILABLE_ERR, errorMessage);
    }

    @Override
    public ScheduledExecutorService streamerFlushExecutor() {
        return streamerFlushExecutor.get();
    }

    @Override
    public CompletableFuture<Long> estimatedSize() {
        HybridTimestamp now = clockService.current();

        var invokeFutures = new CompletableFuture<?>[partitions];

        for (int partId = 0; partId < partitions; partId++) {
            ZonePartitionId replicaGroupId = targetReplicationGroupId(partId);
            ReplicationGroupIdMessage partitionIdMessage = serializeReplicationGroupId(replicaGroupId);

            Function<ReplicaMeta, ReplicaRequest> requestFactory = replicaMeta ->
                    TABLE_MESSAGES_FACTORY.getEstimatedSizeRequest()
                            .groupId(partitionIdMessage)
                            .tableId(tableId)
                            .enlistmentConsistencyToken(enlistmentConsistencyToken(replicaMeta))
                            .timestamp(now)
                            .build();

            invokeFutures[partId] = sendToPrimaryWithRetry(replicaGroupId, now, 5, requestFactory);
        }

        return allOf(invokeFutures)
                .thenApply(v -> Arrays.stream(invokeFutures).mapToLong(f -> (Long) f.join()).sum());
    }

    @Override
    public final ZonePartitionId targetReplicationGroupId(int partitionIndex) {
        return new ZonePartitionId(zoneId, partitionIndex);
    }

    private static ZonePartitionIdMessage serializeReplicationGroupId(ZonePartitionId replicationGroupId) {
        return toZonePartitionIdMessage(REPLICA_MESSAGES_FACTORY, replicationGroupId);
    }

    @Override
    public StreamerReceiverRunner streamerReceiverRunner() {
        return streamerReceiverRunner;
    }

    private <T> CompletableFuture<T> sendToPrimaryWithRetry(
            ZonePartitionId replicationGroupId,
            HybridTimestamp hybridTimestamp,
            int numRetries,
            Function<ReplicaMeta, ReplicaRequest> requestFactory
    ) {
        return awaitPrimaryReplica(replicationGroupId, hybridTimestamp)
                .thenCompose(replicaMeta -> {
                    String leaseHolderName = replicaMeta.getLeaseholder();

                    assert leaseHolderName != null;

                    return replicaSvc.<T>invoke(leaseHolderName, requestFactory.apply(replicaMeta))
                            .handle((response, e) -> {
                                if (e == null) {
                                    return completedFuture(response);
                                }

                                e = unwrapCause(e);

                                if (e instanceof ReplicationException && e.getCause() != null) {
                                    e = e.getCause();
                                }

                                // We do a retry for the following conditions:
                                // 1. Primary Replica has changed between the "awaitPrimaryReplica" and "invoke" calls;
                                // 2. Primary Replica has died and is no longer available.
                                // In both cases, we need to wait for the lease to expire and to get a new Primary Replica.
                                if (e instanceof PrimaryReplicaMissException
                                        || e instanceof UnresolvableConsistentIdException
                                        || e instanceof ReplicationTimeoutException) {
                                    if (numRetries == 0) {
                                        throw new IgniteException(REPLICA_MISS_ERR, e);
                                    }

                                    // Primary Replica has changed, need to wait for the new replica to appear.
                                    return this.<T>sendToPrimaryWithRetry(
                                            replicationGroupId,
                                            replicaMeta.getExpirationTime().tick(),
                                            numRetries - 1,
                                            requestFactory
                                    );
                                } else {
                                    throw new IgniteException(INTERNAL_ERR, e);
                                }
                            });
                })
                .thenCompose(identity());
    }

    private ReplicaRequest upsertAllInternal(
            Collection<? extends BinaryRow> keyRows0,
            InternalTransaction txo,
            ZonePartitionId groupId,
            Long enlistmentConsistencyToken,
            boolean full
    ) {
        return readWriteMultiRowReplicaRequest(RW_UPSERT_ALL, keyRows0, null, txo, groupId, enlistmentConsistencyToken, full);
    }

    private ReplicaRequest upsertAllInternal(
            Collection<? extends BinaryRow> keyRows0,
            @Nullable BitSet deleted,
            InternalTransaction txo,
            ZonePartitionId groupId,
            Long enlistmentConsistencyToken,
            boolean full
    ) {
        return readWriteMultiRowReplicaRequest(
                RW_UPSERT_ALL, keyRows0, deleted, txo, groupId, enlistmentConsistencyToken, full);
    }

    /**
     * Ensure that the exception allows you to restart a transaction.
     *
     * @param e Exception to check.
     * @return True if retrying is possible, false otherwise.
     */
    private static boolean exceptionAllowsImplicitTxRetry(Throwable e) {
        return matchAny(
                unwrapCause(e),
                ACQUIRE_LOCK_ERR,
                GROUP_OVERLOADED_ERR,
                REPLICA_MISS_ERR,
                REPLICA_UNAVAILABLE_ERR,
                REPLICA_ABSENT_ERR,
                PRIMARY_REPLICA_AWAIT_ERR,
                PRIMARY_REPLICA_AWAIT_TIMEOUT_ERR
        );
    }

    private CompletableFuture<ReplicaMeta> awaitPrimaryReplica(ZonePartitionId replicationGroupId, HybridTimestamp timestamp) {
        return placementDriver.awaitPrimaryReplica(
                replicationGroupId,
                timestamp,
                AWAIT_PRIMARY_REPLICA_TIMEOUT,
                SECONDS
        );
    }

    private static long enlistmentConsistencyToken(ReplicaMeta replicaMeta) {
        return replicaMeta.getStartTime().longValue();
    }

    private void checkTransactionFinishStarted(@Nullable InternalTransaction transaction) {
        if (transaction != null && transaction.isFinishingOrFinished()) {
            boolean isFinishedDueToTimeout = transaction.isRolledBackWithTimeoutExceeded();
            Throwable cause = lastException(transaction.id());
            throw new TransactionException(
                    isFinishedDueToTimeout ? TX_ALREADY_FINISHED_WITH_TIMEOUT_ERR : TX_ALREADY_FINISHED_ERR,
                    format("Transaction is already finished or finishing [{}, readOnly={}].",
                            formatTxInfo(transaction.id(), txManager, false),
                            transaction.isReadOnly()
                    ),
                    cause
            );
        }
    }

    @Nullable
    private Throwable lastException(UUID txId) {
        TxStateMeta txStateMeta = txManager.stateMeta(txId);
        if (txStateMeta == null) {
            return null;
        }

        return txStateMeta.lastException();
    }

    @FunctionalInterface
    private interface ReplicaRequestFactory {
        ReplicaRequest create(
                Collection<? extends BinaryRow> keyRows,
                InternalTransaction tx,
                ZonePartitionId groupId,
                Long enlistmentConsistencyToken,
                Boolean full
        );
    }

    @Override
    public ReadWriteMetricSource metrics() {
        return metrics;
    }
}
