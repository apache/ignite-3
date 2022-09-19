/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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

import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.CompletableFuture.failedFuture;

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Flow.Publisher;
import java.util.concurrent.Flow.Subscriber;
import java.util.concurrent.Flow.Subscription;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.ignite.hlc.HybridClock;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.replicator.ReplicaService;
import org.apache.ignite.internal.replicator.exception.PrimaryReplicaMissException;
import org.apache.ignite.internal.replicator.message.ReplicaRequest;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.BinaryRowEx;
import org.apache.ignite.internal.storage.engine.MvTableStorage;
import org.apache.ignite.internal.table.InternalTable;
import org.apache.ignite.internal.table.distributed.TableMessagesFactory;
import org.apache.ignite.internal.table.distributed.replication.request.ReadWriteScanRetrieveBatchReplicaRequest;
import org.apache.ignite.internal.table.distributed.replication.request.ReadWriteScanRetrieveBatchReplicaRequestBuilder;
import org.apache.ignite.internal.table.distributed.replicator.action.RequestType;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.internal.tx.TxManager;
import org.apache.ignite.internal.tx.TxState;
import org.apache.ignite.lang.Function3;
import org.apache.ignite.lang.Function4;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lang.IgniteInternalException;
import org.apache.ignite.lang.IgniteStringFormatter;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.NetworkAddress;
import org.apache.ignite.raft.client.Peer;
import org.apache.ignite.raft.client.service.RaftGroupService;
import org.apache.ignite.tx.TransactionException;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

/**
 * Storage of table rows.
 */
public class InternalTableImpl implements InternalTable {
    /** Log. */
    private static final IgniteLogger LOG = Loggers.forClass(InternalTableImpl.class);

    /** Cursor id generator. */
    private static final AtomicLong CURSOR_ID_GENERATOR = new AtomicLong();

    /** Number of attempts. */
    private static final int ATTEMPTS_TO_ENLIST_PARTITION = 5;

    /** Partition map. */
    protected final Int2ObjectMap<RaftGroupService> partitionMap;

    /** Partitions. */
    private final int partitions;

    /** Table name. */
    private final String tableName;

    /** Table identifier. */
    private final UUID tableId;

    /** Resolver that resolves a network address to node id. */
    private final Function<NetworkAddress, String> netAddrResolver;

    /** Resolver that resolves a network address to cluster node. */
    private final Function<NetworkAddress, ClusterNode> clusterNodeResolver;

    /** Transactional manager. */
    private final TxManager txManager;

    /** Storage for table data. */
    private final MvTableStorage tableStorage;

    /** Replica service. */
    protected final ReplicaService replicaSvc;

    /** Mutex for the partition map update. */
    public Object updatePartMapMux = new Object();

    /** Table messages factory. */
    private final TableMessagesFactory tableMessagesFactory;

    /** A hybrid logical clock. */
    private final HybridClock clock;

    /**
     * Constructor.
     *
     * @param tableName Table name.
     * @param tableId Table id.
     * @param partMap Map partition id to raft group.
     * @param partitions Partitions.
     * @param txManager Transaction manager.
     * @param tableStorage Table storage.
     * @param replicaSvc Replica service.
     * @param clock A hybrid logical clock.
     */
    public InternalTableImpl(
            String tableName,
            UUID tableId,
            Int2ObjectMap<RaftGroupService> partMap,
            int partitions,
            Function<NetworkAddress, String> netAddrResolver,
            Function<NetworkAddress, ClusterNode> clusterNodeResolver,
            TxManager txManager,
            MvTableStorage tableStorage,
            ReplicaService replicaSvc,
            HybridClock clock
    ) {
        this.tableName = tableName;
        this.tableId = tableId;
        this.partitionMap = partMap;
        this.partitions = partitions;
        this.netAddrResolver = netAddrResolver;
        this.clusterNodeResolver = clusterNodeResolver;
        this.txManager = txManager;
        this.tableStorage = tableStorage;
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
    public UUID tableId() {
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
            InternalTransaction tx,
            Function3<InternalTransaction, String, Long, ReplicaRequest> op
    ) {
        final boolean implicit = tx == null;

        final InternalTransaction tx0 = implicit ? txManager.begin() : tx;

        int partId = partId(row);

        String partGroupId = partitionMap.get(partId).groupId();

        IgniteBiTuple<ClusterNode, Long> primaryReplicaAndTerm = tx0.enlistedNodeAndTerm(partGroupId);

        CompletableFuture<R> fut;

        if (primaryReplicaAndTerm != null) {
            ReplicaRequest request = op.apply(tx0, partGroupId, primaryReplicaAndTerm.get2());

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
                    term -> op.apply(tx0, partGroupId, term),
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
            InternalTransaction tx,
            Function4<Collection<BinaryRow>, InternalTransaction, String, Long, ReplicaRequest> op,
            Function<CompletableFuture<Object>[], CompletableFuture<T>> reducer
    ) {
        final boolean implicit = tx == null;

        if (!implicit && tx.state() != null && tx.state() != TxState.PENDING) {
            return failedFuture(new TransactionException(
                    "The operation is attempted for completed transaction"));
        }

        final InternalTransaction tx0 = implicit ? txManager.begin() : tx;

        Int2ObjectOpenHashMap<List<BinaryRow>> keyRowsByPartition = mapRowsToPartitions(keyRows);

        CompletableFuture<Object>[] futures = new CompletableFuture[keyRowsByPartition.size()];

        int batchNum = 0;

        for (Int2ObjectOpenHashMap.Entry<List<BinaryRow>> partToRows : keyRowsByPartition.int2ObjectEntrySet()) {
            String partGroupId = partitionMap.get(partToRows.getIntKey()).groupId();

            IgniteBiTuple<ClusterNode, Long> primaryReplicaAndTerm = tx0.enlistedNodeAndTerm(partGroupId);

            CompletableFuture<Object> fut;

            if (primaryReplicaAndTerm != null) {
                ReplicaRequest request = op.apply(partToRows.getValue(), tx0, partGroupId, primaryReplicaAndTerm.get2());

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
                        partToRows.getIntKey(),
                        term -> op.apply(partToRows.getValue(), tx0, partGroupId, term),
                        ATTEMPTS_TO_ENLIST_PARTITION
                );
            }

            futures[batchNum++] = fut;
        }

        CompletableFuture<T> fut = reducer.apply(futures);

        return postEnlist(fut, implicit, tx0);
    }

    /**
     * Retrieves a batch rows from replication storage.
     *
     * @param tx Internal transaction.
     * @param partId Partition number.
     * @param scanId Scan id.
     * @param batchSize Size of batch.
     * @return Batch of retrieved rows.
     */
    private CompletableFuture<Collection<BinaryRow>> enlistCursorInTx(
            @NotNull InternalTransaction tx,
            int partId,
            long scanId,
            int batchSize
    ) {
        String partGroupId = partitionMap.get(partId).groupId();

        IgniteBiTuple<ClusterNode, Long> primaryReplicaAndTerm = tx.enlistedNodeAndTerm(partGroupId);

        CompletableFuture<Collection<BinaryRow>> fut;

        ReadWriteScanRetrieveBatchReplicaRequestBuilder requestBuilder = tableMessagesFactory.readWriteScanRetrieveBatchReplicaRequest()
                .groupId(partGroupId)
                .transactionId(tx.id())
                .scanId(scanId)
                .batchSize(batchSize)
                .timestamp(clock.now());

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
            fut = enlistWithRetry(tx, partId, term -> requestBuilder.term(term).build(), ATTEMPTS_TO_ENLIST_PARTITION);
        }

        return postEnlist(fut, false, tx);
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
            Function<Long, ReplicaRequest> requestFunction,
            int attempts
    ) {
        CompletableFuture<R> result = new CompletableFuture();

        enlist(partId, tx).<R>thenCompose(
                        primaryReplicaAndTerm -> {
                            try {
                                return replicaSvc.invoke(
                                        primaryReplicaAndTerm.get1(),
                                        requestFunction.apply(primaryReplicaAndTerm.get2())
                                );
                            } catch (PrimaryReplicaMissException e) {
                                throw new TransactionException(e);
                            } catch (Throwable e) {
                                throw new TransactionException(
                                        IgniteStringFormatter.format(
                                                "Failed to enlist partition[tableName={}, partId={}] into a transaction",
                                                tableName,
                                                partId
                                                )
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
        return fut.handle(new BiFunction<T, Throwable, CompletableFuture<T>>() {
            @Override
            public CompletableFuture<T> apply(T r, Throwable e) {
                if (e != null) {
                    return tx0.rollbackAsync().handle((ignored, err) -> {
                        if (err != null) {
                            e.addSuppressed(err);
                        }

                        throw (RuntimeException) e;
                    }); // Preserve failed state.
                } else {
                    tx0.enlistResultFuture(fut);

                    return implicit ? tx0.commitAsync().thenApply(ignored -> r) : completedFuture(r);
                }
            }
        }).thenCompose(x -> x);
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<BinaryRow> get(BinaryRowEx keyRow, InternalTransaction tx) {
        return enlistInTx(
                keyRow,
                tx,
                (txo, groupId, term) -> tableMessagesFactory.readWriteSingleRowReplicaRequest()
                        .groupId(groupId)
                        .binaryRow(keyRow)
                        .transactionId(txo.id())
                        .term(term)
                        .requestType(RequestType.RW_GET)
                        .timestamp(clock.now())
                        .build()
        );
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Collection<BinaryRow>> getAll(Collection<BinaryRowEx> keyRows, InternalTransaction tx) {
        return enlistInTx(
                keyRows,
                tx,
                (keyRows0, txo, groupId, term) -> tableMessagesFactory.readWriteMultiRowReplicaRequest()
                        .groupId(groupId)
                        .binaryRows(keyRows0)
                        .transactionId(txo.id())
                        .term(term)
                        .requestType(RequestType.RW_GET_ALL)
                        .timestamp(clock.now())
                        .build(),
                this::collectMultiRowsResponses);
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Void> upsert(BinaryRowEx row, InternalTransaction tx) {
        return enlistInTx(
                row,
                tx,
                (txo, groupId, term) -> tableMessagesFactory.readWriteSingleRowReplicaRequest()
                        .groupId(groupId)
                        .binaryRow(row)
                        .transactionId(txo.id())
                        .term(term)
                        .requestType(RequestType.RW_UPSERT)
                        .timestamp(clock.now())
                        .build());
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Void> upsertAll(Collection<BinaryRowEx> rows, InternalTransaction tx) {
        return enlistInTx(
                rows,
                tx,
                (keyRows0, txo, groupId, term) -> tableMessagesFactory.readWriteMultiRowReplicaRequest()
                        .groupId(groupId)
                        .binaryRows(keyRows0)
                        .transactionId(txo.id())
                        .term(term)
                        .requestType(RequestType.RW_UPSERT_ALL)
                        .timestamp(clock.now())
                        .build(),
                CompletableFuture::allOf);
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<BinaryRow> getAndUpsert(BinaryRowEx row, InternalTransaction tx) {
        return enlistInTx(
                row,
                tx,
                (txo, groupId, term) -> tableMessagesFactory.readWriteSingleRowReplicaRequest()
                        .groupId(groupId)
                        .binaryRow(row)
                        .transactionId(txo.id())
                        .term(term)
                        .requestType(RequestType.RW_GET_AND_UPSERT)
                        .timestamp(clock.now())
                        .build()
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
                        .binaryRow(row)
                        .transactionId(txo.id())
                        .term(term)
                        .requestType(RequestType.RW_INSERT)
                        .timestamp(clock.now())
                        .build()
        );
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Collection<BinaryRow>> insertAll(Collection<BinaryRowEx> rows, InternalTransaction tx) {
        return enlistInTx(
                rows,
                tx,
                (keyRows0, txo, groupId, term) -> tableMessagesFactory.readWriteMultiRowReplicaRequest()
                        .groupId(groupId)
                        .binaryRows(keyRows0)
                        .transactionId(txo.id())
                        .term(term)
                        .requestType(RequestType.RW_INSERT_ALL)
                        .timestamp(clock.now())
                        .build(),
                this::collectMultiRowsResponses);
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Boolean> replace(BinaryRowEx row, InternalTransaction tx) {
        return enlistInTx(
                row,
                tx,
                (txo, groupId, term) -> tableMessagesFactory.readWriteSingleRowReplicaRequest()
                        .groupId(groupId)
                        .binaryRow(row)
                        .transactionId(txo.id())
                        .term(term)
                        .requestType(RequestType.RW_REPLACE_IF_EXIST)
                        .timestamp(clock.now())
                        .build()
        );
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Boolean> replace(BinaryRowEx oldRow, BinaryRowEx newRow, InternalTransaction tx) {
        return enlistInTx(
                newRow,
                tx,
                (txo, groupId, term) -> tableMessagesFactory.readWriteSwapRowReplicaRequest()
                        .groupId(groupId)
                        .oldBinaryRow(oldRow)
                        .binaryRow(newRow)
                        .transactionId(txo.id())
                        .term(term)
                        .requestType(RequestType.RW_REPLACE)
                        .timestamp(clock.now())
                        .build()
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
                        .binaryRow(row)
                        .transactionId(txo.id())
                        .term(term)
                        .requestType(RequestType.RW_GET_AND_REPLACE)
                        .timestamp(clock.now())
                        .build()
        );
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Boolean> delete(BinaryRowEx keyRow, InternalTransaction tx) {
        return enlistInTx(
                keyRow,
                tx,
                (txo, groupId, term) -> tableMessagesFactory.readWriteSingleRowReplicaRequest()
                        .groupId(groupId)
                        .binaryRow(keyRow)
                        .transactionId(txo.id())
                        .term(term)
                        .requestType(RequestType.RW_DELETE)
                        .timestamp(clock.now())
                        .build()
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
                        .binaryRow(oldRow)
                        .transactionId(txo.id())
                        .term(term)
                        .requestType(RequestType.RW_DELETE_EXACT)
                        .timestamp(clock.now())
                        .build()
        );
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<BinaryRow> getAndDelete(BinaryRowEx row, InternalTransaction tx) {
        return enlistInTx(
                row,
                tx,
                (txo, groupId, term) -> tableMessagesFactory.readWriteSingleRowReplicaRequest()
                        .groupId(groupId)
                        .binaryRow(row)
                        .transactionId(txo.id())
                        .term(term)
                        .requestType(RequestType.RW_GET_AND_DELETE)
                        .timestamp(clock.now())
                        .build()
        );
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Collection<BinaryRow>> deleteAll(Collection<BinaryRowEx> rows, InternalTransaction tx) {
        return enlistInTx(
                rows,
                tx,
                (keyRows0, txo, groupId, term) -> tableMessagesFactory.readWriteMultiRowReplicaRequest()
                        .groupId(groupId)
                        .binaryRows(keyRows0)
                        .transactionId(txo.id())
                        .term(term)
                        .requestType(RequestType.RW_DELETE_ALL)
                        .timestamp(clock.now())
                        .build(),
                this::collectMultiRowsResponses);
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
                (keyRows0, txo, groupId, term) -> tableMessagesFactory.readWriteMultiRowReplicaRequest()
                        .groupId(groupId)
                        .binaryRows(keyRows0)
                        .transactionId(txo.id())
                        .term(term)
                        .requestType(RequestType.RW_DELETE_EXACT_ALL)
                        .timestamp(clock.now())
                        .build(),
                this::collectMultiRowsResponses);
    }

    /** {@inheritDoc} */
    @Override
    public Publisher<BinaryRow> scan(int p, @Nullable InternalTransaction tx) {
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

        final boolean implicit = tx == null;

        final InternalTransaction tx0 = implicit ? txManager.begin() : tx;

        return new PartitionScanPublisher(
                (scanId, batchSize) -> enlistCursorInTx(tx0, p, scanId, batchSize),
                fut -> postEnlist(fut, implicit, tx0)
        );
    }

    /**
     * Map rows to partitions.
     *
     * @param rows Rows.
     * @return Partition -%gt; rows mapping.
     */
    private Int2ObjectOpenHashMap<List<BinaryRow>> mapRowsToPartitions(Collection<BinaryRowEx> rows) {
        Int2ObjectOpenHashMap<List<BinaryRow>> keyRowsByPartition = new Int2ObjectOpenHashMap<>();

        for (BinaryRowEx keyRow : rows) {
            keyRowsByPartition.computeIfAbsent(partId(keyRow), k -> new ArrayList<>()).add(keyRow);
        }

        return keyRowsByPartition;
    }

    // placement driver
    /** {@inheritDoc} */
    @Override
    public List<String> assignments() {
        awaitLeaderInitialization();

        return partitionMap.int2ObjectEntrySet().stream()
                .sorted(Comparator.comparingInt(Int2ObjectOpenHashMap.Entry::getIntKey))
                .map(Map.Entry::getValue)
                .map(RaftGroupService::leader)
                .map(Peer::address)
                .map(netAddrResolver)
                .collect(Collectors.toList());
    }

    @Override
    public ClusterNode leaderAssignment(int partition) {
        awaitLeaderInitialization();

        RaftGroupService raftGroupService = partitionMap.get(partition);
        if (raftGroupService == null) {
            throw new IgniteInternalException("No such partition " + partition + " in table " + tableName);
        }

        return clusterNodeResolver.apply(raftGroupService.leader().address());
    }

    /** {@inheritDoc} */
    @Override
    public RaftGroupService partitionRaftGroupService(int partition) {
        RaftGroupService raftGroupService = partitionMap.get(partition);
        if (raftGroupService == null) {
            throw new IgniteInternalException("No such partition " + partition + " in table " + tableName);
        }

        if (raftGroupService.leader() == null) {
            raftGroupService.refreshLeader().join();
        }

        return raftGroupService;
    }

    private void awaitLeaderInitialization() {
        List<CompletableFuture<Void>> futs = new ArrayList<>();

        for (RaftGroupService raftSvc : partitionMap.values()) {
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
        return partId(keyRow);
    }

    /**
     * Get partition id by key row.
     *
     * @param row Key row.
     * @return partition id.
     */
    private int partId(BinaryRowEx row) {
        int partId = row.colocationHash() % partitions;

        return (partId < 0) ? -partId : partId;
    }

    /**
     * TODO asch keep the same order as for keys Collects multirow responses from multiple futures into a single collection IGNITE-16004.
     *
     * @param futs Futures.
     * @return Row collection.
     */
    private CompletableFuture<Collection<BinaryRow>> collectMultiRowsResponses(CompletableFuture<Object>[] futs) {
        return CompletableFuture.allOf(futs)
                .thenApply(response -> {
                    Collection<BinaryRow> list = new ArrayList<>(futs.length);

                    for (CompletableFuture<Object> future : futs) {
                        Collection<BinaryRow> values = (Collection<BinaryRow>) future.join();

                        if (values != null) {
                            list.addAll(values);
                        }
                    }

                    return list;
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

        synchronized (updatePartMapMux) {
            oldSrvc = partitionMap.put(p, raftGrpSvc);
        }

        if (oldSrvc != null) {
            oldSrvc.shutdown();
        }
    }

    /**
     * Enlists a partition.
     *
     * @param partId Partition id.
     * @param tx     The transaction.
     * @return The enlist future (then will a leader become known).
     */
    protected CompletableFuture<IgniteBiTuple<ClusterNode, Long>> enlist(int partId, InternalTransaction tx) {
        RaftGroupService svc = partitionMap.get(partId);

        // TODO: ticket for placement driver
        CompletableFuture<IgniteBiTuple<Peer, Long>> fut0 = svc.refreshAndGetLeaderWithTerm();

        // TODO asch IGNITE-15091 fixme need to map to the same leaseholder.
        // TODO asch a leader race is possible when enlisting different keys from the same partition.
        // TODO: not sure whether we should use clusterNode or peer as replicaService.ingoke parameter.
        return fut0.handle((primaryPeerAndTerm, e) -> {
            if (primaryPeerAndTerm.get1() == null || e != null) {
                throw new TransactionException("Failed to get the primary replica.");
            }

            return tx.enlist(svc.groupId(),
                    new IgniteBiTuple<>(clusterNodeResolver.apply(primaryPeerAndTerm.get1().address()), primaryPeerAndTerm.get2()));
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
         * @param onClose The closure will be applied when {@link Subscription#cancel} is invoked directly or the cursor is finished.
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

                onClose.apply(t == null ? completedFuture(null) : CompletableFuture.failedFuture(t)).handle((ignore, th) -> {
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
                        binaryRows.forEach(subscriber::onNext);
                    }

                    if (binaryRows.size() < n) {
                        cancel();
                    } else if (requestedItemsCnt.addAndGet(Math.negateExact(binaryRows.size())) > 0) {
                        scanBatch(INTERNAL_BATCH_SIZE);
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
    public void close() throws Exception {
        for (RaftGroupService srv : partitionMap.values()) {
            srv.shutdown();
        }
    }
}
