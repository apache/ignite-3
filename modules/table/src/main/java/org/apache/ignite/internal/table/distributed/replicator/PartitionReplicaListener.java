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

package org.apache.ignite.internal.table.distributed.replicator;

import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.allOf;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.CompletableFuture.failedFuture;
import static org.apache.ignite.internal.util.CollectionUtils.nullOrEmpty;
import static org.apache.ignite.lang.IgniteStringFormatter.format;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.replicator.ReplicationGroupId;
import org.apache.ignite.internal.replicator.command.SafeTimeSyncCommand;
import org.apache.ignite.internal.replicator.exception.PrimaryReplicaMissException;
import org.apache.ignite.internal.replicator.exception.ReplicationException;
import org.apache.ignite.internal.replicator.exception.ReplicationTimeoutException;
import org.apache.ignite.internal.replicator.exception.UnsupportedReplicaRequestException;
import org.apache.ignite.internal.replicator.listener.ReplicaListener;
import org.apache.ignite.internal.replicator.message.ReplicaRequest;
import org.apache.ignite.internal.replicator.message.ReplicaSafeTimeSyncRequest;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.storage.MvPartitionStorage;
import org.apache.ignite.internal.storage.PartitionTimestampCursor;
import org.apache.ignite.internal.storage.ReadResult;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.table.distributed.IndexLocker;
import org.apache.ignite.internal.table.distributed.TableSchemaAwareIndexStorage;
import org.apache.ignite.internal.table.distributed.command.FinishTxCommand;
import org.apache.ignite.internal.table.distributed.command.TxCleanupCommand;
import org.apache.ignite.internal.table.distributed.command.UpdateAllCommand;
import org.apache.ignite.internal.table.distributed.command.UpdateCommand;
import org.apache.ignite.internal.table.distributed.replication.request.ReadOnlyMultiRowReplicaRequest;
import org.apache.ignite.internal.table.distributed.replication.request.ReadOnlyReplicaRequest;
import org.apache.ignite.internal.table.distributed.replication.request.ReadOnlyScanRetrieveBatchReplicaRequest;
import org.apache.ignite.internal.table.distributed.replication.request.ReadOnlySingleRowReplicaRequest;
import org.apache.ignite.internal.table.distributed.replication.request.ReadWriteMultiRowReplicaRequest;
import org.apache.ignite.internal.table.distributed.replication.request.ReadWriteReplicaRequest;
import org.apache.ignite.internal.table.distributed.replication.request.ReadWriteScanCloseReplicaRequest;
import org.apache.ignite.internal.table.distributed.replication.request.ReadWriteScanRetrieveBatchReplicaRequest;
import org.apache.ignite.internal.table.distributed.replication.request.ReadWriteSingleRowReplicaRequest;
import org.apache.ignite.internal.table.distributed.replication.request.ReadWriteSwapRowReplicaRequest;
import org.apache.ignite.internal.table.distributed.replicator.action.RequestType;
import org.apache.ignite.internal.tx.LockKey;
import org.apache.ignite.internal.tx.LockManager;
import org.apache.ignite.internal.tx.LockMode;
import org.apache.ignite.internal.tx.TxManager;
import org.apache.ignite.internal.tx.TxMeta;
import org.apache.ignite.internal.tx.TxState;
import org.apache.ignite.internal.tx.message.TxCleanupReplicaRequest;
import org.apache.ignite.internal.tx.message.TxFinishReplicaRequest;
import org.apache.ignite.internal.tx.message.TxMessagesFactory;
import org.apache.ignite.internal.tx.message.TxStateReplicaRequest;
import org.apache.ignite.internal.tx.storage.state.TxStateStorage;
import org.apache.ignite.internal.util.Cursor;
import org.apache.ignite.internal.util.Lazy;
import org.apache.ignite.internal.util.PendingComparableValuesTracker;
import org.apache.ignite.lang.ErrorGroups.Replicator;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lang.IgniteInternalException;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.network.NetworkAddress;
import org.apache.ignite.network.TopologyService;
import org.apache.ignite.raft.client.Command;
import org.apache.ignite.raft.client.Peer;
import org.apache.ignite.raft.client.service.RaftGroupService;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/** Partition replication listener. */
public class PartitionReplicaListener implements ReplicaListener {
    /** Tx messages factory. */
    private static final TxMessagesFactory FACTORY = new TxMessagesFactory();

    /** Replication group id. */
    private final TablePartitionId replicationGroupId;

    /** Partition id. */
    private final int partId;

    /** Primary key index. */
    public final Lazy<TableSchemaAwareIndexStorage> pkIndexStorage;

    /** Table id. */
    private final UUID tableId;

    /** Versioned partition storage. */
    private final MvPartitionStorage mvDataStorage;

    /** Raft client. */
    private final RaftGroupService raftClient;

    /** Tx manager. */
    private final TxManager txManager;

    /** Lock manager. */
    private final LockManager lockManager;

    /**
     * Cursors map. The key of the map is internal Ignite uuid which consists of a transaction id ({@link UUID}) and a cursor id ({@link
     * Long}).
     */
    private final ConcurrentNavigableMap<IgniteUuid, PartitionTimestampCursor> cursors;

    /** Tx state storage. */
    private final TxStateStorage txStateStorage;

    /** Topology service. */
    private final TopologyService topologyService;

    /** Hybrid clock. */
    private final HybridClock hybridClock;

    /** Safe time. */
    private final PendingComparableValuesTracker<HybridTimestamp> safeTime;

    /** Placement Driver. */
    private final PlacementDriver placementDriver;

    /**
     * Map to control clock's update in the read only transactions concurrently with a commit timestamp.
     * TODO: IGNITE-17261 review this after the commit timestamp will be provided from a commit request (request.commitTimestamp()).
     */
    private final ConcurrentHashMap<UUID, CompletableFuture<TxMeta>> txTimestampUpdateMap = new ConcurrentHashMap<>();

    private final Supplier<Map<UUID, IndexLocker>> indexesLockers;

    /**
     * Function for checking that the given peer is local.
     */
    private final Function<Peer, Boolean> isLocalPeerChecker;

    /**
     * The constructor.
     *
     * @param mvDataStorage Data storage.
     * @param raftClient Raft client.
     * @param txManager Transaction manager.
     * @param lockManager Lock manager.
     * @param partId Partition id.
     * @param tableId Table id.
     * @param hybridClock Hybrid clock.
     * @param safeTime Safe time clock.
     * @param txStateStorage Transaction state storage.
     * @param topologyService Topology services.
     * @param placementDriver Placement driver.
     * @param isLocalPeerChecker Function for checking that the given peer is local.
     */
    public PartitionReplicaListener(
            MvPartitionStorage mvDataStorage,
            RaftGroupService raftClient,
            TxManager txManager,
            LockManager lockManager,
            int partId,
            UUID tableId,
            Supplier<Map<UUID, IndexLocker>> indexesLockers,
            Lazy<TableSchemaAwareIndexStorage> pkIndexStorage,
            HybridClock hybridClock,
            PendingComparableValuesTracker<HybridTimestamp> safeTime,
            TxStateStorage txStateStorage,
            TopologyService topologyService,
            PlacementDriver placementDriver,
            Function<Peer, Boolean> isLocalPeerChecker
    ) {
        this.mvDataStorage = mvDataStorage;
        this.raftClient = raftClient;
        this.txManager = txManager;
        this.lockManager = lockManager;
        this.partId = partId;
        this.tableId = tableId;
        this.indexesLockers = indexesLockers;
        this.pkIndexStorage = pkIndexStorage;
        this.hybridClock = hybridClock;
        this.safeTime = safeTime;
        this.txStateStorage = txStateStorage;
        this.topologyService = topologyService;
        this.placementDriver = placementDriver;
        this.isLocalPeerChecker = isLocalPeerChecker;

        this.replicationGroupId = new TablePartitionId(tableId, partId);

        cursors = new ConcurrentSkipListMap<>((o1, o2) -> {
            if (o1 == o2) {
                return 0;
            }

            int res = o1.globalId().compareTo(o2.globalId());

            if (res == 0) {
                res = Long.compare(o1.localId(), o2.localId());
            }

            return res;
        });
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Object> invoke(ReplicaRequest request) {
        if (request instanceof TxStateReplicaRequest) {
            return processTxStateReplicaRequest((TxStateReplicaRequest) request);
        }

        return ensureReplicaIsPrimary(request)
                .thenCompose((isPrimary) -> {
                    if (request instanceof ReadWriteSingleRowReplicaRequest) {
                        return processSingleEntryAction((ReadWriteSingleRowReplicaRequest) request);
                    } else if (request instanceof ReadWriteMultiRowReplicaRequest) {
                        return processMultiEntryAction((ReadWriteMultiRowReplicaRequest) request);
                    } else if (request instanceof ReadWriteSwapRowReplicaRequest) {
                        return processTwoEntriesAction((ReadWriteSwapRowReplicaRequest) request)
                                .thenApply(Function.identity());
                    } else if (request instanceof ReadWriteScanRetrieveBatchReplicaRequest) {
                        return processScanRetrieveBatchAction((ReadWriteScanRetrieveBatchReplicaRequest) request)
                                .thenApply(Function.identity());
                    } else if (request instanceof ReadWriteScanCloseReplicaRequest) {
                        processScanCloseAction((ReadWriteScanCloseReplicaRequest) request);

                        return completedFuture(null);
                    } else if (request instanceof TxFinishReplicaRequest) {
                        return processTxFinishAction((TxFinishReplicaRequest) request)
                                .thenApply(Function.identity());
                    } else if (request instanceof TxCleanupReplicaRequest) {
                        return processTxCleanupAction((TxCleanupReplicaRequest) request)
                                .thenApply(Function.identity());
                    } else if (request instanceof ReadOnlySingleRowReplicaRequest) {
                        return processReadOnlySingleEntryAction((ReadOnlySingleRowReplicaRequest) request, isPrimary)
                                .thenApply(Function.identity());
                    } else if (request instanceof ReadOnlyMultiRowReplicaRequest) {
                        return processReadOnlyMultiEntryAction((ReadOnlyMultiRowReplicaRequest) request, isPrimary)
                                .thenApply(Function.identity());
                    } else if (request instanceof ReadOnlyScanRetrieveBatchReplicaRequest) {
                        return processReadOnlyScanRetrieveBatchAction((ReadOnlyScanRetrieveBatchReplicaRequest) request, isPrimary)
                                .thenApply(Function.identity());
                    } else if (request instanceof ReplicaSafeTimeSyncRequest) {
                        return processReplicaSafeTimeSyncRequest((ReplicaSafeTimeSyncRequest) request)
                                .thenApply(Function.identity());
                    } else {
                        throw new UnsupportedReplicaRequestException(request.getClass());
                    }
                });
    }

    /**
     * Processes a transaction state request.
     *
     * @param request Transaction state request.
     * @return Result future.
     */
    private CompletableFuture<Object> processTxStateReplicaRequest(TxStateReplicaRequest request) {
        return raftClient.refreshAndGetLeaderWithTerm()
                .thenCompose(replicaAndTerm -> {
                            NetworkAddress leaderAddress = replicaAndTerm.get1().address();

                            if (topologyService.localMember().address().equals(leaderAddress)) {

                                CompletableFuture<TxMeta> txStateFut = getTxStateConcurrently(request);

                                return txStateFut.thenApply(txMeta -> new IgniteBiTuple<>(txMeta, null));
                            } else {
                                return completedFuture(
                                        new IgniteBiTuple<>(null, topologyService.getByAddress(leaderAddress)));
                            }
                        }
                );
    }

    /**
     * Gets a transaction state or {@code null}, if the transaction is not completed.
     *
     * @param txStateReq Transaction state request.
     * @return Future to transaction state meta or {@code null}.
     */
    private CompletableFuture<TxMeta> getTxStateConcurrently(TxStateReplicaRequest txStateReq) {
        //TODO: IGNITE-17261 review this after the commit timestamp will be provided from a commit request (request.commitTimestamp()).
        CompletableFuture<TxMeta> txStateFut = new CompletableFuture<>();

        txTimestampUpdateMap.compute(txStateReq.txId(), (uuid, fut) -> {
            if (fut != null) {
                fut.thenAccept(txMeta -> txStateFut.complete(txMeta));
            } else {
                TxMeta txMeta = txStateStorage.get(txStateReq.txId());

                if (txMeta == null) {
                    // All future transactions will be committed after the resolution processed.
                    hybridClock.update(txStateReq.commitTimestamp());
                }

                txStateFut.complete(txMeta);
            }

            return null;
        });

        return txStateFut;
    }

    /**
     * Processes retrieve batch for read only transaction.
     *
     * @param request Read only retrieve batch request.
     * @param isPrimary Whether the given replica is primary.
     * @return Result future.
     */
    private CompletableFuture<ArrayList<BinaryRow>> processReadOnlyScanRetrieveBatchAction(
            ReadOnlyScanRetrieveBatchReplicaRequest request,
            Boolean isPrimary
    ) {
        requireNonNull(isPrimary);

        UUID txId = request.transactionId();
        int batchCount = request.batchSize();
        HybridTimestamp readTimestamp = request.readTimestamp();

        IgniteUuid cursorId = new IgniteUuid(txId, request.scanId());

        CompletableFuture<Void> safeReadFuture = isPrimary ? completedFuture(null) : safeTime.waitFor(readTimestamp);

        return safeReadFuture.thenCompose(unused -> retrieveExactEntriesUntilCursorEmpty(readTimestamp, cursorId, batchCount));
    }

    /**
     * Extracts exact amount of entries, or less if cursor is become empty, from a cursor on the specific time.
     *
     * @param readTimestamp Timestamp of the moment when that moment when the data will be extracted.
     * @param cursorId Cursor id.
     * @param count Amount of entries which sill be extracted.
     * @return Result future.
     */
    private CompletableFuture<ArrayList<BinaryRow>> retrieveExactEntriesUntilCursorEmpty(
            HybridTimestamp readTimestamp,
            IgniteUuid cursorId,
            int count
    ) {
        @SuppressWarnings("resource") PartitionTimestampCursor cursor = cursors.computeIfAbsent(cursorId,
                id -> mvDataStorage.scan(HybridTimestamp.MAX_VALUE));

        ArrayList<CompletableFuture<BinaryRow>> resolutionFuts = new ArrayList<>(count);

        while (resolutionFuts.size() < count && cursor.hasNext()) {
            ReadResult readResult = cursor.next();
            HybridTimestamp newestCommitTimestamp = readResult.newestCommitTimestamp();

            BinaryRow candidate =
                    newestCommitTimestamp == null || !readResult.isWriteIntent() ? null : cursor.committed(newestCommitTimestamp);

            resolutionFuts.add(resolveReadResult(readResult, readTimestamp, () -> candidate));
        }

        return allOf(resolutionFuts.toArray(new CompletableFuture[0])).thenCompose(unused -> {
            ArrayList<BinaryRow> rows = new ArrayList<>(count);

            for (CompletableFuture<BinaryRow> resolutionFut : resolutionFuts) {
                BinaryRow resolvedReadResult = resolutionFut.join();

                if (resolvedReadResult != null) {
                    rows.add(resolvedReadResult);
                }
            }

            if (rows.size() < count && cursor.hasNext()) {
                return retrieveExactEntriesUntilCursorEmpty(readTimestamp, cursorId, count - rows.size()).thenApply(binaryRows -> {
                    rows.addAll(binaryRows);

                    return rows;
                });
            } else {
                return completedFuture(rows);
            }
        });
    }

    /**
     * Processes single entry request for read only transaction.
     *
     * @param request Read only single entry request.
     * @param isPrimary Whether the given replica is primary.
     * @return Result future.
     */
    private CompletableFuture<BinaryRow> processReadOnlySingleEntryAction(ReadOnlySingleRowReplicaRequest request, Boolean isPrimary) {
        BinaryRow searchRow = request.binaryRow();
        HybridTimestamp readTimestamp = request.readTimestamp();

        if (request.requestType() != RequestType.RO_GET) {
            throw new IgniteInternalException(Replicator.REPLICA_COMMON_ERR,
                    format("Unknown single request [actionType={}]", request.requestType()));
        }

        CompletableFuture<Void> safeReadFuture = isPrimary ? completedFuture(null) : safeTime.waitFor(request.readTimestamp());

        return safeReadFuture.thenCompose(unused -> resolveRowByPk(searchRow, readTimestamp));
    }

    /**
     * Processes multiple entries request for read only transaction.
     *
     * @param request Read only multiple entries request.
     * @param isPrimary Whether the given replica is primary.
     * @return Result future.
     */
    private CompletableFuture<ArrayList<BinaryRow>> processReadOnlyMultiEntryAction(
            ReadOnlyMultiRowReplicaRequest request,
            Boolean isPrimary
    ) {
        Collection<BinaryRow> searchRows = request.binaryRows();
        HybridTimestamp readTimestamp = request.readTimestamp();

        if (request.requestType() != RequestType.RO_GET_ALL) {
            throw new IgniteInternalException(Replicator.REPLICA_COMMON_ERR,
                    format("Unknown single request [actionType={}]", request.requestType()));
        }

        CompletableFuture<Void> safeReadFuture = isPrimary ? completedFuture(null) : safeTime.waitFor(request.readTimestamp());

        return safeReadFuture.thenCompose(unused -> {
            ArrayList<CompletableFuture<BinaryRow>> resolutionFuts = new ArrayList<>(searchRows.size());

            for (BinaryRow searchRow : searchRows) {
                CompletableFuture<BinaryRow> fut = resolveRowByPk(searchRow, readTimestamp);

                resolutionFuts.add(fut);
            }

            return allOf(resolutionFuts.toArray(new CompletableFuture[0])).thenApply(unused1 -> {
                ArrayList<BinaryRow> result = new ArrayList<>(resolutionFuts.size());

                for (CompletableFuture<BinaryRow> resolutionFut : resolutionFuts) {
                    BinaryRow resolvedReadResult = resolutionFut.join();

                    if (resolvedReadResult != null) {
                        result.add(resolvedReadResult);
                    }
                }

                return result;
            });
        });
    }

    /**
     * Handler to process {@link ReplicaSafeTimeSyncRequest}.
     *
     * @param request Request.
     * @return Future.
     */
    private CompletionStage<Void> processReplicaSafeTimeSyncRequest(ReplicaSafeTimeSyncRequest request) {
        return raftClient.run(new SafeTimeSyncCommand());
    }

    /**
     * Close all cursors connected with a transaction.
     *
     * @param txId Transaction id.
     * @throws Exception When an issue happens on cursor closing.
     */
    private void closeAllTransactionCursors(UUID txId) {
        var lowCursorId = new IgniteUuid(txId, Long.MIN_VALUE);
        var upperCursorId = new IgniteUuid(txId, Long.MAX_VALUE);

        Map<IgniteUuid, PartitionTimestampCursor> txCursors = cursors.subMap(lowCursorId, true, upperCursorId, true);

        ReplicationException ex = null;

        for (PartitionTimestampCursor cursor : txCursors.values()) {
            try {
                cursor.close();
            } catch (Exception e) {
                if (ex == null) {
                    ex = new ReplicationException(Replicator.REPLICA_COMMON_ERR,
                            format("Close cursor exception [replicaGrpId={}, msg={}]", replicationGroupId,
                                    e.getMessage()), e);
                } else {
                    ex.addSuppressed(e);
                }
            }
        }

        txCursors.clear();

        if (ex != null) {
            throw ex;
        }
    }

    /**
     * Processes scan close request.
     *
     * @param request Scan close request operation.
     * @return Listener response.
     */
    private void processScanCloseAction(ReadWriteScanCloseReplicaRequest request) {
        UUID txId = request.transactionId();

        IgniteUuid cursorId = new IgniteUuid(txId, request.scanId());

        PartitionTimestampCursor cursor = cursors.remove(cursorId);

        if (cursor != null) {
            try {
                cursor.close();
            } catch (Exception e) {
                throw new ReplicationException(Replicator.REPLICA_COMMON_ERR,
                        format("Close cursor exception [replicaGrpId={}, msg={}]", replicationGroupId,
                                e.getMessage()), e);
            }
        }
    }

    /**
     * Processes scan retrieve batch request.
     *
     * @param request Scan retrieve batch request operation.
     * @return Listener response.
     */
    private CompletableFuture<ArrayList<BinaryRow>> processScanRetrieveBatchAction(ReadWriteScanRetrieveBatchReplicaRequest request) {
        UUID txId = request.transactionId();
        int batchCount = request.batchSize();

        IgniteUuid cursorId = new IgniteUuid(txId, request.scanId());

        return lockManager.acquire(txId, new LockKey(tableId), LockMode.S).thenCompose(tblLock -> {
            ArrayList<BinaryRow> batchRows = new ArrayList<>(batchCount);

            @SuppressWarnings("resource") PartitionTimestampCursor cursor = cursors.computeIfAbsent(cursorId,
                    id -> mvDataStorage.scan(HybridTimestamp.MAX_VALUE));

            while (batchRows.size() < batchCount && cursor.hasNext()) {
                BinaryRow resolvedReadResult = resolveReadResult(cursor.next(), txId);

                if (resolvedReadResult != null && resolvedReadResult.hasValue()) {
                    batchRows.add(resolvedReadResult);
                }
            }

            return completedFuture(batchRows);
        });
    }

    /**
     * Processes transaction finish request.
     * <ol>
     *     <li>Get commit timestamp from finish replica request.</li>
     *     <li>Run specific raft {@code FinishTxCommand} command, that will apply txn state to corresponding txStateStorage.</li>
     *     <li>Send cleanup requests to all enlisted primary replicas.</li>
     * </ol>
     *
     * @param request Transaction finish request.
     * @return future result of the operation.
     */
    // TODO: need to properly handle primary replica changes https://issues.apache.org/jira/browse/IGNITE-17615
    private CompletableFuture<Void> processTxFinishAction(TxFinishReplicaRequest request) {
        List<ReplicationGroupId> aggregatedGroupIds = request.groups().values().stream()
                .flatMap(List::stream).map(IgniteBiTuple::get1).collect(Collectors.toList());

        UUID txId = request.txId();

        boolean commit = request.commit();

        CompletableFuture<Object> changeStateFuture = finishTransaction(aggregatedGroupIds, txId, commit);

        // TODO: https://issues.apache.org/jira/browse/IGNITE-17578 Cleanup process should be asynchronous.
        CompletableFuture[] cleanupFutures = new CompletableFuture[request.groups().size()];
        AtomicInteger cleanupFuturesCnt = new AtomicInteger(0);

        request.groups().forEach(
                (recipientNode, replicationGroupIds) ->
                        cleanupFutures[cleanupFuturesCnt.getAndIncrement()] = changeStateFuture.thenCompose(ignored ->
                                txManager.cleanup(
                                        recipientNode,
                                        replicationGroupIds,
                                        txId,
                                        commit,
                                        request.commitTimestamp()
                                )
                        )
        );

        return allOf(cleanupFutures);
    }

    /**
     * Finishes a transaction.
     *
     * @param aggregatedGroupIds Replication groups identifies which are enlisted in the transaction.
     * @param txId Transaction id.
     * @param commit True is the transaction is committed, false otherwise.
     * @return Future to wait of the finish.
     */
    private CompletableFuture<Object> finishTransaction(List<ReplicationGroupId> aggregatedGroupIds, UUID txId, boolean commit) {
        // TODO: IGNITE-17261 Timestamp from request is not using until the issue has not been fixed (request.commitTimestamp())
        var fut = new CompletableFuture<TxMeta>();

        txTimestampUpdateMap.put(txId, fut);

        HybridTimestamp commitTimestamp =  commit ? hybridClock.now() : null;

        CompletableFuture<Object> changeStateFuture = raftClient.run(
                new FinishTxCommand(
                        txId,
                        commit,
                        commitTimestamp,
                        aggregatedGroupIds
                )
        ).whenComplete((o, throwable) -> {
            fut.complete(new TxMeta(commit ? TxState.COMMITED : TxState.ABORTED, aggregatedGroupIds, commitTimestamp));

            txTimestampUpdateMap.remove(txId);
        });

        return changeStateFuture;
    }


    /**
     * Processes transaction cleanup request:
     * <ol>
     *     <li>Run specific raft {@code TxCleanupCommand} command, that will convert all pending entries(writeIntents)
     *     to either regular values(TxState.COMMITED) or removing them (TxState.ABORTED).</li>
     *     <li>Release all locks that were held on local Replica by given transaction.</li>
     * </ol>
     * This operation is idempotent, so it's safe to retry it.
     *
     * @param request Transaction cleanup request.
     * @return CompletableFuture of void.
     */
    // TODO: need to properly handle primary replica changes https://issues.apache.org/jira/browse/IGNITE-17615
    private CompletableFuture<Void> processTxCleanupAction(TxCleanupReplicaRequest request) {
        try {
            closeAllTransactionCursors(request.txId());
        } catch (Exception e) {
            return failedFuture(e);
        }

        return raftClient
                .run(new TxCleanupCommand(request.txId(), request.commit(), request.commitTimestamp()))
                .thenRun(() -> lockManager.locks(request.txId()).forEachRemaining(lockManager::release));
    }

    /**
     * Finds the row and its identifier by given pk search row.
     *
     * @param tableRow A bytes representing a primary key.
     * @param txId An identifier of the transaction regarding which we need to resolve the given row.
     * @param action An action to perform on a resolved row.
     * @param <T> A type of the value returned by action.
     * @return A future object representing the result of the given action.
     */
    private <T> CompletableFuture<T> resolveRowByPk(
            BinaryRow tableRow,
            UUID txId,
            BiFunction<@Nullable RowId, @Nullable BinaryRow, CompletableFuture<T>> action
    ) {
        IndexLocker pkLocker = indexesLockers.get().get(pkIndexStorage.get().id());

        assert pkLocker != null;

        return pkLocker.locksForLookup(txId, tableRow)
                .thenCompose(ignored -> {
                    try (Cursor<RowId> cursor = pkIndexStorage.get().get(tableRow)) {
                        for (RowId rowId : cursor) {
                            BinaryRow row = resolveReadResult(mvDataStorage.read(rowId, HybridTimestamp.MAX_VALUE), txId);

                            if (row != null && row.hasValue()) {
                                return action.apply(rowId, row);
                            }
                        }

                        return action.apply(null, null);
                    } catch (Exception e) {
                        throw new IgniteInternalException(Replicator.REPLICA_COMMON_ERR,
                                format("Unable to close cursor [tableId={}]", tableId), e);
                    }
                });
    }

    /**
     * Finds the row and its identifier by given pk search row.
     *
     * @param searchKey A bytes representing a primary key.
     * @param ts A timestamp regarding which we need to resolve the given row.
     * @return Result of the given action.
     */
    private CompletableFuture<BinaryRow> resolveRowByPk(BinaryRow searchKey, HybridTimestamp ts) {
        try (Cursor<RowId> cursor = pkIndexStorage.get().get(searchKey)) {
            for (RowId rowId : cursor) {
                ReadResult readResult = mvDataStorage.read(rowId, ts);

                return resolveReadResult(readResult, ts, () -> {
                    HybridTimestamp newestCommitTimestamp = readResult.newestCommitTimestamp();

                    if (newestCommitTimestamp == null) {
                        return null;
                    }

                    ReadResult committedReadResult = mvDataStorage.read(rowId, newestCommitTimestamp);

                    assert !committedReadResult.isWriteIntent() :
                            "The result is not committed [rowId=" + rowId + ", timestamp="
                                    + newestCommitTimestamp + ']';

                    return committedReadResult.binaryRow();
                });
            }

            return completedFuture(null);
        } catch (Exception e) {
            throw new IgniteInternalException(Replicator.REPLICA_COMMON_ERR,
                    format("Unable to close cursor [tableId={}]", tableId), e);
        }
    }

    /**
     * Tests row values for equality.
     *
     * @param row  Row.
     * @param row2 Row.
     * @return Extracted key.
     */
    private boolean equalValues(@NotNull BinaryRow row, @NotNull BinaryRow row2) {
        if (row.hasValue() ^ row2.hasValue()) {
            return false;
        }

        return row.valueSlice().compareTo(row2.valueSlice()) == 0;
    }

    /**
     * Precesses multi request.
     *
     * @param request Multi request operation.
     * @return Listener response.
     */
    private CompletableFuture<Object> processMultiEntryAction(ReadWriteMultiRowReplicaRequest request) {
        UUID txId = request.transactionId();
        TablePartitionId committedPartitionId = request.commitPartitionId();

        assert committedPartitionId != null || request.requestType() == RequestType.RW_GET_ALL
                : "Commit partition partition is null [type=" + request.requestType() + ']';

        switch (request.requestType()) {
            case RW_GET_ALL: {
                CompletableFuture<BinaryRow>[] rowFuts = new CompletableFuture[request.binaryRows().size()];

                int i = 0;

                for (BinaryRow searchRow : request.binaryRows()) {
                    rowFuts[i++] = resolveRowByPk(searchRow, txId, (rowId, row) -> {
                        if (rowId == null) {
                            return completedFuture(null);
                        }

                        return takeLocksForGet(rowId, txId)
                                .thenApply(ignored -> row);
                    });
                }

                return allOf(rowFuts)
                        .thenCompose(ignored -> {
                            ArrayList<BinaryRow> result = new ArrayList<>(request.binaryRows().size());

                            for (int idx = 0; idx < request.binaryRows().size(); idx++) {
                                result.add(rowFuts[idx].join());
                            }

                            return completedFuture(result);
                        });
            }
            case RW_DELETE_ALL: {
                CompletableFuture<RowId>[] rowIdLockFuts = new CompletableFuture[request.binaryRows().size()];

                int i = 0;

                for (BinaryRow searchRow : request.binaryRows()) {
                    rowIdLockFuts[i++] = resolveRowByPk(searchRow, txId, (rowId, row) -> {
                        if (rowId == null) {
                            return completedFuture(null);
                        }

                        return takeLocksForDelete(searchRow, rowId, txId);
                    });
                }

                return allOf(rowIdLockFuts).thenCompose(ignore -> {
                    Collection<RowId> rowIdsToDelete = new ArrayList<>();
                    Collection<BinaryRow> result = new ArrayList<>();

                    int futNum = 0;

                    for (BinaryRow row : request.binaryRows()) {
                        RowId lockedRowId = rowIdLockFuts[futNum++].join();

                        if (lockedRowId != null) {
                            rowIdsToDelete.add(lockedRowId);
                        } else {
                            result.add(row);
                        }
                    }

                    if (rowIdsToDelete.isEmpty()) {
                        return completedFuture(result);
                    }

                    return applyCmdWithExceptionHandling(new UpdateAllCommand(committedPartitionId, rowIdsToDelete, txId))
                            .thenApply(ignored -> result);
                });
            }
            case RW_DELETE_EXACT_ALL: {
                CompletableFuture<RowId>[] deleteExactLockFuts = new CompletableFuture[request.binaryRows().size()];

                int i = 0;

                for (BinaryRow searchRow : request.binaryRows()) {
                    deleteExactLockFuts[i++] = resolveRowByPk(searchRow, txId, (rowId, row) -> {
                        if (rowId == null) {
                            return completedFuture(null);
                        }

                        return takeLocksForDeleteExact(searchRow, rowId, row, txId);
                    });
                }

                return allOf(deleteExactLockFuts).thenCompose(ignore -> {
                    Collection<RowId> rowIdsToDelete = new ArrayList<>();
                    Collection<BinaryRow> result = new ArrayList<>();

                    int futNum = 0;

                    for (BinaryRow row : request.binaryRows()) {
                        RowId lockedRowId = deleteExactLockFuts[futNum++].join();

                        if (lockedRowId != null) {
                            rowIdsToDelete.add(lockedRowId);
                        } else {
                            result.add(row);
                        }
                    }

                    CompletableFuture raftFut = rowIdsToDelete.isEmpty() ? completedFuture(null)
                            : applyCmdWithExceptionHandling(new UpdateAllCommand(committedPartitionId, rowIdsToDelete, txId));

                    return raftFut.thenApply(ignored -> result);
                });
            }
            case RW_INSERT_ALL: {
                CompletableFuture<RowId>[] pkReadLockFuts = new CompletableFuture[request.binaryRows().size()];

                int i = 0;

                for (BinaryRow searchRow : request.binaryRows()) {
                    pkReadLockFuts[i++] = resolveRowByPk(searchRow, txId,
                            (rowId, row) -> completedFuture(rowId));
                }

                return allOf(pkReadLockFuts).thenCompose(ignore -> {
                    Collection<BinaryRow> result = new ArrayList<>();
                    Map<RowId, BinaryRow> rowsToInsert = new HashMap<>();

                    int futNum = 0;

                    for (BinaryRow row : request.binaryRows()) {
                        RowId lockedRow = pkReadLockFuts[futNum++].join();

                        if (lockedRow != null) {
                            result.add(row);
                        } else {
                            if (rowsToInsert.values().stream().noneMatch(row0 -> row0.keySlice().equals(row.keySlice()))) {
                                rowsToInsert.put(new RowId(partId), row);
                            } else {
                                result.add(row);
                            }
                        }
                    }

                    if (rowsToInsert.isEmpty()) {
                        return completedFuture(result);
                    }

                    CompletableFuture<RowId>[] insertLockFuts = new CompletableFuture[rowsToInsert.size()];

                    int idx = 0;

                    for (Map.Entry<RowId, BinaryRow> entry : rowsToInsert.entrySet()) {
                        insertLockFuts[idx++] = takeLocksForInsert(entry.getValue(), entry.getKey(), txId);
                    }

                    return allOf(insertLockFuts)
                            .thenCompose(ignored -> applyCmdWithExceptionHandling(
                                    new UpdateAllCommand(committedPartitionId, rowsToInsert, txId)))
                            .thenApply(ignored -> result);
                });
            }
            case RW_UPSERT_ALL: {
                CompletableFuture<RowId>[] rowIdFuts = new CompletableFuture[request.binaryRows().size()];

                int i = 0;

                for (BinaryRow searchRow : request.binaryRows()) {
                    rowIdFuts[i++] = resolveRowByPk(searchRow, txId, (rowId, row) -> {
                        boolean insert = rowId == null;

                        RowId rowId0 = insert ? new RowId(partId) : rowId;

                        return insert
                                ? takeLocksForInsert(searchRow, rowId0, txId)
                                : takeLocksForUpdate(searchRow, rowId0, txId);
                    });
                }

                return allOf(rowIdFuts).thenCompose(ignore -> {
                    Map<RowId, BinaryRow> rowsToUpdate = new HashMap<>();

                    int futNum = 0;

                    for (BinaryRow row : request.binaryRows()) {
                        RowId lockedRow = rowIdFuts[futNum++].join();

                        rowsToUpdate.put(lockedRow, row);
                    }

                    if (rowsToUpdate.isEmpty()) {
                        return completedFuture(null);
                    }

                    return applyCmdWithExceptionHandling(new UpdateAllCommand(committedPartitionId, rowsToUpdate, txId))
                            .thenApply(ignored -> null);
                });
            }
            default: {
                throw new IgniteInternalException(Replicator.REPLICA_COMMON_ERR,
                        format("Unknown multi request [actionType={}]", request.requestType()));
            }
        }
    }

    /**
     * Executes a command and handles exceptions. A result future can be finished with exception by following rules:
     * <ul>
     *     <li>If RAFT command cannot finish due to timeout, the future finished with {@link ReplicationTimeoutException}.</li>
     *     <li>If RAFT command finish with a runtime exception, the exception is moved to the result future.</li>
     *     <li>If RAFT command finish with any other exception, the future finished with {@link ReplicationException}.
     *     The original exception is set as cause.</li>
     * </ul>
     *
     * @param cmd Raft command.
     * @return Raft future.
     */
    private CompletableFuture<Object> applyCmdWithExceptionHandling(Command cmd) {
        return raftClient.run(cmd).exceptionally(throwable -> {
            if (throwable instanceof TimeoutException) {
                throw new ReplicationTimeoutException(replicationGroupId);
            } else if (throwable instanceof RuntimeException) {
                throw (RuntimeException) throwable;
            } else {
                throw new ReplicationException(replicationGroupId, throwable);
            }
        });
    }

    /**
     * Precesses single request.
     *
     * @param request Single request operation.
     * @return Listener response.
     */
    private CompletableFuture<Object> processSingleEntryAction(ReadWriteSingleRowReplicaRequest request) {
        UUID txId = request.transactionId();
        BinaryRow searchRow = request.binaryRow();
        TablePartitionId commitPartitionId = request.commitPartitionId();

        assert commitPartitionId != null || request.requestType() == RequestType.RW_GET :
                "Commit partition is null [type=" + request.requestType() + ']';

        switch (request.requestType()) {
            case RW_GET: {
                return resolveRowByPk(searchRow, txId, (rowId, row) -> {
                    if (rowId == null) {
                        return completedFuture(null);
                    }

                    return takeLocksForGet(rowId, txId)
                            .thenApply(ignored -> row);
                });
            }
            case RW_DELETE: {
                return resolveRowByPk(searchRow, txId, (rowId, row) -> {
                    if (rowId == null) {
                        return completedFuture(false);
                    }

                    return takeLocksForDelete(searchRow, rowId, txId)
                            .thenCompose(ignored -> applyCmdWithExceptionHandling(new UpdateCommand(commitPartitionId, rowId, txId)))
                            .thenApply(ignored -> true);
                });
            }
            case RW_GET_AND_DELETE: {
                return resolveRowByPk(searchRow, txId, (rowId, row) -> {
                    if (rowId == null) {
                        return completedFuture(null);
                    }

                    return takeLocksForDelete(searchRow, rowId, txId)
                            .thenCompose(ignored -> applyCmdWithExceptionHandling(new UpdateCommand(commitPartitionId, rowId, txId)))
                            .thenApply(ignored -> row);
                });
            }
            case RW_DELETE_EXACT: {
                return resolveRowByPk(searchRow, txId, (rowId, row) -> {
                    if (rowId == null) {
                        return completedFuture(false);
                    }

                    return takeLocksForDeleteExact(searchRow, rowId, row, txId)
                            .thenCompose(validatedRowId -> {
                                if (validatedRowId == null) {
                                    return completedFuture(false);
                                }

                                return applyCmdWithExceptionHandling(new UpdateCommand(commitPartitionId, validatedRowId, txId))
                                        .thenApply(ignored -> true);
                            });
                });
            }
            case RW_INSERT: {
                return resolveRowByPk(searchRow, txId, (rowId, row) -> {
                    if (rowId != null) {
                        return completedFuture(false);
                    }

                    RowId rowId0 = new RowId(partId);

                    return takeLocksForInsert(searchRow, rowId0, txId)
                            .thenCompose(ignored -> applyCmdWithExceptionHandling(
                                    new UpdateCommand(commitPartitionId, rowId0, searchRow, txId)))
                            .thenApply(ignored -> true);
                });
            }
            case RW_UPSERT: {
                return resolveRowByPk(searchRow, txId, (rowId, row) -> {
                    boolean insert = rowId == null;

                    RowId rowId0 = insert ? new RowId(partId) : rowId;

                    CompletableFuture<?> lockFut = insert
                            ? takeLocksForInsert(searchRow, rowId0, txId)
                            : takeLocksForUpdate(searchRow, rowId0, txId);

                    return lockFut
                            .thenCompose(ignored -> applyCmdWithExceptionHandling(
                                    new UpdateCommand(commitPartitionId, rowId0, searchRow, txId)))
                            .thenApply(ignored -> null);
                });
            }
            case RW_GET_AND_UPSERT: {
                return resolveRowByPk(searchRow, txId, (rowId, row) -> {
                    boolean insert = rowId == null;

                    RowId rowId0 = insert ? new RowId(partId) : rowId;

                    CompletableFuture<?> lockFut = insert
                            ? takeLocksForInsert(searchRow, rowId0, txId)
                            : takeLocksForUpdate(searchRow, rowId0, txId);

                    return lockFut
                            .thenCompose(ignored -> applyCmdWithExceptionHandling(
                                    new UpdateCommand(commitPartitionId, rowId0, searchRow, txId)))
                            .thenApply(ignored -> row);
                });
            }
            case RW_GET_AND_REPLACE: {
                return resolveRowByPk(searchRow, txId, (rowId, row) -> {
                    if (rowId == null) {
                        return completedFuture(null);
                    }

                    return takeLocksForUpdate(searchRow, rowId, txId)
                            .thenCompose(ignored -> applyCmdWithExceptionHandling(
                                    new UpdateCommand(commitPartitionId, rowId, searchRow, txId)))
                            .thenApply(ignored0 -> row);
                });
            }
            case RW_REPLACE_IF_EXIST: {
                return resolveRowByPk(searchRow, txId, (rowId, row) -> {
                    if (rowId == null) {
                        return completedFuture(false);
                    }

                    return takeLocksForUpdate(searchRow, rowId, txId)
                            .thenCompose(ignored -> applyCmdWithExceptionHandling(
                                    new UpdateCommand(commitPartitionId, rowId, searchRow, txId)))
                            .thenApply(ignored -> true);
                });
            }
            default: {
                throw new IgniteInternalException(Replicator.REPLICA_COMMON_ERR,
                        format("Unknown single request [actionType={}]", request.requestType()));
            }
        }
    }

    /**
     * Takes all required locks on a key, before upserting.
     *
     * @param txId      Transaction id.
     * @return Future completes with {@link RowId} or {@code null} if there is no value.
     */
    private CompletableFuture<RowId> takeLocksForUpdate(BinaryRow tableRow, RowId rowId, UUID txId) {
        return lockManager.acquire(txId, new LockKey(tableId), LockMode.IX)
                .thenCompose(ignored -> lockManager.acquire(txId, new LockKey(tableId, rowId), LockMode.X))
                .thenCompose(ignored -> takePutLockOnIndexes(tableRow, rowId, txId))
                .thenApply(ignored -> rowId);
    }

    /**
     * Takes all required locks on a key, before inserting the value.
     *
     * @param tableRow Table row.
     * @param txId Transaction id.
     * @return Future completes with {@link RowId} or {@code null} if there is no value.
     */
    private CompletableFuture<RowId> takeLocksForInsert(BinaryRow tableRow, RowId rowId, UUID txId) {
        return lockManager.acquire(txId, new LockKey(tableId), LockMode.IX) // IX lock on table
                .thenCompose(ignored -> takePutLockOnIndexes(tableRow, rowId, txId))
                .thenApply(tblLock -> rowId);
    }

    private CompletableFuture<?> takePutLockOnIndexes(BinaryRow tableRow, RowId rowId, UUID txId) {
        Collection<IndexLocker> indexes = indexesLockers.get().values();

        if (nullOrEmpty(indexes)) {
            return completedFuture(null);
        }

        CompletableFuture<?>[] locks = new CompletableFuture[indexes.size()];
        int idx = 0;

        for (IndexLocker locker : indexes) {
            locks[idx++] = locker.locksForInsert(txId, tableRow, rowId);
        }

        return allOf(locks);
    }

    private CompletableFuture<?> takeRemoveLockOnIndexes(BinaryRow tableRow, RowId rowId, UUID txId) {
        Collection<IndexLocker> indexes = indexesLockers.get().values();

        if (nullOrEmpty(indexes)) {
            return completedFuture(null);
        }

        CompletableFuture<?>[] locks = new CompletableFuture[indexes.size()];
        int idx = 0;

        for (IndexLocker locker : indexes) {
            locks[idx++] = locker.locksForRemove(txId, tableRow, rowId);
        }

        return allOf(locks);
    }

    /**
     * Takes all required locks on a key, before deleting the value.
     *
     * @param txId      Transaction id.
     * @return Future completes with {@link RowId} or {@code null} if there is no value for remove.
     */
    private CompletableFuture<RowId> takeLocksForDeleteExact(BinaryRow expectedRow, RowId rowId, BinaryRow actualRow, UUID txId) {
        return lockManager.acquire(txId, new LockKey(tableId), LockMode.IX) // IX lock on table
                .thenCompose(ignored -> lockManager.acquire(txId, new LockKey(tableId, rowId), LockMode.S)) // S lock on RowId
                .thenCompose(ignored -> {
                    if (equalValues(actualRow, expectedRow)) {
                        return lockManager.acquire(txId, new LockKey(tableId, rowId), LockMode.X) // X lock on RowId
                                .thenCompose(ignored0 -> takeRemoveLockOnIndexes(actualRow, rowId, txId))
                                .thenApply(exclusiveRowLock -> rowId);
                    }

                    return completedFuture(null);
                });
    }

    /**
     * Takes all required locks on a key, before deleting the value.
     *
     * @param txId      Transaction id.
     * @return Future completes with {@link RowId} or {@code null} if there is no value for the key.
     */
    private CompletableFuture<RowId> takeLocksForDelete(BinaryRow tableRow, RowId rowId, UUID txId) {
        return lockManager.acquire(txId, new LockKey(tableId), LockMode.IX) // IX lock on table
                .thenCompose(ignored -> lockManager.acquire(txId, new LockKey(tableId, rowId), LockMode.X)) // X lock on RowId
                .thenCompose(ignored -> takeRemoveLockOnIndexes(tableRow, rowId, txId))
                .thenApply(ignored -> rowId);
    }

    /**
     * Takes all required locks on a key, before getting the value.
     *
     * @param txId      Transaction id.
     * @return Future completes with {@link RowId} or {@code null} if there is no value for the key.
     */
    private CompletableFuture<RowId> takeLocksForGet(RowId rowId, UUID txId) {
        return lockManager.acquire(txId, new LockKey(tableId), LockMode.IS) // IS lock on table
                .thenCompose(tblLock -> lockManager.acquire(txId, new LockKey(tableId, rowId), LockMode.S)) // S lock on RowId
                .thenApply(ignored -> rowId);
    }

    /**
     * Precesses two actions.
     *
     * @param request Two actions operation request.
     * @return Listener response.
     */
    private CompletableFuture<Boolean> processTwoEntriesAction(ReadWriteSwapRowReplicaRequest request) {
        BinaryRow newRow = request.binaryRow();
        BinaryRow expectedRow = request.oldBinaryRow();
        TablePartitionId commitPartitionId = request.commitPartitionId();

        assert commitPartitionId != null : "Commit partition partition is null [type=" + request.requestType() + ']';

        UUID txId = request.transactionId();

        if (request.requestType() == RequestType.RW_REPLACE) {
            return resolveRowByPk(newRow, txId, (rowId, row) -> {
                if (rowId == null) {
                    return completedFuture(false);
                }

                return takeLocksForReplace(expectedRow, row, newRow, rowId, txId)
                        .thenCompose(validatedRowId -> {
                            if (validatedRowId == null) {
                                return completedFuture(false);
                            }

                            return applyCmdWithExceptionHandling(new UpdateCommand(commitPartitionId, validatedRowId, newRow, txId))
                                    .thenApply(ignored -> true);
                        });
            });
        }

        throw new IgniteInternalException(Replicator.REPLICA_COMMON_ERR,
                format("Unknown two actions operation [actionType={}]", request.requestType()));
    }

    /**
     * Takes all required locks on a key, before updating the value.
     *
     * @param txId      Transaction id.
     * @return Future completes with {@link RowId} or {@code null} if there is no suitable row.
     */
    private CompletableFuture<RowId> takeLocksForReplace(BinaryRow expectedRow, BinaryRow oldRow,
            BinaryRow newRow, RowId rowId, UUID txId) {
        return lockManager.acquire(txId, new LockKey(tableId), LockMode.IX)
                .thenCompose(ignored -> lockManager.acquire(txId, new LockKey(tableId, rowId), LockMode.S))
                .thenCompose(ignored -> {
                    if (oldRow != null && equalValues(oldRow, expectedRow)) {
                        return lockManager.acquire(txId, new LockKey(tableId, rowId), LockMode.X) // X lock on RowId
                                .thenCompose(ignored1 -> takePutLockOnIndexes(newRow, rowId, txId))
                                .thenApply(rowLock -> rowId);
                    }

                    return completedFuture(null);
                });
    }

    /**
     * Ensure that the primary replica was not changed.
     *
     * @param request Replica request.
     * @return Future. The result is not null only for {@link ReadOnlyReplicaRequest}. If {@code true}, then replica is primary.
     */
    private CompletableFuture<Boolean> ensureReplicaIsPrimary(ReplicaRequest request) {
        Long expectedTerm;

        if (request instanceof ReadWriteReplicaRequest) {
            expectedTerm = ((ReadWriteReplicaRequest) request).term();

            assert expectedTerm != null;
        } else if (request instanceof TxFinishReplicaRequest) {
            expectedTerm = ((TxFinishReplicaRequest) request).term();

            assert expectedTerm != null;
        } else if (request instanceof TxCleanupReplicaRequest) {
            expectedTerm = ((TxCleanupReplicaRequest) request).term();

            assert expectedTerm != null;
        } else {
            expectedTerm = null;
        }

        if (expectedTerm != null) {
            return raftClient.refreshAndGetLeaderWithTerm()
                    .thenCompose(replicaAndTerm -> {
                                Long currentTerm = replicaAndTerm.get2();

                                if (expectedTerm.equals(currentTerm)) {
                                    return completedFuture(null);
                                } else {
                                    return failedFuture(new PrimaryReplicaMissException(expectedTerm, currentTerm));
                                }
                            }
                    );
        } else if (request instanceof ReadOnlyReplicaRequest) {
            return raftClient.refreshAndGetLeaderWithTerm().thenApply(replicaAndTerm -> isLocalPeerChecker.apply(replicaAndTerm.get1()));
        } else {
            return completedFuture(null);
        }
    }

    /**
     * Resolves a read result for RW transaction.
     *
     * @param readResult Read result to resolve.
     * @param txId Transaction id.
     * @return Resolved binary row.
     */
    private BinaryRow resolveReadResult(ReadResult readResult, UUID txId) {
        // Here is a safety join (waiting of the future result), because the resolution for RW transaction cannot lead to a network request.
        return resolveReadResult(readResult, txId, null, null).join();
    }

    /**
     * Resolves a read result for RO transaction.
     *
     * @param readResult Read result to resolve.
     * @param timestamp Timestamp.
     * @param lastCommitted Action to get the latest committed row.
     * @return Future to resolved binary row.
     */
    private CompletableFuture<BinaryRow> resolveReadResult(
            ReadResult readResult,
            HybridTimestamp timestamp,
            Supplier<BinaryRow> lastCommitted
    ) {
        return resolveReadResult(readResult, null, timestamp, lastCommitted);
    }

    /**
     * Resolves read result to the corresponding binary row. Following rules are used for read result resolution:
     * <ol>
     *     <li>If txId is not null (RW request), assert that retrieved tx id matches proposed one or that retrieved tx id is null
     *     and return binary row. Currently it's only possible to retrieve write intents if they belong to the same transaction,
     *     locks prevent reading write intents created by others.</li>
     *     <li>If txId is not null (RO request), perform write intent resolution if given readResult is a write intent itself
     *     or return binary row otherwise.</li>
     * </ol>
     *
     * @param readResult Read result to resolve.
     * @param txId Nullable transaction id, should be provided if resolution is performed within the context of RW transaction.
     * @param timestamp Timestamp is used in RO transaction only.
     * @param lastCommitted Action to get the latest committed row, it is used in RO transaction only.
     * @return Future to resolved binary row.
     */
    private CompletableFuture<BinaryRow> resolveReadResult(
            ReadResult readResult,
            @Nullable UUID txId,
            @Nullable HybridTimestamp timestamp,
            @Nullable Supplier<BinaryRow> lastCommitted
    ) {
        if (readResult == null) {
            return null;
        } else {
            if (txId != null) {
                // RW request.
                UUID retrievedResultTxId = readResult.transactionId();

                if (retrievedResultTxId == null || txId.equals(retrievedResultTxId)) {
                    // Same transaction - return retrieved value. It may be both writeIntent or regular value.
                    return completedFuture(readResult.binaryRow());
                } else {
                    // Should never happen, currently, locks prevent reading another transaction intents during RW requests.
                    throw new AssertionError("Mismatched transaction id, expectedTxId={" + txId + "},"
                            + " actualTxId={" + retrievedResultTxId + '}');
                }
            } else {
                if (!readResult.isWriteIntent()) {
                    return completedFuture(readResult.binaryRow());
                }

                CompletableFuture<BinaryRow> writeIntentResolutionFut = resolveWriteIntentAsync(
                        readResult, timestamp, lastCommitted);

                // RO request.
                return writeIntentResolutionFut;
            }
        }
    }

    /**
     * Resolves a read result to the matched row.
     * If the result does not match any row, the method returns a future to {@code null}.
     *
     * @param readResult Read result.
     * @param timestamp Timestamp.
     * @param lastCommitted Action to get a last committed row.
     * @return Result future.
     */
    private CompletableFuture<BinaryRow> resolveWriteIntentAsync(
            ReadResult readResult,
            HybridTimestamp timestamp,
            Supplier<BinaryRow> lastCommitted
    ) {
        ReplicationGroupId commitGrpId = new TablePartitionId(readResult.commitTableId(), readResult.commitPartitionId());

        return placementDriver.sendMetaRequest(commitGrpId, FACTORY.txStateReplicaRequest()
                        .groupId(commitGrpId)
                        .commitTimestamp(timestamp)
                        .txId(readResult.transactionId())
                        .build())
                .thenApply(txMeta -> {
                    if (txMeta == null) {
                        return lastCommitted.get();
                    } else if (txMeta.txState() == TxState.COMMITED && txMeta.commitTimestamp().compareTo(timestamp) <= 0) {
                        return readResult.binaryRow();
                    } else {
                        assert txMeta.txState() == TxState.ABORTED : "Unexpected transaction state [state=" + txMeta.txState() + ']';

                        return lastCommitted.get();
                    }
                });
    }
}
