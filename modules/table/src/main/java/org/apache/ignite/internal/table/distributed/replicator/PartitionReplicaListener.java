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

import static java.util.concurrent.CompletableFuture.allOf;
import static java.util.concurrent.CompletableFuture.failedFuture;
import static org.apache.ignite.internal.util.ExceptionUtils.withCause;
import static org.apache.ignite.lang.ErrorGroups.Replicator.CURSOR_CLOSE_ERR;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.replicator.ReplicationGroupId;
import org.apache.ignite.internal.replicator.exception.PrimaryReplicaMissException;
import org.apache.ignite.internal.replicator.exception.ReplicationException;
import org.apache.ignite.internal.replicator.exception.ReplicationTimeoutException;
import org.apache.ignite.internal.replicator.exception.UnsupportedReplicaRequestException;
import org.apache.ignite.internal.replicator.listener.ReplicaListener;
import org.apache.ignite.internal.replicator.message.ReplicaRequest;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.storage.MvPartitionStorage;
import org.apache.ignite.internal.storage.PartitionTimestampCursor;
import org.apache.ignite.internal.storage.ReadResult;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.table.distributed.TableMessagesFactory;
import org.apache.ignite.internal.table.distributed.command.*;
import org.apache.ignite.internal.table.distributed.replication.request.ReadOnlyMultiRowReplicaRequest;
import org.apache.ignite.internal.table.distributed.replication.request.ReadOnlyScanRetrieveBatchReplicaRequest;
import org.apache.ignite.internal.table.distributed.replication.request.ReadOnlySingleRowReplicaRequest;
import org.apache.ignite.internal.table.distributed.replication.request.ReadWriteMultiRowReplicaRequest;
import org.apache.ignite.internal.table.distributed.replication.request.ReadWriteReplicaRequest;
import org.apache.ignite.internal.table.distributed.replication.request.ReadWriteScanCloseReplicaRequest;
import org.apache.ignite.internal.table.distributed.replication.request.ReadWriteScanRetrieveBatchReplicaRequest;
import org.apache.ignite.internal.table.distributed.replication.request.ReadWriteSingleRowReplicaRequest;
import org.apache.ignite.internal.table.distributed.replication.request.ReadWriteSwapRowReplicaRequest;
import org.apache.ignite.internal.table.distributed.replicator.action.RequestType;
import org.apache.ignite.internal.tx.*;
import org.apache.ignite.internal.tx.Lock;
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
import org.apache.ignite.lang.ErrorGroups.Replicator;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lang.IgniteInternalException;
import org.apache.ignite.lang.IgniteStringFormatter;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.network.NetworkAddress;
import org.apache.ignite.network.TopologyService;
import org.apache.ignite.raft.client.Command;
import org.apache.ignite.raft.client.service.RaftGroupService;
import org.apache.ignite.raft.jraft.util.ByteString;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/** Partition replication listener. */
public class PartitionReplicaListener implements ReplicaListener {
    /** */
    private final TableMessagesFactory msgFactory = new TableMessagesFactory();

    /** Tx messages factory. */
    private static final TxMessagesFactory FACTORY = new TxMessagesFactory();

    /** Replication group id. */
    private final TablePartitionId replicationGroupId;

    /** Partition id. */
    private final int partId;

    /** Primary key id. */
    public final UUID indexPkId;

    /** Scan index id. */
    public final UUID indexScanId;

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

    //TODO: https://issues.apache.org/jira/browse/IGNITE-17205 Temporary solution until the implementation of the primary index is done.
    /** Dummy primary index. */
    private final ConcurrentHashMap<ByteBuffer, RowId> primaryIndex;

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

    /** Placement Driver. */
    private final PlacementDriver placementDriver;

    /**
     * Map to control clock's update in the read only transactions concurrently with a commit timestamp.
     * TODO: IGNITE-17261 review this after the commit timestamp will be provided from a commit request (request.commitTimestamp()).
     */
    ConcurrentHashMap<UUID, CompletableFuture<TxMeta>> txTimestampUpdateMap = new ConcurrentHashMap<>();

    /**
     * The constructor.
     *
     * @param mvDataStorage Data storage.
     * @param raftClient Raft client.
     * @param txManager Transaction manager.
     * @param lockManager Lock manager.
     * @param partId Partition id.
     * @param tableId Table id.
     * @param primaryIndex Primary index.
     * @param hybridClock Hybrid clock.
     * @param txStateStorage Transaction state storage.
     * @param topologyService Topology services.
     * @param placementDriver Placement driver.
     */
    public PartitionReplicaListener(
            MvPartitionStorage mvDataStorage,
            RaftGroupService raftClient,
            TxManager txManager,
            LockManager lockManager,
            int partId,
            UUID tableId,
            ConcurrentHashMap<ByteBuffer, RowId> primaryIndex,
            HybridClock hybridClock,
            TxStateStorage txStateStorage,
            TopologyService topologyService,
            PlacementDriver placementDriver
    ) {
        this.mvDataStorage = mvDataStorage;
        this.raftClient = raftClient;
        this.txManager = txManager;
        this.lockManager = lockManager;
        this.partId = partId;
        this.tableId = tableId;
        this.primaryIndex = primaryIndex;
        this.hybridClock = hybridClock;
        this.txStateStorage = txStateStorage;
        this.topologyService = topologyService;
        this.placementDriver = placementDriver;

        //TODO: IGNITE-17479 Integrate indexes into replicaListener command handlers
        this.indexScanId = new UUID(tableId.getMostSignificantBits(), tableId.getLeastSignificantBits() + 1);
        this.indexPkId = new UUID(tableId.getMostSignificantBits(), tableId.getLeastSignificantBits() + 2);
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
                .thenCompose((ignore) -> {
                    if (request instanceof ReadWriteSingleRowReplicaRequest) {
                        return processSingleEntryAction((ReadWriteSingleRowReplicaRequest) request);
                    } else if (request instanceof ReadWriteMultiRowReplicaRequest) {
                        return processMultiEntryAction((ReadWriteMultiRowReplicaRequest) request);
                    } else if (request instanceof ReadWriteSwapRowReplicaRequest) {
                        return processTwoEntriesAction((ReadWriteSwapRowReplicaRequest) request);
                    } else if (request instanceof ReadWriteScanRetrieveBatchReplicaRequest) {
                        return processScanRetrieveBatchAction((ReadWriteScanRetrieveBatchReplicaRequest) request);
                    } else if (request instanceof ReadWriteScanCloseReplicaRequest) {
                        processScanCloseAction((ReadWriteScanCloseReplicaRequest) request);

                        return CompletableFuture.completedFuture(null);
                    } else if (request instanceof TxFinishReplicaRequest) {
                        return processTxFinishAction((TxFinishReplicaRequest) request);
                    } else if (request instanceof TxCleanupReplicaRequest) {
                        return processTxCleanupAction((TxCleanupReplicaRequest) request);
                    } else if (request instanceof ReadOnlySingleRowReplicaRequest) {
                        return processReadOnlySingleEntryAction((ReadOnlySingleRowReplicaRequest) request);
                    } else if (request instanceof ReadOnlyMultiRowReplicaRequest) {
                        return processReadOnlyMultiEntryAction((ReadOnlyMultiRowReplicaRequest) request);
                    } else if (request instanceof ReadOnlyScanRetrieveBatchReplicaRequest) {
                        return processReadOnlyScanRetrieveBatchAction((ReadOnlyScanRetrieveBatchReplicaRequest) request);
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
                                return CompletableFuture.completedFuture(
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
     * @return Result future.
     */
    private CompletableFuture<Object> processReadOnlyScanRetrieveBatchAction(ReadOnlyScanRetrieveBatchReplicaRequest request) {
        UUID txId = request.transactionId();
        int batchCount = request.batchSize();
        HybridTimestamp readTimestamp = request.readTimestamp();

        IgniteUuid cursorId = new IgniteUuid(txId, request.scanId());

        ArrayList<BinaryRow> batchRows = new ArrayList<>(batchCount);

        @SuppressWarnings("resource") PartitionTimestampCursor cursor = cursors.computeIfAbsent(cursorId,
                id -> mvDataStorage.scan(HybridTimestamp.MAX_VALUE));

        while (batchRows.size() < batchCount && cursor.hasNext()) {
            BinaryRow resolvedReadResult = resolveReadResult(cursor.next(), readTimestamp, () -> cursor.committed(readTimestamp));

            if (resolvedReadResult != null) {
                batchRows.add(resolvedReadResult);
            }
        }

        return CompletableFuture.completedFuture(batchRows);
    }

    /**
     * Processes single entry request for read only transaction.
     *
     * @param request Read only single entry request.
     * @return Result future.
     */
    private CompletableFuture<Object> processReadOnlySingleEntryAction(ReadOnlySingleRowReplicaRequest request) {
        ByteBuffer searchKey = request.binaryRow().keySlice();

        if (request.requestType() != RequestType.RO_GET) {
            throw new IgniteInternalException(Replicator.REPLICA_COMMON_ERR,
                    IgniteStringFormatter.format("Unknown single request [actionType={}]", request.requestType()));
        }

        //TODO: IGNITE-17868 Integrate indexes into rowIds resolution along with proper lock management on search rows.
        HybridTimestamp readTimestamp = request.readTimestamp();

        try (PartitionTimestampCursor scan = mvDataStorage.scan(readTimestamp)) {
            while (scan.hasNext()) {
                ReadResult readResult = scan.next();
                HybridTimestamp newestCommitTimestamp = readResult.newestCommitTimestamp();

                if (readResult.binaryRow() == null) {
                    if (newestCommitTimestamp == null) {
                        throw new AssertionError("Unexpected null value of the newest committed timestamp.");
                    }

                    BinaryRow candidate = scan.committed(newestCommitTimestamp);
                    if (candidate == null) {
                        throw new AssertionError("Unexpected null value of the candidate binary row.");
                    }

                    if (candidate.keySlice().equals(searchKey)) {
                        return CompletableFuture.completedFuture(
                                resolveReadResult(
                                        readResult,
                                        readTimestamp,
                                        () -> scan.committed(newestCommitTimestamp)
                                )
                        );
                    }
                } else if (readResult.binaryRow().keySlice().equals(searchKey)) {
                    return CompletableFuture.completedFuture(
                            resolveReadResult(
                                    readResult,
                                    readTimestamp,
                                    () -> newestCommitTimestamp == null ? null : scan.committed(newestCommitTimestamp)
                            )
                    );
                }
            }
        } catch (Exception e) {
            return failedFuture(
                    withCause(
                            ReplicationException::new,
                            CURSOR_CLOSE_ERR,
                            "Failed to close cursor.",
                            e
                    )
            );
        }

        return CompletableFuture.completedFuture(null);
    }

    /**
     * Processes multiple entries request for read only transaction.
     *
     * @param request Read only multiple entries request.
     * @return Result future.
     */
    private CompletableFuture<Object> processReadOnlyMultiEntryAction(ReadOnlyMultiRowReplicaRequest request) {
        Collection<ByteBuffer> keyRows = request.binaryRows().stream().map(br -> br.keySlice()).collect(
                Collectors.toList());

        if (request.requestType() !=  RequestType.RO_GET_ALL) {
            throw new IgniteInternalException(Replicator.REPLICA_COMMON_ERR,
                    IgniteStringFormatter.format("Unknown single request [actionType={}]", request.requestType()));
        }

        ArrayList<BinaryRow> result = new ArrayList<>(keyRows.size());

        //TODO: IGNITE-17868 Integrate indexes into rowIds resolution along with proper lock management on search rows.
        HybridTimestamp readTimestamp = request.readTimestamp();

        try (PartitionTimestampCursor scan = mvDataStorage.scan(readTimestamp)) {
            while (scan.hasNext()) {
                ReadResult readResult = scan.next();
                HybridTimestamp newestCommitTimestamp = readResult.newestCommitTimestamp();

                for (ByteBuffer searchKey : keyRows) {
                    if (readResult.binaryRow() == null) {
                        if (newestCommitTimestamp == null) {
                            throw new AssertionError("Unexpected null value of the newest committed timestamp.");
                        }

                        BinaryRow candidate = scan.committed(readResult.newestCommitTimestamp());
                        if (candidate == null) {
                            throw new AssertionError("Unexpected null value of the candidate binary row.");
                        }

                        if (candidate.keySlice().equals(searchKey)) {
                            result.add(
                                    resolveReadResult(
                                            readResult,
                                            readTimestamp,
                                            () -> scan.committed(readResult.newestCommitTimestamp())
                                    )
                            );
                        }
                    } else if (readResult.binaryRow().keySlice().equals(searchKey)) {
                        result.add(
                                resolveReadResult(
                                        readResult,
                                        readTimestamp,
                                        () -> newestCommitTimestamp == null ? null : scan.committed(readResult.newestCommitTimestamp())
                                )
                        );
                    }
                }
            }
        } catch (Exception e) {
            return failedFuture(
                    withCause(
                            ReplicationException::new,
                            CURSOR_CLOSE_ERR,
                            "Failed to close cursor.",
                            e
                    )
            );
        }

        return CompletableFuture.completedFuture(result);
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
                            IgniteStringFormatter.format("Close cursor exception [replicaGrpId={}, msg={}]", replicationGroupId,
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
                        IgniteStringFormatter.format("Close cursor exception [replicaGrpId={}, msg={}]", replicationGroupId,
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
    private CompletableFuture<Object> processScanRetrieveBatchAction(ReadWriteScanRetrieveBatchReplicaRequest request) {
        UUID txId = request.transactionId();
        int batchCount = request.batchSize();

        IgniteUuid cursorId = new IgniteUuid(txId, request.scanId());

        return lockManager.acquire(txId, new LockKey(tableId), LockMode.S).thenCompose(tblLock -> {
            ArrayList<BinaryRow> batchRows = new ArrayList<>(batchCount);

            @SuppressWarnings("resource") PartitionTimestampCursor cursor = cursors.computeIfAbsent(cursorId,
                    id -> mvDataStorage.scan(HybridTimestamp.MAX_VALUE));

            while (batchRows.size() < batchCount && cursor.hasNext()) {
                BinaryRow resolvedReadResult = resolveReadResult(cursor.next(), txId);

                if (resolvedReadResult != null) {
                    batchRows.add(resolvedReadResult);
                }
            }

            return CompletableFuture.completedFuture(batchRows);
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
    private CompletableFuture<Object> processTxFinishAction(TxFinishReplicaRequest request) {
        List<ReplicationGroupId> aggregatedGroupIds = request.groups().values().stream()
                .flatMap(List::stream)
                .map(IgniteBiTuple::get1)
                .collect(Collectors.toList());

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

        return allOf(cleanupFutures).thenApply(ignored -> null);
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

        HybridTimestamp commitTimestamp = hybridClock.now();

        CompletableFuture<Object> changeStateFuture = raftClient.run(
                msgFactory.finishTxCommand()
                        .txId(txId)
                        .commit(commit)
                        .commitTimestamp(
                                msgFactory.hybridTimestampMessage()
                                        .logical(commitTimestamp.getLogical())
                                        .physical(commitTimestamp.getPhysical())
                                        .build()
                        )
                        .tablePartitionIds(aggregatedGroupIds.stream()
                                .map(rgId -> msgFactory.tablePartitionIdMessage()
                                        .tableId(((TablePartitionId)rgId).getTableId())
                                        .partitionId(((TablePartitionId)rgId).getPartId())
                                        .build())
                                .collect(Collectors.toList()))
                        .build()
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
    private CompletableFuture processTxCleanupAction(TxCleanupReplicaRequest request) {
        try {
            closeAllTransactionCursors(request.txId());
        } catch (Exception e) {
            return failedFuture(e);
        }

        HybridTimestampMessage timestampMsg = hybridTimestamp(request.commitTimestamp());

        TxCleanupCommand txCleanupCmd = msgFactory.txCleanupCommand()
                .txId(request.txId())
                .commit(request.commit())
                .commitTimestamp(timestampMsg)
                .build();

        return raftClient
                .run(txCleanupCmd)
                .thenRun(() -> lockManager.locks(request.txId()).forEachRemaining(lockManager::release));
    }

    /**
     * Returns index id of default {@lonk INDEX_SCAN_ID} index that will be used for operation.
     *
     * @param indexId Index id or {@code null}.
     * @return Index id.
     */
    private @NotNull UUID indexIdOrDefault(@Nullable UUID indexId) {
        return indexId != null ? indexId : indexScanId;
    }

    /**
     * Find out a row id by an index.
     * TODO: IGNITE-17479 Integrate indexes into replicaListener command handlers
     *
     * @param indexId Index id.
     * @param key     Key to find.
     * @return Value or {@code null} if the key does not determine a value.
     */
    private RowId rowIdByKey(@NotNull UUID indexId, ByteBuffer key) {
        if (indexPkId.equals(indexId)) {
            return primaryIndex.get(key);
        }

        if (indexScanId.equals(indexId)) {
            RowId[] rowIdHolder = new RowId[1];

            mvDataStorage.forEach((rowId, binaryRow) -> {
                if (rowIdHolder[0] == null && binaryRow.keySlice().equals(key)) {
                    rowIdHolder[0] = rowId;
                }
            });

            return rowIdHolder[0];
        }

        throw new IgniteInternalException(Replicator.REPLICA_COMMON_ERR,
                IgniteStringFormatter.format("The index does not exist [indexId={}]", indexId));
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
        UUID indexId = indexIdOrDefault(indexPkId/*request.indexToUse()*/);

        UUID txId = request.transactionId();
        TablePartitionId committedPartitionId = request.commitPartitionId();

        assert committedPartitionId != null || request.requestType() == RequestType.RW_GET_ALL
                : "Commit partition partition is null [type=" + request.requestType() + ']';

        switch (request.requestType()) {
            case RW_GET_ALL: {
                CompletableFuture<RowId>[] getLockFuts = new CompletableFuture[request.binaryRows().size()];

                int i = 0;

                for (BinaryRow row : request.binaryRows()) {
                    getLockFuts[i++] = takeLocksForGet(row.keySlice(), indexId, txId);
                }

                return allOf(getLockFuts).thenApply(ignore -> {
                    ArrayList<BinaryRow> result = new ArrayList<>(request.binaryRows().size());

                    for (int futNum = 0; futNum < request.binaryRows().size(); futNum++) {
                        RowId lockedRowId = getLockFuts[futNum].join();

                        result.add(lockedRowId != null
                                ? resolveReadResult(mvDataStorage.read(lockedRowId, HybridTimestamp.MAX_VALUE), txId) : null
                        );
                    }

                    return result;
                });
            }
            case RW_DELETE_ALL: {
                CompletableFuture<RowId>[] deleteLockFuts = new CompletableFuture[request.binaryRows().size()];

                int i = 0;

                for (BinaryRow row : request.binaryRows()) {
                    deleteLockFuts[i++] = takeLocksForDelete(row.keySlice(), indexId, txId);
                }

                return allOf(deleteLockFuts).thenCompose(ignore -> {
                    Map<UUID, ByteString> rowIdsToDelete = new HashMap<>();
                    Collection<BinaryRow> result = new ArrayList<>();

                    int futNum = 0;

                    for (BinaryRow row : request.binaryRows()) {
                        RowId lockedRowId = deleteLockFuts[futNum++].join();

                        if (lockedRowId != null) {
                            rowIdsToDelete.put(new UUID(lockedRowId.mostSignificantBits(), lockedRowId.leastSignificantBits()), null);
                        } else {
                            result.add(row);
                        }
                    }

                    CompletableFuture raftFut = rowIdsToDelete.isEmpty() ? CompletableFuture.completedFuture(null)
                            : applyCmdWithExceptionHandling(
                                    msgFactory.updateAllCommand()
                                            .tablePartitionId(msgFactory.tablePartitionIdMessage()
                                                    .tableId(committedPartitionId.getTableId())
                                                    .partitionId(committedPartitionId.getPartId())
                                                    .build())
                                            .rowsToUpdate(rowIdsToDelete)
                                            .txId(txId)
                                            .build()
                    );

                    return raftFut.thenApply(ignored -> result);
                });
            }
            case RW_DELETE_EXACT_ALL: {
                CompletableFuture<RowId>[] deleteExactLockFuts = new CompletableFuture[request.binaryRows().size()];

                int i = 0;

                for (BinaryRow row : request.binaryRows()) {
                    deleteExactLockFuts[i++] = takeLocksForDeleteExact(row.keySlice(), row, indexId, txId);
                }

                return allOf(deleteExactLockFuts).thenCompose(ignore -> {
                    Map<UUID, ByteString> rowIdsToDelete = new HashMap<>();
                    Collection<BinaryRow> result = new ArrayList<>();

                    int futNum = 0;

                    for (BinaryRow row : request.binaryRows()) {
                        RowId lockedRowId = deleteExactLockFuts[futNum++].join();

                        if (lockedRowId != null) {
                            rowIdsToDelete.put(new UUID(lockedRowId.mostSignificantBits(), lockedRowId.leastSignificantBits()), null);
                        } else {
                            result.add(row);
                        }
                    }

                    CompletableFuture raftFut = rowIdsToDelete.isEmpty() ? CompletableFuture.completedFuture(null)
                            : applyCmdWithExceptionHandling(
                                    msgFactory.updateAllCommand()
                                            .tablePartitionId(msgFactory.tablePartitionIdMessage()
                                                    .tableId(committedPartitionId.getTableId())
                                                    .partitionId(committedPartitionId.getPartId())
                                                    .build())
                                            .rowsToUpdate(rowIdsToDelete)
                                            .txId(txId)
                                            .build());

                    return raftFut.thenApply(ignored -> result);
                });
            }
            case RW_INSERT_ALL: {
                CompletableFuture<RowId>[] insertLockFuts = new CompletableFuture[request.binaryRows().size()];

                int i = 0;

                for (BinaryRow row : request.binaryRows()) {
                    insertLockFuts[i++] = takeLocksForInsert(row.keySlice(), indexId, txId);
                }

                return allOf(insertLockFuts).thenCompose(ignore -> {
                    Collection<BinaryRow> result = new ArrayList<>();
                    Map<UUID, ByteString> rowsToInsert = new HashMap<>();
                    Set<ByteBuffer> uniqueKeys = new HashSet<>();

                    int futNum = 0;

                    for (BinaryRow row : request.binaryRows()) {
                        RowId lockedRow = insertLockFuts[futNum++].join();

                        if (lockedRow != null) {
                            result.add(row);
                        } else {
                            ByteBuffer keyToCheck = row.keySlice();
                            if (!uniqueKeys.contains(keyToCheck)) {
                                uniqueKeys.add(keyToCheck);

                                rowsToInsert.put(Timestamp.nextVersion().toUuid(), new ByteString(row.byteBuffer()));
                            } else {
                                result.add(row);
                            }
                        }
                    }

                    CompletableFuture raftFut = rowsToInsert.isEmpty() ? CompletableFuture.completedFuture(null)
                            : applyCmdWithExceptionHandling(
                                    msgFactory.updateAllCommand()
                                            .tablePartitionId(msgFactory.tablePartitionIdMessage()
                                                    .tableId(committedPartitionId.getTableId())
                                                    .partitionId(committedPartitionId.getPartId())
                                                    .build())
                                            .rowsToUpdate(rowsToInsert)
                                            .txId(txId)
                                            .build());

                    return raftFut.thenApply(ignored -> result);
                });
            }
            case RW_UPSERT_ALL: {
                CompletableFuture<RowId>[] upsertLockFuts = new CompletableFuture[request.binaryRows().size()];

                int i = 0;

                for (BinaryRow row : request.binaryRows()) {
                    upsertLockFuts[i++] = takeLocksForUpsert(row.keySlice(), indexId, txId);
                }

                return allOf(upsertLockFuts).thenCompose(ignore -> {
                    Map<UUID, ByteString> rowsToUpdate = new HashMap<>();

                    int futNum = 0;

                    for (BinaryRow row : request.binaryRows()) {
                        RowId lockedRow = upsertLockFuts[futNum++].join();

                        if (lockedRow != null) {
                            rowsToUpdate.put(new UUID(lockedRow.mostSignificantBits(), lockedRow.leastSignificantBits()),
                                    new ByteString(row.byteBuffer()));
                        } else {
                            rowsToUpdate.put(Timestamp.nextVersion().toUuid(), new ByteString(row.byteBuffer()));
                        }
                    }

                    CompletableFuture raftFut = rowsToUpdate.isEmpty() ? CompletableFuture.completedFuture(null)
                            : applyCmdWithExceptionHandling(
                                    msgFactory.updateAllCommand()
                                            .tablePartitionId(msgFactory.tablePartitionIdMessage()
                                                    .tableId(committedPartitionId.getTableId())
                                                    .partitionId(committedPartitionId.getPartId())
                                                    .build())
                                            .rowsToUpdate(rowsToUpdate)
                                            .txId(txId)
                                            .build());

                    return raftFut.thenApply(ignored -> null);
                });
            }
            default: {
                throw new IgniteInternalException(Replicator.REPLICA_COMMON_ERR,
                        IgniteStringFormatter.format("Unknown multi request [actionType={}]", request.requestType()));
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
                "Commit partition partition is null [type=" + request.requestType() + ']';

        ByteBuffer searchKey = searchRow.keySlice();

        UUID indexId = indexIdOrDefault(indexPkId/*request.indexToUse()*/);

        switch (request.requestType()) {
            case RW_GET: {
                CompletableFuture<RowId> lockFut = takeLocksForGet(searchKey, indexId, txId);

                return lockFut.thenApply(lockedRowId -> {
                    BinaryRow result = lockedRowId != null
                            ? resolveReadResult(mvDataStorage.read(lockedRowId, HybridTimestamp.MAX_VALUE), txId) : null;

                    return result;
                });
            }
            case RW_DELETE: {
                CompletableFuture<RowId> lockFut = takeLocksForDelete(searchKey, indexId, txId);

                return lockFut.thenCompose(lockedRowId -> {
                    boolean removed = lockedRowId != null;

                    CompletableFuture raftFut = removed ? applyCmdWithExceptionHandling(
                            msgFactory.updateCommand()
                                    .tablePartitionId(msgFactory.tablePartitionIdMessage()
                                            .tableId(commitPartitionId.getTableId())
                                            .partitionId(commitPartitionId.getPartId())
                                            .build())
                                    .rowUuid(new UUID(lockedRowId.mostSignificantBits(), lockedRowId.leastSignificantBits()))
                                    .txId(txId)
                                    .build()) :
                            CompletableFuture.completedFuture(null);

                    return raftFut.thenApply(ignored -> removed);
                });
            }
            case RW_GET_AND_DELETE: {
                CompletableFuture<RowId> lockFut = takeLocksForDelete(searchKey, indexId, txId);

                return lockFut.thenCompose(lockedRowId -> {
                    BinaryRow lockedRow = lockedRowId != null
                            ? resolveReadResult(mvDataStorage.read(lockedRowId, HybridTimestamp.MAX_VALUE), txId) : null;

                    CompletableFuture raftFut = lockedRowId != null ? applyCmdWithExceptionHandling(
                            msgFactory.updateCommand()
                                    .tablePartitionId(msgFactory.tablePartitionIdMessage()
                                            .tableId(commitPartitionId.getTableId())
                                            .partitionId(commitPartitionId.getPartId())
                                            .build())
                                    .rowUuid(new UUID(lockedRowId.mostSignificantBits(), lockedRowId.leastSignificantBits()))
                                    .txId(txId)
                                    .build()) :
                            CompletableFuture.completedFuture(null);

                    return raftFut.thenApply(ignored -> lockedRow);
                });
            }
            case RW_DELETE_EXACT: {
                CompletableFuture<RowId> lockFut = takeLocksForDeleteExact(searchKey, searchRow, indexId, txId);

                return lockFut.thenCompose(lockedRow -> {
                    boolean removed = lockedRow != null;

                    CompletableFuture raftFut = removed ? applyCmdWithExceptionHandling(
                            msgFactory.updateCommand()
                                    .tablePartitionId(msgFactory.tablePartitionIdMessage()
                                            .tableId(commitPartitionId.getTableId())
                                            .partitionId(commitPartitionId.getPartId())
                                            .build())
                                    .rowUuid(new UUID(lockedRow.mostSignificantBits(), lockedRow.leastSignificantBits()))
                                    .txId(txId)
                                    .build()) :
                            CompletableFuture.completedFuture(null);

                    return raftFut.thenApply(ignored -> removed);
                });
            }
            case RW_INSERT: {
                CompletableFuture<RowId> lockFut = takeLocksForInsert(searchKey, indexId, txId);

                return lockFut.thenCompose(lockedRowId -> {
                    boolean inserted = lockedRowId == null;

                    CompletableFuture raftFut =
                            lockedRowId == null ? applyCmdWithExceptionHandling(
                                    msgFactory.updateCommand()
                                            .tablePartitionId(msgFactory.tablePartitionIdMessage()
                                                    .tableId(commitPartitionId.getTableId())
                                                    .partitionId(commitPartitionId.getPartId())
                                                    .build())
                                            .rowUuid(Timestamp.nextVersion().toUuid())
                                            .rowBuffer(new ByteString(searchRow.byteBuffer()))
                                            .txId(txId)
                                            .build()) :
                                    CompletableFuture.completedFuture(null);

                    return raftFut.thenApply(ignored -> inserted);
                });
            }
            case RW_UPSERT: {
                CompletableFuture<RowId> lockFut = takeLocksForUpsert(searchKey, indexId, txId);

                return lockFut.thenCompose(lockedRowId -> {
                    UUID rowUuid = lockedRowId != null ? new UUID(lockedRowId.mostSignificantBits(), lockedRowId.leastSignificantBits()) :
                            Timestamp.nextVersion().toUuid();

                    CompletableFuture raftFut = applyCmdWithExceptionHandling(
                                    msgFactory.updateCommand()
                                            .tablePartitionId(msgFactory.tablePartitionIdMessage()
                                                    .tableId(commitPartitionId.getTableId())
                                                    .partitionId(commitPartitionId.getPartId())
                                                    .build())
                                            .rowUuid(rowUuid)
                                            .rowBuffer(new ByteString(searchRow.byteBuffer()))
                                            .txId(txId)
                                            .build());

                    return raftFut.thenApply(ignored -> null);
                });
            }
            case RW_GET_AND_UPSERT: {
                return lockManager.acquire(txId, new LockKey(indexId, searchKey), LockMode.X)
                        .thenCompose(idxLock -> { // Index X lock
                            RowId rowId = rowIdByKey(indexId, searchKey);

                            return lockManager.acquire(txId, new LockKey(tableId), LockMode.IX)
                                    .thenCompose(tblLock -> { // IX lock on table
                                        CompletableFuture<Lock> rowLockFut = (rowId != null)
                                                ? lockManager.acquire(txId, new LockKey(tableId, rowId), LockMode.X)
                                                // X lock on RowId
                                                : CompletableFuture.completedFuture(null);

                                        return rowLockFut.thenCompose(rowLock -> {
                                            BinaryRow result = rowId != null
                                                    ? resolveReadResult(mvDataStorage.read(rowId, HybridTimestamp.MAX_VALUE), txId) : null;

                                            RowId locRowId = (rowId != null) ? rowId : new RowId(partId);

                                            CompletableFuture raftFut =
                                                    applyCmdWithExceptionHandling(
                                                            msgFactory.updateCommand()
                                                                    .tablePartitionId(msgFactory.tablePartitionIdMessage()
                                                                            .tableId(commitPartitionId.getTableId())
                                                                            .partitionId(commitPartitionId.getPartId())
                                                                            .build())
                                                                    .rowUuid(new UUID(
                                                                            locRowId.mostSignificantBits(),
                                                                            locRowId.leastSignificantBits()))
                                                                    .rowBuffer(new ByteString(searchRow.byteBuffer()))
                                                                    .txId(txId)
                                                                    .build()
                                                    );

                                            return raftFut.thenApply(ignored -> result);
                                        });
                                    });
                        });
            }
            case RW_GET_AND_REPLACE: {
                CompletableFuture<RowId> idxLockFut = lockManager.acquire(txId, new LockKey(indexId, searchKey), LockMode.S)
                        .thenCompose(sharedIdxLock -> { // Index S lock
                            RowId rowId = rowIdByKey(indexId, searchKey);

                            if (rowId != null) {
                                return lockManager.acquire(txId, new LockKey(indexId, searchKey), LockMode.X)
                                        .thenApply(exclusiveIdxLock -> rowId); // Index X lock
                            }

                            return CompletableFuture.completedFuture(null);
                        });

                return idxLockFut.thenCompose(lockedRowId -> {
                    return lockManager.acquire(txId, new LockKey(tableId), LockMode.IX)
                            .thenCompose(tblLock -> { // IX lock on table
                                CompletableFuture<BinaryRow> rowLockFut;

                                if (lockedRowId != null) {
                                    rowLockFut = lockManager.acquire(txId, new LockKey(tableId, lockedRowId), LockMode.X)
                                            .thenApply(rowLock -> // X lock on RowId
                                                    resolveReadResult(mvDataStorage.read(lockedRowId, HybridTimestamp.MAX_VALUE), txId)
                                            );
                                } else {
                                    rowLockFut = CompletableFuture.completedFuture(null);
                                }

                                return rowLockFut.thenCompose(lockedRow -> {
                                    CompletableFuture raftFut = lockedRow == null ? CompletableFuture.completedFuture(null) :
                                            applyCmdWithExceptionHandling(
                                                    msgFactory.updateCommand()
                                                            .tablePartitionId(
                                                                    msgFactory.tablePartitionIdMessage()
                                                                            .tableId(commitPartitionId.getTableId())
                                                                            .partitionId(commitPartitionId.getPartId())
                                                                            .build()
                                                            )
                                                            .rowUuid(new UUID(lockedRowId.mostSignificantBits(),
                                                                    lockedRowId.leastSignificantBits()))
                                                            .rowBuffer(new ByteString(searchRow.byteBuffer()))
                                                            .txId(txId)
                                                            .build());

                                    return raftFut.thenApply(ignored -> lockedRow);
                                });
                            });
                });
            }
            case RW_REPLACE_IF_EXIST: {
                CompletableFuture<RowId> lockFut = takeLocksForReplaceIfExist(searchKey, indexId, txId);

                return lockFut.thenCompose(lockedRowId -> {
                    boolean replaced = lockedRowId != null;

                    CompletableFuture raftFut = replaced ? applyCmdWithExceptionHandling(
                            msgFactory.updateCommand()
                                    .tablePartitionId(msgFactory.tablePartitionIdMessage()
                                            .tableId(commitPartitionId.getTableId())
                                            .partitionId(commitPartitionId.getPartId())
                                            .build())
                                    .rowUuid(new UUID(lockedRowId.mostSignificantBits(),
                                            lockedRowId.leastSignificantBits()))
                                    .rowBuffer(new ByteString(searchRow.byteBuffer()))
                                    .txId(txId)
                                    .build()) :
                            CompletableFuture.completedFuture(null);

                    return raftFut.thenApply(ignored -> replaced);
                });
            }
            default: {
                throw new IgniteInternalException(Replicator.REPLICA_COMMON_ERR,
                        IgniteStringFormatter.format("Unknown single request [actionType={}]", request.requestType()));
            }
        }
    }

    /**
     * Takes all required locks on a key, before replacing.
     *
     * @param searchKey Key to search.
     * @param indexId   Index id.
     * @param txId      Transaction id.
     * @return Future completes with {@link RowId} or {@code null} if there is no entry.
     */
    private CompletableFuture<RowId> takeLocksForReplaceIfExist(ByteBuffer searchKey, UUID indexId, UUID txId) {
        return lockManager.acquire(txId, new LockKey(indexId, searchKey), LockMode.S).thenCompose(shareIdxLock -> { // Index R lock
            RowId rowId = rowIdByKey(indexId, searchKey);

            CompletableFuture<Lock> idxLockFut = rowId != null
                    ? lockManager.acquire(txId, new LockKey(indexId, searchKey), LockMode.X) // Index X lock
                    : CompletableFuture.completedFuture(null);

            return idxLockFut.thenCompose(exclusiveIdxLock -> lockManager.acquire(txId, new LockKey(tableId), LockMode.IX)
                    .thenCompose(tblLock -> { // IX lock on table
                        if (rowId != null) {
                            RowId rowIdToLock = rowId;

                            return lockManager.acquire(txId, new LockKey(tableId, rowId), LockMode.X)
                                    .thenApply(rowLock -> rowIdToLock); // X lock on RowId
                        }

                        return CompletableFuture.completedFuture(null);
                    }));
        });
    }

    /**
     * Takes all required locks on a key, before upserting.
     *
     * @param searchKey Key to search.
     * @param indexId   Index id.
     * @param txId      Transaction id.
     * @return Future completes with {@link RowId} or {@code null} if there is no value.
     */
    private CompletableFuture<RowId> takeLocksForUpsert(ByteBuffer searchKey, UUID indexId, UUID txId) {
        return lockManager.acquire(txId, new LockKey(indexId, searchKey), LockMode.X).thenCompose(idxLock -> { // Index X lock
            RowId rowId = rowIdByKey(indexId, searchKey);

            return lockManager.acquire(txId, new LockKey(tableId), LockMode.IX)
                    .thenCompose(tblLock -> { // IX lock on table
                        if (rowId != null) {
                            return lockManager.acquire(txId, new LockKey(tableId, rowId), LockMode.X)
                                    .thenApply(rowLock -> rowId); // X lock on RowId
                        }

                        return CompletableFuture.completedFuture(null);
                    });
        });
    }

    /**
     * Takes all required locks on a key, before inserting the value.
     *
     * @param searchKey Key to search.
     * @param indexId   Index id.
     * @param txId      Transaction id.
     * @return Future completes with {@link RowId} or {@code null} if there is no value.
     */
    private CompletableFuture<RowId> takeLocksForInsert(ByteBuffer searchKey, UUID indexId, UUID txId) {
        return lockManager.acquire(txId, new LockKey(indexId, searchKey), LockMode.S) // Index S lock
                .thenCompose(sharedIdxLock -> {
                    RowId rowId = rowIdByKey(indexId, searchKey);

                    if (rowId == null) {
                        return lockManager.acquire(txId, new LockKey(indexId, searchKey), LockMode.X) // Index X lock
                                .thenCompose(exclusiveIdxLock ->
                                        lockManager.acquire(txId, new LockKey(tableId), LockMode.IX) // IX lock on table
                                                .thenApply(tblLock -> null));
                    }

                    return CompletableFuture.completedFuture(rowId);
                });
    }

    /**
     * Takes all required locks on a key, before deleting the value.
     *
     * @param searchKey Key to search.
     * @param searchRow Row to remove.
     * @param indexId   Index id.
     * @param txId      Transaction id.
     * @return Future completes with {@link RowId} or {@code null} if there is no value for remove.
     */
    private CompletableFuture<RowId> takeLocksForDeleteExact(ByteBuffer searchKey, BinaryRow searchRow, UUID indexId, UUID txId) {
        return lockManager.acquire(txId, new LockKey(indexId, searchKey), LockMode.X).thenCompose(idxLock -> { // Index X lock
            RowId rowId = rowIdByKey(indexId, searchKey);

            return lockManager.acquire(txId, new LockKey(tableId), LockMode.IX) // IX lock on table
                    .thenCompose(tblLock -> {
                        CompletableFuture<RowId> rowLockFut;

                        if (rowId != null) {
                            rowLockFut = lockManager.acquire(txId, new LockKey(tableId, rowId), LockMode.S) // S lock on RowId
                                    .thenCompose(sharedRowLock -> {
                                        BinaryRow curVal = resolveReadResult(mvDataStorage.read(rowId, HybridTimestamp.MAX_VALUE), txId);

                                        if (equalValues(curVal, searchRow)) {
                                            return lockManager.acquire(txId, new LockKey(tableId, rowId),
                                                            LockMode.X) // X lock on RowId
                                                    .thenApply(exclusiveRowLock -> rowId);
                                        }

                                        return CompletableFuture.completedFuture(null);
                                    });
                        } else {
                            rowLockFut = CompletableFuture.completedFuture(null);
                        }

                        return rowLockFut;
                    });
        });
    }

    /**
     * Takes all required locks on a key, before deleting the value.
     *
     * @param searchKey Key to search.
     * @param indexId   Index id.
     * @param txId      Transaction id.
     * @return Future completes with {@link RowId} or {@code null} if there is no value for the key.
     */
    private CompletableFuture<RowId> takeLocksForDelete(ByteBuffer searchKey, UUID indexId, UUID txId) {
        return lockManager.acquire(txId, new LockKey(indexId, searchKey), LockMode.X).thenCompose(idxLock -> { // Index X lock
            RowId rowId = rowIdByKey(indexId, searchKey);

            return lockManager.acquire(txId, new LockKey(tableId), LockMode.IX) // IX lock on table
                    .thenCompose(tblLock -> {
                        if (rowId != null) {
                            return lockManager.acquire(txId, new LockKey(tableId, rowId), LockMode.S) // S lock on RowId
                                    .thenCompose(sharedRowLock -> {
                                        BinaryRow curVal = resolveReadResult(mvDataStorage.read(rowId, HybridTimestamp.MAX_VALUE), txId);

                                        if (curVal != null) {
                                            return lockManager.acquire(txId, new LockKey(tableId, rowId),
                                                            LockMode.X) // X lock on RowId
                                                    .thenApply(exclusiveRowLock -> rowId);
                                        }

                                        return CompletableFuture.completedFuture(null);
                                    });
                        }

                        return CompletableFuture.completedFuture(null);
                    });
        });
    }

    /**
     * Takes all required locks on a key, before getting the value.
     *
     * @param searchKey Key to search.
     * @param indexId   Index id.
     * @param txId      Transaction id.
     * @return Future completes with {@link RowId} or {@code null} if there is no value for the key.
     */
    private CompletableFuture<RowId> takeLocksForGet(ByteBuffer searchKey, UUID indexId, UUID txId) {
        return lockManager.acquire(txId, new LockKey(indexId, searchKey), LockMode.S).thenCompose(idxLock -> { // Index S lock
            RowId rowId = rowIdByKey(indexId, searchKey);

            return lockManager.acquire(txId, new LockKey(tableId), LockMode.IS).thenCompose(tblLock -> { // IS lock on table
                if (rowId != null) {
                    return lockManager.acquire(txId, new LockKey(tableId, rowId), LockMode.S) // S lock on RowId
                            .thenApply(rowLock -> rowId);
                }

                return CompletableFuture.completedFuture(null);
            });
        });
    }

    /**
     * Precesses two actions.
     *
     * @param request Two actions operation request.
     * @return Listener response.
     */
    private CompletableFuture<Object> processTwoEntriesAction(ReadWriteSwapRowReplicaRequest request) {
        BinaryRow searchRow = request.binaryRow();
        BinaryRow oldRow = request.oldBinaryRow();
        TablePartitionId commitPartitionId = request.commitPartitionId();

        assert commitPartitionId != null : "Commit partition partition is null [type=" + request.requestType() + ']';

        ByteBuffer searchKey = searchRow.keySlice();

        UUID indexId = indexIdOrDefault(indexPkId/*request.indexToUse()*/);

        UUID txId = request.transactionId();

        switch (request.requestType()) {
            case RW_REPLACE: {
                CompletableFuture<RowId> lockFut = takeLocsForReplace(searchKey, oldRow, indexId, txId);

                return lockFut.thenCompose(lockedRowId -> {
                    boolean replaced = lockedRowId != null;

                    CompletableFuture raftFut = replaced ? applyCmdWithExceptionHandling(
                            msgFactory.updateCommand()
                                    .tablePartitionId(msgFactory.tablePartitionIdMessage()
                                            .tableId(commitPartitionId.getTableId())
                                            .partitionId(commitPartitionId.getPartId())
                                            .build())
                                    .rowUuid(new UUID(lockedRowId.mostSignificantBits(),
                                            lockedRowId.leastSignificantBits()))
                                    .rowBuffer(new ByteString(searchRow.byteBuffer()))
                                    .txId(txId)
                                    .build()) :
                            CompletableFuture.completedFuture(null);

                    return raftFut.thenApply(ignored -> replaced);
                });
            }
            default: {
                throw new IgniteInternalException(Replicator.REPLICA_COMMON_ERR,
                        IgniteStringFormatter.format("Unknown two actions operation [actionType={}]", request.requestType()));
            }
        }
    }

    /**
     * Takes all required locks on a key, before updating the value.
     *
     * @param searchKey Key to search.
     * @param oldRow    Old row that is expected.
     * @param indexId   Index id.
     * @param txId      Transaction id.
     * @return Future completes with {@link RowId} or {@code null} if there is no suitable row.
     */
    private CompletableFuture<RowId> takeLocsForReplace(ByteBuffer searchKey, BinaryRow oldRow, UUID indexId, UUID txId) {
        return lockManager.acquire(txId, new LockKey(indexId, searchKey), LockMode.S).thenCompose(shareIdxLock -> { // Index R lock
            RowId rowId = rowIdByKey(indexId, searchKey);

            CompletableFuture<Lock> idxLockFut = rowId != null
                    ? lockManager.acquire(txId, new LockKey(indexId, searchKey), LockMode.X) // Index X lock
                    : CompletableFuture.completedFuture(null);

            return idxLockFut.thenCompose(exclusiveIdxLock -> lockManager.acquire(txId, new LockKey(tableId), LockMode.IX)
                    .thenCompose(tblLock -> { // IX lock on table
                        CompletableFuture<RowId> rowLockFut;

                        if (rowId != null) {
                            rowLockFut = lockManager.acquire(txId, new LockKey(tableId, rowId), LockMode.S) // S lock on RowId
                                    .thenCompose(sharedRowLock -> {
                                        BinaryRow curVal = resolveReadResult(mvDataStorage.read(rowId, HybridTimestamp.MAX_VALUE), txId);

                                        if (equalValues(curVal, oldRow)) {
                                            return lockManager.acquire(txId, new LockKey(tableId, rowId),
                                                            LockMode.X) // X lock on RowId
                                                    .thenApply(rowLock -> rowId);
                                        }

                                        return CompletableFuture.completedFuture(null);
                                    });
                        } else {
                            rowLockFut = CompletableFuture.completedFuture(null);
                        }

                        return rowLockFut;
                    }));
        });
    }

    /**
     * Ensure that the primary replica was not changed.
     *
     * @param request Replica request.
     * @return Future.
     */
    private CompletableFuture<Void> ensureReplicaIsPrimary(ReplicaRequest request) {
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
                                    return CompletableFuture.completedFuture(null);
                                } else {
                                    return CompletableFuture.failedFuture(new PrimaryReplicaMissException(expectedTerm, currentTerm));
                                }
                            }
                    );
        } else {
            return CompletableFuture.completedFuture(null);
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
        return resolveReadResult(readResult, txId, null, null);
    }

    /**
     * Resolves a read result for RO transaction.
     *
     * @param readResult Read result to resolve.
     * @param timestamp Timestamp.
     * @param lastCommitted Action to get the latest committed row.
     * @return Resolved binary row.
     */
    private BinaryRow resolveReadResult(ReadResult readResult, HybridTimestamp timestamp, Supplier<BinaryRow> lastCommitted) {

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
     * @return Resolved binary row.
     */
    private BinaryRow resolveReadResult(
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
                    return readResult.binaryRow();
                } else {
                    // Should never happen, currently, locks prevent reading another transaction intents during RW requests.
                    throw new AssertionError("Mismatched transaction id, expectedTxId={" + txId + "},"
                            + " actualTxId={" + retrievedResultTxId + '}');
                }
            } else {
                if (!readResult.isWriteIntent()) {
                    return readResult.binaryRow();
                }

                CompletableFuture<BinaryRow> writeIntentResolutionFut = resolveWriteIntentAsync(
                        readResult, timestamp, lastCommitted);

                // RO request.
                return writeIntentResolutionFut.join();
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

    /**
     * Compounds a RAFT group unique name.
     *
     * @param tblId Table identifier.
     * @param partition Number of table partitions.
     * @return A RAFT group name.
     */
    @NotNull
    private String partitionRaftGroupName(UUID tblId, int partition) {
        return tblId + "_part_" + partition;
    }

    /**
     * @param tmstmp
     * @return
     */
    private HybridTimestampMessage hybridTimestamp(HybridTimestamp tmstmp) {
        return msgFactory.hybridTimestampMessage()
                .physical(tmstmp.getPhysical())
                .logical(tmstmp.getLogical())
                .build();
    }
}
