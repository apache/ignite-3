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
import static org.apache.ignite.internal.util.CollectionUtils.nullOrEmpty;
import static org.apache.ignite.lang.IgniteStringFormatter.format;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.ignite.hlc.HybridClock;
import org.apache.ignite.hlc.HybridTimestamp;
import org.apache.ignite.internal.binarytuple.BinaryTupleBuilder;
import org.apache.ignite.internal.replicator.exception.PrimaryReplicaMissException;
import org.apache.ignite.internal.replicator.exception.ReplicationException;
import org.apache.ignite.internal.replicator.exception.ReplicationTimeoutException;
import org.apache.ignite.internal.replicator.exception.UnsupportedReplicaRequestException;
import org.apache.ignite.internal.replicator.listener.ReplicaListener;
import org.apache.ignite.internal.replicator.message.ReplicaRequest;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.BinaryTuple;
import org.apache.ignite.internal.schema.BinaryTupleSchema;
import org.apache.ignite.internal.schema.BinaryTupleSchema.Element;
import org.apache.ignite.internal.schema.NativeTypes;
import org.apache.ignite.internal.storage.MvPartitionStorage;
import org.apache.ignite.internal.storage.PartitionTimestampCursor;
import org.apache.ignite.internal.storage.ReadResult;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.storage.index.HashIndexStorage;
import org.apache.ignite.internal.table.distributed.TableManager.TableIndex;
import org.apache.ignite.internal.table.distributed.command.FinishTxCommand;
import org.apache.ignite.internal.table.distributed.command.TxCleanupCommand;
import org.apache.ignite.internal.table.distributed.command.UpdateAllCommand;
import org.apache.ignite.internal.table.distributed.command.UpdateCommand;
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
import org.apache.ignite.internal.tx.LockKey;
import org.apache.ignite.internal.tx.LockManager;
import org.apache.ignite.internal.tx.LockMode;
import org.apache.ignite.internal.tx.TxManager;
import org.apache.ignite.internal.tx.message.TxCleanupReplicaRequest;
import org.apache.ignite.internal.tx.message.TxFinishReplicaRequest;
import org.apache.ignite.internal.util.Cursor;
import org.apache.ignite.lang.ErrorGroups.Replicator;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lang.IgniteInternalException;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.raft.client.Command;
import org.apache.ignite.raft.client.service.RaftGroupService;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/** Partition replication listener. */
public class PartitionReplicaListener implements ReplicaListener {
    private static final BinaryTupleSchema PK_KEY_SCHEMA = BinaryTupleSchema.create(new Element[]{
            new Element(NativeTypes.BYTES, false)
    });

    /** Replication group id. */
    private final String replicationGroupId;

    /** Partition id. */
    private final int partId;

    /** Primary key index. */
    public final HashIndexStorage pkConstraintStorage;

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

    private final Supplier<List<TableIndex>> activeIndexes;

    /**
     * The constructor.
     *
     * @param mvDataStorage Data storage.
     * @param raftClient Raft client.
     * @param txManager Transaction manager.
     * @param lockManager Lock manager.
     * @param tableId Table id.
     * @param hybridClock Hybrid clock.
     */
    public PartitionReplicaListener(
            MvPartitionStorage mvDataStorage,
            RaftGroupService raftClient,
            TxManager txManager,
            LockManager lockManager,
            int partId,
            String replicationGroupId,
            UUID tableId,
            Supplier<List<TableIndex>> activeIndexes,
            HashIndexStorage pkConstraintStorage,
            HybridClock hybridClock
    ) {
        this.mvDataStorage = mvDataStorage;
        this.raftClient = raftClient;
        this.txManager = txManager;
        this.lockManager = lockManager;
        this.partId = partId;
        this.replicationGroupId = replicationGroupId;
        this.tableId = tableId;
        this.activeIndexes = activeIndexes;
        this.pkConstraintStorage = pkConstraintStorage;

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
     * Processes retrieve batch for read only transaction.
     *
     * @param request Read only retrieve batch request.
     * @return Result future.
     */
    private CompletableFuture<Object> processReadOnlyScanRetrieveBatchAction(ReadOnlyScanRetrieveBatchReplicaRequest request) {
        UUID txId = request.transactionId();
        int batchCount = request.batchSize();

        IgniteUuid cursorId = new IgniteUuid(txId, request.scanId());

        ArrayList<BinaryRow> batchRows = new ArrayList<>(batchCount);

        @SuppressWarnings("resource") PartitionTimestampCursor cursor = cursors.computeIfAbsent(cursorId,
                id -> mvDataStorage.scan(HybridTimestamp.MAX_VALUE));

        while (batchRows.size() < batchCount && cursor.hasNext()) {
            BinaryRow resolvedReadResult = resolveReadResult(cursor.next(), null);

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

        if (request.requestType() !=  RequestType.RO_GET) {
            throw new IgniteInternalException(Replicator.REPLICA_COMMON_ERR,
                    format("Unknown single request [actionType={}]", request.requestType()));
        }

        //TODO: IGNITE-17868 Integrate indexes into rowIds resolution along with proper lock management on search rows.
        RowId rowId = rowIdByKey(searchKey, request.timestamp(), null);

        BinaryRow result = rowId != null ? resolveReadResult(mvDataStorage.read(rowId, request.timestamp()), null) : null;

        return CompletableFuture.completedFuture(result);
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
                    format("Unknown single request [actionType={}]", request.requestType()));
        }

        ArrayList<BinaryRow> result = new ArrayList<>(keyRows.size());

        for (ByteBuffer searchKey : keyRows) {
            //TODO: IGNITE-17868 Integrate indexes into rowIds resolution along with proper lock management on search rows.
            RowId rowId = rowIdByKey(searchKey, request.timestamp(), null);

            result.add(rowId != null ? resolveReadResult(mvDataStorage.read(rowId, request.timestamp()), null) : null);
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

                if (resolvedReadResult != null && resolvedReadResult.hasValue()) {
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
        List<String> aggregatedGroupIds = request.groups().values().stream()
                .flatMap(List::stream).map(IgniteBiTuple::get1).collect(Collectors.toList());

        UUID txId = request.txId();

        boolean commit = request.commit();

        CompletableFuture<Object> changeStateFuture = raftClient.run(
                new FinishTxCommand(
                        txId,
                        commit,
                        request.commitTimestamp(),
                        aggregatedGroupIds
                )
        );

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

        return raftClient
                .run(new TxCleanupCommand(request.txId(), request.commit(), request.commitTimestamp()))
                .thenRun(() -> lockManager.locks(request.txId()).forEachRemaining(lockManager::release));
    }

    /**
     * Find out a row id by an index.
     *
     * @param key Key to find.
     * @return Value or {@code null} if the key does not determine a value.
     */
    private CompletableFuture<@Nullable RowId> rowIdByKey(ByteBuffer key, UUID txId) {
        return lockManager.acquire(txId, new LockKey(tableId, new LockKey(key)), LockMode.S)
                .thenApply(ignored -> rowIdByKey(key, HybridTimestamp.MAX_VALUE, txId));

    }

    private @Nullable RowId rowIdByKey(ByteBuffer key, HybridTimestamp ts, @Nullable UUID txId) {
        try (Cursor<RowId> cursor = pkConstraintStorage.get(toPkIndexKey(key))) {
            for (RowId rowId : cursor) {
                BinaryRow row = resolveReadResult(mvDataStorage.read(rowId, ts), txId);

                if (row != null && row.hasValue()) {
                    return rowId;
                }
            }

            return null;
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

        switch (request.requestType()) {
            case RW_GET_ALL: {
                CompletableFuture<RowId>[] rowIdFuts = new CompletableFuture[request.binaryRows().size()];

                int i = 0;

                for (BinaryRow row : request.binaryRows()) {
                    rowIdFuts[i++] = rowIdByKey(row.keySlice(), txId)
                            .thenCompose(rowId -> {
                                if (rowId == null) {
                                    return CompletableFuture.completedFuture(null);
                                }

                                return takeLocksForGet(rowId, txId);
                            });
                }

                return allOf(rowIdFuts)
                        .thenCompose(ignored -> {
                            ArrayList<BinaryRow> result = new ArrayList<>(request.binaryRows().size());

                            for (int idx = 0; idx < request.binaryRows().size(); idx++) {
                                RowId lockedRowId = rowIdFuts[idx].join();

                                result.add(lockedRowId != null
                                        ? resolveReadResult(mvDataStorage.read(lockedRowId, HybridTimestamp.MAX_VALUE), txId) : null
                                );
                            }

                            return CompletableFuture.completedFuture(result);
                        });
            }
            case RW_DELETE_ALL: {
                CompletableFuture<RowId>[] rowIdLockFuts = new CompletableFuture[request.binaryRows().size()];

                int i = 0;

                for (BinaryRow row : request.binaryRows()) {
                    rowIdLockFuts[i++] = rowIdByKey(row.keySlice(), txId)
                        .thenCompose(rowId -> {
                            if (rowId == null) {
                                return CompletableFuture.completedFuture(null);
                            }

                            return takeLocksForDelete(row, rowId, txId)
                                    .thenApply(ignored -> rowId);
                        });
                }

                return allOf(rowIdLockFuts).thenCompose(ignore -> {
                    Map<RowId, BinaryRow> rowIdsToDelete = new HashMap<>();
                    Collection<BinaryRow> result = new ArrayList<>();

                    int futNum = 0;

                    for (BinaryRow row : request.binaryRows()) {
                        RowId lockedRowId = rowIdLockFuts[futNum++].join();

                        if (lockedRowId != null) {
                            rowIdsToDelete.put(lockedRowId, row);
                        } else {
                            result.add(row);
                        }
                    }

                    if (rowIdsToDelete.isEmpty()) {
                        return CompletableFuture.completedFuture(result);
                    }

                    return applyCmdWithExceptionHandling(new UpdateAllCommand(rowIdsToDelete, txId))
                            .thenApply(ignored -> result);
                });
            }
            case RW_DELETE_EXACT_ALL: {
                CompletableFuture<RowId>[] deleteExactLockFuts = new CompletableFuture[request.binaryRows().size()];

                int i = 0;

                for (BinaryRow row : request.binaryRows()) {
                    deleteExactLockFuts[i++] = rowIdByKey(row.keySlice(), txId)
                            .thenCompose(rowId -> {
                                if (rowId == null) {
                                    return CompletableFuture.completedFuture(null);
                                }

                                return takeLocksForDeleteExact(row, rowId, txId);
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

                    CompletableFuture raftFut = rowIdsToDelete.isEmpty() ? CompletableFuture.completedFuture(null)
                            : applyCmdWithExceptionHandling(new UpdateAllCommand(rowIdsToDelete, txId));

                    return raftFut.thenApply(ignored -> result);
                });
            }
            case RW_INSERT_ALL: {
                CompletableFuture<RowId>[] pkReadLockFuts = new CompletableFuture[request.binaryRows().size()];

                int i = 0;

                for (BinaryRow row : request.binaryRows()) {
                    pkReadLockFuts[i++] = rowIdByKey(row.keySlice(), txId);
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
                        return CompletableFuture.completedFuture(result);
                    }

                    CompletableFuture<RowId>[] insertLockFuts = new CompletableFuture[rowsToInsert.size()];

                    int idx = 0;
                    List<TableIndex> indexes = activeIndexes.get();

                    for (Map.Entry<RowId, BinaryRow> entry : rowsToInsert.entrySet()) {
                        insertLockFuts[idx++] = takeLocksForInsert(entry.getValue(), entry.getKey(), txId, indexes);
                    }

                    return CompletableFuture.allOf(insertLockFuts)
                            .thenCompose(ignored -> applyCmdWithExceptionHandling(new UpdateAllCommand(rowsToInsert, txId)))
                            .thenCompose(ignored -> {
                                CompletableFuture<?>[] cleanupLockFuts = new CompletableFuture[rowsToInsert.size()];

                                int idx0 = 0;

                                for (Map.Entry<RowId, BinaryRow> entry : rowsToInsert.entrySet()) {
                                    cleanupLockFuts[idx0++] = releaseAfterPutLockOnIndexes(indexes, entry.getValue(), entry.getKey(), txId);
                                }

                                return allOf(cleanupLockFuts);
                            })
                            .thenApply(ignored -> result);
                });
            }
            case RW_UPSERT_ALL: {
                CompletableFuture<RowId>[] rowIdFuts = new CompletableFuture[request.binaryRows().size()];

                int i = 0;

                List<TableIndex> indexes = activeIndexes.get();

                for (BinaryRow row : request.binaryRows()) {
                    rowIdFuts[i++] = rowIdByKey(row.keySlice(), txId)
                            .thenCompose(rowId -> {
                                boolean insert = rowId == null;

                                RowId rowId0 = insert ? new RowId(partId) : rowId;

                                return insert
                                        ? takeLocksForInsert(row, rowId0, txId, indexes)
                                        : takeLocksForUpdate(row, rowId0, txId, indexes);
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
                        return CompletableFuture.completedFuture(null);
                    }

                    return applyCmdWithExceptionHandling(new UpdateAllCommand(rowsToUpdate, txId))
                            .thenCompose(ignored -> {
                                CompletableFuture<?>[] cleanupLockFuts = new CompletableFuture[rowsToUpdate.size()];

                                int idx0 = 0;

                                for (Map.Entry<RowId, BinaryRow> entry : rowsToUpdate.entrySet()) {
                                    cleanupLockFuts[idx0++] = releaseAfterPutLockOnIndexes(indexes, entry.getValue(), entry.getKey(), txId);
                                }

                                return allOf(cleanupLockFuts);
                            })
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
        BinaryRow searchRow = request.binaryRow();

        ByteBuffer searchKey = searchRow.keySlice();

        UUID txId = request.transactionId();

        switch (request.requestType()) {
            case RW_GET: {
                return rowIdByKey(searchKey, txId)
                        .thenCompose(rowId -> {
                            if (rowId == null) {
                                return CompletableFuture.completedFuture(null);
                            }

                            return takeLocksForGet(rowId, txId)
                                    .thenApply(ignored -> resolveReadResult(mvDataStorage.read(rowId, HybridTimestamp.MAX_VALUE), txId));
                        });
            }
            case RW_DELETE: {
                return rowIdByKey(searchKey, txId)
                        .thenCompose(rowId -> {
                            if (rowId == null) {
                                return CompletableFuture.completedFuture(false);
                            }

                            return takeLocksForDelete(searchRow, rowId, txId)
                                    .thenCompose(ignored -> applyCmdWithExceptionHandling(new UpdateCommand(rowId, txId)))
                                    .thenApply(ignored -> true);

                        });
            }
            case RW_GET_AND_DELETE: {
                return rowIdByKey(searchKey, txId)
                        .thenCompose(rowId -> {
                            if (rowId == null) {
                                return CompletableFuture.completedFuture(null);
                            }

                            return takeLocksForDelete(searchRow, rowId, txId)
                                    .thenCompose(ignored -> {
                                        BinaryRow value = resolveReadResult(mvDataStorage.read(rowId, HybridTimestamp.MAX_VALUE), txId);

                                        return applyCmdWithExceptionHandling(new UpdateCommand(rowId, txId))
                                                .thenApply(ignored0 -> value);
                                    });

                        });
            }
            case RW_DELETE_EXACT: {
                return rowIdByKey(searchKey, txId)
                        .thenCompose(rowId -> {
                            if (rowId == null) {
                                return CompletableFuture.completedFuture(false);
                            }

                            return takeLocksForDeleteExact(searchRow, rowId, txId)
                                    .thenCompose(validatedRowId -> {
                                        if (validatedRowId == null) {
                                            return CompletableFuture.completedFuture(false);
                                        }

                                        return applyCmdWithExceptionHandling(new UpdateCommand(validatedRowId, txId))
                                                .thenApply(ignored -> true);
                                    });
                        });
            }
            case RW_INSERT: {
                return rowIdByKey(searchKey, txId)
                        .thenCompose(rowId -> {
                            if (rowId != null) {
                                return CompletableFuture.completedFuture(false);
                            }

                            RowId rowId0 = new RowId(partId);
                            List<TableIndex> indexes = activeIndexes.get();

                            return takeLocksForInsert(searchRow, rowId0, txId, indexes)
                                    .thenCompose(ignored -> applyCmdWithExceptionHandling(new UpdateCommand(rowId0, searchRow, txId)))
                                    .thenCompose(ignored -> releaseAfterPutLockOnIndexes(indexes, searchRow, rowId0, txId))
                                    .thenApply(ignored -> true);
                        });
            }
            case RW_UPSERT: {
                return rowIdByKey(searchKey, txId)
                        .thenCompose(rowId -> {
                            boolean insert = rowId == null;

                            RowId rowId0 = insert ? new RowId(partId) : rowId;
                            List<TableIndex> indexes = activeIndexes.get();

                            CompletableFuture<?> lockFut = insert
                                    ? takeLocksForInsert(searchRow, rowId0, txId, indexes)
                                    : takeLocksForUpdate(searchRow, rowId0, txId, indexes);

                            return lockFut
                                    .thenCompose(ignored -> applyCmdWithExceptionHandling(new UpdateCommand(rowId0, searchRow, txId)))
                                    .thenCompose(ignored -> releaseAfterPutLockOnIndexes(indexes, searchRow, rowId0, txId))
                                    .thenApply(ignored -> null);
                        });
            }
            case RW_GET_AND_UPSERT: {
                return rowIdByKey(searchKey, txId)
                        .thenCompose(rowId -> {
                            boolean insert = rowId == null;

                            RowId rowId0 = insert ? new RowId(partId) : rowId;
                            List<TableIndex> indexes = activeIndexes.get();

                            CompletableFuture<?> lockFut = insert
                                    ? takeLocksForInsert(searchRow, rowId0, txId, indexes)
                                    : takeLocksForUpdate(searchRow, rowId0, txId, indexes);

                            return lockFut
                                    .thenCompose(ignored -> {
                                        BinaryRow value = resolveReadResult(mvDataStorage.read(rowId, HybridTimestamp.MAX_VALUE), txId);

                                        return applyCmdWithExceptionHandling(new UpdateCommand(rowId0, searchRow, txId))
                                                .thenCompose(ignored0 -> releaseAfterPutLockOnIndexes(indexes, searchRow, rowId0, txId))
                                                .thenApply(ignored0 -> value);
                                    });
                        });
            }
            case RW_GET_AND_REPLACE: {
                return rowIdByKey(searchKey, txId)
                        .thenCompose(rowId -> {
                            if (rowId == null) {
                                return CompletableFuture.completedFuture(null);
                            }

                            List<TableIndex> indexes = activeIndexes.get();

                            return takeLocksForUpdate(searchRow, rowId, txId, indexes)
                                    .thenCompose(ignored -> {
                                        BinaryRow value = resolveReadResult(mvDataStorage.read(rowId, HybridTimestamp.MAX_VALUE), txId);

                                        return applyCmdWithExceptionHandling(new UpdateCommand(rowId, searchRow, txId))
                                                .thenCompose(ignored0 -> releaseAfterPutLockOnIndexes(indexes, searchRow, rowId, txId))
                                                .thenApply(ignored0 -> value);
                                    });
                        });
            }
            case RW_REPLACE_IF_EXIST: {
                return rowIdByKey(searchKey, txId)
                        .thenCompose(rowId -> {
                            if (rowId == null) {
                                return CompletableFuture.completedFuture(false);
                            }

                            List<TableIndex> indexes = activeIndexes.get();

                            return takeLocksForUpdate(searchRow, rowId, txId, indexes)
                                    .thenCompose(ignored -> applyCmdWithExceptionHandling(new UpdateCommand(rowId, searchRow, txId)))
                                    .thenCompose(ignored -> releaseAfterPutLockOnIndexes(indexes, searchRow, rowId, txId))
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
    private CompletableFuture<RowId> takeLocksForUpdate(BinaryRow tableRow, RowId rowId, UUID txId, List<TableIndex> indexes) {
        return lockManager.acquire(txId, new LockKey(tableId, tableRow.keySlice()), LockMode.X) // Index X lock
                .thenCompose(ignored -> lockManager.acquire(txId, new LockKey(tableId), LockMode.IX))
                .thenCompose(ignored -> lockManager.acquire(txId, new LockKey(tableId, rowId), LockMode.X))
                .thenCompose(ignored -> takeBeforePutLockOnIndexes(indexes, tableRow, rowId, txId))
                .thenApply(ignored -> rowId);
    }

    /**
     * Takes all required locks on a key, before inserting the value.
     *
     * @param tableRow Table row.
     * @param txId Transaction id.
     * @return Future completes with {@link RowId} or {@code null} if there is no value.
     */
    private CompletableFuture<RowId> takeLocksForInsert(BinaryRow tableRow, RowId rowId, UUID txId, List<TableIndex> indexes) {
        return lockManager.acquire(txId, new LockKey(tableId, tableRow.keySlice()), LockMode.X)
                .thenCompose(exclusiveIdxLock -> lockManager.acquire(txId, new LockKey(tableId), LockMode.IX)) // IX lock on table
                .thenCompose(ignored -> takeBeforePutLockOnIndexes(indexes, tableRow, rowId, txId))
                .thenApply(tblLock -> rowId);
    }

    private CompletableFuture<?> takeBeforePutLockOnIndexes(List<TableIndex> indexes, BinaryRow tableRow, RowId rowId, UUID txId) {
        if (nullOrEmpty(indexes)) {
            return CompletableFuture.completedFuture(null);
        }

        CompletableFuture<?>[] locks = new CompletableFuture[indexes.size()];
        int idx = 0;

        for (TableIndex locker : indexes) {
            locks[idx++] = locker.beforePut(txId, rowId, tableRow);
        }

        return CompletableFuture.allOf(locks);
    }

    private CompletableFuture<?> releaseAfterPutLockOnIndexes(List<TableIndex> indexes, BinaryRow tableRow, RowId rowId, UUID txId) {
        if (nullOrEmpty(indexes)) {
            return CompletableFuture.completedFuture(null);
        }

        CompletableFuture<?>[] locks = new CompletableFuture[indexes.size()];
        int idx = 0;

        for (TableIndex locker : indexes) {
            locks[idx++] = locker.afterPut(txId, rowId, tableRow);
        }

        return CompletableFuture.allOf(locks);
    }

    private BinaryTuple toPkIndexKey(ByteBuffer bytes) {
        return new BinaryTuple(
                PK_KEY_SCHEMA,
                new BinaryTupleBuilder(1, false)
                .appendElementBytes(bytes)
                .build()
        );
    }

    /**
     * Takes all required locks on a key, before deleting the value.
     *
     * @param txId      Transaction id.
     * @return Future completes with {@link RowId} or {@code null} if there is no value for remove.
     */
    private CompletableFuture<RowId> takeLocksForDeleteExact(BinaryRow tableRow, RowId rowId, UUID txId) {
        return lockManager.acquire(txId, new LockKey(tableId, tableRow.keySlice()), LockMode.X)  // Index X lock
                .thenCompose(ignored -> lockManager.acquire(txId, new LockKey(tableId), LockMode.IX)) // IX lock on table
                .thenCompose(ignored -> lockManager.acquire(txId, new LockKey(tableId, rowId), LockMode.S)) // S lock on RowId
                .thenCompose(ignored -> {
                    BinaryRow curVal = resolveReadResult(mvDataStorage.read(rowId, HybridTimestamp.MAX_VALUE), txId);

                    if (curVal != null && equalValues(curVal, tableRow)) {
                        return lockManager.acquire(txId, new LockKey(tableId, rowId), LockMode.X) // X lock on RowId
                                .thenApply(exclusiveRowLock -> rowId);
                    }

                    return CompletableFuture.completedFuture(null);
                });
    }

    /**
     * Takes all required locks on a key, before deleting the value.
     *
     * @param txId      Transaction id.
     * @return Future completes with {@link RowId} or {@code null} if there is no value for the key.
     */
    private CompletableFuture<RowId> takeLocksForDelete(BinaryRow tableRow, RowId rowId, UUID txId) {
        return lockManager.acquire(txId, new LockKey(tableId, tableRow.keySlice()), LockMode.X)
                .thenCompose(ignored -> lockManager.acquire(txId, new LockKey(tableId), LockMode.IX)) // IX lock on table
                .thenCompose(ignored -> lockManager.acquire(txId, new LockKey(tableId, rowId), LockMode.X)) // X lock on RowId
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
    private CompletableFuture<Object> processTwoEntriesAction(ReadWriteSwapRowReplicaRequest request) {
        BinaryRow newRow = request.binaryRow();
        BinaryRow expectedRow = request.oldBinaryRow();

        ByteBuffer searchKey = newRow.keySlice();

        UUID txId = request.transactionId();

        if (request.requestType() == RequestType.RW_REPLACE) {
            return rowIdByKey(searchKey, txId)
                    .thenCompose(rowId -> {
                        if (rowId == null) {
                            return CompletableFuture.completedFuture(false);
                        }

                        List<TableIndex> indexes = activeIndexes.get();

                        return takeLocksForReplace(expectedRow, newRow, rowId, txId, indexes)
                                .thenCompose(validatedRowId -> {
                                    if (validatedRowId == null) {
                                        return CompletableFuture.completedFuture(false);
                                    }

                                    return applyCmdWithExceptionHandling(new UpdateCommand(validatedRowId, newRow, txId))
                                            .thenCompose(ignored -> releaseAfterPutLockOnIndexes(indexes, newRow, rowId, txId))
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
    private CompletableFuture<RowId> takeLocksForReplace(BinaryRow expectedRow, BinaryRow newRow,
            RowId rowId, UUID txId, List<TableIndex> indexes) {
        return lockManager.acquire(txId, new LockKey(tableId, expectedRow.keySlice()), LockMode.X) // Index X lock
                .thenCompose(ignored -> lockManager.acquire(txId, new LockKey(tableId), LockMode.IX))
                .thenCompose(ignored -> lockManager.acquire(txId, new LockKey(tableId, rowId), LockMode.S))
                .thenCompose(ignored -> {
                    BinaryRow curVal = resolveReadResult(mvDataStorage.read(rowId, HybridTimestamp.MAX_VALUE), txId);

                    if (curVal != null && equalValues(curVal, expectedRow)) {
                        return lockManager.acquire(txId, new LockKey(tableId, rowId), LockMode.X) // X lock on RowId
                                .thenCompose(ignored1 -> takeBeforePutLockOnIndexes(indexes, newRow, rowId, txId))
                                .thenApply(rowLock -> rowId);
                    }

                    return CompletableFuture.completedFuture(null);
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
     * @return Resolved binary row.
     */
    private BinaryRow resolveReadResult(ReadResult readResult, @Nullable UUID txId) {
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
                // RO request.
                // TODO: IGNITE-17637 Implement a commit partition path write intent resolution logic
                return readResult.binaryRow();
            }
        }
    }
}
