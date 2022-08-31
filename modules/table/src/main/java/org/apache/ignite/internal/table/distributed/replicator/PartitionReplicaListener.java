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

package org.apache.ignite.internal.table.distributed.replicator;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import org.apache.ignite.internal.replicator.exception.ReplicationException;
import org.apache.ignite.internal.replicator.exception.ReplicationTimeoutException;
import org.apache.ignite.internal.replicator.exception.UnsupportedReplicaRequestException;
import org.apache.ignite.internal.replicator.listener.ReplicaListener;
import org.apache.ignite.internal.replicator.message.ReplicaRequest;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.ByteBufferRow;
import org.apache.ignite.internal.storage.MvPartitionStorage;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.table.distributed.command.DeleteAllCommand;
import org.apache.ignite.internal.table.distributed.command.DeleteCommand;
import org.apache.ignite.internal.table.distributed.command.InsertAndUpdateAllCommand;
import org.apache.ignite.internal.table.distributed.command.InsertCommand;
import org.apache.ignite.internal.table.distributed.command.UpdateCommand;
import org.apache.ignite.internal.table.distributed.replication.request.ReadWriteMultiRowReplicaRequest;
import org.apache.ignite.internal.table.distributed.replication.request.ReadWriteScanCloseReplicaRequest;
import org.apache.ignite.internal.table.distributed.replication.request.ReadWriteScanRetrieveBatchReplicaRequest;
import org.apache.ignite.internal.table.distributed.replication.request.ReadWriteSingleRowReplicaRequest;
import org.apache.ignite.internal.table.distributed.replication.request.ReadWriteSwapRowReplicaRequest;
import org.apache.ignite.internal.tx.Lock;
import org.apache.ignite.internal.tx.LockKey;
import org.apache.ignite.internal.tx.LockManager;
import org.apache.ignite.internal.tx.LockMode;
import org.apache.ignite.internal.util.Cursor;
import org.apache.ignite.lang.ErrorGroups.Replicator;
import org.apache.ignite.lang.IgniteInternalException;
import org.apache.ignite.lang.IgniteStringFormatter;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.raft.client.Command;
import org.apache.ignite.raft.client.service.RaftGroupService;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/** Partition replication listener. */
public class PartitionReplicaListener implements ReplicaListener {
    /** Replication group id. */
    private final String replicationGroupId;

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

    /** Lock manager. */
    private final LockManager lockManager;

    //TODO: https://issues.apache.org/jira/browse/IGNITE-17205 Temporary solution until the implementation of the primary index is done.
    /** Dummy primary index. */
    private final ConcurrentHashMap<ByteBuffer, RowId> primaryIndex;

    /**
     * Cursors map. The key of the map is internal Ignite uuid which consists of a transaction id ({@link UUID}) and a cursor id ({@link
     * Long}).
     */
    private final ConcurrentNavigableMap<IgniteUuid, Cursor<BinaryRow>> cursors;

    /**
     * The constructor.
     *
     * @param mvDataStorage      Data storage.
     * @param raftClient         Raft client.
     * @param lockManager        Lock manager.
     * @param replicationGroupId Replication group id.
     * @param tableId            Table id.
     * @param primaryIndex       Primary index.
     */
    public PartitionReplicaListener(
            MvPartitionStorage mvDataStorage,
            RaftGroupService raftClient,
            LockManager lockManager,
            String replicationGroupId,
            UUID tableId,
            ConcurrentHashMap<ByteBuffer, RowId> primaryIndex
    ) {
        this.mvDataStorage = mvDataStorage;
        this.raftClient = raftClient;
        this.lockManager = lockManager;
        this.replicationGroupId = replicationGroupId;
        this.tableId = tableId;
        this.primaryIndex = primaryIndex;

        //TODO: IGNITE-17479 Integrate indexes into replicaListener command handlers
        this.indexScanId = new UUID(tableId.getMostSignificantBits(), tableId.getLeastSignificantBits() + 1);
        this.indexPkId = new UUID(tableId.getMostSignificantBits(), tableId.getLeastSignificantBits() + 2);

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
        } else {
            throw new UnsupportedReplicaRequestException(request.getClass());
        }
    }

    /**
     * Close all cursors connected with a transaction.
     *
     * @param txId Transaction id.
     * @throws Exception When an issue happens on cursor closing.
     */
    public void closeAllTransactionCursors(UUID txId) {
        var lowCursorId = new IgniteUuid(txId, Long.MIN_VALUE);
        var upperCursorId = new IgniteUuid(txId, Long.MAX_VALUE);

        Map<IgniteUuid, Cursor<BinaryRow>> txCursors = cursors.subMap(lowCursorId, true, upperCursorId, true);

        ReplicationException ex = null;

        for (Cursor<BinaryRow> cursor : txCursors.values()) {
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
     * Precesses scan close request.
     *
     * @param request Scan close request operation.
     * @return Listener response.
     */
    private void processScanCloseAction(ReadWriteScanCloseReplicaRequest request) {
        UUID txId = request.transactionId();

        IgniteUuid cursorId = new IgniteUuid(txId, request.scanId());

        Cursor<BinaryRow> cursor = cursors.remove(cursorId);

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
     * Precesses scan retrieve batch request.
     *
     * @param request Scan retrieve batch request operation.
     * @return Listener response.
     */
    private CompletableFuture<Object> processScanRetrieveBatchAction(ReadWriteScanRetrieveBatchReplicaRequest request) {
        UUID txId = request.transactionId();
        int batchCount = request.batchSize();

        IgniteUuid cursorId = new IgniteUuid(txId, request.scanId());

        return lockManager.acquire(txId, new LockKey(tableId), LockMode.SHARED).thenCompose(tblLock -> {
            ArrayList<BinaryRow> batchRows = new ArrayList<>(batchCount);

            Cursor<BinaryRow> cursor = cursors.computeIfAbsent(cursorId, id -> mvDataStorage.scan(request.rowFilter(), txId));

            for (int i = 0; i < batchCount || !cursor.hasNext(); i++) {
                batchRows.add(cursor.next());
            }

            return CompletableFuture.completedFuture(batchRows);
        });
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
    private RowId rowIdByKey(@NotNull UUID indexId, BinaryRow key) {
        if (indexPkId.equals(indexId)) {
            return primaryIndex.get(key.keySlice());
        }

        if (indexScanId.equals(indexId)) {
            RowId[] rowIdHolder = new RowId[1];

            mvDataStorage.forEach((rowId, binaryRow) -> {
                if (rowIdHolder[0] == null && binaryRow.keySlice().equals(key.keySlice())) {
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
        Collection<BinaryRow> keyRows = request.binaryRows().stream().map(br -> new ByteBufferRow(br.keySlice())).collect(
                Collectors.toList());
        Map<BinaryRow, BinaryRow> keyToRows = request.binaryRows().stream().collect(
                Collectors.toMap(br -> new ByteBufferRow(br.keySlice()), br -> br));

        UUID indexId = indexIdOrDefault(indexPkId/*request.indexToUse()*/);

        UUID txId = request.transactionId();

        switch (request.requestType()) {
            case RW_GET_ALL: {
                CompletableFuture<RowId>[] getLockFuts = new CompletableFuture[keyRows.size()];

                int i = 0;

                for (BinaryRow searchKey : keyRows) {
                    getLockFuts[i++] = takeLocsForGet(searchKey, indexId, txId);
                }

                return CompletableFuture.allOf(getLockFuts).thenApply(ignore -> {
                    ArrayList<BinaryRow> result = new ArrayList<>(keyRows.size());

                    for (int futNum = 0; futNum < keyRows.size(); futNum++) {
                        RowId lockedRowId = getLockFuts[futNum].join();

                        result.add(lockedRowId != null ? mvDataStorage.read(lockedRowId, txId) : null);
                    }

                    return result;
                });
            }
            case RW_DELETE_ALL: {
                CompletableFuture<RowId>[] deleteLockFuts = new CompletableFuture[keyRows.size()];

                int i = 0;

                for (BinaryRow searchKey : keyRows) {
                    deleteLockFuts[i++] = takeLocksForDelete(searchKey, indexId, txId);
                }

                return CompletableFuture.allOf(deleteLockFuts).thenApply(ignore -> {
                    Collection<RowId> rowIdsToDelete = new ArrayList<>();
                    Collection<BinaryRow> result = new ArrayList<>();

                    int futNum = 0;

                    for (BinaryRow searchKey : keyRows) {
                        RowId lockedRowId = deleteLockFuts[futNum++].join();

                        if (lockedRowId != null) {
                            rowIdsToDelete.add(lockedRowId);
                        } else {
                            result.add(keyToRows.get(searchKey));
                        }
                    }

                    CompletableFuture raftFut = rowIdsToDelete.isEmpty() ? CompletableFuture.completedFuture(null)
                            : applyCmdWithExceptionHandling(new DeleteAllCommand(rowIdsToDelete, txId));

                    return raftFut.thenApply(ignored -> result);
                });
            }
            case RW_DELETE_EXACT_ALL: {
                CompletableFuture<RowId>[] deleteExactLockFuts = new CompletableFuture[keyRows.size()];

                int i = 0;

                for (BinaryRow searchKey : keyRows) {
                    deleteExactLockFuts[i++] = takeLocksForDeleteExact(searchKey, keyToRows.get(searchKey), indexId, txId);
                }

                return CompletableFuture.allOf(deleteExactLockFuts).thenApply(ignore -> {
                    Collection<RowId> rowIdsToDelete = new ArrayList<>();
                    Collection<BinaryRow> result = new ArrayList<>();

                    int futNum = 0;

                    for (BinaryRow searchKey : keyRows) {
                        RowId lockedRowId = deleteExactLockFuts[futNum++].join();

                        if (lockedRowId != null) {
                            rowIdsToDelete.add(lockedRowId);
                        } else {
                            result.add(keyToRows.get(searchKey));
                        }
                    }

                    CompletableFuture raftFut = rowIdsToDelete.isEmpty() ? CompletableFuture.completedFuture(null)
                            : applyCmdWithExceptionHandling(new DeleteAllCommand(rowIdsToDelete, txId));

                    return raftFut.thenApply(ignored -> result);
                });
            }
            case RW_INSERT_ALL: {
                CompletableFuture<RowId>[] insertLockFuts = new CompletableFuture[keyRows.size()];

                int i = 0;

                for (BinaryRow searchKey : keyRows) {
                    insertLockFuts[i++] = takeLocksForInsert(searchKey, indexId, txId);
                }

                return CompletableFuture.allOf(insertLockFuts).thenApply(ignore -> {
                    Collection<BinaryRow> result = new ArrayList<>();
                    Collection<BinaryRow> rowsToInsert = new ArrayList<>();

                    int futNum = 0;

                    for (BinaryRow searchKey : keyRows) {
                        RowId lockedRow = insertLockFuts[futNum++].join();

                        if (lockedRow != null) {
                            result.add(keyToRows.get(searchKey));
                        } else {
                            rowsToInsert.add(keyToRows.get(searchKey));
                        }
                    }

                    CompletableFuture raftFut = rowsToInsert.isEmpty() ? CompletableFuture.completedFuture(null)
                            : applyCmdWithExceptionHandling(new InsertAndUpdateAllCommand(rowsToInsert, null, txId));

                    return raftFut.thenApply(ignored -> result);
                });
            }
            case RW_UPSERT_ALL: {
                CompletableFuture<RowId>[] upsertLockFuts = new CompletableFuture[keyRows.size()];

                int i = 0;

                for (BinaryRow searchKey : keyRows) {
                    upsertLockFuts[i++] = takeLocksForUpsert(searchKey, indexId, txId);
                }

                return CompletableFuture.allOf(upsertLockFuts).thenApply(ignore -> {
                    Collection<BinaryRow> rowsToInsert = new ArrayList<>();
                    Map<RowId, BinaryRow> rowsToUpdate = new HashMap<>();

                    int futNum = 0;

                    for (BinaryRow searchKey : keyRows) {
                        RowId lockedRow = upsertLockFuts[futNum++].join();

                        if (lockedRow != null) {
                            rowsToUpdate.put(lockedRow, keyToRows.get(searchKey));
                        } else {
                            rowsToInsert.add(keyToRows.get(searchKey));
                        }
                    }

                    CompletableFuture raftFut = rowsToInsert.isEmpty() ? CompletableFuture.completedFuture(null)
                            : applyCmdWithExceptionHandling(new InsertAndUpdateAllCommand(rowsToInsert, rowsToUpdate, txId));

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
        BinaryRow searchKey = new ByteBufferRow(request.binaryRow().keySlice());

        BinaryRow searchRow = request.binaryRow();

        UUID indexId = indexIdOrDefault(indexPkId/*request.indexToUse()*/);

        UUID txId = request.transactionId();

        switch (request.requestType()) {
            case RW_GET: {
                CompletableFuture<RowId> lockFut = takeLocsForGet(searchKey, indexId, txId);

                return lockFut.thenApply(lockedRowId -> {
                    BinaryRow result = lockedRowId != null ? mvDataStorage.read(lockedRowId, txId) : null;

                    return result;
                });
            }
            case RW_DELETE: {
                CompletableFuture<RowId> lockFut = takeLocksForDelete(searchKey, indexId, txId);

                return lockFut.thenCompose(lockedRowId -> {
                    boolean removed = lockedRowId != null;

                    CompletableFuture raftFut = removed ? applyCmdWithExceptionHandling(new DeleteCommand(lockedRowId, txId)) :
                            CompletableFuture.completedFuture(null);

                    return raftFut.thenApply(ignored -> removed);
                });
            }
            case RW_GET_AND_DELETE: {
                CompletableFuture<RowId> lockFut = takeLocksForDelete(searchKey, indexId, txId);

                return lockFut.thenCompose(lockedRowId -> {
                    BinaryRow lockedRow = lockedRowId != null ? mvDataStorage.read(lockedRowId, txId) : null;

                    CompletableFuture raftFut = lockedRowId != null ? applyCmdWithExceptionHandling(new DeleteCommand(lockedRowId, txId)) :
                            CompletableFuture.completedFuture(null);

                    return raftFut.thenApply(ignored -> lockedRow);
                });
            }
            case RW_DELETE_EXACT: {
                CompletableFuture<RowId> lockFut = takeLocksForDeleteExact(searchKey, searchRow, indexId, txId);

                return lockFut.thenCompose(lockedRow -> {
                    boolean removed = lockedRow != null;

                    CompletableFuture raftFut = removed ? applyCmdWithExceptionHandling(new DeleteCommand(lockedRow, txId)) :
                            CompletableFuture.completedFuture(null);

                    return raftFut.thenApply(ignored -> removed);
                });
            }
            case RW_INSERT: {
                CompletableFuture<RowId> lockFut = takeLocksForInsert(searchKey, indexId, txId);

                return lockFut.thenApply(lockedRowId -> {
                    boolean inserted = lockedRowId == null;

                    CompletableFuture raftFut = lockedRowId == null ? applyCmdWithExceptionHandling(new InsertCommand(searchRow, txId)) :
                            CompletableFuture.completedFuture(null);

                    return raftFut.thenApply(ignored -> inserted);
                });
            }
            case RW_UPSERT: {
                CompletableFuture<RowId> lockFut = takeLocksForUpsert(searchKey, indexId, txId);

                return lockFut.thenApply(lockedRowId -> {
                    CompletableFuture raftFut =
                            lockedRowId != null ? applyCmdWithExceptionHandling(new UpdateCommand(lockedRowId, searchRow, txId)) :
                                    applyCmdWithExceptionHandling(new InsertCommand(searchRow, txId));

                    return raftFut.thenApply(ignored -> null);
                });
            }
            case RW_GET_AND_UPSERT: {
                return lockManager.acquire(txId, new LockKey(indexId, searchKey), LockMode.EXCLUSIVE)
                        .thenCompose(idxLock -> { // Index X lock
                            RowId rowId = rowIdByKey(indexId, searchKey);

                            return lockManager.acquire(txId, new LockKey(tableId), LockMode.INTENTION_EXCLUSIVE)
                                    .thenCompose(tblLock -> { // IX lock on table
                                        CompletableFuture<Lock> rowLockFut = (rowId != null)
                                                ? lockManager.acquire(txId, new LockKey(tableId, rowId), LockMode.EXCLUSIVE)
                                                // X lock on RowId
                                                : CompletableFuture.completedFuture(null);

                                        return rowLockFut.thenApply(rowLock -> {
                                            BinaryRow result = rowId != null ? mvDataStorage.read(rowId, txId) : null;

                                            CompletableFuture raftFut =
                                                    rowId != null ? applyCmdWithExceptionHandling(new UpdateCommand(rowId, searchRow, txId))
                                                            : applyCmdWithExceptionHandling(new InsertCommand(searchRow, txId));

                                            return raftFut.thenApply(ignored -> result);
                                        });
                                    });
                        });
            }
            case RW_GET_AND_REPLACE: {
                CompletableFuture<RowId> idxLockFut = lockManager.acquire(txId, new LockKey(indexId, searchKey), LockMode.SHARED)
                        .thenCompose(sharedIdxLock -> { // Index S lock
                            RowId rowId = rowIdByKey(indexId, searchKey);

                            if (rowId != null) {
                                return lockManager.acquire(txId, new LockKey(indexId, searchKey), LockMode.EXCLUSIVE)
                                        .thenApply(exclusiveIdxLock -> rowId); // Index X lock
                            }

                            return CompletableFuture.completedFuture(null);
                        });

                return idxLockFut.thenCompose(lockedRowId -> {
                    return lockManager.acquire(txId, new LockKey(tableId), LockMode.INTENTION_EXCLUSIVE)
                            .thenCompose(tblLock -> { // IX lock on table
                                CompletableFuture<BinaryRow> rowLockFut;

                                if (lockedRowId != null) {
                                    rowLockFut = lockManager.acquire(txId, new LockKey(tableId, lockedRowId), LockMode.EXCLUSIVE)
                                            .thenApply(rowLock -> // X lock on RowId
                                                    mvDataStorage.read(lockedRowId, txId)
                                            );
                                } else {
                                    rowLockFut = CompletableFuture.completedFuture(null);
                                }

                                return rowLockFut.thenApply(lockedRow -> {
                                    CompletableFuture raftFut = lockedRow == null ? CompletableFuture.completedFuture(null) :
                                            applyCmdWithExceptionHandling(new UpdateCommand(lockedRowId, lockedRow, txId));

                                    return raftFut.thenApply(ignored -> lockedRow);
                                });
                            });
                });
            }
            case RW_REPLACE_IF_EXIST: {
                CompletableFuture<RowId> lockFut = takeLocksForReplaceIfExist(searchKey, indexId, txId);

                return lockFut.thenApply(lockedRowId -> {
                    boolean replaced = lockedRowId != null;

                    CompletableFuture raftFut = replaced ? applyCmdWithExceptionHandling(new UpdateCommand(lockedRowId, searchRow, txId)) :
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
    private CompletableFuture<RowId> takeLocksForReplaceIfExist(BinaryRow searchKey, UUID indexId, UUID txId) {
        return lockManager.acquire(txId, new LockKey(indexId, searchKey), LockMode.SHARED).thenCompose(shareIdxLock -> { // Index R lock
            RowId rowId = rowIdByKey(indexId, searchKey);

            CompletableFuture<Lock> idxLockFut = rowId != null
                    ? lockManager.acquire(txId, new LockKey(indexId, searchKey), LockMode.EXCLUSIVE) // Index X lock
                    : CompletableFuture.completedFuture(null);

            return idxLockFut.thenCompose(exclusiveIdxLock -> lockManager.acquire(txId, new LockKey(tableId), LockMode.INTENTION_EXCLUSIVE)
                    .thenCompose(tblLock -> { // IX lock on table
                        if (rowId != null) {
                            RowId rowIdToLock = rowId;

                            return lockManager.acquire(txId, new LockKey(tableId, rowId), LockMode.EXCLUSIVE)
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
    private CompletableFuture<RowId> takeLocksForUpsert(BinaryRow searchKey, UUID indexId, UUID txId) {
        return lockManager.acquire(txId, new LockKey(indexId, searchKey), LockMode.EXCLUSIVE).thenCompose(idxLock -> { // Index X lock
            RowId rowId = rowIdByKey(indexId, searchKey);

            return lockManager.acquire(txId, new LockKey(tableId), LockMode.INTENTION_EXCLUSIVE)
                    .thenCompose(tblLock -> { // IX lock on table
                        if (rowId != null) {
                            return lockManager.acquire(txId, new LockKey(tableId, rowId), LockMode.EXCLUSIVE)
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
    private CompletableFuture<RowId> takeLocksForInsert(BinaryRow searchKey, UUID indexId, UUID txId) {
        return lockManager.acquire(txId, new LockKey(indexId, searchKey), LockMode.SHARED) // Index S lock
                .thenCompose(sharedIdxLock -> {
                    RowId rowId = rowIdByKey(indexId, searchKey);

                    if (rowId == null) {
                        return lockManager.acquire(txId, new LockKey(indexId, searchKey), LockMode.EXCLUSIVE) // Index X lock
                                .thenCompose(exclusiveIdxLock ->
                                        lockManager.acquire(txId, new LockKey(tableId), LockMode.INTENTION_EXCLUSIVE) // IX lock on table
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
    private CompletableFuture<RowId> takeLocksForDeleteExact(BinaryRow searchKey, BinaryRow searchRow, UUID indexId, UUID txId) {
        return lockManager.acquire(txId, new LockKey(indexId, searchKey), LockMode.EXCLUSIVE).thenCompose(idxLock -> { // Index X lock
            RowId rowId = rowIdByKey(indexId, searchKey);

            return lockManager.acquire(txId, new LockKey(tableId), LockMode.INTENTION_EXCLUSIVE) // IX lock on table
                    .thenCompose(tblLock -> {
                        CompletableFuture<RowId> rowLockFut;

                        if (rowId != null) {
                            rowLockFut = lockManager.acquire(txId, new LockKey(tableId, rowId), LockMode.SHARED) // S lock on RowId
                                    .thenCompose(sharedRowLock -> {
                                        BinaryRow curVal = mvDataStorage.read(rowId, txId);

                                        if (equalValues(curVal, searchRow)) {
                                            return lockManager.acquire(txId, new LockKey(tableId, rowId),
                                                            LockMode.EXCLUSIVE) // X lock on RowId
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
    private CompletableFuture<RowId> takeLocksForDelete(BinaryRow searchKey, UUID indexId, UUID txId) {
        return lockManager.acquire(txId, new LockKey(indexId, searchKey), LockMode.EXCLUSIVE).thenCompose(idxLock -> { // Index X lock
            RowId rowId = rowIdByKey(indexId, searchKey);

            return lockManager.acquire(txId, new LockKey(tableId), LockMode.INTENTION_EXCLUSIVE) // IX lock on table
                    .thenCompose(tblLock -> {
                        if (rowId != null) {
                            return lockManager.acquire(txId, new LockKey(tableId, rowId), LockMode.EXCLUSIVE) // X lock on RowId
                                    .thenApply(rowLock -> rowId);
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
    private CompletableFuture<RowId> takeLocsForGet(BinaryRow searchKey, UUID indexId, UUID txId) {
        return lockManager.acquire(txId, new LockKey(indexId, searchKey), LockMode.SHARED).thenCompose(idxLock -> { // Index S lock
            RowId rowId = rowIdByKey(indexId, searchKey);

            return lockManager.acquire(txId, new LockKey(tableId), LockMode.INTENTION_SHARED).thenCompose(tblLock -> { // IS lock on table
                if (rowId != null) {
                    return lockManager.acquire(txId, new LockKey(tableId, rowId), LockMode.SHARED) // S lock on RowId
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
        BinaryRow searchKey = new ByteBufferRow(request.binaryRow().keySlice());

        BinaryRow searchRow = request.binaryRow();
        BinaryRow oldRow = request.oldBinaryRow();

        UUID indexId = indexIdOrDefault(indexPkId/*request.indexToUse()*/);

        UUID txId = request.transactionId();

        switch (request.requestType()) {
            case RW_REPLACE: {
                CompletableFuture<RowId> lockFut = takeLocsForReplace(searchKey, oldRow, indexId, txId);

                return lockFut.thenApply(lockedRowId -> {
                    boolean replaced = lockedRowId != null;

                    CompletableFuture raftFut = replaced ? applyCmdWithExceptionHandling(new UpdateCommand(lockedRowId, searchRow, txId)) :
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
    private CompletableFuture<RowId> takeLocsForReplace(BinaryRow searchKey, BinaryRow oldRow, UUID indexId, UUID txId) {
        return lockManager.acquire(txId, new LockKey(indexId, searchKey), LockMode.SHARED).thenCompose(shareIdxLock -> { // Index R lock
            RowId rowId = rowIdByKey(indexId, searchKey);

            CompletableFuture<Lock> idxLockFut = rowId != null
                    ? lockManager.acquire(txId, new LockKey(indexId, searchKey), LockMode.EXCLUSIVE) // Index X lock
                    : CompletableFuture.completedFuture(null);

            return idxLockFut.thenCompose(exclusiveIdxLock -> lockManager.acquire(txId, new LockKey(tableId), LockMode.INTENTION_EXCLUSIVE)
                    .thenCompose(tblLock -> { // IX lock on table
                        CompletableFuture<RowId> rowLockFut;

                        if (rowId != null) {
                            rowLockFut = lockManager.acquire(txId, new LockKey(tableId, rowId), LockMode.SHARED) // S lock on RowId
                                    .thenCompose(sharedRowLock -> {
                                        BinaryRow curVal = mvDataStorage.read(rowId, txId);

                                        if (equalValues(curVal, oldRow)) {
                                            return lockManager.acquire(txId, new LockKey(tableId, rowId),
                                                            LockMode.EXCLUSIVE) // X lock on RowId
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
}
