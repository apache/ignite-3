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
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.replicator.exception.ReplicationException;
import org.apache.ignite.internal.replicator.listener.ListenerCompoundResponse;
import org.apache.ignite.internal.replicator.listener.ListenerFutureResponse;
import org.apache.ignite.internal.replicator.listener.ListenerInstantResponse;
import org.apache.ignite.internal.replicator.listener.ListenerResponse;
import org.apache.ignite.internal.replicator.listener.ReplicaListener;
import org.apache.ignite.internal.replicator.message.ReplicaRequest;
import org.apache.ignite.internal.replicator.message.ReplicaRequestLocator;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.ByteBufferRow;
import org.apache.ignite.internal.storage.MvPartitionStorage;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.table.distributed.command.DeleteAllCommand;
import org.apache.ignite.internal.table.distributed.command.DeleteCommand;
import org.apache.ignite.internal.table.distributed.command.DeleteExactAllCommand;
import org.apache.ignite.internal.table.distributed.command.InsertAllCommand;
import org.apache.ignite.internal.table.distributed.command.InsertCommand;
import org.apache.ignite.internal.table.distributed.command.UpsertAllCommand;
import org.apache.ignite.internal.table.distributed.command.UpsertCommand;
import org.apache.ignite.internal.table.distributed.replication.request.ReadWriteDualRowReplicaRequest;
import org.apache.ignite.internal.table.distributed.replication.request.ReadWriteMultiRowReplicaRequest;
import org.apache.ignite.internal.table.distributed.replication.request.ReadWriteSingleRowReplicaRequest;
import org.apache.ignite.internal.tx.Lock;
import org.apache.ignite.internal.tx.LockException;
import org.apache.ignite.internal.tx.LockKey;
import org.apache.ignite.internal.tx.LockManager;
import org.apache.ignite.internal.tx.LockMode;
import org.apache.ignite.lang.ErrorGroups.Replicator;
import org.apache.ignite.lang.IgniteInternalException;
import org.apache.ignite.lang.IgniteStringFormatter;
import org.apache.ignite.raft.client.service.RaftGroupService;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/** Partition replication listener. */
public class PartitionReplicaListener implements ReplicaListener {
    /** Logger. */
    private static final IgniteLogger LOG = Loggers.forClass(PartitionReplicaListener.class);

    /** Primary key id. */
    public static final UUID INDEX_PK_ID = new UUID(0L, 2L);

    /** Scan index id. */
    public static final UUID INDEX_SCAN_ID = new UUID(0L, 1L);

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
     * The constructor.
     *
     * @param mvDataStorage Data storage.
     * @param raftClient    Raft client.
     * @param lockManager   Lock manager.
     */
    public PartitionReplicaListener(
            MvPartitionStorage mvDataStorage,
            RaftGroupService raftClient,
            LockManager lockManager,
            UUID tableId,
            ConcurrentHashMap<ByteBuffer, RowId> primaryIndex
    ) {
        this.mvDataStorage = mvDataStorage;
        this.raftClient = raftClient;
        this.lockManager = lockManager;
        this.tableId = tableId;
        this.primaryIndex = primaryIndex;
    }

    /** {@inheritDoc} */
    @Override
    public ListenerResponse invoke(ReplicaRequest request) {
        if (request instanceof ReadWriteSingleRowReplicaRequest) {
            return processSingleEntryAction((ReadWriteSingleRowReplicaRequest) request).join();
        } else if (request instanceof ReadWriteMultiRowReplicaRequest) {
            return processMultiEntryAction((ReadWriteMultiRowReplicaRequest) request);
        } if (request instanceof ReadWriteDualRowReplicaRequest) {
            return processTwoEntriesAction((ReadWriteDualRowReplicaRequest) request);
        } else {
            return null;
        }
    }

    /**
     * Returns index id of default {@lonk INDEX_SCAN_ID} index that will be used for operation.
     *
     * @param indexId Index id or {@code null}.
     * @return Index id.
     */
    private @NotNull UUID indexIdOrDefault(@Nullable UUID indexId) {
        return indexId != null ? indexId : INDEX_SCAN_ID;
    }

    /**
     * Find out a row id by an index.
     *
     * @param indexId Index id.
     * @param key     Key to find.
     * @return Value or {@code null} if the key does not determine a value.
     */
    private RowId valueByUniqueIndex(@NotNull UUID indexId, BinaryRow key) {
        if (INDEX_PK_ID.equals(indexId)) {
            return primaryIndex.get(key.keySlice());
        }

        if (INDEX_SCAN_ID.equals(indexId)) {
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
    private ListenerResponse processMultiEntryAction(ReadWriteMultiRowReplicaRequest request) {
        Collection<BinaryRow> keyRows = request.binaryRows().stream().map(br -> new ByteBufferRow(br.keySlice())).collect(
                Collectors.toList());
        Map<BinaryRow, BinaryRow> keyToRows = request.binaryRows().stream().collect(
                Collectors.toMap(br -> new ByteBufferRow(br.keySlice()), br -> br));

        HashMap<BinaryRow, BinaryRow> result = new HashMap<>(keyRows.size());
        HashMap<BinaryRow, RowId> rowIdByKey = new HashMap<>(keyRows.size());

        UUID indexId = indexIdOrDefault(null/*request.indexToUse()*/);

        UUID txId = request.transactionId();

        switch (request.requestType()) {
            case RW_GET_ALL: {
                for (BinaryRow keyRow : keyRows) {
                    lockManager.acquire(txId, new LockKey(indexId, keyRow), LockMode.SHARED); // Index S lock

                    rowIdByKey.put(keyRow, valueByUniqueIndex(indexId, keyRow));
                }

                lockManager.acquire(txId, new LockKey(tableId), LockMode.INTENTION_SHARED); // IS lock on table

                for (Map.Entry<BinaryRow, RowId> entry : rowIdByKey.entrySet()) {
                    RowId rowId = entry.getValue();

                    if (rowId != null) {
                        lockManager.acquire(txId, new LockKey(tableId, rowId), LockMode.SHARED); // S lock on RowId

                        result.put(entry.getKey(), mvDataStorage.read(rowId, txId));
                    } else {
                        result.put(entry.getKey(), null);
                    }
                }

                return new ListenerInstantResponse(result);
            }
            case RW_DELETE_ALL: {
                for (BinaryRow keyRow : keyRows) {
                    lockManager.acquire(txId, new LockKey(indexId, keyRow), LockMode.EXCLUSIVE); // Index X lock

                    rowIdByKey.put(keyRow, valueByUniqueIndex(indexId, keyRow));
                }

                lockManager.acquire(txId, new LockKey(tableId), LockMode.INTENTION_EXCLUSIVE); // IX lock on table

                for (Map.Entry<BinaryRow, RowId> entry : rowIdByKey.entrySet()) {
                    RowId rowId = entry.getValue();

                    if (rowId != null) {
                        lockManager.acquire(txId, new LockKey(tableId, rowId), LockMode.EXCLUSIVE); // X lock on RowId
                    }
                }

                Collection<BinaryRow> keysToRemove = rowIdByKey.entrySet().stream()
                        .filter(entry -> entry.getValue() != null)
                        .map(Map.Entry::getKey)
                        .collect(Collectors.toList());

                //TODO: IGNITE-17477 RAFT commands have to apply a row id.
                CompletableFuture raftFut = raftClient.run(new DeleteAllCommand(keysToRemove, txId));

                return new ListenerCompoundResponse(new ReplicaRequestLocator(txId, UUID.randomUUID()), raftFut, keysToRemove);
            }
            case RW_DELETE_EXACT_ALL: {
                for (BinaryRow keyRow : keyRows) {
                    lockManager.acquire(txId, new LockKey(indexId, keyRow), LockMode.EXCLUSIVE); // Index X lock

                    rowIdByKey.put(keyRow, valueByUniqueIndex(indexId, keyRow));
                }

                lockManager.acquire(txId, new LockKey(tableId), LockMode.INTENTION_EXCLUSIVE); // IX lock on table

                for (Map.Entry<BinaryRow, RowId> entry : rowIdByKey.entrySet()) {
                    RowId rowId = entry.getValue();

                    if (rowId != null) {
                        CompletableFuture<Lock> lock = lockManager.acquire(txId, new LockKey(tableId, rowId),
                                LockMode.SHARED); // S lock on RowId - temporary

                        BinaryRow curVal = mvDataStorage.read(rowId, txId);

                        if (equalValues(curVal, keyToRows.get(entry.getKey()))) {
                            lockManager.acquire(txId, new LockKey(tableId, rowId), LockMode.EXCLUSIVE); // X lock on RowId

                            result.put(entry.getKey(), curVal);

                        } else {
                            //TODO: Lock should to be acquired locally only.
                            try {
                                lockManager.release(lock.join()); // Release S lock on RowId - temporary
                            } catch (LockException e) {
                                throw new ReplicationException("group", e);
                            }
                        }

                        result.put(entry.getKey(), null);
                    } else {
                        result.put(entry.getKey(), null);
                    }
                }

                Collection<BinaryRow> keysToRemove = result.entrySet().stream()
                        .filter(entry -> entry.getValue() != null)
                        .map(Map.Entry::getKey)
                        .collect(Collectors.toList());

                Collection<BinaryRow> rowsToRemove = result.entrySet().stream()
                        .filter(entry -> entry.getValue() != null)
                        .map(Map.Entry::getValue)
                        .collect(Collectors.toList());

                //TODO: IGNITE-17477 RAFT commands have to apply a row id.
                CompletableFuture raftFut = raftClient.run(new DeleteExactAllCommand(rowsToRemove, txId));

                return new ListenerCompoundResponse(new ReplicaRequestLocator(txId, UUID.randomUUID()), raftFut, keysToRemove);
            }
            case RW_INSERT_ALL: {
                for (BinaryRow keyRow : keyRows) {
                    CompletableFuture<Lock> lock = lockManager.acquire(txId, new LockKey(indexId, keyRow),
                            LockMode.SHARED); // Index S lock - temporary

                    RowId id = valueByUniqueIndex(indexId, keyRow);

                    if (id != null) {
                        //TODO: Lock should to be acquired locally only.
                        try {
                            lockManager.release(lock.join()); // Release S lock on RowId - temporary
                        } catch (LockException e) {
                            throw new ReplicationException("group", e);
                        }
                    } else {
                        lockManager.acquire(txId, new LockKey(indexId, keyRow), LockMode.EXCLUSIVE); // Index X lock
                    }

                    rowIdByKey.put(keyRow, id);
                }

                lockManager.acquire(txId, new LockKey(tableId), LockMode.INTENTION_EXCLUSIVE); // IX lock on table

                Collection<BinaryRow> rowsToInsert = rowIdByKey.entrySet().stream()
                        .filter(entry -> entry.getValue() == null)
                        .map(entry -> keyToRows.get(entry.getKey()))
                        .collect(Collectors.toList());

                Collection<BinaryRow> restRows = rowIdByKey.entrySet().stream()
                        .filter(entry -> entry.getValue() != null)
                        .map(entry -> keyToRows.get(entry.getKey()))
                        .collect(Collectors.toList());

                //TODO: IGNITE-17477 RAFT commands have to apply a row id.
                CompletableFuture raftFut = raftClient.run(new InsertAllCommand(rowsToInsert, txId));

                return new ListenerCompoundResponse(new ReplicaRequestLocator(txId, UUID.randomUUID()), raftFut, restRows);
            }
            case RW_UPSERT_ALL: {
                for (BinaryRow keyRow : keyRows) {
                    lockManager.acquire(txId, new LockKey(indexId, keyRow), LockMode.EXCLUSIVE); // Index X lock

                    rowIdByKey.put(keyRow, valueByUniqueIndex(indexId, keyRow));
                }

                lockManager.acquire(txId, new LockKey(tableId), LockMode.INTENTION_EXCLUSIVE); // IX lock on table

                for (Map.Entry<BinaryRow, RowId> entry : rowIdByKey.entrySet()) {
                    RowId rowId = entry.getValue();

                    if (rowId != null) {
                        lockManager.acquire(txId, new LockKey(tableId, rowId), LockMode.EXCLUSIVE); // X lock on RowId
                    }
                }

                //TODO: IGNITE-17477 RAFT commands have to apply a row id.
                CompletableFuture raftFut = raftClient.run(new UpsertAllCommand(keyToRows.values(), txId));

                return new ListenerFutureResponse(new ReplicaRequestLocator(txId, UUID.randomUUID()), raftFut);
            }
            default: {
                throw new IgniteInternalException(Replicator.REPLICA_COMMON_ERR,
                        IgniteStringFormatter.format("Unknown multi request [actionType={}]", request.requestType()));
            }
        }
    }

    /**
     * Precesses single request.
     *
     * @param request Single request operation.
     * @return Listener response.
     */
    private CompletableFuture<ListenerResponse> processSingleEntryAction(ReadWriteSingleRowReplicaRequest request) {
        BinaryRow searchKey = new ByteBufferRow(request.binaryRow().keySlice());

        BinaryRow searchRow = request.binaryRow();

        UUID indexId = indexIdOrDefault(null/*request.indexToUse()*/);

        UUID txId = request.transactionId();

        switch (request.requestType()) {
            case RW_GET: {
                return lockManager.acquire(txId, new LockKey(indexId, searchKey), LockMode.SHARED).thenCompose(idxLock -> { // Index S lock
                    RowId rowId = valueByUniqueIndex(indexId, searchKey);

                    return lockManager.acquire(txId, new LockKey(tableId), LockMode.INTENTION_SHARED).thenCompose(tblLock -> { // IS lock on table
                        CompletableFuture<Lock> rowLockFut = rowId != null ?
                                lockManager.acquire(txId, new LockKey(tableId, rowId), LockMode.SHARED) : // S lock on RowId
                                CompletableFuture.completedFuture(null);

                        return rowLockFut.thenApply(rowLock -> {
                            BinaryRow result = rowId != null ? mvDataStorage.read(rowId, txId) : null;

                            return new ListenerInstantResponse(result);
                        });
                    });
                });
            }
            case RW_DELETE:
            case RW_GET_AND_DELETE: {
                return lockManager.acquire(txId, new LockKey(indexId, searchKey), LockMode.EXCLUSIVE).thenCompose(idxLock -> { // Index X lock
                    RowId rowId = valueByUniqueIndex(indexId, searchKey);

                    return lockManager.acquire(txId, new LockKey(tableId), LockMode.INTENTION_EXCLUSIVE).thenCompose(tblLock -> { // IX lock on table
                        CompletableFuture<Lock> rowLockFut = rowId != null ?
                                lockManager.acquire(txId, new LockKey(tableId, rowId), LockMode.EXCLUSIVE) : // X lock on RowId
                                CompletableFuture.completedFuture(null);

                        return rowLockFut.thenApply(rowLock -> {
                            BinaryRow result = rowId != null ? mvDataStorage.read(rowId, txId) : null;

                            CompletableFuture raftFut = rowId != null ? raftClient.run(new DeleteCommand(searchKey, txId)) :
                                    CompletableFuture.completedFuture(null);

                            return new ListenerCompoundResponse(new ReplicaRequestLocator(txId, UUID.randomUUID()), raftFut, result);
                        });
                    });
                });
            }
            case RW_DELETE_EXACT: {
                return lockManager.acquire(txId, new LockKey(indexId, searchKey), LockMode.EXCLUSIVE).thenCompose(idxLock -> { // Index X lock
                    RowId rowId = valueByUniqueIndex(indexId, searchKey);

                    return lockManager.acquire(txId, new LockKey(tableId), LockMode.INTENTION_EXCLUSIVE).thenCompose(tblLock -> { // IX lock on table
                        CompletableFuture<BinaryRow> rowLockFut;

                        if (rowId != null) {
                            rowLockFut = lockManager.acquire(txId, new LockKey(tableId, rowId),
                                    LockMode.SHARED).thenCompose(tempRowLock -> { // S lock on RowId - temporary
                                BinaryRow curVal = mvDataStorage.read(rowId, txId);

                                if (equalValues(curVal, searchRow)) {
                                    return lockManager.acquire(txId, new LockKey(tableId, rowId), LockMode.EXCLUSIVE)
                                            .thenApply(rowLock -> curVal); // X lock on RowId
                                } else {
                                    try {
                                        lockManager.release(tempRowLock); // Release S lock on RowId - temporary
                                    } catch (LockException e) {
                                        throw new ReplicationException("group", e);
                                    }

                                    return CompletableFuture.completedFuture(null);
                                }
                            });
                        } else {
                            rowLockFut = CompletableFuture.completedFuture(null);
                        }

                        return rowLockFut.thenApply(lockedRow -> {
                            //TODO: IGNITE-17477 RAFT commands have to apply a row id.
                            CompletableFuture raftFut = lockedRow != null ? raftClient.run(new DeleteCommand(lockedRow, txId)) :
                                    CompletableFuture.completedFuture(null);

                            return new ListenerCompoundResponse(new ReplicaRequestLocator(txId, UUID.randomUUID()), raftFut, lockedRow);
                        });
                    });
                });
            }
            case RW_INSERT: {
                CompletableFuture<RowId> idxLockFut = lockManager.acquire(txId, new LockKey(indexId, searchKey), LockMode.SHARED).thenCompose(tempIdxLock -> { // Index S lock - temporary
                    RowId rowId = valueByUniqueIndex(indexId, searchKey);

                    if (rowId != null) {
                        try {
                            lockManager.release(tempIdxLock); // Release S lock on Index - temporary
                        } catch (LockException e) {
                            throw new ReplicationException("group", e);
                        }

                        return CompletableFuture.completedFuture(rowId);
                    } else {
                        return lockManager.acquire(txId, new LockKey(indexId, searchKey), LockMode.EXCLUSIVE). thenApply(idxLock -> null); // Index X lock
                    }
                });

                return idxLockFut.thenCompose(lockedRowId -> {
                    return lockManager.acquire(txId, new LockKey(tableId), LockMode.INTENTION_EXCLUSIVE).thenApply(tblLock -> { // IX lock on table
                        //TODO: IGNITE-17477 RAFT commands have to apply a row id.
                        CompletableFuture raftFut = lockedRowId == null ? raftClient.run(new InsertCommand(searchRow, txId)) :
                                CompletableFuture.completedFuture(null);

                        return new ListenerCompoundResponse(new ReplicaRequestLocator(txId, UUID.randomUUID()), raftFut,
                                lockedRowId != null ? searchRow : null);
                    });
                });
            }
            case RW_UPSERT: {
                return lockManager.acquire(txId, new LockKey(indexId, searchKey), LockMode.EXCLUSIVE).thenCompose(idxLock -> { // Index X lock
                    RowId rowId = valueByUniqueIndex(indexId, searchKey);

                    return lockManager.acquire(txId, new LockKey(tableId), LockMode.INTENTION_EXCLUSIVE).thenCompose(tblLock -> { // IX lock on table
                        CompletableFuture<Lock> rowLockFut = (rowId != null) ?
                                lockManager.acquire(txId, new LockKey(tableId, rowId), LockMode.EXCLUSIVE) : // X lock on RowId
                                CompletableFuture.completedFuture(null);

                        return rowLockFut.thenApply(rowLock -> {
                            //TODO: IGNITE-17477 RAFT commands have to apply a row id.
                            CompletableFuture raftFut = rowId != null ? raftClient.run(new UpsertCommand(searchRow, txId)) :
                                    raftClient.run(new InsertCommand(searchRow, txId));

                            return new ListenerFutureResponse(new ReplicaRequestLocator(txId, UUID.randomUUID()), raftFut);
                        });
                    });
                });
            }
            case RW_GET_AND_UPSERT: {
                return lockManager.acquire(txId, new LockKey(indexId, searchKey), LockMode.EXCLUSIVE).thenCompose(idxLock -> { // Index X lock
                    RowId rowId = valueByUniqueIndex(indexId, searchKey);

                    return lockManager.acquire(txId, new LockKey(tableId), LockMode.INTENTION_EXCLUSIVE).thenCompose(tblLock -> { // IX lock on table
                        CompletableFuture<Lock> rowLockFut = (rowId != null) ?
                                lockManager.acquire(txId, new LockKey(tableId, rowId), LockMode.EXCLUSIVE) : // X lock on RowId
                                CompletableFuture.completedFuture(null);

                        return rowLockFut.thenApply(rowLock -> {
                            BinaryRow result = rowId != null ? mvDataStorage.read(rowId, txId) : null;

                            //TODO: IGNITE-17477 RAFT commands have to apply a row id.
                            CompletableFuture raftFut = rowId != null ? raftClient.run(new UpsertCommand(searchRow, txId)) :
                                    raftClient.run(new InsertCommand(searchRow, txId));

                            return new ListenerCompoundResponse(new ReplicaRequestLocator(txId, UUID.randomUUID()), raftFut, result);
                        });
                    });
                });
            }
            case RW_GET_AND_REPLACE: {
                // lock management
                // call raft client

                return null;
            }
            default: {
                throw new IgniteInternalException(Replicator.REPLICA_COMMON_ERR,
                        IgniteStringFormatter.format("Unknown single request [actionType={}]", request.requestType()));
            }
        }
    }

    /**
     * Precesses two actions.
     *
     * @param action Two actions operation.
     * @return Listener response.
     */
    private ListenerResponse processTwoEntriesAction(ReadWriteDualRowReplicaRequest request) {
        switch (request.requestType()) {
            case RW_REPLACE: {
                // lock management
                // call raft client

                return null;
            }
            case RW_REPLACE_IF_EXIST: {
                // lock management
                // call raft client

                return null;
            }
            default: {
                throw new IgniteInternalException(Replicator.REPLICA_COMMON_ERR,
                        IgniteStringFormatter.format("Unknown two actions operation [actionType={}]", request.requestType()));
            }
        }
    }
}
