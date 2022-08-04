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

import static java.util.concurrent.TimeUnit.MILLISECONDS;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.replicator.listener.ListenerCompoundResponse;
import org.apache.ignite.internal.replicator.listener.ListenerFutureResponse;
import org.apache.ignite.internal.replicator.listener.ListenerInstantResponse;
import org.apache.ignite.internal.replicator.listener.ListenerResponse;
import org.apache.ignite.internal.replicator.listener.ReplicaListener;
import org.apache.ignite.internal.replicator.message.ReplicaRequest;
import org.apache.ignite.internal.replicator.message.ReplicaRequestLocator;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.storage.MvPartitionStorage;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.table.distributed.command.DeleteCommand;
import org.apache.ignite.internal.table.distributed.command.InsertCommand;
import org.apache.ignite.internal.table.distributed.replicator.action.PartitionAction;
import org.apache.ignite.internal.table.distributed.replicator.action.PartitionMultiAction;
import org.apache.ignite.internal.table.distributed.replicator.action.PartitionSingleAction;
import org.apache.ignite.internal.thread.NamedThreadFactory;
import org.apache.ignite.internal.tx.LockManager;
import org.apache.ignite.lang.IgniteInternalException;
import org.apache.ignite.lang.IgniteStringFormatter;
import org.apache.ignite.raft.client.service.RaftGroupService;
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
    private ConcurrentHashMap<ByteBuffer, RowId> primaryIndex = new ConcurrentHashMap<>();

    /** Executor. */
    private final ExecutorService executor;

    /**
     * The constructor.
     *
     * @param mvDataStorage Data storage.
     * @param raftClient    Raft client.
     * @param lockManager   Lock manager.
     */
    public PartitionReplicaListener(MvPartitionStorage mvDataStorage, RaftGroupService raftClient, LockManager lockManager, UUID tableId, String listenerName) {
        this.mvDataStorage = mvDataStorage;
        this.raftClient = raftClient;
        this.lockManager = lockManager;
        this.tableId = tableId;

        this.executor = new ThreadPoolExecutor(
                2 * Runtime.getRuntime().availableProcessors(),
                2 * Runtime.getRuntime().availableProcessors(),
                30_000,
                MILLISECONDS,
                new LinkedBlockingQueue<>(),
                new NamedThreadFactory(listenerName + "-exec", LOG)
        );
    }

    /** {@inheritDoc} */
    @Override
    public ListenerResponse invoke(ReplicaRequest request) {
//        if (request instanceof ActionRequest) {
//            var actionRequest = (ActionRequest) request;
//
//            return processAction((PartitionAction) actionRequest.action());
//        } else {
////            var cleanupRequest = (CleanupRequest)request;
//
//            return null;
//        }
        return null;
    }

    /**
     * Handles an action.
     *
     * @param action Partition action.
     * @return Listener response.
     */
    @Nullable
    private ListenerResponse processAction(PartitionAction action) {
        switch (action.actionType()) {
            case RW_GET: {
                var singleEntryAction = (PartitionSingleAction) action;

                RowId rowId = null;

                BinaryRow keyRow = singleEntryAction.entryContainer().getRow();

                if (singleEntryAction.indexToUse() != null) {
                    assert INDEX_PK_ID.equals(singleEntryAction.indexToUse()) : IgniteStringFormatter.format(
                            "The action can use only primary key index [indexToUse={}]", singleEntryAction.indexToUse());

                    //lockManager.acquire(action.txId(), new LockKey(INDEX_PK_ID, keyRow, LockMode.S); // Index S lock

                    rowId = primaryIndex.get(keyRow.keySlice());
                } else {
                    //lockManager.acquire(action.txId(), new LockKey(INDEX_SCAN_ID, keyRow, LockMode.S); // Index S lock

                    //TODO: Implement it through the scan index.
                    for (Map.Entry<ByteBuffer, RowId> entry : primaryIndex.entrySet()) {
                        if (keyRow.keySlice().equals(entry.getKey())) {
                            rowId = entry.getValue();

                            break;
                        }
                    }
                }

                BinaryRow row = null;

                if (rowId != null) {
                    //lockManager.acquire(action.txId(), new LockKey(tableId), LockMode.IS)); // IS lock on table
                    //lockManager.acquire(action.txId(), new LockKey(tableId, rowId), LockMode.S)); // S lock on RowId

                    row = mvDataStorage.read(rowId, action.txId());
                }

                return new ListenerInstantResponse(row);
            }
            case RW_GET_ALL: {
                var multiEntryAction = (PartitionMultiAction) action;

                Collection<BinaryRow> keyRows = multiEntryAction.entryContainer().getRows();

                HashMap<BinaryRow, BinaryRow> result = new HashMap<>(keyRows.size());
                HashMap<BinaryRow, RowId> rowIdByKey = new HashMap<>(keyRows.size());

                for (BinaryRow keyRow: keyRows) {
                    result.put(keyRow, null);
                    rowIdByKey.put(keyRow, null);
                }

                if (multiEntryAction.indexToUse() != null) {
                    assert INDEX_PK_ID.equals(multiEntryAction.indexToUse()) : IgniteStringFormatter.format(
                            "The action can use only primary key index [indexToUse={}]", multiEntryAction.indexToUse());

                    for (BinaryRow keyRow: keyRows) {
                        //lockManager.acquire(action.txId(), new LockKey(INDEX_PK_ID, keyRow, LockMode.S); // Index S lock

                        rowIdByKey.put(keyRow, primaryIndex.get(keyRow.keySlice()));
                    }
                } else {
                    for (BinaryRow keyRow: keyRows) {
                        //lockManager.acquire(action.txId(), new LockKey(INDEX_SCAN_ID, keyRow, LockMode.S); // Index S lock

                        //TODO: Implement it through the scan index.
                        for (Map.Entry<ByteBuffer, RowId> entry : primaryIndex.entrySet()) {
                            if (keyRow.keySlice().equals(entry.getKey())) {
                                rowIdByKey.put(keyRow, entry.getValue());

                                break;
                            }
                        }
                    }
                }

                for (Map.Entry<BinaryRow, RowId> entry : rowIdByKey.entrySet()) {
                    RowId rowId = entry.getValue();

                    if (rowId != null) {
                        //lockManager.acquire(action.txId(), new LockKey(tableId), LockMode.IS)); // IS lock on table
                        //lockManager.acquire(action.txId(), new LockKey(tableId, rowId), LockMode.S)); // S lock on RowId

                        result.put(entry.getKey(), mvDataStorage.read(rowId, action.txId()));
                    }
                }

                return new ListenerInstantResponse(result);
            }
            case RW_DELETE: {
                var singleEntryAction = (PartitionSingleAction) action;

                RowId rowId = null;

                BinaryRow keyRow = singleEntryAction.entryContainer().getRow();

                if (singleEntryAction.indexToUse() != null) {
                    assert INDEX_PK_ID.equals(singleEntryAction.indexToUse()) : IgniteStringFormatter.format(
                            "The action can use only primary key index [indexToUse={}]", singleEntryAction.indexToUse());

                    //lockManager.acquire(action.txId(), new LockKey(INDEX_PK_ID, keyRow, LockMode.X); // Index X lock

                    rowId = primaryIndex.remove(singleEntryAction.entryContainer().getRow().keySlice());
                } else {
                    //lockManager.acquire(action.txId(), new LockKey(INDEX_SCAN_ID, keyRow, LockMode.X); // Index X lock

                    //TODO: Implement it through the scan index.
                    for (Map.Entry<ByteBuffer, RowId> entry : primaryIndex.entrySet()) {
                        if (keyRow.keySlice().equals(entry.getKey())) {
                            rowId = entry.getValue();

                            break;
                        }
                    }
                }

                //lockManager.acquire(action.txId(), new LockKey(tableId), LockMode.IX)); // IX lock on table
                //lockManager.acquire(action.txId(), new LockKey(tableId, rowId), LockMode.X)); // X lock on RowId

                BinaryRow row = mvDataStorage.read(rowId, action.txId());

                mvDataStorage.addWrite(rowId, row, action.txId());

                CompletableFuture fut = new CompletableFuture();

                executor.submit(() -> {
                    //TODO: RAFT commands have to apply a row id and shouldn't depend of txId.
                    raftClient.run(new DeleteCommand(keyRow, action.txId())).whenComplete((o, throwable) -> {
                        if (throwable != null) {
                            fut.completeExceptionally(throwable);
                        }
                        else {
                            fut.complete(o);
                        }
                    });
                });

                return new ListenerCompoundResponse(new ReplicaRequestLocator(action.txId(), UUID.randomUUID()), fut, rowId != null);
            }
            case RW_DELETE_ALL: {
                // lock management
                // call raft client

                return null;
            }
            case RW_DELETE_EXACT: {
                // lock management
                // call raft client

                return null;
            }
            case RW_DELETE_EXACT_ALL: {
                // lock management
                // call raft client

                return null;
            }
            case RW_INSERT: {
                var singleEntryAction = (PartitionSingleAction) action;

                BinaryRow row = singleEntryAction.entryContainer().getRow();

                //lockManager.acquire(action.txId(), new LockKey(tableId), LockMode.IX)); // IX lock on table
                RowId rowId = mvDataStorage.insert(row, action.txId());

                if (singleEntryAction.indexToUse() != null) {
                    assert INDEX_PK_ID.equals(singleEntryAction.indexToUse()) : IgniteStringFormatter.format(
                            "The action can use only primary key index [indexToUse={}]", singleEntryAction.indexToUse());

                    //lockManager.acquire(action.txId(), new LockKey(INDEX_PK_ID, keyRow, LockMode.X); // Index X lock

                    rowId = primaryIndex.put(row.valueSlice(), rowId);

                    assert rowId == null : "Primary index already has row";
                } else {
                    //lockManager.acquire(action.txId(), new LockKey(INDEX_SCAN_ID, keyRow, LockMode.X); // Index X lock
                }

                CompletableFuture fut = new CompletableFuture();

                executor.submit(() -> {
                    //TODO: RAFT commands have to apply a row id and shouldn't depend of txId.
                    raftClient.run(new InsertCommand(row, action.txId())).whenComplete((o, throwable) -> {
                        if (throwable != null) {
                            fut.completeExceptionally(throwable);
                        }
                        else {
                            fut.complete(o);
                        }
                    });
                });

                return new ListenerFutureResponse(new ReplicaRequestLocator(action.txId(), UUID.randomUUID()), fut);
            }
            case RW_INSERT_ALL: {
                // lock management
                // call raft client

                return null;
            }
            case RW_UPSERT: {
                // lock management
                // call raft client

                return null;
            }
            case RW_UPSERT_ALL: {
                // lock management
                // call raft client

                return null;
            }
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
            case RW_GET_AND_DELETE: {
                // lock management
                // call raft client

                return null;
            }
            case RW_GET_AND_REPLACE: {
                // lock management
                // call raft client

                return null;
            }
            case RW_GET_AND_UPSERT: {
                // lock management
                // call raft client

                return null;
            }
            case RO_GET: {
                // lock management
                // call raft client

                return null;
            }
            case RO_GET_ALL: {
                // lock management
                // call raft client

                return null;
            }
            case RO_SCAN: {
                // lock management
                // call raft client

                return null;
            }
            default:
                throw new IgniteInternalException(
                        IgniteStringFormatter.format("The action type is not expected [opType={}]", action.actionType()));
        }
    }
}
