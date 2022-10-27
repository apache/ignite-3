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

package org.apache.ignite.internal.table.distributed.raft;

import static org.apache.ignite.internal.tx.TxState.ABORTED;
import static org.apache.ignite.internal.tx.TxState.COMMITED;
import static org.apache.ignite.lang.ErrorGroups.Transactions.TX_UNEXPECTED_STATE_ERR;
import static org.apache.ignite.lang.IgniteStringFormatter.format;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.apache.ignite.internal.lock.AutoLockup;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.replicator.ReplicationGroupId;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.ByteBufferRow;
import org.apache.ignite.internal.storage.MvPartitionStorage;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.table.distributed.command.FinishTxCommand;
import org.apache.ignite.internal.table.distributed.command.TxCleanupCommand;
import org.apache.ignite.internal.table.distributed.command.UpdateAllCommand;
import org.apache.ignite.internal.table.distributed.command.UpdateCommand;
import org.apache.ignite.internal.tx.TxManager;
import org.apache.ignite.internal.tx.TxMeta;
import org.apache.ignite.internal.tx.TxState;
import org.apache.ignite.internal.tx.storage.state.TxStateStorage;
import org.apache.ignite.internal.util.CollectionUtils;
import org.apache.ignite.lang.IgniteInternalException;
import org.apache.ignite.raft.client.Command;
import org.apache.ignite.raft.client.ReadCommand;
import org.apache.ignite.raft.client.WriteCommand;
import org.apache.ignite.raft.client.service.CommandClosure;
import org.apache.ignite.raft.client.service.RaftGroupListener;
import org.apache.ignite.raft.jraft.util.ByteString;
import org.jetbrains.annotations.TestOnly;

/**
 * Partition command handler.
 */
public class PartitionListener implements RaftGroupListener {
    /** Logger. */
    private static final IgniteLogger LOG = Loggers.forClass(PartitionListener.class);

    /** Versioned partition storage. */
    private final PartitionDataStorage storage;

    /** Transaction state storage. */
    private final TxStateStorage txStateStorage;

    /** Transaction manager. */
    private final TxManager txManager;

    //TODO: https://issues.apache.org/jira/browse/IGNITE-17205 Temporary solution until the implementation of the primary index is done.
    /** Dummy primary index. */
    private final ConcurrentHashMap<ByteBuffer, RowId> primaryIndex;

    /** Partition ID */
    private int partitionId;

    /** Keys that were inserted by a transaction. */
    private HashMap<UUID, Set<ByteBuffer>> txsInsertedKeys = new HashMap<>();

    /** Keys that were removed by a transaction. */
    private HashMap<UUID, Set<ByteBuffer>> txsRemovedKeys = new HashMap<>();

    /** Rows that were inserted, updated or removed. */
    private HashMap<UUID, Set<RowId>> txsPendingRowIds = new HashMap<>();

    /**
     * The constructor.
     *
     * @param store  The storage.
     * @param txStateStorage Transaction state storage.
     * @param txManager Transaction manager.
     * @param primaryIndex Primary index map.
     * @param partitionId Partition ID this listener serves.
     */
    public PartitionListener(
            PartitionDataStorage store,
            TxStateStorage txStateStorage,
            TxManager txManager,
            ConcurrentHashMap<ByteBuffer, RowId> primaryIndex,
            int partitionId
    ) {
        this.storage = store;
        this.txStateStorage = txStateStorage;
        this.txManager = txManager;
        this.primaryIndex = primaryIndex;
        this.partitionId = partitionId;
    }

    /** {@inheritDoc} */
    @Override
    public void onRead(Iterator<CommandClosure<ReadCommand>> iterator) {
        iterator.forEachRemaining((CommandClosure<? extends ReadCommand> clo) -> {
            Command command = clo.command();

            assert false : "No read commands expected, [cmd=" + command + ']';
        });
    }

    /** {@inheritDoc} */
    @Override
    public void onWrite(Iterator<CommandClosure<WriteCommand>> iterator) {
        iterator.forEachRemaining((CommandClosure<? extends WriteCommand> clo) -> {
            Command command = clo.command();

            long commandIndex = clo.index();

            long storageAppliedIndex = storage.lastAppliedIndex();

            assert storageAppliedIndex < commandIndex
                    : "Pending write command has a higher index than already processed commands [commandIndex=" + commandIndex
                    + ", storageAppliedIndex=" + storageAppliedIndex + ']';

            try (AutoLockup ignoredPartitionSnapshotsReadLockup = storage.acquirePartitionSnapshotsReadLock()) {
                if (command instanceof UpdateCommand) {
                    handleUpdateCommand((UpdateCommand) command, commandIndex);
                } else if (command instanceof UpdateAllCommand) {
                    handleUpdateAllCommand((UpdateAllCommand) command, commandIndex);
                } else if (command instanceof FinishTxCommand) {
                    handleFinishTxCommand((FinishTxCommand) command, commandIndex);
                } else if (command instanceof TxCleanupCommand) {
                    handleTxCleanupCommand((TxCleanupCommand) command, commandIndex);
                } else {
                    assert false : "Command was not found [cmd=" + command + ']';
                }
                clo.result(null);
            } catch (IgniteInternalException e) {
                clo.result(e);
            }
        });
    }

    /**
     * Handler for the {@link UpdateCommand}.
     *
     * @param cmd Command.
     */
    private void handleUpdateCommand(UpdateCommand cmd, long commandIndex) {
        storage.runConsistently(() -> {
            BinaryRow row = cmd.rowBuffer() != null ? new ByteBufferRow(cmd.rowBuffer().toByteArray()) : null;
            UUID rowUuid = cmd.rowUuid();
            RowId rowId = new RowId(partitionId, rowUuid.getMostSignificantBits(), rowUuid.getLeastSignificantBits());
            UUID txId = cmd.txId();
            UUID commitTblId = cmd.tablePartitionId().tableId();
            int commitPartId = cmd.tablePartitionId().partitionId();

            storage.addWrite(rowId, row, txId, commitTblId, commitPartId);

            txsPendingRowIds.computeIfAbsent(txId, entry -> new HashSet<>()).add(rowId);

            if (row == null) {
                // Remove entry.
                List<ByteBuffer> keys = primaryIndex.entrySet().stream()
                        .filter(e -> e.getValue().equals(rowId))
                        .map(Entry::getKey)
                        .collect(Collectors.toList());

                assert keys.size() <= 1;

                if (keys.size() == 1) {
                    txsRemovedKeys.computeIfAbsent(txId, entry -> new HashSet<>()).add(keys.get(0));
                    txsInsertedKeys.computeIfAbsent(txId, entry -> new HashSet<>()).remove(keys.get(0));
                }
            } else if (!primaryIndex.containsKey(row.keySlice())) {
                // Insert entry.
                txsInsertedKeys.computeIfAbsent(txId, entry -> new HashSet<>()).add(row.keySlice());
                txsRemovedKeys.computeIfAbsent(txId, entry -> new HashSet<>()).remove(row.keySlice());

                primaryIndex.put(row.keySlice(), rowId);
            } else if (primaryIndex.containsKey(row.keySlice())) {
                txsRemovedKeys.computeIfAbsent(txId, entry -> new HashSet<>()).remove(row.keySlice());
            }

            storage.lastAppliedIndex(commandIndex);

            return null;
        });
    }

    /**
     * Handler for the {@link UpdateAllCommand}.
     *
     * @param cmd Command.
     */
    private void handleUpdateAllCommand(UpdateAllCommand cmd, long commandIndex) {
        storage.runConsistently(() -> {
            UUID txId = cmd.txId();
            Map<UUID, ByteString> rowsToUpdate = cmd.rowsToUpdate();
            UUID commitTblId = cmd.tablePartitionId().tableId();
            int commitPartId = cmd.tablePartitionId().partitionId();

            if (!CollectionUtils.nullOrEmpty(rowsToUpdate)) {
                for (Map.Entry<UUID, ByteString> entry : rowsToUpdate.entrySet()) {
                    UUID rowIdUuid = entry.getKey();
                    RowId rowId = new RowId(partitionId, rowIdUuid.getMostSignificantBits(), rowIdUuid.getLeastSignificantBits());
                    ByteString rowStr = entry.getValue();
                    BinaryRow row = rowStr != null ? new ByteBufferRow(rowStr.toByteArray()) : null;
                    // TODO: IGNITE-17759 Need pass appropriate commitTableId and commitPartitionId.
                    storage.addWrite(rowId, row, txId, commitTblId, commitPartId);

                    txsPendingRowIds.computeIfAbsent(txId, entry0 -> new HashSet<>()).add(rowId);

                    if (row == null) {
                        // Remove entry.
                        List<ByteBuffer> keys = primaryIndex.entrySet().stream()
                                .filter(e -> e.getValue().equals(rowId))
                                .map(Entry::getKey)
                                .collect(Collectors.toList());

                        assert keys.size() <= 1;

                        if (keys.size() == 1) {
                            txsRemovedKeys.computeIfAbsent(txId, entry0 -> new HashSet<>()).add(keys.get(0));
                            txsInsertedKeys.computeIfAbsent(txId, entry0 -> new HashSet<>()).remove(keys.get(0));
                        }
                    } else if (!primaryIndex.containsKey(row.keySlice())) {
                        // Insert entry.
                        txsInsertedKeys.computeIfAbsent(txId, entry0 -> new HashSet<>()).add(row.keySlice());
                        txsRemovedKeys.computeIfAbsent(txId, entry0 -> new HashSet<>()).remove(row.keySlice());

                        primaryIndex.put(row.keySlice(), rowId);
                    } else if (primaryIndex.containsKey(row.keySlice())) {
                        txsRemovedKeys.computeIfAbsent(txId, entry0 -> new HashSet<>()).remove(row.keySlice());
                    }
                }
            }
            storage.lastAppliedIndex(commandIndex);

            return null;
        });
    }

    /**
     * Handler for the {@link FinishTxCommand}.
     *
     * @param cmd          Command.
     * @param commandIndex Index of the RAFT command.
     * @throws IgniteInternalException if an exception occurred during a transaction state change.
     */
    private void handleFinishTxCommand(FinishTxCommand cmd, long commandIndex) throws IgniteInternalException {
        UUID txId = cmd.txId();

        TxState stateToSet = cmd.commit() ? TxState.COMMITED : TxState.ABORTED;

        TxMeta txMetaToSet = new TxMeta(
                stateToSet,
                cmd.tablePartitionIds()
                        .stream()
                        .map(tpIdMsg -> (ReplicationGroupId) tpIdMsg.asTablePartitionId())
                        .collect(Collectors.toList()),
                cmd.commitTimestamp().asHybridTimestamp()
        );

        TxMeta txMetaBeforeCas = txStateStorage.get(txId);

        boolean txStateChangeRes = txStateStorage.compareAndSet(
                txId,
                null,
                txMetaToSet,
                commandIndex
        );

        LOG.debug("Finish the transaction txId = {}, state = {}, txStateChangeRes = {}", txId, txMetaToSet, txStateChangeRes);

        if (!txStateChangeRes) {
            UUID traceId = UUID.randomUUID();

            String errorMsg = format("Fail to finish the transaction txId = {} because of inconsistent state = {},"
                            + " expected state = null, state to set = {}",
                    txId,
                    txMetaBeforeCas,
                    txMetaToSet
            );

            IgniteInternalException stateChangeException = new IgniteInternalException(traceId, TX_UNEXPECTED_STATE_ERR, errorMsg);

            // Exception is explicitly logged because otherwise it can be lost if it did not occur on the leader.
            LOG.error(errorMsg);

            throw stateChangeException;
        }
    }


    /**
     * Handler for the {@link TxCleanupCommand}.
     *
     * @param cmd Command.
     */
    private void handleTxCleanupCommand(TxCleanupCommand cmd, long commandIndex) {
        storage.runConsistently(() -> {
            UUID txId = cmd.txId();

            Set<ByteBuffer> removedKeys = txsRemovedKeys.getOrDefault(txId, Collections.emptySet());

            Set<ByteBuffer> insertedKeys = txsInsertedKeys.getOrDefault(txId, Collections.emptySet());

            Set<RowId> pendingRowIds = txsPendingRowIds.getOrDefault(txId, Collections.emptySet());

            if (cmd.commit()) {
                pendingRowIds.forEach(rowId -> storage.commitWrite(rowId, cmd.commitTimestamp().asHybridTimestamp()));
            } else {
                pendingRowIds.forEach(rowId -> storage.abortWrite(rowId));
            }

            if (cmd.commit()) {
                for (ByteBuffer key : removedKeys) {
                    primaryIndex.remove(key);
                }
            } else {
                for (ByteBuffer key : insertedKeys) {
                    primaryIndex.remove(key);
                }
            }

            txsRemovedKeys.remove(txId);
            txsInsertedKeys.remove(txId);
            txsPendingRowIds.remove(txId);

            // TODO: IGNITE-17638 TestOnly code, let's consider using Txn state map instead of states.
            txManager.changeState(txId, null, cmd.commit() ? COMMITED : ABORTED);

            storage.lastAppliedIndex(commandIndex);

            return null;
        });
    }

    /** {@inheritDoc} */
    @Override
    public void onSnapshotSave(Path path, Consumer<Throwable> doneClo) {
        storage.flush();
    }

    /** {@inheritDoc} */
    @Override
    public boolean onSnapshotLoad(Path path) {
        return true;
    }

    /** {@inheritDoc} */
    @Override
    public void onShutdown() {
        // TODO: IGNITE-17958 - probably, we should not close the storage here as PartitionListener did not create the storage.
        try {
            storage.close();
        } catch (Exception e) {
            throw new IgniteInternalException("Failed to close storage: " + e.getMessage(), e);
        }
    }

    /**
     * Returns underlying storage.
     */
    @TestOnly
    public MvPartitionStorage getMvStorage() {
        return storage.getMvStorage();
    }

    /**
     * Returns a primary index map.
     */
    @TestOnly
    public Map<ByteBuffer, RowId> getPk() {
        return primaryIndex;
    }
}
