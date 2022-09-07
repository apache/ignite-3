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

package org.apache.ignite.internal.table.distributed.raft;

import static org.apache.ignite.lang.ErrorGroups.Transactions.TX_UNEXPECTED_STATE;
import static org.apache.ignite.lang.IgniteStringFormatter.format;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.storage.DataRow;
import org.apache.ignite.internal.storage.MvPartitionStorage;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.storage.basic.BinarySearchRow;
import org.apache.ignite.internal.storage.basic.DelegatingDataRow;
import org.apache.ignite.internal.table.distributed.command.DeleteAllCommand;
import org.apache.ignite.internal.table.distributed.command.DeleteCommand;
import org.apache.ignite.internal.table.distributed.command.FinishTxCommand;
import org.apache.ignite.internal.table.distributed.command.InsertAndUpdateAllCommand;
import org.apache.ignite.internal.table.distributed.command.InsertCommand;
import org.apache.ignite.internal.table.distributed.command.TxCleanupCommand;
import org.apache.ignite.internal.table.distributed.command.UpdateCommand;
import org.apache.ignite.internal.tx.Timestamp;
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
import org.apache.ignite.raft.jraft.util.concurrent.ConcurrentHashSet;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.TestOnly;

/**
 * Partition command handler.
 */
public class PartitionListener implements RaftGroupListener {
    /** Logger. */
    private static final IgniteLogger LOG = Loggers.forClass(PartitionListener.class);

    /** Versioned partition storage. */
    private final MvPartitionStorage storage;

    /** Transaction state storage. */
    private final TxStateStorage txStateStorage;

    /** Transaction manager. */
    private final TxManager txManager;

    //TODO: https://issues.apache.org/jira/browse/IGNITE-17205 Temporary solution until the implementation of the primary index is done.
    /** Dummy primary index. */
    private final ConcurrentHashMap<ByteBuffer, RowId> primaryIndex;

    /** Keys that were inserted by the transaction. */
    private ConcurrentHashMap<UUID, Set<RowId>> txsInsertedKeys = new ConcurrentHashMap<>();

    /** Keys that were removed by the transaction. */
    private ConcurrentHashMap<UUID, Set<RowId>> txsRemovedKeys = new ConcurrentHashMap<>();

    /** Rows that were inserted, updated or removed. */
    private ConcurrentHashMap<UUID, Set<RowId>> txsPendingRowIds = new ConcurrentHashMap<>();

    /**
     * The constructor.
     *
     * @param store  The storage.
     * @param txStateStorage Transaction state storage.
     * @param txManager Transaction manager.
     * @param primaryIndex Primary index map.
     */
    public PartitionListener(
            MvPartitionStorage store,
            TxStateStorage txStateStorage,
            TxManager txManager,
            ConcurrentHashMap<ByteBuffer, RowId> primaryIndex
    ) {
        this.storage = store;
        this.txStateStorage = txStateStorage;
        this.txManager = txManager;
        this.primaryIndex = primaryIndex;
    }

    /** {@inheritDoc} */
    @Override
    public void onRead(Iterator<CommandClosure<ReadCommand>> iterator) {
        iterator.forEachRemaining((CommandClosure<? extends ReadCommand> clo) -> {
            Command command = clo.command();

            assert false : "No read commands expected, [cmd=" + clo.command() + ']';
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

            if (command instanceof InsertCommand) {
                handleInsertCommand((InsertCommand) command, commandIndex);

                clo.result(null);
            } else if (command instanceof DeleteCommand) {
                handleDeleteCommand((DeleteCommand) command, commandIndex);

                clo.result(null);
            } else if (command instanceof UpdateCommand) {
                handleUpdateCommand((UpdateCommand) command, commandIndex);

                clo.result(null);
            } else if (command instanceof InsertAndUpdateAllCommand) {
                handleInsertAndUpdateAllCommand((InsertAndUpdateAllCommand) command, commandIndex);

                clo.result(null);
            } else if (command instanceof DeleteAllCommand) {
                handleDeleteAllCommand((DeleteAllCommand) command, commandIndex);

                clo.result(null);
            } else if (command instanceof FinishTxCommand) {
                try {
                    handleFinishTxCommand((FinishTxCommand) command, commandIndex);
                } catch (IgniteInternalException e) {
                    clo.result(e);
                }

                clo.result(null);
            } else if (command instanceof TxCleanupCommand) {
                handleTxCleanupCommand((TxCleanupCommand) command, commandIndex);

                clo.result(null);
            } else {
                assert false : "Command was not found [cmd=" + command + ']';
            }
        });
    }

    /**
     * Handler for the {@link InsertCommand}.
     *
     * @param cmd Command.
     * @param commandIndex Index of the RAFT command.
     */
    private void handleInsertCommand(InsertCommand cmd, long commandIndex) {
        BinaryRow row = cmd.getRow();
        UUID txId = cmd.txId();

        RowId rowId = storage.insert(row,  txId);

        primaryIndex.put(row.keySlice(), rowId);

        txsInsertedKeys.computeIfAbsent(txId, entry -> new ConcurrentHashSet<>()).add(rowId);

        txsPendingRowIds.computeIfAbsent(txId, entry -> new ConcurrentHashSet<>()).add(rowId);

        storage.lastAppliedIndex(commandIndex);
    }

    /**
     * Handler for the {@link DeleteCommand}.
     *
     * @param cmd Command.
     * @param commandIndex Index of the RAFT command.
     */
    private void handleDeleteCommand(DeleteCommand cmd, long commandIndex) {
        RowId rowId = cmd.getRowId();
        UUID txId = cmd.txId();

        storage.addWrite(rowId, null, txId);

        txsRemovedKeys.computeIfAbsent(txId, entry -> new ConcurrentHashSet<>()).add(rowId);

        txsPendingRowIds.computeIfAbsent(txId, entry -> new ConcurrentHashSet<>()).add(rowId);

        storage.lastAppliedIndex(commandIndex);
    }

    /**
     * Handler for the {@link UpdateCommand}.
     *
     * @param cmd Command.
     * @param commandIndex Index of the RAFT command.
     */
    private void handleUpdateCommand(UpdateCommand cmd, long commandIndex) {
        BinaryRow row = cmd.getRow();
        RowId rowId = cmd.getRowId();
        UUID txId = cmd.txId();

        storage.addWrite(rowId, row,  txId);

        txsPendingRowIds.computeIfAbsent(txId, entry -> new ConcurrentHashSet<>()).add(rowId);

        storage.lastAppliedIndex(commandIndex);
    }

    /**
     * Handler for the {@link InsertAndUpdateAllCommand}.
     *
     * @param cmd Command.
     * @param commandIndex Index of the RAFT command.
     */
    private void handleInsertAndUpdateAllCommand(InsertAndUpdateAllCommand cmd, long commandIndex) {
        UUID txId = cmd.txId();
        Collection<BinaryRow> rowsToInsert = cmd.getRows();
        Map<RowId, BinaryRow> rowsToUpdate = cmd.getRowsToUpdate();

        if (!CollectionUtils.nullOrEmpty(rowsToInsert)) {
            for (BinaryRow row : rowsToInsert) {
                RowId rowId = storage.insert(row,  txId);

                primaryIndex.put(row.keySlice(), rowId);

                txsInsertedKeys.computeIfAbsent(txId, entry -> new ConcurrentHashSet<>()).add(rowId);

                txsPendingRowIds.computeIfAbsent(txId, entry -> new ConcurrentHashSet<>()).add(rowId);
            }
        }

        if (!CollectionUtils.nullOrEmpty(rowsToUpdate)) {
            for (Map.Entry<RowId, BinaryRow> entry : rowsToUpdate.entrySet()) {
                storage.addWrite(entry.getKey(), entry.getValue(), txId);

                txsPendingRowIds.computeIfAbsent(txId, entry0 -> new ConcurrentHashSet<>()).add(entry.getKey());
            }
        }

        storage.lastAppliedIndex(commandIndex);
    }

    /**
     * Handler for the {@link DeleteAllCommand}.
     *
     * @param cmd Command.
     * @param commandIndex Index of the RAFT command.
     */
    private void handleDeleteAllCommand(DeleteAllCommand cmd, long commandIndex) {
        UUID txId = cmd.txId();
        Collection<RowId> rowIds = cmd.getRowIds();

        for (RowId rowId : rowIds) {
            storage.addWrite(rowId, null, txId);

            txsRemovedKeys.computeIfAbsent(txId, entry -> new ConcurrentHashSet<>()).add(rowId);

            txsPendingRowIds.computeIfAbsent(txId, entry -> new ConcurrentHashSet<>()).add(rowId);
        }

        storage.lastAppliedIndex(commandIndex);
    }

    /**
     * Handler for the {@link FinishTxCommand}.
     *
     * @param cmd Command.
     * @param commandIndex Index of the RAFT command.
     * @throws IgniteInternalException if exception occurred during txn state change.
     */
    private void handleFinishTxCommand(FinishTxCommand cmd, long commandIndex) throws IgniteInternalException {
        UUID txId = cmd.txId();

        TxState stateToSet = cmd.commit() ? TxState.COMMITED : TxState.ABORTED;

        boolean txStateChangeRes = txStateStorage.compareAndSet(
                txId,
                null,
                new TxMeta(
                        stateToSet,
                        cmd.replicationGroupIds(),
                        cmd.commitTimestamp()
                )
        );

        if (!txStateChangeRes) {
            UUID traceId = UUID.randomUUID();

            String errorMsg = format("Fail to finish the transaction txId = {} because of inconsistent state = {},"
                    + " expected state = null, state to set = {}", txId, txStateStorage.get(txId), stateToSet);

            IgniteInternalException stateChangeException = new IgniteInternalException(traceId, TX_UNEXPECTED_STATE, errorMsg);

            // Exception is explicitly logged because otherwise it can be lost if it did not occur on the leader.
            LOG.error(errorMsg);

            throw stateChangeException;
        }

        storage.lastAppliedIndex(commandIndex);
    }


    /**
     * Handler for the {@link TxCleanupCommand}.
     *
     * @param cmd Command.
     * @param commandIndex Index of the RAFT command.
     */
    private void handleTxCleanupCommand(TxCleanupCommand cmd, long commandIndex) {
        UUID txId = cmd.txId();

        Set<RowId> removedRowIds = txsRemovedKeys.get(txId);

        Set<RowId> insertedRowIds = txsInsertedKeys.get(txId);

        Set<RowId> pendingRowIds = txsPendingRowIds.get(txId);

        // TODO: https://issues.apache.org/jira/browse/IGNITE-17577 Use HybridTimestamp instead.
        Timestamp commitTimestamp = new Timestamp(cmd.commitTimestamp().getPhysical(), cmd.commitTimestamp().getLogical());

        if (cmd.commit()) {
            pendingRowIds.forEach(rowId -> storage.commitWrite(rowId, commitTimestamp));
        } else {
            pendingRowIds.forEach(rowId -> storage.abortWrite(rowId));
        }

        if (cmd.commit()) {
            for (RowId rowId : removedRowIds) {
                primaryIndex.remove(rowId);
            }
        } else {
            for (RowId rowId : insertedRowIds) {
                primaryIndex.remove(rowId);
            }
        }

        txsRemovedKeys.remove(txId);
        txsInsertedKeys.remove(txId);
        txsPendingRowIds.remove(txId);

        storage.lastAppliedIndex(commandIndex);
    }

    /** {@inheritDoc} */
    @Override
    public void onSnapshotSave(Path path, Consumer<Throwable> doneClo) {
        // TODO: IGNITE-16644 Support snapshots.
        CompletableFuture.completedFuture(null).whenComplete((unused, throwable) -> {
            doneClo.accept(throwable);
        });
    }

    /** {@inheritDoc} */
    @Override
    public boolean onSnapshotLoad(Path path) {
        // TODO: IGNITE-16644 Support snapshots.
        return true;
    }

    /** {@inheritDoc} */
    @Override
    public void onShutdown() {
        try {
            storage.close();
        } catch (Exception e) {
            throw new IgniteInternalException("Failed to close storage: " + e.getMessage(), e);
        }
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Void> onBeforeApply(Command command) {

        return null;
    }

    /**
     * Extracts a key and a value from the {@link BinaryRow} and wraps it in a {@link DataRow}.
     *
     * @param row Binary row.
     * @return Data row.
     */
    @NotNull
    private static DataRow extractAndWrapKeyValue(@NotNull BinaryRow row) {
        return new DelegatingDataRow(new BinarySearchRow(row), row.bytes());
    }

    /**
     * Returns underlying storage.
     */
    @TestOnly
    public MvPartitionStorage getStorage() {
        return storage;
    }

    /**
     * Returns a primary index map.
     */
    @TestOnly
    public Map<ByteBuffer, RowId> getPk() {
        return primaryIndex;
    }
}
