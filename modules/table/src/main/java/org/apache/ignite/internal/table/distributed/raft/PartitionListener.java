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
import org.apache.ignite.internal.table.distributed.command.PartitionCommand;
import org.apache.ignite.internal.table.distributed.command.UpdateCommand;
import org.apache.ignite.internal.tx.LockKey;
import org.apache.ignite.internal.tx.LockManager;
import org.apache.ignite.internal.tx.LockMode;
import org.apache.ignite.internal.tx.Timestamp;
import org.apache.ignite.internal.tx.TxManager;
import org.apache.ignite.internal.tx.TxState;
import org.apache.ignite.internal.util.CollectionUtils;
import org.apache.ignite.lang.IgniteInternalException;
import org.apache.ignite.raft.client.Command;
import org.apache.ignite.raft.client.ReadCommand;
import org.apache.ignite.raft.client.WriteCommand;
import org.apache.ignite.raft.client.service.CommandClosure;
import org.apache.ignite.raft.client.service.RaftGroupListener;
import org.apache.ignite.raft.jraft.util.concurrent.ConcurrentHashSet;
import org.apache.ignite.tx.TransactionException;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.TestOnly;

/**
 * Partition command handler.
 */
public class PartitionListener implements RaftGroupListener {
    /**
     * Lock context id.
     *
     * @see LockKey#contextId()
     */
    private final UUID lockContextId;

    /** Versioned partition storage. */
    private final MvPartitionStorage storage;

    /** Transaction manager. */
    private final TxManager txManager;

    //TODO: https://issues.apache.org/jira/browse/IGNITE-17205 Temporary solution until the implementation of the primary index is done.
    /** Dummy primary index. */
    private final ConcurrentHashMap<ByteBuffer, RowId> primaryIndex;

    /** Keys that were inserted by the transaction. */
    private ConcurrentHashMap<UUID, Set<RowId>> txsInsertedKeys = new ConcurrentHashMap<>();

    /** Keys that were removed by the transaction. */
    private ConcurrentHashMap<UUID, Set<RowId>> txsRemovedKeys = new ConcurrentHashMap<>();

    /**
     * The constructor.
     *
     * @param tableId Table id.
     * @param store  The storage.
     * @param txManager Transaction manager.
     * @param primaryIndex Primary index map.
     */
    public PartitionListener(
            UUID tableId,
            MvPartitionStorage store,
            TxManager txManager,
            ConcurrentHashMap<ByteBuffer, RowId> primaryIndex
    ) {
        this.lockContextId = tableId;
        this.storage = store;
        this.txManager = txManager;
        this.primaryIndex = primaryIndex;
    }

    /** {@inheritDoc} */
    @Override
    public void onRead(Iterator<CommandClosure<ReadCommand>> iterator) {
        iterator.forEachRemaining((CommandClosure<? extends ReadCommand> clo) -> {
            Command command = clo.command();

            if (!tryEnlistIntoTransaction(command, clo)) {
                return;
            }

            assert false : "Command was not found [cmd=" + clo.command() + ']';
        });
    }

    /** {@inheritDoc} */
    @Override
    public void onWrite(Iterator<CommandClosure<WriteCommand>> iterator) {
        iterator.forEachRemaining((CommandClosure<? extends WriteCommand> clo) -> {
            Command command = clo.command();

            if (!tryEnlistIntoTransaction(command, clo)) {
                return;
            }

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
                clo.result(handleFinishTxCommand((FinishTxCommand) command, commandIndex));
            } else {
                assert false : "Command was not found [cmd=" + command + ']';
            }
        });
    }

    /**
     * Attempts to enlist a command into a transaction.
     *
     * @param command The command.
     * @param clo     The closure.
     * @return {@code true} if a command is compatible with a transaction state or a command is not transactional.
     */
    private boolean tryEnlistIntoTransaction(Command command, CommandClosure<?> clo) {
        if (command instanceof PartitionCommand) {
            UUID txId = ((PartitionCommand) command).getTxId();

            TxState state = txManager.getOrCreateTransaction(txId);

            if (state != null && state != TxState.PENDING) {
                clo.result(new TransactionException(format("Failed to enlist a key into a transaction, state={}", state)));

                return false;
            }
        }

        return true;
    }

    /**
     * Handler for the {@link InsertCommand}.
     *
     * @param cmd Command.
     * @param commandIndex Index of the RAFT command.
     */
    private void handleInsertCommand(InsertCommand cmd, long commandIndex) {
        BinaryRow row = cmd.getRow();
        UUID txId = cmd.getTxId();

        RowId rowId = storage.insert(row,  txId);

        primaryIndex.put(row.keySlice(), rowId);

        txsInsertedKeys.computeIfAbsent(txId, entry -> new ConcurrentHashSet<RowId>()).add(rowId);

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
        UUID txId = cmd.getTxId();

        storage.addWrite(rowId, null, txId);

        txsRemovedKeys.computeIfAbsent(txId, entry -> new ConcurrentHashSet<RowId>()).add(rowId);

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
        UUID txId = cmd.getTxId();

        storage.addWrite(rowId, row,  txId);

        storage.lastAppliedIndex(commandIndex);
    }

    /**
     * Handler for the {@link InsertAndUpdateAllCommand}.
     *
     * @param cmd Command.
     * @param commandIndex Index of the RAFT command.
     */
    private void handleInsertAndUpdateAllCommand(InsertAndUpdateAllCommand cmd, long commandIndex) {
        UUID txId = cmd.getTxId();
        Collection<BinaryRow> rowsToInsert = cmd.getRows();
        Map<RowId, BinaryRow> rowsToUpdate = cmd.getRowsToUpdate();

        if (!CollectionUtils.nullOrEmpty(rowsToInsert)) {
            for (BinaryRow row : rowsToInsert) {
                RowId rowId = storage.insert(row,  txId);

                primaryIndex.put(row.keySlice(), rowId);

                txsInsertedKeys.computeIfAbsent(txId, entry -> new ConcurrentHashSet<RowId>()).add(rowId);
            }
        }

        if (!CollectionUtils.nullOrEmpty(rowsToUpdate)) {
            for (Map.Entry<RowId, BinaryRow> entry : rowsToUpdate.entrySet()) {
                storage.addWrite(entry.getKey(), entry.getValue(), txId);
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
        UUID txId = cmd.getTxId();
        Collection<RowId> rowIds = cmd.getRowIds();

        for (RowId rowId : rowIds) {
            storage.addWrite(rowId, null, txId);

            txsRemovedKeys.computeIfAbsent(txId, entry -> new ConcurrentHashSet<RowId>()).add(rowId);
        }

        storage.lastAppliedIndex(commandIndex);
    }

    /**
     * Handler for the {@link FinishTxCommand}.
     *
     * @param cmd Command.
     * @param commandIndex Index of the RAFT command.
     * @return Result.
     */
    private boolean handleFinishTxCommand(FinishTxCommand cmd, long commandIndex) {
        UUID txId = cmd.getTxId();

        boolean stateChanged = txManager.changeState(txId, TxState.PENDING, cmd.finish() ? TxState.COMMITED : TxState.ABORTED);

        LockManager lockManager = txManager.lockManager();

        // This code is technically incorrect and assumes that "stateChanged" is always true.
        // This was done because transaction state is not persisted
        // and thus FinishTxCommand couldn't be completed on recovery after node restart ("changeState" uses "replace").
        if (/*txManager.state(txId) == TxState.COMMITED*/cmd.finish()) {
            lockManager.locks(txId)
                    .forEachRemaining(
                            lock -> {
                                if (lock.lockMode() == LockMode.EXCLUSIVE && lock.lockKey().key() instanceof RowId) {
                                    storage.commitWrite((RowId) lock.lockKey().key(), new Timestamp(txId));
                                }
                            }
                    );
        } else /*if (txManager.state(txId) == TxState.ABORTED)*/ {
            lockManager.locks(txId)
                    .forEachRemaining(
                            lock -> {
                                if (lock.lockMode() == LockMode.EXCLUSIVE && lock.lockKey().key() instanceof RowId) {
                                    storage.abortWrite((RowId) lock.lockKey().key());
                                }
                            }
                    );
        }

        // TODO: tmp
        if (/*txManager.state(txId) == TxState.COMMITED*/cmd.finish()) {
            for (RowId rowId : txsRemovedKeys.get(txId)) {
                primaryIndex.remove(rowId);
            }
        } else /*if (txManager.state(txId) == TxState.ABORTED)*/ {
            for (RowId rowId : txsInsertedKeys.get(txId)) {
                primaryIndex.remove(rowId);
            }
        }

        txsRemovedKeys.remove(txId);
        txsInsertedKeys.remove(txId);

        storage.lastAppliedIndex(commandIndex);

        return stateChanged;
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
