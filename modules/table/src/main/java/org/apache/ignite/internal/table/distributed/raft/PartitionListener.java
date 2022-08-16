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
import java.util.Collections;
import java.util.Iterator;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.storage.DataRow;
import org.apache.ignite.internal.storage.basic.BinarySearchRow;
import org.apache.ignite.internal.storage.basic.DelegatingDataRow;
import org.apache.ignite.internal.table.distributed.command.DeleteAllCommand;
import org.apache.ignite.internal.table.distributed.command.DeleteCommand;
import org.apache.ignite.internal.table.distributed.command.DeleteExactAllCommand;
import org.apache.ignite.internal.table.distributed.command.DeleteExactCommand;
import org.apache.ignite.internal.table.distributed.command.FinishTxCommand;
import org.apache.ignite.internal.table.distributed.command.GetAllCommand;
import org.apache.ignite.internal.table.distributed.command.GetAndDeleteCommand;
import org.apache.ignite.internal.table.distributed.command.GetAndReplaceCommand;
import org.apache.ignite.internal.table.distributed.command.GetAndUpsertCommand;
import org.apache.ignite.internal.table.distributed.command.GetCommand;
import org.apache.ignite.internal.table.distributed.command.InsertAllCommand;
import org.apache.ignite.internal.table.distributed.command.InsertCommand;
import org.apache.ignite.internal.table.distributed.command.MultiKeyCommand;
import org.apache.ignite.internal.table.distributed.command.ReplaceCommand;
import org.apache.ignite.internal.table.distributed.command.ReplaceIfExistCommand;
import org.apache.ignite.internal.table.distributed.command.SingleKeyCommand;
import org.apache.ignite.internal.table.distributed.command.TransactionalCommand;
import org.apache.ignite.internal.table.distributed.command.UpsertAllCommand;
import org.apache.ignite.internal.table.distributed.command.UpsertCommand;
import org.apache.ignite.internal.table.distributed.command.response.MultiRowsResponse;
import org.apache.ignite.internal.table.distributed.command.response.SingleRowResponse;
import org.apache.ignite.internal.table.distributed.storage.VersionedRowStore;
import org.apache.ignite.internal.tx.LockKey;
import org.apache.ignite.internal.tx.LockManager;
import org.apache.ignite.internal.tx.TxManager;
import org.apache.ignite.internal.tx.TxState;
import org.apache.ignite.lang.IgniteInternalException;
import org.apache.ignite.raft.client.Command;
import org.apache.ignite.raft.client.ReadCommand;
import org.apache.ignite.raft.client.WriteCommand;
import org.apache.ignite.raft.client.service.CommandClosure;
import org.apache.ignite.raft.client.service.RaftGroupListener;
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

    /** The versioned storage. */
    private final VersionedRowStore storage;

    /** Transaction manager. */
    private final TxManager txManager;

    /**
     * The constructor.
     *
     * @param tableId Table id.
     * @param store  The storage.
     */
    public PartitionListener(UUID tableId, VersionedRowStore store) {
        this.lockContextId = tableId;
        this.storage = store;
        this.txManager = store.txManager();
    }

    /** {@inheritDoc} */
    @Override
    public void onRead(Iterator<CommandClosure<ReadCommand>> iterator) {
        iterator.forEachRemaining((CommandClosure<? extends ReadCommand> clo) -> {
            Command command = clo.command();

            if (!tryEnlistIntoTransaction(command, clo)) {
                return;
            }

            if (command instanceof GetCommand) {
                clo.result(handleGetCommand((GetCommand) command));
            } else if (command instanceof GetAllCommand) {
                clo.result(handleGetAllCommand((GetAllCommand) command));
            } else {
                assert false : "Command was not found [cmd=" + clo.command() + ']';
            }
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

            // TODO IGNITE-17081 IGNITE-17077
            // Applied index is set non-atomically. This is a wrong and non-recoverable behavior. Will be fixed later.
            storage.lastAppliedIndex(commandIndex);

            if (command instanceof InsertCommand) {
                clo.result(handleInsertCommand((InsertCommand) command));
            } else if (command instanceof DeleteCommand) {
                clo.result(handleDeleteCommand((DeleteCommand) command));
            } else if (command instanceof ReplaceCommand) {
                clo.result(handleReplaceCommand((ReplaceCommand) command));
            } else if (command instanceof UpsertCommand) {
                handleUpsertCommand((UpsertCommand) command);

                clo.result(null);
            } else if (command instanceof InsertAllCommand) {
                clo.result(handleInsertAllCommand((InsertAllCommand) command));
            } else if (command instanceof UpsertAllCommand) {
                handleUpsertAllCommand((UpsertAllCommand) command);

                clo.result(null);
            } else if (command instanceof DeleteAllCommand) {
                clo.result(handleDeleteAllCommand((DeleteAllCommand) command));
            } else if (command instanceof DeleteExactCommand) {
                clo.result(handleDeleteExactCommand((DeleteExactCommand) command));
            } else if (command instanceof DeleteExactAllCommand) {
                clo.result(handleDeleteExactAllCommand((DeleteExactAllCommand) command));
            } else if (command instanceof ReplaceIfExistCommand) {
                clo.result(handleReplaceIfExistsCommand((ReplaceIfExistCommand) command));
            } else if (command instanceof GetAndDeleteCommand) {
                clo.result(handleGetAndDeleteCommand((GetAndDeleteCommand) command));
            } else if (command instanceof GetAndReplaceCommand) {
                clo.result(handleGetAndReplaceCommand((GetAndReplaceCommand) command));
            } else if (command instanceof GetAndUpsertCommand) {
                clo.result(handleGetAndUpsertCommand((GetAndUpsertCommand) command));
            } else if (command instanceof FinishTxCommand) {
                clo.result(handleFinishTxCommand((FinishTxCommand) command));
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
        if (command instanceof TransactionalCommand) {
            UUID txId = ((TransactionalCommand) command).getTxId();

            TxState state = txManager.getOrCreateTransaction(txId);

            if (state != null && state != TxState.PENDING) {
                clo.result(new TransactionException(format("Failed to enlist a key into a transaction, state={}", state)));

                return false;
            }
        }

        return true;
    }

    /**
     * Handler for the {@link GetCommand}.
     *
     * @param cmd Command.
     * @return Result.
     */
    private SingleRowResponse handleGetCommand(GetCommand cmd) {
        return new SingleRowResponse(storage.get(cmd.getRow(), cmd.getTxId()));
    }

    /**
     * Handler for the {@link GetAllCommand}.
     *
     * @param cmd Command.
     * @return Result.
     */
    private MultiRowsResponse handleGetAllCommand(GetAllCommand cmd) {
        Collection<BinaryRow> keyRows = cmd.getRows();

        assert keyRows != null && !keyRows.isEmpty();

        // TODO asch IGNITE-15934 all reads are sequential, can be parallelized ?
        return new MultiRowsResponse(storage.getAll(keyRows, cmd.getTxId()));
    }

    /**
     * Handler for the {@link InsertCommand}.
     *
     * @param cmd Command.
     * @return Result.
     */
    private boolean handleInsertCommand(InsertCommand cmd) {
        return storage.insert(cmd.getRow(), cmd.getTxId());
    }

    /**
     * Handler for the {@link DeleteCommand}.
     *
     * @param cmd Command.
     * @return Result.
     */
    private boolean handleDeleteCommand(DeleteCommand cmd) {
        return storage.delete(cmd.getRow(), cmd.getTxId());
    }

    /**
     * Handler for the {@link ReplaceCommand}.
     *
     * @param cmd Command.
     * @return Result.
     */
    private boolean handleReplaceCommand(ReplaceCommand cmd) {
        return storage.replace(cmd.getOldRow(), cmd.getRow(), cmd.getTxId());
    }

    /**
     * Handler for the {@link UpsertCommand}.
     *
     * @param cmd Command.
     */
    private void handleUpsertCommand(UpsertCommand cmd) {
        storage.upsert(cmd.getRow(), cmd.getTxId());
    }

    /**
     * Handler for the {@link InsertAllCommand}.
     *
     * @param cmd Command.
     * @return Result.
     */
    private MultiRowsResponse handleInsertAllCommand(InsertAllCommand cmd) {
        Collection<BinaryRow> rows = cmd.getRows();

        assert rows != null && !rows.isEmpty();

        return new MultiRowsResponse(storage.insertAll(rows, cmd.getTxId()));
    }

    /**
     * Handler for the {@link UpsertAllCommand}.
     *
     * @param cmd Command.
     * @return Result.
     */
    private void handleUpsertAllCommand(UpsertAllCommand cmd) {
        Collection<BinaryRow> rows = cmd.getRows();

        assert rows != null && !rows.isEmpty();

        storage.upsertAll(rows, cmd.getTxId());
    }

    /**
     * Handler for the {@link DeleteAllCommand}.
     *
     * @param cmd Command.
     * @return Result.
     */
    private MultiRowsResponse handleDeleteAllCommand(DeleteAllCommand cmd) {
        Collection<BinaryRow> rows = cmd.getRows();

        assert rows != null && !rows.isEmpty();

        return new MultiRowsResponse(storage.deleteAll(rows, cmd.getTxId()));
    }

    /**
     * Handler for the {@link DeleteExactCommand}.
     *
     * @param cmd Command.
     * @return Result.
     */
    private boolean handleDeleteExactCommand(DeleteExactCommand cmd) {
        BinaryRow row = cmd.getRow();

        assert row != null;
        assert row.hasValue();

        return storage.deleteExact(row, cmd.getTxId());
    }

    /**
     * Handler for the {@link DeleteExactAllCommand}.
     *
     * @param cmd Command.
     * @return Result.
     */
    private MultiRowsResponse handleDeleteExactAllCommand(DeleteExactAllCommand cmd) {
        Collection<BinaryRow> rows = cmd.getRows();

        assert rows != null && !rows.isEmpty();

        return new MultiRowsResponse(storage.deleteAllExact(rows, cmd.getTxId()));
    }

    /**
     * Handler for the {@link ReplaceIfExistCommand}.
     *
     * @param cmd Command.
     * @return Result.
     */
    private boolean handleReplaceIfExistsCommand(ReplaceIfExistCommand cmd) {
        BinaryRow row = cmd.getRow();

        assert row != null;

        return storage.replace(row, cmd.getTxId());
    }

    /**
     * Handler for the {@link GetAndDeleteCommand}.
     *
     * @param cmd Command.
     * @return Result.
     */
    private SingleRowResponse handleGetAndDeleteCommand(GetAndDeleteCommand cmd) {
        BinaryRow row = cmd.getRow();

        assert row != null;

        return new SingleRowResponse(storage.getAndDelete(row, cmd.getTxId()));
    }

    /**
     * Handler for the {@link GetAndReplaceCommand}.
     *
     * @param cmd Command.
     * @return Result.
     */
    private SingleRowResponse handleGetAndReplaceCommand(GetAndReplaceCommand cmd) {
        BinaryRow row = cmd.getRow();

        assert row != null && row.hasValue();

        return new SingleRowResponse(storage.getAndReplace(row, cmd.getTxId()));
    }

    /**
     * Handler for the {@link GetAndUpsertCommand}.
     *
     * @param cmd Command.
     * @return Result.
     */
    private SingleRowResponse handleGetAndUpsertCommand(GetAndUpsertCommand cmd) {
        BinaryRow row = cmd.getRow();

        assert row != null && row.hasValue();

        return new SingleRowResponse(storage.getAndUpsert(row, cmd.getTxId()));
    }

    /**
     * Handler for the {@link FinishTxCommand}.
     *
     * @param cmd Command.
     * @return Result.
     */
    private boolean handleFinishTxCommand(FinishTxCommand cmd) {
        UUID txId = cmd.txId();

        boolean stateChanged = txManager.changeState(txId, TxState.PENDING, cmd.finish() ? TxState.COMMITED : TxState.ABORTED);

        LockManager lockManager = txManager.lockManager();

        // This code is technically incorrect and assumes that "stateChanged" is always true. This was done because transaction state is not
        // persisted and thus FinishTxCommand couldn't be completed on recovery after node restart ("changeState" uses "replace").
        if (/*txManager.state(txId) == TxState.COMMITED*/cmd.finish()) {
            lockManager.locks(txId)
                    .forEachRemaining(
                            lock -> {
                                storage.commitWrite((ByteBuffer) lock.lockKey().key(), txId);
                            }
                    );
        } else /*if (txManager.state(txId) == TxState.ABORTED)*/ {
            lockManager.locks(txId)
                    .forEachRemaining(
                            lock -> {
                                storage.abortWrite((ByteBuffer) lock.lockKey().key());
                            }
                    );
        }

        // TODO: tmp
        if (/*txManager.state(txId) == TxState.COMMITED*/cmd.finish()) {
            storage.pendingKeys.getOrDefault(txId, Collections.emptyList()).forEach(key -> storage.commitWrite((ByteBuffer) key, txId));
        } else /*if (txManager.state(txId) == TxState.ABORTED)*/ {
            storage.pendingKeys.getOrDefault(txId, Collections.emptyList()).forEach(key -> storage.abortWrite((ByteBuffer) key));
        }

        return stateChanged;
    }

    /** {@inheritDoc} */
    @Override
    public void onSnapshotSave(Path path, Consumer<Throwable> doneClo) {
        storage.snapshot(path).whenComplete((unused, throwable) -> {
            doneClo.accept(throwable);
        });
    }

    /** {@inheritDoc} */
    @Override
    public boolean onSnapshotLoad(Path path) {
        storage.restoreSnapshot(path);

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
        if (command instanceof SingleKeyCommand) {
            SingleKeyCommand cmd0 = (SingleKeyCommand) command;

            return cmd0 instanceof ReadCommand
                    ? txManager.readLock(lockContextId, cmd0.getRow().keySlice(), cmd0.getTxId()).thenAccept(ignored -> {}) :
                    txManager.writeLock(lockContextId, cmd0.getRow().keySlice(), cmd0.getTxId()).thenAccept(ignored -> {});
        } else if (command instanceof MultiKeyCommand) {
            MultiKeyCommand cmd0 = (MultiKeyCommand) command;

            Collection<BinaryRow> rows = cmd0.getRows();

            CompletableFuture<Void>[] futs = new CompletableFuture[rows.size()];

            int i = 0;
            boolean read = cmd0 instanceof ReadCommand;

            for (BinaryRow row : rows) {
                futs[i++] = read ? txManager.readLock(lockContextId, row.keySlice(), cmd0.getTxId()).thenAccept(ignored -> {}) :
                        txManager.writeLock(lockContextId, row.keySlice(), cmd0.getTxId()).thenAccept(ignored -> {});
            }

            return CompletableFuture.allOf(futs);
        }

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
    public VersionedRowStore getStorage() {
        return storage;
    }
}
