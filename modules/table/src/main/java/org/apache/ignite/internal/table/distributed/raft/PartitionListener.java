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

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.storage.DataRow;
import org.apache.ignite.internal.storage.SearchRow;
import org.apache.ignite.internal.storage.basic.SimpleDataRow;
import org.apache.ignite.internal.table.distributed.command.DeleteAllCommand;
import org.apache.ignite.internal.table.distributed.command.DeleteCommand;
import org.apache.ignite.internal.table.distributed.command.DeleteExactAllCommand;
import org.apache.ignite.internal.table.distributed.command.DeleteExactCommand;
import org.apache.ignite.internal.raft.server.FinishTxCommand;
import org.apache.ignite.internal.table.distributed.command.GetAllCommand;
import org.apache.ignite.internal.table.distributed.command.GetAndDeleteCommand;
import org.apache.ignite.internal.table.distributed.command.GetAndReplaceCommand;
import org.apache.ignite.internal.table.distributed.command.GetAndUpsertCommand;
import org.apache.ignite.internal.table.distributed.command.GetCommand;
import org.apache.ignite.internal.table.distributed.command.InsertAllCommand;
import org.apache.ignite.internal.table.distributed.command.InsertCommand;
import org.apache.ignite.internal.table.distributed.command.ReplaceCommand;
import org.apache.ignite.internal.table.distributed.command.ReplaceIfExistCommand;
import org.apache.ignite.internal.table.distributed.command.UpsertAllCommand;
import org.apache.ignite.internal.table.distributed.command.UpsertCommand;
import org.apache.ignite.internal.table.distributed.command.response.MultiRowsResponse;
import org.apache.ignite.internal.table.distributed.command.response.SingleRowResponse;
import org.apache.ignite.internal.table.distributed.storage.VersionedRowStore;
import org.apache.ignite.internal.tx.Timestamp;
import org.apache.ignite.internal.tx.TxManager;
import org.apache.ignite.internal.tx.TxState;
import org.apache.ignite.lang.IgniteInternalException;
import org.apache.ignite.raft.client.Command;
import org.apache.ignite.raft.client.ReadCommand;
import org.apache.ignite.raft.client.WriteCommand;
import org.apache.ignite.raft.client.service.CommandClosure;
import org.apache.ignite.raft.client.service.RaftGroupListener;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.TestOnly;

/**
 * Partition command handler.
 */
public class PartitionListener implements RaftGroupListener {
    /** Table ID. */
    private final UUID tableId;

    /**
     * Storage.
     * This is a temporary solution, it will apply until persistence layer would not be implemented.
     * TODO: IGNITE-14790.
     */
    private final VersionedRowStore storage;

    /** TX manager. */
    private final TxManager txManager;

    /**
     * @param tableId Table id.
     * @param store The storage.
     */
    public PartitionListener(UUID tableId, VersionedRowStore store) {
        this.tableId = tableId;
        this.storage = store;
        this.txManager = store.txManager();
    }

    /** {@inheritDoc} */
    @Override public void onRead(Iterator<CommandClosure<ReadCommand>> iterator) {
        iterator.forEachRemaining((CommandClosure<? extends ReadCommand> clo) -> {
            if (clo.command() instanceof GetCommand)
                handleGetCommand((CommandClosure<GetCommand>) clo);
            else if (clo.command() instanceof GetAllCommand)
                handleGetAllCommand((CommandClosure<GetAllCommand>) clo);
            else
                assert false : "Command was not found [cmd=" + clo.command() + ']';
        });
    }

    /** {@inheritDoc} */
    @Override public void onWrite(Iterator<CommandClosure<WriteCommand>> iterator) {
        iterator.forEachRemaining((CommandClosure<? extends WriteCommand> clo) -> {
            Command command = clo.command();

            if (command instanceof InsertCommand)
                handleInsertCommand((CommandClosure<InsertCommand>) clo);
            else if (command instanceof DeleteCommand)
                handleDeleteCommand((CommandClosure<DeleteCommand>) clo);
            else if (command instanceof ReplaceCommand)
                handleReplaceCommand((CommandClosure<ReplaceCommand>) clo);
            else if (command instanceof UpsertCommand)
                handleUpsertCommand((CommandClosure<UpsertCommand>) clo);
            else if (command instanceof InsertAllCommand)
                handleInsertAllCommand((CommandClosure<InsertAllCommand>) clo);
            else if (command instanceof UpsertAllCommand)
                handleUpsertAllCommand((CommandClosure<UpsertAllCommand>) clo);
            else if (command instanceof DeleteAllCommand)
                handleDeleteAllCommand((CommandClosure<DeleteAllCommand>) clo);
            else if (command instanceof DeleteExactCommand)
                handleDeleteExactCommand((CommandClosure<DeleteExactCommand>) clo);
            else if (command instanceof DeleteExactAllCommand)
                handleDeleteExactAllCommand((CommandClosure<DeleteExactAllCommand>) clo);
            else if (command instanceof ReplaceIfExistCommand)
                handleReplaceIfExistsCommand((CommandClosure<ReplaceIfExistCommand>) clo);
            else if (command instanceof GetAndDeleteCommand)
                handleGetAndDeleteCommand((CommandClosure<GetAndDeleteCommand>) clo);
            else if (command instanceof GetAndReplaceCommand)
                handleGetAndReplaceCommand((CommandClosure<GetAndReplaceCommand>) clo);
            else if (command instanceof GetAndUpsertCommand)
                handleGetAndUpsertCommand((CommandClosure<GetAndUpsertCommand>) clo);
            else if (command instanceof FinishTxCommand)
                handleFinishTxCommand((CommandClosure<FinishTxCommand>) clo);
            else
                assert false : "Command was not found [cmd=" + command + ']';
        });
    }

    /**
     * Handler for the {@link GetCommand}.
     *
     * @param clo Command closure.
     */
    private void handleGetCommand(CommandClosure<GetCommand> clo) {
        GetCommand cmd = (GetCommand) clo.command();

        clo.result(new SingleRowResponse(storage.get(cmd.getKeyRow(), cmd.getTimestamp())));
    }

    /**
     * Handler for the {@link GetAllCommand}.
     *
     * @param clo Command closure.
     */
    private void handleGetAllCommand(CommandClosure<GetAllCommand> clo) {
        Set<BinaryRow> keyRows = ((GetAllCommand)clo.command()).getKeyRows();

        assert keyRows != null && !keyRows.isEmpty();

        // TODO asch all reads are sequeti
        clo.result(new MultiRowsResponse(storage.getAll(keyRows, null)));
    }

    /**
     * Handler for the {@link InsertCommand}.
     *
     * @param clo Command closure.
     */
    private void handleInsertCommand(CommandClosure<InsertCommand> clo) {
        InsertCommand cmd = (InsertCommand) clo.command();

        clo.result(storage.insert(cmd.getRow(), cmd.getTimestamp()));
    }

    /**
     * Handler for the {@link DeleteCommand}.
     *
     * @param clo Command closure.
     */
    private void handleDeleteCommand(CommandClosure<DeleteCommand> clo) {
        DeleteCommand cmd = clo.command();

        clo.result(storage.delete(cmd.getKeyRow(), cmd.getTimestamp()));
    }

    /**
     * Handler for the {@link ReplaceCommand}.
     *
     * @param clo Command closure.
     */
    private void handleReplaceCommand(CommandClosure<ReplaceCommand> clo) {
        ReplaceCommand cmd = ((ReplaceCommand)clo.command());

        clo.result(storage.replace(cmd.getOldRow(), cmd.getRow(), null));
    }

    /**
     * Handler for the {@link UpsertCommand}.
     *
     * @param clo Command closure.
     */
    private void handleUpsertCommand(CommandClosure<UpsertCommand> clo) {
        UpsertCommand cmd = (UpsertCommand) clo.command();

        storage.upsert(cmd.getRow(), cmd.getTimestamp());

        clo.result(null);
    }

    /**
     * Handler for the {@link InsertAllCommand}.
     *
     * @param clo Command closure.
     */
    private void handleInsertAllCommand(CommandClosure<InsertAllCommand> clo) {
        Set<BinaryRow> rows = ((InsertAllCommand)clo.command()).getRows();

        assert rows != null && !rows.isEmpty();

        clo.result(new MultiRowsResponse(storage.insertAll(rows, null)));
    }

    /**
     * Handler for the {@link UpsertAllCommand}.
     *
     * @param clo Command closure.
     */
    private void handleUpsertAllCommand(CommandClosure<UpsertAllCommand> clo) {
        Set<BinaryRow> rows = ((UpsertAllCommand)clo.command()).getRows();

        assert rows != null && !rows.isEmpty();

        storage.upsertAll(rows, null);

        clo.result(null);
    }

    /**
     * Handler for the {@link DeleteAllCommand}.
     *
     * @param clo Command closure.
     */
    private void handleDeleteAllCommand(CommandClosure<DeleteAllCommand> clo) {
        Set<BinaryRow> rows = ((DeleteAllCommand)clo.command()).getRows();

        assert rows != null && !rows.isEmpty();

        clo.result(new MultiRowsResponse(storage.deleteAll(rows, null)));
    }

    /**
     * Handler for the {@link DeleteExactCommand}.
     *
     * @param clo Command closure.
     */
    private void handleDeleteExactCommand(CommandClosure<DeleteExactCommand> clo) {
        BinaryRow row = ((DeleteExactCommand)clo.command()).getRow();

        assert row != null;
        assert row.hasValue();

        clo.result(storage.deleteExact(row, null));
    }

    /**
     * Handler for the {@link DeleteExactAllCommand}.
     *
     * @param clo Command closure.
     */
    private void handleDeleteExactAllCommand(CommandClosure<DeleteExactAllCommand> clo) {
        Set<BinaryRow> rows = ((DeleteExactAllCommand)clo.command()).getRows();

        assert rows != null && !rows.isEmpty();

        clo.result(new MultiRowsResponse(storage.deleteAll(rows, null)));
    }

    /**
     * Handler for the {@link ReplaceIfExistCommand}.
     *
     * @param clo Command closure.
     */
    private void handleReplaceIfExistsCommand(CommandClosure<ReplaceIfExistCommand> clo) {
        BinaryRow row = ((ReplaceIfExistCommand)clo.command()).getRow();

        assert row != null;

        clo.result(storage.replace(row, null));
    }

    /**
     * Handler for the {@link GetAndDeleteCommand}.
     *
     * @param clo Command closure.
     */
    private void handleGetAndDeleteCommand(CommandClosure<GetAndDeleteCommand> clo) {
        BinaryRow row = ((GetAndDeleteCommand)clo.command()).getKeyRow();

        assert row != null;

        clo.result(new SingleRowResponse(storage.getAndDelete(row, null)));
    }

    /**
     * Handler for the {@link GetAndReplaceCommand}.
     *
     * @param clo Command closure.
     */
    private void handleGetAndReplaceCommand(CommandClosure<GetAndReplaceCommand> clo) {
        BinaryRow row = ((GetAndReplaceCommand)clo.command()).getRow();

        assert row != null && row.hasValue();

        clo.result(new SingleRowResponse(storage.getAndReplace(row, null)));
    }

    /**
     * Handler for the {@link GetAndUpsertCommand}.
     *
     * @param clo Command closure.
     */
    private void handleGetAndUpsertCommand(CommandClosure<GetAndUpsertCommand> clo) {
        BinaryRow row = ((GetAndUpsertCommand)clo.command()).getKeyRow();

        assert row != null && row.hasValue();

        clo.result(new SingleRowResponse(storage.getAndUpsert(row, null)));
    }

    /**
     * Handler for the {@link FinishTxCommand}.
     *
     * @param clo Command closure.
     */
    private void handleFinishTxCommand(CommandClosure<FinishTxCommand> clo) {
        FinishTxCommand cmd = clo.command();

        Timestamp ts = cmd.timestamp();
        boolean commit = cmd.finish();

        clo.result(txManager.changeState(ts, TxState.PENDING, commit ? TxState.COMMITED : TxState.ABORTED));
    }

    /** {@inheritDoc} */
    @Override public void onSnapshotSave(Path path, Consumer<Throwable> doneClo) {
        storage.snapshot(path).whenComplete((unused, throwable) -> {
            doneClo.accept(throwable);
        });
    }

    /** {@inheritDoc} */
    @Override public boolean onSnapshotLoad(Path path) {
        storage.restoreSnapshot(path);

        return true;
    }

    /** {@inheritDoc} */
    @Override public void onShutdown() {
        try {
            storage.close();
        }
        catch (Exception e) {
            throw new IgniteInternalException("Failed to close storage: " + e.getMessage(), e);
        }
    }

    /** {@inheritDoc}
     * @param cmd*/
    @Override public CompletableFuture<Void> onBeforeApply(Command cmd) {
        // TODO asch refactor copypaste
        if (cmd instanceof InsertCommand) {
            InsertCommand cmd0 = (InsertCommand) cmd;

            txManager.getOrCreateTransaction(cmd0.getTimestamp()); // TODO asch handle race between rollback and lock.

            return txManager.writeLock(tableId, cmd0.getRow().keySlice(), cmd0.getTimestamp());
        }
        else if (cmd instanceof UpsertCommand) {
            UpsertCommand cmd0 = (UpsertCommand) cmd;

            txManager.getOrCreateTransaction(cmd0.getTimestamp());

            return txManager.writeLock(tableId, cmd0.getRow().keySlice(), cmd0.getTimestamp());
        }
        else if (cmd instanceof GetCommand) {
            GetCommand cmd0 = (GetCommand) cmd;

            txManager.getOrCreateTransaction(cmd0.getTimestamp());

            return txManager.readLock(tableId, cmd0.getKeyRow().keySlice(), cmd0.getTimestamp());
        }

        return null;
    }

    /**
     * Extracts a key and a value from the {@link BinaryRow} and wraps it in a {@link DataRow}.
     *
     * @param row Binary row.
     * @return Data row.
     */
    @NotNull private static DataRow extractAndWrapKeyValue(@NotNull BinaryRow row) {
        byte[] key = new byte[row.keySlice().capacity()];

        row.keySlice().get(key);

        return new SimpleDataRow(key, row.bytes());
    }

    /**
     * Adapter that converts a {@link BinaryRow} into a {@link SearchRow}.
     */
    private static class BinarySearchRow implements SearchRow {
        /** Search key. */
        private final byte[] keyBytes;

        /**
         * Constructor.
         *
         * @param row Row to search for.
         */
        BinarySearchRow(BinaryRow row) {
            keyBytes = new byte[row.keySlice().capacity()];

            row.keySlice().get(keyBytes);
        }

        /** {@inheritDoc} */
        @Override public byte @NotNull [] keyBytes() {
            return keyBytes;
        }

        /** {@inheritDoc} */
        @Override public @NotNull ByteBuffer key() {
            return ByteBuffer.wrap(keyBytes);
        }
    }

    /**
     * @return Underlying storage.
     */
    @TestOnly
    public VersionedRowStore getStorage() {
        return storage;
    }
}
