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

import java.nio.file.Path;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.table.distributed.command.DeleteAllCommand;
import org.apache.ignite.internal.table.distributed.command.DeleteCommand;
import org.apache.ignite.internal.table.distributed.command.DeleteExactAllCommand;
import org.apache.ignite.internal.table.distributed.command.DeleteExactCommand;
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
import org.apache.ignite.internal.tx.TxManager;
import org.apache.ignite.lang.IgniteInternalException;
import org.apache.ignite.raft.client.Command;
import org.apache.ignite.raft.client.ReadCommand;
import org.apache.ignite.raft.client.WriteCommand;
import org.apache.ignite.raft.client.service.CommandClosure;
import org.apache.ignite.raft.client.service.RaftGroupListener;
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
        while (iterator.hasNext()) {
            CommandClosure<ReadCommand> clo = iterator.next();

            if (clo.command() instanceof GetCommand) {
                GetCommand cmd = (GetCommand) clo.command();

                clo.result(new SingleRowResponse(storage.get(cmd.getKeyRow(), cmd.getTimestamp())));
            }
            else if (clo.command() instanceof GetAllCommand) {
                Set<BinaryRow> keyRows = ((GetAllCommand)clo.command()).getKeyRows();

                assert keyRows != null && !keyRows.isEmpty();

                // TODO asch all reads are sequeti
                clo.result(new MultiRowsResponse(storage.getAll(keyRows, null)));
            }
            else
                assert false : "Command was not found [cmd=" + clo.command() + ']';
        }
    }

    /** {@inheritDoc} */
    @Override public void onWrite(Iterator<CommandClosure<WriteCommand>> iterator) {
        while (iterator.hasNext()) {
            CommandClosure<WriteCommand> clo = iterator.next();

            if (clo.command() instanceof InsertCommand) {
                InsertCommand cmd = (InsertCommand) clo.command();

                clo.result(storage.insert(cmd.getRow(), cmd.getTimestamp()));
            }
            else if (clo.command() instanceof DeleteCommand)
                clo.result(storage.delete(((DeleteCommand)clo.command()).getKeyRow(), null));
            else if (clo.command() instanceof ReplaceCommand) {
                ReplaceCommand cmd = ((ReplaceCommand)clo.command());

                clo.result(storage.replace(cmd.getOldRow(), cmd.getRow(), null));
            }
            else if (clo.command() instanceof UpsertCommand) {
                UpsertCommand cmd = (UpsertCommand) clo.command();

                storage.upsert(cmd.getRow(), cmd.getTimestamp());

                clo.result(null);
            }
            else if (clo.command() instanceof InsertAllCommand) {
                Set<BinaryRow> rows = ((InsertAllCommand)clo.command()).getRows();

                assert rows != null && !rows.isEmpty();

                clo.result(new MultiRowsResponse(storage.insertAll(rows, null)));
            }
            else if (clo.command() instanceof UpsertAllCommand) {
                Set<BinaryRow> rows = ((UpsertAllCommand)clo.command()).getRows();

                assert rows != null && !rows.isEmpty();

                storage.upsertAll(rows, null);

                clo.result(null);
            }
            else if (clo.command() instanceof DeleteAllCommand) {
                Set<BinaryRow> rows = ((DeleteAllCommand)clo.command()).getRows();

                assert rows != null && !rows.isEmpty();

                clo.result(new MultiRowsResponse(storage.deleteAll(rows, null)));
            }
            else if (clo.command() instanceof DeleteExactCommand) {
                BinaryRow row = ((DeleteExactCommand)clo.command()).getRow();

                assert row != null;
                assert row.hasValue();

                clo.result(storage.deleteExact(row, null));
            }
            else if (clo.command() instanceof DeleteExactAllCommand) {
                Set<BinaryRow> rows = ((DeleteExactAllCommand)clo.command()).getRows();

                assert rows != null && !rows.isEmpty();

                clo.result(new MultiRowsResponse(storage.deleteAll(rows, null)));
            }
            else if (clo.command() instanceof ReplaceIfExistCommand) {
                BinaryRow row = ((ReplaceIfExistCommand)clo.command()).getRow();

                assert row != null;

                clo.result(storage.replace(row, null));
            }
            else if (clo.command() instanceof GetAndDeleteCommand) {
                BinaryRow row = ((GetAndDeleteCommand)clo.command()).getKeyRow();

                assert row != null;

                clo.result(new SingleRowResponse(storage.getAndDelete(row, null)));
            }
            else if (clo.command() instanceof GetAndReplaceCommand) {
                BinaryRow row = ((GetAndReplaceCommand)clo.command()).getRow();

                assert row != null && row.hasValue();

                clo.result(new SingleRowResponse(storage.getAndReplace(row, null)));
            }
            else if (clo.command() instanceof GetAndUpsertCommand) {
                BinaryRow row = ((GetAndUpsertCommand)clo.command()).getKeyRow();

                assert row != null && row.hasValue();

                clo.result(new SingleRowResponse(storage.getAndUpsert(row, null)));
            }
            else
                assert false : "Command was not found [cmd=" + clo.command() + ']';
        }
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
     * Wrapper provides correct byte[] comparison.
     */
    private static class KeyWrapper {
        /** Data. */
        private final byte[] data;

        /** Hash. */
        private final int hash;

        /**
         * Constructor.
         *
         * @param data Wrapped data.
         */
        KeyWrapper(byte[] data, int hash) {
            assert data != null;

            this.data = data;
            this.hash = hash;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            KeyWrapper wrapper = (KeyWrapper)o;
            return Arrays.equals(data, wrapper.data);
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return hash;
        }
    }

    /**
     * Compares two rows.
     *
     * @param row Row to compare.
     * @param row2 Row to compare.
     * @return True if these rows is equivalent, false otherwise.
     */
    private boolean equalValues(BinaryRow row, BinaryRow row2) {
        if (row == row2)
            return true;

        if (row == null || row2 == null)
            return false;

        if (row.hasValue() ^ row2.hasValue())
            return false;

        return row.valueSlice().compareTo(row2.valueSlice()) == 0;
    }

    /**
     * @return Underlying storage.
     */
    @TestOnly
    public VersionedRowStore getStorage() {
        return storage;
    }
}
