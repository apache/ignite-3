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

import java.util.Arrays;
import java.util.Iterator;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
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
import org.apache.ignite.raft.client.ReadCommand;
import org.apache.ignite.raft.client.WriteCommand;
import org.apache.ignite.raft.client.service.CommandClosure;
import org.apache.ignite.raft.client.service.RaftGroupCommandListener;
import org.jetbrains.annotations.NotNull;

/**
 * Partition command handler.
 */
public class PartitionCommandListener implements RaftGroupCommandListener {
    /** Storage. */
    private ConcurrentHashMap<KeyWrapper, BinaryRow> storage = new ConcurrentHashMap<>();

    /** {@inheritDoc} */
    @Override public void onRead(Iterator<CommandClosure<ReadCommand>> iterator) {
        while (iterator.hasNext()) {
            CommandClosure<ReadCommand> clo = iterator.next();

            if (clo.command() instanceof GetCommand) {
                clo.success(new SingleRowResponse(storage.get(
                    extractAndWrapKey(((GetCommand)clo.command()).getKeyRow())
                )));
            }
            else if (clo.command() instanceof GetAllCommand) {
                Set<BinaryRow> keyRows = ((GetAllCommand)clo.command()).getKeyRows();

                assert keyRows != null && !keyRows.isEmpty();

                final Set<BinaryRow> res = keyRows.stream()
                    .map(this::extractAndWrapKey)
                    .map(storage::get)
                    .filter(Objects::nonNull)
                    .filter(BinaryRow::hasValue)
                    .collect(Collectors.toSet());

                clo.success(new MultiRowsResponse(res));
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
                BinaryRow previous = storage.putIfAbsent(
                    extractAndWrapKey(((InsertCommand)clo.command()).getRow()),
                    ((InsertCommand)clo.command()).getRow()
                );

                clo.success(previous == null);
            }
            else if (clo.command() instanceof DeleteCommand) {
                BinaryRow deleted = storage.remove(
                    extractAndWrapKey(((DeleteCommand)clo.command()).getKeyRow())
                );

                clo.success(deleted != null);
            }
            else if (clo.command() instanceof ReplaceCommand) {
                ReplaceCommand cmd = ((ReplaceCommand)clo.command());

                BinaryRow expected = cmd.getOldRow();

                KeyWrapper key = extractAndWrapKey(expected);

                BinaryRow current = storage.get(key);

                if ((current == null && !expected.hasValue()) ||
                    equalValues(current, expected)) {
                    storage.put(key, cmd.getRow());

                    clo.success(true);
                }
                else
                    clo.success(false);
            }
            else if (clo.command() instanceof UpsertCommand) {
                storage.put(
                    extractAndWrapKey(((UpsertCommand)clo.command()).getRow()),
                    ((UpsertCommand)clo.command()).getRow()
                );

                clo.success(null);
            }
            else if (clo.command() instanceof InsertAllCommand) {
                Set<BinaryRow> rows = ((InsertAllCommand)clo.command()).getRows();

                assert rows != null && !rows.isEmpty();

                final Set<BinaryRow> res = rows.stream()
                    .map(k -> storage.putIfAbsent(extractAndWrapKey(k), k) == null ? null : k)
                    .filter(Objects::nonNull)
                    .filter(BinaryRow::hasValue)
                    .collect(Collectors.toSet());

                clo.success(new MultiRowsResponse(res));
            }
            else if (clo.command() instanceof UpsertAllCommand) {
                Set<BinaryRow> rows = ((UpsertAllCommand)clo.command()).getRows();

                assert rows != null && !rows.isEmpty();

                rows.stream()
                    .forEach(k -> storage.put(extractAndWrapKey(k), k));

                clo.success(null);
            }
            else if (clo.command() instanceof DeleteAllCommand) {
                Set<BinaryRow> rows = ((DeleteAllCommand)clo.command()).getRows();

                assert rows != null && !rows.isEmpty();

                final Set<BinaryRow> res = rows.stream()
                    .map(k -> {
                        if (k.hasValue())
                            return null;
                        else {
                            BinaryRow r = storage.remove(extractAndWrapKey(k));

                            if (r == null)
                                return null;
                            else
                                return r;
                        }
                    })
                    .filter(Objects::nonNull)
                    .filter(BinaryRow::hasValue)
                    .collect(Collectors.toSet());

                clo.success(new MultiRowsResponse(res));
            }
            else if (clo.command() instanceof DeleteExactCommand) {
                BinaryRow row = ((DeleteExactCommand)clo.command()).getRow();

                assert row != null;
                assert row.hasValue();

                final KeyWrapper key = extractAndWrapKey(row);
                final BinaryRow old = storage.get(key);

                if (old == null || !old.hasValue())
                    clo.success(false);
                else
                    clo.success(equalValues(row, old) && storage.remove(key) != null);
            }
            else if (clo.command() instanceof DeleteExactAllCommand) {
                Set<BinaryRow> rows = ((DeleteExactAllCommand)clo.command()).getRows();

                assert rows != null && !rows.isEmpty();

                final Set<BinaryRow> res = rows.stream()
                    .map(k -> {
                        final KeyWrapper key = extractAndWrapKey(k);
                        final BinaryRow old = storage.get(key);

                        if (old == null || !old.hasValue() || !equalValues(k, old))
                            return null;

                        return storage.remove(key);
                    })
                    .filter(Objects::nonNull)
                    .filter(BinaryRow::hasValue)
                    .collect(Collectors.toSet());

                clo.success(new MultiRowsResponse(res));
            }
            else if (clo.command() instanceof ReplaceIfExistCommand) {
                BinaryRow row = ((ReplaceIfExistCommand)clo.command()).getRow();

                assert row != null;

                final KeyWrapper key = extractAndWrapKey(row);
                final BinaryRow oldRow = storage.get(key);

                if (oldRow == null || !oldRow.hasValue())
                    clo.success(false);
                else
                    clo.success(storage.put(key, row) == oldRow);
            }
            else if (clo.command() instanceof GetAndDeleteCommand) {
                BinaryRow row = ((GetAndDeleteCommand)clo.command()).getKeyRow();

                assert row != null;

                BinaryRow oldRow = storage.remove(extractAndWrapKey(row));

                if (oldRow == null || !oldRow.hasValue())
                    clo.success(new SingleRowResponse(null));
                else
                    clo.success(new SingleRowResponse(oldRow));
            }
            else if (clo.command() instanceof GetAndReplaceCommand) {
                BinaryRow row = ((GetAndReplaceCommand)clo.command()).getKeyRow();

                assert row != null && row.hasValue();

                BinaryRow oldRow = storage.get(extractAndWrapKey(row));

                storage.computeIfPresent(extractAndWrapKey(row), (key, val) -> row);

                if (oldRow == null || !oldRow.hasValue())
                    clo.success(new SingleRowResponse(null));
                else
                    clo.success(new SingleRowResponse(oldRow));
            }
            else if (clo.command() instanceof GetAndUpsertCommand) {
                BinaryRow row = ((GetAndReplaceCommand)clo.command()).getKeyRow();

                assert row != null && row.hasValue();

                BinaryRow oldRow = storage.put(extractAndWrapKey(row), row);

                if (oldRow == null || !oldRow.hasValue())
                    clo.success(new SingleRowResponse(null));
                else
                    clo.success(new SingleRowResponse(oldRow));
            }
            else
                assert false : "Command was not found [cmd=" + clo.command() + ']';
        }
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
     * @param row Row.
     * @return Extracted key.
     */
    @NotNull private boolean equalValues(@NotNull BinaryRow row, @NotNull BinaryRow row2) {
        if (row.hasValue() ^ row2.hasValue())
            return false;

        return row.valueSlice().compareTo(row2.valueSlice()) == 0;
    }

    /**
     * @param row Row.
     * @return Extracted key.
     */
    @NotNull private KeyWrapper extractAndWrapKey(@NotNull BinaryRow row) {
        final byte[] bytes = new byte[row.keySlice().capacity()];
        row.keySlice().get(bytes);

        return new KeyWrapper(bytes, row.hash());
    }
}
