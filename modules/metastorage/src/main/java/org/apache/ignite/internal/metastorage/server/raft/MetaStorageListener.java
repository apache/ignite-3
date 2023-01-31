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

package org.apache.ignite.internal.metastorage.server.raft;

import static java.util.Objects.requireNonNull;
import static org.apache.ignite.lang.ErrorGroups.MetaStorage.CURSOR_CLOSING_ERR;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import org.apache.ignite.internal.metastorage.Entry;
import org.apache.ignite.internal.metastorage.command.GetAllCommand;
import org.apache.ignite.internal.metastorage.command.GetCommand;
import org.apache.ignite.internal.metastorage.command.MultipleEntryResponse;
import org.apache.ignite.internal.metastorage.command.PrefixCommand;
import org.apache.ignite.internal.metastorage.command.RangeCommand;
import org.apache.ignite.internal.metastorage.command.SingleEntryResponse;
import org.apache.ignite.internal.metastorage.command.cursor.CursorCloseCommand;
import org.apache.ignite.internal.metastorage.command.cursor.CursorHasNextCommand;
import org.apache.ignite.internal.metastorage.command.cursor.CursorNextCommand;
import org.apache.ignite.internal.metastorage.command.cursor.CursorsCloseCommand;
import org.apache.ignite.internal.metastorage.exceptions.MetaStorageException;
import org.apache.ignite.internal.metastorage.server.KeyValueStorage;
import org.apache.ignite.internal.raft.ReadCommand;
import org.apache.ignite.internal.raft.WriteCommand;
import org.apache.ignite.internal.raft.service.CommandClosure;
import org.apache.ignite.internal.raft.service.RaftGroupListener;
import org.apache.ignite.internal.util.Cursor;
import org.apache.ignite.lang.IgniteUuid;
import org.jetbrains.annotations.Nullable;

/**
 * Meta storage listener.
 * TODO: IGNITE-14693 Implement Meta storage exception handling logic.
 */
public class MetaStorageListener implements RaftGroupListener {
    private final MetaStorageWriteHandler writeHandler;

    /** Storage. */
    private final KeyValueStorage storage;

    /** Cursors map. */
    private final Map<IgniteUuid, CursorMeta> cursors;

    /**
     * Constructor.
     *
     * @param storage Storage.
     */
    public MetaStorageListener(KeyValueStorage storage) {
        this.storage = storage;
        this.writeHandler = new MetaStorageWriteHandler(storage);
        this.cursors = new ConcurrentHashMap<>();
    }

    /** {@inheritDoc} */
    @Override
    public void onRead(Iterator<CommandClosure<ReadCommand>> iter) {
        while (iter.hasNext()) {
            CommandClosure<ReadCommand> clo = iter.next();

            ReadCommand command = clo.command();

            if (command instanceof GetCommand) {
                GetCommand getCmd = (GetCommand) command;

                Entry e;

                if (getCmd.revision() != 0) {
                    e = storage.get(getCmd.key(), getCmd.revision());
                } else {
                    e = storage.get(getCmd.key());
                }

                SingleEntryResponse resp = new SingleEntryResponse(e.key(), e.value(), e.revision(), e.updateCounter());

                clo.result(resp);
            } else if (command instanceof GetAllCommand) {
                GetAllCommand getAllCmd = (GetAllCommand) command;

                Collection<Entry> entries;

                if (getAllCmd.revision() != 0) {
                    entries = storage.getAll(getAllCmd.keys(), getAllCmd.revision());
                } else {
                    entries = storage.getAll(getAllCmd.keys());
                }

                List<SingleEntryResponse> res = new ArrayList<>(entries.size());

                for (Entry e : entries) {
                    res.add(new SingleEntryResponse(e.key(), e.value(), e.revision(), e.updateCounter()));
                }

                clo.result(new MultipleEntryResponse(res));
            } else if (command instanceof CursorHasNextCommand) {
                CursorHasNextCommand cursorHasNextCmd = (CursorHasNextCommand) command;

                CursorMeta cursorDesc = cursors.get(cursorHasNextCmd.cursorId());

                clo.result(cursorDesc != null && cursorDesc.cursor().hasNext());
            } else {
                assert false : "Command was not found [cmd=" + command + ']';
            }
        }
    }

    /** {@inheritDoc} */
    @Override
    public void onWrite(Iterator<CommandClosure<WriteCommand>> iter) {
        while (iter.hasNext()) {
            CommandClosure<WriteCommand> clo = iter.next();

            if (writeHandler.handleWriteCommand(clo)) {
                continue;
            }

            WriteCommand command = clo.command();

            if (command instanceof RangeCommand) {
                RangeCommand rangeCmd = (RangeCommand) command;

                IgniteUuid cursorId = rangeCmd.cursorId();

                Cursor<Entry> cursor = rangeCmd.revUpperBound() != -1
                        ? storage.range(rangeCmd.keyFrom(), rangeCmd.keyTo(), rangeCmd.revUpperBound(), rangeCmd.includeTombstones())
                        : storage.range(rangeCmd.keyFrom(), rangeCmd.keyTo(), rangeCmd.includeTombstones());

                var cursorMeta = new CursorMeta(cursor, rangeCmd.requesterNodeId(), rangeCmd.batchSize());

                cursors.put(cursorId, cursorMeta);

                clo.result(cursorId);
            } else if (command instanceof PrefixCommand) {
                var prefixCmd = (PrefixCommand) command;

                IgniteUuid cursorId = prefixCmd.cursorId();

                Cursor<Entry> cursor = prefixCmd.revUpperBound() == -1
                        ? storage.prefix(prefixCmd.prefix(), prefixCmd.includeTombstones())
                        : storage.prefix(prefixCmd.prefix(), prefixCmd.revUpperBound(), prefixCmd.includeTombstones());

                var cursorMeta = new CursorMeta(cursor, prefixCmd.requesterNodeId(), prefixCmd.batchSize());

                cursors.put(cursorId, cursorMeta);

                clo.result(cursorId);
            } else if (command instanceof CursorNextCommand) {
                CursorNextCommand cursorNextCmd = (CursorNextCommand) command;

                CursorMeta cursorDesc = cursors.get(cursorNextCmd.cursorId());

                if (cursorDesc == null) {
                    clo.result(new NoSuchElementException("Corresponding cursor on the server side is not found."));

                    return;
                }

                try {
                    int batchSize = requireNonNull(cursorDesc.batchSize());

                    var resp = new ArrayList<SingleEntryResponse>(batchSize);

                    Cursor<Entry> cursor = cursorDesc.cursor();

                    for (int i = 0; i < batchSize && cursor.hasNext(); i++) {
                        Entry e = cursor.next();

                        resp.add(new SingleEntryResponse(e.key(), e.value(), e.revision(), e.updateCounter()));
                    }

                    clo.result(new MultipleEntryResponse(resp));
                } catch (NoSuchElementException e) {
                    clo.result(e);
                }
            } else if (command instanceof CursorCloseCommand) {
                CursorCloseCommand cursorCloseCmd = (CursorCloseCommand) command;

                CursorMeta cursorDesc = cursors.remove(cursorCloseCmd.cursorId());

                if (cursorDesc == null) {
                    clo.result(null);

                    return;
                }

                try {
                    cursorDesc.cursor().close();
                } catch (Exception e) {
                    throw new MetaStorageException(CURSOR_CLOSING_ERR, e);
                }

                clo.result(null);
            } else if (command instanceof CursorsCloseCommand) {
                CursorsCloseCommand cursorsCloseCmd = (CursorsCloseCommand) command;

                Iterator<CursorMeta> cursorsIter = cursors.values().iterator();

                while (cursorsIter.hasNext()) {
                    CursorMeta cursorDesc = cursorsIter.next();

                    if (cursorDesc.requesterNodeId().equals(cursorsCloseCmd.nodeId())) {
                        try {
                            cursorDesc.cursor().close();
                        } catch (Exception e) {
                            throw new MetaStorageException(CURSOR_CLOSING_ERR, e);
                        }

                        cursorsIter.remove();
                    }

                }

                clo.result(null);
            } else {
                assert false : "Command was not found [cmd=" + command + ']';
            }
        }
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
    }

    /**
     * Cursor meta information: origin node id and type.
     */
    private static class CursorMeta {
        /** Cursor. */
        private final Cursor<Entry> cursor;

        /** Id of the node that creates cursor. */
        private final String requesterNodeId;

        /** Maximum size of the batch that is sent in single response message. */
        private final @Nullable Integer batchSize;

        /**
         * The constructor.
         *
         * @param cursor          Cursor.
         * @param requesterNodeId Id of the node that creates cursor.
         * @param batchSize       Batch size.
         */
        CursorMeta(Cursor<Entry> cursor, String requesterNodeId, @Nullable Integer batchSize) {
            this.cursor = cursor;
            this.requesterNodeId = requesterNodeId;
            this.batchSize = batchSize;
        }

        /**
         * Returns cursor.
         */
        Cursor<Entry> cursor() {
            return cursor;
        }

        /**
         * Returns id of the node that creates cursor.
         */
        String requesterNodeId() {
            return requesterNodeId;
        }

        /**
         * Returns maximum size of the batch that is sent in single response message.
         */
        @Nullable Integer batchSize() {
            return batchSize;
        }
    }
}
