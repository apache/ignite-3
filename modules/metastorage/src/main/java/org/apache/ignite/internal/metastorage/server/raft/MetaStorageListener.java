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

import static org.apache.ignite.lang.ErrorGroups.MetaStorage.CURSOR_CLOSING_ERR;

import java.io.Serializable;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import org.apache.ignite.internal.metastorage.Entry;
import org.apache.ignite.internal.metastorage.command.GetAllCommand;
import org.apache.ignite.internal.metastorage.command.GetCommand;
import org.apache.ignite.internal.metastorage.command.cursor.CloseAllCursorsCommand;
import org.apache.ignite.internal.metastorage.command.cursor.CloseCursorCommand;
import org.apache.ignite.internal.metastorage.command.cursor.CreatePrefixCursorCommand;
import org.apache.ignite.internal.metastorage.command.cursor.CreateRangeCursorCommand;
import org.apache.ignite.internal.metastorage.command.cursor.NextBatchCommand;
import org.apache.ignite.internal.metastorage.command.response.BatchResponse;
import org.apache.ignite.internal.metastorage.exceptions.MetaStorageException;
import org.apache.ignite.internal.metastorage.server.KeyValueStorage;
import org.apache.ignite.internal.raft.ReadCommand;
import org.apache.ignite.internal.raft.WriteCommand;
import org.apache.ignite.internal.raft.service.CommandClosure;
import org.apache.ignite.internal.raft.service.RaftGroupListener;
import org.apache.ignite.internal.util.Cursor;
import org.apache.ignite.lang.IgniteUuid;

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

                clo.result(e);
            } else if (command instanceof GetAllCommand) {
                GetAllCommand getAllCmd = (GetAllCommand) command;

                Collection<Entry> entries;

                if (getAllCmd.revision() != 0) {
                    entries = storage.getAll(getAllCmd.keys(), getAllCmd.revision());
                } else {
                    entries = storage.getAll(getAllCmd.keys());
                }

                clo.result((Serializable) entries);
            } else {
                assert false : "Command was not found [cmd=" + command + ']';
            }
        }
    }

    @Override
    public void onWrite(Iterator<CommandClosure<WriteCommand>> iter) {
        while (iter.hasNext()) {
            CommandClosure<WriteCommand> clo = iter.next();

            if (writeHandler.handleWriteCommand(clo)) {
                continue;
            }

            WriteCommand command = clo.command();

            if (command instanceof CreateRangeCursorCommand) {
                var rangeCmd = (CreateRangeCursorCommand) command;

                IgniteUuid cursorId = rangeCmd.cursorId();

                Cursor<Entry> cursor = rangeCmd.revUpperBound() != -1
                        ? storage.range(rangeCmd.keyFrom(), rangeCmd.keyTo(), rangeCmd.revUpperBound(), rangeCmd.includeTombstones())
                        : storage.range(rangeCmd.keyFrom(), rangeCmd.keyTo(), rangeCmd.includeTombstones());

                var cursorMeta = new CursorMeta(cursor, rangeCmd.requesterNodeId());

                cursors.put(cursorId, cursorMeta);

                clo.result(cursorId);
            } else if (command instanceof CreatePrefixCursorCommand) {
                var prefixCmd = (CreatePrefixCursorCommand) command;

                IgniteUuid cursorId = prefixCmd.cursorId();

                Cursor<Entry> cursor = prefixCmd.revUpperBound() == -1
                        ? storage.prefix(prefixCmd.prefix(), prefixCmd.includeTombstones())
                        : storage.prefix(prefixCmd.prefix(), prefixCmd.revUpperBound(), prefixCmd.includeTombstones());

                var cursorMeta = new CursorMeta(cursor, prefixCmd.requesterNodeId());

                cursors.put(cursorId, cursorMeta);

                clo.result(cursorId);
            } else if (command instanceof NextBatchCommand) {
                var nextBatchCommand = (NextBatchCommand) command;

                CursorMeta cursorMeta = cursors.get(nextBatchCommand.cursorId());

                if (cursorMeta == null) {
                    clo.result(new NoSuchElementException("Corresponding cursor on the server side is not found."));

                    return;
                }

                try {
                    var resp = new ArrayList<Entry>(nextBatchCommand.batchSize());

                    Cursor<Entry> cursor = cursorMeta.cursor();

                    for (int i = 0; i < nextBatchCommand.batchSize() && cursor.hasNext(); i++) {
                        resp.add(cursor.next());
                    }

                    if (!cursor.hasNext()) {
                        closeCursor(nextBatchCommand.cursorId());
                    }

                    clo.result(new BatchResponse(resp, cursor.hasNext()));
                } catch (Exception e) {
                    clo.result(e);
                }
            } else if (command instanceof CloseCursorCommand) {
                var closeCursorCommand = (CloseCursorCommand) command;

                try {
                    closeCursor(closeCursorCommand.cursorId());

                    clo.result(null);
                } catch (Exception e) {
                    clo.result(new MetaStorageException(CURSOR_CLOSING_ERR, e));
                }
            } else if (command instanceof CloseAllCursorsCommand) {
                var cursorsCloseCmd = (CloseAllCursorsCommand) command;

                Iterator<CursorMeta> cursorsIter = cursors.values().iterator();

                Exception ocurredException = null;

                while (cursorsIter.hasNext()) {
                    CursorMeta cursorDesc = cursorsIter.next();

                    if (cursorDesc.requesterNodeId().equals(cursorsCloseCmd.nodeId())) {
                        try {
                            cursorDesc.cursor().close();
                        } catch (Exception e) {
                            if (ocurredException == null) {
                                ocurredException = e;
                            } else {
                                ocurredException.addSuppressed(e);
                            }
                        }

                        cursorsIter.remove();
                    }

                }

                clo.result(ocurredException == null ? null : new MetaStorageException(CURSOR_CLOSING_ERR, ocurredException));
            } else {
                assert false : "Command was not found [cmd=" + command + ']';
            }
        }
    }

    private void closeCursor(IgniteUuid cursorId) {
        CursorMeta cursorMeta = cursors.remove(cursorId);

        if (cursorMeta != null) {
            cursorMeta.cursor().close();
        }
    }

    @Override
    public void onSnapshotSave(Path path, Consumer<Throwable> doneClo) {
        storage.snapshot(path).whenComplete((unused, throwable) -> {
            doneClo.accept(throwable);
        });
    }

    @Override
    public boolean onSnapshotLoad(Path path) {
        storage.restoreSnapshot(path);
        return true;
    }

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

        /**
         * The constructor.
         *
         * @param cursor          Cursor.
         * @param requesterNodeId Id of the node that creates cursor.
         */
        CursorMeta(Cursor<Entry> cursor, String requesterNodeId) {
            this.cursor = cursor;
            this.requesterNodeId = requesterNodeId;
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
    }
}
