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

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.function.Consumer;
import java.util.logging.Logger;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.metastorage.Entry;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.metastorage.command.GetAllCommand;
import org.apache.ignite.internal.metastorage.command.GetCommand;
import org.apache.ignite.internal.metastorage.command.GetPrefixCommand;
import org.apache.ignite.internal.metastorage.command.GetRangeCommand;
import org.apache.ignite.internal.metastorage.command.PaginationCommand;
import org.apache.ignite.internal.metastorage.command.response.BatchResponse;
import org.apache.ignite.internal.metastorage.server.KeyValueStorage;
import org.apache.ignite.internal.metastorage.server.time.ClusterTimeImpl;
import org.apache.ignite.internal.raft.Command;
import org.apache.ignite.internal.raft.ReadCommand;
import org.apache.ignite.internal.raft.WriteCommand;
import org.apache.ignite.internal.raft.service.CommandClosure;
import org.apache.ignite.internal.raft.service.RaftGroupListener;
import org.apache.ignite.internal.util.Cursor;
import org.jetbrains.annotations.Nullable;

/**
 * Meta storage listener.
 * TODO: IGNITE-14693 Implement Meta storage exception handling logic.
 */
public class MetaStorageListener implements RaftGroupListener {
    private final MetaStorageWriteHandler writeHandler;

    /** Storage. */
    private final KeyValueStorage storage;

    private IgniteLogger log = Loggers.forClass(MetaStorageListener.class);

    /**
     * Constructor.
     *
     * @param storage Storage.
     */
    public MetaStorageListener(KeyValueStorage storage, ClusterTimeImpl clusterTime) {
        this.storage = storage;
        this.writeHandler = new MetaStorageWriteHandler(storage, clusterTime);
    }

    @Override
    public void onRead(Iterator<CommandClosure<ReadCommand>> iter) {
        while (iter.hasNext()) {
            CommandClosure<ReadCommand> clo = iter.next();

            ReadCommand command = clo.command();

            try {
                if (command instanceof GetCommand) {
                    GetCommand getCmd = (GetCommand) command;

                    Entry e = getCmd.revision() == MetaStorageManager.LATEST_REVISION
                            ? storage.get(getCmd.key())
                            : storage.get(getCmd.key(), getCmd.revision());

                    clo.result(e);
                } else if (command instanceof GetAllCommand) {
                    GetAllCommand getAllCmd = (GetAllCommand) command;

                    Collection<Entry> entries = getAllCmd.revision() == MetaStorageManager.LATEST_REVISION
                            ? storage.getAll(getAllCmd.keys())
                            : storage.getAll(getAllCmd.keys(), getAllCmd.revision());

                    clo.result((Serializable) entries);
                } else if (command instanceof GetRangeCommand) {
                    long start = System.currentTimeMillis();
                    var rangeCmd = (GetRangeCommand) command;

                    byte[] previousKey = rangeCmd.previousKey();

                    byte[] keyFrom = previousKey == null
                            ? rangeCmd.keyFrom()
                            : requireNonNull(storage.nextKey(previousKey));

                    BatchResponse b = handlePaginationCommand(keyFrom, rangeCmd.keyTo(), rangeCmd);
                    clo.result(b);
                    long duration = System.currentTimeMillis() - start;
                    long bytesSize = b.entries().stream().mapToInt(e -> e.key().length + (e.value() == null ? 0 : e.value().length)).sum();
                    log.info("qqq get range command, keyFrom: " + (new String(keyFrom, StandardCharsets.UTF_8) +
                            ", time: " + duration + ", batch size: " + b.entries().size() + ", size in bytes: " + bytesSize));
                } else if (command instanceof GetPrefixCommand) {
                    var prefixCmd = (GetPrefixCommand) command;

                    byte[] previousKey = prefixCmd.previousKey();

                    byte[] keyFrom = previousKey == null
                            ? prefixCmd.prefix()
                            : requireNonNull(storage.nextKey(previousKey));

                    byte[] keyTo = storage.nextKey(prefixCmd.prefix());

                    clo.result(handlePaginationCommand(keyFrom, keyTo, prefixCmd));
                } else {
                    assert false : "Command was not found [cmd=" + command + ']';
                }
            } catch (Exception e) {
                clo.result(e);
            }
        }
    }

    private BatchResponse handlePaginationCommand(byte[] keyFrom, byte @Nullable [] keyTo, PaginationCommand command) {
        assert command.batchSize() > 0 : command.batchSize();

        Cursor<Entry> cursor = command.revUpperBound() == MetaStorageManager.LATEST_REVISION
                ? storage.range(keyFrom, keyTo)
                : storage.range(keyFrom, keyTo, command.revUpperBound());

        try (cursor) {
            var entries = new ArrayList<Entry>();

            for (Entry entry : cursor) {
                if (command.includeTombstones() || !entry.tombstone()) {
                    entries.add(entry);

                    if (entries.size() == command.batchSize()) {
                        break;
                    }
                }
            }

            return new BatchResponse(entries, cursor.hasNext());
        }
    }

    @Override
    public void onWrite(Iterator<CommandClosure<WriteCommand>> iter) {
        iter.forEachRemaining(writeHandler::handleWriteCommand);
    }

    @Override
    public void onBeforeApply(Command command) {
        writeHandler.beforeApply(command);
    }

    @Override
    public void onSnapshotSave(Path path, Consumer<Throwable> doneClo) {
        storage.snapshot(path)
                .whenComplete((unused, throwable) -> doneClo.accept(throwable));
    }

    @Override
    public boolean onSnapshotLoad(Path path) {
        storage.restoreSnapshot(path);
        return true;
    }

    @Override
    public void onShutdown() {
    }
}
