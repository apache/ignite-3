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
import static org.apache.ignite.internal.util.ByteUtils.toByteArray;
import static org.apache.ignite.internal.util.ByteUtils.toByteArrayList;

import java.io.Serializable;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.LongSupplier;
import org.apache.ignite.configuration.ConfigurationValue;
import org.apache.ignite.internal.metastorage.Entry;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.metastorage.command.GetAllCommand;
import org.apache.ignite.internal.metastorage.command.GetCommand;
import org.apache.ignite.internal.metastorage.command.GetCurrentRevisionCommand;
import org.apache.ignite.internal.metastorage.command.GetPrefixCommand;
import org.apache.ignite.internal.metastorage.command.GetRangeCommand;
import org.apache.ignite.internal.metastorage.command.PaginationCommand;
import org.apache.ignite.internal.metastorage.command.response.BatchResponse;
import org.apache.ignite.internal.metastorage.server.KeyValueStorage;
import org.apache.ignite.internal.metastorage.server.time.ClusterTimeImpl;
import org.apache.ignite.internal.raft.Command;
import org.apache.ignite.internal.raft.ReadCommand;
import org.apache.ignite.internal.raft.WriteCommand;
import org.apache.ignite.internal.raft.service.BeforeApplyHandler;
import org.apache.ignite.internal.raft.service.CommandClosure;
import org.apache.ignite.internal.raft.service.RaftGroupListener;
import org.apache.ignite.internal.util.Cursor;
import org.jetbrains.annotations.Nullable;

/**
 * Meta storage listener.
 * TODO: IGNITE-14693 Implement Meta storage exception handling logic.
 */
public class MetaStorageListener implements RaftGroupListener, BeforeApplyHandler {
    private final MetaStorageWriteHandler writeHandler;

    /** Storage. */
    private final KeyValueStorage storage;

    /**
     * Constructor.
     *
     * @param storage Storage.
     */
    public MetaStorageListener(
            KeyValueStorage storage,
            ClusterTimeImpl clusterTime,
            ConfigurationValue<Long> idempotentCacheTtl,
            CompletableFuture<LongSupplier> maxClockSkewMillisFuture
    ) {
        this.storage = storage;
        this.writeHandler = new MetaStorageWriteHandler(
                storage,
                clusterTime,
                idempotentCacheTtl,
                maxClockSkewMillisFuture
        );
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
                            ? storage.get(toByteArray(getCmd.key()))
                            : storage.get(toByteArray(getCmd.key()), getCmd.revision());

                    clo.result(e);
                } else if (command instanceof GetAllCommand) {
                    GetAllCommand getAllCmd = (GetAllCommand) command;

                    Collection<Entry> entries = getAllCmd.revision() == MetaStorageManager.LATEST_REVISION
                            ? storage.getAll(toByteArrayList(getAllCmd.keys()))
                            : storage.getAll(toByteArrayList(getAllCmd.keys()), getAllCmd.revision());

                    clo.result((Serializable) entries);
                } else if (command instanceof GetRangeCommand) {
                    var rangeCmd = (GetRangeCommand) command;

                    byte[] previousKey = rangeCmd.previousKey();

                    byte[] keyFrom = previousKey == null
                            ? rangeCmd.keyFrom()
                            : requireNonNull(storage.nextKey(previousKey));

                    clo.result(handlePaginationCommand(keyFrom, rangeCmd.keyTo(), rangeCmd));
                } else if (command instanceof GetPrefixCommand) {
                    var prefixCmd = (GetPrefixCommand) command;

                    byte[] previousKey = prefixCmd.previousKey();

                    byte[] keyFrom = previousKey == null
                            ? prefixCmd.prefix()
                            : requireNonNull(storage.nextKey(previousKey));

                    byte[] keyTo = storage.nextKey(prefixCmd.prefix());

                    clo.result(handlePaginationCommand(keyFrom, keyTo, prefixCmd));
                } else if (command instanceof GetCurrentRevisionCommand) {
                    long revision = storage.revision();

                    clo.result(revision);
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
    public boolean onBeforeApply(Command command) {
        return writeHandler.beforeApply(command);
    }

    @Override
    public void onSnapshotSave(Path path, Consumer<Throwable> doneClo) {
        storage.snapshot(path)
                .whenComplete((unused, throwable) -> doneClo.accept(throwable));
    }

    @Override
    public boolean onSnapshotLoad(Path path) {
        storage.restoreSnapshot(path);
        writeHandler.onSnapshotLoad();
        return true;
    }

    @Override
    public void onShutdown() {
    }

    /**
     * Removes obsolete entries from both volatile and persistent idempotent command cache.
     */
    @Deprecated(forRemoval = true)
    // TODO: https://issues.apache.org/jira/browse/IGNITE-19417 cache eviction should be triggered by MS GC instead.
    public void evictIdempotentCommandsCache() {
        writeHandler.evictIdempotentCommandsCache();
    }
}
