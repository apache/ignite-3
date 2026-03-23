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
import java.util.Iterator;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.IntConsumer;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.metastorage.Entry;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.metastorage.Revisions;
import org.apache.ignite.internal.metastorage.command.GetAllCommand;
import org.apache.ignite.internal.metastorage.command.GetChecksumCommand;
import org.apache.ignite.internal.metastorage.command.GetCommand;
import org.apache.ignite.internal.metastorage.command.GetCurrentRevisionsCommand;
import org.apache.ignite.internal.metastorage.command.GetPrefixCommand;
import org.apache.ignite.internal.metastorage.command.GetRangeCommand;
import org.apache.ignite.internal.metastorage.command.PaginationCommand;
import org.apache.ignite.internal.metastorage.command.response.BatchResponse;
import org.apache.ignite.internal.metastorage.command.response.ChecksumInfo;
import org.apache.ignite.internal.metastorage.command.response.RevisionsInfo;
import org.apache.ignite.internal.metastorage.server.ChecksumAndRevisions;
import org.apache.ignite.internal.metastorage.server.KeyValueStorage;
import org.apache.ignite.internal.metastorage.server.time.ClusterTimeImpl;
import org.apache.ignite.internal.raft.Command;
import org.apache.ignite.internal.raft.RaftGroupConfiguration;
import org.apache.ignite.internal.raft.RaftGroupConfigurationConverter;
import org.apache.ignite.internal.raft.ReadCommand;
import org.apache.ignite.internal.raft.WriteCommand;
import org.apache.ignite.internal.raft.service.BeforeApplyHandler;
import org.apache.ignite.internal.raft.service.CommandClosure;
import org.apache.ignite.internal.raft.service.RaftGroupListener;
import org.apache.ignite.internal.util.Cursor;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

/**
 * Meta storage listener.
 * TODO: IGNITE-14693 Implement Meta storage exception handling logic.
 */
public class MetaStorageListener implements RaftGroupListener, BeforeApplyHandler {
    private final IgniteSpinBusyLock busyLock = new IgniteSpinBusyLock();

    private final MetaStorageWriteHandler writeHandler;

    private final KeyValueStorage storage;

    private final Consumer<RaftGroupConfiguration> onConfigurationCommitted;

    private final RaftGroupConfigurationConverter configurationConverter = new RaftGroupConfigurationConverter();

    /** Constructor. */
    @TestOnly
    public MetaStorageListener(KeyValueStorage storage, HybridClock clock, ClusterTimeImpl clusterTime) {
        this(storage, clock, clusterTime, newConfig -> {}, a -> {});
    }

    /** Constructor. */
    public MetaStorageListener(
            KeyValueStorage storage,
            HybridClock clock,
            ClusterTimeImpl clusterTime,
            Consumer<RaftGroupConfiguration> onConfigurationCommitted,
            IntConsumer idempotentCacheSizeListener
    ) {
        this.storage = storage;
        this.onConfigurationCommitted = onConfigurationCommitted;

        writeHandler = new MetaStorageWriteHandler(storage, clock, clusterTime, idempotentCacheSizeListener);
    }

    @Override
    public void onRead(Iterator<CommandClosure<ReadCommand>> iter) {
        if (!busyLock.enterBusy()) {
            iter.forEachRemaining(clo -> clo.result(new ShutdownException()));
        }

        try {
            onReadBusy(iter);
        } finally {
            busyLock.leaveBusy();
        }
    }

    private void onReadBusy(Iterator<CommandClosure<ReadCommand>> iter) {
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

                    List<Entry> entries = getAllCmd.revision() == MetaStorageManager.LATEST_REVISION
                            ? storage.getAll(toByteArrayList(getAllCmd.keys()))
                            : storage.getAll(toByteArrayList(getAllCmd.keys()), getAllCmd.revision());

                    clo.result((Serializable) entries);
                } else if (command instanceof GetRangeCommand) {
                    var rangeCmd = (GetRangeCommand) command;

                    byte[] previousKey = rangeCmd.previousKey();

                    byte[] keyFrom = previousKey == null
                            ? toByteArray(rangeCmd.keyFrom())
                            : requireNonNull(storage.nextKey(previousKey));

                    byte @Nullable [] keyTo = rangeCmd.keyTo() == null ? null : toByteArray(rangeCmd.keyTo());

                    clo.result(handlePaginationCommand(keyFrom, keyTo, rangeCmd));
                } else if (command instanceof GetPrefixCommand) {
                    var prefixCmd = (GetPrefixCommand) command;

                    byte[] previousKey = prefixCmd.previousKey();

                    byte[] prefix = toByteArray(prefixCmd.prefix());

                    byte[] keyFrom = previousKey == null ? prefix : requireNonNull(storage.nextKey(previousKey));

                    byte[] keyTo = storage.nextKey(prefix);

                    clo.result(handlePaginationCommand(keyFrom, keyTo, prefixCmd));
                } else if (command instanceof GetCurrentRevisionsCommand) {
                    Revisions currentRevisions = storage.revisions();

                    clo.result(RevisionsInfo.of(currentRevisions));
                } else if (command instanceof GetChecksumCommand) {
                    ChecksumAndRevisions checksumInfo = storage.checksumAndRevisions(((GetChecksumCommand) command).revision());

                    clo.result(new ChecksumInfo(
                            checksumInfo.checksum(),
                            checksumInfo.minChecksummedRevision(),
                            checksumInfo.maxChecksummedRevision()
                    ));
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
    public Command onBeforeApply(Command command) {
        return writeHandler.beforeApply(command);
    }

    @Override
    public void onConfigurationCommitted(
            RaftGroupConfiguration config,
            long lastAppliedIndex,
            long lastAppliedTerm
    ) {
        storage.saveConfiguration(
                configurationConverter.toBytes(config),
                lastAppliedIndex,
                lastAppliedTerm
        );

        onConfigurationCommitted.accept(config);
    }

    @Override
    public void onSnapshotSave(Path path, Consumer<Throwable> doneClo) {
        storage.snapshot(path)
                .whenComplete((unused, throwable) -> doneClo.accept(throwable));
    }

    @Override
    public boolean onSnapshotLoad(Path path) {
        // Startup snapshot should always be ignored, because we always restore from rocksdb folder instead of a separate set of SST files.
        if (!path.toString().isEmpty()) { // See "org.apache.ignite.internal.metastorage.impl.raft.StartupMetaStorageSnapshotReader.getPath"
            storage.restoreSnapshot(path);
        }

        // Restore internal state.
        writeHandler.onSnapshotLoad();
        return true;
    }

    @Override
    public void onShutdown() {
        busyLock.block();
    }
}
