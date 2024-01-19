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

package org.apache.ignite.internal.catalog.storage;

import static java.util.concurrent.CompletableFuture.allOf;
import static java.util.concurrent.CompletableFuture.failedFuture;
import static org.apache.ignite.internal.metastorage.dsl.Conditions.notExists;
import static org.apache.ignite.internal.metastorage.dsl.Conditions.or;
import static org.apache.ignite.internal.metastorage.dsl.Conditions.value;
import static org.apache.ignite.internal.metastorage.dsl.Operations.ops;
import static org.apache.ignite.internal.metastorage.dsl.Operations.put;
import static org.apache.ignite.internal.metastorage.dsl.Statements.iif;
import static org.apache.ignite.internal.util.ByteUtils.bytesToInt;
import static org.apache.ignite.internal.util.ByteUtils.fromBytes;
import static org.apache.ignite.internal.util.ByteUtils.intToBytes;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.lang.ByteArray;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.internal.lang.NodeStoppingException;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.metastorage.Entry;
import org.apache.ignite.internal.metastorage.EntryEvent;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.metastorage.WatchEvent;
import org.apache.ignite.internal.metastorage.WatchListener;
import org.apache.ignite.internal.metastorage.dsl.Condition;
import org.apache.ignite.internal.metastorage.dsl.Iif;
import org.apache.ignite.internal.metastorage.dsl.Operation;
import org.apache.ignite.internal.metastorage.dsl.Operations;
import org.apache.ignite.internal.metastorage.dsl.StatementResult;
import org.apache.ignite.internal.metastorage.dsl.Update;
import org.apache.ignite.internal.util.ByteUtils;
import org.apache.ignite.internal.util.CompletableFutures;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.lang.ErrorGroups.Common;
import org.apache.ignite.lang.IgniteException;
import org.jetbrains.annotations.Nullable;

/**
 * Metastore-based implementation of UpdateLog.
 */
public class UpdateLogImpl implements UpdateLog {
    private static final IgniteLogger LOG = Loggers.forClass(UpdateLogImpl.class);

    private final IgniteSpinBusyLock busyLock = new IgniteSpinBusyLock();

    private final AtomicBoolean stopGuard = new AtomicBoolean();

    private final MetaStorageManager metastore;

    private volatile OnUpdateHandler onUpdateHandler;

    private volatile @Nullable UpdateListener listener;

    /**
     * Creates the object.
     *
     * @param metastore A metastore is used to store and distribute updates across the cluster.
     */
    public UpdateLogImpl(MetaStorageManager metastore) {
        this.metastore = metastore;
    }

    @Override
    public CompletableFuture<Void> start() {
        if (!busyLock.enterBusy()) {
            throw new IgniteException(Common.NODE_STOPPING_ERR, new NodeStoppingException());
        }

        try {
            OnUpdateHandler handler = onUpdateHandler;

            if (handler == null) {
                throw new IgniteInternalException(
                        Common.INTERNAL_ERR,
                        "Handler must be registered prior to component start"
                );
            }

            recoveryStateFromMetastore(handler);

            UpdateListener listener = new UpdateListener(onUpdateHandler);
            this.listener = listener;

            metastore.registerPrefixWatch(CatalogKey.updatePrefix(), listener);
        } finally {
            busyLock.leaveBusy();
        }

        return nullCompletedFuture();
    }

    @Override
    public void stop() throws Exception {
        if (!stopGuard.compareAndSet(false, true)) {
            return;
        }

        busyLock.block();

        UpdateListener listener = this.listener;
        this.listener = null;

        if (listener != null) {
            metastore.unregisterWatch(listener);
        }
    }

    @Override
    public void registerUpdateHandler(OnUpdateHandler handler) {
        onUpdateHandler = handler;
    }

    @Override
    public CompletableFuture<Boolean> append(VersionedUpdate update) {
        if (!busyLock.enterBusy()) {
            return failedFuture(new IgniteException(Common.NODE_STOPPING_ERR, new NodeStoppingException()));
        }

        try {
            int newVersion = update.version();
            int expectedVersion = newVersion - 1;

            Condition versionAsExpected = or(
                    notExists(CatalogKey.currentVersion()),
                    value(CatalogKey.currentVersion()).eq(intToBytes(expectedVersion))
            );
            Update appendUpdateEntryAndBumpVersion = ops(
                    put(CatalogKey.update(newVersion), ByteUtils.toBytes(update)),
                    put(CatalogKey.currentVersion(), intToBytes(newVersion))
            ).yield(true);

            Iif iif = iif(versionAsExpected, appendUpdateEntryAndBumpVersion, ops().yield(false));

            return metastore.invoke(iif).thenApply(StatementResult::getAsBoolean);
        } finally {
            busyLock.leaveBusy();
        }
    }

    @Override
    public CompletableFuture<Boolean> saveSnapshot(SnapshotUpdate update) {
        if (!busyLock.enterBusy()) {
            return failedFuture(new IgniteException(Common.NODE_STOPPING_ERR, new NodeStoppingException()));
        }

        // Note: below, we optimistically get local snapshot version, then prepare list of outdated updates, then atomically replace
        // old snapshot using relaxed condition (old snapshop version < new snapshot version) and cleaup the log.
        // If someone bumps snapshot version to a intermediate version in-between, then it means some outdated versions were removed.
        // So, some remove operations may become be no-op, which is ok and we no need to retry.
        try {
            int snapshotVersion = update.version();

            Entry oldSnapshotEntry = metastore.getLocally(CatalogKey.snapshotVersion(), metastore.appliedRevision());
            int oldSnapshotVersion = oldSnapshotEntry.empty() ? 0 : bytesToInt(Objects.requireNonNull(oldSnapshotEntry.value()));

            if (oldSnapshotVersion >= snapshotVersion) {
                // Nothing to do.
                return CompletableFutures.trueCompletedFuture();
            }

            Condition versionIsRecent = or(
                    notExists(CatalogKey.snapshotVersion()),
                    value(CatalogKey.snapshotVersion()).lt(intToBytes(snapshotVersion))
            );
            Update saveSnapshotAndDropOutdatedUpdates = ops(Stream.concat(
                    Stream.of(
                            put(CatalogKey.snapshotVersion(), intToBytes(snapshotVersion)),
                            put(CatalogKey.snapshot(), ByteUtils.toBytes(update))
                    ),
                    IntStream.range(oldSnapshotVersion, snapshotVersion + 1).mapToObj(ver -> Operations.remove(CatalogKey.update(ver)))
            ).toArray(Operation[]::new)).yield(true);

            Iif iif = iif(versionIsRecent, saveSnapshotAndDropOutdatedUpdates, ops().yield(false));

            return metastore.invoke(iif).thenApply(StatementResult::getAsBoolean);
        } finally {
            busyLock.leaveBusy();
        }
    }

    private void recoveryStateFromMetastore(OnUpdateHandler handler) {
        CompletableFuture<Long> recoveryFinishedFuture = metastore.recoveryFinishedFuture();

        assert recoveryFinishedFuture.isDone();

        long recoveryRevision = recoveryFinishedFuture.join();

        int ver = recoverSnapshot(handler, recoveryRevision);

        recoverUpdates(handler, recoveryRevision, ver + 1);
    }

    private void recoverUpdates(OnUpdateHandler handler, long recoveryRevision, int ver) {
        // TODO: IGNITE-19790 Read range from metastore
        while (true) {
            ByteArray key = CatalogKey.update(ver++);
            Entry entry = metastore.getLocally(key, recoveryRevision);

            if (entry.empty() || entry.tombstone()) {
                break;
            }

            VersionedUpdate update = fromBytes(Objects.requireNonNull(entry.value()));

            long revision = entry.revision();

            handler.handle(update, metastore.timestampByRevision(revision), revision);
        }
    }

    /**
     * Recover snapshot and return restored version, or {@code 0} if no snapshot found.
     */
    private int recoverSnapshot(OnUpdateHandler handler, long recoveryRevision) {
        Entry snapshotEntry = metastore.getLocally(CatalogKey.snapshot(), recoveryRevision);
        if (snapshotEntry.empty()) {
            return 0;
        }

        SnapshotUpdate snapshot = fromBytes(Objects.requireNonNull(snapshotEntry.value()));

        long revision = snapshotEntry.revision();

        //TODO: Fix timestamp.
        handler.handle(snapshot, HybridTimestamp.nullableHybridTimestamp(0L), revision);

        return snapshot.version();
    }

    private static class CatalogKey {
        private CatalogKey() {
            throw new AssertionError();
        }

        static ByteArray currentVersion() {
            return ByteArray.fromString("catalog.version");
        }

        static ByteArray update(int version) {
            return ByteArray.fromString("catalog.update." + version);
        }

        static ByteArray updatePrefix() {
            return ByteArray.fromString("catalog.update.");
        }

        public static ByteArray snapshot() {
            return ByteArray.fromString("catalog.snapshot");
        }

        public static ByteArray snapshotVersion() {
            return ByteArray.fromString("catalog.snapshot.version");
        }
    }

    private static class UpdateListener implements WatchListener {
        private final OnUpdateHandler onUpdateHandler;

        private UpdateListener(OnUpdateHandler onUpdateHandler) {
            this.onUpdateHandler = onUpdateHandler;
        }

        @Override
        public CompletableFuture<Void> onUpdate(WatchEvent event) {
            Collection<EntryEvent> entryEvents = event.entryEvents();

            var handleFutures = new ArrayList<CompletableFuture<Void>>(entryEvents.size());

            for (EntryEvent eventEntry : entryEvents) {
                if (eventEntry.newEntry().tombstone()) {
                    continue;
                }

                byte[] payload = eventEntry.newEntry().value();

                assert payload != null : eventEntry;

                VersionedUpdate update = fromBytes(payload);

                handleFutures.add(onUpdateHandler.handle(update, event.timestamp(), event.revision()));
            }

            return allOf(handleFutures.toArray(CompletableFuture[]::new));
        }

        @Override
        public void onError(Throwable e) {
            LOG.warn("Unable to process catalog event", e);
        }
    }
}
