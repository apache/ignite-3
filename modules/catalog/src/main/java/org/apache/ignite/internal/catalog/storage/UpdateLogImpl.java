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
import static org.apache.ignite.internal.util.ByteUtils.bytesToIntKeepingOrder;
import static org.apache.ignite.internal.util.ByteUtils.intToBytesKeepingOrder;
import static org.apache.ignite.internal.util.CompletableFutures.falseCompletedFuture;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.ignite.internal.catalog.storage.serialization.CatalogMarshallerException;
import org.apache.ignite.internal.catalog.storage.serialization.UpdateLogMarshaller;
import org.apache.ignite.internal.catalog.storage.serialization.UpdateLogMarshallerImpl;
import org.apache.ignite.internal.lang.ByteArray;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.internal.lang.NodeStoppingException;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.manager.ComponentContext;
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
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.lang.ErrorGroups.Common;
import org.apache.ignite.lang.IgniteException;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

/**
 * Metastore-based implementation of UpdateLog.
 */
public class UpdateLogImpl implements UpdateLog {
    private static final IgniteLogger LOG = Loggers.forClass(UpdateLogImpl.class);

    private final IgniteSpinBusyLock busyLock = new IgniteSpinBusyLock();

    private final AtomicBoolean stopGuard = new AtomicBoolean();

    private final MetaStorageManager metastore;

    private final UpdateLogMarshaller marshaller;

    private volatile OnUpdateHandler onUpdateHandler;

    private volatile @Nullable UpdateListener listener;

    /**
     * Creates the object.
     *
     * @param metastore A metastore is used to store and distribute updates across the cluster.
     */
    public UpdateLogImpl(MetaStorageManager metastore) {
        this.metastore = metastore;
        this.marshaller = new UpdateLogMarshallerImpl();
    }

    @TestOnly
    public UpdateLogImpl(MetaStorageManager metastore, UpdateLogMarshaller marshaller) {
        this.metastore = metastore;
        this.marshaller = marshaller;
    }

    @Override
    public CompletableFuture<Void> startAsync(ComponentContext componentContext) {
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

            UpdateListener listener = new UpdateListener(handler, marshaller);
            this.listener = listener;

            metastore.registerPrefixWatch(CatalogKey.updatePrefix(), listener);
        } finally {
            busyLock.leaveBusy();
        }

        return nullCompletedFuture();
    }

    @Override
    public CompletableFuture<Void> stopAsync(ComponentContext componentContext) {
        if (!stopGuard.compareAndSet(false, true)) {
            return nullCompletedFuture();
        }

        busyLock.block();

        UpdateListener listener = this.listener;
        this.listener = null;

        if (listener != null) {
            metastore.unregisterWatch(listener);
        }

        return nullCompletedFuture();
    }

    @Override
    public synchronized void registerUpdateHandler(OnUpdateHandler handler) {
        if (onUpdateHandler != null) {
            throw new IllegalStateException("onUpdateHandler handler already registered");
        }

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
                    value(CatalogKey.currentVersion()).eq(intToBytesKeepingOrder(expectedVersion))
            );
            Update appendUpdateEntryAndBumpVersion = ops(
                    put(CatalogKey.update(newVersion), marshaller.marshall(update)),
                    put(CatalogKey.currentVersion(), intToBytesKeepingOrder(newVersion))
            ).yield(true);

            Iif iif = iif(versionAsExpected, appendUpdateEntryAndBumpVersion, ops().yield(false));

            return metastore.invoke(iif).thenApply(StatementResult::getAsBoolean);
        } catch (CatalogMarshallerException ex) {
            LOG.warn("Failed to append update log.", ex);

            // TODO: IGNITE-14611 Pass exception to an error handler because catalog got into inconsistent state.
            return failedFuture(ex);
        } finally {
            busyLock.leaveBusy();
        }
    }

    @Override
    public CompletableFuture<Boolean> saveSnapshot(SnapshotEntry update) {
        if (!busyLock.enterBusy()) {
            return failedFuture(new IgniteException(Common.NODE_STOPPING_ERR, new NodeStoppingException()));
        }

        // Note: below, we optimistically get local snapshot version, then prepare list of outdated updates, then atomically replace
        // old snapshot using relaxed condition (old snapshot version < new snapshot version) and cleanup the log.
        // If someone bumps snapshot version to a intermediate version in-between, then it means some outdated versions were removed.
        // So, some remove operations may become be no-op, which is ok and we no need to retry.
        try {
            int snapshotVersion = update.version();

            Entry oldSnapshotEntry = metastore.getLocally(CatalogKey.snapshotVersion(), metastore.appliedRevision());
            int oldSnapshotVersion = oldSnapshotEntry.empty()
                    ? 1
                    : bytesToIntKeepingOrder(Objects.requireNonNull(oldSnapshotEntry.value()));

            if (oldSnapshotVersion >= snapshotVersion) {
                // Nothing to do.
                return falseCompletedFuture();
            }

            // We need to keep order for the comparison to work correctly.
            byte[] snapshotVersionValue = intToBytesKeepingOrder(snapshotVersion);

            Condition versionIsRecent = or(
                    notExists(CatalogKey.snapshotVersion()),
                    value(CatalogKey.snapshotVersion()).lt(snapshotVersionValue)
            );
            Update saveSnapshotAndDropOutdatedUpdates = ops(Stream.concat(
                    Stream.of(
                            put(CatalogKey.snapshotVersion(), snapshotVersionValue),
                            put(CatalogKey.update(snapshotVersion), marshaller.marshall(update))
                    ),
                    IntStream.range(oldSnapshotVersion, snapshotVersion).mapToObj(ver -> Operations.remove(CatalogKey.update(ver)))
            ).toArray(Operation[]::new)).yield(true);

            Iif iif = iif(versionIsRecent, saveSnapshotAndDropOutdatedUpdates, ops().yield(false));

            LOG.info("Snapshot version is updated. [Old version: {}, new version: {}]", oldSnapshotVersion, snapshotVersion);

            return metastore.invoke(iif).thenApply(StatementResult::getAsBoolean);
        } catch (CatalogMarshallerException ex) {
            LOG.warn("Failed to append update log.", ex);

            // TODO: IGNITE-14611 Pass exception to an error handler because catalog got into inconsistent state.
            return failedFuture(ex);
        } finally {
            busyLock.leaveBusy();
        }
    }

    private void recoveryStateFromMetastore(OnUpdateHandler handler) {
        CompletableFuture<Long> recoveryFinishedFuture = metastore.recoveryFinishedFuture();

        assert recoveryFinishedFuture.isDone();

        long recoveryRevision = recoveryFinishedFuture.join();

        Entry earliestVersion = metastore.getLocally(CatalogKey.snapshotVersion(), recoveryRevision);

        int ver = earliestVersion.empty() ? 1 : bytesToIntKeepingOrder(Objects.requireNonNull(earliestVersion.value()));

        recoverUpdates(handler, recoveryRevision, ver);
    }

    private void recoverUpdates(OnUpdateHandler handler, long recoveryRevision, int ver) {
        // TODO: IGNITE-19790 Read range from metastore
        while (true) {
            ByteArray key = CatalogKey.update(ver++);
            Entry entry = metastore.getLocally(key, recoveryRevision);

            if (entry.empty() || entry.tombstone()) {
                break;
            }

            UpdateLogEvent update = marshaller.unmarshall(Objects.requireNonNull(entry.value()));

            long revision = entry.revision();

            handler.handle(update, metastore.timestampByRevision(revision), revision);
        }
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

        static ByteArray snapshotVersion() {
            return ByteArray.fromString("catalog.snapshot.version");
        }
    }

    private static class UpdateListener implements WatchListener {
        private final OnUpdateHandler onUpdateHandler;
        private final UpdateLogMarshaller marshaller;

        private UpdateListener(OnUpdateHandler onUpdateHandler, UpdateLogMarshaller marshaller) {
            this.onUpdateHandler = onUpdateHandler;
            this.marshaller = marshaller;
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

                try {
                    UpdateLogEvent update = marshaller.unmarshall(payload);

                    handleFutures.add(onUpdateHandler.handle(update, event.timestamp(), event.revision()));
                } catch (CatalogMarshallerException ex) {
                    LOG.warn("Failed to deserialize update.", ex);

                    // TODO: IGNITE-14611 Pass exception to an error handler because catalog got into inconsistent state.
                    return failedFuture(ex);
                }
            }

            return allOf(handleFutures.toArray(CompletableFuture[]::new));
        }

        @Override
        public void onError(Throwable e) {
            LOG.warn("Unable to process catalog event", e);
        }
    }
}
