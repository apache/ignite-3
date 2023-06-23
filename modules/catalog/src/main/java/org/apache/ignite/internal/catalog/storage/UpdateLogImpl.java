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

import static java.util.concurrent.CompletableFuture.failedFuture;
import static org.apache.ignite.internal.metastorage.dsl.Conditions.notExists;
import static org.apache.ignite.internal.metastorage.dsl.Conditions.or;
import static org.apache.ignite.internal.metastorage.dsl.Conditions.value;
import static org.apache.ignite.internal.metastorage.dsl.Operations.ops;
import static org.apache.ignite.internal.metastorage.dsl.Operations.put;
import static org.apache.ignite.internal.metastorage.dsl.Statements.iif;
import static org.apache.ignite.internal.util.ByteUtils.fromBytes;
import static org.apache.ignite.internal.util.ByteUtils.intToBytes;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.internal.metastorage.EntryEvent;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.metastorage.WatchEvent;
import org.apache.ignite.internal.metastorage.WatchListener;
import org.apache.ignite.internal.metastorage.dsl.Condition;
import org.apache.ignite.internal.metastorage.dsl.Iif;
import org.apache.ignite.internal.metastorage.dsl.StatementResult;
import org.apache.ignite.internal.metastorage.dsl.Update;
import org.apache.ignite.internal.util.ByteUtils;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.internal.vault.VaultEntry;
import org.apache.ignite.internal.vault.VaultManager;
import org.apache.ignite.lang.ByteArray;
import org.apache.ignite.lang.ErrorGroups.Common;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.lang.IgniteInternalException;
import org.apache.ignite.lang.NodeStoppingException;
import org.jetbrains.annotations.Nullable;

/**
 * Metastore-based implementation of UpdateLog.
 */
public class UpdateLogImpl implements UpdateLog {
    private final IgniteSpinBusyLock busyLock = new IgniteSpinBusyLock();
    private final AtomicBoolean stopGuard = new AtomicBoolean();

    private final MetaStorageManager metastore;
    private final VaultManager vault;

    private volatile OnUpdateHandler onUpdateHandler;
    private volatile @Nullable UpdateListener listener = null;

    /**
     * Creates the object.
     *
     * @param metastore A metastore is used to store and distribute updates across the cluster.
     * @param vault A vault is used to recover state and replay updates on start.
     */
    public UpdateLogImpl(
            MetaStorageManager metastore,
            VaultManager vault
    ) {
        this.vault = vault;
        this.metastore = metastore;
    }

    /** {@inheritDoc} */
    @Override
    public void start() {
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

            restoreStateFromVault(handler);

            UpdateListener listener = new UpdateListener(handler);
            this.listener = listener;

            metastore.registerPrefixWatch(CatalogKey.updatePrefix(), listener);

        } finally {
            busyLock.leaveBusy();
        }
    }

    /** {@inheritDoc} */
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

    /** {@inheritDoc} */
    @Override
    public void registerUpdateHandler(OnUpdateHandler handler) {
        onUpdateHandler = handler;
    }

    /** {@inheritDoc} */
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

    private void restoreStateFromVault(OnUpdateHandler handler) {
        int ver = 1;

        while (true) {
            VaultEntry entry = vault.get(CatalogKey.update(ver++)).join();

            if (entry == null) {
                break;
            }

            VersionedUpdate update = fromBytes(entry.value());

            handler.handle(update);
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
    }

    private static class UpdateListener implements WatchListener {
        private final OnUpdateHandler onUpdateHandler;

        UpdateListener(OnUpdateHandler onUpdateHandler) {
            this.onUpdateHandler = onUpdateHandler;
        }

        /** {@inheritDoc} */
        @Override
        public CompletableFuture<Void> onUpdate(WatchEvent event) {
            for (EntryEvent eventEntry : event.entryEvents()) {
                assert eventEntry.newEntry() != null;
                assert !eventEntry.newEntry().empty();

                byte[] payload = eventEntry.newEntry().value();

                assert payload != null;

                VersionedUpdate update = fromBytes(payload);

                onUpdateHandler.handle(update);
            }

            return CompletableFuture.completedFuture(null);
        }

        /** {@inheritDoc} */
        @Override
        public void onError(Throwable e) {
            assert false;
        }
    }
}
