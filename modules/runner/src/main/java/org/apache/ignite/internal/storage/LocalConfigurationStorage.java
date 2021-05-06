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

package org.apache.ignite.internal.storage;

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.configuration.storage.ConfigurationStorage;
import org.apache.ignite.configuration.storage.ConfigurationStorageListener;
import org.apache.ignite.configuration.storage.ConfigurationType;
import org.apache.ignite.configuration.storage.Data;
import org.apache.ignite.configuration.storage.StorageException;
import org.apache.ignite.internal.util.ByteUtils;
import org.apache.ignite.internal.vault.VaultManager;
import org.apache.ignite.internal.vault.common.Entry;
import org.apache.ignite.internal.vault.common.VaultListener;
import org.apache.ignite.internal.vault.common.VaultWatch;
import org.apache.ignite.lang.ByteArray;
import org.apache.ignite.lang.IgniteLogger;
import org.jetbrains.annotations.NotNull;

/**
 * Local configuration storage.
 */
public class LocalConfigurationStorage implements ConfigurationStorage {
    /** Prefix that we add to configuration keys to distinguish them in metastorage. */
    private static String LOCAL_PREFIX = "loc-cfg";

    /** Logger. */
    private static final IgniteLogger LOG = IgniteLogger.forClass(LocalConfigurationStorage.class);

    /** Id of watch that is responsible for configuration update. */
    private long watchId = 0L;

    /** Vault manager. */
    private final VaultManager vaultMgr;

    /** Latch for waiting all changes that we are expecting from watcher */
    private CountDownLatch latch = new CountDownLatch(0);

    /**
     * Constructor.
     *
     * @param vaultMgr Vault manager.
     */
    public LocalConfigurationStorage(VaultManager vaultMgr) {
        this.vaultMgr = vaultMgr;
    }

    /** Change listeners. */
    private List<ConfigurationStorageListener> listeners = new CopyOnWriteArrayList<>();

    /** Storage version. */
    private AtomicLong version = new AtomicLong(0);

    /** Start key in ragne for searching local configuration keys. */
    private ByteArray localKeysStartRange = ByteArray.fromString(LOCAL_PREFIX + ".");

    /** End key in range for searching local configuration keys. */
    private ByteArray localKeysEndRange = ByteArray.fromString(LOCAL_PREFIX + (char)('.' + 1));

    /** {@inheritDoc} */
    @Override public synchronized Data readAll() throws StorageException {
        Iterator<Entry> iter =
            vaultMgr.range(localKeysStartRange, localKeysEndRange);

        HashMap<String, Serializable> data = new HashMap<>();

        while (iter.hasNext()) {
            Entry val = iter.next();

            data.put(val.key().toString().replaceFirst(LOCAL_PREFIX + ".", ""),
                (Serializable)ByteUtils.fromBytes(val.value()));
        }

        // TODO: Need to restore version from pds when restart
        return new Data(data, version.get());
    }

    /** {@inheritDoc} */
    @Override public synchronized CompletableFuture<Boolean> write(Map<String, Serializable> newValues, long sentVersion) {
        if (sentVersion != version.get())
            return CompletableFuture.completedFuture(false);

        CompletableFuture[] futs = new CompletableFuture[newValues.size()];

        int i = 0;

        latch = new CountDownLatch(newValues.size());

        for (Map.Entry<String, Serializable> entry : newValues.entrySet()) {
            ByteArray key = ByteArray.fromString(LOCAL_PREFIX + "." + entry.getKey());

            if (entry.getValue() != null)
                futs[i++] = vaultMgr.put(key, ByteUtils.toBytes(entry.getValue()));
            else
                futs[i++] = vaultMgr.remove(key);
        }

        try {
            CompletableFuture.allOf(futs).get();

            latch.await();

            for (Map.Entry<String, Serializable> entry : newValues.entrySet()) {
                ByteArray key = ByteArray.fromString(LOCAL_PREFIX + "." + entry.getKey());

                Entry e = vaultMgr.get(key).get();

                if (Arrays.equals(e.value(), ByteUtils.toBytes(entry.getValue())))
                    // value by some key was overwritten, that means that changes not
                    // from LocalConfigurationStorage.write overlapped with current changes, so write should be retried.
                    return CompletableFuture.completedFuture(false);
            }
        }
        catch (InterruptedException | ExecutionException e) {
            return CompletableFuture.completedFuture(false);
        }

        return CompletableFuture.completedFuture(true);
    }

    /** {@inheritDoc} */
    @Override public void addListener(ConfigurationStorageListener listener) {
        listeners.add(listener);

        if (watchId == 0) {
            try {
                watchId = vaultMgr.watch(new VaultWatch(localKeysStartRange, localKeysEndRange, new VaultListener() {
                    // In the current implementation entries always contains only one entry
                    @Override public boolean onUpdate(@NotNull Iterable<Entry> entries) {
                        HashMap<String, Serializable> data = new HashMap<>();

                        for (Entry e : entries) {
                            data.put(e.key().toString().replaceFirst(LOCAL_PREFIX + ".", ""),
                                (Serializable)ByteUtils.fromBytes(e.value()));
                        }

                        listeners.forEach(listener -> listener.onEntriesChanged(new Data(data, version.incrementAndGet())));

                        latch.countDown();

                        return true;
                    }

                    @Override public void onError(@NotNull Throwable e) {
                        LOG.error("Vault listener issue", e);
                    }
                })).get();
            }
            catch (InterruptedException | ExecutionException e) {
                LOG.error("Vault watch issue", e);
            }
        }
    }

    /** {@inheritDoc} */
    @Override public void removeListener(ConfigurationStorageListener listener) {
        listeners.remove(listener);

        if (listeners.isEmpty()) {
            vaultMgr.stopWatch(watchId);

            watchId = 0;
        }
    }

    /** {@inheritDoc} */
    @Override public void notifyApplied(long storageRevision) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public ConfigurationType type() {
        return ConfigurationType.LOCAL;
    }
}
