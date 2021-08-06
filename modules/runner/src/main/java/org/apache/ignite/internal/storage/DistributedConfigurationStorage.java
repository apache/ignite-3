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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.configuration.annotation.ConfigurationType;
import org.apache.ignite.internal.configuration.storage.ConfigurationStorage;
import org.apache.ignite.internal.configuration.storage.ConfigurationStorageListener;
import org.apache.ignite.internal.configuration.storage.Data;
import org.apache.ignite.internal.configuration.storage.StorageException;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.metastorage.client.Conditions;
import org.apache.ignite.internal.metastorage.client.Entry;
import org.apache.ignite.internal.metastorage.client.EntryEvent;
import org.apache.ignite.internal.metastorage.client.Operation;
import org.apache.ignite.internal.metastorage.client.Operations;
import org.apache.ignite.internal.metastorage.client.WatchEvent;
import org.apache.ignite.internal.metastorage.client.WatchListener;
import org.apache.ignite.internal.util.ByteUtils;
import org.apache.ignite.internal.util.Cursor;
import org.apache.ignite.internal.vault.VaultEntry;
import org.apache.ignite.internal.vault.VaultManager;
import org.apache.ignite.lang.ByteArray;
import org.apache.ignite.lang.IgniteLogger;
import org.jetbrains.annotations.NotNull;

/**
 * Distributed configuration storage.
 */
public class DistributedConfigurationStorage implements ConfigurationStorage {
    /** Logger. */
    private static final IgniteLogger LOG = IgniteLogger.forClass(DistributedConfigurationStorage.class);

    /** Prefix added to configuration keys to distinguish them in the meta storage. Must end with a dot. */
    public static final String DISTRIBUTED_PREFIX = "dst-cfg.";

    /**
     * Key for CAS-ing configuration keys to meta storage.
     */
    private static final ByteArray MASTER_KEY = new ByteArray(DISTRIBUTED_PREFIX + "$master$key");

    /**
     * Prefix for all keys in the distributed storage. This key is expected to be the first key in lexicographical
     * order of distributed configuration keys.
     */
    private static final ByteArray DST_KEYS_START_RANGE = new ByteArray(DISTRIBUTED_PREFIX);

    /**
     * This key is expected to be the last key in lexicographical order of distributed configuration keys. It is
     * possible because keys are in lexicographical order in meta storage and adding {@code (char)('.' + 1)} to the end
     * will produce all keys with prefix {@link DistributedConfigurationStorage#DISTRIBUTED_PREFIX}
     */
    private static final ByteArray DST_KEYS_END_RANGE =
            new ByteArray(DISTRIBUTED_PREFIX.substring(0, DISTRIBUTED_PREFIX.length() - 1) + (char)('.' + 1));

    /** Meta storage manager. */
    private final MetaStorageManager metaStorageMgr;

    /** Vault manager. */
    private final VaultManager vaultMgr;

    /** Configuration changes listener. */
    private volatile ConfigurationStorageListener lsnr;

    /** Storage version. It stores actual meta storage revision, that is applied to configuration manager. */
    private final AtomicLong changeId = new AtomicLong(0L);

    /**
     * Constructor.
     *
     * @param metaStorageMgr Meta storage manager.
     * @param vaultMgr Vault manager.
     */
    public DistributedConfigurationStorage(MetaStorageManager metaStorageMgr, VaultManager vaultMgr) {
        this.metaStorageMgr = metaStorageMgr;

        this.vaultMgr = vaultMgr;
    }

    /** {@inheritDoc} */
    @Override public Data readAll() throws StorageException {
        var data = new HashMap<String, Serializable>();

        VaultEntry appliedRevEntry = vaultMgr.get(MetaStorageManager.APPLIED_REV).join();

        long appliedRevision = appliedRevEntry.value() == null ? 0L : ByteUtils.bytesToLong(appliedRevEntry.value());

        try (Cursor<VaultEntry> entries = storedDistributedConfigKeys()) {
            for (VaultEntry entry : entries) {
                ByteArray key = entry.key();
                byte[] value = entry.value();

                // vault iterator should not return nulls as values
                assert value != null;

                if (key.equals(MASTER_KEY))
                    continue;

                String dataKey = key.toString().substring(DISTRIBUTED_PREFIX.length());

                data.put(dataKey, (Serializable)ByteUtils.fromBytes(value));
            }
        }
        catch (Exception e) {
            throw new StorageException("Exception when closing a Vault cursor", e);
        }

        assert data.isEmpty() || appliedRevision > 0;

        changeId.set(appliedRevision);

        return new Data(data, appliedRevision);
    }

    /** {@inheritDoc} */
    @Override public CompletableFuture<Boolean> write(Map<String, Serializable> newValues, long curChangeId) {
        assert curChangeId <= changeId.get();
        assert lsnr != null : "Configuration listener must be initialized before write.";

        if (curChangeId < changeId.get())
            // This means that curChangeId is less than version and other node has already updated configuration and
            // write should be retried. Actual version will be set when watch and corresponding configuration listener
            // updates configuration.
            return CompletableFuture.completedFuture(false);

        Set<Operation> operations = new HashSet<>();

        for (Map.Entry<String, Serializable> entry : newValues.entrySet()) {
            ByteArray key = new ByteArray(DISTRIBUTED_PREFIX + entry.getKey());

            if (entry.getValue() != null)
                // TODO: investigate overhead when serialize int, long, double, boolean, string, arrays of above
                // TODO: https://issues.apache.org/jira/browse/IGNITE-14698
                operations.add(Operations.put(key, ByteUtils.toBytes(entry.getValue())));
            else
                operations.add(Operations.remove(key));
        }

        operations.add(Operations.put(MASTER_KEY, ByteUtils.longToBytes(curChangeId)));

        if (curChangeId == 0) {
            return metaStorageMgr.invoke(
                Conditions.notExists(MASTER_KEY),
                operations,
                Set.of(Operations.noop())
            );
        }
        else {
            return metaStorageMgr.invoke(
                Conditions.revision(MASTER_KEY).le(curChangeId),
                operations,
                Set.of(Operations.noop())
            );
        }
    }

    /** {@inheritDoc} */
    @Override public synchronized void registerConfigurationListener(@NotNull ConfigurationStorageListener lsnr) {
        if (this.lsnr == null) {
            this.lsnr = lsnr;

            // TODO: registerWatchByPrefix could throw OperationTimeoutException and CompactedException and we should
            // TODO: properly handle such cases https://issues.apache.org/jira/browse/IGNITE-14604
            metaStorageMgr.registerWatchByPrefix(DST_KEYS_START_RANGE, new WatchListener() {
                @Override public boolean onUpdate(@NotNull WatchEvent events) {
                    Map<String, Serializable> data = new HashMap<>();

                    Entry masterKeyEntry = null;

                    for (EntryEvent event : events.entryEvents()) {
                        Entry e = event.newEntry();

                        if (e.key().equals(MASTER_KEY))
                            masterKeyEntry = e;
                        else {
                            String key = e.key().toString().substring(DISTRIBUTED_PREFIX.length());

                            Serializable value = e.value() == null ?
                                null :
                                (Serializable)ByteUtils.fromBytes(e.value());

                            data.put(key, value);
                        }
                    }

                    // Contract of meta storage ensures that all updates of one revision will come in one batch.
                    // Also masterKey should be updated every time when we update cfg.
                    // That means that masterKey update must be included in the batch.
                    assert masterKeyEntry != null;

                    long newChangeId = masterKeyEntry.revision();

                    assert newChangeId > changeId.get();

                    changeId.set(newChangeId);

                    lsnr.onEntriesChanged(new Data(data, newChangeId)).join();

                    return true;
                }

                @Override public void onError(@NotNull Throwable e) {
                    // TODO: need to handle this case and there should some mechanism for registering new watch as far as
                    // TODO: onError unregisters failed watch https://issues.apache.org/jira/browse/IGNITE-14604
                    LOG.error("Meta storage listener issue", e);
                }
            });
        } else
            LOG.warn("Configuration listener has already been set.");
    }

    /** {@inheritDoc} */
    @Override public ConfigurationType type() {
        return ConfigurationType.DISTRIBUTED;
    }

    /**
     * Method that returns all distributed configuration keys from the meta storage that were stored in the vault
     * filtered out by the current applied revision as an upper bound. Applied revision is a revision of the last
     * successful vault update.
     * <p>
     * This is possible to distinguish cfg keys from meta storage because we add a special prefix {@link
     * DistributedConfigurationStorage#DISTRIBUTED_PREFIX} to all configuration keys that we put to the meta storage.
     *
     * @return Iterator built upon all distributed configuration entries stored in vault.
     */
    private @NotNull Cursor<VaultEntry> storedDistributedConfigKeys() {
        return vaultMgr.range(DST_KEYS_START_RANGE, DST_KEYS_END_RANGE);
    }
}
