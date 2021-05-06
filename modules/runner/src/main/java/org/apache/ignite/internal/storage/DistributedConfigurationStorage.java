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
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.configuration.storage.ConfigurationStorage;
import org.apache.ignite.configuration.storage.ConfigurationStorageListener;
import org.apache.ignite.configuration.storage.ConfigurationType;
import org.apache.ignite.configuration.storage.Data;
import org.apache.ignite.configuration.storage.StorageException;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.util.ByteUtils;
import org.apache.ignite.lang.IgniteLogger;
import org.apache.ignite.metastorage.common.Conditions;
import org.apache.ignite.metastorage.common.Cursor;
import org.apache.ignite.metastorage.common.Entry;
import org.apache.ignite.metastorage.common.Key;
import org.apache.ignite.metastorage.common.Operation;
import org.apache.ignite.metastorage.common.Operations;
import org.apache.ignite.metastorage.common.WatchEvent;
import org.apache.ignite.metastorage.common.WatchListener;
import org.jetbrains.annotations.NotNull;

/**
 * Distributed configuration storage.
 */
public class DistributedConfigurationStorage implements ConfigurationStorage {
    /** Prefix that we add to configuration keys to distinguish them in metastorage. */
    private static String DISTRIBUTED_PREFIX = "dst-cfg";

    /** Logger. */
    private static final IgniteLogger LOG = IgniteLogger.forClass(DistributedConfigurationStorage.class);

    /** Key for CAS-ing configuration keys to metastorage. */
    private static final Key masterKey = new Key(DISTRIBUTED_PREFIX + ".");

    /** Id of watch that is responsible for configuration update. */
    private CompletableFuture<Long> watchId;

    /** MetaStorage manager */
    private final MetaStorageManager metaStorageMgr;

    /**
     * Constructor.
     *
     * @param metaStorageMgr MetaStorage Manager.
     */
    public DistributedConfigurationStorage(MetaStorageManager metaStorageMgr) {
        this.metaStorageMgr = metaStorageMgr;
    }

    /** Change listeners. */
    private List<ConfigurationStorageListener> listeners = new CopyOnWriteArrayList<>();

    /** Storage version. It stores actual metastorage revision, that is applied to configuration manager. */
    private AtomicLong version = new AtomicLong(0L);

    /** {@inheritDoc} */
    @Override public synchronized Data readAll() throws StorageException {

        Cursor<Entry> cur = metaStorageMgr.rangeWithAppliedRevision(new Key(DISTRIBUTED_PREFIX + "."),
            new Key(DISTRIBUTED_PREFIX + (char)('.' + 1)));

        HashMap<String, Serializable> data = new HashMap<>();

        long maxRevision = 0L;

        Entry entryForMasterKey = null;

        for (Entry entry : cur) {
            if (!entry.key().equals(masterKey)) {
                data.put(entry.key().toString().replaceFirst(DISTRIBUTED_PREFIX + ".", ""),
                    (Serializable)ByteUtils.fromBytes(entry.value()));

                // Move to stream
                if (maxRevision < entry.revision())
                    maxRevision = entry.revision();
            } else
                entryForMasterKey = entry;
        }

        if (!data.isEmpty()) {
            assert entryForMasterKey != null;

            assert maxRevision == entryForMasterKey.revision();

            assert maxRevision >= version.get();

            return new Data(data, maxRevision);
        }

        return new Data(data, version.get());
    }

    /** {@inheritDoc} */
    @Override public synchronized CompletableFuture<Boolean> write(Map<String, Serializable> newValues, long sentVersion) {
        assert sentVersion <= version.get();

        if (sentVersion != version.get())
            // This means that sentVersion is less than version and other node has already updated configuration and
            // write should be retried. Actual version will be set when watch and corresponding configuration listener
            // updates configuration and notifyApplied is triggered afterwards.
            return CompletableFuture.completedFuture(false);

        HashSet<Operation> operations = new HashSet<>();

        HashSet<Operation> failures = new HashSet<>();

        for (Map.Entry<String, Serializable> entry : newValues.entrySet()) {
            Key key = new Key(DISTRIBUTED_PREFIX + "." + entry.getKey());

            if (entry.getValue() != null)
                // TODO: investigate overhead when deserialize int, long, double, boolean, string, arrays of above
                operations.add(Operations.put(key, ByteUtils.toBytes(entry.getValue())));
            else
                operations.add(Operations.remove(key));

            failures.add(Operations.noop());
        }

        operations.add(Operations.put(masterKey, ByteUtils.longToBytes(sentVersion)));

        return metaStorageMgr.invoke(Conditions.key(masterKey).revision().eq(version.get()), operations, failures);
    }

    /** {@inheritDoc} */
    @Override public synchronized void addListener(ConfigurationStorageListener listener) {
        listeners.add(listener);

        if (watchId == null) {
            watchId = metaStorageMgr.registerWatchByPrefix(masterKey, new WatchListener() {
                @Override public boolean onUpdate(@NotNull Iterable<WatchEvent> events) {
                    HashMap<String, Serializable> data = new HashMap<>();

                    long maxRevision = 0L;

                    Entry entryForMasterKey = null;

                    for (WatchEvent event : events) {
                        Entry e = event.newEntry();

                        if (!e.key().equals(masterKey)) {
                            data.put(e.key().toString().replaceFirst(DISTRIBUTED_PREFIX + ".", ""),
                                (Serializable)ByteUtils.fromBytes(e.value()));

                            if (maxRevision < e.revision())
                                maxRevision = e.revision();
                        } else
                            entryForMasterKey = e;
                    }

                    // Contract of metastorage ensures that all updates of one revision will come in one batch.
                    // Also masterKey should be updated every time when we update cfg.
                    // That means that masterKey update must be included in the batch.
                    assert entryForMasterKey != null;

                    assert maxRevision == entryForMasterKey.revision();

                    assert maxRevision >= version.get();

                    long finalMaxRevision = maxRevision;

                    listeners.forEach(listener -> listener.onEntriesChanged(new Data(data, finalMaxRevision)));

                    return true;
                }

                @Override public void onError(@NotNull Throwable e) {
                    LOG.error("Metastorage listener issue", e);
                }
            });

        }
    }

    /** {@inheritDoc} */
    @Override public synchronized void removeListener(ConfigurationStorageListener listener) {
        listeners.remove(listener);

        if (listeners.isEmpty()) {
            try {
                metaStorageMgr.unregisterWatch(watchId.get());
            }
            catch (InterruptedException | ExecutionException e) {
                LOG.error("Failed to register watch in metastore", e);
            }

            watchId = null;
        }
    }

    /** {@inheritDoc} */
    @Override public void notifyApplied(long storageRevision) {
        assert version.get() <= storageRevision;

        version.set(storageRevision);

        // Also we should persist version,
        // this should be done when nodes restart is introduced.
    }

    /** {@inheritDoc} */
    @Override public ConfigurationType type() {
        return ConfigurationType.DISTRIBUTED;
    }
}
