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

package org.apache.ignite.internal.configuration.storage;

import static java.util.concurrent.CompletableFuture.supplyAsync;
import static java.util.stream.Collectors.toUnmodifiableMap;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.configuration.annotation.ConfigurationType;

/**
 * Test configuration storage.
 */
public class TestConfigurationStorage implements ConfigurationStorage {
    /** Configuration type. */
    private final ConfigurationType configurationType;

    /** Map to store values. */
    private final Map<String, Serializable> map = new HashMap<>();

    /** Change listeners. Guarded by {@code this}. */
    private final Collection<ConfigurationStorageListener> listeners = new ArrayList<>();

    /** Storage version. Guarded by {@code this}. */
    private long version = 0;

    /** Should fail on every operation. Guarded by {@code this}. */
    private boolean fail = false;

    /**
     * Constructor.
     *
     * @param type Configuration type.
     */
    public TestConfigurationStorage(ConfigurationType type) {
        this(type, Map.of());
    }

    public TestConfigurationStorage(ConfigurationType type, Map<String, Serializable> data) {
        configurationType = type;
        map.putAll(data);
    }

    @Override
    public void close() {
        // No-op.
    }

    /**
     * Set fail flag.
     *
     * @param fail Fail flag.
     */
    public synchronized void fail(boolean fail) {
        this.fail = fail;
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Map<String, ? extends Serializable>> readAllLatest(String prefix) {
        return supplyAsync(() -> {
            synchronized (this) {
                if (fail) {
                    throw new StorageException("Failed to read data");
                }

                return map.entrySet().stream()
                        .filter(e -> e.getKey().startsWith(prefix))
                        .collect(toUnmodifiableMap(Map.Entry::getKey, Map.Entry::getValue));
            }
        });
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Serializable> readLatest(String key) throws StorageException {
        return supplyAsync(() -> {
            synchronized (this) {
                if (fail) {
                    throw new StorageException("Failed to read data");
                }

                return map.get(key);
            }
        });
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Data> readDataOnRecovery() {
        return supplyAsync(() -> {
            synchronized (this) {
                if (fail) {
                    throw new StorageException("Failed to read data");
                }

                return new Data(new HashMap<>(map), version);
            }
        });
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Boolean> write(Map<String, ? extends Serializable> newValues, long sentVersion) {
        return supplyAsync(() -> {
            synchronized (this) {
                if (fail) {
                    throw new StorageException("Failed to write data");
                }

                if (sentVersion != version) {
                    return false;
                }

                for (Map.Entry<String, ? extends Serializable> entry : newValues.entrySet()) {
                    if (entry.getValue() != null) {
                        map.put(entry.getKey(), entry.getValue());
                    } else {
                        map.remove(entry.getKey());
                    }
                }

                version++;

                var data = new Data(newValues, version);

                listeners.forEach(listener -> listener.onEntriesChanged(data).join());

                return true;
            }
        });
    }

    /** {@inheritDoc} */
    @Override
    public synchronized void registerConfigurationListener(ConfigurationStorageListener listener) {
        listeners.add(listener);
    }

    /** {@inheritDoc} */
    @Override
    public ConfigurationType type() {
        return configurationType;
    }

    /** {@inheritDoc} */
    @Override
    public synchronized CompletableFuture<Long> lastRevision() {
        return CompletableFuture.completedFuture(version);
    }

    @Override
    public CompletableFuture<Long> localRevision() {
        return lastRevision();
    }

    /**
     * Increase the current revision of the storage.
     *
     * <p>New configuration changes will wait when the new configuration is updated from the repository.
     *
     * <p>For pending updates to apply, you will need to call {@link #decrementAndGetRevision}
     * and make an additional (new) configuration change.
     *
     * @return Storage revision.
     */
    public synchronized long incrementAndGetRevision() {
        return ++version;
    }

    /**
     * Decrease the current revision of the storage.
     *
     * @return Repository revision.
     * @see #incrementAndGetRevision
     */
    public synchronized long decrementAndGetRevision() {
        return --version;
    }
}
