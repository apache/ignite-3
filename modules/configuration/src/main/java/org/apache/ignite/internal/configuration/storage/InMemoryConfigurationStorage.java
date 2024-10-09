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

import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.CompletableFuture.runAsync;
import static java.util.stream.Collectors.toUnmodifiableMap;
import static org.apache.ignite.internal.util.CompletableFutures.falseCompletedFuture;
import static org.apache.ignite.internal.util.CompletableFutures.trueCompletedFuture;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.configuration.annotation.ConfigurationType;
import org.apache.ignite.internal.future.InFlightFutures;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;

/**
 * Test configuration storage.
 */
public class InMemoryConfigurationStorage implements ConfigurationStorage {
    private static final IgniteLogger LOG = Loggers.forClass(InMemoryConfigurationStorage.class);

    /** Configuration type. */
    private final ConfigurationType configurationType;

    /** Map to store values. */
    private final Map<String, Serializable> map = new HashMap<>();

    /** Configuration changes listener. */
    private final AtomicReference<ConfigurationStorageListener> lsnrRef = new AtomicReference<>();

    private final InFlightFutures futureTracker = new InFlightFutures();

    /** Storage version. Guarded by {@code this}. */
    private long version = 0;

    /**
     * Constructor.
     *
     * @param type Configuration type.
     */
    public InMemoryConfigurationStorage(ConfigurationType type) {
        configurationType = type;
    }

    @Override
    public void close() {
        futureTracker.cancelInFlightFutures();
    }

    @Override
    public synchronized CompletableFuture<Map<String, ? extends Serializable>> readAllLatest(String prefix) {
        return completedFuture(map.entrySet().stream()
                .filter(e -> e.getKey().startsWith(prefix))
                .collect(toUnmodifiableMap(Map.Entry::getKey, Map.Entry::getValue)));
    }

    @Override
    public synchronized CompletableFuture<Serializable> readLatest(String key) throws StorageException {
        return completedFuture(map.get(key));
    }

    @Override
    public synchronized CompletableFuture<Data> readDataOnRecovery() {
        return completedFuture(new Data(new HashMap<>(map), version));
    }

    @Override
    public synchronized CompletableFuture<Boolean> write(Map<String, ? extends Serializable> newValues, long sentVersion) {
        if (sentVersion != version) {
            return falseCompletedFuture();
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

        futureTracker.registerFuture(runAsync(() -> lsnrRef.get().onEntriesChanged(data)));

        return trueCompletedFuture();
    }

    @Override
    public synchronized void registerConfigurationListener(ConfigurationStorageListener listener) {
        if (!lsnrRef.compareAndSet(null, listener)) {
            LOG.warn("Configuration listener has already been set");
        }
    }

    @Override
    public ConfigurationType type() {
        return configurationType;
    }

    @Override
    public synchronized CompletableFuture<Long> lastRevision() {
        return completedFuture(version);
    }

    @Override
    public CompletableFuture<Long> localRevision() {
        return lastRevision();
    }
}
