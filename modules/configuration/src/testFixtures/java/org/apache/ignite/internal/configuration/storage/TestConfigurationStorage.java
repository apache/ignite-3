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

import static java.util.concurrent.CompletableFuture.runAsync;
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
    private final InMemoryConfigurationStorage configurationStorage;

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
        configurationStorage = new InMemoryConfigurationStorage(type, data);
    }

    @Override
    public CompletableFuture<ReadEntry> readDataOnRecovery() {
        return checkFail().thenCompose(unused -> configurationStorage.readDataOnRecovery());

    }

    @Override
    public CompletableFuture<Map<String, ? extends Serializable>> readAllLatest(String prefix) {
        return checkFail().thenCompose(unused -> configurationStorage.readAllLatest(prefix));
    }

    @Override
    public CompletableFuture<Serializable> readLatest(String key) {
        return checkFail().thenCompose(unused -> configurationStorage.readLatest(key));
    }

    @Override
    public CompletableFuture<Boolean> write(WriteEntry writeEntry) {
        return checkFail().thenCompose(unused -> configurationStorage.write(writeEntry));
    }

    @Override
    public void registerConfigurationListener(ConfigurationStorageListener lsnr) {
        configurationStorage.registerConfigurationListener(lsnr);
    }

    @Override
    public ConfigurationType type() {
        return configurationStorage.type();
    }

    @Override
    public CompletableFuture<Long> lastRevision() {
        return configurationStorage.lastRevision();
    }

    @Override
    public CompletableFuture<Long> localRevision() {
        return configurationStorage.localRevision();
    }

    @Override
    public void close() {
        configurationStorage.close();
    }


    private CompletableFuture<Void> checkFail() {
        return runAsync(() -> {
            synchronized (this) {
                if (fail) {
                    throw new StorageException("Failed to read data");
                }
            }
        });
    }

    /**
     * Set fail flag.
     *
     * @param fail Fail flag.
     */
    public synchronized void fail(boolean fail) {
        this.fail = fail;
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
        return ++configurationStorage.version;
    }

    /**
     * Decrease the current revision of the storage.
     *
     * @return Repository revision.
     * @see #incrementAndGetRevision
     */
    public synchronized long decrementAndGetRevision() {
        return --configurationStorage.version;
    }
}
