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

package org.apache.ignite.internal.storage;

import static java.util.concurrent.CompletableFuture.failedFuture;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.apache.ignite.internal.util.IgniteUtils.closeAll;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.storage.configurations.StorageConfiguration;
import org.apache.ignite.internal.storage.configurations.StorageProfileConfiguration;
import org.apache.ignite.internal.storage.configurations.StorageProfileView;
import org.apache.ignite.internal.storage.engine.StorageEngine;
import org.apache.ignite.internal.tostring.S;
import org.jetbrains.annotations.Nullable;

/** Data storage manager. */
public class DataStorageManager implements IgniteComponent {
    /** Mapping: {@link DataStorageModule#name} -> {@link StorageEngine}. */
    private final Map<String, StorageEngine> engines;

    /** Mapping: {@link StorageProfileConfiguration#name()} -> {@link StorageEngine#name()}. */
    private Map<String, String> profilesToEngines;

    /** Storage configuration. **/
    private StorageConfiguration storageConfiguration;

    /**
     * Constructor.
     *
     * @param engines Storage engines unique by {@link DataStorageModule#name name}.
     * @param storageConfiguration Storage configuration. Needed to extract the storage profiles configurations after start.
     */
    public DataStorageManager(Map<String, StorageEngine> engines, StorageConfiguration storageConfiguration) {
        assert !engines.isEmpty();

        this.engines = engines;
        this.storageConfiguration = storageConfiguration;
    }

    @Override
    public CompletableFuture<Void> startAsync(ExecutorService startupExecutor) throws StorageException {
        engines.values().forEach(StorageEngine::start);

        profilesToEngines = storageConfiguration.value().profiles().stream()
                .collect(Collectors.toMap(StorageProfileView::name, StorageProfileView::engine));

        return nullCompletedFuture();
    }

    @Override
    public CompletableFuture<Void> stopAsync() {
        try {
            closeAll(engines.values().stream().map(engine -> engine::stop));
        } catch (Exception e) {
            return failedFuture(e);
        }

        return nullCompletedFuture();
    }

    /**
     * Get storage engine by storage profile name.
     *
     * @param storageProfile Name of storage profile.
     * @return Storage engine of the input storage profile or {@code null} if storage profile is not exist on the current node.
     */
    public @Nullable StorageEngine engineByStorageProfile(String storageProfile) {
        String engine = profilesToEngines.get(storageProfile);

        assert engine != null : "Unknown storage profile '" + storageProfile + "'";

        return engines.get(engine);
    }

    @Override
    public String toString() {
        return S.toString(DataStorageManager.class, this);
    }
}
