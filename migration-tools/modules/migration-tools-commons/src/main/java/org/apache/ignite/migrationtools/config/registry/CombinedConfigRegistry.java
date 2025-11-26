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

package org.apache.ignite.migrationtools.config.registry;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite3.configuration.ConfigurationTree;
import org.apache.ignite3.configuration.RootKey;
import org.apache.ignite3.internal.configuration.ConfigurationRegistry;
import org.apache.ignite3.internal.manager.ComponentContext;
import org.apache.ignite3.internal.util.IgniteUtils;

/**
 * Registry has two separated internal registries for cluster and node configurations.
 */
public class CombinedConfigRegistry implements ConfigurationRegistryInterface {

    private final ConfigurationRegistry localRegistry;

    private final ConfigurationRegistry distributedRegistry;

    public CombinedConfigRegistry(ConfigurationRegistry localRegistry, ConfigurationRegistry distributedRegistry) {
        this.localRegistry = localRegistry;
        this.distributedRegistry = distributedRegistry;
    }

    @Override
    public CompletableFuture<Void> startAsync(ComponentContext context) {
        return IgniteUtils.startAsync(context, localRegistry, distributedRegistry);
    }

    @Override
    public CompletableFuture<Void> stopAsync(ComponentContext context) {
        return IgniteUtils.stopAsync(context, localRegistry, distributedRegistry);
    }

    @Override public <V, C extends V, T extends ConfigurationTree<? super V, ? super C>> T getConfiguration(RootKey<T, V, C> rootKey) {
        return Optional.ofNullable(localRegistry.getConfiguration(rootKey))
                .orElseGet(() -> distributedRegistry.getConfiguration(rootKey));
    }

    public CompletableFuture<Void> onDefaultsPersisted() {
        return CompletableFuture.allOf(localRegistry.onDefaultsPersisted(), distributedRegistry.onDefaultsPersisted());
    }
}
