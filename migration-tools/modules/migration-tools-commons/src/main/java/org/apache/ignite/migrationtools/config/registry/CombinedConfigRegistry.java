/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
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

    @Override public <V, C, T extends ConfigurationTree<V, C>> T getConfiguration(RootKey<T, V> rootKey) {
        return Optional.ofNullable(localRegistry.getConfiguration(rootKey))
                .orElseGet(() -> distributedRegistry.getConfiguration(rootKey));
    }

    public CompletableFuture<Void> onDefaultsPersisted() {
        return CompletableFuture.allOf(localRegistry.onDefaultsPersisted(), distributedRegistry.onDefaultsPersisted());
    }
}
