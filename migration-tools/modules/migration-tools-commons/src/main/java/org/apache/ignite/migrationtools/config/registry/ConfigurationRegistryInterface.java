/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.migrationtools.config.registry;

import org.apache.ignite3.configuration.ConfigurationTree;
import org.apache.ignite3.configuration.RootKey;
import org.apache.ignite3.internal.manager.IgniteComponent;

/**
 * Custom Configuration Registry Interface.
 */
public interface ConfigurationRegistryInterface extends IgniteComponent {
    <V, C, T extends ConfigurationTree<V, C>> T getConfiguration(RootKey<T, V> rootKey);
}
