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

package org.apache.ignite.internal.configuration;

import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import org.apache.ignite.configuration.ConfigurationTree;
import org.apache.ignite.configuration.KeyIgnorer;
import org.apache.ignite.configuration.RootKey;
import org.apache.ignite.configuration.SuperRootChange;
import org.apache.ignite.internal.configuration.tree.ConfigurationSource;
import org.apache.ignite.internal.manager.IgniteComponent;

/**
 * Configuration registry interface.
 */
public interface ConfigurationRegistry extends IgniteComponent {
    /**
     * Gets the public configuration tree.
     *
     * @param rootKey Root key.
     * @param <V> View type.
     * @param <C> Change type.
     * @param <T> Configuration tree type.
     * @return Public configuration tree.
     */
    <V, C extends V, T extends ConfigurationTree<? super V, ? super C>> T getConfiguration(RootKey<T, V, C> rootKey);

    /**
     * Returns a future that resolves after the defaults are persisted to the storage.
     */
    CompletableFuture<Void> onDefaultsPersisted();

    /**
     * Change configuration. Gives the possibility to atomically update several root trees.
     *
     * @param change Closure that would consume a mutable super root instance.
     * @return Future that is completed on change completion.
     */
    CompletableFuture<Void> change(Consumer<SuperRootChange> change);

    /**
     * Change configuration.
     *
     * @param changesSrc Configuration source to create patch from it.
     * @return Future that is completed on change completion.
     */
    CompletableFuture<Void> change(ConfigurationSource changesSrc);

    /**
     * Initializes the configuration with the given source. This method should be used only for the initial setup of the configuration. The
     * configuration is initialized with the provided source only if the storage is empty, and it is saved along with the defaults. This
     * method must be called before {@link #startAsync}.
     *
     * @param configurationSource the configuration source to initialize with.
     */
    void initializeConfigurationWith(ConfigurationSource configurationSource);

    /**
     * Determines if key should be ignored.
     */
    KeyIgnorer keyIgnorer();

    /**
     * Returns the count of configuration listener notifications.
     *
     * <p>Monotonically increasing value that should be incremented each time an attempt is made to notify all listeners of the
     * configuration. Allows to guarantee that new listeners will be called only on the next notification of all configuration listeners.
     */
    long notificationCount();
}
