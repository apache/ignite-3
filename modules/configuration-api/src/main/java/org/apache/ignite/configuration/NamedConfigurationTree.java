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

package org.apache.ignite.configuration;

import java.util.List;
import java.util.UUID;
import org.apache.ignite.configuration.notifications.ConfigurationNamedListListener;
import org.jetbrains.annotations.Nullable;

/**
 * Configuration tree representing arbitrary set of named underlying configuration tree of the same type.
 *
 * @param <T> Type of the underlying configuration tree.
 * @param <VIEWT> Value type of the underlying node.
 * @param <CHANGET> Type of the object that changes underlying nodes values.
 */
public interface NamedConfigurationTree<T extends ConfigurationProperty<VIEWT>, VIEWT, CHANGET extends VIEWT>
        extends ConfigurationTree<NamedListView<VIEWT>, NamedListChange<VIEWT, CHANGET>> {
    /**
     * Get named configuration by name.
     *
     * @param name Name.
     */
    @Nullable T get(String name);

    /**
     * Retrieves a named list element by its internal id.
     *
     * @param internalId Internal id.
     * @return Named list element, associated with the passed internal id, or {@code null} if it doesn't exist.
     */
    @Nullable T get(UUID internalId);

    /**
     * Returns all internal ids of the elements from the list.
     */
    List<UUID> internalIds();

    /**
     * Add named-list-specific configuration values listener.
     *
     * <p>NOTE: If this method is called from another listener, then it is guaranteed to be called starting from the next configuration
     * update only.
     *
     * @param listener Listener.
     */
    void listenElements(ConfigurationNamedListListener<VIEWT> listener);

    /**
     * Removes named-list-specific configuration values listener.
     *
     * <p>NOTE: Unpredictable behavior if the method is called inside other listeners.
     *
     * @param listener Listener.
     */
    void stopListenElements(ConfigurationNamedListListener<VIEWT> listener);

    /**
     * Returns a placeholder that allows you to add listeners for changing configuration value of any element of the named list and any of
     * its nested configurations.
     *
     * <p>NOTE: {@link ConfigurationListenOnlyException} will be thrown when trying to get/update the configuration values.
     */
    T any();

    @Override
    @SuppressWarnings("unchecked")
    NamedConfigurationTree<T, VIEWT, CHANGET> directProxy();
}
