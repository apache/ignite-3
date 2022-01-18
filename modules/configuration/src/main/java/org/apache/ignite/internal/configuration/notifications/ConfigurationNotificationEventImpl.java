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

package org.apache.ignite.internal.configuration.notifications;

import java.util.Map;
import org.apache.ignite.configuration.ConfigurationProperty;
import org.apache.ignite.configuration.notifications.ConfigurationNotificationEvent;
import org.jetbrains.annotations.Nullable;

/**
 * Implementation of the {@link ConfigurationNotificationEvent}.
 *
 * @param <VIEWT> Type of the subtree or the value that has been changed.
 */
class ConfigurationNotificationEventImpl<VIEWT> implements ConfigurationNotificationEvent<VIEWT> {
    /** Old value. */
    @Nullable
    private final VIEWT oldValue;

    /** New value. */
    @Nullable
    private final VIEWT newValue;

    /** Storage revision. */
    private final long storageRevision;

    /** Configuration container. */
    private final Map<Class<?>, ConfigurationContainer> configs;

    /**
     * Constructor.
     *
     * @param oldValue Old value.
     * @param newValue New value.
     * @param storageRevision Storage revision.
     * @param configs Configuration containers.
     */
    ConfigurationNotificationEventImpl(
            @Nullable VIEWT oldValue,
            @Nullable VIEWT newValue,
            long storageRevision,
            Map<Class<?>, ConfigurationContainer> configs
    ) {
        this.oldValue = oldValue;
        this.newValue = newValue;
        this.storageRevision = storageRevision;
        this.configs = configs;
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable VIEWT oldValue() {
        return oldValue;
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable VIEWT newValue() {
        return newValue;
    }

    /** {@inheritDoc} */
    @Override
    public long storageRevision() {
        return storageRevision;
    }

    /** {@inheritDoc} */
    @Override
    public <T extends ConfigurationProperty> @Nullable T config(Class<?> configClass) {
        ConfigurationContainer container = configs.get(configClass);

        return container == null ? null : (T) container.config;
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable String name(Class<?> configClass) {
        ConfigurationContainer container = configs.get(configClass);

        return container == null ? null : container.name;
    }
}
