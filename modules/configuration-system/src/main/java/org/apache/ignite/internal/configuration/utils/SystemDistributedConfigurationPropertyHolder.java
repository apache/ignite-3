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

package org.apache.ignite.internal.configuration.utils;

import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;

import java.util.function.Function;
import java.util.function.ObjLongConsumer;
import org.apache.ignite.internal.configuration.SystemDistributedConfiguration;
import org.apache.ignite.internal.configuration.SystemDistributedView;
import org.apache.ignite.internal.configuration.SystemPropertyView;

/** Holder of system distributed configuration property with auto-update and support of external listener. */
public class SystemDistributedConfigurationPropertyHolder<T> {
    /** Configuration property name. */
    private final String propertyName;

    /** Default value. */
    private final T defaultValue;

    /** System distributed configuration. */
    private final SystemDistributedConfiguration systemDistributedConfig;

    /** Current value of target system distributed configuration property. */
    private volatile T currentValue;

    /** Listener, which receives (newValue, revision) on every configuration update. */
    private final ObjLongConsumer<T> valueListener;

    /** Converter to translate {@link String} representation of property value to target type. */
    private final Function<String, T> propertyConverter;

    /**
     * Constructor.
     *
     * @param systemDistributedConfig System distributed configuration.
     * @param valueListener Listener, which receives (newValue, revision) on every configuration update.
     * @param propertyName Configuration property name.
     * @param defaultValue Default value.
     * @param propertyConverter Converter to translate {@link String} representation of property value to target type.
     */
    public SystemDistributedConfigurationPropertyHolder(
            SystemDistributedConfiguration systemDistributedConfig,
            ObjLongConsumer<T> valueListener,
            String propertyName,
            T defaultValue,
            Function<String, T> propertyConverter
    ) {
        this.systemDistributedConfig = systemDistributedConfig;
        this.valueListener = valueListener;
        this.propertyName = propertyName;
        this.defaultValue = defaultValue;
        this.propertyConverter = propertyConverter;

        systemDistributedConfig.listen(ctx -> {
            updateSystemProperties(ctx.newValue(), ctx.storageRevision());

            return nullCompletedFuture();
        });
    }

    /**
     * Init property value, but doesn't call the listener.
     *
     * <p>If this method's call or first configuration update will not occur before holder usage, it will produce a {@code null} value.
     */
    public void init() {
        updateSystemProperties(systemDistributedConfig.value(), -1);
    }

    /**
     * Returns current value of configuration property.
     *
     * @return Current value.
     */
    public T currentValue() {
        return currentValue;
    }

    /**
     * Update current value and call listener (if revision != -1).
     *
     * @param view System distributed view.
     * @param revision Metastorage revision.
     */
    private void updateSystemProperties(SystemDistributedView view, long revision) {
        SystemPropertyView systemPropertyView = view.properties().get(propertyName);

        T value = (systemPropertyView == null) ? defaultValue : propertyConverter.apply(systemPropertyView.propertyValue());

        currentValue = value;

        if (revision != -1) {
            valueListener.accept(value, revision);
        }
    }
}
