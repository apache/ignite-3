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

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Function;
import org.apache.ignite.internal.configuration.SystemDistributedConfiguration;
import org.apache.ignite.internal.configuration.SystemDistributedView;
import org.apache.ignite.internal.configuration.SystemPropertyView;
import org.jetbrains.annotations.TestOnly;

/** Configuration for zones high availability configurations. */
public class SystemDistributedConfigurationHolder<T> {
    /** Configuration property name. */
    private final String propertyName;

    /** Default value. */
    private final T defaultValue;

    /** System distributed configuration. */
    private final SystemDistributedConfiguration systemDistributedConfig;

    /** Current value of target system distributed configuration property. */
    private final AtomicReference<T> currentValue = new AtomicReference<>();

    /** Listener, which receives (newValue, revision) on every configuration update. */
    private final BiConsumer<T, Long> valueListener;

    /** Converter to translate String representation to target type. */
    private final Function<String, T> propertyConverter;

    /** Constructor. */
    public SystemDistributedConfigurationHolder(
            SystemDistributedConfiguration systemDistributedConfig,
            BiConsumer<T, Long> valueListener,
            String propertyName,
            T defaultValue,
            Function<String, T> propertyConverter
    ) {
        this.systemDistributedConfig = systemDistributedConfig;
        this.valueListener = valueListener;
        this.propertyName = propertyName;
        this.defaultValue = defaultValue;
        this.propertyConverter = propertyConverter;
    }

    /** Starts component. */
    public void start() {
        updateSystemProperties(systemDistributedConfig.value(), -1);

        systemDistributedConfig.listen(ctx -> {
            updateSystemProperties(ctx.newValue(), ctx.storageRevision());

            return CompletableFuture.completedFuture(null);
        });
    }

    /** Starts the component and initializes the configuration immediately. */
    @TestOnly
    void startAndInit() {
        start();

        updateSystemProperties(systemDistributedConfig.value(), 0);
    }

    /** Returns current value of configuration property. */
    public T currentValue() {
        return currentValue.get();
    }

    private void updateSystemProperties(SystemDistributedView view, long revision) {
        SystemPropertyView systemPropertyView = view.properties().get(propertyName);

        T value = (systemPropertyView == null) ? defaultValue : propertyConverter.apply(systemPropertyView.propertyValue());

        currentValue.set(value);

        if (revision != -1) {
            valueListener.accept(value, revision);
        }
    }
}
