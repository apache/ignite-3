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

import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.configuration.RootKey;
import org.apache.ignite.internal.configuration.storage.ConfigurationStorage;
import org.apache.ignite.internal.configuration.validation.ConfigurationValidator;
import org.apache.ignite.internal.manager.ComponentContext;
import org.apache.ignite.internal.manager.IgniteComponent;

/**
 * Configuration manager is responsible for handling configuration lifecycle and provides configuration API.
 */
public class ConfigurationManager implements IgniteComponent {
    /** Configuration registry. */
    private final ConfigurationRegistry registry;

    /**
     * Constructor.
     *
     * @param rootKeys                    Configuration root keys.
     * @param storage                     Configuration storage.
     * @param generator                   Configuration tree generator.
     * @throws IllegalArgumentException If the configuration type of the root keys is not equal to the storage type, or if the schema or its
     *                                  extensions are not valid.
     */
    public ConfigurationManager(
            Collection<RootKey<?, ?>> rootKeys,
            ConfigurationStorage storage,
            ConfigurationTreeGenerator generator,
            ConfigurationValidator configurationValidator
    ) {
        registry = new ConfigurationRegistry(
                rootKeys,
                storage,
                generator,
                configurationValidator
        );
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Void> startAsync(ComponentContext componentContext) {
        return registry.startAsync(componentContext);
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Void> stopAsync(ComponentContext componentContext) {
        // TODO: IGNITE-15161 Implement component's stop.
        return registry.stopAsync(componentContext);
    }

    /**
     * Get configuration registry.
     *
     * @return Configuration registry.
     */
    public ConfigurationRegistry configurationRegistry() {
        return registry;
    }
}
