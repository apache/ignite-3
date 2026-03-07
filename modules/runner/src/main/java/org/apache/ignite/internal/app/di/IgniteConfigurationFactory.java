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

package org.apache.ignite.internal.app.di;

import io.micronaut.context.annotation.Factory;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import org.apache.ignite.internal.configuration.ConfigurationModules;
import org.apache.ignite.internal.configuration.ConfigurationRegistry;
import org.apache.ignite.internal.configuration.ConfigurationTreeGenerator;
import org.apache.ignite.internal.configuration.storage.LocalFileConfigurationStorage;
import org.apache.ignite.internal.configuration.validation.ConfigurationValidator;
import org.apache.ignite.internal.configuration.validation.ConfigurationValidatorImpl;

/**
 * Micronaut factory that produces local (node) configuration beans.
 *
 * <p>These form the configuration chain:
 * {@link ConfigurationModules} → {@link ConfigurationTreeGenerator} → {@link LocalFileConfigurationStorage}
 * → {@link ConfigurationValidator} → {@link ConfigurationRegistry}.
 */
@Factory
public class IgniteConfigurationFactory {
    private final NodeSeedParams seedParams;

    private final ConfigurationModules modules;

    public IgniteConfigurationFactory(NodeSeedParams seedParams, ConfigurationModules modules) {
        this.seedParams = seedParams;
        this.modules = modules;
    }

    /** Creates the tree generator for local configuration schemas. */
    @Singleton
    @Named("local")
    public ConfigurationTreeGenerator localConfigurationTreeGenerator() {
        return new ConfigurationTreeGenerator(
                modules.local().rootKeys(),
                modules.local().schemaExtensions(),
                modules.local().polymorphicSchemaExtensions()
        );
    }

    /** Creates the file-based storage for local configuration. */
    @Singleton
    public LocalFileConfigurationStorage localFileConfigurationStorage(
            @Named("local") ConfigurationTreeGenerator generator
    ) {
        return new LocalFileConfigurationStorage(
                seedParams.nodeName(),
                seedParams.configPath(),
                generator,
                modules.local()
        );
    }

    /** Creates the validator for local configuration. */
    @Singleton
    @Named("local")
    public ConfigurationValidator localConfigurationValidator(
            @Named("local") ConfigurationTreeGenerator generator
    ) {
        return ConfigurationValidatorImpl.withDefaultValidators(generator, modules.local().validators());
    }

    /** Creates the local (node) configuration registry. */
    @Singleton
    @Named("nodeConfig")
    public ConfigurationRegistry nodeConfigRegistry(
            LocalFileConfigurationStorage storage,
            @Named("local") ConfigurationTreeGenerator generator,
            @Named("local") ConfigurationValidator validator
    ) {
        return ConfigurationRegistry.create(modules.local(), storage, generator, validator);
    }
}
