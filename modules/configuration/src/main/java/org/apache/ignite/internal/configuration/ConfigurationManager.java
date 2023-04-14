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

import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigObject;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import org.apache.ignite.configuration.RootKey;
import org.apache.ignite.configuration.validation.Validator;
import org.apache.ignite.internal.configuration.hocon.HoconConverter;
import org.apache.ignite.internal.configuration.storage.ConfigurationStorage;
import org.apache.ignite.internal.manager.IgniteComponent;

/**
 * Configuration manager is responsible for handling configuration lifecycle and provides configuration API.
 */
public class ConfigurationManager implements IgniteComponent {
    /** Configuration registry. */
    private final ConfigurationRegistry registry;

    /** Runtime implementations generator for node classes. */
    private final ConfigurationTreeGenerator generator;

    /**
     * Constructor.
     *
     * @param rootKeys                    Configuration root keys.
     * @param validators                  Validators.
     * @param storage                     Configuration storage.
     * @throws IllegalArgumentException If the configuration type of the root keys is not equal to the storage type, or if the schema or its
     *                                  extensions are not valid.
     */
    public ConfigurationManager(
            Collection<RootKey<?, ?>> rootKeys,
            Set<Validator<?, ?>> validators,
            ConfigurationStorage storage,
            ConfigurationTreeGenerator generator
    ) {
        // we don't need to close the generator here, because it's passed from the outside
        this.generator = null;
        this.registry = new ConfigurationRegistry(
                rootKeys,
                validators,
                storage,
                generator
        );
    }

    /**
     * Constructor.
     *
     * @param rootKeys                    Configuration root keys.
     * @param validators                  Validators.
     * @param storage                     Configuration storage.
     * @throws IllegalArgumentException If the configuration type of the root keys is not equal to the storage type, or if the schema or its
     *                                  extensions are not valid.
     */
    public ConfigurationManager(
            Collection<RootKey<?, ?>> rootKeys,
            Set<Validator<?, ?>> validators,
            ConfigurationStorage storage,
            Collection<Class<?>> internalSchemaExtensions,
            Collection<Class<?>> polymorphicSchemaExtensions
    ) {
        this.generator = new ConfigurationTreeGenerator(rootKeys, internalSchemaExtensions, polymorphicSchemaExtensions);
        this.registry = new ConfigurationRegistry(
                rootKeys,
                validators,
                storage,
                generator
        );
    }

    /** {@inheritDoc} */
    @Override
    public void start() {
        registry.start();
    }

    /** {@inheritDoc} */
    @Override
    public void stop() throws Exception {
        // TODO: IGNITE-15161 Implement component's stop.
        registry.stop();
        if (generator != null) {
            generator.close();
        }
    }

    /**
     * Bootstrap configuration manager with customer user cfg.
     *
     * @param configPath Customer configuration in hocon format.
     * @throws InterruptedException If thread is interrupted during bootstrap.
     * @throws ExecutionException   If configuration update failed for some reason.
     */
    public void bootstrap(Path configPath) throws InterruptedException, ExecutionException {
        ConfigObject hoconCfg = ConfigFactory.parseFile(configPath.toFile()).root();

        registry.change(HoconConverter.hoconSource(hoconCfg)).get();
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
