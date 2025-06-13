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

package org.apache.ignite.migrationtools.config;

import java.nio.file.Path;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.ignite.migrationtools.config.registry.CombinedConfigRegistry;
import org.apache.ignite.migrationtools.config.storage.NoDefaultsStorageConfiguration;
import org.apache.ignite3.configuration.ConfigurationModule;
import org.apache.ignite3.configuration.RootKey;
import org.apache.ignite3.configuration.annotation.ConfigurationType;
import org.apache.ignite3.configuration.validation.Validator;
import org.apache.ignite3.internal.configuration.ConfigurationManager;
import org.apache.ignite3.internal.configuration.ConfigurationModules;
import org.apache.ignite3.internal.configuration.ConfigurationRegistry;
import org.apache.ignite3.internal.configuration.ConfigurationTreeGenerator;
import org.apache.ignite3.internal.configuration.ServiceLoaderModulesProvider;
import org.apache.ignite3.internal.configuration.storage.LocalFileConfigurationStorage;
import org.apache.ignite3.internal.configuration.validation.ConfigurationValidator;
import org.apache.ignite3.internal.configuration.validation.ConfigurationValidatorImpl;
import org.apache.ignite3.internal.manager.ComponentContext;
import org.apache.ignite3.internal.security.authentication.validator.AuthenticationProvidersValidatorImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility methods for loading Ignite 3 Configuration Modules.
 * TODO: This class was heavily adapted from the Ignite Runner.
 */
public class Ignite3ConfigurationUtils {
    private static final Logger LOGGER = LoggerFactory.getLogger(Ignite3ConfigurationUtils.class);

    /**
     * Loads a special combined registry with modules from both the Cluster and Node modules.
     *
     * @param nodeCfgPath Node configuration path.
     * @param clusterCfgPath Cluster configuration path.
     * @param includeDefaults Include defaults.
     */
    public static CombinedConfigRegistry loadCombinedRegistry(Path nodeCfgPath, Path clusterCfgPath, boolean includeDefaults) {
        var locReg = loadNodeConfiguration(nodeCfgPath, includeDefaults);
        var distReg = loadClusterConfiguration(clusterCfgPath, includeDefaults);
        return new CombinedConfigRegistry(locReg, distReg);
    }

    /**
     * Loads a Configuration Registry with only Node modules.
     *
     * @param cfgPath Configuration path.
     * @param includeDefaults Include defaults.
     */
    public static ConfigurationRegistry loadNodeConfiguration(Path cfgPath, boolean includeDefaults) {
        return loadConfigurations(cfgPath, loadConfigurationModules().local(), includeDefaults);
    }

    /**
     * Loads a Configuration Registry with only Cluster modules.
     *
     * @param cfgPath Config path.
     * @param includeDefaults Include defaults.
     */
    public static ConfigurationRegistry loadClusterConfiguration(Path cfgPath, boolean includeDefaults) {
        // Hack so that it passes the validation
        // TODO: This is another hack that needs to be cleaned. We don't really need the ConfigurationRegistry.
        var distributedModule = loadConfigurationModules().distributed();
        for (RootKey<?, ?, ?> key : distributedModule.rootKeys()) {
            try {
                FieldUtils.writeDeclaredField(key, "storageType", ConfigurationType.LOCAL, true);
            } catch (IllegalAccessException e) {
                LOGGER.error("Could not override configuration type on key: {}", key);
            }
        }

        return loadConfigurations(cfgPath, distributedModule, includeDefaults);
    }

    /**
     * Sets up a configuration registry.
     *
     * @param cfgPath Config path.
     * @param module Module.
     * @param includeDefaults Include defaults.
     */
    private static ConfigurationRegistry loadConfigurations(Path cfgPath, ConfigurationModule module, boolean includeDefaults) {
        ConfigurationTreeGenerator localConfigurationGenerator =
                new ConfigurationTreeGenerator(module.rootKeys(), module.schemaExtensions(), module.polymorphicSchemaExtensions());

        LocalFileConfigurationStorage localFileConfigurationStorage = (includeDefaults)
                ? new LocalFileConfigurationStorage(cfgPath, localConfigurationGenerator, module)
                : new NoDefaultsStorageConfiguration(cfgPath, localConfigurationGenerator, module);

        // Remove the authentication validator because I cannot get the module.patchConfigurationWithDynamicDefaults(change); to work.
        // TODO: Check if this will create an error on the service.
        Set<? extends Validator<?, ?>> myValidators = module.validators()
                .stream()
                .filter(v -> !(v instanceof AuthenticationProvidersValidatorImpl))
                .collect(Collectors.toSet());

        ConfigurationValidator localConfigurationValidator =
                ConfigurationValidatorImpl.withDefaultValidators(localConfigurationGenerator, myValidators);

        var nodeCfgMgr = new ConfigurationManager(module.rootKeys(), localFileConfigurationStorage, localConfigurationGenerator,
                localConfigurationValidator, c -> {}, s -> false);

        nodeCfgMgr.startAsync(new ComponentContext()).join();
        return nodeCfgMgr.configurationRegistry();
    }

    /**
     * Loads all the configuration modules in the main classloader.
     */
    private static ConfigurationModules loadConfigurationModules() {
        // TODO: Copied
        var modulesProvider = new ServiceLoaderModulesProvider();
        List<ConfigurationModule> modules = modulesProvider.modules(null);

        if (modules.isEmpty()) {
            throw new IllegalStateException("No configuration modules were loaded, this means Ignite cannot start. "
                    + "Please make sure that the classloader for loading services is correct.");
        }

        return new ConfigurationModules(modules);
    }
}
