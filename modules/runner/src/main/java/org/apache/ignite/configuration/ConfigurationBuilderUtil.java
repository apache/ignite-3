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

import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;

import com.typesafe.config.ConfigRenderOptions;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import org.apache.ignite.configuration.annotation.ConfigurationType;
import org.apache.ignite.configuration.validation.Validator;
import org.apache.ignite.internal.configuration.ConfigurationChanger;
import org.apache.ignite.internal.configuration.ConfigurationChanger.ConfigurationUpdateListener;
import org.apache.ignite.internal.configuration.ConfigurationModules;
import org.apache.ignite.internal.configuration.ConfigurationTreeGenerator;
import org.apache.ignite.internal.configuration.ServiceLoaderModulesProvider;
import org.apache.ignite.internal.configuration.hocon.HoconConverter;
import org.apache.ignite.internal.configuration.storage.ConfigurationStorage;
import org.apache.ignite.internal.configuration.storage.InMemoryConfigurationStorage;
import org.apache.ignite.internal.configuration.tree.ConverterToMapVisitor;
import org.apache.ignite.internal.configuration.tree.InnerNode;
import org.apache.ignite.internal.configuration.validation.ConfigurationValidator;
import org.apache.ignite.internal.configuration.validation.ConfigurationValidatorImpl;
import org.jetbrains.annotations.Nullable;

public class ConfigurationBuilderUtil {
    public static ConfigurationModules loadConfigurationModules(@Nullable ClassLoader classLoader) {
        var modulesProvider = new ServiceLoaderModulesProvider();
        List<ConfigurationModule> modules = modulesProvider.modules(classLoader);

        if (modules.isEmpty()) {
            throw new IllegalStateException("No configuration modules were loaded. "
                    + "Please make sure that the classloader for loading services is correct.");
        }

        return new ConfigurationModules(modules);
    }

     public static ConfigurationChanger createChanger(
             ConfigurationType configurationType,
             ConfigurationTreeGenerator configurationGenerator,
             Collection<RootKey<?, ?>> rootKeys
     ) {
        return createChanger(new InMemoryConfigurationStorage(configurationType), configurationGenerator, rootKeys, Set.of());
     }

     public static ConfigurationChanger createChanger(
             ConfigurationStorage storage,
             ConfigurationTreeGenerator configurationGenerator,
             Collection<RootKey<?, ?>> rootKeys,
             Set<Validator<?, ?>> validators
     ) {
        ConfigurationValidator configurationValidator =
                ConfigurationValidatorImpl.withDefaultValidators(configurationGenerator, validators);

        ConfigurationUpdateListener empty = (oldRoot, newRoot, storageRevision, notificationNumber) -> nullCompletedFuture();

        return new ConfigurationChanger(empty, rootKeys, storage, configurationValidator) {
            @Override
            public InnerNode createRootNode(RootKey<?, ?> rootKey) {
                return configurationGenerator.instantiateNode(rootKey.schemaClass());
            }
        };
    }

    static String renderConfig(ConfigurationChanger changer) {
        ConverterToMapVisitor visitor = ConverterToMapVisitor.builder()
                .includeInternal(false)
                .skipEmptyValues(true)
                .maskSecretValues(false)
                .build();

        return HoconConverter.represent(changer.superRoot().copy(), List.of(), visitor)
                .render(ConfigRenderOptions.concise().setFormatted(true).setJson(false));
    }
}
