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

import static org.apache.ignite.configuration.annotation.ConfigurationType.LOCAL;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;

import com.typesafe.config.ConfigRenderOptions;
import java.util.List;
import java.util.Set;
import org.apache.ignite.configuration.validation.Validator;
import org.apache.ignite.failure.configuration.FailureProcessorConfigurationBuilder;
import org.apache.ignite.internal.configuration.ConfigurationChanger;
import org.apache.ignite.internal.configuration.ConfigurationChanger.ConfigurationUpdateListener;
import org.apache.ignite.internal.configuration.ConfigurationTreeGenerator;
import org.apache.ignite.internal.configuration.hocon.HoconConverter;
import org.apache.ignite.internal.configuration.storage.InMemoryConfigurationStorage;
import org.apache.ignite.internal.configuration.tree.ConverterToMapVisitor;
import org.apache.ignite.internal.configuration.tree.InnerNode;
import org.apache.ignite.internal.configuration.validation.ConfigurationValidator;
import org.apache.ignite.internal.configuration.validation.ConfigurationValidatorImpl;
import org.apache.ignite.internal.failure.configuration.FailureProcessorConfiguration;
import org.apache.ignite.internal.failure.configuration.FailureProcessorConfigurationBuilderImpl;
import org.apache.ignite.internal.failure.handlers.configuration.IgnoredFailureTypesValidator;
import org.apache.ignite.internal.failure.handlers.configuration.NoOpFailureHandlerConfigurationSchema;
import org.apache.ignite.internal.failure.handlers.configuration.StopNodeFailureHandlerConfigurationSchema;
import org.apache.ignite.internal.failure.handlers.configuration.StopNodeOrHaltFailureHandlerConfigurationSchema;

public class NodeConfigurationImpl implements NodeConfiguration {
    private static final List<RootKey<?, ?>> rootKeys = List.of(FailureProcessorConfiguration.KEY);

    private static final List<Class<?>> schemaExtensions = List.of();

    private static final List<Class<?>> polymorphicSchemaExtensions = List.of(
            NoOpFailureHandlerConfigurationSchema.class,
            StopNodeFailureHandlerConfigurationSchema.class,
            StopNodeOrHaltFailureHandlerConfigurationSchema.class
    );

    private static final Set<Validator<?, ?>> validators = Set.of(IgnoredFailureTypesValidator.INSTANCE);

    private FailureProcessorConfigurationBuilderImpl failureHandler;

    @Override
    public FailureProcessorConfigurationBuilder withFailureHandler() {
        FailureProcessorConfigurationBuilderImpl builder = new FailureProcessorConfigurationBuilderImpl();
        failureHandler = builder;
        return builder;
    }

    public String build() {
        ConfigurationTreeGenerator configurationGenerator = new ConfigurationTreeGenerator(
                rootKeys, schemaExtensions, polymorphicSchemaExtensions);

        ConfigurationChanger changer = createChanger(configurationGenerator);
        changer.start();


        FailureProcessorConfiguration failureProcessorConfiguration = (FailureProcessorConfiguration) configurationGenerator.instantiateCfg(
                FailureProcessorConfiguration.KEY, changer);

        if (failureHandler != null) {
            failureHandler.buildToConfiguration(failureProcessorConfiguration);
        }


        ConverterToMapVisitor visitor = ConverterToMapVisitor.builder()
                .includeInternal(false)
                .skipEmptyValues(true)
                .maskSecretValues(false)
                .build();

        String rendered = HoconConverter.represent(changer.superRoot().copy(), List.of(), visitor)
                .render(ConfigRenderOptions.concise().setFormatted(true).setJson(false));

        changer.stop();
        return rendered;
    }

    private static ConfigurationChanger createChanger(ConfigurationTreeGenerator configurationGenerator) {
        InMemoryConfigurationStorage storage = new InMemoryConfigurationStorage(LOCAL);

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
}
