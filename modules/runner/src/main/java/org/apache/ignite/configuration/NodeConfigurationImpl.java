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

import static org.apache.ignite.configuration.ConfigurationBuilderUtil.createChanger;
import static org.apache.ignite.configuration.ConfigurationBuilderUtil.loadConfigurationModules;
import static org.apache.ignite.configuration.ConfigurationBuilderUtil.renderConfig;
import static org.apache.ignite.configuration.annotation.ConfigurationType.LOCAL;

import org.apache.ignite.failure.configuration.FailureProcessorConfigurationBuilder;
import org.apache.ignite.internal.configuration.ConfigurationChanger;
import org.apache.ignite.internal.configuration.ConfigurationModules;
import org.apache.ignite.internal.configuration.ConfigurationTreeGenerator;
import org.apache.ignite.internal.failure.configuration.FailureProcessorConfiguration;
import org.apache.ignite.internal.failure.configuration.FailureProcessorConfigurationBuilderImpl;
import org.jetbrains.annotations.Nullable;

public class NodeConfigurationImpl implements NodeConfiguration {
    private FailureProcessorConfigurationBuilderImpl failureHandler;

    @Override
    public FailureProcessorConfigurationBuilder withFailureHandler() {
        FailureProcessorConfigurationBuilderImpl builder = new FailureProcessorConfigurationBuilderImpl();
        failureHandler = builder;
        return builder;
    }

    public String build(@Nullable ClassLoader serviceLoaderClassLoader) {
        ConfigurationModules modules = loadConfigurationModules(serviceLoaderClassLoader);
        ConfigurationTreeGenerator configurationGenerator = new ConfigurationTreeGenerator(
                modules.local().rootKeys(),
                modules.local().schemaExtensions(),
                modules.local().polymorphicSchemaExtensions()
        );

        ConfigurationChanger changer = createChanger(LOCAL, configurationGenerator, modules.local().rootKeys());
        changer.start();
        changer.onDefaultsPersisted().join();


        FailureProcessorConfiguration failureProcessorConfiguration = (FailureProcessorConfiguration) configurationGenerator.instantiateCfg(
                FailureProcessorConfiguration.KEY, changer);

        if (failureHandler != null) {
            failureProcessorConfiguration.change(failureHandler::change).join();
        }

        String rendered = renderConfig(changer);
        changer.stop();
        return rendered;
    }
}
