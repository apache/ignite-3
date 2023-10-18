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

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigRenderOptions;
import java.util.List;
import org.apache.ignite.configuration.ConfigurationDynamicDefaultsPatcher;
import org.apache.ignite.configuration.ConfigurationModule;
import org.apache.ignite.configuration.SuperRootChange;
import org.apache.ignite.configuration.validation.ConfigurationValidationException;
import org.apache.ignite.internal.configuration.hocon.HoconConverter;
import org.apache.ignite.internal.configuration.tree.ConfigurationSource;
import org.apache.ignite.internal.configuration.tree.ConverterToMapVisitor;

/**
 * Implementation of {@link ConfigurationDynamicDefaultsPatcher}.
 */
public class ConfigurationDynamicDefaultsPatcherImpl implements ConfigurationDynamicDefaultsPatcher {
    /**
     * Configuration module.
     */
    private final ConfigurationModule configurationModule;

    /**
     * Configuration tree generator.
     */
    private final ConfigurationTreeGenerator generator;


    public ConfigurationDynamicDefaultsPatcherImpl(
            ConfigurationModule configurationModule,
            ConfigurationTreeGenerator generator
    ) {
        this.configurationModule = configurationModule;
        this.generator = generator;
    }

    @Override
    public String patchWithDynamicDefaults(String hocon) {
        SuperRoot superRoot = convertToSuperRoot(hocon);

        SuperRootChange rootChange = new SuperRootChangeImpl(superRoot);

        configurationModule.patchConfigurationWithDynamicDefaults(rootChange);

        ConverterToMapVisitor visitor = ConverterToMapVisitor.builder()
                .includeInternal(true)
                .maskSecretValues(false)
                .skipEmptyValues(true)
                .build();

        ConfigRenderOptions renderOptions = ConfigRenderOptions.concise()
                .setJson(false);

        return HoconConverter.represent(superRoot, List.of(), visitor).render(renderOptions);
    }

    private SuperRoot convertToSuperRoot(String hocon) {
        try {
            Config config = ConfigFactory.parseString(hocon);
            ConfigurationSource hoconSource = HoconConverter.hoconSource(config.root());

            SuperRoot superRoot = generator.createSuperRoot();
            hoconSource.descend(superRoot);

            return superRoot;
        } catch (Exception e) {
            throw new ConfigurationValidationException("Failed to parse HOCON: " + e.getMessage());
        }
    }
}
