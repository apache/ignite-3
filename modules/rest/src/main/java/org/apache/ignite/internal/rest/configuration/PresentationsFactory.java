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

package org.apache.ignite.internal.rest.configuration;

import io.micronaut.context.annotation.Bean;
import io.micronaut.context.annotation.Factory;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import org.apache.ignite.internal.configuration.ConfigurationRegistry;
import org.apache.ignite.internal.configuration.presentation.ConfigurationPresentation;
import org.apache.ignite.internal.configuration.presentation.HoconPresentation;
import org.apache.ignite.internal.rest.RestFactory;

/**
 * Factory that defines beans required for the rest module.
 */
@Factory
public class PresentationsFactory implements RestFactory {
    private ConfigurationPresentation<String> nodeCfgPresentation;
    private ConfigurationPresentation<String> clusterCfgPresentation;

    public PresentationsFactory(ConfigurationRegistry nodeConfigRegistry, ConfigurationRegistry clusterConfigRegistry) {
        this.nodeCfgPresentation = new HoconPresentation(nodeConfigRegistry);
        this.clusterCfgPresentation = new HoconPresentation(clusterConfigRegistry);
    }

    @Bean
    @Singleton
    @Named("clusterCfgPresentation")
    public ConfigurationPresentation<String> clusterCfgPresentation() {
        return clusterCfgPresentation;
    }

    @Bean
    @Singleton
    @Named("nodeCfgPresentation")
    public ConfigurationPresentation<String> nodeCfgPresentation() {
        return nodeCfgPresentation;
    }

    @Override
    public void cleanResources() {
        nodeCfgPresentation = null;
        clusterCfgPresentation = null;
    }
}
