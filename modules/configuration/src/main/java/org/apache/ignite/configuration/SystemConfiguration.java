/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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

import java.util.HashMap;
import java.util.Map;
import org.apache.ignite.configuration.internal.DynamicConfiguration;
import org.apache.ignite.configuration.presentation.json.JsonPresentation;

/** */
public class SystemConfiguration {
    /** */
    private final Map<String, Configurator<?>> configs = new HashMap<>();

    /** */
    private final Map<String, ConfigurationTree<?, ?>> multirootConfiguration = new HashMap<>();

    /** */
    private final ConfigurationPresentation<String> presentation = new JsonPresentation(configs);

    /** */
    public <T extends DynamicConfiguration<?, ?, ?>> void registerConfigurator(Configurator<T> unitConfig) {
        String key = unitConfig.getRoot().key();

        configs.put(key, unitConfig);

        multirootConfiguration.put(key, unitConfig.getRoot());
    }

    /** */
    public <V, C, T extends ConfigurationTree<V, C>> T getConfiguration(RootKey<T> rootKey) {
        return (T) multirootConfiguration.get(rootKey.key());
    }

    /** */
    public ConfigurationPresentation<String> presentation() {
        return presentation;
    }
}
