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
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.ignite.configuration.internal.DynamicConfiguration;

/** */
public class SystemConfiguration {
    /** */
    private Map<String, Configurator<?>> configs = new HashMap<>();

    /** */
    private Map<String, Function<String, Object>> selectorsMap = new HashMap<>();

    /** */
    public <T extends DynamicConfiguration<?, ?, ?>> void registerConfigurator(Configurator<T> unitConfig, Function<String, Object> selectors) {
        String key = unitConfig.getRoot().key();

        configs.put(key, unitConfig);

        selectorsMap.put(key, selectors);
    }

    /** */
    public <T extends DynamicConfiguration<?, ?, ?>> Configurator<T> getConfigurator(String key) {
        return (Configurator<T>) configs.get(key);
    }

    /** */
    public Object getConfigurationProperty(String propertyPath) {
        String root = propertyPath.contains(".") ? propertyPath.substring(0, propertyPath.indexOf('.')) :
            propertyPath;

        return selectorsMap.get(root).apply(propertyPath);
    }

    /** */
    public Map<String, Object> getAllConfigurators() {
        return configs.entrySet().stream().collect(Collectors.toMap(
            e -> e.getKey(),
            e -> e.getValue().getRoot().value()
        ));
    }
}
