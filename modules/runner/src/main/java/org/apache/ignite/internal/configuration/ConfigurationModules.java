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

package org.apache.ignite.internal.configuration;

import java.util.List;
import java.util.stream.Collectors;
import org.apache.ignite.configuration.annotation.ConfigurationType;
import org.apache.ignite.configuration.validation.ConfigurationModule;

/**
 *
 */
public class ConfigurationModules {
    private final List<ConfigurationModule> modules;

    public ConfigurationModules(List<ConfigurationModule> modules) {
        this.modules = List.copyOf(modules);
    }

    public ConfigurationModule local() {
        return compoundOfType(ConfigurationType.LOCAL);
    }

    public ConfigurationModule distributed() {
        return compoundOfType(ConfigurationType.DISTRIBUTED);
    }

    private CompoundModule compoundOfType(ConfigurationType configurationType) {
        List<ConfigurationModule> modulesOfType = modules.stream()
                .filter(module -> module.type() == configurationType)
                .collect(Collectors.toUnmodifiableList());
        return new CompoundModule(configurationType, modulesOfType);
    }
}
