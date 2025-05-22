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

import java.util.Collection;
import java.util.List;
import org.apache.ignite.configuration.ConfigurationModule;
import org.apache.ignite.configuration.RootKey;
import org.apache.ignite.configuration.SuperRootChange;
import org.apache.ignite.configuration.annotation.ConfigurationType;

/**
 * Test configuration module.
 */
public class TestConfigurationModule implements ConfigurationModule {
    @Override
    public ConfigurationType type() {
        return ConfigurationType.DISTRIBUTED;
    }

    @Override
    public Collection<RootKey<?, ?, ?>> rootKeys() {
        return List.of(TestConfiguration.KEY);
    }

    @Override
    public Collection<Class<?>> polymorphicSchemaExtensions() {
        return List.of();
    }

    @Override
    public void patchConfigurationWithDynamicDefaults(SuperRootChange rootChange) {
        rootChange.changeRoot(TestConfiguration.KEY)
                .changeTestSubConfiguration()
                .changeTestSecret("secret")
                .changeTestInt(42);
    }
}
