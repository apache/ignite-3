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

import static java.util.stream.Collectors.toUnmodifiableList;
import static java.util.stream.Collectors.toUnmodifiableSet;

import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import org.apache.ignite.configuration.ConfigurationModule;
import org.apache.ignite.configuration.RootKey;
import org.apache.ignite.configuration.SuperRootChange;
import org.apache.ignite.configuration.annotation.ConfigurationType;
import org.apache.ignite.configuration.validation.Validator;

/**
 * {@link ConfigurationModule} that merges a few {@code ConfigurationModule}s.
 */
public class CompoundModule implements ConfigurationModule {
    private final ConfigurationType type;
    private final List<ConfigurationModule> modules;

    public CompoundModule(ConfigurationType type, Collection<ConfigurationModule> modules) {
        this.type = type;
        this.modules = List.copyOf(modules);
    }

    /** {@inheritDoc} */
    @Override
    public ConfigurationType type() {
        return type;
    }

    /** {@inheritDoc} */
    @Override
    public Collection<RootKey<?, ?, ?>> rootKeys() {
        return unionFromModulesExtractedWith(ConfigurationModule::rootKeys);
    }

    private <T> List<T> unionFromModulesExtractedWith(Function<? super ConfigurationModule, ? extends Collection<T>> extractor) {
        return modules.stream()
                .flatMap(module -> extractor.apply(module).stream())
                .collect(toUnmodifiableList());
    }

    /** {@inheritDoc} */
    @Override
    public Set<Validator<?, ?>> validators() {
        return modules.stream()
                .flatMap(module -> module.validators().stream())
                .collect(toUnmodifiableSet());
    }

    /** {@inheritDoc} */
    @Override
    public Collection<Class<?>> schemaExtensions() {
        return unionFromModulesExtractedWith(ConfigurationModule::schemaExtensions);
    }

    /** {@inheritDoc} */
    @Override
    public Collection<Class<?>> polymorphicSchemaExtensions() {
        return unionFromModulesExtractedWith(ConfigurationModule::polymorphicSchemaExtensions);
    }

    @Override
    public void patchConfigurationWithDynamicDefaults(SuperRootChange rootChange) {
        modules.forEach(module -> module.patchConfigurationWithDynamicDefaults(rootChange));
    }

    @Override
    public void migrateDeprecatedConfigurations(SuperRootChange superRootChange) {
        modules.forEach(module -> module.migrateDeprecatedConfigurations(superRootChange));
    }

    @Override
    public Collection<String> deletedPrefixes() {
        return unionFromModulesExtractedWith(ConfigurationModule::deletedPrefixes);
    }
}
