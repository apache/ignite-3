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

import static java.util.stream.Collectors.toUnmodifiableList;

import java.lang.annotation.Annotation;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import org.apache.ignite.configuration.ConfigurationModule;
import org.apache.ignite.configuration.RootKey;
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

    @Override
    public ConfigurationType type() {
        return type;
    }

    @Override
    public Collection<RootKey<?, ?>> rootKeys() {
        return unionFromModulesExtractedWith(ConfigurationModule::rootKeys);
    }

    private <T> List<T> unionFromModulesExtractedWith(
            Function<? super ConfigurationModule, ? extends Collection<T>> extractor) {
        return modules.stream()
                .flatMap(module -> extractor.apply(module).stream())
                .collect(toUnmodifiableList());
    }

    @Override
    public Map<Class<? extends Annotation>, Set<Validator<? extends Annotation, ?>>> validators() {
        Map<Class<? extends Annotation>, Set<Validator<? extends Annotation, ?>>> result = new HashMap<>();

        var validatorsMaps = modules.stream().map(ConfigurationModule::validators);
        validatorsMaps.forEach(validatorsMap -> {
            validatorsMap.forEach((key, validatorsSet) -> {
                var runningUnionUnderKey = result.computeIfAbsent(key, ignored -> new HashSet<>());
                runningUnionUnderKey.addAll(validatorsSet);
            });
        });

        return Map.copyOf(result);
    }

    @Override
    public Collection<Class<?>> internalSchemaExtensions() {
        return unionFromModulesExtractedWith(ConfigurationModule::internalSchemaExtensions);
    }

    @Override
    public Collection<Class<?>> polymorphicSchemaExtensions() {
        return unionFromModulesExtractedWith(ConfigurationModule::polymorphicSchemaExtensions);
    }
}
