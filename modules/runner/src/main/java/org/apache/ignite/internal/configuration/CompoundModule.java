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
import static java.util.stream.Collectors.toUnmodifiableSet;

import java.lang.annotation.Annotation;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Stream;
import org.apache.ignite.configuration.RootKey;
import org.apache.ignite.configuration.annotation.ConfigurationType;
import org.apache.ignite.configuration.validation.ConfigurationModule;
import org.apache.ignite.configuration.validation.Validator;

/**
 *
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
        return unionFromAllModules(ConfigurationModule::rootKeys);
    }

    private <T> List<T> unionFromAllModules(Function<? super ConfigurationModule, ? extends Collection<T>> extractor) {
        return modules.stream()
                .flatMap(module -> extractor.apply(module).stream())
                .collect(toUnmodifiableList());
    }

    @Override
    public Map<Class<? extends Annotation>, Set<Validator<? extends Annotation, ?>>> validators() {
        List<Map<Class<? extends Annotation>, Set<Validator<? extends Annotation, ?>>>> validatorMaps = modules.stream()
                .map(ConfigurationModule::validators)
                .collect(toUnmodifiableList());

        Set<Class<? extends Annotation>> allMapKeys = validatorMaps.stream()
                .flatMap(map -> map.keySet().stream())
                .collect(toUnmodifiableSet());

        Map<Class<? extends Annotation>, Set<Validator<? extends Annotation, ?>>> result = new HashMap<>();
        allMapKeys.forEach(key -> {
            Stream<Set<Validator<? extends Annotation, ?>>> setsUnderKey = validatorMaps.stream()
                    .map(map -> map.get(key))
                    .filter(Objects::nonNull);
            setsUnderKey.forEach(set -> {
                var runningUnionUnderKey = result.computeIfAbsent(key, ignored -> new HashSet<>());
                runningUnionUnderKey.addAll(set);
            });
        });

        return Map.copyOf(result);
    }

    @Override
    public Collection<Class<?>> internalSchemaExtensions() {
        return unionFromAllModules(ConfigurationModule::internalSchemaExtensions);
    }

    @Override
    public Collection<Class<?>> polymorphicSchemaExtensions() {
        return unionFromAllModules(ConfigurationModule::polymorphicSchemaExtensions);
    }
}
