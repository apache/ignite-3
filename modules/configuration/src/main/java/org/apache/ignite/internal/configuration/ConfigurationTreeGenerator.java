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

import static java.util.function.Function.identity;
import static java.util.function.Predicate.not;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static org.apache.ignite.internal.configuration.util.ConfigurationUtil.collectSchemas;
import static org.apache.ignite.internal.configuration.util.ConfigurationUtil.isPolymorphicId;
import static org.apache.ignite.internal.configuration.util.ConfigurationUtil.mapIterable;
import static org.apache.ignite.internal.configuration.util.ConfigurationUtil.polymorphicInstanceId;
import static org.apache.ignite.internal.configuration.util.ConfigurationUtil.polymorphicSchemaExtensions;
import static org.apache.ignite.internal.configuration.util.ConfigurationUtil.schemaExtensions;
import static org.apache.ignite.internal.configuration.util.ConfigurationUtil.schemaFields;
import static org.apache.ignite.internal.util.CollectionUtils.difference;

import io.micronaut.core.annotation.Creator;
import jakarta.inject.Singleton;
import java.lang.reflect.Field;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import org.apache.ignite.configuration.RootKey;
import org.apache.ignite.configuration.annotation.Config;
import org.apache.ignite.configuration.annotation.ConfigurationExtension;
import org.apache.ignite.configuration.annotation.ConfigurationRoot;
import org.apache.ignite.configuration.annotation.PolymorphicConfigInstance;
import org.apache.ignite.configuration.annotation.PolymorphicId;
import org.apache.ignite.internal.close.ManuallyCloseable;
import org.apache.ignite.internal.configuration.asm.ConfigurationAsmGenerator;
import org.apache.ignite.internal.configuration.tree.InnerNode;
import org.apache.ignite.internal.configuration.util.ConfigurationUtil;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

/** Schema-aware configuration generator. */
@Singleton
public class ConfigurationTreeGenerator implements ManuallyCloseable {

    private final Map<String, RootKey<?, ?, ?>> rootKeys;

    @Nullable
    private final ConfigurationAsmGenerator generator = new ConfigurationAsmGenerator();

    /**
     * Constructor that takes a collection of root keys. Internal and polymorphic schema extensions are empty by default.
     *
     * @param rootKeys Root keys.
     */
    @TestOnly
    public ConfigurationTreeGenerator(RootKey<?, ?, ?>... rootKeys) {
        this(List.of(rootKeys), Set.of(), Set.of());
    }

    /**
     * Constructor that takes a collection of root keys and a collection of internal schema extensions.
     *
     * @param rootKeys Root keys.
     * @param schemaExtensions Schema extensions.
     * @param polymorphicSchemaExtensions Polymorphic schema extensions.
     */
    public ConfigurationTreeGenerator(
            Collection<RootKey<?, ?, ?>> rootKeys,
            Collection<Class<?>> schemaExtensions,
            Collection<Class<?>> polymorphicSchemaExtensions) {

        this.rootKeys = rootKeys.stream().collect(toMap(RootKey::key, identity()));

        Set<Class<?>> allSchemas = collectAllSchemas(rootKeys, schemaExtensions, polymorphicSchemaExtensions);

        Map<Class<?>, Set<Class<?>>> extensions = extensionsWithCheck(allSchemas, schemaExtensions);
        Map<Class<?>, Set<Class<?>>> polymorphicExtensions = polymorphicExtensionsWithCheck(allSchemas, polymorphicSchemaExtensions);

        rootKeys.forEach(key -> generator.compileRootSchema(key.schemaClass(), extensions, polymorphicExtensions));
    }

    /**
     * Creates a new instance of {@link ConfigurationTreeGenerator} using the provided {@link ConfigurationModules}.
     *
     * @param modules The configuration modules from which root keys, schema extensions,
     *                and polymorphic schema extensions are retrieved to initialize the generator.
     * @return A new instance of {@link ConfigurationTreeGenerator}.
     */
    @Creator
    public static ConfigurationTreeGenerator instance(ConfigurationModules modules) {
        return new ConfigurationTreeGenerator(
                modules.local().rootKeys(),
                modules.local().schemaExtensions(),
                modules.local().polymorphicSchemaExtensions()
        );
    }

    /**
     * Creates a new instance of {@link SuperRoot} with all the roots created.
     *
     * @return New instance of {@link SuperRoot}.
     */
    public synchronized SuperRoot createSuperRoot() {
        assert generator != null : "ConfigurationTreeGenerator is already closed";

        SuperRoot superRoot = new SuperRoot(rootCreator());
        for (RootKey<?, ?, ?> rootKey : rootKeys.values()) {
            superRoot.addRoot(rootKey, createRootNode(rootKey));
        }

        return superRoot;
    }

    /**
     * Creates new instance of {@code *Configuration} class corresponding to the given Configuration Schema.
     *
     * @param rootKey Root key of the configuration root.
     * @param changer Configuration changer instance to pass into constructor.
     * @return Configuration instance.
     */
    public synchronized DynamicConfiguration<?, ?> instantiateCfg(RootKey<?, ?, ?> rootKey, DynamicConfigurationChanger changer) {
        assert generator != null : "ConfigurationTreeGenerator is already closed";

        return generator.instantiateCfg(rootKey, changer);
    }

    /**
     * Creates new instance of {@code *Node} class corresponding to the given Configuration Schema class.
     *
     * @param schemaClass Configuration Schema class.
     * @return Node instance.
     */
    public synchronized InnerNode instantiateNode(Class<?> schemaClass) {
        assert generator != null : "ConfigurationTreeGenerator is already closed";

        return generator.instantiateNode(schemaClass);
    }

    /**
     * Creates new instance of root {@code *Node} class corresponding to the given root key.
     */
    public synchronized InnerNode createRootNode(RootKey<?, ?, ?> rootKey) {
        return instantiateNode(rootKey.schemaClass());
    }

    private Function<String, RootInnerNode> rootCreator() {
        return key -> {
            RootKey<?, ?, ?> rootKey = rootKeys.get(key);

            return rootKey == null ? null : new RootInnerNode(rootKey, createRootNode(rootKey));
        };
    }

    @Override
    public synchronized void close() {
    }

    /**
     * Collects all schemas and subschemas (recursively) from root keys, internal and polymorphic schema extensions.
     *
     * @param rootKeys root keys
     * @param internalSchemaExtensions internal schema extensions
     * @param polymorphicSchemaExtensions polymorphic schema extensions
     * @return set of all schema classes
     */
    private static Set<Class<?>> collectAllSchemas(
            Collection<RootKey<?, ?, ?>> rootKeys,
            Collection<Class<?>> internalSchemaExtensions,
            Collection<Class<?>> polymorphicSchemaExtensions
    ) {
        Set<Class<?>> allSchemas = new HashSet<>();

        allSchemas.addAll(collectSchemas(mapIterable(rootKeys, RootKey::schemaClass)));
        allSchemas.addAll(collectSchemas(internalSchemaExtensions));
        allSchemas.addAll(collectSchemas(polymorphicSchemaExtensions));

        return allSchemas;
    }

    /**
     * Get configuration schemas and their validated internal extensions with checks.
     *
     * @param allSchemas All configuration schemas.
     * @param schemaExtensions Extensions ({@link ConfigurationExtension}) of configuration schemas
     *         ({@link ConfigurationRoot} and {@link Config}).
     * @return Mapping: original of the schema -> internal schema extensions.
     * @throws IllegalArgumentException If the schema extension is invalid.
     */
    private static Map<Class<?>, Set<Class<?>>> extensionsWithCheck(
            Set<Class<?>> allSchemas,
            Collection<Class<?>> schemaExtensions
    ) {
        if (schemaExtensions.isEmpty()) {
            return Map.of();
        }

        Map<Class<?>, Set<Class<?>>> extensions = schemaExtensions(schemaExtensions);

        Set<Class<?>> notInAllSchemas = difference(extensions.keySet(), allSchemas);

        if (!notInAllSchemas.isEmpty()) {
            throw new IllegalArgumentException(
                    "Extensions for which no parent configuration schemas were found: " + notInAllSchemas
            );
        }

        return extensions;
    }

    /**
     * Get polymorphic extensions of configuration schemas with checks.
     *
     * @param allSchemas All configuration schemas.
     * @param polymorphicSchemaExtensions Polymorphic extensions ({@link PolymorphicConfigInstance}) of configuration schemas.
     * @return Mapping: polymorphic scheme -> extensions (instances) of polymorphic configuration.
     * @throws IllegalArgumentException If the schema extension is invalid.
     */
    private static Map<Class<?>, Set<Class<?>>> polymorphicExtensionsWithCheck(
            Set<Class<?>> allSchemas,
            Collection<Class<?>> polymorphicSchemaExtensions
    ) {
        Map<Class<?>, Set<Class<?>>> polymorphicExtensionsByParent = polymorphicSchemaExtensions(polymorphicSchemaExtensions);

        Set<Class<?>> notInAllSchemas = difference(polymorphicExtensionsByParent.keySet(), allSchemas);

        if (!notInAllSchemas.isEmpty()) {
            throw new IllegalArgumentException(
                    "Polymorphic extensions for which no polymorphic configuration schemas were found: " + notInAllSchemas
            );
        }

        Collection<Class<?>> noPolymorphicExtensionsSchemas = allSchemas.stream()
                .filter(ConfigurationUtil::isPolymorphicConfig)
                .filter(not(polymorphicExtensionsByParent::containsKey))
                .collect(toList());

        if (!noPolymorphicExtensionsSchemas.isEmpty()) {
            throw new IllegalArgumentException(
                    "Polymorphic configuration schemas for which no extensions were found: " + noPolymorphicExtensionsSchemas
            );
        }

        checkPolymorphicConfigIds(polymorphicExtensionsByParent);

        for (Map.Entry<Class<?>, Set<Class<?>>> e : polymorphicExtensionsByParent.entrySet()) {
            Class<?> schemaClass = e.getKey();

            Field typeIdField = schemaFields(schemaClass).get(0);

            if (!isPolymorphicId(typeIdField)) {
                throw new IllegalArgumentException(String.format(
                        "First field in a polymorphic configuration schema must contain @%s: %s",
                        PolymorphicId.class,
                        schemaClass.getName()
                ));
            }
        }

        return polymorphicExtensionsByParent;
    }

    /**
     * Checks that there are no conflicts between ids of a polymorphic configuration and its extensions (instances).
     *
     * @param polymorphicExtensions Mapping: polymorphic scheme -> extensions (instances) of polymorphic configuration.
     * @throws IllegalArgumentException If a polymorphic configuration id conflict is found.
     * @see PolymorphicConfigInstance#value
     */
    private static void checkPolymorphicConfigIds(Map<Class<?>, Set<Class<?>>> polymorphicExtensions) {
        // Mapping: id -> configuration schema.
        Map<String, Class<?>> ids = new HashMap<>();

        for (Map.Entry<Class<?>, Set<Class<?>>> e : polymorphicExtensions.entrySet()) {
            for (Class<?> schemaClass : e.getValue()) {
                String id = polymorphicInstanceId(schemaClass);
                Class<?> prev = ids.put(id, schemaClass);

                if (prev != null) {
                    throw new IllegalArgumentException("Found an id conflict for a polymorphic configuration [id="
                            + id + ", schemas=" + List.of(prev, schemaClass));
                }
            }

            ids.clear();
        }
    }
}
