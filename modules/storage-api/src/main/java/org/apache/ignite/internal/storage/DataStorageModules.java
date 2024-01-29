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

package org.apache.ignite.internal.storage;

import static java.util.function.Predicate.not;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toUnmodifiableMap;

import java.lang.reflect.Field;
import java.nio.file.Path;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.ignite.configuration.annotation.PolymorphicConfigInstance;
import org.apache.ignite.configuration.annotation.Value;
import org.apache.ignite.internal.components.LongJvmPauseDetector;
import org.apache.ignite.internal.configuration.ConfigurationRegistry;
import org.apache.ignite.internal.schema.configuration.storage.DataStorageConfigurationSchema;
import org.apache.ignite.internal.storage.configurations.StorageEngineView;
import org.apache.ignite.internal.storage.configurations.StoragesConfiguration;
import org.apache.ignite.internal.storage.engine.StorageEngine;
import org.jetbrains.annotations.Nullable;

/**
 * Auxiliary class for working with {@link DataStorageModule}.
 */
public class DataStorageModules {
    /** Mapping: {@link DataStorageModule#name} -> DataStorageModule. */
    private final Map<String, DataStorageModule> modules;

    /**
     * Constructor.
     *
     * <p>Modules are expected to have a unique {@link DataStorageModule#name}
     * equal to {@link DataStorageConfigurationSchema#name schema name}.
     *
     * @param dataStorageModules Data storage modules.
     * @throws IllegalStateException If the module is not correct.
     */
    public DataStorageModules(Iterable<DataStorageModule> dataStorageModules) {
        Map<String, DataStorageModule> modules = new HashMap<>();

        for (DataStorageModule module : dataStorageModules) {
            String name = module.name();

            if (modules.containsKey(name)) {
                throw new IllegalStateException(String.format(
                        "Duplicate name [name=%s, factories=%s]",
                        name,
                        List.of(modules.get(name), module)
                ));
            }

            modules.put(name, module);
        }

        assert !modules.isEmpty();

        this.modules = modules;
    }

    /**
     * Creates new storage engines unique by {@link DataStorageModule#name name}.
     *
     * @param igniteInstanceName String igniteInstanceName
     * @param configRegistry Configuration register.
     * @param storagePath Storage path.
     * @param longJvmPauseDetector Long JVM pause detector.
     * @throws StorageException If there is an error when creating the storage engines.
     */
    public Map<String, StorageEngine> createStorageEngines(
            String igniteInstanceName,
            ConfigurationRegistry configRegistry,
            Path storagePath,
            @Nullable LongJvmPauseDetector longJvmPauseDetector
    ) {
        return modules.entrySet().stream().collect(toUnmodifiableMap(
                Entry::getKey,
                e -> e.getValue().createEngine(igniteInstanceName, configRegistry, storagePath, longJvmPauseDetector)
        ));
    }

    /**
     * Collects {@link DataStorageConfigurationSchema data storage schema} fields (with {@link Value}).
     *
     * @param polymorphicSchemaExtensions {@link PolymorphicConfigInstance Polymorphic schema extensions} that contain extensions of {@link
     *      DataStorageConfigurationSchema}.
     * @return Mapping: {@link DataStorageModule#name Data storage name} -> filed name -> field type.
     * @throws IllegalStateException If the {@link DataStorageConfigurationSchema data storage schemas} are not valid.
     */
    public Map<String, Map<String, Class<?>>> collectSchemasFields(Collection<Class<?>> polymorphicSchemaExtensions) {
        Map<String, Class<? extends DataStorageConfigurationSchema>> schemas = polymorphicSchemaExtensions.stream()
                .filter(DataStorageConfigurationSchema.class::isAssignableFrom)
                .collect(toUnmodifiableMap(
                        schemaCls -> schemaName((Class<? extends DataStorageConfigurationSchema>) schemaCls),
                        schemaCls -> (Class<? extends DataStorageConfigurationSchema>) schemaCls
                ));

        checkSchemas(modules, schemas);

        return modules.entrySet().stream().collect(toUnmodifiableMap(
                Entry::getKey,
                e -> schemaValueFields(schemas.get(e.getKey()))
        ));
    }

    private Map<String, Class<?>> schemaValueFields(Class<? extends DataStorageConfigurationSchema> dataStorageSchema) {
        return Stream.of(dataStorageSchema.getDeclaredFields())
                .filter(field -> field.isAnnotationPresent(Value.class))
                .collect(toUnmodifiableMap(
                        Field::getName,
                        Field::getType
                ));
    }

    private static String schemaName(Class<? extends DataStorageConfigurationSchema> dataStorageSchema) {
        PolymorphicConfigInstance polymorphicConfigInstance = dataStorageSchema.getAnnotation(PolymorphicConfigInstance.class);

        assert polymorphicConfigInstance != null : dataStorageSchema;

        return polymorphicConfigInstance.value();
    }

    private static void checkSchemas(
            Map<String, DataStorageModule> modules,
            Map<String, Class<? extends DataStorageConfigurationSchema>> schemas
    ) {
        if (!modules.keySet().equals(schemas.keySet())) {
            List<String> dataStorageWithoutSchema = modules.keySet().stream()
                    .filter(not(schemas::containsKey))
                    .collect(toList());

            if (!dataStorageWithoutSchema.isEmpty()) {
                throw new IllegalStateException(
                        "Missing configuration schemas (DataStorageConfigurationSchema heir) for data storage engines: "
                                + dataStorageWithoutSchema
                );
            }

            List<Class<? extends DataStorageConfigurationSchema>> schemasWithoutDataStorages = schemas.entrySet().stream()
                    .filter(e -> !modules.containsKey(e.getKey()))
                    .map(Entry::getValue)
                    .collect(toList());

            if (!schemasWithoutDataStorages.isEmpty()) {
                throw new IllegalStateException(
                        "Missing data storage engines for schemas: " + schemasWithoutDataStorages
                );
            }
        }
    }
}
