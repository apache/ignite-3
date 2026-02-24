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

package org.apache.ignite.migrationtools.sql.sql;

import static java.util.Collections.emptyMap;
import static java.util.Map.entry;
import static org.apache.ignite.migrationtools.sql.SqlDdlGenerator.EXTRA_FIELDS_COLUMN_NAME;
import static org.apache.ignite.migrationtools.sql.sql.SqlDdlGeneratorTest.ColumnRecord.nonKey;
import static org.apache.ignite.migrationtools.sql.sql.SqlDdlGeneratorTest.ColumnRecord.primaryKey;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.groups.Tuple.tuple;
import static org.junit.jupiter.api.Named.named;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.DayOfWeek;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.BiFunction;
import java.util.function.UnaryOperator;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.collections4.IterableUtils;
import org.apache.commons.lang3.ClassUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.examples.model.Organization;
import org.apache.ignite.examples.model.Person;
import org.apache.ignite.migrationtools.sql.SqlDdlGenerator;
import org.apache.ignite.migrationtools.sql.SqlDdlGenerator.GenerateTableResult;
import org.apache.ignite.migrationtools.tablemanagement.TableTypeDescriptor;
import org.apache.ignite.migrationtools.tablemanagement.TableTypeRegistryMapImpl;
import org.apache.ignite.migrationtools.tests.models.ComplexKeyIntStr;
import org.apache.ignite.migrationtools.tests.models.InterceptingFieldsModel;
import org.apache.ignite.migrationtools.tests.models.SimplePojo;
import org.apache.ignite3.catalog.ColumnSorted;
import org.apache.ignite3.catalog.definitions.ColumnDefinition;
import org.apache.ignite3.catalog.definitions.TableDefinition;
import org.assertj.core.api.SoftAssertions;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.Named;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.FieldSource;
import org.junit.jupiter.params.provider.MethodSource;

class SqlDdlGeneratorTest {
    private static final List<Named<Boolean>> EXTRA_FIELDS_ENABLED_ARG = List.of(
            named("No extra fields support", false),
            named("With extra fields support", true)
    );

    private static final List<ColumnRecord> PERSON_EXPECTED_FIELDS = List.of(
            primaryKey("KEY", "INT"),
            nonKey("id", "BIGINT", true),
            nonKey("orgId", "BIGINT", true),
            nonKey("firstName", "VARCHAR", true),
            nonKey("lastName", "VARCHAR", true),
            nonKey("resume", "VARCHAR", true),
            nonKey("salary", "DOUBLE", false)
    );

    private static final QueryEntity POJO_WITH_PRIMITIVES_QE = new QueryEntity(Object.class.getName(), Object.class.getName())
            .setFields(
                    Stream.of(
                            entry("ID", long.class.getName()),
                            entry("NAME", String.class.getName()),
                            entry("AGE", int.class.getName()),
                            entry("SALARY", double.class.getName())
                    ).collect(linkedMapCollector())
            )
            .setKeyFields(new HashSet<>(Collections.singleton("ID")));

    private static final List<ColumnRecord> POJO_WITH_PRIMITIVES_FIELDS = List.of(
            primaryKey("ID", "BIGINT"),
            nonKey("NAME", "VARCHAR", true),
            nonKey("AGE", "INT", true),
            nonKey("SALARY", "DOUBLE", true)
    );

    static CacheConfiguration<?, ?> configWithIndexType(Class<?> keyType, Class<?> valType) {
        CacheConfiguration<?, ?> cacheCfg = new CacheConfiguration<>();
        cacheCfg.setName("SomeCacheName");
        cacheCfg.setIndexedTypes(keyType, valType);
        return cacheCfg;
    }

    static CacheConfiguration<?, ?> configWithQeKeyValue(Class<?> keyType, Class<?> valType) {
        CacheConfiguration<?, ?> cacheCfg = new CacheConfiguration<>();
        cacheCfg.setName("SomeCacheName");
        QueryEntity qe = new QueryEntity(keyType, valType);
        cacheCfg.setQueryEntities(Collections.singleton(qe));
        return cacheCfg;
    }

    static TableDefinition generateTableDef(CacheConfiguration<?, ?> cacheCfg, boolean allowExtraFields) {
        SqlDdlGenerator gen = new SqlDdlGenerator(new TableTypeRegistryMapImpl(), allowExtraFields);
        return gen.generateTableDefinition(cacheCfg);
    }

    static List<Arguments> provideSupportedClasses() {
        // TODO: Check lengths
        List<Map.Entry<Class<?>, String>> primitives = List.of(
                entry(boolean.class, "BOOLEAN"),
                entry(byte.class, "TINYINT"),
                entry(char.class, "CHAR"),
                entry(short.class, "SMALLINT"),
                entry(int.class, "INT"),
                entry(long.class, "BIGINT"),
                entry(double.class, "DOUBLE"),
                entry(float.class, "REAL"),
                entry(byte[].class, "VARBINARY"),
                // More complex stuff
                entry(UUID.class, "UUID"),
                entry(BitSet.class, "VARBINARY"),
                entry(LocalTime.class, "TIME"),
                entry(LocalDate.class, "DATE"),
                entry(LocalDateTime.class, "TIMESTAMP"),
                entry(Instant.class, "TIMESTAMP WITH LOCAL TIME ZONE"),
                entry(Date.class, "DATE"),
                entry(Time.class, "TIME"),
                entry(Timestamp.class, "TIMESTAMP"),
                entry(DayOfWeek.class, "VARCHAR")
        );

        // These types are only supported on the value side.
        List<Map.Entry<Class<?>, String>> supportedOnlyHasValues = List.of(
                entry(boolean[].class, "VARBINARY"),
                entry(char[].class, "VARBINARY"),
                entry(short[].class, "VARBINARY"),
                entry(int[].class, "VARBINARY"),
                entry(long[].class, "VARBINARY"),
                entry(double[].class, "VARBINARY"),
                entry(float[].class, "VARBINARY"),
                entry(List.class, "VARBINARY")
        );

        List<Arguments> ret = new ArrayList<>(primitives.size() * primitives.size());
        for (var cacheCfgArgs : provideCacheConfigSupplier()) {
            for (var keyTypeRef : primitives) {
                for (var valTypeRef : IterableUtils.chainedIterable(primitives, supportedOnlyHasValues)) {
                    var args = arguments(
                            Stream.concat(
                                    Arrays.stream(cacheCfgArgs.get()),
                                    Stream.of(keyTypeRef.getKey(), keyTypeRef.getValue(), valTypeRef.getKey(), valTypeRef.getValue())
                            ).toArray());

                    ret.add(args);
                }
            }
        }

        return ret;
    }

    static List<Arguments> provideCacheConfigSupplier() {
        var cfgGenerators = List.of(
                named("From indexType",
                        (BiFunction<Class<?>, Class<?>, CacheConfiguration<?, ?>>) SqlDdlGeneratorTest::configWithIndexType),
                named("From QE key and value fields",
                        (BiFunction<Class<?>, Class<?>, CacheConfiguration<?, ?>>) SqlDdlGeneratorTest::configWithQeKeyValue)
        );

        List<Arguments> ret = new ArrayList<>(EXTRA_FIELDS_ENABLED_ARG.size() * cfgGenerators.size());
        for (var extraFieldProp : EXTRA_FIELDS_ENABLED_ARG) {
            for (var cfgGen : cfgGenerators) {
                ret.add(arguments(cfgGen, extraFieldProp));
            }
        }

        return ret;
    }

    private static <K> UnaryOperator<K> skipNth(int numElements, UnaryOperator<K> operator) {
        return new UnaryOperator<>() {
            int counter = 0;

            @Override
            public K apply(K k) {
                return (counter++ < numElements) ? k : operator.apply(k);
            }
        };
    }

    private static void testCacheConfig(
            CacheConfiguration<?, ?> cacheCfg,
            boolean allowExtraFields,
            List<ColumnRecord> asserts,
            Map.Entry<String, String> typeHints,
            @Nullable Map<String, String> expectedKeyColumMappings,
            @Nullable Map<String, String> expectedValColumMappings
    ) {
        SqlDdlGenerator gen = new SqlDdlGenerator(new TableTypeRegistryMapImpl(), allowExtraFields);
        GenerateTableResult res = gen.generate(cacheCfg);

        assertThat(res.typeHints()).isEqualTo(typeHints);

        TableDefinition tableDef = res.tableDefinition();
        Stream<ColumnRecord> allowFieldsCol = (allowExtraFields)
                ? Stream.of(new ColumnRecord(EXTRA_FIELDS_COLUMN_NAME, "VARBINARY", true, false))
                : Stream.empty();

        var expectedColumns = Stream.concat(asserts.stream(), allowFieldsCol)
                .map(e -> tuple(e.name, e.type, e.nullable))
                .collect(Collectors.toList());

        var expectedPrimaryKeys = asserts.stream()
                .filter(e -> e.isPk)
                .map(e -> e.name)
                .collect(Collectors.toList());

        SoftAssertions sa = new SoftAssertions();

        sa.assertThat(tableDef.primaryKeyColumns())
                .as("Primary Keys")
                .extracting(ColumnSorted::columnName)
                .containsExactlyElementsOf(expectedPrimaryKeys);

        sa.assertThat(tableDef.columns())
                .as("Columns: (name, type, nullable)")
                .extracting(ColumnDefinition::name, d -> d.type().typeName(), d -> d.type().nullable())
                .containsExactlyInAnyOrderElementsOf(expectedColumns);

        sa.assertAll();

        TableTypeDescriptor tableTypeDescriptor = res.tableTypeDescriptor();

        // Check key mappings;
        assertThat(tableTypeDescriptor.keyFieldNameForColumn())
                .describedAs("Key column mappings")
                .isEqualTo(expectedKeyColumMappings);

        // Check val mappings;
        assertThat(tableTypeDescriptor.valFieldNameForColumn())
                .describedAs("Val column mappings")
                .isEqualTo(expectedValColumMappings);
    }

    private static <K, V> Collector<Map.Entry<K, V>, ?, LinkedHashMap<K, V>> linkedMapCollector() {
        return Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue,
                (a, b) -> {
                    throw new RuntimeException("Should never happen");
                },
                LinkedHashMap::new);
    }

    @ParameterizedTest
    @MethodSource("provideSupportedClasses")
    @SuppressWarnings("PMD.UnusedFormalParameter")
    void testTableDefUsingIndexedTypes(
            BiFunction<Class<?>, Class<?>, CacheConfiguration<?, ?>> cacheConfigSupplier,
            boolean allowExtraFields,
            Class keyType,
            String keyDef,
            Class valType,
            String valDef
    ) {
        String keyTypeName = ClassUtils.primitiveToWrapper(keyType).getName();
        String valTypeName = ClassUtils.primitiveToWrapper(valType).getName();

        var cacheCfg = configWithIndexType(keyType, valType);
        testCacheConfig(
                cacheCfg,
                false,
                List.of(
                    primaryKey("ID", keyDef),
                    nonKey("VAL", valDef, false)
                ),
                entry(keyTypeName, valTypeName),
                emptyMap(),
                emptyMap()
        );
    }

    @ParameterizedTest
    @MethodSource("provideCacheConfigSupplier")
    void testTableDefWithComplexKeyAndSimplePojo(
            BiFunction<Class<?>, Class<?>, CacheConfiguration<?, ?>> cacheConfigSupplier,
            boolean allowExtraFields
    ) {
        var cacheCfg = cacheConfigSupplier.apply(ComplexKeyIntStr.class, SimplePojo.class);
        {
            QueryEntity qe = cacheCfg.getQueryEntities().stream().findFirst().orElseThrow();
            Map<String, String> aliases = qe.getAliases();
            // We currently cannot make an alias to switch the field case due to an hack to support case-insensitive mappings.
            // aliases.put("id", "ID");
            aliases.put("affinityStr", "AFFINITY_STR");
        }

        // TODO: This is wrong, we are not doing the aliasses.
        testCacheConfig(
                cacheCfg,
                allowExtraFields,
                List.of(
                    primaryKey("id", "INT"),
                    primaryKey("AFFINITY_STR", "VARCHAR"),
                    nonKey("name", "VARCHAR", true),
                    nonKey("amount", "INT", false),
                    nonKey("decimalAmount", "DECIMAL", true)
                ),
                entry(ComplexKeyIntStr.class.getName(), SimplePojo.class.getName()),
                Map.ofEntries(
                        entry("id", "id"),
                        entry("AFFINITY_STR", "affinityStr")
                ),
                Map.ofEntries(
                        entry("name", "name"),
                        entry("amount", "amount"),
                        entry("decimalAmount", "decimalAmount")
                )
        );
    }

    @ParameterizedTest
    @MethodSource("provideCacheConfigSupplier")
    void testTableDefWithOrganizationPojo(
            BiFunction<Class<?>, Class<?>, CacheConfiguration<?, ?>> cacheConfigSupplier,
            boolean allowExtraFields
    ) {
        var cacheCfg = cacheConfigSupplier.apply(int.class, Organization.class);
        testCacheConfig(
                cacheCfg,
                allowExtraFields,
                List.of(
                    primaryKey("KEY", "INT"),
                    nonKey("id", "BIGINT", true),
                    nonKey("name", "VARCHAR", true),
                    nonKey("type", "VARCHAR", true),
                    nonKey("lastUpdated", "TIMESTAMP", true)
                ),
                entry(Integer.class.getName(), Organization.class.getName()),
                emptyMap(),
                Map.ofEntries(
                        entry("id", "id"),
                        entry("name", "name"),
                        entry("type", "type"),
                        entry("lastUpdated", "lastUpdated")
                )
        );
    }

    @ParameterizedTest
    @MethodSource("provideCacheConfigSupplier")
    void testTableDefWithPersonPojo(
            BiFunction<Class<?>, Class<?>, CacheConfiguration<?, ?>> cacheConfigSupplier,
            boolean allowExtraFields
    ) {
        // TODO: Make dynamic pojos with BB to cover all the possible scenarios...
        var cacheCfg = cacheConfigSupplier.apply(int.class, Person.class);

        Map<String, String> expectedValFieldToColumnMappings = Map.ofEntries(
                entry("id", "id"),
                entry("orgId", "orgId"),
                entry("firstName", "firstName"),
                entry("lastName", "lastName"),
                entry("resume", "resume"),
                entry("salary", "salary")
        );
        testCacheConfig(
                cacheCfg,
                allowExtraFields,
                PERSON_EXPECTED_FIELDS,
                entry(Integer.class.getName(), Person.class.getName()),
                emptyMap(),
                expectedValFieldToColumnMappings
        );
    }

    @ParameterizedTest
    @MethodSource("provideCacheConfigSupplier")
    void testTableDefWithPersonPojoNotInClasspath(
            BiFunction<Class<?>, Class<?>, CacheConfiguration<?, ?>> cacheConfigSupplier,
            boolean allowExtraFields
    ) {
        String valueTypeName = Person.class.getName().replace("Person", "FakePerson");
        var cacheCfg = cacheConfigSupplier.apply(int.class, Person.class);
        QueryEntity qe = cacheCfg.getQueryEntities().stream().findFirst().orElseThrow();
        qe.setValueType(valueTypeName);

        Set<String> notNullFields = new HashSet<>();
        notNullFields.add("salary");
        qe.setNotNullFields(notNullFields);

        // Since the class is not in the classpath we expect empty mappings.
        Map<String, String> expectedFieldToColumnMappings = emptyMap();
        testCacheConfig(
                cacheCfg,
                allowExtraFields,
                PERSON_EXPECTED_FIELDS,
                entry(Integer.class.getName(), valueTypeName),
                expectedFieldToColumnMappings,
                null
        );
    }

    @ParameterizedTest
    @MethodSource("provideCacheConfigSupplier")
    void testCasingMismatchBetweenQueryEntityAndClass(
            BiFunction<Class<?>, Class<?>, CacheConfiguration<?, ?>> cacheConfigSupplier,
            boolean allowExtraFields
    ) {
        var cacheCfg = cacheConfigSupplier.apply(int.class, Person.class);
        QueryEntity qe = cacheCfg.getQueryEntities().stream().findFirst().orElseThrow();
        // Skips the key, randomize the casing for the field name.
        qe.setFields(
                qe.getFields().entrySet().stream()
                        .map(e -> entry(StringUtils.swapCase(e.getKey()), e.getValue()))
                        .collect(linkedMapCollector())
        );

        var renameExpectedColumns = PERSON_EXPECTED_FIELDS.stream()
                .map(skipNth(1, c -> new ColumnRecord(StringUtils.swapCase(c.name), c.type, c.nullable, c.isPk)))
                .collect(Collectors.toList());

        var expectedValFieldToColumnMappings = PERSON_EXPECTED_FIELDS.stream()
                .skip(1)
                .collect(Collectors.toMap(c -> StringUtils.swapCase(c.name), c -> c.name));

        testCacheConfig(
                cacheCfg,
                allowExtraFields,
                renameExpectedColumns,
                entry(Integer.class.getName(), Person.class.getName()),
                emptyMap(),
                expectedValFieldToColumnMappings
        );
    }

    @ParameterizedTest
    @MethodSource("provideCacheConfigSupplier")
    void testKeyAndValuePojoWithInterceptingFieldNames(
            BiFunction<Class<?>, Class<?>, CacheConfiguration<?, ?>> cacheConfigSupplier,
            boolean allowExtraFields
    ) {
        var cacheCfg = cacheConfigSupplier.apply(InterceptingFieldsModel.Key.class, InterceptingFieldsModel.Value.class);

        // This order should not matter much.
        List<ColumnRecord> expectedFields = List.of(
                nonKey("key1", "BIGINT", false),
                nonKey("key2", "BIGINT", false),
                nonKey("\"VALUE\"", "VARCHAR", true),
                primaryKey("ID", "INT"),
                primaryKey("KEY", "INT")
        );

        Map<String, String> expectedKeyFieldToColumnMappings = Map.ofEntries(
                entry("ID", "key1"),
                entry("KEY", "key2")
        );

        Map<String, String> expectedValFieldToColumnMappings = Map.ofEntries(
                entry("key1", "key1"),
                entry("key2", "key2"),
                entry("value", "value")
        );

        testCacheConfig(
                cacheCfg,
                allowExtraFields,
                expectedFields,
                entry(InterceptingFieldsModel.Key.class.getName(), InterceptingFieldsModel.Value.class.getName()),
                expectedKeyFieldToColumnMappings,
                expectedValFieldToColumnMappings
        );
    }

    @ParameterizedTest
    @FieldSource("EXTRA_FIELDS_ENABLED_ARG")
    void testTableDefWithPojoWithPrimitiveFieldsDefined(boolean allowExtraFields) {
        CacheConfiguration<?, ?> cacheCfg = new CacheConfiguration<>("some-cache");
        cacheCfg.setQueryEntities(Collections.singletonList(POJO_WITH_PRIMITIVES_QE));

        testCacheConfig(
                cacheCfg,
                allowExtraFields,
                POJO_WITH_PRIMITIVES_FIELDS,
                entry(Long.class.getName(), Object.class.getName()),
                emptyMap(),
                null
        );
    }

    @ParameterizedTest
    @FieldSource("EXTRA_FIELDS_ENABLED_ARG")
    void testTableDefWithPojoWithPrimitiveFieldsDefinedInTypes(boolean allowExtraFields) {
        String valType = "PersonRecordClassInRoot";

        CacheConfiguration<?, ?> cacheCfg = new CacheConfiguration<>("some-cache");
        cacheCfg.setQueryEntities(Collections.singletonList(
                new QueryEntity(POJO_WITH_PRIMITIVES_QE).setKeyType(long.class.getName()).setValueType(valType)));

        testCacheConfig(
                cacheCfg,
                allowExtraFields,
                POJO_WITH_PRIMITIVES_FIELDS,
                entry(Long.class.getName(), valType),
                emptyMap(),
                null
        );
    }

    @Test
    void checkPrintUsingStub() {
        var cacheCfg = configWithIndexType(int.class, Person.class);
        var tableDef = generateTableDef(cacheCfg, false);

        var sqlStr = SqlDdlGenerator.createDdlQuery(tableDef);
        assertThat(sqlStr).isNotBlank();
    }

    static class ColumnRecord {
        String name;

        String type;

        boolean nullable;

        boolean isPk;

        public ColumnRecord(String name, String type, boolean nullable, boolean isPk) {
            this.name = name;
            this.type = type;
            this.nullable = nullable;
            this.isPk = isPk;
        }

        static ColumnRecord primaryKey(String name, String type) {
            return new ColumnRecord(name, type, false, true);
        }

        static ColumnRecord nonKey(String name, String type, boolean nullable) {
            return new ColumnRecord(name, type, nullable, false);
        }
    }

    @Nested
    @TestInstance(Lifecycle.PER_CLASS)
    class Schemas {
        SqlDdlGenerator gen = new SqlDdlGenerator();

        @Test
        void noSchema() {
            var cfg = new CacheConfiguration<>("MyCacheName");

            TableDefinition tblDef = gen.generateTableDefinition(cfg);
            assertThat(tblDef.schemaName()).isEqualTo("PUBLIC");
        }

        @ParameterizedTest
        @MethodSource("schemaArgs")
        void schema(String configuredSchema, String expectedSchema) {
            var cfg = new CacheConfiguration<>("MyCacheName");
            cfg.setSqlSchema(configuredSchema);

            TableDefinition tblDef = gen.generateTableDefinition(cfg);
            assertThat(tblDef.schemaName()).isEqualTo(expectedSchema);
        }

        Stream<Arguments> schemaArgs() {
            return Stream.of(
                    arguments("MyCustomSchema", "MYCUSTOMSCHEMA"),
                    arguments("My_Custom_Schema", "MY_CUSTOM_SCHEMA"),
                    arguments("\"MyCustomSchema\"", "\"MyCustomSchema\"")
            );
        }
    }
}
