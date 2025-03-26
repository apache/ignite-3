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

package org.apache.ignite.internal.table;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.ignite.internal.lang.IgniteStringFormatter;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.SchemaTestUtils;
import org.apache.ignite.internal.testframework.IgniteTestUtils;
import org.apache.ignite.internal.type.NativeType;
import org.apache.ignite.internal.type.NativeTypeSpec;
import org.apache.ignite.internal.type.NativeTypes;
import org.apache.ignite.lang.MarshallerException;
import org.apache.ignite.lang.UnexpectedNullValueException;
import org.apache.ignite.table.KeyValueView;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Named;
import org.junit.jupiter.api.function.Executable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Unified KeyValueView API test with simple value type.
 */
public class ItKeyValueViewSimpleSchemaApiTest extends ItKeyValueViewApiBaseTest {
    private static final String TABLE_NAME_SIMPLE_TYPE = "test_simple";

    private static final String TABLE_NAME_NON_NULLABLE_VALUE = "test_non_nullable_value";

    @BeforeAll
    void createTable() {
        List<TestTableDefinition> tables = new ArrayList<>();
        Column[] nullableValue = {new Column("VAL", NativeTypes.INT64, true)};

        tables.add(new TestTableDefinition(TABLE_NAME_SIMPLE_TYPE, DEFAULT_KEY, nullableValue, true));

        tables.add(new TestTableDefinition(
                TABLE_NAME_SIMPLE_TYPE,
                DEFAULT_KEY,
                new Column[] {new Column("VAL", NativeTypes.INT64, false)},
                true
        ));

        for (NativeType type : SchemaTestUtils.ALL_TYPES) {
            String tableName = "T_" + type.spec().name();

            tables.add(new TestTableDefinition(tableName, DEFAULT_KEY, nullableValue));
        }

        // Validate all types are tested.
        Set<NativeTypeSpec> nativeTypes = EnumSet.allOf(NativeTypeSpec.class);

        assertEquals(nativeTypes,
                SchemaTestUtils.ALL_TYPES.stream().map(NativeType::spec).collect(Collectors.toSet()));

        createTables(tables);
    }

    @ParameterizedTest
    @MethodSource("testCases")
    void put(TestCase<Long, Long> testCase) {
        KeyValueView<Long, Long> tbl = testCase.view();

        assertNull(tbl.get(null, 1L));

        // Put KV pair.
        tbl.put(null, 1L, 11L);

        assertEquals(11L, tbl.get(null, 1L));
        assertEquals(11L, tbl.get(null, 1L));

        // Update KV pair.
        tbl.put(null, 1L, 22L);

        assertEquals(22L, tbl.get(null, 1L));
        assertEquals(22L, tbl.get(null, 1L));

        // Put `null` value.
        tbl.put(null, 1L, null);
        assertTrue(tbl.contains(null, 1L));

        testCase.checkNullValueError(() -> tbl.get(null, 1L), "getNullable");
        assertNull(tbl.getNullable(null, 1L).get());

        // Put KV pair.
        tbl.put(null, 1L, 33L);
        assertEquals(33L, tbl.get(null, 1L));
    }

    @ParameterizedTest
    @MethodSource("testCases")
    void putIfAbsent(TestCase<Long, Long> testCase) {
        KeyValueView<Long, Long> tbl = testCase.view();

        assertNull(tbl.get(null, 1L));

        // Insert new KV pair.
        assertTrue(tbl.putIfAbsent(null, 1L, 11L));
        assertEquals(11L, tbl.get(null, 1L));

        // Update KV pair.
        assertFalse(tbl.putIfAbsent(null, 1L, 22L));
        assertEquals(11L, tbl.get(null, 1L));

        // Put null value
        assertFalse(tbl.putIfAbsent(null, 1L, null));
        assertEquals(11L, tbl.get(null, 1L));

        assertTrue(tbl.putIfAbsent(null, 2L, null));
        assertTrue(tbl.contains(null, 2L));
        assertNull(tbl.getNullable(null, 2L).get());
    }

    @ParameterizedTest
    @MethodSource("testCases")
    void getNullable(TestCase<Long, Long> testCase) {
        KeyValueView<Long, Long> tbl = testCase.view();

        assertNull(tbl.getNullable(null, 1L));

        // Put KV pair.
        tbl.put(null, 1L, 11L);

        assertEquals(11L, tbl.getNullable(null, 1L).get());

        tbl.put(null, 1L, null);

        testCase.checkNullValueError(() -> tbl.get(null, 1L), "getNullable");
        assertNull(tbl.getNullable(null, 1L).get());

        // Remove KV pair.
        tbl.remove(null, 1L);

        assertNull(tbl.get(null, 1L));
        assertNull(tbl.getNullable(null, 1L));

        // Put KV pair.
        tbl.put(null, 1L, 22L);
        assertEquals(22L, tbl.get(null, 1L));
        assertEquals(22L, tbl.getNullable(null, 1L).get());
    }

    @ParameterizedTest
    @MethodSource("testCases")
    void getOrDefault(TestCase<Long, Long> testCase) {
        KeyValueView<Long, Long> tbl = testCase.view();

        assertEquals(Long.MAX_VALUE, tbl.getOrDefault(null, 1L, Long.MAX_VALUE));
        assertNull(tbl.getOrDefault(null, 1L, null));

        // Put KV pair.
        tbl.put(null, 1L, 11L);

        assertEquals(11L, tbl.getOrDefault(null, 1L, Long.MAX_VALUE));
        assertEquals(11L, tbl.getOrDefault(null, 1L, null));

        tbl.put(null, 1L, null);

        testCase.checkNullValueError(() -> tbl.get(null, 1L), "getNullable");
        assertNull(tbl.getOrDefault(null, 1L, null));

        // TODO https://issues.apache.org/jira/browse/IGNITE-21793 getOrDefault should return default value for null
        if (!testCase.thin) {
            assertEquals(Long.MAX_VALUE, tbl.getOrDefault(null, 1L, Long.MAX_VALUE));
        }

        // Remove KV pair.
        tbl.remove(null, 1L);

        assertNull(tbl.get(null, 1L));
        assertNull(tbl.getOrDefault(null, 1L, null));
        assertEquals(Long.MAX_VALUE, tbl.getOrDefault(null, 1L, Long.MAX_VALUE));

        // Put KV pair.
        tbl.put(null, 1L, 22L);
        assertEquals(22L, tbl.get(null, 1L));
        assertEquals(22L, tbl.getOrDefault(null, 1L, null));
        assertEquals(22L, tbl.getOrDefault(null, 1L, Long.MAX_VALUE));
    }

    @ParameterizedTest
    @MethodSource("testCases")
    void getAndPut(TestCase<Long, Long> testCase) {
        KeyValueView<Long, Long> tbl = testCase.view();

        // Insert new tuple.
        assertNull(tbl.getAndPut(null, 1L, 11L));

        assertEquals(11L, tbl.get(null, 1L));

        assertEquals(11L, tbl.getAndPut(null, 1L, 22L));
        assertEquals(22L, tbl.get(null, 1L));

        tbl.put(null, 1L, null);
        assertTrue(tbl.contains(null, 1L));
        assertNull(tbl.getNullable(null, 1L).get());

        testCase.checkNullValueError(() -> tbl.getAndPut(null, 1L, 33L), "getNullableAndPut");
        assertEquals(33L, tbl.getNullable(null, 1L).get()); // Previous operation applied.

        // Check null value
        assertNotNull(tbl.getAndPut(null, 1L, null));
    }

    @ParameterizedTest
    @MethodSource("testCases")
    void getNullableAndPut(TestCase<Long, Long> testCase) {
        KeyValueView<Long, Long> tbl = testCase.view();

        assertNull(tbl.getAndPut(null, 1L, 11L));
        assertEquals(11L, tbl.get(null, 1L));

        assertEquals(11L, tbl.getNullableAndPut(null, 1L, 22L).get());
        assertEquals(22L, tbl.get(null, 1L));

        assertEquals(22L, tbl.getNullableAndPut(null, 1L, null).get());
        assertTrue(tbl.contains(null, 1L));
        assertNull(tbl.getNullable(null, 1L).get());

        assertNull(tbl.getNullableAndPut(null, 1L, 33L).get());
        assertEquals(33L, tbl.get(null, 1L));
    }

    @ParameterizedTest
    @MethodSource("testCases")
    void contains(TestCase<Long, Long> testCase) {
        KeyValueView<Long, Long> tbl = testCase.view();

        // Not-existed value.
        assertFalse(tbl.contains(null, 1L));

        // Put KV pair.
        tbl.put(null, 1L, 11L);
        assertTrue(tbl.contains(null, 1L));

        // Delete key.
        assertTrue(tbl.remove(null, 1L));
        assertFalse(tbl.contains(null, 1L));

        // Put KV pair.
        tbl.put(null, 1L, 22L);
        assertTrue(tbl.contains(null, 1L));

        // Put null value.
        tbl.put(null, 1L, null);
        assertTrue(tbl.contains(null, 1L));
        assertNull(tbl.getNullable(null, 1L).get());
    }

    @ParameterizedTest
    @MethodSource("testCases")
    void remove(TestCase<Long, Long> testCase) {
        KeyValueView<Long, Long> tbl = testCase.view();

        // Delete not existed key.
        assertFalse(tbl.contains(null, 1L));
        assertFalse(tbl.remove(null, 1L));

        // Put KV pair.
        tbl.put(null, 1L, 11L);

        // Delete existed key.
        assertEquals(11L, tbl.get(null, 1L));
        assertTrue(tbl.remove(null, 1L));
        assertNull(tbl.get(null, 1L));

        // Delete already deleted key.
        assertFalse(tbl.remove(null, 1L));

        // Put KV pair.
        tbl.put(null, 1L, 22L);
        assertEquals(22L, tbl.get(null, 1L));

        // Delete existed key.
        assertTrue(tbl.remove(null, 1L));
        assertNull(tbl.get(null, 1L));

        // Delete null-value.
        tbl.put(null, 1L, null);
        assertTrue(tbl.contains(null, 1L));

        assertTrue(tbl.remove(null, 1L));
        assertFalse(tbl.contains(null, 1L));
    }

    @ParameterizedTest
    @MethodSource("testCases")
    void getAndRemove(TestCase<Long, Long> testCase) {
        KeyValueView<Long, Long> tbl = testCase.view();

        // Delete not existed key.
        assertNull(tbl.getAndRemove(null, 1L));

        // Put KV pair.
        tbl.put(null, 1L, 11L);

        // Delete existed key.
        assertEquals(11L, tbl.getAndRemove(null, 1L));
        assertFalse(tbl.contains(null, 1L));

        // Delete already deleted key.
        assertNull(tbl.getAndRemove(null, 1L));
        assertFalse(tbl.contains(null, 1L));

        // Put KV pair.
        tbl.put(null, 1L, 22L);

        // Delete existed key.
        assertEquals(22L, tbl.getAndRemove(null, 1L));
        assertFalse(tbl.contains(null, 1L));

        tbl.put(null, 1L, null);
        assertTrue(tbl.contains(null, 1L));

        testCase.checkNullValueError(() -> tbl.getAndRemove(null, 1L), "getNullableAndRemove");
        assertFalse(tbl.contains(null, 1L));
        assertNull(tbl.getNullable(null, 1L));
    }

    @ParameterizedTest
    @MethodSource("testCases")
    void getNullableAndRemove(TestCase<Long, Long> testCase) {
        KeyValueView<Long, Long> tbl = testCase.view();

        // Delete not existed key.
        assertNull(tbl.getNullableAndRemove(null, 1L));

        // Put KV pair.
        tbl.put(null, 1L, 11L);

        // Delete existed key.
        assertEquals(11L, tbl.getNullableAndRemove(null, 1L).get());
        assertFalse(tbl.contains(null, 1L));

        // Delete already deleted key.
        assertNull(tbl.getNullableAndRemove(null, 1L));
        assertFalse(tbl.contains(null, 1L));

        // Put KV pair.
        tbl.put(null, 1L, 22L);

        // Delete existed key.
        assertEquals(22L, tbl.getNullableAndRemove(null, 1L).get());
        assertFalse(tbl.contains(null, 1L));

        tbl.put(null, 1L, null);
        assertTrue(tbl.contains(null, 1L));

        assertNull(tbl.getNullableAndRemove(null, 1L).get());
        assertFalse(tbl.contains(null, 1L));
        assertNull(tbl.getNullable(null, 1L));
    }

    @ParameterizedTest
    @MethodSource("testCases")
    void removeExact(TestCase<Long, Long> testCase) {
        KeyValueView<Long, Long> tbl = testCase.view();

        // Put KV pair.
        tbl.put(null, 1L, 11L);
        assertEquals(11L, tbl.get(null, 1L));

        // Fails to delete KV pair with unexpected value.
        assertFalse(tbl.remove(null, 1L, 22L));
        assertEquals(11L, tbl.get(null, 1L));

        // Delete KV pair with expected value.
        assertTrue(tbl.remove(null, 1L, 11L));
        assertNull(tbl.get(null, 1L));

        // Once again.
        assertFalse(tbl.remove(null, 1L, 11L));
        assertNull(tbl.get(null, 1L));

        // Try to remove non-existed key.
        assertFalse(tbl.remove(null, 1L, 11L));
        assertNull(tbl.get(null, 1L));

        // Put KV pair.
        tbl.put(null, 1L, 22L);
        assertEquals(22L, tbl.get(null, 1L));

        // Remove null value.
        assertFalse(tbl.remove(null, 1L, null));
        assertEquals(22L, tbl.get(null, 1L));

        tbl.put(null, 1L, null);

        assertFalse(tbl.remove(null, 1L, 22L));
        assertNull(tbl.getNullable(null, 1L).get());

        assertTrue(tbl.remove(null, 1L, null));
        assertNull(tbl.get(null, 1L));
    }

    @ParameterizedTest
    @MethodSource("testCases")
    void replace(TestCase<Long, Long> testCase) {
        KeyValueView<Long, Long> tbl = testCase.view();

        // Ignore replace operation for non-existed KV pair.
        assertFalse(tbl.replace(null, 1L, 11L));
        assertNull(tbl.get(null, 1L));

        tbl.put(null, 1L, 11L);

        // Replace existed KV pair.
        assertTrue(tbl.replace(null, 1L, 22L));
        assertEquals(22L, tbl.get(null, 1L));

        // Replace with null-value.
        assertTrue(tbl.replace(null, 1L, null));
        assertTrue(tbl.contains(null, 1L));
        assertNull(tbl.getNullable(null, 1L).get());

        // Replace null-value
        assertTrue(tbl.replace(null, 1L, 33L));
        assertEquals(33L, tbl.get(null, 1L));
    }

    @ParameterizedTest
    @MethodSource("testCases")
    void getAndReplace(TestCase<Long, Long> testCase) {
        KeyValueView<Long, Long> tbl = testCase.view();

        // Ignore replace operation for non-existed KV pair.
        assertNull(tbl.getAndReplace(null, 1L, 11L));
        assertNull(tbl.get(null, 1L));

        tbl.put(null, 1L, 11L);

        // Replace existed KV pair.
        assertEquals(11, tbl.getAndReplace(null, 1L, 22L));
        assertEquals(22L, tbl.get(null, 1L));

        // Replace with null-value.
        tbl.put(null, 1L, null);
        assertTrue(tbl.contains(null, 1L));
        assertNull(tbl.getNullable(null, 1L).get());

        // Replace null-value
        testCase.checkNullValueError(() -> tbl.getAndReplace(null, 1L, 33L), "getNullableAndReplace");
        assertEquals(33L, tbl.get(null, 1L));

        // Check null value.
        assertEquals(33, tbl.getAndReplace(null, 1L, null));
        assertNull(tbl.getNullable(null, 1L).get());
    }

    @ParameterizedTest
    @MethodSource("testCases")
    void replaceExact(TestCase<Long, Long> testCase) {
        KeyValueView<Long, Long> tbl = testCase.view();

        // Ignore non-existed KV pair.
        assertFalse(tbl.replace(null, 1L, null, 11L));
        assertNull(tbl.get(null, 1L));

        tbl.put(null, 1L, 11L);

        // Ignore replace operation for non-existed KV pair.
        assertFalse(tbl.replace(null, 2L, 11L, 22L));
        assertNull(tbl.get(null, 2L));

        // Replace existed KV pair.
        assertTrue(tbl.replace(null, 1L, 11L, 22L));
        assertEquals(22L, tbl.get(null, 1L));

        // Replace with null value.
        assertTrue(tbl.replace(null, 1L, 22L, null));
        assertNull(tbl.getNullable(null, 1L).get());

        // Replace null value.
        assertTrue(tbl.replace(null, 1L, null, 33L));
        assertEquals(33L, tbl.get(null, 1L));

        // Check non-existed KV pair.
        assertFalse(tbl.replace(null, 2L, null, null));
        assertNull(tbl.getNullable(null, 2L));
    }

    @ParameterizedTest
    @MethodSource("allTypeColumnsTestsCases")
    void putGetAllTypes(AllTypesTestCase testCase) {
        try {
            Random rnd = new Random();
            Long key = 42L;

            Object val = SchemaTestUtils.generateRandomValue(rnd, testCase.type);

            KeyValueView<Long, Object> kvView = testCase.view();

            kvView.put(null, key, val);

            if (val instanceof byte[]) {
                assertArrayEquals((byte[]) val, (byte[]) kvView.get(null, key));
            } else {
                assertEquals(val, kvView.get(null, key));
            }
        } finally {
            // It's very inefficient to clear all tables after each test.
            sql("DELETE FROM " + testCase.tableName);
        }
    }

    @ParameterizedTest
    @MethodSource("testCases")
    void getAll(TestCase<Long, Long> testCase) {
        KeyValueView<Long, Long> tbl = testCase.view();

        tbl.put(null, 1L, 11L);
        tbl.put(null, 3L, 33L);
        tbl.put(null, 4L, null);

        // Check missed value
        Map<Long, Long> res = tbl.getAll(null, List.of(1L, 2L, 3L));

        assertEquals(2, res.size());
        assertEquals(11L, res.get(1L));
        assertNull(res.get(2L));
        assertEquals(33L, res.get(3L));

        // Check null value
        res = tbl.getAll(null, List.of(1L, 2L, 4L));

        assertEquals(2, res.size());
        assertEquals(11L, res.get(1L));
        assertNull(res.get(2L));
        assertNull(res.get(4L)); // 'null' value exists.
        assertTrue(res.containsKey(4L));

        // Check getOrDefault for result.
        assertEquals(Long.MAX_VALUE, res.getOrDefault(2L, Long.MAX_VALUE));
        assertEquals(Long.MAX_VALUE, res.getOrDefault(3L, Long.MAX_VALUE));
        assertNull(res.getOrDefault(4L, Long.MAX_VALUE));

        // Check empty keys collection.
        res = tbl.getAll(null, List.of());
        assertTrue(res.isEmpty());

        // Check empty result.
        res = tbl.getAll(null, List.of(2L));
        assertTrue(res.isEmpty());

        // Check null value in result.
        res = tbl.getAll(null, List.of(3L));
        assertFalse(res.isEmpty());
        assertTrue(res.containsKey(3L));
    }

    @ParameterizedTest
    @MethodSource("testCases")
    void putAll(TestCase<Long, Long> testCase) {
        KeyValueView<Long, Long> tbl = testCase.view();

        // Check empty collection.
        tbl.putAll(null, Map.of());

        // Check non-null values.
        tbl.putAll(null, Map.of(1L, 11L, 3L, 33L));

        assertEquals(11L, tbl.get(null, 1L));
        assertEquals(33L, tbl.get(null, 3L));

        // Check null values.
        Map<Long, Long> map = new HashMap<>();
        map.put(1L, 11L);
        map.put(3L, null);

        tbl.putAll(null, map);

        assertEquals(11L, tbl.get(null, 1L));
        assertNull(tbl.get(null, 2L));
        assertNull(tbl.getNullable(null, 3L).get());
    }

    @ParameterizedTest
    @MethodSource("testCases")
    void removeAll(TestCase<Long, Long> testCase) {
        KeyValueView<Long, Long> tbl = testCase.view();

        tbl.put(null, 1L, 11L);
        tbl.put(null, 3L, 33L);
        tbl.put(null, 4L, null);

        assertTrue(tbl.contains(null, 4L));

        // Check null values.
        Collection<Long> notFound = tbl.removeAll(null, List.of(2L, 3L, 4L));

        assertEquals(1, notFound.size());
        assertEquals(2L, notFound.iterator().next());

        assertEquals(11L, tbl.get(null, 1L));
        assertFalse(tbl.contains(null, 3L));
        assertFalse(tbl.contains(null, 4L));

        // Check empty collection
        notFound = tbl.removeAll(null, List.of());

        assertTrue(notFound.isEmpty());
        assertEquals(11L, tbl.get(null, 1L));

        // Check simple collection.
        assertTrue(tbl.removeAll(null, List.of(1L)).isEmpty());

        assertEquals(1L, tbl.removeAll(null, List.of(1L)).iterator().next());
    }

    @SuppressWarnings("ConstantConditions")
    @ParameterizedTest
    @MethodSource("testCases")
    void nullKeyValidation(TestCase<Long, Long> testCase) {
        final KeyValueView<Long, Long> tbl = testCase.view();

        // Null key.
        testCase.checkNullKeyError(() -> tbl.contains(null, null));

        testCase.checkNullKeyError(() -> tbl.get(null, null));
        testCase.checkNullKeyError(() -> tbl.getAndPut(null, null, 1L));
        testCase.checkNullKeyError(() -> tbl.getAndRemove(null, null));
        testCase.checkNullKeyError(() -> tbl.getAndReplace(null, null, 1L));
        testCase.checkNullKeyError(() -> tbl.getNullable(null, null));
        testCase.checkNullKeyError(() -> tbl.getNullableAndRemove(null, null));
        testCase.checkNullKeyError(() -> tbl.getNullableAndReplace(null, null, 1L));
        testCase.checkNullKeyError(() -> tbl.getNullableAndPut(null, null, 1L));
        testCase.checkNullKeyError(() -> tbl.getOrDefault(null, null, 1L));

        testCase.checkNullKeyError(() -> tbl.put(null, null, 1L));
        testCase.checkNullKeyError(() -> tbl.putIfAbsent(null, null, 1L));
        testCase.checkNullKeyError(() -> tbl.remove(null, null));
        testCase.checkNullKeyError(() -> tbl.remove(null, null, 1L));
        testCase.checkNullKeyError(() -> tbl.replace(null, null, 1L));
        testCase.checkNullKeyError(() -> tbl.replace(null, null, 1L, 2L));

        testCase.checkNullKeysError(() -> tbl.getAll(null, null));
        testCase.checkNullKeyError(() -> tbl.getAll(null, Collections.singleton(null)));
        testCase.checkNullPairsError(() -> tbl.putAll(null, null));
        testCase.checkNullKeyError(() -> tbl.putAll(null, Collections.singletonMap(null, 1L)));
        testCase.checkNullKeyError(() -> tbl.removeAll(null, null));
        testCase.checkNullKeyError(() -> tbl.removeAll(null, Collections.singleton(null)));
    }

    @ParameterizedTest
    @MethodSource("nonNullableValueTestCases")
    void nonNullableValueColumn(TestCase<Long, Long> testCase) {
        KeyValueView<Long, Long> tbl = testCase.view();

        testCase.checkNotNullableColumnError(() -> tbl.getAndPut(null, 1L, null));
        testCase.checkNotNullableColumnError(() -> tbl.getAndReplace(null, 1L, null));
        testCase.checkNotNullableColumnError(() -> tbl.getNullableAndReplace(null, 1L, null));
        testCase.checkNotNullableColumnError(() -> tbl.getNullableAndPut(null, 1L, null));

        testCase.checkNotNullableColumnError(() -> tbl.put(null, 1L, null));
        testCase.checkNotNullableColumnError(() -> tbl.putIfAbsent(null, 1L, null));
        //noinspection DataFlowIssue
        testCase.checkNotNullableColumnError(() -> tbl.remove(null, 1L, null));
        testCase.checkNotNullableColumnError(() -> tbl.replace(null, 1L, null));
        testCase.checkNotNullableColumnError(() -> tbl.replace(null, 1L, null, 2L));
        testCase.checkNotNullableColumnError(() -> tbl.replace(null, 1L, 1L, null));

        testCase.checkNotNullableColumnError(() -> tbl.putAll(null, Collections.singletonMap(1L, null)));
    }

    private List<Arguments> testCases() {
        return generateKeyValueTestArguments(TABLE_NAME_SIMPLE_TYPE, Long.class, Long.class);
    }

    private List<Arguments> nonNullableValueTestCases() {
        return generateKeyValueTestArguments(TABLE_NAME_NON_NULLABLE_VALUE, Long.class, Long.class);
    }

    private List<Arguments> allTypeColumnsTestsCases() {
        List<Arguments> arguments = new ArrayList<>();

        for (NativeType nativeType : SchemaTestUtils.ALL_TYPES) {
            String tableName = "T_" + nativeType.spec().name();

            Class<?> valueClass = NativeTypeSpec.toClass(nativeType.spec(), true);

            TestCaseFactory factory = getFactory(tableName);

            for (TestCaseType testType : TestCaseType.values()) {
                arguments.add(Arguments.of(Named.of(
                        nativeType.spec().name() + " " + testType.description(),
                        new AllTypesTestCase(factory.create(testType, Long.class, valueClass), tableName, nativeType)
                )));
            }
        }

        return arguments;
    }

    @Override
    TestCaseFactory getFactory(String name) {
        return new TestCaseFactory(name) {
            @Override
            <K, V> BaseTestCase<K, V> create(boolean async, boolean thin, Class<K> keyClass, Class<V> valueClass) {
                KeyValueView<K, V> view = thin
                        ? client.tables().table(tableName).keyValueView(keyClass, valueClass)
                        : CLUSTER.aliveNode().tables().table(tableName).keyValueView(keyClass, valueClass);

                if (async) {
                    view = new AsyncApiKeyValueViewAdapter<>(view);
                }

                return new TestCase<>(async, thin, view);
            }
        };
    }

    static class TestCase<K, V> extends BaseTestCase<K, V> {
        TestCase(boolean async, boolean thin, KeyValueView<K, V> view) {
            super(async, thin, view);
        }

        @SuppressWarnings("ThrowableNotThrown")
        private void checkNotNullableColumnError(Executable run) {
            IgniteTestUtils.assertThrows(MarshallerException.class, run, "Column 'VAL' does not allow NULLs");
        }

        @SuppressWarnings("ThrowableNotThrown")
        private void checkNullValueError(Executable run, String methodName) {
            String targetMethodName = async ? methodName + "Async" : methodName;

            String expMessage = IgniteStringFormatter.format(
                    "Got unexpected null value: use `{}` sibling method instead.",
                    targetMethodName
            );

            // TODO https://issues.apache.org/jira/browse/IGNITE-21793 Thin client should handle null without MarshallerException
            if (thin) {
                IgniteTestUtils.assertThrowsWithCause(
                        run::execute, UnexpectedNullValueException.class, expMessage);

                return;
            }

            IgniteTestUtils.assertThrows(
                    UnexpectedNullValueException.class, run, expMessage);
        }
    }

    static class AllTypesTestCase {
        private final NativeType type;
        private final String tableName;
        private final KeyValueView<Long, Object> view;

        AllTypesTestCase(BaseTestCase<Long, ?> testCase, String tableName, NativeType type) {
            this.type = type;
            this.tableName = tableName;
            this.view = (KeyValueView<Long, Object>) testCase.view();
        }

        KeyValueView<Long, Object> view() {
            return view;
        }
    }
}
