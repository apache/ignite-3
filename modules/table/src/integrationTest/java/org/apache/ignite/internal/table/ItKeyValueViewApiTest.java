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

import static org.apache.ignite.internal.table.KeyValueTestUtils.newKey;
import static org.apache.ignite.internal.table.KeyValueTestUtils.newValue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.apache.ignite.internal.lang.IgniteStringFormatter;
import org.apache.ignite.internal.marshaller.testobjects.TestObjectWithAllTypes;
import org.apache.ignite.internal.table.KeyValueTestUtils.TestKeyObject;
import org.apache.ignite.internal.testframework.IgniteTestUtils;
import org.apache.ignite.table.KeyValueView;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.function.Executable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Unified KeyValueView API test with composite value type.
 */
@SuppressWarnings("ClassEscapesDefinedScope")
public class ItKeyValueViewApiTest extends ItKeyValueViewApiBaseTest {
    private static final String TABLE_NAME_COMPOSITE_TYPE = "test";

    private final long seed = System.currentTimeMillis();

    private final Random rnd = new Random(seed);

    @BeforeAll
    public void createTables() {
        createTables(List.of(new TestTableDefinition(TABLE_NAME_COMPOSITE_TYPE, DEFAULT_KEY, KeyValueTestUtils.ALL_TYPES_COLUMNS, true)));
    }

    @BeforeEach
    void printSeed() {
        log.info("Seed: " + seed);
    }

    @ParameterizedTest
    @MethodSource("testCases")
    public void put(TestCase<TestKeyObject, TestObjectWithAllTypes> testCase) {
        KeyValueView<TestKeyObject, TestObjectWithAllTypes> view = testCase.view();
        TestKeyObject key = newKey(rnd);
        TestObjectWithAllTypes obj = newValue(rnd);
        TestObjectWithAllTypes obj2 = newValue(rnd);
        TestObjectWithAllTypes obj3 = newValue(rnd);

        assertNull(view.get(null, key));

        // Put KV pair.
        view.put(null, key, obj);

        assertEquals(obj, view.get(null, key));
        assertEquals(obj, view.get(null, key));

        // Update KV pair.
        view.put(null, key, obj2);

        assertEquals(obj2, view.get(null, key));
        assertEquals(obj2, view.get(null, key));

        // Remove KV pair.
        view.remove(null, key);

        assertNull(view.get(null, key));

        // Put KV pair.
        view.put(null, key, obj3);
        assertEquals(obj3, view.get(null, key));
    }

    @ParameterizedTest
    @MethodSource("testCases")
    public void putIfAbsent(TestCase<TestKeyObject, TestObjectWithAllTypes> testCase) {
        KeyValueView<TestKeyObject, TestObjectWithAllTypes> view = testCase.view();
        TestKeyObject key = TestKeyObject.randomObject(rnd);
        TestObjectWithAllTypes obj = TestObjectWithAllTypes.randomObject(rnd);
        TestObjectWithAllTypes obj2 = TestObjectWithAllTypes.randomObject(rnd);

        assertNull(view.get(null, key));

        // Insert new KV pair.
        assertTrue(view.putIfAbsent(null, key, obj));

        assertEquals(obj, view.get(null, key));

        // Update KV pair.
        assertFalse(view.putIfAbsent(null, key, obj2));

        assertEquals(obj, view.get(null, key));
    }

    @ParameterizedTest
    @MethodSource("testCases")
    public void getNullable(TestCase<TestKeyObject, TestObjectWithAllTypes> testCase) {
        KeyValueView<TestKeyObject, TestObjectWithAllTypes> view = testCase.view();
        TestKeyObject key = TestKeyObject.randomObject(rnd);

        testCase.checkNotNullableError(() -> view.put(null, key, null));
        testCase.checkNullableNotSupportedError(() -> view.getNullable(null, key), "getNullable");
    }

    @ParameterizedTest
    @MethodSource("testCases")
    public void getOrDefault(TestCase<TestKeyObject, TestObjectWithAllTypes> testCase) {
        KeyValueView<TestKeyObject, TestObjectWithAllTypes> view = testCase.view();
        TestKeyObject key = TestKeyObject.randomObject(rnd);
        TestObjectWithAllTypes val = TestObjectWithAllTypes.randomObject(rnd);
        TestObjectWithAllTypes val2 = TestObjectWithAllTypes.randomObject(rnd);
        TestObjectWithAllTypes defaultTuple = TestObjectWithAllTypes.randomObject(rnd);

        assertEquals(defaultTuple, view.getOrDefault(null, key, defaultTuple));
        assertNull(view.getOrDefault(null, key, null));

        // Put KV pair.
        view.put(null, key, val);

        assertEquals(val, view.get(null, key));
        assertEquals(val, view.getOrDefault(null, key, null));
        assertEquals(val, view.getOrDefault(null, key, defaultTuple));

        // Remove KV pair.
        view.remove(null, key);

        assertNull(view.get(null, key));
        assertNull(view.getOrDefault(null, key, null));
        assertEquals(defaultTuple, view.getOrDefault(null, key, defaultTuple));

        // Put KV pair.
        view.put(null, key, val2);
        assertEquals(val2, view.get(null, key));
        assertEquals(val2, view.getOrDefault(null, key, defaultTuple));
    }

    @ParameterizedTest
    @MethodSource("testCases")
    public void getAndPut(TestCase<TestKeyObject, TestObjectWithAllTypes> testCase) {
        KeyValueView<TestKeyObject, TestObjectWithAllTypes> view = testCase.view();
        TestKeyObject key = TestKeyObject.randomObject(rnd);
        TestObjectWithAllTypes obj = TestObjectWithAllTypes.randomObject(rnd);
        TestObjectWithAllTypes obj2 = TestObjectWithAllTypes.randomObject(rnd);
        TestObjectWithAllTypes obj3 = TestObjectWithAllTypes.randomObject(rnd);

        assertNull(view.get(null, key));

        // Insert new KV pair.
        assertNull(view.getAndPut(null, key, obj));
        assertEquals(obj, view.get(null, key));

        // Update KV pair.
        assertEquals(obj, view.getAndPut(null, key, obj2));
        assertEquals(obj2, view.getAndPut(null, key, obj3));

        assertEquals(obj3, view.get(null, key));

        // Check null value
        testCase.checkNotNullableError(() -> view.getAndPut(null, key, null));
        assertEquals(obj3, view.get(null, key));
    }

    @ParameterizedTest
    @MethodSource("testCases")
    public void getNullableAndPut(TestCase<TestKeyObject, TestObjectWithAllTypes> testCase) {
        KeyValueView<TestKeyObject, TestObjectWithAllTypes> view = testCase.view();
        TestKeyObject key = TestKeyObject.randomObject(rnd);
        TestObjectWithAllTypes obj = TestObjectWithAllTypes.randomObject(rnd);
        testCase.checkNullableNotSupportedError(() -> view.getNullableAndPut(null, key, obj), "getNullableAndPut");
    }

    @ParameterizedTest
    @MethodSource("testCases")
    public void contains(TestCase<TestKeyObject, TestObjectWithAllTypes> testCase) {
        KeyValueView<TestKeyObject, TestObjectWithAllTypes> view = testCase.view();
        TestKeyObject key = TestKeyObject.randomObject(rnd);
        TestKeyObject key2 = TestKeyObject.randomObject(rnd);
        TestObjectWithAllTypes obj = TestObjectWithAllTypes.randomObject(rnd);
        TestObjectWithAllTypes obj2 = TestObjectWithAllTypes.randomObject(rnd);

        // Not-existed value.
        assertFalse(view.contains(null, key));

        // Put KV pair.
        view.put(null, key, obj);
        assertTrue(view.contains(null, key));

        // Delete key.
        assertTrue(view.remove(null, key));
        assertFalse(view.contains(null, key));

        // Put KV pair.
        view.put(null, key, obj2);
        assertTrue(view.contains(null, key));

        // Delete key.
        view.remove(null, key2);
        assertFalse(view.contains(null, key2));
    }

    @ParameterizedTest
    @MethodSource("testCases")
    public void containsAll(TestCase<TestKeyObject, TestObjectWithAllTypes> testCase) {
        KeyValueView<TestKeyObject, TestObjectWithAllTypes> view = testCase.view();
        TestKeyObject firstKey = TestKeyObject.randomObject(rnd);
        TestObjectWithAllTypes firstVal = TestObjectWithAllTypes.randomObject(rnd);

        TestKeyObject secondKey = TestKeyObject.randomObject(rnd);
        TestObjectWithAllTypes secondVal = TestObjectWithAllTypes.randomObject(rnd);

        TestKeyObject thirdKey = TestKeyObject.randomObject(rnd);
        TestObjectWithAllTypes thirdVal = TestObjectWithAllTypes.randomObject(rnd);

        Map<TestKeyObject, TestObjectWithAllTypes> kvs = Map.of(
                firstKey, firstVal,
                secondKey, secondVal,
                thirdKey, thirdVal
        );

        view.putAll(null, kvs);

        testCase.checkNullKeysError(() -> view.containsAll(null, null));
        testCase.checkNullKeyError(() -> view.containsAll(null, Arrays.asList(firstKey, null, thirdKey)));

        assertTrue(view.containsAll(null, List.of()));
        assertTrue(view.containsAll(null, List.of(firstKey)));
        assertTrue(view.containsAll(null, List.of(firstKey, secondKey, thirdKey)));

        TestKeyObject missedKey = TestKeyObject.randomObject(rnd);

        assertFalse(view.containsAll(null, List.of(missedKey)));
        assertFalse(view.containsAll(null, List.of(firstKey, secondKey, missedKey)));
    }

    @ParameterizedTest
    @MethodSource("testCases")
    public void remove(TestCase<TestKeyObject, TestObjectWithAllTypes> testCase) {
        KeyValueView<TestKeyObject, TestObjectWithAllTypes> view = testCase.view();
        TestKeyObject key = TestKeyObject.randomObject(rnd);
        TestKeyObject key2 = TestKeyObject.randomObject(rnd);
        TestObjectWithAllTypes obj = TestObjectWithAllTypes.randomObject(rnd);
        TestObjectWithAllTypes obj2 = TestObjectWithAllTypes.randomObject(rnd);

        // Put KV pair.
        view.put(null, key, obj);

        // Delete existed key.
        assertEquals(obj, view.get(null, key));
        assertTrue(view.remove(null, key));
        assertNull(view.get(null, key));

        // Delete already deleted key.
        assertFalse(view.remove(null, key));

        // Put KV pair.
        view.put(null, key, obj2);
        assertEquals(obj2, view.get(null, key));

        // Delete existed key.
        assertTrue(view.remove(null, key));
        assertNull(view.get(null, key));

        // Delete not existed key.
        assertNull(view.get(null, key2));
        assertFalse(view.remove(null, key2));
    }

    @ParameterizedTest
    @MethodSource("testCases")
    public void getAndRemove(TestCase<TestKeyObject, TestObjectWithAllTypes> testCase) {
        KeyValueView<TestKeyObject, TestObjectWithAllTypes> view = testCase.view();
        TestKeyObject key = TestKeyObject.randomObject(rnd);
        TestKeyObject key2 = TestKeyObject.randomObject(rnd);
        TestObjectWithAllTypes obj = TestObjectWithAllTypes.randomObject(rnd);
        TestObjectWithAllTypes obj2 = TestObjectWithAllTypes.randomObject(rnd);

        // Put KV pair.
        view.put(null, key, obj);

        // Delete existed key.
        assertEquals(obj, view.getAndRemove(null, key));
        assertFalse(view.contains(null, key));

        // Delete already deleted key.
        assertNull(view.getAndRemove(null, key));
        assertFalse(view.contains(null, key));

        // Put KV pair.
        view.put(null, key, obj2);
        assertTrue(view.contains(null, key));

        // Delete existed key.
        assertEquals(obj2, view.getAndRemove(null, key));
        assertFalse(view.contains(null, key));

        // Delete not existed key.
        assertNull(view.getAndRemove(null, key2));
    }

    @ParameterizedTest
    @MethodSource("testCases")
    public void getNullableAndRemove(TestCase<TestKeyObject, TestObjectWithAllTypes> testCase) {
        KeyValueView<TestKeyObject, TestObjectWithAllTypes> view = testCase.view();
        TestKeyObject key = TestKeyObject.randomObject(rnd);

        testCase.checkNullableNotSupportedError(() -> view.getNullableAndRemove(null, key), "getNullableAndRemove");
    }

    @ParameterizedTest
    @MethodSource("testCases")
    public void removeExact(TestCase<TestKeyObject, TestObjectWithAllTypes> testCase) {
        KeyValueView<TestKeyObject, TestObjectWithAllTypes> view = testCase.view();
        TestKeyObject key = TestKeyObject.randomObject(rnd);
        TestKeyObject key2 = TestKeyObject.randomObject(rnd);
        TestObjectWithAllTypes obj = TestObjectWithAllTypes.randomObject(rnd);
        TestObjectWithAllTypes obj2 = TestObjectWithAllTypes.randomObject(rnd);

        // Put KV pair.
        view.put(null, key, obj);
        assertEquals(obj, view.get(null, key));

        // Fails to delete KV pair with unexpected value.
        assertFalse(view.remove(null, key, obj2));
        assertEquals(obj, view.get(null, key));

        // Delete KV pair with expected value.
        assertTrue(view.remove(null, key, obj));
        assertNull(view.get(null, key));

        // Once again.
        assertFalse(view.remove(null, key, obj));
        assertNull(view.get(null, key));

        // Try to remove non-existed key.
        assertFalse(view.remove(null, key, obj));
        assertNull(view.get(null, key));

        // Put KV pair.
        view.put(null, key, obj2);
        assertEquals(obj2, view.get(null, key));

        // Check null value.
        //noinspection DataFlowIssue
        testCase.checkNotNullableError(() -> view.remove(null, key, null));

        assertEquals(obj2, view.get(null, key));

        // Delete KV pair with expected value.
        assertTrue(view.remove(null, key, obj2));
        assertNull(view.get(null, key));

        assertFalse(view.remove(null, key2, obj2));
        assertNull(view.get(null, key2));
    }

    @ParameterizedTest
    @MethodSource("testCases")
    public void replace(TestCase<TestKeyObject, TestObjectWithAllTypes> testCase) {
        KeyValueView<TestKeyObject, TestObjectWithAllTypes> view = testCase.view();
        TestKeyObject key = TestKeyObject.randomObject(rnd);
        TestObjectWithAllTypes obj = TestObjectWithAllTypes.randomObject(rnd);
        TestObjectWithAllTypes obj2 = TestObjectWithAllTypes.randomObject(rnd);
        TestObjectWithAllTypes obj3 = TestObjectWithAllTypes.randomObject(rnd);

        // Ignore replace operation for non-existed KV pair.
        assertFalse(view.replace(null, key, obj));
        assertNull(view.get(null, key));

        view.put(null, key, obj);

        // Replace existed KV pair.
        assertTrue(view.replace(null, key, obj2));
        assertEquals(obj2, view.get(null, key));

        view.remove(null, key);

        // Ignore replace operation for non-existed KV pair.
        assertFalse(view.replace(null, key, obj3));
        assertNull(view.get(null, key));

        view.put(null, key, obj3);
        assertEquals(obj3, view.get(null, key));

        // Check null value.
        testCase.checkNotNullableError(() -> view.replace(null, key, null));
        assertEquals(obj3, view.get(null, key));
    }

    @ParameterizedTest
    @MethodSource("testCases")
    public void getAndReplace(TestCase<TestKeyObject, TestObjectWithAllTypes> testCase) {
        KeyValueView<TestKeyObject, TestObjectWithAllTypes> view = testCase.view();
        TestKeyObject key = TestKeyObject.randomObject(rnd);
        TestObjectWithAllTypes obj = TestObjectWithAllTypes.randomObject(rnd);
        TestObjectWithAllTypes obj2 = TestObjectWithAllTypes.randomObject(rnd);
        TestObjectWithAllTypes obj3 = TestObjectWithAllTypes.randomObject(rnd);

        // Ignore replace operation for non-existed KV pair.
        assertNull(view.getAndReplace(null, key, obj));
        assertFalse(view.contains(null, key));

        view.put(null, key, obj);

        // Replace existed KV pair.
        assertEquals(obj, view.getAndReplace(null, key, obj2));
        assertEquals(obj2, view.get(null, key));

        view.remove(null, key);

        // Ignore replace operation for non-existed KV pair.
        assertNull(view.getAndReplace(null, key, obj3));
        assertNull(view.get(null, key));

        view.put(null, key, obj3);
        assertEquals(obj3, view.get(null, key));

        // Check null value.
        testCase.checkNotNullableError(() -> view.getAndReplace(null, key, null));
        assertEquals(obj3, view.get(null, key));
    }

    @ParameterizedTest
    @MethodSource("testCases")
    public void getNullableAndReplace(TestCase<TestKeyObject, TestObjectWithAllTypes> testCase) {
        KeyValueView<TestKeyObject, TestObjectWithAllTypes> view = testCase.view();
        TestKeyObject key = TestKeyObject.randomObject(rnd);
        TestObjectWithAllTypes obj = TestObjectWithAllTypes.randomObject(rnd);

        testCase.checkNullableNotSupportedError(() -> view.getNullableAndReplace(null, key, obj), "getNullableAndReplace");
    }

    @ParameterizedTest
    @MethodSource("testCases")
    public void replaceExact(TestCase<TestKeyObject, TestObjectWithAllTypes> testCase) {
        KeyValueView<TestKeyObject, TestObjectWithAllTypes> view = testCase.view();
        TestKeyObject key = TestKeyObject.randomObject(rnd);
        TestKeyObject key2 = TestKeyObject.randomObject(rnd);
        TestObjectWithAllTypes obj = TestObjectWithAllTypes.randomObject(rnd);
        TestObjectWithAllTypes obj2 = TestObjectWithAllTypes.randomObject(rnd);

        // Insert KV pair.
        testCase.checkNotNullableError(() -> view.replace(null, key, null, obj));
        view.put(null, key, obj);
        assertEquals(obj, view.get(null, key));
        assertNull(view.get(null, key2));

        // Ignore replace operation for non-existed KV pair.
        assertFalse(view.replace(null, key2, obj, obj2));
        assertNull(view.get(null, key2));

        // Replace existed KV pair.
        assertTrue(view.replace(null, key, obj, obj2));
        assertEquals(obj2, view.get(null, key));

        // Check null value pair.
        testCase.checkNotNullableError(() -> view.replace(null, key, obj2, null));
        assertEquals(obj2, view.get(null, key));
    }

    @ParameterizedTest
    @MethodSource("testCases")
    public void getAll(TestCase<TestKeyObject, TestObjectWithAllTypes> testCase) {
        KeyValueView<TestKeyObject, TestObjectWithAllTypes> view = testCase.view();
        TestKeyObject key1 = newKey(rnd);
        TestKeyObject key2 = newKey(rnd);
        TestKeyObject key3 = newKey(rnd);
        TestObjectWithAllTypes val1 = newValue(rnd);
        TestObjectWithAllTypes val3 = newValue(rnd);

        view.putAll(
                null,
                Map.of(
                        key1, val1,
                        key3, val3
                ));

        Map<TestKeyObject, TestObjectWithAllTypes> res = view.getAll(null, List.of(key1, key2, key3));

        assertEquals(2, res.size());
        assertEquals(val1, res.get(key1));
        assertEquals(val3, res.get(key3));
        assertNull(res.get(key2));
    }

    @SuppressWarnings("DataFlowIssue")
    @ParameterizedTest
    @MethodSource("testCases")
    public void nullKeyValidation(TestCase<TestKeyObject, TestObjectWithAllTypes> testCase) {
        KeyValueView<TestKeyObject, TestObjectWithAllTypes> view = testCase.view();
        TestObjectWithAllTypes val = TestObjectWithAllTypes.randomObject(rnd);
        TestObjectWithAllTypes val2 = TestObjectWithAllTypes.randomObject(rnd);

        // Null key.
        testCase.checkNullKeyError(() -> view.contains(null, null));
        testCase.checkNullKeyError(() -> view.contains(null, null));

        testCase.checkNullKeyError(() -> view.get(null, null));
        testCase.checkNullKeyError(() -> view.getAndPut(null, null, val));
        testCase.checkNullKeyError(() -> view.getAndRemove(null, null));
        testCase.checkNullKeyError(() -> view.getAndReplace(null, null, val));
        testCase.checkNullKeyError(() -> view.getNullable(null, null));
        testCase.checkNullKeyError(() -> view.getNullableAndRemove(null, null));
        testCase.checkNullKeyError(() -> view.getNullableAndReplace(null, null, val));
        testCase.checkNullKeyError(() -> view.getNullableAndPut(null, null, val));
        testCase.checkNullKeyError(() -> view.getOrDefault(null, null, val));

        testCase.checkNullKeyError(() -> view.put(null, null, val));
        testCase.checkNullKeyError(() -> view.putIfAbsent(null, null, val));
        testCase.checkNullKeyError(() -> view.remove(null, null));
        testCase.checkNullKeyError(() -> view.remove(null, null, val));
        testCase.checkNullKeyError(() -> view.replace(null, null, val));
        testCase.checkNullKeyError(() -> view.replace(null, null, val, val2));

        testCase.checkNullKeysError(() -> view.getAll(null, null));
        testCase.checkNullKeyError(() -> view.getAll(null, Collections.singleton(null)));
        testCase.checkNullPairsError(() -> view.putAll(null, null));
        testCase.checkNullKeyError(() -> view.putAll(null, Collections.singletonMap(null, val)));
        testCase.checkNullKeysError(() -> view.removeAll(null, null));
        testCase.checkNullKeyError(() -> view.removeAll(null, Collections.singleton(null)));
    }

    @ParameterizedTest
    @MethodSource("testCases")
    public void nonNullableValueColumn(TestCase<TestKeyObject, TestObjectWithAllTypes> testCase) {
        KeyValueView<TestKeyObject, TestObjectWithAllTypes> view = testCase.view();
        TestKeyObject key = TestKeyObject.randomObject(rnd);
        TestObjectWithAllTypes val = TestObjectWithAllTypes.randomObject(rnd);

        testCase.checkNotNullableError(() -> view.getAndPut(null, key, null));
        testCase.checkNotNullableError(() -> view.getAndReplace(null, key, null));
        testCase.checkNullableNotSupportedError(() -> view.getNullableAndPut(null, key, null), "getNullableAndPut");
        testCase.checkNullableNotSupportedError(() -> view.getNullableAndReplace(null, key, null), "getNullableAndReplace");
        assertNull(view.getOrDefault(null, key, null));

        testCase.checkNotNullableError(() -> view.put(null, key, null));
        testCase.checkNotNullableError(() -> view.putIfAbsent(null, key, null));

        //noinspection DataFlowIssue
        testCase.checkNotNullableError(() -> view.remove(null, key, null));

        testCase.checkNotNullableError(() -> view.replace(null, key, null));
        testCase.checkNotNullableError(() -> view.replace(null, key, null, val));
        testCase.checkNotNullableError(() -> view.replace(null, key, val, null));

        testCase.checkNotNullableError(() -> view.putAll(null, Collections.singletonMap(key, null)));
    }

    private List<Arguments> testCases() {
        return generateKeyValueTestArguments(TABLE_NAME_COMPOSITE_TYPE, TestKeyObject.class, TestObjectWithAllTypes.class);
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
        private void checkNotNullableError(Executable run) {
            IgniteTestUtils.assertThrows(
                    NullPointerException.class,
                    run,
                    "null value cannot be used when a value is not mapped to a simple type"
            );
        }

        @SuppressWarnings("ThrowableNotThrown")
        void checkNullableNotSupportedError(Executable run, String methodName) {
            String targetMethodName = async ? methodName + "Async" : methodName;

            String expMessage = IgniteStringFormatter.format(
                    "{} cannot be used when a value is not mapped to a simple type",
                    targetMethodName
            );

            IgniteTestUtils.assertThrows(UnsupportedOperationException.class, run, expMessage);
        }
    }
}
