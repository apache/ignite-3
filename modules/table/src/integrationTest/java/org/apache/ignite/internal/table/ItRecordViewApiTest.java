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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Random;
import org.apache.ignite.internal.marshaller.testobjects.TestObjectWithAllTypes;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.table.RecordView;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Integration tests for record view API.
 */
@SuppressWarnings("ClassEscapesDefinedScope")
public class ItRecordViewApiTest extends ItRecordViewApiBaseTest {
    private static final String TABLE_NAME = "test";

    private final long seed = System.currentTimeMillis();

    private final Random rnd = new Random(seed);

    @BeforeAll
    public void createTables() {
        Column[] keyColumns = new Column[1];
        Column[] valueColumns = new Column[KeyValueTestUtils.ALL_TYPES_COLUMNS.length - 1];

        int counter = 0;

        for (Column column : KeyValueTestUtils.ALL_TYPES_COLUMNS) {
            if ("primitivelongcol".equalsIgnoreCase(column.name())) {
                keyColumns[0] = column;

                continue;
            }

            valueColumns[counter++] = column;
        }

        TestTableDefinition table = new TestTableDefinition(TABLE_NAME, keyColumns, valueColumns, true);

        createTables(List.of(table));
    }

    @BeforeEach
    void printSeed() {
        log.info("Seed: " + seed);
    }

    @ParameterizedTest
    @MethodSource("testCases")
    public void upsert(TestCase<TestObjectWithAllTypes> testCase) {
        TestObjectWithAllTypes key = key(rnd);

        TestObjectWithAllTypes obj = randomObject(rnd, key);
        TestObjectWithAllTypes obj2 = randomObject(rnd, key);
        TestObjectWithAllTypes obj3 = randomObject(rnd, key);

        RecordView<TestObjectWithAllTypes> tbl = testCase.view();

        assertNull(tbl.get(null, key));

        // Insert new row.
        tbl.upsert(null, obj);
        assertEquals(obj, tbl.get(null, key));

        // Upsert row.
        tbl.upsert(null, obj2);
        assertEquals(obj2, tbl.get(null, key));

        // Remove row.
        tbl.delete(null, key);
        assertNull(tbl.get(null, key));

        // Insert new row.
        tbl.upsert(null, obj3);
        assertEquals(obj3, tbl.get(null, key));
    }

    @ParameterizedTest
    @MethodSource("testCases")
    public void insert(TestCase<TestObjectWithAllTypes> testCase) {
        TestObjectWithAllTypes key = key(rnd);
        TestObjectWithAllTypes obj = randomObject(rnd, key);
        TestObjectWithAllTypes obj2 = randomObject(rnd, key);

        RecordView<TestObjectWithAllTypes> tbl = testCase.view();

        assertNull(tbl.get(null, key));

        // Insert new row.
        assertTrue(tbl.insert(null, obj));
        assertEquals(obj, tbl.get(null, key));

        // Ignore existed row pair.
        assertFalse(tbl.insert(null, obj2));
        assertEquals(obj, tbl.get(null, key));
    }

    @ParameterizedTest
    @MethodSource("testCases")
    public void getAndUpsert(TestCase<TestObjectWithAllTypes> testCase) {
        TestObjectWithAllTypes key = key(rnd);
        TestObjectWithAllTypes obj = randomObject(rnd, key);
        TestObjectWithAllTypes obj2 = randomObject(rnd, key);
        TestObjectWithAllTypes obj3 = randomObject(rnd, key);

        RecordView<TestObjectWithAllTypes> tbl = testCase.view();

        assertNull(tbl.get(null, key));

        // Insert new row.
        assertNull(tbl.getAndUpsert(null, obj));
        assertEquals(obj, tbl.get(null, key));

        // Update exited row.
        assertEquals(obj, tbl.getAndUpsert(null, obj2));
        assertEquals(obj2, tbl.getAndUpsert(null, obj3));

        assertEquals(obj3, tbl.get(null, key));
    }

    @ParameterizedTest
    @MethodSource("testCases")
    public void remove(TestCase<TestObjectWithAllTypes> testCase) {
        TestObjectWithAllTypes key = key(rnd);
        TestObjectWithAllTypes obj = randomObject(rnd, key);
        TestObjectWithAllTypes obj2 = randomObject(rnd, key);

        RecordView<TestObjectWithAllTypes> tbl = testCase.view();

        // Delete not existed key.
        assertNull(tbl.get(null, key));
        assertFalse(tbl.delete(null, key));

        // Insert a new row.
        tbl.upsert(null, obj);

        // Delete existed row.
        assertEquals(obj, tbl.get(null, key));
        assertTrue(tbl.delete(null, key));
        assertNull(tbl.get(null, key));

        // Delete already deleted row.
        assertFalse(tbl.delete(null, key));

        // Insert a new row.
        tbl.upsert(null, obj2);
        assertEquals(obj2, tbl.get(null, key));
    }

    @ParameterizedTest
    @MethodSource("testCases")
    public void removeExact(TestCase<TestObjectWithAllTypes> testCase) {
        TestObjectWithAllTypes key = key(rnd);
        TestObjectWithAllTypes obj = randomObject(rnd, key);
        TestObjectWithAllTypes obj2 = randomObject(rnd, key);

        RecordView<TestObjectWithAllTypes> tbl = testCase.view();

        // Insert a new row.
        tbl.upsert(null, obj);
        assertEquals(obj, tbl.get(null, key));

        // Fails to delete row with unexpected value.
        assertFalse(tbl.deleteExact(null, obj2));
        assertEquals(obj, tbl.get(null, key));

        // Delete row with expected value.
        assertTrue(tbl.deleteExact(null, obj));
        assertNull(tbl.get(null, key));

        // Try to remove non-existed key.
        assertFalse(tbl.deleteExact(null, obj));
        assertNull(tbl.get(null, key));

        // Insert a new row.
        tbl.upsert(null, obj2);
        assertEquals(obj2, tbl.get(null, key));

        // Delete row with expected value.
        assertTrue(tbl.delete(null, obj2));
        assertNull(tbl.get(null, key));

        assertFalse(tbl.delete(null, obj2));
        assertNull(tbl.get(null, obj2));
    }

    @ParameterizedTest
    @MethodSource("testCases")
    public void replace(TestCase<TestObjectWithAllTypes> testCase) {
        TestObjectWithAllTypes key = key(rnd);
        TestObjectWithAllTypes obj = randomObject(rnd, key);
        TestObjectWithAllTypes obj2 = randomObject(rnd, key);
        TestObjectWithAllTypes obj3 = randomObject(rnd, key);

        RecordView<TestObjectWithAllTypes> tbl = testCase.view();

        // Ignore replace operation for non-existed row.
        assertFalse(tbl.replace(null, obj));
        assertNull(tbl.get(null, key));

        // Insert new row.
        tbl.upsert(null, obj);

        // Replace existed row.
        assertTrue(tbl.replace(null, obj2));
        assertEquals(obj2, tbl.get(null, key));

        // Replace existed row.
        assertTrue(tbl.replace(null, obj3));
        assertEquals(obj3, tbl.get(null, key));

        // Remove existed row.
        assertTrue(tbl.delete(null, key));
        assertNull(tbl.get(null, key));

        tbl.upsert(null, obj);
        assertEquals(obj, tbl.get(null, key));
    }

    @ParameterizedTest
    @MethodSource("testCases")
    public void replaceExact(TestCase<TestObjectWithAllTypes> testCase) {
        TestObjectWithAllTypes key = key(rnd);
        TestObjectWithAllTypes obj = randomObject(rnd, key);
        TestObjectWithAllTypes obj2 = randomObject(rnd, key);
        TestObjectWithAllTypes obj3 = randomObject(rnd, key);
        TestObjectWithAllTypes obj4 = randomObject(rnd, key);

        RecordView<TestObjectWithAllTypes> tbl = testCase.view();

        // Ignore replace operation for non-existed row.
        assertFalse(tbl.replace(null, obj, obj2));
        assertNull(tbl.get(null, key));

        // Insert new row.
        tbl.upsert(null, obj);

        // Ignore un-exepected row replacement.
        assertFalse(tbl.replace(null, obj2, obj3));
        assertEquals(obj, tbl.get(null, key));

        // Replace existed row.
        assertTrue(tbl.replace(null, obj, obj2));
        assertEquals(obj2, tbl.get(null, key));

        // Replace existed KV pair.
        assertTrue(tbl.replace(null, obj2, obj3));
        assertEquals(obj3, tbl.get(null, key));

        // Remove existed row.
        assertTrue(tbl.delete(null, key));
        assertNull(tbl.get(null, key));

        assertFalse(tbl.replace(null, key, obj4));
        assertNull(tbl.get(null, key));
    }

    @ParameterizedTest
    @MethodSource("testCases")
    public void getAll(TestCase<TestObjectWithAllTypes> testCase) {
        TestObjectWithAllTypes key1 = key(rnd);
        TestObjectWithAllTypes key2 = key(rnd);
        TestObjectWithAllTypes key3 = key(rnd);
        TestObjectWithAllTypes val1 = randomObject(rnd, key1);
        TestObjectWithAllTypes val3 = randomObject(rnd, key3);

        RecordView<TestObjectWithAllTypes> tbl = testCase.view();

        testCase.checkNullKeyRecsError(() -> tbl.getAll(null, null));
        testCase.checkNullKeyError(() -> tbl.getAll(null, Arrays.asList(key1, null)));
        testCase.checkNullRecsError(() -> tbl.upsertAll(null, null));
        testCase.checkNullRecError(() -> tbl.upsertAll(null, Arrays.asList(val1, null)));

        tbl.upsertAll(null, List.of(val1, val3));

        Collection<TestObjectWithAllTypes> res = tbl.getAll(null, List.of(key1, key2, key3));

        assertThat(res, Matchers.contains(val1, null, val3));
    }

    @ParameterizedTest
    @MethodSource("testCases")
    public void contains(TestCase<TestObjectWithAllTypes> testCase) {
        TestObjectWithAllTypes key = key(rnd);
        TestObjectWithAllTypes wrongKey = key(rnd);
        TestObjectWithAllTypes val = randomObject(rnd, key);
        RecordView<TestObjectWithAllTypes> tbl = testCase.view();

        tbl.insert(null, val);

        assertTrue(tbl.contains(null, key));
        assertTrue(tbl.contains(null, val));
        assertFalse(tbl.contains(null, wrongKey));
        assertFalse(tbl.contains(null, randomObject(rnd, wrongKey)));
    }

    @ParameterizedTest
    @MethodSource("testCases")
    public void containsAll(TestCase<TestObjectWithAllTypes> testCase) {
        RecordView<TestObjectWithAllTypes> recordView = testCase.view();

        TestObjectWithAllTypes firstKey = key(rnd);
        TestObjectWithAllTypes firstVal = randomObject(rnd, firstKey);

        TestObjectWithAllTypes secondKey = key(rnd);
        TestObjectWithAllTypes secondVal = randomObject(rnd, secondKey);

        TestObjectWithAllTypes thirdKey = key(rnd);
        TestObjectWithAllTypes thirdVal = randomObject(rnd, thirdKey);

        testCase.checkNullRecsError(() -> recordView.insertAll(null, null));
        testCase.checkNullRecError(() -> recordView.insertAll(null, Arrays.asList(firstKey, null, thirdKey)));

        List<TestObjectWithAllTypes> recs = List.of(firstVal, secondVal, thirdVal);

        recordView.insertAll(null, recs);

        testCase.checkNullKeysError(() -> recordView.containsAll(null, null));
        testCase.checkNullKeyError(() -> recordView.containsAll(null, Arrays.asList(firstKey, null, thirdKey)));

        assertTrue(recordView.containsAll(null, List.of()));
        assertTrue(recordView.containsAll(null, List.of(firstKey)));
        assertTrue(recordView.containsAll(null, List.of(firstVal)));
        assertTrue(recordView.containsAll(null, List.of(firstKey, secondKey, thirdKey)));
        assertTrue(recordView.containsAll(null, List.of(firstVal, secondVal, thirdVal)));

        TestObjectWithAllTypes missedKey = key(rnd);
        TestObjectWithAllTypes missedVal = randomObject(rnd, missedKey);

        assertFalse(recordView.containsAll(null, List.of(missedKey)));
        assertFalse(recordView.containsAll(null, List.of(missedVal)));
        assertFalse(recordView.containsAll(null, List.of(firstKey, secondKey, missedKey)));
        assertFalse(recordView.containsAll(null, List.of(firstVal, secondVal, missedVal)));
    }

    private TestObjectWithAllTypes randomObject(Random rnd, TestObjectWithAllTypes key) {
        TestObjectWithAllTypes obj = TestObjectWithAllTypes.randomObject(rnd);

        obj.setPrimitiveLongCol(key.getPrimitiveLongCol());

        return obj;
    }

    private static TestObjectWithAllTypes key(Random rnd) {
        TestObjectWithAllTypes key = new TestObjectWithAllTypes();

        key.setPrimitiveLongCol(rnd.nextLong());

        return key;
    }

    private List<Arguments> testCases() {
        return generateRecordViewTestArguments(TABLE_NAME, TestObjectWithAllTypes.class);
    }

    @Override
    TestCaseFactory getFactory(String name) {
        return new TestCaseFactory(name) {
            @Override
            <V> TestCase<V> create(boolean async, boolean thin, Class<V> recordClass) {
                RecordView<V> view = thin
                        ? client.tables().table(tableName).recordView(recordClass)
                        : CLUSTER.aliveNode().tables().table(tableName).recordView(recordClass);

                if (async) {
                    view = new AsyncApiRecordViewAdapter<>(view);
                }

                return new TestCase<>(async, thin, view);
            }
        };
    }
}
