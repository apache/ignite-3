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

import static org.apache.ignite.internal.testframework.IgniteTestUtils.assertThrows;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.ignite.Ignite;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.InvalidTypeException;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.SchemaMismatchException;
import org.apache.ignite.internal.testframework.IgniteTestUtils;
import org.apache.ignite.internal.type.NativeTypes;
import org.apache.ignite.lang.ErrorGroups.Marshalling;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.lang.MarshallerException;
import org.apache.ignite.sql.ColumnType;
import org.apache.ignite.table.KeyValueView;
import org.apache.ignite.table.Tuple;
import org.apache.ignite.tx.Transaction;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.function.Executable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Integration tests for binary {@link KeyValueView} API.
 */
@SuppressWarnings("ClassEscapesDefinedScope")
public class ItKeyValueBinaryViewApiTest extends ItKeyValueViewApiBaseTest {
    private static final String TABLE_NAME_API_TEST = "test_api";

    private static final String TABLE_NAME_API_TEST_QUOTED = "\"#%\"\"$\\@?!^.[table1]\"";

    private static final String TABLE_COMPOUND_KEY = "test_tuple_compound_key";

    private static final String TABLE_NAME_FOR_SCHEMA_VALIDATION = "test_schema";

    private static final String TABLE_NAME_FOR_TYPE_CAST = "test_type_cast";

    Map<String, TestTableDefinition> createdTables;

    @BeforeAll
    void createTables() {
        Column[] simpleKey = {new Column("ID", NativeTypes.INT64, false)};
        Column[] simpleValue = {new Column("VAL", NativeTypes.STRING, true)};

        Stream<TestTableDefinition> tables = Stream.of(
                new TestTableDefinition(TABLE_NAME_API_TEST, simpleKey, simpleValue, true),
                new TestTableDefinition(
                        TABLE_NAME_API_TEST_QUOTED,
                        new Column[]{new Column("_-#$%/\"\"\\@?!^.[key]", NativeTypes.INT64, false)},
                        new Column[]{new Column("_-#$%/\"\"\\@?!^.[val]", NativeTypes.STRING, true)},
                        true
                ),
                new TestTableDefinition(
                        TABLE_COMPOUND_KEY,
                        new Column[]{
                                new Column("ID", NativeTypes.INT64, false),
                                new Column("AFFID", NativeTypes.INT64, false)
                        },
                        simpleValue
                ),
                new TestTableDefinition(
                        TABLE_NAME_FOR_SCHEMA_VALIDATION,
                        simpleKey,
                        new Column[]{
                                new Column("VAL", NativeTypes.INT64, true),
                                new Column("STR", NativeTypes.stringOf(3), true),
                                new Column("BLOB", NativeTypes.blobOf(3), true)
                        }
                ),
                new TestTableDefinition(
                        TABLE_NAME_FOR_TYPE_CAST,
                        simpleKey,
                        new Column[]{
                                new Column("C_BYTE", NativeTypes.INT8, true),
                                new Column("C_SHORT", NativeTypes.INT16, true),
                                new Column("C_INT", NativeTypes.INT32, true),
                                new Column("C_LONG", NativeTypes.INT64, true),
                                new Column("C_FLOAT", NativeTypes.FLOAT, true),
                                new Column("C_DOUBLE", NativeTypes.DOUBLE, true)
                        }
                )
        );

        createdTables = tables.collect(Collectors.toMap(e -> e.name, Function.identity()));

        createTables(createdTables.values());
    }

    @ParameterizedTest
    @MethodSource("testCases")
    public void put(TestCase testCase) {
        KeyValueView<Tuple, Tuple> tbl = testCase.view();
        String keyName = testCase.keyColumnName(0);
        String valName = testCase.valColumnName(0);

        Tuple key = Tuple.create().set(keyName, 1L);
        Tuple val = Tuple.create().set(valName, "ab");
        Tuple val2 = Tuple.create().set(valName, "cd");
        Tuple val3 = Tuple.create().set(valName, "ef");

        assertNull(tbl.get(null, key));

        // Put KV pair.
        tbl.put(null, key, val);

        assertEqualsValues(testCase.schema(), val, tbl.get(null, key));
        assertEqualsValues(testCase.schema(), val, tbl.get(null, Tuple.create().set(keyName, 1L)));

        // Update KV pair.
        tbl.put(null, key, val2);

        assertEqualsValues(testCase.schema(), val2, tbl.get(null, key));
        assertEqualsValues(testCase.schema(), val2, tbl.get(null, Tuple.create().set(keyName, 1L)));

        // Remove KV pair.
        tbl.remove(null, key);

        assertNull(tbl.get(null, key));

        // Put KV pair.
        tbl.put(null, key, val3);
        assertEqualsValues(testCase.schema(), val3, tbl.get(null, key));
    }

    @ParameterizedTest
    @MethodSource("testCases")
    public void putIfAbsent(TestCase testCase) {
        KeyValueView<Tuple, Tuple> tbl = testCase.view();
        String keyName = testCase.keyColumnName(0);
        String valName = testCase.valColumnName(0);

        Tuple key = Tuple.create().set(keyName, 1L);
        Tuple val = Tuple.create().set(valName, "john");
        Tuple val2 = Tuple.create().set(valName, "jane");

        assertNull(tbl.get(null, key));

        // Insert new KV pair.
        assertTrue(tbl.putIfAbsent(null, key, val));

        assertEqualsValues(testCase.schema(), val, tbl.get(null, key));
        assertEqualsValues(testCase.schema(), val, tbl.get(null, Tuple.create().set(keyName, 1L)));

        // Update KV pair.
        assertFalse(tbl.putIfAbsent(null, key, val2));

        assertEqualsValues(testCase.schema(), val, tbl.get(null, key));
        assertEqualsValues(testCase.schema(), val, tbl.get(null, Tuple.create().set(keyName, 1L)));
    }

    @ParameterizedTest
    @MethodSource("testCases")
    public void getAndPut(TestCase testCase) {
        KeyValueView<Tuple, Tuple> tbl = testCase.view();
        String keyName = testCase.keyColumnName(0);
        String valName = testCase.valColumnName(0);

        Tuple key = Tuple.create().set(keyName, 1L);
        Tuple val = Tuple.create().set(valName, "john");
        Tuple val2 = Tuple.create().set(valName, "jane");
        Tuple val3 = Tuple.create().set(valName, "mark");

        assertNull(tbl.get(null, key));

        // Insert new tuple.
        assertNull(tbl.getAndPut(null, key, val));

        assertEqualsValues(testCase.schema(), val, tbl.get(null, key));
        assertEqualsValues(testCase.schema(), val, tbl.get(null, Tuple.create().set(keyName, 1L)));

        assertEqualsValues(testCase.schema(), val, tbl.getAndPut(null, key, val2));
        assertEqualsValues(testCase.schema(), val2, tbl.getAndPut(null, key, Tuple.create().set(valName, "mark")));

        assertEqualsValues(testCase.schema(), val3, tbl.get(null, key));
        assertNull(tbl.get(null, Tuple.create().set(keyName, 2L)));
    }

    @ParameterizedTest
    @MethodSource("testCases")
    public void nullables(TestCase testCase) {
        KeyValueView<Tuple, Tuple> tbl = testCase.view();
        String keyName = testCase.keyColumnName(0);
        String valName = testCase.valColumnName(0);

        Tuple key = Tuple.create().set(keyName, 1L);
        Tuple val = Tuple.create().set(valName, "john");
        Tuple val2 = Tuple.create().set(valName, "jane");
        Tuple val3 = Tuple.create().set(valName, "mark");

        assertNull(tbl.getNullable(null, key));

        tbl.put(null, key, val);

        assertEqualsValues(testCase.schema(), val, tbl.getNullable(null, key).get());

        assertEqualsValues(testCase.schema(), val, tbl.getNullableAndPut(null, key, val2).get());

        assertEqualsValues(testCase.schema(), val2, tbl.getNullableAndReplace(null, key, val3).get());

        assertEqualsValues(testCase.schema(), val3, tbl.getNullableAndRemove(null, key).get());

        assertNull(tbl.getNullable(null, key));
    }

    @ParameterizedTest
    @MethodSource("testCases")
    public void getOrDefault(TestCase testCase) {
        KeyValueView<Tuple, Tuple> tbl = testCase.view();
        String keyName = testCase.keyColumnName(0);
        String valName = testCase.valColumnName(0);

        Tuple key = Tuple.create().set(keyName, 1L);
        Tuple val = Tuple.create().set(valName, "john");
        Tuple val2 = Tuple.create().set(valName, "jane");
        Tuple defaultTuple = Tuple.create().set(valName, "undefined");

        assertEquals(defaultTuple, tbl.getOrDefault(null, key, defaultTuple));
        assertNull(tbl.getOrDefault(null, key, null));

        // Put KV pair.
        tbl.put(null, key, val);

        assertEquals(val, tbl.getOrDefault(null, key, defaultTuple));
        assertEquals(val, tbl.getOrDefault(null, key, null));

        // Remove KV pair.
        tbl.remove(null, key);

        assertNull(tbl.get(null, key));
        assertNull(tbl.getOrDefault(null, key, null));
        assertEquals(defaultTuple, tbl.getOrDefault(null, key, defaultTuple));

        // Remove KV pair.
        tbl.remove(null, key);

        assertNull(tbl.get(null, key));
        assertEquals(defaultTuple, tbl.getOrDefault(null, key, defaultTuple));
        assertNull(tbl.getOrDefault(null, key, null));

        // Put KV pair.
        tbl.put(null, key, val2);
        assertEquals(val2, tbl.get(null, key));
        assertEquals(val2, tbl.getOrDefault(null, key, defaultTuple));
    }

    @ParameterizedTest
    @MethodSource("testCases")
    public void contains(TestCase testCase) {
        KeyValueView<Tuple, Tuple> tbl = testCase.view();
        String keyName = testCase.keyColumnName(0);
        String valName = testCase.valColumnName(0);

        Tuple key = Tuple.create().set(keyName, 1L);
        Tuple val = Tuple.create().set(valName, "john");
        Tuple val2 = Tuple.create().set(valName, "jane");

        // Not-existed value.
        assertFalse(tbl.contains(null, key));

        // Put KV pair.
        tbl.put(null, key, val);
        assertTrue(tbl.contains(null, Tuple.create().set(keyName, 1L)));

        // Delete key.
        assertTrue(tbl.remove(null, key));
        assertFalse(tbl.contains(null, Tuple.create().set(keyName, 1L)));

        // Put KV pair.
        tbl.put(null, key, val2);
        assertTrue(tbl.contains(null, Tuple.create().set(keyName, 1L)));

        // Non-existed key.
        assertFalse(tbl.contains(null, Tuple.create().set(keyName, 2L)));
        tbl.remove(null, Tuple.create().set(keyName, 2L));
        assertFalse(tbl.contains(null, Tuple.create().set(keyName, 2L)));
    }

    @ParameterizedTest
    @MethodSource("testCases")
    public void containsAll(TestCase testCase) {
        KeyValueView<Tuple, Tuple> tbl = testCase.view();
        String keyName = testCase.keyColumnName(0);
        String valName = testCase.valColumnName(0);

        Tuple key1 = Tuple.create().set(keyName, 101L);
        Tuple val1 = Tuple.create().set(valName, "aaa");
        Tuple key2 = Tuple.create().set(keyName, 102L);
        Tuple val2 = Tuple.create().set(valName, "bbb");
        Tuple key3 = Tuple.create().set(keyName, 103L);
        Tuple val3 = Tuple.create().set(valName, "ccc");

        tbl.putAll(null, Map.of(key1, val1, key2, val2, key3, val3));

        testCase.checkNullKeyError(() -> tbl.containsAll(null, null));
        testCase.checkNullKeyError(() -> tbl.containsAll(null, Arrays.asList(key1, null, key3)));

        assertTrue(tbl.containsAll(null, List.of()));
        assertTrue(tbl.containsAll(null, List.of(key1)));
        assertTrue(tbl.containsAll(null, List.of(key1, key2, key3)));

        Tuple missedKey = Tuple.create().set(keyName, 0L);
        assertFalse(tbl.containsAll(null, List.of(missedKey)));
        assertFalse(tbl.containsAll(null, List.of(key1, key2, missedKey)));
    }

    @ParameterizedTest
    @MethodSource("testCases")
    public void remove(TestCase testCase) {
        KeyValueView<Tuple, Tuple> tbl = testCase.view();
        String keyName = testCase.keyColumnName(0);
        String valName = testCase.valColumnName(0);

        Tuple key = Tuple.create().set(keyName, 1L);
        Tuple key2 = Tuple.create().set(keyName, 2L);
        Tuple val = Tuple.create().set(valName, "john");
        Tuple val2 = Tuple.create().set(valName, "jane");

        tbl.put(null, key, val);
        assertEquals(val, tbl.get(null, key));
        assertTrue(tbl.remove(null, key));
        assertNull(tbl.get(null, key));

        assertFalse(tbl.remove(null, key));

        tbl.put(null, key, val2);
        assertEquals(val2, tbl.get(null, key));
        assertTrue(tbl.remove(null, key));
        assertNull(tbl.get(null, key));

        assertNull(tbl.get(null, key2));
        assertFalse(tbl.remove(null, key2));
    }

    @ParameterizedTest
    @MethodSource("testCases")
    public void removeExact(TestCase testCase) {
        KeyValueView<Tuple, Tuple> tbl = testCase.view();
        String keyName = testCase.keyColumnName(0);
        String valName = testCase.valColumnName(0);

        Tuple key = Tuple.create().set(keyName, 1L);
        Tuple key2 = Tuple.create().set(keyName, 2L);
        Tuple val = Tuple.create().set(valName, "john");
        Tuple val2 = Tuple.create().set(valName, "jane");

        tbl.put(null, key, val);
        assertEquals(val, tbl.get(null, key));

        assertFalse(tbl.remove(null, key, val2));
        assertEquals(val, tbl.get(null, key));

        assertTrue(tbl.remove(null, key, val));
        assertNull(tbl.get(null, key));

        assertFalse(tbl.remove(null, key, val));
        assertNull(tbl.get(null, key));

        testCase.checkNullValueError(() -> tbl.remove(null, key, null));
        assertNull(tbl.get(null, key));

        tbl.put(null, key, val2);
        assertEquals(val2, tbl.get(null, key));

        testCase.checkNullValueError(() -> tbl.remove(null, key, null));
        assertEquals(val2, tbl.get(null, key));

        assertTrue(tbl.remove(null, key, val2));
        assertNull(tbl.get(null, key));

        assertFalse(tbl.remove(null, key2, val2));
        assertNull(tbl.get(null, key2));
    }

    @ParameterizedTest
    @MethodSource("testCases")
    public void replace(TestCase testCase) {
        KeyValueView<Tuple, Tuple> tbl = testCase.view();
        String keyName = testCase.keyColumnName(0);
        String valName = testCase.valColumnName(0);

        Tuple key = Tuple.create().set(keyName, 1L);
        Tuple key2 = Tuple.create().set(keyName, 2L);
        Tuple val = Tuple.create().set(valName, "john");
        Tuple val2 = Tuple.create().set(valName, "jane");
        Tuple val3 = Tuple.create().set(valName, "mark");

        assertFalse(tbl.replace(null, key, val));
        assertNull(tbl.get(null, key));

        tbl.put(null, key, val);
        assertTrue(tbl.replace(null, key, val2));
        assertEquals(val2, tbl.get(null, key));

        assertFalse(tbl.replace(null, key2, val3));
        assertNull(tbl.get(null, key2));

        tbl.put(null, key, val3);
        assertEquals(val3, tbl.get(null, key));
    }

    @ParameterizedTest
    @MethodSource("testCases")
    public void replaceExact(TestCase testCase) {
        KeyValueView<Tuple, Tuple> tbl = testCase.view();
        String keyName = testCase.keyColumnName(0);
        String valName = testCase.valColumnName(0);

        Tuple key = Tuple.create().set(keyName, 1L);
        Tuple key2 = Tuple.create().set(keyName, 2L);
        Tuple val = Tuple.create().set(valName, "john");
        Tuple val2 = Tuple.create().set(valName, "jane");

        assertFalse(tbl.replace(null, key2, val, val2));
        assertNull(tbl.get(null, key2));

        tbl.put(null, key, val);
        assertTrue(tbl.replace(null, key, val, val2));
        assertEquals(val2, tbl.get(null, key));
    }

    @ParameterizedTest
    @MethodSource("schemaValidationTestCases")
    public void validateSchema(TestCase testCase) {
        KeyValueView<Tuple, Tuple> tbl = testCase.view();

        Tuple keyTuple0 = Tuple.create().set("id", 0.0d).set("id1", 0);
        Tuple keyTuple1 = Tuple.create().set("id1", 0);
        Tuple key = Tuple.create().set("id", 1L);

        Tuple valTuple0 = Tuple.create();
        Tuple valTuple1 = Tuple.create().set("str", "qweqweqwe").set("val", 11L);
        Tuple valTuple2 = Tuple.create().set("blob", new byte[]{0, 1, 2, 3}).set("val", 22L);

        testCase.checkValueTypeDoesNotMatchError(() -> tbl.get(null, keyTuple0));
        testCase.checkMissedKeyColumnError(() -> tbl.get(null, keyTuple1));
        testCase.checkKeyColumnDoesntMatchSchemaError(() -> tbl.get(null, Tuple.create().set("id", 1L).set("val", 1L)));

        String strTooLongErr = "Value too long [column='STR', type=STRING(3)]";
        String byteArrayTooLongErr = "Value too long [column='BLOB', type=BYTE_ARRAY(3)]";

        tbl.put(null, key, valTuple0);

        testCase.checkInvalidTypeError(() -> tbl.replace(null, key, valTuple1), strTooLongErr);
        testCase.checkInvalidTypeError(() -> tbl.replace(null, key, valTuple2), byteArrayTooLongErr);

        testCase.checkInvalidTypeError(() -> tbl.put(null, key, valTuple1), strTooLongErr);
        testCase.checkInvalidTypeError(() -> tbl.put(null, key, valTuple2), byteArrayTooLongErr);

        testCase.checkInvalidTypeError(() -> tbl.replace(null, key, valTuple1), strTooLongErr);
        testCase.checkInvalidTypeError(() -> tbl.replace(null, key, valTuple2), byteArrayTooLongErr);
    }

    @ParameterizedTest
    @MethodSource("testCases")
    public void getAll(TestCase testCase) {
        KeyValueView<Tuple, Tuple> tbl = testCase.view();
        String keyName = testCase.keyColumnName(0);
        String valName = testCase.valColumnName(0);

        Tuple key1 = Tuple.create().set(keyName, 1L);
        Tuple key2 = Tuple.create().set(keyName, 2L);
        Tuple key3 = Tuple.create().set(keyName, 3L);

        tbl.putAll(null, Map.of(key1, Tuple.create().set(valName, "john"), key3, Tuple.create().set(valName, "mark")));

        Map<Tuple, Tuple> res = tbl.getAll(null, List.of(key1, key2, key3));

        assertEquals(2, res.size());
        assertEquals(Tuple.create().set(valName, "john"), res.get(key1));
        assertEquals(Tuple.create().set(valName, "mark"), res.get(key3));
        assertNull(res.get(key2));
    }

    @SuppressWarnings("DataFlowIssue")
    @ParameterizedTest
    @MethodSource("testCases")
    public void nullKeyValidation(TestCase testCase) {
        KeyValueView<Tuple, Tuple> tbl = testCase.view();
        String valName = testCase.valColumnName(0);

        Tuple val = Tuple.create().set(valName, "john");
        Tuple val2 = Tuple.create().set(valName, "john");

        testCase.checkNullKeyError(() -> tbl.contains(null, null));
        testCase.checkNullKeyError(() -> tbl.get(null, null));
        testCase.checkNullKeyError(() -> tbl.getAndPut(null, null, val));
        testCase.checkNullKeyError(() -> tbl.getAndRemove(null, null));
        testCase.checkNullKeyError(() -> tbl.getAndReplace(null, null, val));
        testCase.checkNullKeyError(() -> tbl.getOrDefault(null, null, val));
        testCase.checkNullKeyError(() -> tbl.put(null, null, val));
        testCase.checkNullKeyError(() -> tbl.putIfAbsent(null, null, val));
        testCase.checkNullKeyError(() -> tbl.remove(null, null));
        testCase.checkNullKeyError(() -> tbl.remove(null, null, val));
        testCase.checkNullKeyError(() -> tbl.replace(null, null, val));
        testCase.checkNullKeyError(() -> tbl.replace(null, null, val, val2));
        testCase.checkNullKeysError(() -> tbl.getAll(null, null));
        testCase.checkNullKeyError(() -> tbl.getAll(null, Collections.singleton(null)));
        testCase.checkNullPairsError(() -> tbl.putAll(null, null));
        testCase.checkNullKeyError(() -> tbl.putAll(null, Collections.singletonMap(null, val)));
        testCase.checkNullKeysError(() -> tbl.removeAll(null, null));
        testCase.checkNullKeyError(() -> tbl.removeAll(null, Collections.singleton(null)));
    }

    @ParameterizedTest
    @MethodSource("testCases")
    public void nonNullableValueColumn(TestCase testCase) {
        KeyValueView<Tuple, Tuple> tbl = testCase.view();
        String keyName = testCase.keyColumnName(0);
        String valName = testCase.valColumnName(0);

        Tuple key = Tuple.create().set(keyName, 11L);
        Tuple val = Tuple.create().set(valName, "john");

        testCase.checkNullValueError(() -> tbl.getAndPut(null, key, null));
        testCase.checkNullValueError(() -> tbl.getAndReplace(null, key, null));
        testCase.checkNullValueError(() -> tbl.put(null, key, null));
        testCase.checkNullValueError(() -> tbl.putIfAbsent(null, key, null));
        testCase.checkNullValueError(() -> tbl.remove(null, key, null));
        testCase.checkNullValueError(() -> tbl.replace(null, key, null));
        BaseTestCase.checkNpeMessage(() -> tbl.replace(null, key, null, val), "oldVal");
        BaseTestCase.checkNpeMessage(() -> tbl.replace(null, key, val, null), "newVal");
        testCase.checkNullValueError(() -> tbl.putAll(null, Collections.singletonMap(key, null)));
    }

    @ParameterizedTest
    @MethodSource("compoundPkTestCases")
    public void schemaMismatch(TestCase testCase) {
        KeyValueView<Tuple, Tuple> view = testCase.view();

        testCase.checkSchemaMismatchError(
                tx -> view.get(null, Tuple.create().set("id", 0L)),
                "Missed key column: AFFID"
        );

        // TODO https://issues.apache.org/jira/browse/IGNITE-21793 Thin client must throw exception
        if (!testCase.thin) {
            testCase.checkSchemaMismatchError(
                    tx -> view.get(null, Tuple.create().set("id", 0L).set("affId", 1L).set("val", 0L)),
                    "Key tuple doesn't match schema: schemaVersion=1, extraColumns=[VAL]"
            );
        }

        testCase.checkSchemaMismatchError(
                tx -> view.put(tx, Tuple.create().set("id", 0L), Tuple.create()),
                "Missed key column: AFFID"
        );
        testCase.checkSchemaMismatchError(
                tx -> view.put(tx, Tuple.create().set("id", 0L).set("affId", 1L).set("val", 0L), Tuple.create()),
                "Key tuple doesn't match schema: schemaVersion=1, extraColumns=[VAL]"
        );

        // TODO https://issues.apache.org/jira/browse/IGNITE-21793 Message should be identical
        String expectedMessage = testCase.thin
                ? "Value type does not match [column='VAL', expected=STRING, actual=INT64]"
                : "Value type does not match [column='VAL', expected=STRING(65536), actual=INT64]";

        testCase.checkInvalidTypeError(
                () -> view.put(
                        null,
                        Tuple.create().set("id", 0L).set("affId", 1L),
                        Tuple.create().set("id", 0L).set("val", 0L)
                ),
                expectedMessage
        );
    }

    @ParameterizedTest
    @MethodSource("typeCastTestCases")
    public void testWriteAsByte(TestCase testCase) {
        KeyValueView<Tuple, Tuple> tbl = testCase.view();
        String keyName = testCase.keyColumnName(0);
        String valName = testCase.valColumnName(0);
        Tuple key = Tuple.create().set(keyName, 1L);
        ColumnType targetType = ColumnType.INT8;

        // Put short value.
        {
            Tuple val = Tuple.create().set(valName, (short) Byte.MAX_VALUE);

            tbl.put(null, key, val);
            assertThat(tbl.get(null, key).byteValue(valName), is(Byte.MAX_VALUE));

            Tuple outOfRange = Tuple.create().set(valName, (short) (Byte.MAX_VALUE + 1));
            expectTypeMismatch(() -> tbl.put(null, key, outOfRange), valName, targetType, ColumnType.INT16);
        }

        {
            Tuple val = Tuple.create().set(valName, (short) Byte.MIN_VALUE);

            tbl.put(null, key, val);
            assertThat(tbl.get(null, key).byteValue(valName), is(Byte.MIN_VALUE));

            Tuple outOfRange = Tuple.create().set(valName, (short) (Byte.MIN_VALUE - 1));
            expectTypeMismatch(() -> tbl.put(null, key, outOfRange), valName, targetType, ColumnType.INT16);
        }

        // Put int value.
        {
            Tuple val = Tuple.create().set(valName, (int) Byte.MAX_VALUE);

            tbl.put(null, key, val);
            assertThat(tbl.get(null, key).byteValue(valName), is(Byte.MAX_VALUE));

            Tuple outOfRange = Tuple.create().set(valName, Byte.MAX_VALUE + 1);
            expectTypeMismatch(() -> tbl.put(null, key, outOfRange), valName, targetType, ColumnType.INT32);
        }

        {
            Tuple val = Tuple.create().set(valName, (short) Byte.MIN_VALUE);

            tbl.put(null, key, val);
            assertThat(tbl.get(null, key).byteValue(valName), is(Byte.MIN_VALUE));

            Tuple outOfRange = Tuple.create().set(valName, (short) (Byte.MIN_VALUE - 1));
            expectTypeMismatch(() -> tbl.put(null, key, outOfRange), valName, targetType, ColumnType.INT16);
        }

        // Put long value.
        {
            Tuple val = Tuple.create().set(valName, (long) Byte.MAX_VALUE);

            tbl.put(null, key, val);
            assertThat(tbl.get(null, key).byteValue(valName), is(Byte.MAX_VALUE));

            Tuple outOfRange = Tuple.create().set(valName, (long) (Byte.MAX_VALUE + 1));
            expectTypeMismatch(() -> tbl.put(null, key, outOfRange), valName, targetType, ColumnType.INT64);
        }

        {
            Tuple val = Tuple.create().set(valName, (long) Byte.MIN_VALUE);

            tbl.put(null, key, val);
            assertThat(tbl.get(null, key).byteValue(valName), is(Byte.MIN_VALUE));

            Tuple outOfRange = Tuple.create().set(valName, (long) (Byte.MIN_VALUE - 1));
            expectTypeMismatch(() -> tbl.put(null, key, outOfRange), valName, targetType, ColumnType.INT64);
        }

        // Wrong (floating point) types
        {
            Tuple floatValue = Tuple.create().set(valName, Float.MAX_VALUE);
            expectTypeMismatch(() -> tbl.put(null, key, floatValue), valName, targetType, ColumnType.FLOAT);

            Tuple doubleValue = Tuple.create().set(valName, Double.MAX_VALUE);
            expectTypeMismatch(() -> tbl.put(null, key, doubleValue), valName, targetType, ColumnType.DOUBLE);
        }

        // Wrong (decimal) type
        {
            Tuple decimalValue = Tuple.create().set(valName, new BigDecimal(1));
            expectTypeMismatch(() -> tbl.put(null, key, decimalValue), valName, targetType, ColumnType.DECIMAL);
        }
    }

    @ParameterizedTest
    @MethodSource("typeCastTestCases")
    public void testWriteAsShort(TestCase testCase) {
        KeyValueView<Tuple, Tuple> tbl = testCase.view();
        String keyName = testCase.keyColumnName(0);
        String valName = testCase.valColumnName(1);
        Tuple key = Tuple.create().set(keyName, 1L);
        ColumnType targetType = ColumnType.INT16;

        // Put byte value.
        {
            Tuple val = Tuple.create().set(valName, Byte.MAX_VALUE);

            tbl.put(null, key, val);
            assertThat(tbl.get(null, key).shortValue(valName), is((short) Byte.MAX_VALUE));
        }

        // Put int value.
        {
            Tuple val = Tuple.create().set(valName, (int) Short.MAX_VALUE);

            tbl.put(null, key, val);
            assertThat(tbl.get(null, key).shortValue(valName), is(Short.MAX_VALUE));

            Tuple outOfRange = Tuple.create().set(valName, Short.MAX_VALUE + 1);
            expectTypeMismatch(() -> tbl.put(null, key, outOfRange), valName, targetType, ColumnType.INT32);
        }

        {
            Tuple val = Tuple.create().set(valName, (int) Short.MIN_VALUE);

            tbl.put(null, key, val);
            assertThat(tbl.get(null, key).shortValue(valName), is(Short.MIN_VALUE));

            Tuple outOfRange = Tuple.create().set(valName, Short.MIN_VALUE - 1);
            expectTypeMismatch(() -> tbl.put(null, key, outOfRange), valName, targetType, ColumnType.INT32);
        }

        // Put long value.
        {
            Tuple val = Tuple.create().set(valName, (long) Short.MAX_VALUE);

            tbl.put(null, key, val);
            assertThat(tbl.get(null, key).shortValue(valName), is(Short.MAX_VALUE));

            Tuple outOfRange = Tuple.create().set(valName, (long) (Short.MAX_VALUE + 1));
            expectTypeMismatch(() -> tbl.put(null, key, outOfRange), valName, targetType, ColumnType.INT64);
        }

        {
            Tuple val = Tuple.create().set(valName, (long) Short.MIN_VALUE);

            tbl.put(null, key, val);
            assertThat(tbl.get(null, key).shortValue(valName), is(Short.MIN_VALUE));

            Tuple outOfRange = Tuple.create().set(valName, (long) (Short.MIN_VALUE - 1));
            expectTypeMismatch(() -> tbl.put(null, key, outOfRange), valName, targetType, ColumnType.INT64);
        }

        // Wrong (floating point) types
        {
            Tuple floatValue = Tuple.create().set(valName, Float.MAX_VALUE);
            expectTypeMismatch(() -> tbl.put(null, key, floatValue), valName, targetType, ColumnType.FLOAT);

            Tuple doubleValue = Tuple.create().set(valName, Double.MAX_VALUE);
            expectTypeMismatch(() -> tbl.put(null, key, doubleValue), valName, targetType, ColumnType.DOUBLE);
        }

        // Wrong (decimal) type
        {
            Tuple decimalValue = Tuple.create().set(valName, new BigDecimal(1));
            expectTypeMismatch(() -> tbl.put(null, key, decimalValue), valName, targetType, ColumnType.DECIMAL);
        }
    }

    @ParameterizedTest
    @MethodSource("typeCastTestCases")
    public void testWriteAsInt(TestCase testCase) {
        KeyValueView<Tuple, Tuple> tbl = testCase.view();
        String keyName = testCase.keyColumnName(0);
        String valName = testCase.valColumnName(2);
        Tuple key = Tuple.create().set(keyName, 1L);
        ColumnType targetType = ColumnType.INT32;

        // Put byte value.
        {
            Tuple val = Tuple.create().set(valName, Byte.MAX_VALUE);

            tbl.put(null, key, val);
            assertThat(tbl.get(null, key).intValue(valName), is((int) Byte.MAX_VALUE));
        }

        // Put short value.
        {
            Tuple val = Tuple.create().set(valName, Short.MAX_VALUE);

            tbl.put(null, key, val);
            assertThat(tbl.get(null, key).intValue(valName), is((int) Short.MAX_VALUE));
        }

        // Put long value.
        {
            Tuple val = Tuple.create().set(valName, (long) Integer.MAX_VALUE);

            tbl.put(null, key, val);
            assertThat(tbl.get(null, key).intValue(valName), is(Integer.MAX_VALUE));

            Tuple outOfRange = Tuple.create().set(valName, ((long) Integer.MAX_VALUE) + 1);
            expectTypeMismatch(() -> tbl.put(null, key, outOfRange), valName, targetType, ColumnType.INT64);
        }

        {
            Tuple val = Tuple.create().set(valName, (long) Integer.MIN_VALUE);

            tbl.put(null, key, val);
            assertThat(tbl.get(null, key).intValue(valName), is(Integer.MIN_VALUE));

            Tuple outOfRange = Tuple.create().set(valName, ((long) Integer.MIN_VALUE) - 1);
            expectTypeMismatch(() -> tbl.put(null, key, outOfRange), valName, targetType, ColumnType.INT64);
        }

        // Wrong (floating point) types
        {
            Tuple floatValue = Tuple.create().set(valName, Float.MAX_VALUE);
            expectTypeMismatch(() -> tbl.put(null, key, floatValue), valName, targetType, ColumnType.FLOAT);

            Tuple doubleValue = Tuple.create().set(valName, Double.MAX_VALUE);
            expectTypeMismatch(() -> tbl.put(null, key, doubleValue), valName, targetType, ColumnType.DOUBLE);
        }

        // Wrong (decimal) type
        {
            Tuple decimalValue = Tuple.create().set(valName, new BigDecimal(1));
            expectTypeMismatch(() -> tbl.put(null, key, decimalValue), valName, targetType, ColumnType.DECIMAL);
        }
    }

    @ParameterizedTest
    @MethodSource("typeCastTestCases")
    public void testWriteAsLong(TestCase testCase) {
        KeyValueView<Tuple, Tuple> tbl = testCase.view();
        String keyName = testCase.keyColumnName(0);
        String valName = testCase.valColumnName(3);
        Tuple key = Tuple.create().set(keyName, 1L);

        // Put byte value.
        {
            Tuple val = Tuple.create().set(valName, Byte.MAX_VALUE);

            tbl.put(null, key, val);
            assertThat(tbl.get(null, key).longValue(valName), is((long) Byte.MAX_VALUE));
        }

        // Put short value.
        {
            Tuple val = Tuple.create().set(valName, Short.MAX_VALUE);

            tbl.put(null, key, val);
            assertThat(tbl.get(null, key).longValue(valName), is((long) Short.MAX_VALUE));
        }

        // Put int value.
        {
            Tuple val = Tuple.create().set(valName, Integer.MAX_VALUE);

            tbl.put(null, key, val);
            assertThat(tbl.get(null, key).longValue(valName), is((long) Integer.MAX_VALUE));
        }

        ColumnType targetType = ColumnType.INT64;

        // Wrong (floating point) types
        {
            Tuple floatValue = Tuple.create().set(valName, Float.MAX_VALUE);
            expectTypeMismatch(() -> tbl.put(null, key, floatValue), valName, targetType, ColumnType.FLOAT);

            Tuple doubleValue = Tuple.create().set(valName, Double.MAX_VALUE);
            expectTypeMismatch(() -> tbl.put(null, key, doubleValue), valName, targetType, ColumnType.DOUBLE);
        }

        // Wrong (decimal) type
        {
            Tuple decimalValue = Tuple.create().set(valName, new BigDecimal(1));
            expectTypeMismatch(() -> tbl.put(null, key, decimalValue), valName, targetType, ColumnType.DECIMAL);
        }
    }

    @ParameterizedTest
    @MethodSource("typeCastTestCases")
    public void testWriteAsFloat(TestCase testCase) {
        KeyValueView<Tuple, Tuple> tbl = testCase.view();
        String keyName = testCase.keyColumnName(0);
        String valName = testCase.valColumnName(4);
        Tuple key = Tuple.create().set(keyName, 1L);
        ColumnType targetType = ColumnType.FLOAT;

        // Put double value.
        {
            Tuple val = Tuple.create().set(valName, (double) Float.MAX_VALUE);

            tbl.put(null, key, val);
            assertThat(tbl.get(null, key).floatValue(valName), is(Float.MAX_VALUE));

            Tuple outOfRange = Tuple.create().set(valName, Double.MAX_VALUE);
            expectTypeMismatch(() -> tbl.put(null, key, outOfRange), valName, targetType, ColumnType.DOUBLE);
        }

        {
            Tuple val = Tuple.create().set(valName, (double) Float.MIN_VALUE);

            tbl.put(null, key, val);
            assertThat(tbl.get(null, key).floatValue(valName), is(Float.MIN_VALUE));

            Tuple outOfRange = Tuple.create().set(valName, Double.MIN_VALUE);
            expectTypeMismatch(() -> tbl.put(null, key, outOfRange), valName, targetType, ColumnType.DOUBLE);
        }

        // NaN value.
        {
            Tuple val = Tuple.create().set(valName, Double.NaN);

            tbl.put(null, key, val);
            assertThat(tbl.get(null, key).floatValue(valName), is(Float.NaN));
        }

        // Positive infinity value.
        {
            Tuple val = Tuple.create().set(valName, Double.POSITIVE_INFINITY);

            tbl.put(null, key, val);
            assertThat(tbl.get(null, key).floatValue(valName), is(Float.POSITIVE_INFINITY));
        }

        // Negative infinity value.
        {
            Tuple val = Tuple.create().set(valName, Double.NEGATIVE_INFINITY);

            tbl.put(null, key, val);
            assertThat(tbl.get(null, key).floatValue(valName), is(Float.NEGATIVE_INFINITY));
        }

        // Wrong (integer) types
        {
            Tuple byteValue = Tuple.create().set(valName, Byte.MAX_VALUE);
            expectTypeMismatch(() -> tbl.put(null, key, byteValue), valName, targetType, ColumnType.INT8);

            Tuple shortValue = Tuple.create().set(valName, Short.MAX_VALUE);
            expectTypeMismatch(() -> tbl.put(null, key, shortValue), valName, targetType, ColumnType.INT16);

            Tuple intValue = Tuple.create().set(valName, Integer.MAX_VALUE);
            expectTypeMismatch(() -> tbl.put(null, key, intValue), valName, targetType, ColumnType.INT32);

            Tuple longValue = Tuple.create().set(valName, Long.MAX_VALUE);
            expectTypeMismatch(() -> tbl.put(null, key, longValue), valName, targetType, ColumnType.INT64);
        }

        // Wrong (decimal) type
        {
            Tuple decimalValue = Tuple.create().set(valName, new BigDecimal(1));
            expectTypeMismatch(() -> tbl.put(null, key, decimalValue), valName, targetType, ColumnType.DECIMAL);
        }
    }

    @ParameterizedTest
    @MethodSource("typeCastTestCases")
    public void testWriteAsDouble(TestCase testCase) {
        KeyValueView<Tuple, Tuple> tbl = testCase.view();
        String keyName = testCase.keyColumnName(0);
        String valName = testCase.valColumnName(5);
        Tuple key = Tuple.create().set(keyName, 1L);
        ColumnType targetType = ColumnType.DOUBLE;

        // Put float value.
        {
            Tuple val = Tuple.create().set(valName, Float.MAX_VALUE);

            tbl.put(null, key, val);
            assertThat(tbl.get(null, key).doubleValue(valName), is((double) Float.MAX_VALUE));
        }

        // Wrong (integer) types
        {
            Tuple byteValue = Tuple.create().set(valName, Byte.MAX_VALUE);
            expectTypeMismatch(() -> tbl.put(null, key, byteValue), valName, targetType, ColumnType.INT8);

            Tuple shortValue = Tuple.create().set(valName, Short.MAX_VALUE);
            expectTypeMismatch(() -> tbl.put(null, key, shortValue), valName, targetType, ColumnType.INT16);

            Tuple intValue = Tuple.create().set(valName, Integer.MAX_VALUE);
            expectTypeMismatch(() -> tbl.put(null, key, intValue), valName, targetType, ColumnType.INT32);

            Tuple longValue = Tuple.create().set(valName, Long.MAX_VALUE);
            expectTypeMismatch(() -> tbl.put(null, key, longValue), valName, targetType, ColumnType.INT64);
        }

        // Wrong (decimal) type
        {
            Tuple decimalValue = Tuple.create().set(valName, new BigDecimal(1));
            expectTypeMismatch(() -> tbl.put(null, key, decimalValue), valName, targetType, ColumnType.DECIMAL);
        }
    }

    private List<Arguments> testCases() {
        List<Arguments> args1 = generateKeyValueTestArguments(TABLE_NAME_API_TEST, Tuple.class, Tuple.class);
        List<Arguments> args2 = generateKeyValueTestArguments(TABLE_NAME_API_TEST_QUOTED, Tuple.class, Tuple.class, " (quoted names)");

        args1.addAll(args2);

        return args1;
    }

    private List<Arguments> compoundPkTestCases() {
        return generateKeyValueTestArguments(TABLE_COMPOUND_KEY, Tuple.class, Tuple.class);
    }

    private List<Arguments> schemaValidationTestCases() {
        return generateKeyValueTestArguments(TABLE_NAME_FOR_SCHEMA_VALIDATION, Tuple.class, Tuple.class);
    }

    private List<Arguments> typeCastTestCases() {
        return generateKeyValueTestArguments(TABLE_NAME_FOR_TYPE_CAST, Tuple.class, Tuple.class);
    }

    @Override
    TestCaseFactory getFactory(String name) {
        return new TestCaseFactory(name) {
            @Override
            <K, V> BaseTestCase<K, V> create(boolean async, boolean thin, Class<K> keyClass, Class<V> valueClass) {
                assert keyClass == Tuple.class : keyClass;
                assert valueClass == Tuple.class : valueClass;

                KeyValueView<Tuple, Tuple> view = thin
                        ? client.tables().table(tableName).keyValueView()
                        : CLUSTER.aliveNode().tables().table(tableName).keyValueView();

                if (async) {
                    view = new AsyncApiKeyValueViewAdapter<>(view);
                }

                return (BaseTestCase<K, V>) new TestCase(async, thin, view, createdTables.get(name), thin ? client : CLUSTER.aliveNode());
            }
        };
    }

    static class TestCase extends BaseTestCase<Tuple, Tuple> {
        final List<String> keyColumns;

        final List<String> valueColumns;

        final SchemaDescriptor schema;

        final Ignite ignite;

        String keyColumnName(int index) {
            return keyColumns.get(index);
        }

        String valColumnName(int index) {
            return valueColumns.get(index);
        }

        public SchemaDescriptor schema() {
            return schema;
        }

        TestCase(boolean async, boolean thin, KeyValueView<Tuple, Tuple> view, TestTableDefinition tableDefinition, Ignite ignite) {
            super(async, thin, view);

            this.ignite = ignite;

            this.keyColumns = quoteOrLowercaseNames(tableDefinition.schemaDescriptor.keyColumns());
            this.valueColumns = quoteOrLowercaseNames(tableDefinition.schemaDescriptor.valueColumns());
            this.schema = tableDefinition.schemaDescriptor;
        }

        protected Executable wrap(Consumer<Transaction> run) {
            return () -> run.accept(null);
        }

        @SuppressWarnings("ThrowableNotThrown")
        void checkNullValueError(Executable run) {
            assertThrows(NullPointerException.class, run, "val");
        }

        @SuppressWarnings("ThrowableNotThrown")
        void checkSchemaMismatchError(Consumer<Transaction> run, String expectedMessage) {
            Executable e = wrap(run);

            if (thin) {
                assertThrows(IgniteException.class, e, expectedMessage);
            } else {
                IgniteTestUtils.assertThrowsWithCause(e::execute, SchemaMismatchException.class, expectedMessage);
            }
        }

        @SuppressWarnings("ThrowableNotThrown")
        void checkInvalidTypeError(Executable run, String expectedMessage) {
            if (thin) {
                // TODO https://issues.apache.org/jira/browse/IGNITE-21793 Must throw MarshallerException
                assertThrows(IgniteException.class, run, expectedMessage);
            } else {
                IgniteTestUtils.assertThrowsWithCause(run::execute, InvalidTypeException.class, expectedMessage);
            }
        }

        void checkValueTypeDoesNotMatchError(Executable run) {
            String expectedMessage = "Value type does not match [column='ID', expected=INT64, actual=DOUBLE]";

            if (thin) {
                MarshallerException ex = (MarshallerException) assertThrows(MarshallerException.class, run, expectedMessage);

                assertThat(ex.code(), is(Marshalling.COMMON_ERR));
            } else {
                //noinspection ThrowableNotThrown
                IgniteTestUtils.assertThrowsWithCause(run::execute, SchemaMismatchException.class, expectedMessage);
            }
        }

        @SuppressWarnings("ThrowableNotThrown")
        void checkMissedKeyColumnError(Executable run) {
            String expectedMessage = "Missed key column: ID";

            if (thin) {
                assertThrows(MarshallerException.class, run, expectedMessage);
            } else {
                IgniteTestUtils.assertThrowsWithCause(run::execute, SchemaMismatchException.class, expectedMessage);
            }
        }

        void checkKeyColumnDoesntMatchSchemaError(Executable run) {
            // TODO https://issues.apache.org/jira/browse/IGNITE-21793 Thin client must also throw exception
            if (!thin) {
                IgniteTestUtils.assertThrowsWithCause(run::execute, SchemaMismatchException.class,
                        "Key tuple doesn't match schema: schemaVersion=1, extraColumns=[VAL]");
            }
        }
    }
}
