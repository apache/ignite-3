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
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.InvalidTypeException;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.SchemaMismatchException;
import org.apache.ignite.internal.testframework.IgniteTestUtils;
import org.apache.ignite.internal.type.NativeTypes;
import org.apache.ignite.lang.ErrorGroups.Client;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.lang.MarshallerException;
import org.apache.ignite.lang.util.IgniteNameUtils;
import org.apache.ignite.table.RecordView;
import org.apache.ignite.table.Tuple;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.function.Executable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Integration tests for binary record view API.
 */
@SuppressWarnings("ClassEscapesDefinedScope")
public class ItRecordBinaryViewApiTest extends ItRecordViewApiBaseTest {
    private static final String TABLE_NAME_API_TEST = "test_api";

    private static final String TABLE_NAME_API_TEST_QUOTED = "\"#%\"\"$\\@?!^.[table1]\"";

    private static final String TABLE_NAME_WITH_DEFAULT_VALUES = "test_defaults";

    private static final String TABLE_NAME_FOR_SCHEMA_VALIDATION = "test_schema";

    private static final String TABLE_COMPOUND_KEY = "test_tuple_compound_key";

    private static final String TABLE_TYPE_MISMATCH = "test_type_mismatch";

    private static final String TABLE_STRING_TYPE_MATCH = "test_string_type_match";

    private static final String TABLE_BYTE_TYPE_MATCH = "test_byte_type_match";

    private Map<String, TestTableDefinition> schemaAwareTestTables;

    @BeforeAll
    void createTables() {
        Column[] valueColumns = {new Column("VAL", NativeTypes.INT64, true)};

        TestTableDefinition testApiTable1 = new TestTableDefinition(TABLE_NAME_API_TEST, DEFAULT_KEY, valueColumns, true);

        TestTableDefinition testApiTable2 = new TestTableDefinition(
                TABLE_NAME_API_TEST_QUOTED,
                new Column[]{new Column("_-#$%/\"\"\\@?!^.[key]", NativeTypes.INT64, false)},
                new Column[]{new Column("_-#$%/\"\"\\@?!^.[val]", NativeTypes.INT64, true)},
                true
        );

        List<TestTableDefinition> tablesToCreate = List.of(
                testApiTable1,
                testApiTable2,
                new TestTableDefinition(
                        TABLE_COMPOUND_KEY,
                        new Column[]{
                                new Column("ID", NativeTypes.INT64, false),
                                new Column("AFFID", NativeTypes.INT64, false)
                        },
                        valueColumns
                ),
                new TestTableDefinition(
                        TABLE_TYPE_MISMATCH,
                        DEFAULT_KEY,
                        new Column[]{
                                new Column("VALSTRING", NativeTypes.stringOf(3), true),
                                new Column("VALBYTES", NativeTypes.blobOf(3), true)
                        }
                ),
                new TestTableDefinition(
                        TABLE_STRING_TYPE_MATCH,
                        DEFAULT_KEY,
                        new Column[]{new Column("VALSTRING", NativeTypes.stringOf(3), true)}
                ),
                new TestTableDefinition(
                        TABLE_BYTE_TYPE_MATCH,
                        DEFAULT_KEY,
                        new Column[]{
                                new Column("VALUNLIMITED", NativeTypes.BYTES, true),
                                new Column("VALLIMITED", NativeTypes.blobOf(2), true)
                        }
                ),
                new TestTableDefinition(
                        TABLE_NAME_FOR_SCHEMA_VALIDATION,
                        DEFAULT_KEY,
                        new Column[]{
                                new Column("VAL", NativeTypes.INT64, true),
                                new Column("STR", NativeTypes.stringOf(3), true),
                                new Column("BLOB", NativeTypes.blobOf(3), true)
                        }
                )
        );

        createTables(tablesToCreate);

        TestTableDefinition testApiTable3 = new TestTableDefinition(
                TABLE_NAME_WITH_DEFAULT_VALUES,
                DEFAULT_KEY,
                new Column[]{
                        new Column("VAL", NativeTypes.INT64, true),
                        new Column("STR", NativeTypes.stringOf(3), true),
                        new Column("BLOB", NativeTypes.blobOf(3), true)
                }
        );

        sql("CREATE TABLE " + testApiTable3.name + " ("
                + "ID BIGINT PRIMARY KEY, "
                + "VAL BIGINT DEFAULT 28, "
                + "STR VARCHAR(3) DEFAULT 'ABC', "
                + "BLOB VARBINARY(3) DEFAULT X'000102'"
                + ")");

        schemaAwareTestTables = Map.of(
                testApiTable1.name, testApiTable1,
                testApiTable2.name, testApiTable2,
                testApiTable3.name, testApiTable3
        );
    }

    @ParameterizedTest
    @MethodSource("apiTestCases")
    public void upsert(BinApiTestCase testCase) {
        RecordView<Tuple> tbl = testCase.view();
        String keyName = testCase.keyColumnName(0);
        String valName = testCase.valColumnName(0);

        Tuple tuple = Tuple.create().set(keyName, 1L).set(valName, 11L);
        Tuple newTuple = Tuple.create().set(keyName, 1L).set(valName, 22L);
        Tuple nonExistedTuple = Tuple.create().set(keyName, 2L);

        assertNull(tbl.get(null, Tuple.create().set(keyName, 1L)));

        // Insert new tuple.
        tbl.upsert(null, tuple);

        assertEqualsRows(testCase.schema(), tuple, tbl.get(null, Tuple.create().set(keyName, 1L)));

        // Update exited row.
        tbl.upsert(null, newTuple);

        assertEqualsRows(testCase.schema(), newTuple, tbl.get(null, Tuple.create().set(keyName, 1L)));

        assertNull(tbl.get(null, nonExistedTuple));
    }

    @ParameterizedTest
    @MethodSource("apiTestCases")
    public void getAndUpsert(BinApiTestCase testCase) {
        RecordView<Tuple> tbl = testCase.view();
        String keyName = testCase.keyColumnName(0);
        String valName = testCase.valColumnName(0);

        Tuple tuple = Tuple.create().set(keyName, 1L).set(valName, 11L);
        Tuple newTuple = Tuple.create().set(keyName, 1L).set(valName, 22L);

        assertNull(tbl.get(null, Tuple.create().set(keyName, 1L)));

        // Insert new tuple.
        assertNull(tbl.getAndUpsert(null, tuple));

        assertEqualsRows(testCase.schema(), tuple, tbl.get(null, Tuple.create().set(keyName, 1L)));

        // Update exited row.
        assertEqualsRows(testCase.schema(), tuple, tbl.getAndUpsert(null, newTuple));

        assertEqualsRows(testCase.schema(), newTuple, tbl.get(null, Tuple.create().set(keyName, 1L)));
    }

    @ParameterizedTest
    @MethodSource("apiTestCases")
    public void remove(BinApiTestCase testCase) {
        RecordView<Tuple> tbl = testCase.view();
        String keyName = testCase.keyColumnName(0);
        String valName = testCase.valColumnName(0);

        tbl.upsert(null, Tuple.create().set(keyName, 1L).set(valName, 11L));

        Tuple keyTuple = Tuple.create().set(keyName, 1L);

        // Delete not existed keyTuple.
        assertFalse(tbl.delete(null, Tuple.create().set(keyName, 2L)));

        // Delete existed keyTuple.
        assertTrue(tbl.delete(null, keyTuple));
        assertNull(tbl.get(null, keyTuple));

        // Delete already deleted keyTuple.
        assertFalse(tbl.delete(null, keyTuple));
    }

    @ParameterizedTest
    @MethodSource("apiTestCases")
    public void removeExact(BinApiTestCase testCase) {
        RecordView<Tuple> tbl = testCase.view();
        String keyName = testCase.keyColumnName(0);
        String valName = testCase.valColumnName(0);

        Tuple keyTuple = Tuple.create().set(keyName, 1L);
        Tuple tuple = Tuple.create().set(keyName, 1L).set(valName, 11L);
        Tuple tuple2 = Tuple.create().set(keyName, 1L).set(valName, 22L);
        Tuple nonExistedTuple = Tuple.create().set(keyName, 2L).set(valName, 22L);

        tbl.insert(null, tuple);

        assertEqualsRows(testCase.schema(), tuple, tbl.get(null, keyTuple));

        // Fails to delete not existed tuple.
        assertFalse(tbl.deleteExact(null, nonExistedTuple));
        assertEqualsRows(testCase.schema(), tuple, tbl.get(null, keyTuple));

        // Fails to delete tuple with unexpected value.
        assertFalse(tbl.deleteExact(null, tuple2));
        assertEqualsRows(testCase.schema(), tuple, tbl.get(null, keyTuple));

        assertFalse(tbl.deleteExact(null, keyTuple));
        assertEqualsRows(testCase.schema(), tuple, tbl.get(null, keyTuple));

        // Delete tuple with expected value.
        assertTrue(tbl.deleteExact(null, tuple));
        assertNull(tbl.get(null, keyTuple));

        // Once again.
        assertFalse(tbl.deleteExact(null, tuple));
        assertNull(tbl.get(null, keyTuple));

        // Insert new.
        tbl.insert(null, tuple2);
        assertEqualsRows(testCase.schema(), tuple2, tbl.get(null, keyTuple));

        // Delete tuple with expected value.
        assertTrue(tbl.deleteExact(null, tuple2));
        assertNull(tbl.get(null, keyTuple));
    }

    @ParameterizedTest
    @MethodSource("apiTestCases")
    public void replace(BinApiTestCase testCase) {
        RecordView<Tuple> tbl = testCase.view();
        String keyName = testCase.keyColumnName(0);
        String valName = testCase.valColumnName(0);

        Tuple keyTuple = Tuple.create().set(keyName, 1L);
        Tuple tuple = Tuple.create().set(keyName, 1L).set(valName, 11L);
        Tuple tuple2 = Tuple.create().set(keyName, 1L).set(valName, 22L);

        assertNull(tbl.get(null, keyTuple));

        // Ignore replace operation for non-existed row.
        assertFalse(tbl.replace(null, tuple));

        assertNull(tbl.get(null, keyTuple));

        // Insert row.
        tbl.insert(null, tuple);

        // Replace existed row.
        assertTrue(tbl.replace(null, tuple2));

        assertEqualsRows(testCase.schema(), tuple2, tbl.get(null, keyTuple));
    }

    @ParameterizedTest
    @MethodSource("apiTestCases")
    public void replaceExact(BinApiTestCase testCase) {
        RecordView<Tuple> tbl = testCase.view();
        String keyName = testCase.keyColumnName(0);
        String valName = testCase.valColumnName(0);

        Tuple keyTuple = Tuple.create().set(keyName, 1L);
        Tuple tuple = Tuple.create().set(keyName, 1L).set(valName, 11L);
        Tuple tuple2 = Tuple.create().set(keyName, 1L).set(valName, 22L);

        assertNull(tbl.get(null, keyTuple));

        // Ignore replace operation for non-existed row.
        assertFalse(tbl.replace(null, tuple));
        assertNull(tbl.get(null, keyTuple));

        // Insert row.
        assertTrue(tbl.insert(null, tuple));

        // Replace existed row.
        assertTrue(tbl.replace(null, tuple, tuple2));

        assertEqualsRows(testCase.schema(), tuple2, tbl.get(null, Tuple.create().set(keyName, 1L)));
    }

    @ParameterizedTest
    @MethodSource("schemaValidationTestCases")
    public void validateSchema(BinTestCase testCase) {
        sql("DELETE FROM " + TABLE_NAME_FOR_SCHEMA_VALIDATION);

        RecordView<Tuple> tbl = testCase.view();

        Tuple keyTuple0 = Tuple.create().set("id", 0).set("id1", 0);
        Tuple keyTuple1 = Tuple.create().set("id1", 0);
        Tuple tuple0 = Tuple.create().set("id", 1L).set("str", "qweqweqwe").set("val", 11L);
        Tuple tuple1 = Tuple.create().set("id", 1L).set("blob", new byte[]{0, 1, 2, 3}).set("val", 22L);

        testCase.checkValueTypeDoesNotMatchError(() -> tbl.get(null, keyTuple0));
        testCase.checkMissedKeyColumnError(() -> tbl.get(null, keyTuple1));
        testCase.checkKeyColumnContainsExtraColumns(() -> tbl.get(null, Tuple.create().set("id", 1L).set("val", 1L)));

        String strTooLongErr = "Value too long [column='STR', type=STRING(3)]";
        String byteArrayTooLongErr = "Value too long [column='BLOB', type=BYTE_ARRAY(3)]";

        testCase.checkInvalidTypeError(() -> tbl.replace(null, tuple0), strTooLongErr);
        testCase.checkInvalidTypeError(() -> tbl.replace(null, tuple1), byteArrayTooLongErr);

        testCase.checkInvalidTypeError(() -> tbl.insert(null, tuple0), strTooLongErr);
        testCase.checkInvalidTypeError(() -> tbl.insert(null, tuple1), byteArrayTooLongErr);

        testCase.checkInvalidTypeError(() -> tbl.replace(null, tuple0), strTooLongErr);
        testCase.checkInvalidTypeError(() -> tbl.replace(null, tuple1), byteArrayTooLongErr);
    }

    @ParameterizedTest
    @MethodSource("defaultValueTestCases")
    public void defaultValues(BinApiTestCase testCase) {
        sql("DELETE FROM " + TABLE_NAME_WITH_DEFAULT_VALUES);

        RecordView<Tuple> tbl = testCase.view();

        Tuple keyTuple0 = Tuple.create().set("id", 0L);
        Tuple keyTuple1 = Tuple.create().set("id", 1L);

        Tuple tuple0 = Tuple.create().set("id", 0L);
        Tuple tupleExpected0 = Tuple.create().set("id", 0L).set("val", 28L).set("str", "ABC").set("blob", new byte[]{0, 1, 2});
        Tuple tuple1 = Tuple.create().set("id", 1L).set("val", null).set("str", null).set("blob", null);

        tbl.insert(null, tuple0);
        tbl.insert(null, tuple1);

        assertEqualsRows(testCase.schema(), tupleExpected0, tbl.get(null, keyTuple0));
        assertEqualsRows(testCase.schema(), tuple1, tbl.get(null, keyTuple1));
    }

    @ParameterizedTest
    @MethodSource("apiTestCases")
    public void getAll(BinApiTestCase testCase) {
        RecordView<Tuple> tbl = testCase.view();
        String keyName = testCase.keyColumnName(0);
        String valName = testCase.valColumnName(0);

        Tuple rec1 = Tuple.create().set(keyName, 1L).set(valName, 11L);
        Tuple rec3 = Tuple.create().set(keyName, 3L).set(valName, 33L);

        testCase.checkNullKeyRecsError(() -> tbl.getAll(null, null));
        testCase.checkNullKeyError(() -> tbl.getAll(null, Arrays.asList(rec1, null)));
        testCase.checkNullRecsError(() -> tbl.upsertAll(null, null));
        testCase.checkNullRecError(() -> tbl.upsertAll(null, Arrays.asList(rec1, null)));

        tbl.upsertAll(null, List.of(rec1, rec3));

        Collection<Tuple> res = tbl.getAll(
                null,
                List.of(
                        Tuple.create().set(keyName, 1L),
                        Tuple.create().set(keyName, 2L),
                        Tuple.create().set(keyName, 3L)
                ));

        assertThat(res, Matchers.contains(rec1, null, rec3));
    }

    @ParameterizedTest
    @MethodSource("apiTestCases")
    public void contains(BinApiTestCase testCase) {
        RecordView<Tuple> tbl = testCase.view();
        String keyName = testCase.keyColumnName(0);
        String valName = testCase.valColumnName(0);

        final long keyId = 1L;
        Tuple rec = Tuple.create()
                .set(keyName, keyId)
                .set(valName, 11L);
        Tuple keyRec = Tuple.create()
                .set(keyName, keyId);

        tbl.insert(null, rec);
        assertTrue(tbl.contains(null, keyRec));
        assertFalse(tbl.contains(null, Tuple.create().set(keyName, -1L)));

        tbl.delete(null, keyRec);
        assertFalse(tbl.contains(null, keyRec));

        Tuple nullValRec = Tuple.create().set(keyName, 1L).set(valName, null);
        tbl.insert(null, nullValRec);
        assertTrue(tbl.contains(null, keyRec));
    }

    @ParameterizedTest
    @MethodSource("apiTestCases")
    public void containsAll(BinApiTestCase testCase) {
        RecordView<Tuple> recordView = testCase.view();
        String keyName = testCase.keyColumnName(0);
        String valName = testCase.valColumnName(0);

        long firstKey = 101L;
        Tuple firstKeyTuple = Tuple.create()
                .set(keyName, firstKey);
        Tuple firstValTuple = Tuple.create()
                .set(keyName, firstKey)
                .set(valName, 201L);

        long secondKey = 102L;
        Tuple secondKeyTuple = Tuple.create()
                .set(keyName, secondKey);
        Tuple secondValTuple = Tuple.create()
                .set(keyName, secondKey)
                .set(valName, 202L);

        long thirdKey = 103L;
        Tuple thirdKeyTuple = Tuple.create()
                .set(keyName, thirdKey);
        Tuple thirdValTuple = Tuple.create()
                .set(keyName, thirdKey)
                .set(valName, 203L);

        testCase.checkNullRecsError(() -> recordView.insertAll(null, null));
        testCase.checkNullRecError(() -> recordView.insertAll(null, Arrays.asList(firstKeyTuple, null)));

        List<Tuple> recs = List.of(firstValTuple, secondValTuple, thirdValTuple);

        recordView.insertAll(null, recs);

        testCase.checkNullKeysError(() -> recordView.containsAll(null, null));
        testCase.checkNullKeyError(() -> recordView.containsAll(null, Arrays.asList(firstKeyTuple, null, thirdKeyTuple)));

        assertTrue(recordView.containsAll(null, List.of()));
        assertTrue(recordView.containsAll(null, List.of(firstKeyTuple)));
        assertTrue(recordView.containsAll(null, List.of(firstKeyTuple, secondKeyTuple, thirdKeyTuple)));

        long missedKey = 0L;
        Tuple missedKeyTuple = Tuple.create()
                .set(keyName, missedKey);

        assertFalse(recordView.containsAll(null, List.of(missedKeyTuple)));
        assertFalse(recordView.containsAll(null, List.of(firstKeyTuple, secondKeyTuple, missedKeyTuple)));
    }

    @ParameterizedTest
    @MethodSource("apiTestCases")
    public void upsertAllAfterInsertAll(BinApiTestCase testCase) {
        RecordView<Tuple> tbl = testCase.view();
        String keyName = testCase.keyColumnName(0);
        String valName = testCase.valColumnName(0);

        Tuple rec1 = Tuple.create().set(keyName, 1L).set(valName, 11L);
        Tuple rec3 = Tuple.create().set(keyName, 3L).set(valName, 33L);

        tbl.insertAll(null, List.of(rec1, rec3));

        Collection<Tuple> res = tbl.getAll(
                null,
                List.of(
                        Tuple.create().set(keyName, 1L),
                        Tuple.create().set(keyName, 2L),
                        Tuple.create().set(keyName, 3L)
                ));

        assertThat(res, Matchers.contains(rec1, null, rec3));

        Tuple upRec1 = Tuple.create().set(keyName, 1L).set(valName, 112L);
        Tuple rec2 = Tuple.create().set(keyName, 2L).set(valName, 22L);
        Tuple upRec3 = Tuple.create().set(keyName, 3L).set(valName, 332L);

        tbl.upsertAll(null, List.of(upRec1, rec2, upRec3));

        res = tbl.getAll(
                null,
                List.of(
                        Tuple.create().set(keyName, 1L),
                        Tuple.create().set(keyName, 2L),
                        Tuple.create().set(keyName, 3L)
                ));

        assertThat(res, Matchers.contains(upRec1, rec2, upRec3));
    }

    @ParameterizedTest
    @MethodSource("apiTestCases")
    public void deleteVsDeleteExact(BinApiTestCase testCase) {
        RecordView<Tuple> tbl = testCase.view();
        String keyName = testCase.keyColumnName(0);
        String valName = testCase.valColumnName(0);

        Tuple rec = Tuple.create().set(keyName, 1L).set(valName, 11L);
        Tuple recReplace = Tuple.create().set(keyName, 1L).set(valName, 12L);

        tbl.insert(null, rec);

        tbl.upsert(null, recReplace);

        assertFalse(tbl.deleteExact(null, rec));
        assertTrue(tbl.deleteExact(null, recReplace));

        tbl.upsert(null, recReplace);

        assertTrue(tbl.delete(null, Tuple.create().set(keyName, 1L)));

        assertNull(tbl.get(null, Tuple.create().set(keyName, 1L)));
    }

    @ParameterizedTest
    @MethodSource("apiTestCases")
    public void getAndReplace(BinApiTestCase testCase) {
        RecordView<Tuple> tbl = testCase.view();
        String keyName = testCase.keyColumnName(0);
        String valName = testCase.valColumnName(0);

        long val = 0;

        tbl.insert(null, Tuple.create().set(keyName, 1L).set(valName, val));

        for (int i = 1; i < 10; i++) {
            val = i;

            assertEquals(
                    val - 1,
                    tbl.getAndReplace(null, Tuple.create().set(keyName, 1L).set(valName, val))
                            .longValue(1)
            );
        }
    }

    @ParameterizedTest
    @MethodSource("apiTestCases")
    public void getAndDelete(BinApiTestCase testCase) {
        RecordView<Tuple> tbl = testCase.view();
        String keyName = testCase.keyColumnName(0);
        String valName = testCase.valColumnName(0);

        Tuple tuple = Tuple.create().set(keyName, 1L).set(valName, 1L);

        tbl.insert(null, tuple);

        Tuple removedTuple = tbl.getAndDelete(null, Tuple.create().set(keyName, 1L));

        assertEquals(tuple, removedTuple);

        assertNull(tbl.getAndDelete(null, Tuple.create().set(keyName, 1L)));
    }

    @ParameterizedTest
    @MethodSource("apiTestCases")
    public void deleteAll(BinApiTestCase testCase) {
        RecordView<Tuple> tbl = testCase.view();
        String keyName = testCase.keyColumnName(0);
        String valName = testCase.valColumnName(0);

        Tuple tuple1 = Tuple.create().set(keyName, 1L).set(valName, 11L);
        Tuple tuple2 = Tuple.create().set(keyName, 2L).set(valName, 22L);
        Tuple tuple3 = Tuple.create().set(keyName, 3L).set(valName, 33L);

        tbl.insertAll(null, List.of(tuple1, tuple2, tuple3));

        Collection<Tuple> current = tbl.getAll(
                null,
                List.of(
                        Tuple.create().set(keyName, 1L),
                        Tuple.create().set(keyName, 2L),
                        Tuple.create().set(keyName, 3L)
                ));

        assertEquals(3, current.size());

        assertTrue(current.contains(tuple1));
        assertTrue(current.contains(tuple2));
        assertTrue(current.contains(tuple3));

        Collection<Tuple> notRemovedTuples = tbl.deleteAll(
                null,
                List.of(
                        Tuple.create().set(keyName, 1L),
                        Tuple.create().set(keyName, 3L),
                        Tuple.create().set(keyName, 4L)
                )
        );

        assertEquals(1, notRemovedTuples.size());
        assertTrue(notRemovedTuples.contains(Tuple.create().set(keyName, 4L)));

        current = tbl.getAll(
                null,
                List.of(
                        Tuple.create().set(keyName, 1L),
                        Tuple.create().set(keyName, 2L),
                        Tuple.create().set(keyName, 3L)
                ));

        assertThat(current, Matchers.contains(null, tuple2, null));
    }

    @ParameterizedTest
    @MethodSource("apiTestCases")
    public void deleteExact(BinApiTestCase testCase) {
        RecordView<Tuple> tbl = testCase.view();
        String keyName = testCase.keyColumnName(0);
        String valName = testCase.valColumnName(0);

        Tuple tuple1 = Tuple.create().set(keyName, 1L).set(valName, 11L);
        Tuple tuple2 = Tuple.create().set(keyName, 2L).set(valName, 22L);
        Tuple tuple3 = Tuple.create().set(keyName, 3L).set(valName, 33L);

        tbl.insertAll(null, List.of(tuple1, tuple2, tuple3));

        Collection<Tuple> current = tbl.getAll(
                null,
                List.of(
                        Tuple.create().set(keyName, 1L),
                        Tuple.create().set(keyName, 2L),
                        Tuple.create().set(keyName, 3L)
                ));

        assertEquals(3, current.size());

        assertTrue(current.contains(tuple1));
        assertTrue(current.contains(tuple2));
        assertTrue(current.contains(tuple3));

        Tuple tuple3Upsert = Tuple.create().set(keyName, 3L).set(valName, 44L);

        tbl.upsert(null, tuple3Upsert);

        Tuple tuple4NotExists = Tuple.create().set(keyName, 4L).set(valName, 55L);

        Collection<Tuple> notRemovedTuples = tbl.deleteAllExact(null,
                List.of(tuple1, tuple2, tuple3, tuple4NotExists));

        assertEquals(2, notRemovedTuples.size());
        assertTrue(notRemovedTuples.contains(tuple3));
        assertTrue(notRemovedTuples.contains(tuple4NotExists));

        current = tbl.getAll(
                null,
                List.of(
                        Tuple.create().set(keyName, 1L),
                        Tuple.create().set(keyName, 2L),
                        Tuple.create().set(keyName, 3L)
                ));

        assertThat(current, Matchers.contains(null, null, tuple3Upsert));
    }

    @ParameterizedTest
    @MethodSource("apiTestCases")
    public void getAndReplaceVsGetAndUpsert(BinApiTestCase testCase) {
        RecordView<Tuple> tbl = testCase.view();
        String keyName = testCase.keyColumnName(0);
        String valName = testCase.valColumnName(0);

        Tuple tuple1 = Tuple.create().set(keyName, 1L).set(valName, 11L);

        assertNull(tbl.getAndUpsert(null, tuple1));

        Tuple tuple = tbl.get(null, Tuple.create().set(keyName, 1L));

        assertNotNull(tuple);

        assertEquals(tuple, tuple1);

        assertTrue(tbl.deleteExact(null, tuple));

        assertNull(tbl.getAndReplace(null, tuple));

        assertNull(tbl.get(null, Tuple.create().set(keyName, 1L)));
    }

    @ParameterizedTest
    @MethodSource("compoundPkTestCases")
    void schemaMismatch(BinTestCase testCase) {
        RecordView<Tuple> recordView = testCase.view();

        // TODO https://issues.apache.org/jira/browse/IGNITE-21793 Thin client must throw exception
        if (!testCase.thin) {
            testCase.checkSchemaMismatchError(
                    () -> recordView.get(null, Tuple.create().set("id", 0L).set("affId", 1L).set("val", 0L)),
                    "Key tuple contains extra columns: [VAL]"
            );
        }

        testCase.checkSchemaMismatchError(
                () -> recordView.get(null, Tuple.create().set("id", 0L)),
                "Missed key column: AFFID"
        );
    }

    @ParameterizedTest
    @MethodSource("typeMismatchTestCases")
    public void typeMismatch(BinTestCase testCase) {
        RecordView<Tuple> tbl = testCase.view();

        // Check not-nullable column.
        testCase.checkSchemaMismatchError(
                () -> tbl.insert(null, Tuple.create().set("id", null)),
                "Column 'ID' does not allow NULLs"
        );

        testCase.checkInvalidTypeError(
                () -> tbl.insert(null, Tuple.create().set("id", 0L).set("valString", "qweqwe")),
                "Value too long [column='VALSTRING', type=STRING(3)]"
        );

        testCase.checkInvalidTypeError(
                () -> tbl.insert(null, Tuple.create().set("id", 0L).set("valBytes", new byte[]{0, 1, 2, 3})),
                "Value too long [column='VALBYTES', type=BYTE_ARRAY(3)]"
        );
    }

    @ParameterizedTest
    @MethodSource("stringTypeMatchTestCases")
    public void stringTypeMatch(BinTestCase testCase) {
        try {
            RecordView<Tuple> view = testCase.view();

            Tuple tuple = Tuple.create().set("id", 1L);

            view.insert(null, tuple.set("valString", "qwe"));
            view.insert(null, tuple.set("valString", "qw"));
            view.insert(null, tuple.set("valString", "q"));
            view.insert(null, tuple.set("valString", ""));
            view.insert(null, tuple.set("valString", null));

            // Check string 3 char length and 9 bytes.
            view.insert(null, tuple.set("valString", "我是谁"));

            testCase.checkInvalidTypeError(() -> view.insert(null, tuple.set("valString", "我是谁我")),
                    "Value too long [column='VALSTRING', type=STRING(3)]");
        } finally {
            sql("DELETE FROM " + TABLE_STRING_TYPE_MATCH);
        }
    }

    @ParameterizedTest
    @MethodSource("byteTypeMatchTestCases")
    public void bytesTypeMatch(BinTestCase testCase) {
        try {
            RecordView<Tuple> recordView = testCase.view();

            Tuple tuple = Tuple.create().set("id", 1L);

            recordView.insert(null, tuple.set("valUnlimited", null));
            recordView.insert(null, tuple.set("valLimited", null));
            recordView.insert(null, tuple.set("valUnlimited", new byte[2]));
            recordView.insert(null, tuple.set("valLimited", new byte[2]));
            recordView.insert(null, tuple.set("valUnlimited", new byte[3]));

            testCase.checkInvalidTypeError(
                    () -> recordView.insert(null, tuple.set("valLimited", new byte[3])),
                    "Value too long [column='VALLIMITED', type=BYTE_ARRAY(2)]"
            );
        } finally {
            sql("DELETE FROM " + TABLE_BYTE_TYPE_MATCH);
        }
    }

    /**
     * Check tuples equality.
     *
     * @param expected Expected tuple.
     * @param actual Actual tuple.
     */
    private static void assertEqualsRows(SchemaDescriptor schema, Tuple expected, Tuple actual) {
        assertEqualsKeys(schema, expected, actual);
        assertEqualsValues(schema, expected, actual);

        assertTrue(Tuple.equals(expected, actual));
    }

    /**
     * Check key columns equality.
     *
     * @param schema   Schema.
     * @param expected Expected tuple.
     * @param actual   Actual tuple.
     */
    private static void assertEqualsKeys(SchemaDescriptor schema, Tuple expected, Tuple actual) {
        int nonNullKey = 0;

        for (int i = 0; i < schema.keyColumns().size(); i++) {
            Column col = schema.keyColumns().get(i);

            Object val1 = expected.value(IgniteNameUtils.quoteIfNeeded(col.name()));
            Object val2 = actual.value(IgniteNameUtils.quoteIfNeeded(col.name()));

            assertEquals(val1, val2, "Value columns equality check failed: " + col);

            if (col.positionInKey() != -1 && val1 != null) {
                nonNullKey++;
            }
        }

        assertTrue(nonNullKey > 0, "At least one non-null key column must exist.");
    }

    private List<Arguments> apiTestCases() {
        List<Arguments> args1 = generateRecordViewTestArguments(TABLE_NAME_API_TEST, Tuple.class);
        List<Arguments> args2 = generateRecordViewTestArguments(TABLE_NAME_API_TEST_QUOTED, Tuple.class, " (quoted names)");

        args1.addAll(args2);

        return args1;
    }

    private List<Arguments> schemaValidationTestCases() {
        return generateRecordViewTestArguments(TABLE_NAME_FOR_SCHEMA_VALIDATION, Tuple.class);
    }

    private List<Arguments> defaultValueTestCases() {
        return generateRecordViewTestArguments(TABLE_NAME_WITH_DEFAULT_VALUES, Tuple.class);
    }

    private List<Arguments> compoundPkTestCases() {
        return generateRecordViewTestArguments(TABLE_COMPOUND_KEY, Tuple.class);
    }

    private List<Arguments> typeMismatchTestCases() {
        return generateRecordViewTestArguments(TABLE_TYPE_MISMATCH, Tuple.class);
    }

    private List<Arguments> stringTypeMatchTestCases() {
        return generateRecordViewTestArguments(TABLE_STRING_TYPE_MATCH, Tuple.class);
    }

    private List<Arguments> byteTypeMatchTestCases() {
        return generateRecordViewTestArguments(TABLE_BYTE_TYPE_MATCH, Tuple.class);
    }

    @Override
    TestCaseFactory getFactory(String name) {
        return new TestCaseFactory(name) {
            @Override
            <V> TestCase<V> create(boolean async, boolean thin, Class<V> recordClass) {
                RecordView<Tuple> view = thin
                        ? client.tables().table(tableName).recordView()
                        : CLUSTER.aliveNode().tables().table(tableName).recordView();

                if (async) {
                    view = new AsyncApiRecordViewAdapter<>(view);
                }

                if (schemaAwareTestTables.containsKey(name)) {
                    return (TestCase<V>) new BinApiTestCase(async, thin, view, schemaAwareTestTables.get(name));
                }

                return (TestCase<V>) new BinTestCase(async, thin, view);
            }
        };
    }

    static class BinTestCase extends TestCase<Tuple> {
        BinTestCase(boolean async, boolean thin, RecordView<Tuple> view) {
            super(async, thin, view);
        }

        void checkValueTypeDoesNotMatchError(Executable run) {
            String expectedMessage = "Value type does not match [column='ID', expected=INT64, actual=INT32]";

            if (thin) {
                IgniteException ex = (IgniteException) IgniteTestUtils.assertThrows(IgniteException.class, run, expectedMessage);

                assertThat(ex.code(), is(Client.PROTOCOL_ERR));
            } else {
                //noinspection ThrowableNotThrown
                IgniteTestUtils.assertThrowsWithCause(run::execute, SchemaMismatchException.class, expectedMessage);
            }
        }

        @SuppressWarnings("ThrowableNotThrown")
        void checkMissedKeyColumnError(Executable run) {
            String expectedMessage = "Missed key column: ID";

            if (thin) {
                IgniteTestUtils.assertThrows(MarshallerException.class, run, expectedMessage);
            } else {
                IgniteTestUtils.assertThrowsWithCause(run::execute, SchemaMismatchException.class, expectedMessage);
            }
        }

        @SuppressWarnings("ThrowableNotThrown")
        void checkInvalidTypeError(Executable run, String expectedMessage) {
            if (thin) {
                IgniteTestUtils.assertThrows(MarshallerException.class, run, expectedMessage);
            } else {
                IgniteTestUtils.assertThrowsWithCause(run::execute, InvalidTypeException.class, expectedMessage);
            }
        }

        @SuppressWarnings("ThrowableNotThrown")
        void checkKeyColumnContainsExtraColumns(Executable run) {
            // TODO https://issues.apache.org/jira/browse/IGNITE-21793 Thin client must also throw exception
            if (!thin) {
                IgniteTestUtils.assertThrowsWithCause(run::execute, SchemaMismatchException.class,
                        "Key tuple contains extra columns: [VAL]");
            }
        }

        @SuppressWarnings("ThrowableNotThrown")
        void checkSchemaMismatchError(Executable run, String expectedMessage) {
            if (thin) {
                IgniteTestUtils.assertThrows(IgniteException.class, run, expectedMessage);
            } else {
                IgniteTestUtils.assertThrowsWithCause(run::execute, SchemaMismatchException.class, expectedMessage);
            }
        }
    }

    static class BinApiTestCase extends BinTestCase {
        final List<String> keyColumns;

        final List<String> valueColumns;

        final SchemaDescriptor schema;

        String keyColumnName(int index) {
            return keyColumns.get(index);
        }

        String valColumnName(int index) {
            return valueColumns.get(index);
        }

        public SchemaDescriptor schema() {
            return schema;
        }

        BinApiTestCase(boolean async, boolean thin, RecordView<Tuple> view, TestTableDefinition tableDefinition) {
            super(async, thin, view);

            this.keyColumns = quoteOrLowercaseNames(tableDefinition.schemaDescriptor.keyColumns());
            this.valueColumns = quoteOrLowercaseNames(tableDefinition.schemaDescriptor.valueColumns());
            this.schema = tableDefinition.schemaDescriptor;
        }
    }
}
