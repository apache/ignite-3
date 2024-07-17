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

import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.CompletableFuture.failedFuture;
import static org.apache.ignite.internal.schema.DefaultValueProvider.constantProvider;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.Collection;
import java.util.List;
import org.apache.ignite.internal.marshaller.ReflectionMarshallersProvider;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.InvalidTypeException;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.SchemaMismatchException;
import org.apache.ignite.internal.schema.marshaller.TupleMarshallerImpl;
import org.apache.ignite.internal.table.distributed.replicator.InternalSchemaVersionMismatchException;
import org.apache.ignite.internal.table.impl.DummySchemaManagerImpl;
import org.apache.ignite.internal.table.impl.TestTupleBuilder;
import org.apache.ignite.internal.type.NativeTypes;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.sql.IgniteSql;
import org.apache.ignite.table.RecordView;
import org.apache.ignite.table.Tuple;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

/**
 * Basic table operations test.
 */
public class RecordBinaryViewOperationsTest extends TableKvOperationsTestBase {
    @Test
    public void insert() {
        SchemaDescriptor schema = schemaDescriptor();

        RecordView<Tuple> tbl = createTable(schema).recordView();

        final Tuple tuple = Tuple.create().set("id", 1L).set("val", 11L);
        final Tuple newTuple = Tuple.create().set("id", 1L).set("val", 22L);
        final Tuple nonExistedTuple = Tuple.create().set("id", 2L);

        assertNull(tbl.get(null, Tuple.create().set("id", 1L)));

        // Insert new tuple.
        assertTrue(tbl.insert(null, tuple));

        assertEqualsRows(schema, tuple, tbl.get(null, Tuple.create().set("id", 1L)));

        // Ignore insert operation for exited row.
        assertFalse(tbl.insert(null, newTuple));

        assertEqualsRows(schema, tuple, tbl.get(null, Tuple.create().set("id", 1L)));

        assertNull(tbl.get(null, nonExistedTuple));
    }

    private static SchemaDescriptor schemaDescriptor() {
        return new SchemaDescriptor(
                SCHEMA_VERSION,
                new Column[]{new Column("id".toUpperCase(), NativeTypes.INT64, false)},
                new Column[]{new Column("val".toUpperCase(), NativeTypes.INT64, true)}
        );
    }

    @Test
    public void upsert() {
        SchemaDescriptor schema = schemaDescriptor();

        RecordView<Tuple> tbl = createTable(schema).recordView();

        final Tuple tuple = Tuple.create().set("id", 1L).set("val", 11L);
        final Tuple newTuple = Tuple.create().set("id", 1L).set("val", 22L);
        final Tuple nonExistedTuple = Tuple.create().set("id", 2L);

        assertNull(tbl.get(null, Tuple.create().set("id", 1L)));

        // Insert new tuple.
        tbl.upsert(null, tuple);

        assertEqualsRows(schema, tuple, tbl.get(null, Tuple.create().set("id", 1L)));

        // Update exited row.
        tbl.upsert(null, newTuple);

        assertEqualsRows(schema, newTuple, tbl.get(null, Tuple.create().set("id", 1L)));

        assertNull(tbl.get(null, nonExistedTuple));
    }

    @Test
    public void getAndUpsert() {
        SchemaDescriptor schema = schemaDescriptor();

        RecordView<Tuple> tbl = createTable(schema).recordView();

        final Tuple tuple = Tuple.create().set("id", 1L).set("val", 11L);
        final Tuple newTuple = Tuple.create().set("id", 1L).set("val", 22L);

        assertNull(tbl.get(null, Tuple.create().set("id", 1L)));

        // Insert new tuple.
        assertNull(tbl.getAndUpsert(null, tuple));

        assertEqualsRows(schema, tuple, tbl.get(null, Tuple.create().set("id", 1L)));

        // Update exited row.
        assertEqualsRows(schema, tuple, tbl.getAndUpsert(null, newTuple));

        assertEqualsRows(schema, newTuple, tbl.get(null, Tuple.create().set("id", 1L)));
    }

    @Test
    public void remove() {
        SchemaDescriptor schema = schemaDescriptor();

        RecordView<Tuple> tbl = createTable(schema).recordView();

        tbl.upsert(null, Tuple.create().set("id", 1L).set("val", 11L));

        final Tuple keyTuple = Tuple.create().set("id", 1L);

        // Delete not existed keyTuple.
        assertFalse(tbl.delete(null, Tuple.create().set("id", 2L)));

        // Delete existed keyTuple.
        assertTrue(tbl.delete(null, keyTuple));
        assertNull(tbl.get(null, keyTuple));

        // Delete already deleted keyTuple.
        assertFalse(tbl.delete(null, keyTuple));
    }

    @Test
    public void removeExact() {
        SchemaDescriptor schema = schemaDescriptor();

        RecordView<Tuple> tbl = createTable(schema).recordView();

        final Tuple keyTuple = Tuple.create().set("id", 1L);
        final Tuple tuple = Tuple.create().set("id", 1L).set("val", 11L);
        final Tuple tuple2 = Tuple.create().set("id", 1L).set("val", 22L);
        final Tuple nonExistedTuple = Tuple.create().set("id", 2L).set("val", 22L);

        tbl.insert(null, tuple);

        assertEqualsRows(schema, tuple, tbl.get(null, keyTuple));

        // Fails to delete not existed tuple.
        assertFalse(tbl.deleteExact(null, nonExistedTuple));
        assertEqualsRows(schema, tuple, tbl.get(null, keyTuple));

        // Fails to delete tuple with unexpected value.
        assertFalse(tbl.deleteExact(null, tuple2));
        assertEqualsRows(schema, tuple, tbl.get(null, keyTuple));

        // TODO: IGNITE-14479: Fix default value usage.
        //        assertFalse(tbl.deleteExact(keyTuple));
        //        assertEqualsRows(schema, tuple, tbl.get(keyTuple));

        // Delete tuple with expected value.
        assertTrue(tbl.deleteExact(null, tuple));
        assertNull(tbl.get(null, keyTuple));

        // Once again.
        assertFalse(tbl.deleteExact(null, tuple));
        assertNull(tbl.get(null, keyTuple));

        // Insert new.
        tbl.insert(null, tuple2);
        assertEqualsRows(schema, tuple2, tbl.get(null, keyTuple));

        // Delete tuple with expected value.
        assertTrue(tbl.deleteExact(null, tuple2));
        assertNull(tbl.get(null, keyTuple));
    }

    @Test
    public void replace() {
        SchemaDescriptor schema = schemaDescriptor();

        RecordView<Tuple> tbl = createTable(schema).recordView();

        final Tuple keyTuple = Tuple.create().set("id", 1L);
        final Tuple tuple = Tuple.create().set("id", 1L).set("val", 11L);
        final Tuple tuple2 = Tuple.create().set("id", 1L).set("val", 22L);

        assertNull(tbl.get(null, keyTuple));

        // Ignore replace operation for non-existed row.
        assertFalse(tbl.replace(null, tuple));

        assertNull(tbl.get(null, keyTuple));

        // Insert row.
        tbl.insert(null, tuple);

        // Replace existed row.
        assertTrue(tbl.replace(null, tuple2));

        assertEqualsRows(schema, tuple2, tbl.get(null, keyTuple));
    }

    @Test
    public void replaceExact() {
        SchemaDescriptor schema = schemaDescriptor();

        RecordView<Tuple> tbl = createTable(schema).recordView();

        final Tuple tuple = Tuple.create().set("id", 1L).set("val", 11L);
        final Tuple tuple2 = Tuple.create().set("id", 1L).set("val", 22L);

        assertNull(tbl.get(null, Tuple.create().set("id", 1L)));

        // Ignore replace operation for non-existed row.
        // TODO: IGNITE-14479: Fix default value usage.
        //        assertTrue(tbl.replace(keyTuple, tuple));

        //        assertNull(tbl.get(keyTuple));
        //        assertNull(tbl.get(tbl.tupleBuilder().set("id", 1L).set("val", -1)));

        // Insert row.
        tbl.insert(null, tuple);

        // Replace existed row.
        assertTrue(tbl.replace(null, tuple, tuple2));

        assertEqualsRows(schema, tuple2, tbl.get(null, Tuple.create().set("id", 1L)));
    }

    @Test
    public void validateSchema() {
        SchemaDescriptor schema = new SchemaDescriptor(
                SCHEMA_VERSION,
                new Column[]{new Column("id".toUpperCase(), NativeTypes.INT64, false)},
                new Column[]{
                        new Column("val".toUpperCase(), NativeTypes.INT64, true),
                        new Column("str".toUpperCase(), NativeTypes.stringOf(3), true),
                        new Column("blob".toUpperCase(), NativeTypes.blobOf(3), true)
                }
        );

        RecordView<Tuple> tbl = createTable(schema).recordView();

        final Tuple keyTuple0 = new TestTupleBuilder().set("id", 0).set("id1", 0);
        final Tuple keyTuple1 = new TestTupleBuilder().set("id1", 0);
        final Tuple tuple0 = new TestTupleBuilder().set("id", 1L).set("str", "qweqweqwe").set("val", 11L);
        final Tuple tuple1 = new TestTupleBuilder().set("id", 1L).set("blob", new byte[]{0, 1, 2, 3}).set("val", 22L);

        assertThrowsWithCause(InvalidTypeException.class, () -> tbl.get(null, keyTuple0));
        assertThrowsWithCause(SchemaMismatchException.class, () -> tbl.get(null, keyTuple1));

        assertThrowsWithCause(InvalidTypeException.class, () -> tbl.replace(null, tuple0));
        assertThrowsWithCause(InvalidTypeException.class, () -> tbl.replace(null, tuple1));

        assertThrowsWithCause(InvalidTypeException.class, () -> tbl.insert(null, tuple0));
        assertThrowsWithCause(InvalidTypeException.class, () -> tbl.insert(null, tuple1));

        assertThrowsWithCause(InvalidTypeException.class, () -> tbl.replace(null, tuple0));
        assertThrowsWithCause(InvalidTypeException.class, () -> tbl.replace(null, tuple1));
    }

    @Test
    public void defaultValues() {
        SchemaDescriptor schema = new SchemaDescriptor(
                SCHEMA_VERSION,
                new Column[]{new Column("id".toUpperCase(), NativeTypes.INT64, false)},
                new Column[]{
                        new Column("val".toUpperCase(), NativeTypes.INT64, true, constantProvider(28L)),
                        new Column("str".toUpperCase(), NativeTypes.stringOf(3), true, constantProvider("ABC")),
                        new Column("blob".toUpperCase(), NativeTypes.blobOf(3), true, constantProvider(new byte[]{0, 1, 2}))
                }
        );

        RecordView<Tuple> tbl = createTable(schema).recordView();

        final Tuple keyTuple0 = Tuple.create().set("id", 0L);
        final Tuple keyTuple1 = Tuple.create().set("id", 1L);

        final Tuple tuple0 = Tuple.create().set("id", 0L);
        final Tuple tupleExpected0 = Tuple.create().set("id", 0L).set("val", 28L).set("str", "ABC").set("blob", new byte[]{0, 1, 2});
        final Tuple tuple1 = Tuple.create().set("id", 1L).set("val", null).set("str", null).set("blob", null);

        tbl.insert(null, tuple0);
        tbl.insert(null, tuple1);

        assertEqualsRows(schema, tupleExpected0, tbl.get(null, keyTuple0));
        assertEqualsRows(schema, tuple1, tbl.get(null, keyTuple1));
    }

    @Test
    public void getAll() {
        SchemaDescriptor schema = schemaDescriptor();

        RecordView<Tuple> tbl = createTable(schema).recordView();

        Tuple rec1 = Tuple.create().set("id", 1L).set("val", 11L);
        Tuple rec3 = Tuple.create().set("id", 3L).set("val", 33L);

        tbl.upsertAll(null, List.of(rec1, rec3));

        Collection<Tuple> res = tbl.getAll(
                null,
                List.of(
                        Tuple.create().set("id", 1L),
                        Tuple.create().set("id", 2L),
                        Tuple.create().set("id", 3L)
                ));

        assertThat(res, contains(rec1, null, rec3));
    }

    @Test
    public void testContains() {
        SchemaDescriptor schema = schemaDescriptor();
        RecordView<Tuple> tbl = createTable(schema).recordView();

        final long keyId = 1L;
        Tuple rec = Tuple.create()
                .set("id", keyId)
                .set("val", 11L);
        Tuple keyRec = Tuple.create()
                .set("id", keyId);

        tbl.insert(null, rec);
        assertTrue(tbl.contains(null, keyRec));
        assertFalse(tbl.contains(null, Tuple.create().set("id", -1L)));

        tbl.delete(null, keyRec);
        assertFalse(tbl.contains(null, keyRec));

        Tuple nullValRec = Tuple.create().set("id", 1L).set("val", null);
        tbl.insert(null, nullValRec);
        assertTrue(tbl.contains(null, keyRec));
    }

    @Test
    public void testContainsAll() {
        SchemaDescriptor schema = schemaDescriptor();
        RecordView<Tuple> recordView = createTable(schema).recordView();

        long firstKey = 101L;
        Tuple firstKeyTuple = Tuple.create()
                .set("id", firstKey);
        Tuple firstValTuple = Tuple.create()
                .set("id", firstKey)
                .set("val", 201L);

        long secondKey = 102L;
        Tuple secondKeyTuple = Tuple.create()
                .set("id", secondKey);
        Tuple secondValTuple = Tuple.create()
                .set("id", secondKey)
                .set("val", 202L);

        long thirdKey = 103L;
        Tuple thirdKeyTuple = Tuple.create()
                .set("id", thirdKey);
        Tuple thirdValTuple = Tuple.create()
                .set("id", thirdKey)
                .set("val", 203L);

        List<Tuple> recs = List.of(firstValTuple, secondValTuple, thirdValTuple);

        recordView.insertAll(null, recs);

        assertThrows(NullPointerException.class, () -> recordView.containsAll(null, null));
        assertThrows(NullPointerException.class, () -> recordView.containsAll(null, List.of(firstKeyTuple, null, thirdKeyTuple)));

        assertTrue(recordView.containsAll(null, List.of()));
        assertTrue(recordView.containsAll(null, List.of(firstKeyTuple)));
        assertTrue(recordView.containsAll(null, List.of(firstKeyTuple, secondKeyTuple, thirdKeyTuple)));

        long missedKey = 0L;
        Tuple missedKeyTuple = Tuple.create()
                .set("id", missedKey);

        assertFalse(recordView.containsAll(null, List.of(missedKeyTuple)));
        assertFalse(recordView.containsAll(null, List.of(firstKeyTuple, secondKeyTuple, missedKeyTuple)));
    }

    @Test
    public void upsertAllAfterInsertAll() {
        SchemaDescriptor schema = schemaDescriptor();

        RecordView<Tuple> tbl = createTable(schema).recordView();

        Tuple rec1 = Tuple.create().set("id", 1L).set("val", 11L);
        Tuple rec3 = Tuple.create().set("id", 3L).set("val", 33L);

        tbl.insertAll(null, List.of(rec1, rec3));

        Collection<Tuple> res = tbl.getAll(
                null,
                List.of(
                        Tuple.create().set("id", 1L),
                        Tuple.create().set("id", 2L),
                        Tuple.create().set("id", 3L)
                ));

        assertThat(res, contains(rec1, null, rec3));

        Tuple upRec1 = Tuple.create().set("id", 1L).set("val", 112L);
        Tuple rec2 = Tuple.create().set("id", 2L).set("val", 22L);
        Tuple upRec3 = Tuple.create().set("id", 3L).set("val", 332L);

        tbl.upsertAll(null, List.of(upRec1, rec2, upRec3));

        res = tbl.getAll(
                null,
                List.of(
                        Tuple.create().set("id", 1L),
                        Tuple.create().set("id", 2L),
                        Tuple.create().set("id", 3L)
                ));

        assertThat(res, contains(upRec1, rec2, upRec3));
    }

    @Test
    public void deleteVsDeleteExact() {
        SchemaDescriptor schema = schemaDescriptor();

        RecordView<Tuple> tbl = createTable(schema).recordView();

        Tuple rec = Tuple.create().set("id", 1L).set("val", 11L);
        Tuple recReplace = Tuple.create().set("id", 1L).set("val", 12L);

        tbl.insert(null, rec);

        tbl.upsert(null, recReplace);

        assertFalse(tbl.deleteExact(null, rec));
        assertTrue(tbl.deleteExact(null, recReplace));

        tbl.upsert(null, recReplace);

        assertTrue(tbl.delete(null, Tuple.create().set("id", 1L)));

        assertNull(tbl.get(null, Tuple.create().set("id", 1L)));
    }

    @Test
    public void getAndReplace() {
        SchemaDescriptor schema = new SchemaDescriptor(
                SCHEMA_VERSION,
                new Column[]{new Column("id".toUpperCase(), NativeTypes.INT64, false)},
                new Column[]{new Column("val".toUpperCase(), NativeTypes.INT32, false)}
        );

        RecordView<Tuple> tbl = createTable(schema).recordView();

        int val = 0;

        tbl.insert(null, Tuple.create().set("id", 1L).set("val", val));

        for (int i = 1; i < 100; i++) {
            val = i;

            assertEquals(
                    val - 1,
                    tbl.getAndReplace(null, Tuple.create().set("id", 1L).set("val", val))
                            .intValue(1)
            );
        }
    }

    @Test
    public void getAndDelete() {
        SchemaDescriptor schema = new SchemaDescriptor(
                SCHEMA_VERSION,
                new Column[]{new Column("id".toUpperCase(), NativeTypes.INT64, false)},
                new Column[]{new Column("val".toUpperCase(), NativeTypes.INT32, false)}
        );

        RecordView<Tuple> tbl = createTable(schema).recordView();

        Tuple tuple = Tuple.create().set("id", 1L).set("val", 1);

        tbl.insert(null, tuple);

        Tuple removedTuple = tbl.getAndDelete(null, Tuple.create().set("id", 1L));

        assertEquals(tuple, removedTuple);

        assertNull(tbl.getAndDelete(null, Tuple.create().set("id", 1L)));
    }

    @Test
    public void deleteAll() {
        SchemaDescriptor schema = new SchemaDescriptor(
                SCHEMA_VERSION,
                new Column[]{new Column("id".toUpperCase(), NativeTypes.INT64, false)},
                new Column[]{new Column("val".toUpperCase(), NativeTypes.INT32, false)}
        );

        RecordView<Tuple> tbl = createTable(schema).recordView();

        Tuple tuple1 = Tuple.create().set("id", 1L).set("val", 11);
        Tuple tuple2 = Tuple.create().set("id", 2L).set("val", 22);
        Tuple tuple3 = Tuple.create().set("id", 3L).set("val", 33);

        tbl.insertAll(null, List.of(tuple1, tuple2, tuple3));

        Collection<Tuple> current = tbl.getAll(
                null,
                List.of(
                        Tuple.create().set("id", 1L),
                        Tuple.create().set("id", 2L),
                        Tuple.create().set("id", 3L)
                ));

        assertEquals(3, current.size());

        assertTrue(current.contains(tuple1));
        assertTrue(current.contains(tuple2));
        assertTrue(current.contains(tuple3));

        Collection<Tuple> notRemovedTuples = tbl.deleteAll(
                null,
                List.of(
                        Tuple.create().set("id", 1L),
                        Tuple.create().set("id", 3L),
                        Tuple.create().set("id", 4L)
                )
        );

        assertEquals(1, notRemovedTuples.size());
        assertTrue(notRemovedTuples.contains(Tuple.create().set("id", 4L)));

        current = tbl.getAll(
                null,
                List.of(
                        Tuple.create().set("id", 1L),
                        Tuple.create().set("id", 2L),
                        Tuple.create().set("id", 3L)
                ));

        assertThat(current, contains(null, tuple2, null));
    }

    @Test
    public void deleteExact() {
        SchemaDescriptor schema = new SchemaDescriptor(
                SCHEMA_VERSION,
                new Column[]{new Column("id".toUpperCase(), NativeTypes.INT64, false)},
                new Column[]{new Column("val".toUpperCase(), NativeTypes.INT32, false)}
        );

        RecordView<Tuple> tbl = createTable(schema).recordView();

        Tuple tuple1 = Tuple.create().set("id", 1L).set("val", 11);
        Tuple tuple2 = Tuple.create().set("id", 2L).set("val", 22);
        Tuple tuple3 = Tuple.create().set("id", 3L).set("val", 33);

        tbl.insertAll(null, List.of(tuple1, tuple2, tuple3));

        Collection<Tuple> current = tbl.getAll(
                null,
                List.of(
                        Tuple.create().set("id", 1L),
                        Tuple.create().set("id", 2L),
                        Tuple.create().set("id", 3L)
                ));

        assertEquals(3, current.size());

        assertTrue(current.contains(tuple1));
        assertTrue(current.contains(tuple2));
        assertTrue(current.contains(tuple3));

        Tuple tuple3Upsert = Tuple.create().set("id", 3L).set("val", 44);

        tbl.upsert(null, tuple3Upsert);

        Tuple tuple4NotExists = Tuple.create().set("id", 4L).set("val", 55);

        Collection<Tuple> notRemovedTuples = tbl.deleteAllExact(null,
                List.of(tuple1, tuple2, tuple3, tuple4NotExists));

        assertEquals(2, notRemovedTuples.size());
        assertTrue(notRemovedTuples.contains(tuple3));
        assertTrue(notRemovedTuples.contains(tuple4NotExists));

        current = tbl.getAll(
                null,
                List.of(
                        Tuple.create().set("id", 1L),
                        Tuple.create().set("id", 2L),
                        Tuple.create().set("id", 3L)
                ));

        assertThat(current, contains(null, null, tuple3Upsert));
    }

    @Test
    public void getAndReplaceVsGetAndUpsert() {
        SchemaDescriptor schema = schemaDescriptor();

        RecordView<Tuple> tbl = createTable(schema).recordView();

        Tuple tuple1 = Tuple.create().set("id", 1L).set("val", 11L);

        assertNull(tbl.getAndUpsert(null, tuple1));

        Tuple tuple = tbl.get(null, Tuple.create().set("id", 1L));

        assertNotNull(tuple);

        assertEquals(tuple, tuple1);

        assertTrue(tbl.deleteExact(null, tuple));

        assertNull(tbl.getAndReplace(null, tuple));

        assertNull(tbl.get(null, Tuple.create().set("id", 1L)));
    }

    @Test
    void retriesOnInternalSchemaVersionMismatchException() throws Exception {
        SchemaDescriptor schema = schemaDescriptor();
        InternalTable internalTable = spy(createInternalTable(schema));
        ReflectionMarshallersProvider marshallers = new ReflectionMarshallersProvider();

        RecordView<Tuple> view = new RecordBinaryViewImpl(
                internalTable,
                new DummySchemaManagerImpl(schema),
                schemaVersions,
                mock(IgniteSql.class),
                marshallers
        );

        BinaryRow resultRow = new TupleMarshallerImpl(schema).marshal(Tuple.create().set("id", 1L).set("val", 2L));

        doReturn(failedFuture(new InternalSchemaVersionMismatchException()))
                .doReturn(completedFuture(resultRow))
                .when(internalTable).get(any(), any());

        Tuple result = view.get(null, Tuple.create().set("id", 1L));

        assertThat(result.longValue("val"), is(2L));

        verify(internalTable, times(2)).get(any(), isNull());
    }

    /**
     * Check tuples equality.
     *
     * @param schema   Schema.
     * @param expected Expected tuple.
     * @param actual   Actual tuple.
     */
    void assertEqualsRows(SchemaDescriptor schema, Tuple expected, Tuple actual) {
        assertEqualsKeys(schema, expected, actual);
        assertEqualsValues(schema, expected, actual);
    }

    /**
     * Check key columns equality.
     *
     * @param schema   Schema.
     * @param expected Expected tuple.
     * @param actual   Actual tuple.
     */
    void assertEqualsKeys(SchemaDescriptor schema, Tuple expected, Tuple actual) {
        int nonNullKey = 0;

        for (int i = 0; i < schema.keyColumns().size(); i++) {
            final Column col = schema.keyColumns().get(i);

            final Object val1 = expected.value(col.name());
            final Object val2 = actual.value(col.name());

            assertEquals(val1, val2, "Value columns equality check failed: " + col);

            if (col.positionInKey() != -1 && val1 != null) {
                nonNullKey++;
            }
        }

        assertTrue(nonNullKey > 0, "At least one non-null key column must exist.");
    }

    /**
     * Check value columns equality.
     *
     * @param schema   Schema.
     * @param expected Expected tuple.
     * @param actual   Actual tuple.
     */
    void assertEqualsValues(SchemaDescriptor schema, Tuple expected, Tuple actual) {
        for (int i = 0; i < schema.valueColumns().size(); i++) {
            final Column col = schema.valueColumns().get(i);

            final Object val1 = expected.value(col.name());
            final Object val2 = actual.value(col.name());

            if (val1 instanceof byte[] && val2 instanceof byte[]) {
                Assertions.assertArrayEquals((byte[]) val1, (byte[]) val2, "Equality check failed: colIdx=" + col.positionInRow());
            } else {
                assertEquals(val1, val2, "Equality check failed: colIdx=" + col.positionInRow());
            }
        }
    }

    private <T extends Throwable> void assertThrowsWithCause(Class<T> expectedType, Executable executable) {
        Throwable ex = assertThrows(IgniteException.class, executable);

        while (ex.getCause() != null) {
            if (expectedType.isInstance(ex.getCause())) {
                return;
            }

            ex = ex.getCause();
        }

        fail("Expected cause wasn't found.");
    }
}
