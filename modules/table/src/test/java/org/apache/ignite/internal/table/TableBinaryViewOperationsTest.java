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

package org.apache.ignite.internal.table;

import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.InvalidTypeException;
import org.apache.ignite.internal.schema.NativeTypes;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.table.impl.DummyInternalTableImpl;
import org.apache.ignite.internal.table.impl.DummySchemaManagerImpl;
import org.apache.ignite.internal.table.impl.TestTupleBuilder;
import org.apache.ignite.table.Table;
import org.apache.ignite.table.Tuple;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Basic table operations test.
 * <p>
 * TODO: IGNITE-14486 Add tests for invoke operations.
 * TODO: IGNITE-14486 Add tests for bulk operations.
 * TODO: IGNITE-14486 Add tests for async operations.
 */
public class TableBinaryViewOperationsTest {
    /** Table ID test value. */
    public final java.util.UUID tableId = java.util.UUID.randomUUID();

    /**
     *
     */
    @Test
    public void insert() {
        SchemaDescriptor schema = new SchemaDescriptor(
            tableId,
            1,
            new Column[] {new Column("id", NativeTypes.LONG, false)},
            new Column[] {new Column("val", NativeTypes.LONG, false)}
        );

        Table tbl = new TableImpl(new DummyInternalTableImpl(), new DummySchemaManagerImpl(schema));

        final Tuple tuple = tbl.tupleBuilder().set("id", 1L).set("val", 11L).build();
        final Tuple newTuple = tbl.tupleBuilder().set("id", 1L).set("val", 22L).build();
        final Tuple nonExistedTuple = tbl.tupleBuilder().set("id", 2L).build();

        assertNull(tbl.get(tuple));

        // Insert new tuple.
        assertTrue(tbl.insert(tuple));

        assertEqualsRows(schema, tuple, tbl.get(tuple));
        assertEqualsRows(schema, tuple, tbl.get(tbl.tupleBuilder().set("id", 1L).set("val", -1L).build()));

        // Ignore insert operation for exited row.
        assertFalse(tbl.insert(newTuple));

        assertEqualsRows(schema, tuple, tbl.get(newTuple));
        assertEqualsRows(schema, tuple, tbl.get(tbl.tupleBuilder().set("id", 1L).set("val", -1L).build()));

        assertNull(tbl.get(nonExistedTuple));
    }

    /**
     *
     */
    @Test
    public void upsert() {
        SchemaDescriptor schema = new SchemaDescriptor(
            tableId,
            1,
            new Column[] {new Column("id", NativeTypes.LONG, false)},
            new Column[] {new Column("val", NativeTypes.LONG, false)}
        );

        Table tbl = new TableImpl(new DummyInternalTableImpl(), new DummySchemaManagerImpl(schema));

        final Tuple tuple = tbl.tupleBuilder().set("id", 1L).set("val", 11L).build();
        final Tuple newTuple = tbl.tupleBuilder().set("id", 1L).set("val", 22L).build();
        final Tuple nonExistedTuple = tbl.tupleBuilder().set("id", 2L).build();

        assertNull(tbl.get(tbl.tupleBuilder().set("id", 1L).build()));
        assertNull(tbl.get(tuple));

        // Insert new tuple.
        tbl.upsert(tuple);

        assertEqualsRows(schema, tuple, tbl.get(tuple));
        assertEqualsRows(schema, tuple, tbl.get(tbl.tupleBuilder().set("id", 1L).set("val", -1L).build()));

        // Update exited row.
        tbl.upsert(newTuple);

        assertEqualsRows(schema, newTuple, tbl.get(tuple));
        assertEqualsRows(schema, newTuple, tbl.get(tbl.tupleBuilder().set("id", 1L).set("val", -1L).build()));

        assertNull(tbl.get(nonExistedTuple));
    }

    /**
     *
     */
    @Test
    public void getAndUpsert() {
        SchemaDescriptor schema = new SchemaDescriptor(
            tableId,
            1,
            new Column[] {new Column("id", NativeTypes.LONG, false)},
            new Column[] {new Column("val", NativeTypes.LONG, false)}
        );

        Table tbl = new TableImpl(new DummyInternalTableImpl(), new DummySchemaManagerImpl(schema));

        final Tuple tuple = tbl.tupleBuilder().set("id", 1L).set("val", 11L).build();
        final Tuple newTuple = tbl.tupleBuilder().set("id", 1L).set("val", 22L).build();

        assertNull(tbl.get(tbl.tupleBuilder().set("id", 1L).build()));
        assertNull(tbl.get(tuple));

        // Insert new tuple.
        assertNull(tbl.getAndUpsert(tuple));

        assertEqualsRows(schema, tuple, tbl.get(tuple));
        assertEqualsRows(schema, tuple, tbl.get(tbl.tupleBuilder().set("id", 1L).set("val", -1L).build()));

        // Update exited row.
        assertEqualsRows(schema, tuple, tbl.getAndUpsert(newTuple));

        assertEqualsRows(schema, newTuple, tbl.get(tuple));
        assertEqualsRows(schema, newTuple, tbl.get(tbl.tupleBuilder().set("id", 1L).set("val", -1L).build()));
    }

    /**
     *
     */
    @Test
    public void remove() {
        SchemaDescriptor schema = new SchemaDescriptor(
            tableId,
            1,
            new Column[] {new Column("id", NativeTypes.LONG, false)},
            new Column[] {new Column("val", NativeTypes.LONG, false)}
        );

        Table tbl = new TableImpl(new DummyInternalTableImpl(), new DummySchemaManagerImpl(schema));

        final Tuple tuple = tbl.tupleBuilder().set("id", 1L).set("val", 11L).build();

        tbl.upsert(tuple);

        assertEqualsRows(schema, tuple, tbl.get(tuple));

        // Delete not existed tuple.
        assertEqualsRows(schema, tuple, tbl.get(tbl.tupleBuilder().set("id", 1L).build()));

        // Delete existed tuple.
        assertTrue(tbl.delete(tuple));
        assertNull(tbl.get(tuple));

        // Delete already deleted tuple.
        assertFalse(tbl.delete(tuple));
    }

    /**
     *
     */
    @Test
    public void removeExact() {
        SchemaDescriptor schema = new SchemaDescriptor(
            tableId,
            1,
            new Column[] {new Column("id", NativeTypes.LONG, false)},
            new Column[] {new Column("val", NativeTypes.LONG, false)}
        );

        Table tbl = new TableImpl(new DummyInternalTableImpl(), new DummySchemaManagerImpl(schema));

        final Tuple keyTuple = tbl.tupleBuilder().set("id", 1L).build();
        final Tuple tuple = tbl.tupleBuilder().set("id", 1L).set("val", 11L).build();
        final Tuple tuple2 = tbl.tupleBuilder().set("id", 1L).set("val", 22L).build();
        final Tuple nonExistedTuple = tbl.tupleBuilder().set("id", 2L).set("val", 22L).build();

        tbl.insert(tuple);

        assertEqualsRows(schema, tuple, tbl.get(keyTuple));

        // Fails to delete not existed tuple.
        assertFalse(tbl.deleteExact(nonExistedTuple));
        assertEqualsRows(schema, tuple, tbl.get(keyTuple));

        // Fails to delete tuple with unexpected value.
        assertFalse(tbl.deleteExact(tuple2));
        assertEqualsRows(schema, tuple, tbl.get(keyTuple));

        // TODO: IGNITE-14479: Fix default value usage.
//        assertFalse(tbl.deleteExact(keyTuple));
//        assertEqualsRows(schema, tuple, tbl.get(keyTuple));

        // Delete tuple with expected value.
        assertTrue(tbl.deleteExact(tuple));
        assertNull(tbl.get(keyTuple));

        // Once again.
        assertFalse(tbl.deleteExact(tuple));
        assertNull(tbl.get(keyTuple));

        // Insert new.
        tbl.insert(tuple2);
        assertEqualsRows(schema, tuple2, tbl.get(keyTuple));

        // Delete tuple with expected value.
        assertTrue(tbl.deleteExact(tuple2));
        assertNull(tbl.get(keyTuple));
    }

    /**
     *
     */
    @Test
    public void replace() {
        SchemaDescriptor schema = new SchemaDescriptor(
            tableId,
            1,
            new Column[] {new Column("id", NativeTypes.LONG, false)},
            new Column[] {new Column("val", NativeTypes.LONG, false)}
        );

        Table tbl = new TableImpl(new DummyInternalTableImpl(), new DummySchemaManagerImpl(schema));

        final Tuple keyTuple = tbl.tupleBuilder().set("id", 1L).build();
        final Tuple tuple = tbl.tupleBuilder().set("id", 1L).set("val", 11L).build();
        final Tuple tuple2 = tbl.tupleBuilder().set("id", 1L).set("val", 22L).build();

        assertNull(tbl.get(keyTuple));

        // Ignore replace operation for non-existed row.
        assertFalse(tbl.replace(tuple));

        assertNull(tbl.get(keyTuple));
        assertNull(tbl.get(tbl.tupleBuilder().set("id", 1L).set("val", -1L).build()));

        // Insert row.
        tbl.insert(tuple);

        // Replace existed row.
        assertTrue(tbl.replace(tuple2));

        assertEqualsRows(schema, tuple2, tbl.get(keyTuple));
        assertEqualsRows(schema, tuple2, tbl.get(tbl.tupleBuilder().set("id", 1L).set("val", -1L).build()));
    }

    /**
     *
     */
    @Test
    public void replaceExact() {
        SchemaDescriptor schema = new SchemaDescriptor(
            tableId,
            1,
            new Column[] {new Column("id", NativeTypes.LONG, false)},
            new Column[] {new Column("val", NativeTypes.LONG, false)}
        );

        Table tbl = new TableImpl(new DummyInternalTableImpl(), new DummySchemaManagerImpl(schema));

        final Tuple keyTuple = tbl.tupleBuilder().set("id", 1L).build();
        final Tuple tuple = tbl.tupleBuilder().set("id", 1L).set("val", 11L).build();
        final Tuple tuple2 = tbl.tupleBuilder().set("id", 1L).set("val", 22L).build();

        assertNull(tbl.get(keyTuple));

        // Ignore replace operation for non-existed row.
        // TODO: IGNITE-14479: Fix default value usage.
//        assertTrue(tbl.replace(keyTuple, tuple));

//        assertNull(tbl.get(keyTuple));
//        assertNull(tbl.get(tbl.tupleBuilder().set("id", 1L).set("val", -1).build()));

        // Insert row.
        tbl.insert(tuple);

        // Replace existed row.
        assertTrue(tbl.replace(tuple, tuple2));

        assertEqualsRows(schema, tuple2, tbl.get(keyTuple));
        assertEqualsRows(schema, tuple2, tbl.get(tbl.tupleBuilder().set("id", 1L).set("val", -1L).build()));
    }

    /**
     *
     */
    @Test
    public void validateSchema() {
        SchemaDescriptor schema = new SchemaDescriptor(
            tableId,
            1,
            new Column[] {new Column("id", NativeTypes.LONG, false)},
            new Column[] {
                new Column("val", NativeTypes.LONG, true),
                new Column("str", NativeTypes.stringOf(3), true),
                new Column("blob", NativeTypes.blobOf(3), true)
            }
        );

        Table tbl = new TableImpl(new DummyInternalTableImpl(), new DummySchemaManagerImpl(schema));

        final Tuple keyTuple0 = new TestTupleBuilder().set("id", 0).set("id1", 0).build();
        final Tuple keyTuple1 = new TestTupleBuilder().set("id1", 0).build();
        final Tuple tuple0 = new TestTupleBuilder().set("id", 1L).set("str", "qweqweqwe").set("val", 11L).build();
        final Tuple tuple1 = new TestTupleBuilder().set("id", 1L).set("blob", new byte[] {0, 1, 2, 3}).set("val", 22L).build();

        assertThrows(InvalidTypeException.class, () -> tbl.get(keyTuple0));
        assertThrows(IllegalArgumentException.class, () -> tbl.get(keyTuple1));

        assertThrows(InvalidTypeException.class, () -> tbl.replace(tuple0));
        assertThrows(InvalidTypeException.class, () -> tbl.replace(tuple1));

        assertThrows(InvalidTypeException.class, () -> tbl.insert(tuple0));
        assertThrows(InvalidTypeException.class, () -> tbl.insert(tuple1));

        assertThrows(InvalidTypeException.class, () -> tbl.replace(tuple0));
        assertThrows(InvalidTypeException.class, () -> tbl.replace(tuple1));
    }

    /**
     *
     */
    @Test
    public void defaultValues() {
        SchemaDescriptor schema = new SchemaDescriptor(
            tableId,
            1,
            new Column[] {new Column("id", NativeTypes.LONG, false)},
            new Column[] {
                new Column("val", NativeTypes.LONG, true, () -> 28L),
                new Column("str", NativeTypes.stringOf(3), true, () -> "ABC"),
                new Column("blob", NativeTypes.blobOf(3), true, () -> new byte[] {0, 1, 2})
            }
        );

        Table tbl = new TableImpl(new DummyInternalTableImpl(), new DummySchemaManagerImpl(schema));

        final Tuple keyTuple0 = tbl.tupleBuilder().set("id", 0L).build();
        final Tuple keyTuple1 = tbl.tupleBuilder().set("id", 1L).build();

        final Tuple tuple0 = tbl.tupleBuilder().set("id", 0L).build();
        final Tuple tupleExpected0 = tbl.tupleBuilder().set("id", 0L).set("val", 28L).set("str", "ABC").set("blob", new byte[] {0, 1, 2}).build();
        final Tuple tuple1 = tbl.tupleBuilder().set("id", 1L).set("val", null).set("str", null).set("blob", null).build();

        tbl.insert(tuple0);
        tbl.insert(tuple1);

        assertEqualsRows(schema, tupleExpected0, tbl.get(keyTuple0));
        assertEqualsRows(schema, tuple1, tbl.get(keyTuple1));
    }

    /**
     * Check tuples equality.
     *
     * @param schema Schema.
     * @param expected Expected tuple.
     * @param actual Actual tuple.
     */
    void assertEqualsRows(SchemaDescriptor schema, Tuple expected, Tuple actual) {
        assertEqualsKeys(schema, expected, actual);
        assertEqualsValues(schema, expected, actual);
    }

    /**
     * Check key columns equality.
     *
     * @param schema Schema.
     * @param expected Expected tuple.
     * @param actual Actual tuple.
     */
    void assertEqualsKeys(SchemaDescriptor schema, Tuple expected, Tuple actual) {
        int nonNullKey = 0;

        for (int i = 0; i < schema.keyColumns().length(); i++) {
            final Column col = schema.keyColumns().column(i);

            final Object val1 = expected.value(col.name());
            final Object val2 = actual.value(col.name());

            Assertions.assertEquals(val1, val2, "Value columns equality check failed: colIdx=" + col.schemaIndex());

            if (schema.isKeyColumn(i) && val1 != null)
                nonNullKey++;
        }

        assertTrue(nonNullKey > 0, "At least one non-null key column must exist.");
    }

    /**
     * Check value columns equality.
     *
     * @param schema Schema.
     * @param expected Expected tuple.
     * @param actual Actual tuple.
     */
    void assertEqualsValues(SchemaDescriptor schema, Tuple expected, Tuple actual) {
        for (int i = 0; i < schema.valueColumns().length(); i++) {
            final Column col = schema.valueColumns().column(i);

            final Object val1 = expected.value(col.name());
            final Object val2 = actual.value(col.name());

            if (val1 instanceof byte[] && val2 instanceof byte[])
                Assertions.assertArrayEquals((byte[])val1, (byte[])val2, "Equality check failed: colIdx=" + col.schemaIndex());
            else
               Assertions.assertEquals(val1, val2, "Equality check failed: colIdx=" + col.schemaIndex());
        }
    }
}
