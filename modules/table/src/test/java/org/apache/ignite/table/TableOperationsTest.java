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

package org.apache.ignite.table;

import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.NativeType;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.table.TableImpl;
import org.apache.ignite.table.impl.DummySchemaManagerImpl;
import org.apache.ignite.table.impl.DummyInternalTableImpl;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Basic table operations test.
 *
 * TODO: Add bulk operations tests.
 * TODO: Add async operations tests.
 * TODO: Refactor, to check different schema configurations (varlen fields, composite keys, binary fields and etc).
 */
public class TableOperationsTest {
    /**
     *
     */
    @Test
    public void testInsert() {
        SchemaDescriptor schema = new SchemaDescriptor(
            1,
            new Column[] {new Column("id", NativeType.LONG, false)},
            new Column[] {new Column("val", NativeType.LONG, false)}
        );

        Table tbl = new TableImpl(new DummyInternalTableImpl(), new DummySchemaManagerImpl(schema));

        final Tuple tuple = tbl.tupleBuilder().set("id", 1L).set("val", 11L).build();
        final Tuple newTuple = tbl.tupleBuilder().set("id", 1L).set("val", 111L).build();
        final Tuple nonExistedTuple = tbl.tupleBuilder().set("id", 2L).set("val", 22L).build();

        assertNull(tbl.get(tbl.tupleBuilder().set("id", 1L).build()));
        assertNull(tbl.get(tuple));

        // Insert new tuple.
        assertTrue(tbl.insert(tuple));

        assertEqualsRows(schema, tuple, tbl.get(tbl.tupleBuilder().set("id", 1L).build()));
        assertEqualsRows(schema, tuple, tbl.get(tuple));

        assertNull(tbl.get(nonExistedTuple));

        // Ignore insert operation for exited row.
        assertFalse(tbl.insert(newTuple));

        assertEqualsRows(schema, tuple, tbl.get(tbl.tupleBuilder().set("id", 1L).build()));
        assertEqualsRows(schema, tuple, tbl.get(newTuple));
    }

    /**
     *
     */
    @Test
    public void testUpsert() {
        SchemaDescriptor schema = new SchemaDescriptor(
            1,
            new Column[] {new Column("id", NativeType.LONG, false)},
            new Column[] {new Column("val", NativeType.LONG, false)}
        );

        Table tbl = new TableImpl(new DummyInternalTableImpl(), new DummySchemaManagerImpl(schema));

        final Tuple tuple = tbl.tupleBuilder().set("id", 1L).set("val", 11L).build();
        final Tuple newTuple = tbl.tupleBuilder().set("id", 1L).set("val", 111L).build();
        final Tuple nonExistedTuple = tbl.tupleBuilder().set("id", 2L).set("val", 22L).build();

        assertNull(tbl.get(tbl.tupleBuilder().set("id", 1L).build()));
        assertNull(tbl.get(tuple));

        // Insert new tuple.
        tbl.upsert(tuple);

        assertEqualsRows(schema, tuple, tbl.get(tbl.tupleBuilder().set("id", 1L).build()));
        assertEqualsRows(schema, tuple, tbl.get(tuple));

        assertNull(tbl.get(nonExistedTuple));

        // Update exited row.
        tbl.upsert(newTuple);

        assertEqualsRows(schema, newTuple, tbl.get(tbl.tupleBuilder().set("id", 1L).build()));
        assertEqualsRows(schema, newTuple, tbl.get(tuple));
    }

    /**
     *
     */
    @Test
    public void testGetAndUpsert() {
        SchemaDescriptor schema = new SchemaDescriptor(
            1,
            new Column[] {new Column("id", NativeType.LONG, false)},
            new Column[] {new Column("val", NativeType.LONG, false)}
        );

        Table tbl = new TableImpl(new DummyInternalTableImpl(), new DummySchemaManagerImpl(schema));

        final Tuple tuple = tbl.tupleBuilder().set("id", 1L).set("val", 11L).build();
        final Tuple newTuple = tbl.tupleBuilder().set("id", 1L).set("val", 111L).build();
        final Tuple nonExistedTuple = tbl.tupleBuilder().set("id", 2L).set("val", 22L).build();

        assertNull(tbl.get(tbl.tupleBuilder().set("id", 1L).build()));
        assertNull(tbl.get(tuple));

        // Insert new tuple.
        assertNull(tbl.getAndUpsert(tuple));

        assertEqualsRows(schema, tuple, tbl.get(tbl.tupleBuilder().set("id", 1L).build()));
        assertEqualsRows(schema, tuple, tbl.get(tuple));

        assertNull(tbl.get(nonExistedTuple));

        // Update exited row.
        assertEqualsRows(schema, tuple, tbl.getAndUpsert(newTuple));

        assertEqualsRows(schema, newTuple, tbl.get(tbl.tupleBuilder().set("id", 1L).build()));
        assertEqualsRows(schema, newTuple, tbl.get(tuple));
    }

    /**
     *
     */
    @Test
    public void testRemove() {
        SchemaDescriptor schema = new SchemaDescriptor(
            1,
            new Column[] {new Column("id", NativeType.LONG, false)},
            new Column[] {new Column("val", NativeType.LONG, false)}
        );

        Table tbl = new TableImpl(new DummyInternalTableImpl(), new DummySchemaManagerImpl(schema));

        final Tuple tuple = tbl.tupleBuilder().set("id", 1L).set("val", 11L).build();
        final Tuple nonExistedTuple = tbl.tupleBuilder().set("id", 2L).set("val", 22L).build();

        tbl.upsert(tuple);

        assertNotNull(tbl.get(tuple));
        assertNull(tbl.get(nonExistedTuple));

        // Delete not existed tuple.
        assertFalse(tbl.delete(nonExistedTuple));
        // TODO: check no tombstone exists.

        // Delete existed tuple.
        assertTrue(tbl.delete(tuple));
        assertNull(tbl.get(tuple));
        // TODO: check tombstone exists.

        // Delete already deleted tuple.
        assertFalse(tbl.delete(tuple));
        // TODO: check tombstone still exists.
    }

    /**
     *
     */
    @Test
    public void testReplace() {
        SchemaDescriptor schema = new SchemaDescriptor(
            1,
            new Column[] {new Column("id", NativeType.LONG, false)},
            new Column[] {new Column("val", NativeType.LONG, false)}
        );

        Table tbl = new TableImpl(new DummyInternalTableImpl(), new DummySchemaManagerImpl(schema));

        final Tuple tuple = tbl.tupleBuilder().set("id", 1L).set("val", 11L).build();
        final Tuple newTuple = tbl.tupleBuilder().set("id", 1L).set("val", 111L).build();
        final Tuple nonExistedTuple = tbl.tupleBuilder().set("id", 2L).set("val", 22L).build();

        assertNull(tbl.get(tbl.tupleBuilder().set("id", 1L).build()));
        assertNull(tbl.get(tuple));

        // Ignore replace operation for non-existed row.
        assertFalse(tbl.replace(tuple));

        assertNull(tbl.get(tbl.tupleBuilder().set("id", 1L).build()));
        assertNull(tbl.get(tuple));

        assertNull(tbl.get(nonExistedTuple));

        // Insert row.
        tbl.insert(tuple);

        // Replace existed row.
        assertTrue(tbl.replace(newTuple));

        assertEqualsRows(schema, newTuple, tbl.get(tbl.tupleBuilder().set("id", 1L).build()));
        assertEqualsRows(schema, newTuple, tbl.get(tuple));
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

            if (schema.keyColumn(i) && val1 != null)
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

            Assertions.assertEquals(val1, val2, "Key columns equality check failed: colIdx=" + col.schemaIndex());
        }
    }
}
