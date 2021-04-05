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
import org.apache.ignite.internal.table.KVBinaryViewImpl;
import org.apache.ignite.table.impl.DummyInternalTableImpl;
import org.apache.ignite.table.impl.DummySchemaManagerImpl;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Basic table operations test.
 * <p>
 * TODO: Add bulk operations tests.
 * TODO: Add async operations tests.
 * TODO: Refactor, to check different schema configurations (varlen fields, composite keys, binary fields and etc).
 * TODO: Check non-key fields is ignored for keys.
 * TODO: Check non-value fields is ignored or exception is thrown
 */
public class KVViewOperationsTest {
    /**
     *
     */
    @Test
    public void testPut() {
        SchemaDescriptor schema = new SchemaDescriptor(
            1,
            new Column[] {new Column("id", NativeType.LONG, false)},
            new Column[] {new Column("val", NativeType.LONG, false)}
        );

        KeyValueBinaryView tbl = new KVBinaryViewImpl(new DummyInternalTableImpl(), new DummySchemaManagerImpl(schema));

        final Tuple key = tbl.tupleBuilder().set("id", 1L).build();
        final Tuple val = tbl.tupleBuilder().set("val", 11L).build();
        final Tuple val2 = tbl.tupleBuilder().set("val", 22L).build();
        final Tuple val3 = tbl.tupleBuilder().set("val", 33L).build();

        assertNull(tbl.get(key));

        // Put KV pair.
        tbl.put(key, val);

        assertEqualsValues(schema, val, tbl.get(key));
        assertEqualsValues(schema, val, tbl.get(tbl.tupleBuilder().set("id", 1L).set("val", -1L).build()));

        // Update KV pair.
        tbl.put(key, val2);

        assertEqualsValues(schema, val2, tbl.get(key));
        assertEqualsValues(schema, val2, tbl.get(tbl.tupleBuilder().set("id", 1L).set("val", -1L).build()));

        // Remove KV pair.
        tbl.put(key, null);

        assertNull(tbl.get(key));
        // TODO: check tombstone exists.

        // Overwrite tombstone.
        tbl.put(key, val3);
        assertEqualsValues(schema, val3, tbl.get(key));
    }

    /**
     *
     */
    @Test
    public void testPutIfAbsent() {
        SchemaDescriptor schema = new SchemaDescriptor(
            1,
            new Column[] {new Column("id", NativeType.LONG, false)},
            new Column[] {new Column("val", NativeType.LONG, false)}
        );

        KeyValueBinaryView tbl = new KVBinaryViewImpl(new DummyInternalTableImpl(), new DummySchemaManagerImpl(schema));

        final Tuple key = tbl.tupleBuilder().set("id", 1L).build();
        final Tuple val = tbl.tupleBuilder().set("val", 11L).build();
        final Tuple val2 = tbl.tupleBuilder().set("val", 22L).build();

        assertNull(tbl.get(key));

        // Insert new KV pair.
        assertTrue(tbl.putIfAbsent(key, val));

        assertEqualsValues(schema, val, tbl.get(key));
        assertEqualsValues(schema, val, tbl.get(tbl.tupleBuilder().set("id", 1L).set("val", -1L).build()));

        // Update KV pair.
        assertFalse(tbl.putIfAbsent(key, val2));

        assertEqualsValues(schema, val, tbl.get(key));
        assertEqualsValues(schema, val, tbl.get(tbl.tupleBuilder().set("id", 1L).set("val", -1L).build()));
    }

    /**
     *
     */
    @Test
    public void testGetAndPut() {
        SchemaDescriptor schema = new SchemaDescriptor(
            1,
            new Column[] {new Column("id", NativeType.LONG, false)},
            new Column[] {new Column("val", NativeType.LONG, false)}
        );

        KeyValueBinaryView tbl = new KVBinaryViewImpl(new DummyInternalTableImpl(), new DummySchemaManagerImpl(schema));

        final Tuple key = tbl.tupleBuilder().set("id", 1L).build();
        final Tuple val = tbl.tupleBuilder().set("val", 11L).build();
        final Tuple val2 = tbl.tupleBuilder().set("val", 22L).build();
        final Tuple val3 = tbl.tupleBuilder().set("val", 33L).build();

        assertNull(tbl.get(key));

        // Insert new tuple.
        assertNull(tbl.getAndPut(key, val));

        assertEqualsValues(schema, val, tbl.get(key));
        assertEqualsValues(schema, val, tbl.get(tbl.tupleBuilder().set("id", 1L).set("val", -1L).build()));

        // Check non-value fields is ignored.
        assertEqualsValues(schema, val, tbl.getAndPut(key, val2));
        assertEqualsValues(schema, val2, tbl.getAndPut(key, tbl.tupleBuilder().set("id", 2L).set("val", 33L).build()));

        assertEqualsValues(schema, val3, tbl.get(key));
        assertNull(tbl.get(tbl.tupleBuilder().set("id", 2L).build()));
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

        KeyValueBinaryView tbl = new KVBinaryViewImpl(new DummyInternalTableImpl(), new DummySchemaManagerImpl(schema));

        final Tuple key = tbl.tupleBuilder().set("id", 1L).build();
        final Tuple key2 = tbl.tupleBuilder().set("id", 2L).build();
        final Tuple val = tbl.tupleBuilder().set("val", 11L).build();
        final Tuple val2 = tbl.tupleBuilder().set("val", 22L).build();

        tbl.put(key, val);

        // Delete existed tuple.
        assertEqualsValues(schema, val, tbl.get(key));
        assertTrue(tbl.remove(key));
        assertNull(tbl.get(key));
        // TODO: check tombstone exists.

        // Delete already deleted tuple.
        assertFalse(tbl.remove(key));
        // TODO: check tombstone still exists.

        // Overwrite tombstone.
        tbl.put(key, val2);
        assertEqualsValues(schema, val2, tbl.get(key));

        // Delete existed tuple.
        assertTrue(tbl.remove(tbl.tupleBuilder().set("id", 1L).set("val", -1L).build()));
        assertNull(tbl.get(key));
        // TODO: check tombstone exists.

        // Delete not existed tuple.
        assertNull(tbl.get(key2));
        assertFalse(tbl.remove(key2));
        // TODO: check no tombstone was created.
    }

    /**
     *
     */
    @Test
    public void testRemoveExact() {
        SchemaDescriptor schema = new SchemaDescriptor(
            1,
            new Column[] {new Column("id", NativeType.LONG, false)},
            new Column[] {new Column("val", NativeType.LONG, false)}
        );

        KeyValueBinaryView tbl = new KVBinaryViewImpl(new DummyInternalTableImpl(), new DummySchemaManagerImpl(schema));

        final Tuple key = tbl.tupleBuilder().set("id", 1L).build();
        final Tuple key2 = tbl.tupleBuilder().set("id", 2L).build();
        final Tuple val = tbl.tupleBuilder().set("val", 11L).build();
        final Tuple val2 = tbl.tupleBuilder().set("val", 22L).build();

        tbl.put(key, val);
        assertEqualsValues(schema, val, tbl.get(key));

        // Fails to delete KV pair with unexpected value.
        assertFalse(tbl.remove(key, val2));
        assertEqualsValues(schema, val, tbl.get(key));

        // Delete KV pair with expected value.
        assertTrue(tbl.remove(key, val));
        assertNull(tbl.get(key));
        // TODO: check tombstone exists.

        // Once again.
        assertFalse(tbl.remove(key, val));
        assertNull(tbl.get(key));
        // TODO: check tombstone exists.

        // Try to remove tombstone.
        assertThrows(NullPointerException.class, () -> tbl.remove(key, null));
        assertNull(tbl.get(key));
        // TODO: check tombstone still exists.

        // Overwrite tombstone.
        tbl.put(key, val2);
        assertEqualsValues(schema, val2, tbl.get(key));

        // Check null value ignored.
        assertThrows(NullPointerException.class, () -> tbl.remove(key, null));
        assertEqualsValues(schema, val2, tbl.get(key));

        // Delete KV pair with expected value.
        assertTrue(tbl.remove(key, val2));
        assertNull(tbl.get(key));
        // TODO: check tombstone exists.

        assertThrows(NullPointerException.class, () -> tbl.remove(key2, null));
        // TODO: check no tombstone was created.

        assertFalse(tbl.remove(key2, val2));
        // TODO: check no tombstone was created.

        assertNull(tbl.get(key2));
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

        KeyValueBinaryView tbl = new KVBinaryViewImpl(new DummyInternalTableImpl(), new DummySchemaManagerImpl(schema));

        final Tuple key = tbl.tupleBuilder().set("id", 1L).build();
        final Tuple key2 = tbl.tupleBuilder().set("id", 2L).build();
        final Tuple val = tbl.tupleBuilder().set("val", 11L).build();
        final Tuple val2 = tbl.tupleBuilder().set("val", 22L).build();
        final Tuple val3 = tbl.tupleBuilder().set("val", 33L).build();

        // Ignore replace operation for non-existed KV pair.
        assertFalse(tbl.replace(key, val));
        assertNull(tbl.get(key));
        // TODO: check no tombstone existed.

        tbl.put(key, val);

        // Replace existed KV pair.
        assertTrue(tbl.replace(key, val2));
        assertEqualsValues(schema, val2, tbl.get(key));

        // Remove existed KV pair.
        assertTrue(tbl.replace(key, null));
        assertNull(tbl.get(key));
        // TODO: check tombstone exists.

        // Try to overwrite tombstone.
        assertFalse(tbl.replace(key, val3));
        assertNull(tbl.get(key));
        // TODO: check tombstone exists.

        tbl.put(key, val3);
        assertEqualsValues(schema, val3, tbl.get(key));

        // Remove non-existed KV pair.
        assertFalse(tbl.replace(key2, null));
        assertNull(tbl.get(key2));
        // TODO: check no tombstone existed.


    }

    /**
     *
     */
    @Test
    public void testReplaceExact() {
        SchemaDescriptor schema = new SchemaDescriptor(
            1,
            new Column[] {new Column("id", NativeType.LONG, false)},
            new Column[] {new Column("val", NativeType.LONG, false)}
        );

        KeyValueBinaryView tbl = new KVBinaryViewImpl(new DummyInternalTableImpl(), new DummySchemaManagerImpl(schema));

        final Tuple key = tbl.tupleBuilder().set("id", 1L).build();
        final Tuple key2 = tbl.tupleBuilder().set("id", 2L).build();
        final Tuple val = tbl.tupleBuilder().set("val", 11L).build();
        final Tuple val2 = tbl.tupleBuilder().set("val", 22L).build();
        final Tuple val3 = tbl.tupleBuilder().set("val", 33L).build();

        // Insert row.
        assertTrue(tbl.replace(key, null, val));
        assertEqualsValues(schema, val, tbl.get(key));
        assertNull(tbl.get(key2));

        // Ignore replace operation for non-existed KV pair.
        assertFalse(tbl.replace(key2, val, val2));
        assertNull(tbl.get(key2));
        // TODO: check no tombstone existed.

        // Replace existed KV pair.
        assertTrue(tbl.replace(key, val, val2));
        assertEqualsValues(schema, val2, tbl.get(key));

        // Remove existed KV pair.
        assertTrue(tbl.replace(key, val2, null));
        assertNull(tbl.get(key));
        // TODO: check tombstone exists.

        // Overwrite tombstone.
        assertTrue(tbl.replace(key, null, val3));
        assertEqualsValues(schema, val3, tbl.get(key));
        // TODO: check tombstone exists.

        // Remove non-existed KV pair.
        assertTrue(tbl.replace(key2, null, null));
        // TODO: check no tombstone existed.
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
