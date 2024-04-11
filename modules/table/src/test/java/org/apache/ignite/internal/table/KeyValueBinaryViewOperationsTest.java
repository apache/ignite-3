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
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.ignite.internal.marshaller.ReflectionMarshallersProvider;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.marshaller.TupleMarshallerImpl;
import org.apache.ignite.internal.table.distributed.replicator.InternalSchemaVersionMismatchException;
import org.apache.ignite.internal.table.impl.DummySchemaManagerImpl;
import org.apache.ignite.internal.type.NativeTypes;
import org.apache.ignite.sql.IgniteSql;
import org.apache.ignite.table.KeyValueView;
import org.apache.ignite.table.Tuple;
import org.junit.jupiter.api.Test;

/**
 * Basic table operations test.
 */
public class KeyValueBinaryViewOperationsTest extends TableKvOperationsTestBase {
    @Test
    public void put() {
        SchemaDescriptor schema = schemaDescriptor();

        KeyValueView<Tuple, Tuple> tbl = createTable(schema).keyValueView();

        final Tuple key = Tuple.create().set("id", 1L);
        final Tuple val = Tuple.create().set("val", 11L);
        final Tuple val2 = Tuple.create().set("val", 22L);
        final Tuple val3 = Tuple.create().set("val", 33L);

        assertNull(tbl.get(null, key));

        // Put KV pair.
        tbl.put(null, key, val);

        assertEqualsValues(schema, val, tbl.get(null, key));
        assertEqualsValues(schema, val, tbl.get(null, Tuple.create().set("id", 1L)));

        // Update KV pair.
        tbl.put(null, key, val2);

        assertEqualsValues(schema, val2, tbl.get(null, key));
        assertEqualsValues(schema, val2, tbl.get(null, Tuple.create().set("id", 1L)));

        // Remove KV pair.
        tbl.remove(null, key);

        assertNull(tbl.get(null, key));

        // Put KV pair.
        tbl.put(null, key, val3);
        assertEqualsValues(schema, val3, tbl.get(null, key));
    }

    @Test
    public void putIfAbsent() {
        SchemaDescriptor schema = schemaDescriptor();

        KeyValueView<Tuple, Tuple> tbl = createTable(schema).keyValueView();

        final Tuple key = Tuple.create().set("id", 1L);
        final Tuple val = Tuple.create().set("val", 11L);
        final Tuple val2 = Tuple.create().set("val", 22L);

        assertNull(tbl.get(null, key));

        // Insert new KV pair.
        assertTrue(tbl.putIfAbsent(null, key, val));

        assertEqualsValues(schema, val, tbl.get(null, key));
        assertEqualsValues(schema, val, tbl.get(null, Tuple.create().set("id", 1L)));

        // Update KV pair.
        assertFalse(tbl.putIfAbsent(null, key, val2));

        assertEqualsValues(schema, val, tbl.get(null, key));
        assertEqualsValues(schema, val, tbl.get(null, Tuple.create().set("id", 1L)));
    }

    @Test
    public void getAndPut() {
        SchemaDescriptor schema = schemaDescriptor();

        KeyValueView<Tuple, Tuple> tbl = createTable(schema).keyValueView();

        final Tuple key = Tuple.create().set("id", 1L);
        final Tuple val = Tuple.create().set("val", 11L);
        final Tuple val2 = Tuple.create().set("val", 22L);
        final Tuple val3 = Tuple.create().set("val", 33L);

        assertNull(tbl.get(null, key));

        // Insert new tuple.
        assertNull(tbl.getAndPut(null, key, val));

        assertEqualsValues(schema, val, tbl.get(null, key));
        assertEqualsValues(schema, val, tbl.get(null, Tuple.create().set("id", 1L)));

        assertEqualsValues(schema, val, tbl.getAndPut(null, key, val2));
        assertEqualsValues(schema, val2, tbl.getAndPut(null, key, Tuple.create().set("val", 33L)));

        assertEqualsValues(schema, val3, tbl.get(null, key));
        assertNull(tbl.get(null, Tuple.create().set("id", 2L)));
    }

    @Test
    public void unsupportedOperations() {
        SchemaDescriptor schema = schemaDescriptor();

        KeyValueView<Tuple, Tuple> tbl = createTable(schema).keyValueView();

        assertThrows(UnsupportedOperationException.class, () -> tbl.getNullable(null, Tuple.create(Map.of("id", 1L))));
        assertThrows(UnsupportedOperationException.class, () -> tbl.getNullableAndPut(
                null,
                Tuple.create(Map.of("id", 1L)),
                Tuple.create(Map.of("id", 1L)))
        );
        assertThrows(UnsupportedOperationException.class, () -> tbl.getNullableAndReplace(
                null,
                Tuple.create(Map.of("id", 1L)),
                Tuple.create(Map.of("id", 1L)))
        );
        assertThrows(UnsupportedOperationException.class, () -> tbl.getNullableAndRemove(
                null,
                Tuple.create(Map.of("id", 1L)))
        );
    }

    @Test
    public void getOrDefault() {
        SchemaDescriptor schema = schemaDescriptor();

        KeyValueView<Tuple, Tuple> tbl = createTable(schema).keyValueView();

        final Tuple key = Tuple.create().set("id", 1L);
        final Tuple val = Tuple.create().set("val", 11L);
        final Tuple val2 = Tuple.create().set("val", 22L);
        final Tuple defaultTuple = Tuple.create().set("val", "undefined");

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

    @Test
    public void contains() {
        SchemaDescriptor schema = schemaDescriptor();

        KeyValueView<Tuple, Tuple> tbl = createTable(schema).keyValueView();

        final Tuple key = Tuple.create().set("id", 1L);
        final Tuple val = Tuple.create().set("val", 11L);
        final Tuple val2 = Tuple.create().set("val", 22L);

        // Not-existed value.
        assertFalse(tbl.contains(null, key));

        // Put KV pair.
        tbl.put(null, key, val);
        assertTrue(tbl.contains(null, Tuple.create().set("id", 1L)));

        // Delete key.
        assertTrue(tbl.remove(null, key));
        assertFalse(tbl.contains(null, Tuple.create().set("id", 1L)));

        // Put KV pair.
        tbl.put(null, key, val2);
        assertTrue(tbl.contains(null, Tuple.create().set("id", 1L)));

        // Non-existed key.
        assertFalse(tbl.contains(null, Tuple.create().set("id", 2L)));
        tbl.remove(null, Tuple.create().set("id", 2L));
        assertFalse(tbl.contains(null, Tuple.create().set("id", 2L)));
    }

    @Test
    public void remove() {
        SchemaDescriptor schema = schemaDescriptor();

        KeyValueView<Tuple, Tuple> tbl = createTable(schema).keyValueView();

        final Tuple key = Tuple.create().set("id", 1L);
        final Tuple key2 = Tuple.create().set("id", 2L);
        final Tuple val = Tuple.create().set("val", 11L);
        final Tuple val2 = Tuple.create().set("val", 22L);

        // Put KV pair.
        tbl.put(null, key, val);

        // Delete existed key.
        assertEqualsValues(schema, val, tbl.get(null, key));
        assertTrue(tbl.remove(null, key));
        assertNull(tbl.get(null, key));

        // Delete already deleted key.
        assertFalse(tbl.remove(null, key));

        // Put KV pair.
        tbl.put(null, key, val2);
        assertEqualsValues(schema, val2, tbl.get(null, key));

        // Delete existed key.
        assertTrue(tbl.remove(null, Tuple.create().set("id", 1L)));
        assertNull(tbl.get(null, key));

        // Delete not existed key.
        assertNull(tbl.get(null, key2));
        assertFalse(tbl.remove(null, key2));
    }

    @Test
    public void removeExact() {
        SchemaDescriptor schema = schemaDescriptor();

        final KeyValueView<Tuple, Tuple> tbl = createTable(schema).keyValueView();

        final Tuple key = Tuple.create().set("id", 1L);
        final Tuple key2 = Tuple.create().set("id", 2L);
        final Tuple val = Tuple.create().set("val", 11L);
        final Tuple val2 = Tuple.create().set("val", 22L);

        // Put KV pair.
        tbl.put(null, key, val);
        assertEqualsValues(schema, val, tbl.get(null, key));

        // Fails to delete KV pair with unexpected value.
        assertFalse(tbl.remove(null, key, val2));
        assertEqualsValues(schema, val, tbl.get(null, key));

        // Delete KV pair with expected value.
        assertTrue(tbl.remove(null, key, val));
        assertNull(tbl.get(null, key));

        // Once again.
        assertFalse(tbl.remove(null, key, val));
        assertNull(tbl.get(null, key));

        // Try to remove non-existed key.
        assertThrows(Exception.class, () -> tbl.remove(null, key, null));
        assertNull(tbl.get(null, key));

        // Put KV pair.
        tbl.put(null, key, val2);
        assertEqualsValues(schema, val2, tbl.get(null, key));

        // Check null value ignored.
        assertThrows(Exception.class, () -> tbl.remove(null, key, null));
        assertEqualsValues(schema, val2, tbl.get(null, key));

        // Delete KV pair with expected value.
        assertTrue(tbl.remove(null, key, val2));
        assertNull(tbl.get(null, key));

        assertFalse(tbl.remove(null, key2, val2));
        assertNull(tbl.get(null, key2));
    }

    @Test
    public void replace() {
        SchemaDescriptor schema = schemaDescriptor();

        KeyValueView<Tuple, Tuple> tbl = createTable(schema).keyValueView();

        final Tuple key = Tuple.create().set("id", 1L);
        final Tuple key2 = Tuple.create().set("id", 2L);
        final Tuple val = Tuple.create().set("val", 11L);
        final Tuple val2 = Tuple.create().set("val", 22L);
        final Tuple val3 = Tuple.create().set("val", 33L);

        // Ignore replace operation for non-existed KV pair.
        assertFalse(tbl.replace(null, key, val));
        assertNull(tbl.get(null, key));

        tbl.put(null, key, val);

        // Replace existed KV pair.
        assertTrue(tbl.replace(null, key, val2));
        assertEqualsValues(schema, val2, tbl.get(null, key));

        // Ignore replace operation for non-existed KV pair.
        assertFalse(tbl.replace(null, key2, val3));
        assertNull(tbl.get(null, key2));

        tbl.put(null, key, val3);
        assertEqualsValues(schema, val3, tbl.get(null, key));
    }

    @Test
    public void replaceExact() {
        SchemaDescriptor schema = schemaDescriptor();

        KeyValueView<Tuple, Tuple> tbl = createTable(schema).keyValueView();

        final Tuple key = Tuple.create().set("id", 1L);
        final Tuple key2 = Tuple.create().set("id", 2L);
        final Tuple val = Tuple.create().set("val", 11L);
        final Tuple val2 = Tuple.create().set("val", 22L);

        // Ignore replace operation for non-existed KV pair.
        assertFalse(tbl.replace(null, key2, val, val2));
        assertNull(tbl.get(null, key2));

        tbl.put(null, key, val);

        // Replace existed KV pair.
        assertTrue(tbl.replace(null, key, val, val2));
        assertEqualsValues(schema, val2, tbl.get(null, key));
    }

    @Test
    public void getAll() {
        SchemaDescriptor schema = schemaDescriptor();

        KeyValueView<Tuple, Tuple> tbl = createTable(schema).keyValueView();

        Tuple key1 = Tuple.create().set("id", 1L);
        Tuple key2 = Tuple.create().set("id", 2L);
        Tuple key3 = Tuple.create().set("id", 3L);

        tbl.putAll(
                null,
                Map.of(
                        key1, Tuple.create().set("val", 11L),
                        key3, Tuple.create().set("val", 33L)
                ));

        Map<Tuple, Tuple> res = tbl.getAll(null, List.of(key1, key2, key3));

        assertEquals(2, res.size());
        assertEquals(Tuple.create().set("val", 11L), res.get(key1));
        assertEquals(Tuple.create().set("val", 33L), res.get(key3));
        assertNull(res.get(key2));
    }

    @SuppressWarnings("ConstantConditions")
    @Test
    public void nullKeyValidation() {
        SchemaDescriptor schema = schemaDescriptor();
        KeyValueView<Tuple, Tuple> tbl = createTable(schema).keyValueView();

        final Tuple val = Tuple.create().set("val", 11L);
        final Tuple val2 = Tuple.create().set("val", 22L);

        // Null key.
        assertThrows(NullPointerException.class, () -> tbl.contains(null, null));

        assertThrows(NullPointerException.class, () -> tbl.get(null, null));
        assertThrows(NullPointerException.class, () -> tbl.getAndPut(null, null, val));
        assertThrows(NullPointerException.class, () -> tbl.getAndRemove(null, null));
        assertThrows(NullPointerException.class, () -> tbl.getAndReplace(null, null, val));
        assertThrows(NullPointerException.class, () -> tbl.getOrDefault(null, null, val));

        assertThrows(NullPointerException.class, () -> tbl.put(null, null, val));
        assertThrows(NullPointerException.class, () -> tbl.putIfAbsent(null, null, val));
        assertThrows(NullPointerException.class, () -> tbl.remove(null, null));
        assertThrows(NullPointerException.class, () -> tbl.remove(null, null, val));
        assertThrows(NullPointerException.class, () -> tbl.replace(null, null, val));
        assertThrows(NullPointerException.class, () -> tbl.replace(null, null, val, val2));

        assertThrows(NullPointerException.class, () -> tbl.getAll(null, null));
        assertThrows(NullPointerException.class, () -> tbl.getAll(null, Collections.singleton(null)));
        assertThrows(NullPointerException.class, () -> tbl.putAll(null, null));
        assertThrows(NullPointerException.class, () -> tbl.putAll(null, Collections.singletonMap(null, val)));
        assertThrows(NullPointerException.class, () -> tbl.removeAll(null, null));
        assertThrows(NullPointerException.class, () -> tbl.removeAll(null, Collections.singleton(null)));
    }

    @SuppressWarnings("ConstantConditions")
    @Test
    public void nonNullableValueColumn() {
        SchemaDescriptor schema = schemaDescriptor();
        KeyValueView<Tuple, Tuple> tbl = createTable(schema).keyValueView();

        final Tuple key = Tuple.create().set("id", 11L);
        final Tuple val = Tuple.create().set("val", 22L);

        assertThrows(NullPointerException.class, () -> tbl.getAndPut(null, key, null));
        assertThrows(NullPointerException.class, () -> tbl.getAndReplace(null, key, null));

        assertThrows(NullPointerException.class, () -> tbl.put(null, key, null));
        assertThrows(NullPointerException.class, () -> tbl.putIfAbsent(null, key, null));
        assertThrows(NullPointerException.class, () -> tbl.remove(null, key, null));
        assertThrows(NullPointerException.class, () -> tbl.replace(null, key, null));
        assertThrows(NullPointerException.class, () -> tbl.replace(null, key, null, val));
        assertThrows(NullPointerException.class, () -> tbl.replace(null, key, val, null));

        assertThrows(NullPointerException.class, () -> tbl.putAll(null, Collections.singletonMap(key, null)));
    }

    @Test
    void retriesOnInternalSchemaVersionMismatchException() throws Exception {
        SchemaDescriptor schema = schemaDescriptor();
        InternalTable internalTable = spy(createInternalTable(schema));
        ReflectionMarshallersProvider marshallers = new ReflectionMarshallersProvider();

        KeyValueView<Tuple, Tuple> view = new KeyValueBinaryViewImpl(
                internalTable,
                new DummySchemaManagerImpl(schema),
                schemaVersions,
                mock(IgniteSql.class),
                marshallers
        );

        BinaryRow resultRow = new TupleMarshallerImpl(schema).marshal(Tuple.create().set("ID", 1L).set("VAL", 2L));

        doReturn(failedFuture(new InternalSchemaVersionMismatchException()))
                .doReturn(completedFuture(resultRow))
                .when(internalTable).get(any(), any());

        Tuple result = view.get(null, Tuple.create().set("ID", 1L));

        assertThat(result.longValue("VAL"), is(2L));

        verify(internalTable, times(2)).get(any(), isNull());
    }

    private SchemaDescriptor schemaDescriptor() {
        return new SchemaDescriptor(
                SCHEMA_VERSION,
                new Column[]{new Column("ID", NativeTypes.INT64, false)},
                new Column[]{new Column("VAL", NativeTypes.INT64, false)}
        );
    }

    /**
     * Check value columns equality.
     *
     * @param schema Schema.
     * @param expected Expected tuple.
     * @param actual Actual tuple.
     */
    void assertEqualsValues(SchemaDescriptor schema, Tuple expected, Tuple actual) {
        for (int i = 0; i < schema.valueColumns().size(); i++) {
            final Column col = schema.valueColumns().get(i);

            final Object val1 = expected.value(col.name());
            final Object val2 = actual.value(col.name());

            assertEquals(val1, val2, "Key columns equality check failed: colIdx=" + col.positionInRow());
        }
    }
}
