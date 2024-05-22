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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.SchemaTestUtils;
import org.apache.ignite.internal.type.NativeType;
import org.apache.ignite.internal.type.NativeTypeSpec;
import org.apache.ignite.internal.type.NativeTypes;
import org.apache.ignite.lang.MarshallerException;
import org.apache.ignite.lang.UnexpectedNullValueException;
import org.apache.ignite.table.KeyValueView;
import org.apache.ignite.table.mapper.Mapper;
import org.junit.jupiter.api.Test;

/**
 * Basic table operations test.
 */
public class KeyValueViewOperationsSimpleSchemaTest extends TableKvOperationsTestBase {
    /**
     * Creates table view.
     *
     * @return Table KV-view.
     */
    private KeyValueView<Long, Long> kvView() {
        return kvViewForValueType(NativeTypes.INT64, Long.class, true);
    }

    @Test
    public void put() {
        KeyValueView<Long, Long> tbl = kvView();

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

        assertThrows(UnexpectedNullValueException.class, () -> tbl.get(null, 1L));
        assertNull(tbl.getNullable(null, 1L).get());

        // Put KV pair.
        tbl.put(null, 1L, 33L);
        assertEquals(33L, tbl.get(null, 1L));
    }

    @Test
    public void putIfAbsent() {
        KeyValueView<Long, Long> tbl = kvView();

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

    @Test
    public void getNullable() {
        KeyValueView<Long, Long> tbl = kvView();

        assertNull(tbl.getNullable(null, 1L));

        // Put KV pair.
        tbl.put(null, 1L, 11L);

        assertEquals(11L, tbl.getNullable(null, 1L).get());

        tbl.put(null, 1L, null);

        assertThrows(UnexpectedNullValueException.class, () -> tbl.get(null, 1L));
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

    @Test
    public void getOrDefault() {
        KeyValueView<Long, Long> tbl = kvView();

        assertEquals(Long.MAX_VALUE, tbl.getOrDefault(null, 1L, Long.MAX_VALUE));
        assertNull(tbl.getOrDefault(null, 1L, null));

        // Put KV pair.
        tbl.put(null, 1L, 11L);

        assertEquals(11L, tbl.getOrDefault(null, 1L, Long.MAX_VALUE));
        assertEquals(11L, tbl.getOrDefault(null, 1L, null));

        tbl.put(null, 1L, null);

        assertThrows(UnexpectedNullValueException.class, () -> tbl.get(null, 1L));
        assertNull(tbl.getOrDefault(null, 1L, null));
        assertEquals(Long.MAX_VALUE, tbl.getOrDefault(null, 1L, Long.MAX_VALUE));

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

    @Test
    public void getAndPut() {
        KeyValueView<Long, Long> tbl = kvView();

        // Insert new tuple.
        assertNull(tbl.getAndPut(null, 1L, 11L));

        assertEquals(11L, tbl.get(null, 1L));

        assertEquals(11L, tbl.getAndPut(null, 1L, 22L));
        assertEquals(22L, tbl.get(null, 1L));

        tbl.put(null, 1L, null);
        assertTrue(tbl.contains(null, 1L));
        assertNull(tbl.getNullable(null, 1L).get());

        assertThrows(UnexpectedNullValueException.class, () -> tbl.getAndPut(null, 1L, 33L));
        assertEquals(33L, tbl.getNullable(null, 1L).get()); // Previous operation applied.

        // Check null value
        assertNotNull(tbl.getAndPut(null, 1L, null));
    }

    @Test
    public void getNullableAndPut() {
        KeyValueView<Long, Long> tbl = kvView();

        // getNullableAndPut
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

    @Test
    public void contains() {
        KeyValueView<Long, Long> tbl = kvView();

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

    @Test
    public void remove() {
        KeyValueView<Long, Long> tbl = kvView();

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

    @Test
    public void getAndRemove() {
        KeyValueView<Long, Long> tbl = kvView();

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

        assertThrows(UnexpectedNullValueException.class, () -> tbl.getAndRemove(null, 1L));
        assertFalse(tbl.contains(null, 1L));
        assertNull(tbl.getNullable(null, 1L));
    }

    @Test
    public void getNullableAndRemove() {
        KeyValueView<Long, Long> tbl = kvView();

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

    @Test
    public void removeExact() {
        KeyValueView<Long, Long> tbl = kvView();

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

    @Test
    public void replace() {
        KeyValueView<Long, Long> tbl = kvView();

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

    @Test
    public void getAndReplace() {
        KeyValueView<Long, Long> tbl = kvView();

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
        assertThrows(UnexpectedNullValueException.class, () -> tbl.getAndReplace(null, 1L, 33L));
        assertEquals(33L, tbl.get(null, 1L));

        // Check null value.
        assertEquals(33, tbl.getAndReplace(null, 1L, null));
        assertNull(tbl.getNullable(null, 1L).get());
    }

    @Test
    public void replaceExact() {
        KeyValueView<Long, Long> tbl = kvView();

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

    @Test
    public void putGetAllTypes() {
        Random rnd = new Random();
        Long key = 42L;

        List<NativeType> allTypes = List.of(
                NativeTypes.BOOLEAN,
                NativeTypes.INT8,
                NativeTypes.INT16,
                NativeTypes.INT32,
                NativeTypes.INT64,
                NativeTypes.FLOAT,
                NativeTypes.DOUBLE,
                NativeTypes.DATE,
                NativeTypes.UUID,
                NativeTypes.numberOf(20),
                NativeTypes.decimalOf(25, 5),
                NativeTypes.bitmaskOf(22),
                NativeTypes.time(0),
                NativeTypes.datetime(6),
                NativeTypes.timestamp(6),
                NativeTypes.BYTES,
                NativeTypes.STRING
        );

        // Validate all types are tested.
        assertEquals(Set.of(NativeTypeSpec.values()),
                allTypes.stream().map(NativeType::spec).collect(Collectors.toSet()));

        for (NativeType type : allTypes) {
            final Object val = SchemaTestUtils.generateRandomValue(rnd, type);

            assertFalse(type.mismatch(NativeTypes.fromObject(val)));

            KeyValueView<Long, Object> kvView = kvViewForValueType(type, (Class<Object>) val.getClass(), true);

            kvView.put(null, key, val);

            if (val instanceof byte[]) {
                assertArrayEquals((byte[]) val, (byte[]) kvView.get(null, key));
            } else {
                assertEquals(val, kvView.get(null, key));
            }
        }
    }

    @Test
    public void getAll() {
        KeyValueView<Long, Long> tbl = kvView();

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

    @Test
    public void putAll() {
        KeyValueView<Long, Long> tbl = kvView();

        // Check empty collection.
        tbl.putAll(null, Map.of());

        // Check non-null values.
        tbl.putAll(null, Map.of(1L, 11L, 3L, 33L));

        assertEquals(11L, tbl.get(null, 1L));
        assertEquals(33L, tbl.get(null, 3L));

        // Check null values.
        Map<Long, Long> map = new HashMap<>() {
            {
                put(1L, 11L);
                put(3L, null);
            }
        };

        tbl.putAll(null, map);

        assertEquals(11L, tbl.get(null, 1L));
        assertNull(tbl.get(null, 2L));
        assertNull(tbl.getNullable(null, 3L).get());
    }

    @Test
    public void removeAll() {
        KeyValueView<Long, Long> tbl = kvView();

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
    @Test
    public void nullKeyValidation() {
        final KeyValueView<Long, Long> tbl = kvView();

        // Null key.
        assertThrows(NullPointerException.class, () -> tbl.contains(null, null));

        assertThrows(NullPointerException.class, () -> tbl.get(null, null));
        assertThrows(NullPointerException.class, () -> tbl.getAndPut(null, null, 1L));
        assertThrows(NullPointerException.class, () -> tbl.getAndRemove(null, null));
        assertThrows(NullPointerException.class, () -> tbl.getAndReplace(null, null, 1L));
        assertThrows(NullPointerException.class, () -> tbl.getNullable(null, null));
        assertThrows(NullPointerException.class, () -> tbl.getNullableAndRemove(null, null));
        assertThrows(NullPointerException.class, () -> tbl.getNullableAndReplace(null, null, 1L));
        assertThrows(NullPointerException.class, () -> tbl.getNullableAndPut(null, null, 1L));
        assertThrows(NullPointerException.class, () -> tbl.getOrDefault(null, null, 1L));

        assertThrows(NullPointerException.class, () -> tbl.put(null, null, 1L));
        assertThrows(NullPointerException.class, () -> tbl.putIfAbsent(null, null, 1L));
        assertThrows(NullPointerException.class, () -> tbl.remove(null, null));
        assertThrows(NullPointerException.class, () -> tbl.remove(null, null, 1L));
        assertThrows(NullPointerException.class, () -> tbl.replace(null, null, 1L));
        assertThrows(NullPointerException.class, () -> tbl.replace(null, null, 1L, 2L));

        assertThrows(NullPointerException.class, () -> tbl.getAll(null, null));
        assertThrows(NullPointerException.class, () -> tbl.getAll(null, Collections.singleton(null)));
        assertThrows(NullPointerException.class, () -> tbl.putAll(null, null));
        assertThrows(NullPointerException.class, () -> tbl.putAll(null, Collections.singletonMap(null, 1L)));
        assertThrows(NullPointerException.class, () -> tbl.removeAll(null, null));
        assertThrows(NullPointerException.class, () -> tbl.removeAll(null, Collections.singleton(null)));
    }

    @SuppressWarnings("ConstantConditions")
    @Test
    public void nonNullableValueColumn() {
        KeyValueView<Long, Long> tbl = kvViewForValueType(NativeTypes.INT64, Long.class, false);

        assertThrows(MarshallerException.class, () -> tbl.getAndPut(null, 1L, null));
        assertThrows(MarshallerException.class, () -> tbl.getAndReplace(null, 1L, null));
        assertThrows(MarshallerException.class, () -> tbl.getNullableAndReplace(null, 1L, null));
        assertThrows(MarshallerException.class, () -> tbl.getNullableAndPut(null, 1L, null));

        assertThrows(MarshallerException.class, () -> tbl.put(null, 1L, null));
        assertThrows(MarshallerException.class, () -> tbl.putIfAbsent(null, 1L, null));
        assertThrows(MarshallerException.class, () -> tbl.remove(null, 1L, null));
        assertThrows(MarshallerException.class, () -> tbl.replace(null, 1L, null));
        assertThrows(MarshallerException.class, () -> tbl.replace(null, 1L, null, 2L));
        assertThrows(MarshallerException.class, () -> tbl.replace(null, 1L, 1L, null));

        assertThrows(MarshallerException.class, () -> tbl.putAll(null, Collections.singletonMap(1L, null)));
    }

    /**
     * Creates key-value view.
     *
     * @param type Value column native type.
     * @param valueClass Value class.
     * @param nullable Nullability flag for the value type.
     */
    private <T> KeyValueView<Long, T> kvViewForValueType(NativeType type, Class<T> valueClass, boolean nullable) {
        Mapper<Long> keyMapper = Mapper.of(Long.class, "id");
        Mapper<T> valMapper = Mapper.of(valueClass, "val");

        SchemaDescriptor schema = new SchemaDescriptor(
                SCHEMA_VERSION,
                new Column[]{new Column("ID", NativeTypes.INT64, false)},
                new Column[]{new Column("VAL", type, nullable)}
        );

        TableViewInternal table = createTable(schema);

        return table.keyValueView(keyMapper, valMapper);
    }
}
