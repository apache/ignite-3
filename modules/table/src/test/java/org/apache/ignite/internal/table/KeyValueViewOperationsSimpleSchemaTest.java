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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Answers.RETURNS_DEEP_STUBS;

import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.NativeType;
import org.apache.ignite.internal.schema.NativeTypeSpec;
import org.apache.ignite.internal.schema.NativeTypes;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.SchemaTestUtils;
import org.apache.ignite.internal.storage.basic.ConcurrentHashMapPartitionStorage;
import org.apache.ignite.internal.table.distributed.storage.VersionedRowStore;
import org.apache.ignite.internal.table.impl.DummyInternalTableImpl;
import org.apache.ignite.internal.table.impl.DummySchemaManagerImpl;
import org.apache.ignite.internal.tx.TxManager;
import org.apache.ignite.internal.tx.impl.HeapLockManager;
import org.apache.ignite.internal.tx.impl.TxManagerImpl;
import org.apache.ignite.lang.UnexpectedNullValueException;
import org.apache.ignite.network.ClusterService;
import org.apache.ignite.table.KeyValueView;
import org.apache.ignite.table.mapper.Mapper;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

/**
 * Basic table operations test.
 *
 * <p>TODO: IGNITE-14487 Add bulk operations tests.
 * TODO: IGNITE-14487 Add async operations tests.
 */
public class KeyValueViewOperationsSimpleSchemaTest {
    /**
     * Creates table view.
     *
     * @return Table KV-view.
     */
    private KeyValueView<Long, Long> kvView() {
        return kvViewForValueType(NativeTypes.INT64, Long.class);
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

        // Remove KV pair.
        tbl.remove(null, 1L);

        assertNull(tbl.get(null, 1L));

        // Put `null` value.
        tbl.put(null, 1L, null);

        assertThrows(UnexpectedNullValueException.class, () -> tbl.get(null, 1L));
        assertNull(tbl.getNullable(null, 1L).get());

        // Put KV pair.
        tbl.put(null, 1L, 33L);
        assertEquals(33L, tbl.get(null, 1L));

        assertThrows(IllegalArgumentException.class, () -> tbl.put(null, null, 33L));
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

        // Insert KV pair.
        assertTrue(tbl.putIfAbsent(null, 2L, null));
        assertNull(tbl.getNullable(null, 2L).get());
        assertTrue(tbl.contains(null, 2L));

        // Update KV pair.
        assertFalse(tbl.putIfAbsent(null, 2L, 33L));
        assertNull(tbl.getNullable(null, 2L).get());

        assertThrows(IllegalArgumentException.class, () -> tbl.putIfAbsent(null, null, 33L));
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

        assertThrows(IllegalArgumentException.class, () -> tbl.getNullable(null, null));
    }

    @Test
    public void getOrDefault() {
        KeyValueView<Long, Long> tbl = kvView();

        assertEquals(Long.MAX_VALUE, tbl.getOrDefault(null, 1L, Long.MAX_VALUE));

        // Put KV pair.
        tbl.put(null, 1L, 11L);

        assertEquals(11L, tbl.getOrDefault(null, 1L, Long.MAX_VALUE));

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

        assertThrows(IllegalArgumentException.class, () -> tbl.getOrDefault(null, null, Long.MAX_VALUE));
    }

    @Test
    public void getAndPut() {
        KeyValueView<Long, Long> tbl = kvView();

        // Insert new tuple.
        assertNull(tbl.getAndPut(null, 1L, 11L));

        assertEquals(11L, tbl.get(null, 1L));

        assertEquals(11L, tbl.getAndPut(null, 1L, 22L));
        assertEquals(22L, tbl.get(null, 1L));

        assertEquals(22L, tbl.getAndPut(null, 1L, null));
        assertTrue(tbl.contains(null, 1L));
        assertNull(tbl.getNullable(null, 1L).get());

        assertThrows(UnexpectedNullValueException.class, () -> tbl.getAndPut(null, 1L, 33L));
        assertEquals(33L, tbl.getNullable(null, 1L).get()); // Previous operation applied.

        assertThrows(IllegalArgumentException.class, () -> tbl.getAndPut(null, null, 1L));
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

        assertThrows(IllegalArgumentException.class, () -> tbl.getNullableAndPut(null, null, 1L));
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

        assertThrows(IllegalArgumentException.class, () -> tbl.contains(null, null));
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

        assertThrows(IllegalArgumentException.class, () -> tbl.remove(null, null));
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

        assertThrows(IllegalArgumentException.class, () -> tbl.getAndRemove(null, null));
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

        assertThrows(IllegalArgumentException.class, () -> tbl.getNullableAndRemove(null, null));
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

        assertThrows(IllegalArgumentException.class, () -> tbl.remove(null, null));
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

        assertThrows(IllegalArgumentException.class, () -> tbl.replace(null, null, 33L));
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
        assertEquals(22L, tbl.getAndReplace(null, 1L, null));
        assertTrue(tbl.contains(null, 1L));
        assertNull(tbl.getNullable(null, 1L).get());

        // Replace null-value
        assertThrows(UnexpectedNullValueException.class, () -> tbl.getAndReplace(null, 1L, 33L));
        assertEquals(33L, tbl.get(null, 1L));

        assertThrows(IllegalArgumentException.class, () -> tbl.replace(null, null, 33L));
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

        // Remove non-existed KV pair.
        assertFalse(tbl.replace(null, 2L, null, null));

        assertThrows(IllegalArgumentException.class, () -> tbl.replace(null, null, null, 33L));
    }

    @Test
    public void putGetAllTypes() {
        Random rnd = new Random();
        Long key = 42L;

        List<NativeType> allTypes = List.of(
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
                NativeTypes.time(),
                NativeTypes.datetime(),
                NativeTypes.timestamp(),
                NativeTypes.BYTES,
                NativeTypes.STRING);

        // Validate all types are tested.
        assertEquals(Set.of(NativeTypeSpec.values()),
                allTypes.stream().map(NativeType::spec).collect(Collectors.toSet()));

        for (NativeType type : allTypes) {
            final Object val = SchemaTestUtils.generateRandomValue(rnd, type);

            assertFalse(type.mismatch(NativeTypes.fromObject(val)));

            KeyValueViewImpl<Long, Object> kvView = kvViewForValueType(NativeTypes.fromObject(val),
                    (Class<Object>) val.getClass());

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
        KeyValueView<Long, Long> kvView = kvView();

        kvView.putAll(
                null,
                Map.of(
                        1L, 11L,
                        3L, 33L
                ));

        Map<Long, Long> res = kvView.getAll(null, List.of(1L, 2L, 3L));

        assertEquals(2, res.size());
        assertEquals(11L, res.get(1L));
        assertEquals(33L, res.get(3L));
        assertNull(res.get(22L));
    }

    /**
     * Creates key-value view.
     *
     * @param type Value column native type.
     * @param valueClass Value class.
     */
    private <T> KeyValueViewImpl<Long, T> kvViewForValueType(NativeType type, Class<T> valueClass) {
        ClusterService clusterService = Mockito.mock(ClusterService.class, RETURNS_DEEP_STUBS);
        Mockito.when(clusterService.topologyService().localMember().address())
                .thenReturn(DummyInternalTableImpl.ADDR);

        TxManager txManager = new TxManagerImpl(clusterService, new HeapLockManager());

        DummyInternalTableImpl table = new DummyInternalTableImpl(
                new VersionedRowStore(new ConcurrentHashMapPartitionStorage(), txManager), txManager);

        Mapper<Long> keyMapper = Mapper.of(Long.class, "id");
        Mapper<T> valMapper = Mapper.of(valueClass, "val");

        SchemaDescriptor schema = new SchemaDescriptor(
                1,
                new Column[]{new Column("id", NativeTypes.INT64, false)},
                new Column[]{new Column("val", type, true)}
        );

        return new KeyValueViewImpl<>(
                table,
                new DummySchemaManagerImpl(schema),
                keyMapper,
                valMapper
        );
    }
}
