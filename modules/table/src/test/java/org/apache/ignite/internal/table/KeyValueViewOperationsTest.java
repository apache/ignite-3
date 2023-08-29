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

import static org.apache.ignite.internal.schema.NativeTypes.BOOLEAN;
import static org.apache.ignite.internal.schema.NativeTypes.BYTES;
import static org.apache.ignite.internal.schema.NativeTypes.DATE;
import static org.apache.ignite.internal.schema.NativeTypes.DOUBLE;
import static org.apache.ignite.internal.schema.NativeTypes.FLOAT;
import static org.apache.ignite.internal.schema.NativeTypes.INT16;
import static org.apache.ignite.internal.schema.NativeTypes.INT32;
import static org.apache.ignite.internal.schema.NativeTypes.INT64;
import static org.apache.ignite.internal.schema.NativeTypes.INT8;
import static org.apache.ignite.internal.schema.NativeTypes.STRING;
import static org.apache.ignite.internal.schema.NativeTypes.datetime;
import static org.apache.ignite.internal.schema.NativeTypes.time;
import static org.apache.ignite.internal.schema.NativeTypes.timestamp;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Answers.RETURNS_DEEP_STUBS;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.ignite.internal.replicator.ReplicaService;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.NativeTypeSpec;
import org.apache.ignite.internal.schema.NativeTypes;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.testobjects.TestObjectWithAllTypes;
import org.apache.ignite.internal.table.impl.DummyInternalTableImpl;
import org.apache.ignite.internal.table.impl.DummySchemaManagerImpl;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.lang.MarshallerException;
import org.apache.ignite.network.ClusterService;
import org.apache.ignite.network.MessagingService;
import org.apache.ignite.table.KeyValueView;
import org.apache.ignite.table.mapper.Mapper;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

/**
 * Basic table operations test.
 */
public class KeyValueViewOperationsTest extends BaseIgniteAbstractTest {
    private final Random rnd = new Random();

    @Test
    public void put() {
        final TestKeyObject key = TestKeyObject.randomObject(rnd);
        final TestObjectWithAllTypes obj = TestObjectWithAllTypes.randomObject(rnd);
        final TestObjectWithAllTypes obj2 = TestObjectWithAllTypes.randomObject(rnd);
        final TestObjectWithAllTypes obj3 = TestObjectWithAllTypes.randomObject(rnd);

        KeyValueView<TestKeyObject, TestObjectWithAllTypes> tbl = kvView();

        assertNull(tbl.get(null, key));

        // Put KV pair.
        tbl.put(null, key, obj);

        assertEquals(obj, tbl.get(null, key));
        assertEquals(obj, tbl.get(null, key));

        // Update KV pair.
        tbl.put(null, key, obj2);

        assertEquals(obj2, tbl.get(null, key));
        assertEquals(obj2, tbl.get(null, key));

        // Remove KV pair.
        tbl.remove(null, key);

        assertNull(tbl.get(null, key));

        // Put KV pair.
        tbl.put(null, key, obj3);
        assertEquals(obj3, tbl.get(null, key));
    }

    @Test
    public void putIfAbsent() {
        final TestKeyObject key = TestKeyObject.randomObject(rnd);
        final TestObjectWithAllTypes obj = TestObjectWithAllTypes.randomObject(rnd);
        final TestObjectWithAllTypes obj2 = TestObjectWithAllTypes.randomObject(rnd);

        KeyValueView<TestKeyObject, TestObjectWithAllTypes> tbl = kvView();

        assertNull(tbl.get(null, key));

        // Insert new KV pair.
        assertTrue(tbl.putIfAbsent(null, key, obj));

        assertEquals(obj, tbl.get(null, key));

        // Update KV pair.
        assertFalse(tbl.putIfAbsent(null, key, obj2));

        assertEquals(obj, tbl.get(null, key));
    }

    @Test
    public void getNullable() {
        final TestKeyObject key = TestKeyObject.randomObject(rnd);
        final TestObjectWithAllTypes val = TestObjectWithAllTypes.randomObject(rnd);
        final TestObjectWithAllTypes val2 = TestObjectWithAllTypes.randomObject(rnd);

        KeyValueView<TestKeyObject, TestObjectWithAllTypes> tbl = kvView();

        assertNull(tbl.getNullable(null, key));

        // Put KV pair.
        tbl.put(null, key, val);

        assertEquals(val, tbl.getNullable(null, key).get());

        // Remove KV pair.
        tbl.remove(null, key);

        assertNull(tbl.get(null, key));
        assertNull(tbl.getNullable(null, key));

        // Put KV pair.
        tbl.put(null, key, val2);
        assertEquals(val2, tbl.get(null, key));
        assertEquals(val2, tbl.getNullable(null, key).get());
    }

    @Test
    public void getOrDefault() {
        final TestKeyObject key = TestKeyObject.randomObject(rnd);
        final TestObjectWithAllTypes val = TestObjectWithAllTypes.randomObject(rnd);
        final TestObjectWithAllTypes val2 = TestObjectWithAllTypes.randomObject(rnd);
        final TestObjectWithAllTypes defaultTuple = TestObjectWithAllTypes.randomObject(rnd);

        KeyValueView<TestKeyObject, TestObjectWithAllTypes> tbl = kvView();

        assertEquals(defaultTuple, tbl.getOrDefault(null, key, defaultTuple));
        assertNull(tbl.getOrDefault(null, key, null));

        // Put KV pair.
        tbl.put(null, key, val);

        assertEquals(val, tbl.get(null, key));
        assertEquals(val, tbl.getOrDefault(null, key, null));
        assertEquals(val, tbl.getOrDefault(null, key, defaultTuple));

        // Remove KV pair.
        tbl.remove(null, key);

        assertNull(tbl.get(null, key));
        assertNull(tbl.getOrDefault(null, key, null));
        assertEquals(defaultTuple, tbl.getOrDefault(null, key, defaultTuple));

        // Put KV pair.
        tbl.put(null, key, val2);
        assertEquals(val2, tbl.get(null, key));
        assertEquals(val2, tbl.getOrDefault(null, key, defaultTuple));
    }

    @Test
    public void getAndPut() {
        final TestKeyObject key = TestKeyObject.randomObject(rnd);
        final TestObjectWithAllTypes obj = TestObjectWithAllTypes.randomObject(rnd);
        final TestObjectWithAllTypes obj2 = TestObjectWithAllTypes.randomObject(rnd);
        final TestObjectWithAllTypes obj3 = TestObjectWithAllTypes.randomObject(rnd);

        KeyValueView<TestKeyObject, TestObjectWithAllTypes> tbl = kvView();

        assertNull(tbl.get(null, key));

        // Insert new KV pair.
        assertNull(tbl.getAndPut(null, key, obj));
        assertEquals(obj, tbl.get(null, key));

        // Update KV pair.
        assertEquals(obj, tbl.getAndPut(null, key, obj2));
        assertEquals(obj2, tbl.getAndPut(null, key, obj3));

        assertEquals(obj3, tbl.get(null, key));

        // Check null value
        assertThrows(NullPointerException.class, () -> tbl.getAndPut(null, key, null));
        assertEquals(obj3, tbl.get(null, key));
    }

    @Test
    public void getNullableAndPut() {
        final TestKeyObject key = TestKeyObject.randomObject(rnd);
        final TestObjectWithAllTypes obj = TestObjectWithAllTypes.randomObject(rnd);
        final TestObjectWithAllTypes obj2 = TestObjectWithAllTypes.randomObject(rnd);
        final TestObjectWithAllTypes obj3 = TestObjectWithAllTypes.randomObject(rnd);

        KeyValueView<TestKeyObject, TestObjectWithAllTypes> tbl = kvView();

        assertNull(tbl.get(null, key));

        // Insert new KV pair.
        assertNull(tbl.getNullableAndPut(null, key, obj));
        assertEquals(obj, tbl.get(null, key));

        // Update KV pair.
        assertEquals(obj, tbl.getNullableAndPut(null, key, obj2).get());
        assertEquals(obj2, tbl.getNullableAndPut(null, key, obj3).get());

        assertEquals(obj3, tbl.get(null, key));

        // Check null value.
        assertThrows(MarshallerException.class, () -> tbl.getNullableAndPut(null, key, null));
        assertEquals(obj3, tbl.get(null, key));
    }

    @Test
    public void contains() {
        final TestKeyObject key = TestKeyObject.randomObject(rnd);
        final TestKeyObject key2 = TestKeyObject.randomObject(rnd);
        final TestObjectWithAllTypes obj = TestObjectWithAllTypes.randomObject(rnd);
        final TestObjectWithAllTypes obj2 = TestObjectWithAllTypes.randomObject(rnd);

        KeyValueView<TestKeyObject, TestObjectWithAllTypes> tbl = kvView();

        // Not-existed value.
        assertFalse(tbl.contains(null, key));

        // Put KV pair.
        tbl.put(null, key, obj);
        assertTrue(tbl.contains(null, key));

        // Delete key.
        assertTrue(tbl.remove(null, key));
        assertFalse(tbl.contains(null, key));

        // Put KV pair.
        tbl.put(null, key, obj2);
        assertTrue(tbl.contains(null, key));

        // Delete key.
        tbl.remove(null, key2);
        assertFalse(tbl.contains(null, key2));
    }

    @Test
    public void remove() {
        final TestKeyObject key = TestKeyObject.randomObject(rnd);
        final TestKeyObject key2 = TestKeyObject.randomObject(rnd);
        final TestObjectWithAllTypes obj = TestObjectWithAllTypes.randomObject(rnd);
        final TestObjectWithAllTypes obj2 = TestObjectWithAllTypes.randomObject(rnd);

        KeyValueView<TestKeyObject, TestObjectWithAllTypes> tbl = kvView();

        // Put KV pair.
        tbl.put(null, key, obj);

        // Delete existed key.
        assertEquals(obj, tbl.get(null, key));
        assertTrue(tbl.remove(null, key));
        assertNull(tbl.get(null, key));

        // Delete already deleted key.
        assertFalse(tbl.remove(null, key));

        // Put KV pair.
        tbl.put(null, key, obj2);
        assertEquals(obj2, tbl.get(null, key));

        // Delete existed key.
        assertTrue(tbl.remove(null, key));
        assertNull(tbl.get(null, key));

        // Delete not existed key.
        assertNull(tbl.get(null, key2));
        assertFalse(tbl.remove(null, key2));
    }

    @Test
    public void getAndRemove() {
        final TestKeyObject key = TestKeyObject.randomObject(rnd);
        final TestKeyObject key2 = TestKeyObject.randomObject(rnd);
        final TestObjectWithAllTypes obj = TestObjectWithAllTypes.randomObject(rnd);
        final TestObjectWithAllTypes obj2 = TestObjectWithAllTypes.randomObject(rnd);

        KeyValueView<TestKeyObject, TestObjectWithAllTypes> tbl = kvView();

        // Put KV pair.
        tbl.put(null, key, obj);

        // Delete existed key.
        assertEquals(obj, tbl.getAndRemove(null, key));
        assertFalse(tbl.contains(null, key));

        // Delete already deleted key.
        assertNull(tbl.getAndRemove(null, key));
        assertFalse(tbl.contains(null, key));

        // Put KV pair.
        tbl.put(null, key, obj2);
        assertTrue(tbl.contains(null, key));

        // Delete existed key.
        assertEquals(obj2, tbl.getAndRemove(null, key));
        assertFalse(tbl.contains(null, key));

        // Delete not existed key.
        assertNull(tbl.getAndRemove(null, key2));
    }

    @Test
    public void getNullableAndRemove() {
        final TestKeyObject key = TestKeyObject.randomObject(rnd);
        final TestKeyObject key2 = TestKeyObject.randomObject(rnd);
        final TestObjectWithAllTypes obj = TestObjectWithAllTypes.randomObject(rnd);
        final TestObjectWithAllTypes obj2 = TestObjectWithAllTypes.randomObject(rnd);

        KeyValueView<TestKeyObject, TestObjectWithAllTypes> tbl = kvView();

        // Put KV pair.
        tbl.put(null, key, obj);

        // Delete existed key.
        assertEquals(obj, tbl.getNullableAndRemove(null, key).get());
        assertFalse(tbl.contains(null, key));

        // Delete already deleted key.
        assertNull(tbl.getNullableAndRemove(null, key));
        assertFalse(tbl.contains(null, key));

        // Put KV pair.
        tbl.put(null, key, obj2);
        assertTrue(tbl.contains(null, key));

        // Delete existed key.
        assertEquals(obj2, tbl.getNullableAndRemove(null, key).get());
        assertFalse(tbl.contains(null, key));

        // Delete not existed key.
        assertNull(tbl.getNullableAndRemove(null, key2));
    }

    @Test
    public void removeExact() {
        final TestKeyObject key = TestKeyObject.randomObject(rnd);
        final TestKeyObject key2 = TestKeyObject.randomObject(rnd);
        final TestObjectWithAllTypes obj = TestObjectWithAllTypes.randomObject(rnd);
        final TestObjectWithAllTypes obj2 = TestObjectWithAllTypes.randomObject(rnd);

        KeyValueView<TestKeyObject, TestObjectWithAllTypes> tbl = kvView();

        // Put KV pair.
        tbl.put(null, key, obj);
        assertEquals(obj, tbl.get(null, key));

        // Fails to delete KV pair with unexpected value.
        assertFalse(tbl.remove(null, key, obj2));
        assertEquals(obj, tbl.get(null, key));

        // Delete KV pair with expected value.
        assertTrue(tbl.remove(null, key, obj));
        assertNull(tbl.get(null, key));

        // Once again.
        assertFalse(tbl.remove(null, key, obj));
        assertNull(tbl.get(null, key));

        // Try to remove non-existed key.
        assertFalse(tbl.remove(null, key, obj));
        assertNull(tbl.get(null, key));

        // Put KV pair.
        tbl.put(null, key, obj2);
        assertEquals(obj2, tbl.get(null, key));

        // Check null value.
        assertThrows(MarshallerException.class, () -> tbl.remove(null, key, null));
        assertEquals(obj2, tbl.get(null, key));

        // Delete KV pair with expected value.
        assertTrue(tbl.remove(null, key, obj2));
        assertNull(tbl.get(null, key));

        assertFalse(tbl.remove(null, key2, obj2));
        assertNull(tbl.get(null, key2));
    }

    @Test
    public void replace() {
        final TestKeyObject key = TestKeyObject.randomObject(rnd);
        final TestObjectWithAllTypes obj = TestObjectWithAllTypes.randomObject(rnd);
        final TestObjectWithAllTypes obj2 = TestObjectWithAllTypes.randomObject(rnd);
        final TestObjectWithAllTypes obj3 = TestObjectWithAllTypes.randomObject(rnd);

        KeyValueView<TestKeyObject, TestObjectWithAllTypes> tbl = kvView();

        // Ignore replace operation for non-existed KV pair.
        assertFalse(tbl.replace(null, key, obj));
        assertNull(tbl.get(null, key));

        tbl.put(null, key, obj);

        // Replace existed KV pair.
        assertTrue(tbl.replace(null, key, obj2));
        assertEquals(obj2, tbl.get(null, key));

        tbl.remove(null, key);

        // Ignore replace operation for non-existed KV pair.
        assertFalse(tbl.replace(null, key, obj3));
        assertNull(tbl.get(null, key));

        tbl.put(null, key, obj3);
        assertEquals(obj3, tbl.get(null, key));

        // Check null value.
        assertThrows(MarshallerException.class, () -> tbl.replace(null, key, null));
        assertEquals(obj3, tbl.get(null, key));
    }

    @Test
    public void getAndReplace() {
        final TestKeyObject key = TestKeyObject.randomObject(rnd);
        final TestObjectWithAllTypes obj = TestObjectWithAllTypes.randomObject(rnd);
        final TestObjectWithAllTypes obj2 = TestObjectWithAllTypes.randomObject(rnd);
        final TestObjectWithAllTypes obj3 = TestObjectWithAllTypes.randomObject(rnd);

        KeyValueView<TestKeyObject, TestObjectWithAllTypes> tbl = kvView();

        // Ignore replace operation for non-existed KV pair.
        assertNull(tbl.getAndReplace(null, key, obj));
        assertFalse(tbl.contains(null, key));

        tbl.put(null, key, obj);

        // Replace existed KV pair.
        assertEquals(obj, tbl.getAndReplace(null, key, obj2));
        assertEquals(obj2, tbl.get(null, key));

        tbl.remove(null, key);

        // Ignore replace operation for non-existed KV pair.
        assertNull(tbl.getAndReplace(null, key, obj3));
        assertNull(tbl.get(null, key));

        tbl.put(null, key, obj3);
        assertEquals(obj3, tbl.get(null, key));

        // Check null value.
        assertThrows(NullPointerException.class, () -> tbl.getAndReplace(null, key, null));
        assertEquals(obj3, tbl.get(null, key));
    }

    @Test
    public void getNullableAndReplace() {
        final TestKeyObject key = TestKeyObject.randomObject(rnd);
        final TestObjectWithAllTypes obj = TestObjectWithAllTypes.randomObject(rnd);
        final TestObjectWithAllTypes obj2 = TestObjectWithAllTypes.randomObject(rnd);
        final TestObjectWithAllTypes obj3 = TestObjectWithAllTypes.randomObject(rnd);

        KeyValueView<TestKeyObject, TestObjectWithAllTypes> tbl = kvView();

        // Ignore replace operation for non-existed KV pair.
        assertNull(tbl.getNullableAndReplace(null, key, obj));
        assertFalse(tbl.contains(null, key));

        tbl.put(null, key, obj);

        // Replace existed KV pair.
        assertEquals(obj, tbl.getNullableAndReplace(null, key, obj2).get());
        assertEquals(obj2, tbl.get(null, key));

        tbl.remove(null, key);

        // Ignore replace operation for non-existed KV pair.
        assertNull(tbl.getNullableAndReplace(null, key, obj3));
        assertNull(tbl.get(null, key));

        tbl.put(null, key, obj3);
        assertEquals(obj3, tbl.get(null, key));

        // Check null value.
        assertThrows(MarshallerException.class, () -> tbl.getNullableAndReplace(null, key, null));
        assertEquals(obj3, tbl.get(null, key));
    }

    @Test
    public void replaceExact() {
        final TestKeyObject key = TestKeyObject.randomObject(rnd);
        final TestKeyObject key2 = TestKeyObject.randomObject(rnd);
        final TestObjectWithAllTypes obj = TestObjectWithAllTypes.randomObject(rnd);
        final TestObjectWithAllTypes obj2 = TestObjectWithAllTypes.randomObject(rnd);

        KeyValueView<TestKeyObject, TestObjectWithAllTypes> tbl = kvView();

        // Insert KV pair.
        assertThrows(MarshallerException.class, () -> tbl.replace(null, key, null, obj));
        tbl.put(null, key, obj);
        assertEquals(obj, tbl.get(null, key));
        assertNull(tbl.get(null, key2));

        // Ignore replace operation for non-existed KV pair.
        assertFalse(tbl.replace(null, key2, obj, obj2));
        assertNull(tbl.get(null, key2));

        // Replace existed KV pair.
        assertTrue(tbl.replace(null, key, obj, obj2));
        assertEquals(obj2, tbl.get(null, key));

        // Check null value pair.
        assertThrows(MarshallerException.class, () -> tbl.replace(null, key, obj2, null));
        assertEquals(obj2, tbl.get(null, key));
    }

    @Test
    public void getAll() {
        KeyValueView<TestKeyObject, TestObjectWithAllTypes> kvView = kvView();

        final TestKeyObject key1 = TestKeyObject.randomObject(rnd);
        final TestKeyObject key2 = TestKeyObject.randomObject(rnd);
        final TestKeyObject key3 = TestKeyObject.randomObject(rnd);
        final TestObjectWithAllTypes val1 = TestObjectWithAllTypes.randomObject(rnd);
        final TestObjectWithAllTypes val3 = TestObjectWithAllTypes.randomObject(rnd);

        kvView.putAll(
                null,
                Map.of(
                        key1, val1,
                        key3, val3
                ));

        Map<TestKeyObject, TestObjectWithAllTypes> res = kvView.getAll(null, List.of(key1, key2, key3));

        assertEquals(2, res.size());
        assertEquals(val1, res.get(key1));
        assertEquals(val3, res.get(key3));
        assertNull(res.get(key2));
    }

    @SuppressWarnings("ConstantConditions")
    @Test
    public void nullKeyValidation() {
        KeyValueViewImpl<TestKeyObject, TestObjectWithAllTypes> tbl = kvView();

        TestObjectWithAllTypes val = TestObjectWithAllTypes.randomObject(rnd);
        TestObjectWithAllTypes val2 = TestObjectWithAllTypes.randomObject(rnd);

        // Null key.
        assertThrows(NullPointerException.class, () -> tbl.contains(null, null));

        assertThrows(NullPointerException.class, () -> tbl.get(null, null));
        assertThrows(NullPointerException.class, () -> tbl.getAndPut(null, null, val));
        assertThrows(NullPointerException.class, () -> tbl.getAndRemove(null, null));
        assertThrows(NullPointerException.class, () -> tbl.getAndReplace(null, null, val));
        assertThrows(NullPointerException.class, () -> tbl.getNullable(null, null));
        assertThrows(NullPointerException.class, () -> tbl.getNullableAndRemove(null, null));
        assertThrows(NullPointerException.class, () -> tbl.getNullableAndReplace(null, null, val));
        assertThrows(NullPointerException.class, () -> tbl.getNullableAndPut(null, null, val));
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
        KeyValueViewImpl<TestKeyObject, TestObjectWithAllTypes> tbl = kvView();

        final TestKeyObject key = TestKeyObject.randomObject(rnd);
        final TestObjectWithAllTypes val = TestObjectWithAllTypes.randomObject(rnd);

        assertThrows(NullPointerException.class, () -> tbl.getAndPut(null, key, null));
        assertThrows(NullPointerException.class, () -> tbl.getAndReplace(null, key, null));
        assertThrows(MarshallerException.class, () -> tbl.getNullableAndPut(null, key, null));
        assertThrows(MarshallerException.class, () -> tbl.getNullableAndReplace(null, key, null));
        assertNull(tbl.getOrDefault(null, key, null));

        assertThrows(MarshallerException.class, () -> tbl.put(null, key, null));
        assertThrows(MarshallerException.class, () -> tbl.putIfAbsent(null, key, null));
        assertThrows(MarshallerException.class, () -> tbl.remove(null, key, null));
        assertThrows(MarshallerException.class, () -> tbl.replace(null, key, null));
        assertThrows(MarshallerException.class, () -> tbl.replace(null, key, null, val));
        assertThrows(MarshallerException.class, () -> tbl.replace(null, key, val, null));

        assertThrows(MarshallerException.class, () -> tbl.putAll(null, Collections.singletonMap(key, null)));
    }

    /**
     * Creates key-value view.
     */
    private KeyValueViewImpl<TestKeyObject, TestObjectWithAllTypes> kvView() {
        ClusterService clusterService = Mockito.mock(ClusterService.class, RETURNS_DEEP_STUBS);
        Mockito.when(clusterService.topologyService().localMember().address())
                .thenReturn(DummyInternalTableImpl.ADDR);

        Mockito.when(clusterService.messagingService()).thenReturn(Mockito.mock(MessagingService.class, RETURNS_DEEP_STUBS));

        Mapper<TestKeyObject> keyMapper = Mapper.of(TestKeyObject.class);
        Mapper<TestObjectWithAllTypes> valMapper = Mapper.of(TestObjectWithAllTypes.class);

        Column[] valCols = {
                new Column("primitiveBooleanCol".toUpperCase(), BOOLEAN, false),
                new Column("primitiveByteCol".toUpperCase(), INT8, false),
                new Column("primitiveShortCol".toUpperCase(), INT16, false),
                new Column("primitiveIntCol".toUpperCase(), INT32, false),
                new Column("primitiveLongCol".toUpperCase(), INT64, false),
                new Column("primitiveFloatCol".toUpperCase(), FLOAT, false),
                new Column("primitiveDoubleCol".toUpperCase(), DOUBLE, false),

                new Column("booleanCol".toUpperCase(), BOOLEAN, true),
                new Column("byteCol".toUpperCase(), INT8, true),
                new Column("shortCol".toUpperCase(), INT16, true),
                new Column("intCol".toUpperCase(), INT32, true),
                new Column("longCol".toUpperCase(), INT64, true),
                new Column("nullLongCol".toUpperCase(), INT64, true),
                new Column("floatCol".toUpperCase(), FLOAT, true),
                new Column("doubleCol".toUpperCase(), DOUBLE, true),

                new Column("dateCol".toUpperCase(), DATE, true),
                new Column("timeCol".toUpperCase(), time(), true),
                new Column("dateTimeCol".toUpperCase(), datetime(), true),
                new Column("timestampCol".toUpperCase(), timestamp(), true),

                new Column("uuidCol".toUpperCase(), NativeTypes.UUID, true),
                new Column("bitmaskCol".toUpperCase(), NativeTypes.bitmaskOf(42), true),
                new Column("stringCol".toUpperCase(), STRING, true),
                new Column("nullBytesCol".toUpperCase(), BYTES, true),
                new Column("bytesCol".toUpperCase(), BYTES, true),
                new Column("numberCol".toUpperCase(), NativeTypes.numberOf(12), true),
                new Column("decimalCol".toUpperCase(), NativeTypes.decimalOf(19, 3), true),
        };

        SchemaDescriptor schema = new SchemaDescriptor(
                1,
                new Column[]{new Column("id".toUpperCase(), NativeTypes.INT64, false)},
                valCols
        );

        DummyInternalTableImpl table = new DummyInternalTableImpl(Mockito.mock(ReplicaService.class, RETURNS_DEEP_STUBS), schema);

        // Validate all types are tested.
        Set<NativeTypeSpec> testedTypes = Arrays.stream(valCols).map(c -> c.type().spec())
                .collect(Collectors.toSet());
        Set<NativeTypeSpec> missedTypes = Arrays.stream(NativeTypeSpec.values())
                .filter(t -> !testedTypes.contains(t)).collect(Collectors.toSet());

        assertEquals(Collections.emptySet(), missedTypes);

        return new KeyValueViewImpl<>(
                table,
                new DummySchemaManagerImpl(schema),
                keyMapper,
                valMapper
        );
    }

    /**
     * Test object.
     */
    @SuppressWarnings({"InstanceVariableMayNotBeInitialized", "unused"})
    public static class TestKeyObject {
        public static TestKeyObject randomObject(Random rnd) {
            return new TestKeyObject(rnd.nextLong());
        }

        private long id;

        private TestKeyObject() {
        }

        public TestKeyObject(long id) {
            this.id = id;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }

            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            TestKeyObject that = (TestKeyObject) o;

            return id == that.id;
        }

        @Override
        public int hashCode() {
            return Objects.hash(id);
        }
    }
}
