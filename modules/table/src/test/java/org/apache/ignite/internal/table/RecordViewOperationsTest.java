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
import static org.apache.ignite.internal.type.NativeTypes.BOOLEAN;
import static org.apache.ignite.internal.type.NativeTypes.BYTES;
import static org.apache.ignite.internal.type.NativeTypes.DATE;
import static org.apache.ignite.internal.type.NativeTypes.DOUBLE;
import static org.apache.ignite.internal.type.NativeTypes.FLOAT;
import static org.apache.ignite.internal.type.NativeTypes.INT16;
import static org.apache.ignite.internal.type.NativeTypes.INT32;
import static org.apache.ignite.internal.type.NativeTypes.INT64;
import static org.apache.ignite.internal.type.NativeTypes.INT8;
import static org.apache.ignite.internal.type.NativeTypes.STRING;
import static org.apache.ignite.internal.type.NativeTypes.datetime;
import static org.apache.ignite.internal.type.NativeTypes.time;
import static org.apache.ignite.internal.type.NativeTypes.timestamp;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Answers.RETURNS_DEEP_STUBS;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.ignite.internal.marshaller.ReflectionMarshallersProvider;
import org.apache.ignite.internal.marshaller.testobjects.TestObjectWithAllTypes;
import org.apache.ignite.internal.network.ClusterService;
import org.apache.ignite.internal.network.MessagingService;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.marshaller.reflection.RecordMarshallerImpl;
import org.apache.ignite.internal.table.distributed.replicator.InternalSchemaVersionMismatchException;
import org.apache.ignite.internal.table.impl.DummyInternalTableImpl;
import org.apache.ignite.internal.table.impl.DummySchemaManagerImpl;
import org.apache.ignite.internal.type.NativeTypeSpec;
import org.apache.ignite.internal.type.NativeTypes;
import org.apache.ignite.sql.IgniteSql;
import org.apache.ignite.table.RecordView;
import org.apache.ignite.table.mapper.Mapper;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Basic table operations test.
 */
public class RecordViewOperationsTest extends TableKvOperationsTestBase {

    private final Random rnd = new Random();

    private final Column[] valCols = {
            new Column("primitiveBooleanCol".toUpperCase(), BOOLEAN, false),
            new Column("primitiveByteCol".toUpperCase(), INT8, false),
            new Column("primitiveShortCol".toUpperCase(), INT16, false),
            new Column("primitiveIntCol".toUpperCase(), INT32, false),
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
            new Column("timeCol".toUpperCase(), time(0), true),
            new Column("dateTimeCol".toUpperCase(), datetime(3), true),
            new Column("timestampCol".toUpperCase(), timestamp(3), true),

            new Column("uuidCol".toUpperCase(), NativeTypes.UUID, true),
            new Column("bitmaskCol".toUpperCase(), NativeTypes.bitmaskOf(42), true),
            new Column("stringCol".toUpperCase(), STRING, true),
            new Column("nullBytesCol".toUpperCase(), BYTES, true),
            new Column("bytesCol".toUpperCase(), BYTES, true),
            new Column("numberCol".toUpperCase(), NativeTypes.numberOf(12), true),
            new Column("decimalCol".toUpperCase(), NativeTypes.decimalOf(19, 3), true),
    };

    private final SchemaDescriptor schema = new SchemaDescriptor(
            SCHEMA_VERSION,
            new Column[]{new Column("primitiveLongCol".toUpperCase(), INT64, false)},
            valCols
    );

    private final Mapper<TestObjectWithAllTypes> recMapper = Mapper.of(TestObjectWithAllTypes.class);

    private DummyInternalTableImpl internalTable;

    @BeforeEach
    void createInternalTable() {
        internalTable = spy(createInternalTable(schema));
    }

    @Test
    public void upsert() {
        final TestObjectWithAllTypes key = key(rnd);

        final TestObjectWithAllTypes obj = randomObject(rnd, key);
        final TestObjectWithAllTypes obj2 = randomObject(rnd, key);
        final TestObjectWithAllTypes obj3 = randomObject(rnd, key);

        RecordView<TestObjectWithAllTypes> tbl = recordView();

        assertNull(tbl.get(null, key));

        // Insert new row.
        tbl.upsert(null, obj);
        assertEquals(obj, tbl.get(null, key));

        // Upsert row.
        tbl.upsert(null, obj2);
        assertEquals(obj2, tbl.get(null, key));

        // Remove row.
        tbl.delete(null, key);
        assertNull(tbl.get(null, key));

        // Insert new row.
        tbl.upsert(null, obj3);
        assertEquals(obj3, tbl.get(null, key));
    }

    @Test
    public void insert() {
        final TestObjectWithAllTypes key = key(rnd);
        final TestObjectWithAllTypes obj = randomObject(rnd, key);
        final TestObjectWithAllTypes obj2 = randomObject(rnd, key);

        RecordView<TestObjectWithAllTypes> tbl = recordView();

        assertNull(tbl.get(null, key));

        // Insert new row.
        assertTrue(tbl.insert(null, obj));
        assertEquals(obj, tbl.get(null, key));

        // Ignore existed row pair.
        assertFalse(tbl.insert(null, obj2));
        assertEquals(obj, tbl.get(null, key));
    }

    @Test
    public void getAndUpsert() {
        final TestObjectWithAllTypes key = key(rnd);
        final TestObjectWithAllTypes obj = randomObject(rnd, key);
        final TestObjectWithAllTypes obj2 = randomObject(rnd, key);
        final TestObjectWithAllTypes obj3 = randomObject(rnd, key);

        RecordView<TestObjectWithAllTypes> tbl = recordView();

        assertNull(tbl.get(null, key));

        // Insert new row.
        assertNull(tbl.getAndUpsert(null, obj));
        assertEquals(obj, tbl.get(null, key));

        // Update exited row.
        assertEquals(obj, tbl.getAndUpsert(null, obj2));
        assertEquals(obj2, tbl.getAndUpsert(null, obj3));

        assertEquals(obj3, tbl.get(null, key));
    }

    @Test
    public void remove() {
        final TestObjectWithAllTypes key = key(rnd);
        final TestObjectWithAllTypes obj = randomObject(rnd, key);
        final TestObjectWithAllTypes obj2 = randomObject(rnd, key);

        RecordView<TestObjectWithAllTypes> tbl = recordView();

        // Delete not existed key.
        assertNull(tbl.get(null, key));
        assertFalse(tbl.delete(null, key));

        // Insert a new row.
        tbl.upsert(null, obj);

        // Delete existed row.
        assertEquals(obj, tbl.get(null, key));
        assertTrue(tbl.delete(null, key));
        assertNull(tbl.get(null, key));

        // Delete already deleted row.
        assertFalse(tbl.delete(null, key));

        // Insert a new row.
        tbl.upsert(null, obj2);
        assertEquals(obj2, tbl.get(null, key));
    }

    @Test
    public void removeExact() {
        final TestObjectWithAllTypes key = key(rnd);
        final TestObjectWithAllTypes obj = randomObject(rnd, key);
        final TestObjectWithAllTypes obj2 = randomObject(rnd, key);

        RecordView<TestObjectWithAllTypes> tbl = recordView();

        // Insert a new row.
        tbl.upsert(null, obj);
        assertEquals(obj, tbl.get(null, key));

        // Fails to delete row with unexpected value.
        assertFalse(tbl.deleteExact(null, obj2));
        assertEquals(obj, tbl.get(null, key));

        // Delete row with expected value.
        assertTrue(tbl.deleteExact(null, obj));
        assertNull(tbl.get(null, key));

        // Try to remove non-existed key.
        assertFalse(tbl.deleteExact(null, obj));
        assertNull(tbl.get(null, key));

        // Insert a new row.
        tbl.upsert(null, obj2);
        assertEquals(obj2, tbl.get(null, key));

        // Delete row with expected value.
        assertTrue(tbl.delete(null, obj2));
        assertNull(tbl.get(null, key));

        assertFalse(tbl.delete(null, obj2));
        assertNull(tbl.get(null, obj2));
    }

    @Test
    public void replace() {
        final TestObjectWithAllTypes key = key(rnd);
        final TestObjectWithAllTypes obj = randomObject(rnd, key);
        final TestObjectWithAllTypes obj2 = randomObject(rnd, key);
        final TestObjectWithAllTypes obj3 = randomObject(rnd, key);

        RecordView<TestObjectWithAllTypes> tbl = recordView();

        // Ignore replace operation for non-existed row.
        assertFalse(tbl.replace(null, obj));
        assertNull(tbl.get(null, key));

        // Insert new row.
        tbl.upsert(null, obj);

        // Replace existed row.
        assertTrue(tbl.replace(null, obj2));
        assertEquals(obj2, tbl.get(null, key));

        // Replace existed row.
        assertTrue(tbl.replace(null, obj3));
        assertEquals(obj3, tbl.get(null, key));

        // Remove existed row.
        assertTrue(tbl.delete(null, key));
        assertNull(tbl.get(null, key));

        tbl.upsert(null, obj);
        assertEquals(obj, tbl.get(null, key));
    }

    @Test
    public void replaceExact() {
        final TestObjectWithAllTypes key = key(rnd);
        final TestObjectWithAllTypes obj = randomObject(rnd, key);
        final TestObjectWithAllTypes obj2 = randomObject(rnd, key);
        final TestObjectWithAllTypes obj3 = randomObject(rnd, key);
        final TestObjectWithAllTypes obj4 = randomObject(rnd, key);

        RecordView<TestObjectWithAllTypes> tbl = recordView();

        // Ignore replace operation for non-existed row.
        assertFalse(tbl.replace(null, obj, obj2));
        assertNull(tbl.get(null, key));

        // Insert new row.
        tbl.upsert(null, obj);

        // Ignore un-exepected row replacement.
        assertFalse(tbl.replace(null, obj2, obj3));
        assertEquals(obj, tbl.get(null, key));

        // Replace existed row.
        assertTrue(tbl.replace(null, obj, obj2));
        assertEquals(obj2, tbl.get(null, key));

        // Replace existed KV pair.
        assertTrue(tbl.replace(null, obj2, obj3));
        assertEquals(obj3, tbl.get(null, key));

        // Remove existed row.
        assertTrue(tbl.delete(null, key));
        assertNull(tbl.get(null, key));

        assertFalse(tbl.replace(null, key, obj4));
        assertNull(tbl.get(null, key));
    }

    @Test
    public void getAll() {
        final TestObjectWithAllTypes key1 = key(rnd);
        final TestObjectWithAllTypes key2 = key(rnd);
        final TestObjectWithAllTypes key3 = key(rnd);
        final TestObjectWithAllTypes val1 = randomObject(rnd, key1);
        final TestObjectWithAllTypes val3 = randomObject(rnd, key3);

        RecordView<TestObjectWithAllTypes> tbl = recordView();

        tbl.upsertAll(null, List.of(val1, val3));

        Collection<TestObjectWithAllTypes> res = tbl.getAll(null, List.of(key1, key2, key3));

        assertThat(res, Matchers.contains(val1, null, val3));
    }

    @Test
    public void contains() {
        final TestObjectWithAllTypes key = key(rnd);
        final TestObjectWithAllTypes wrongKey = key(rnd);
        final TestObjectWithAllTypes val = randomObject(rnd, key);
        final RecordView<TestObjectWithAllTypes> tbl = recordView();

        tbl.insert(null, val);

        assertTrue(tbl.contains(null, key));
        assertTrue(tbl.contains(null, val));
        assertFalse(tbl.contains(null, wrongKey));
        assertFalse(tbl.contains(null, randomObject(rnd, wrongKey)));
    }

    @Test
    void retriesOnInternalSchemaVersionMismatchException() throws Exception {
        RecordView<TestObjectWithAllTypes> view = recordView();

        TestObjectWithAllTypes expectedRecord = TestObjectWithAllTypes.randomObject(rnd);
        ReflectionMarshallersProvider marshallers = new ReflectionMarshallersProvider();

        BinaryRow resultRow = new RecordMarshallerImpl<>(schema, marshallers, recMapper)
                .marshal(expectedRecord);

        doReturn(failedFuture(new InternalSchemaVersionMismatchException()))
                .doReturn(completedFuture(resultRow))
                .when(internalTable).get(any(), any());

        TestObjectWithAllTypes result = view.get(null, new TestObjectWithAllTypes());

        assertThat(result, is(equalTo(expectedRecord)));

        verify(internalTable, times(2)).get(any(), isNull());
    }

    /**
     * Creates RecordView.
     */
    private RecordViewImpl<TestObjectWithAllTypes> recordView() {
        ClusterService clusterService = mock(ClusterService.class, RETURNS_DEEP_STUBS);
        when(clusterService.topologyService().localMember().address())
                .thenReturn(DummyInternalTableImpl.ADDR);

        when(clusterService.messagingService()).thenReturn(mock(MessagingService.class, RETURNS_DEEP_STUBS));

        // Validate all types are tested.
        Set<NativeTypeSpec> testedTypes = Arrays.stream(valCols).map(c -> c.type().spec())
                .collect(Collectors.toSet());
        Set<NativeTypeSpec> missedTypes = Arrays.stream(NativeTypeSpec.values())
                .filter(t -> !testedTypes.contains(t)).collect(Collectors.toSet());

        assertEquals(Collections.emptySet(), missedTypes);

        ReflectionMarshallersProvider marshallers = new ReflectionMarshallersProvider();

        return new RecordViewImpl<>(
                internalTable,
                new DummySchemaManagerImpl(schema),
                schemaVersions,
                mock(IgniteSql.class),
                marshallers,
                recMapper
        );
    }

    private TestObjectWithAllTypes randomObject(Random rnd, TestObjectWithAllTypes key) {
        TestObjectWithAllTypes obj = TestObjectWithAllTypes.randomObject(rnd);

        obj.setPrimitiveLongCol(key.getPrimitiveLongCol());

        return obj;
    }

    private static TestObjectWithAllTypes key(Random rnd) {
        TestObjectWithAllTypes key = new TestObjectWithAllTypes();

        key.setPrimitiveLongCol(rnd.nextLong());

        return key;
    }
}
