/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
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

package org.apache.ignite.internal.storage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.UUID;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.NativeTypes;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.marshaller.KvMarshaller;
import org.apache.ignite.internal.schema.marshaller.MarshallerException;
import org.apache.ignite.internal.schema.marshaller.MarshallerFactory;
import org.apache.ignite.internal.schema.marshaller.reflection.ReflectionMarshallerFactory;
import org.apache.ignite.internal.schema.row.Row;
import org.apache.ignite.internal.storage.MvStorage.TxIdMismatchException;
import org.apache.ignite.internal.tostring.S;
import org.apache.ignite.internal.tx.Timestamp;
import org.apache.ignite.internal.util.Cursor;
import org.apache.ignite.lang.IgniteException;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Base test for MV partition storages.
 */
public abstract class AbstractMvPartitionStorageTest {
    /** Default reflection marshaller factory. */
    protected static MarshallerFactory marshallerFactory;

    /** Schema descriptor for {@link TestPojo}. */
    protected static SchemaDescriptor schemaDescriptor;

    /** Key-value marshaller for {@link TestPojo}. */
    protected static KvMarshaller<TestPojo, TestPojo> kvMarshaller;

    @BeforeAll
    static void beforeAll() {
        marshallerFactory = new ReflectionMarshallerFactory();

        schemaDescriptor = new SchemaDescriptor(1, new Column[]{
                new Column("intKey".toUpperCase(Locale.ROOT), NativeTypes.INT32, false),
                new Column("strKey".toUpperCase(Locale.ROOT), NativeTypes.STRING, false),
        }, new Column[]{
                new Column("intKey".toUpperCase(Locale.ROOT), NativeTypes.INT32, false),
                new Column("strKey".toUpperCase(Locale.ROOT), NativeTypes.STRING, false),
                new Column("intVal".toUpperCase(Locale.ROOT), NativeTypes.INT32, false),
                new Column("strVal".toUpperCase(Locale.ROOT), NativeTypes.STRING, false),
        });

        kvMarshaller = marshallerFactory.create(schemaDescriptor, TestPojo.class, TestPojo.class);
    }

    @AfterAll
    static void afterAll() {
        kvMarshaller = null;
        schemaDescriptor = null;
        marshallerFactory = null;
    }

    /**
     * Creates a storage instance for testing.
     */
    protected abstract MvStorage partitionStorage();

    /**
     * Tests that reads and scan from empty storage return empty results.
     */
    @Test
    public void testEmpty() throws Exception {
        MvStorage pk = partitionStorage();

        BinaryRow binaryKey = kvMarshaller.marshal(new TestPojo(10, "foo"));

        // Read.
        assertNull(pk.read(binaryKey, null));
        assertNull(pk.read(binaryKey, Timestamp.nextVersion()));

        // Scan.
        assertEquals(List.of(), convert(pk.scan(row -> true, null)));
        assertEquals(List.of(), convert(pk.scan(row -> true, Timestamp.nextVersion())));
    }

    /**
     * Tests basic invariants of {@link MvStorage#addWrite(BinaryRow, UUID)}.
     */
    @Test
    public void testAddWrite() throws Exception {
        MvStorage pk = partitionStorage();

        TestPojo row = new TestPojo(10, "foo", 20, "bar");

        BinaryRow binaryRow = kvMarshaller.marshal(row, row);

        pk.addWrite(binaryRow, UUID.randomUUID());

        // Attempt to write from another transaction.
        assertThrows(TxIdMismatchException.class, () -> pk.addWrite(binaryRow, UUID.randomUUID()));

        // Read without timestamp returns uncommited row.
        assertEquals(row, row(pk.read(binaryRow, null)));

        // Read with timestamp returns null.
        assertNull(pk.read(binaryRow, Timestamp.nextVersion()));
    }

    /**
     * Tests basic invariants of {@link MvStorage#abortWrite(BinaryRow)}.
     */
    @Test
    public void testAbortWrite() throws Exception {
        MvStorage pk = partitionStorage();

        TestPojo row = new TestPojo(10, "foo", 20, "bar");

        BinaryRow binaryRow = kvMarshaller.marshal(row, row);

        pk.addWrite(binaryRow, UUID.randomUUID());

        pk.abortWrite(binaryRow);

        assertNull(pk.read(binaryRow, null));
    }

    /**
     * Tests basic invariants of {@link MvStorage#commitWrite(BinaryRow, Timestamp)}.
     */
    @Test
    public void testCommitWrite() throws Exception {
        MvStorage pk = partitionStorage();

        TestPojo row = new TestPojo(10, "foo", 20, "bar");

        BinaryRow binaryRow = kvMarshaller.marshal(row, row);

        pk.addWrite(binaryRow, UUID.randomUUID());

        Timestamp tsBefore = Timestamp.nextVersion();

        Timestamp tsExact = Timestamp.nextVersion();
        pk.commitWrite(binaryRow, tsExact);

        Timestamp tsAfter = Timestamp.nextVersion();

        assertNull(pk.read(binaryRow, tsBefore));

        assertEquals(row, row(pk.read(binaryRow, null)));
        assertEquals(row, row(pk.read(binaryRow, tsExact)));
        assertEquals(row, row(pk.read(binaryRow, tsAfter)));

        TestPojo newRow = new TestPojo(10, "foo", 30, "duh");

        pk.addWrite(kvMarshaller.marshal(newRow, newRow), UUID.randomUUID());

        assertNull(pk.read(binaryRow, tsBefore));

        assertEquals(newRow, row(pk.read(binaryRow, null)));

        assertEquals(row, row(pk.read(binaryRow, tsExact)));
        assertEquals(row, row(pk.read(binaryRow, tsAfter)));
        assertEquals(row, row(pk.read(binaryRow, Timestamp.nextVersion())));
    }

    /**
     * Tests basic invariants of {@link MvStorage#scan(Predicate, Timestamp)}.
     */
    @Test
    public void testScan() throws Exception {
        MvStorage pk = partitionStorage();

        TestPojo row1 = new TestPojo(1, "1", 10, "xxx");
        BinaryRow binaryRow1 = kvMarshaller.marshal(row1, row1);

        TestPojo row2 = new TestPojo(2, "2", 20, "yyy");
        BinaryRow binaryRow2 = kvMarshaller.marshal(row2, row2);

        pk.addWrite(binaryRow1, UUID.randomUUID());
        pk.addWrite(binaryRow2, UUID.randomUUID());

        assertEquals(List.of(row1, row2), convert(pk.scan(row -> true, null)));
        assertEquals(List.of(row1), convert(pk.scan(row -> row(row).intKey == 1, null)));
        assertEquals(List.of(row2), convert(pk.scan(row -> row(row).intKey == 2, null)));

        Timestamp ts1 = Timestamp.nextVersion();

        Timestamp ts2 = Timestamp.nextVersion();
        pk.commitWrite(binaryRow1, ts2);

        Timestamp ts3 = Timestamp.nextVersion();

        Timestamp ts4 = Timestamp.nextVersion();
        pk.commitWrite(binaryRow2, ts4);

        Timestamp ts5 = Timestamp.nextVersion();

        assertEquals(List.of(), convert(pk.scan(row -> true, ts1)));

        assertEquals(List.of(row1), convert(pk.scan(row -> true, ts2)));
        assertEquals(List.of(row1), convert(pk.scan(row -> true, ts3)));

        assertEquals(List.of(row1, row2), convert(pk.scan(row -> true, ts4)));
        assertEquals(List.of(row1, row2), convert(pk.scan(row -> true, ts5)));
    }

    @Nullable
    protected TestPojo row(BinaryRow binaryRow) {
        try {
            return kvMarshaller.unmarshalValue(new Row(schemaDescriptor, binaryRow));
        } catch (MarshallerException e) {
            throw new IgniteException(e);
        }
    }

    /**
     * Test pojo.
     */
    protected static class TestPojo implements Comparable<TestPojo> {
        public int intKey;

        public String strKey;

        public Integer intVal;

        public String strVal;

        public TestPojo() {
        }

        public TestPojo(int intKey, String strKey) {
            this.intKey = intKey;
            this.strKey = strKey;
        }

        public TestPojo(int intKey, String strKey, Integer intVal, String strVal) {
            this.intKey = intKey;
            this.strKey = strKey;
            this.intVal = intVal;
            this.strVal = strVal;
        }

        /** {@inheritDoc} */
        @Override
        public int compareTo(TestPojo o) {
            int cmp = Integer.compare(intKey, o.intKey);

            return cmp == 0 ? strKey.compareTo(o.strKey) : cmp;
        }

        /** {@inheritDoc} */
        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            TestPojo testPojo = (TestPojo) o;
            return intKey == testPojo.intKey && Objects.equals(strKey, testPojo.strKey) && Objects.equals(intVal,
                    testPojo.intVal) && Objects.equals(strVal, testPojo.strVal);
        }

        /** {@inheritDoc} */
        @Override
        public int hashCode() {
            return Objects.hash(intKey, strKey, intVal, strVal);
        }

        /** {@inheritDoc} */
        @Override
        public String toString() {
            return S.toString(TestPojo.class, this);
        }
    }

    private List<TestPojo> convert(Cursor<BinaryRow> cursor) throws Exception {
        try (cursor) {
            List<TestPojo> list = StreamSupport.stream(cursor.spliterator(), false)
                    .map(this::row)
                    .collect(Collectors.toList());

            Collections.sort(list);

            return list;
        }
    }
}
