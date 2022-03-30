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
import java.util.UUID;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.storage.MvPartitionStorage.TxIdMismatchException;
import org.apache.ignite.internal.tx.Timestamp;
import org.apache.ignite.internal.util.Cursor;
import org.junit.jupiter.api.Test;

/**
 * Base test for MV partition storages.
 */
public abstract class AbstractMvPartitionStorageTest extends BaseMvStoragesTest {
    /**
     * Creates a storage instance for testing.
     */
    protected abstract MvPartitionStorage partitionStorage();

    /**
     * Tests that reads and scan from empty storage return empty results.
     */
    @Test
    public void testEmpty() throws Exception {
        MvPartitionStorage pk = partitionStorage();

        BinaryRow binaryKey = binaryKey(new TestKey(10, "foo"));

        // Read.
        assertNull(pk.read(binaryKey, null));
        assertNull(pk.read(binaryKey, Timestamp.nextVersion()));

        // Scan.
        assertEquals(List.of(), convert(pk.scan(row -> true, null)));
        assertEquals(List.of(), convert(pk.scan(row -> true, Timestamp.nextVersion())));
    }

    /**
     * Tests basic invariants of {@link MvPartitionStorage#addWrite(BinaryRow, UUID)}.
     */
    @Test
    public void testAddWrite() throws Exception {
        MvPartitionStorage pk = partitionStorage();

        TestKey key = new TestKey(10, "foo");
        TestValue value = new TestValue(20, "bar");

        BinaryRow binaryRow = binaryRow(key, value);

        pk.addWrite(binaryRow, UUID.randomUUID());

        // Attempt to write from another transaction.
        assertThrows(TxIdMismatchException.class, () -> pk.addWrite(binaryRow, UUID.randomUUID()));

        // Read without timestamp returns uncommited row.
        assertEquals(value, value(pk.read(binaryKey(key), null)));

        // Read with timestamp returns null.
        assertNull(pk.read(binaryKey(key), Timestamp.nextVersion()));
    }

    /**
     * Tests basic invariants of {@link MvPartitionStorage#abortWrite(BinaryRow)}.
     */
    @Test
    public void testAbortWrite() throws Exception {
        MvPartitionStorage pk = partitionStorage();

        TestKey key = new TestKey(10, "foo");
        TestValue value = new TestValue(20, "bar");

        pk.addWrite(binaryRow(key, value), UUID.randomUUID());

        pk.abortWrite(binaryKey(key));

        // Aborted row can't be read.
        assertNull(pk.read(binaryKey(key), null));
    }

    /**
     * Tests basic invariants of {@link MvPartitionStorage#commitWrite(BinaryRow, Timestamp)}.
     */
    @Test
    public void testCommitWrite() throws Exception {
        MvPartitionStorage pk = partitionStorage();

        TestKey key = new TestKey(10, "foo");
        TestValue value = new TestValue(20, "bar");

        BinaryRow binaryRow = binaryRow(key, value);

        pk.addWrite(binaryRow, UUID.randomUUID());

        Timestamp tsBefore = Timestamp.nextVersion();

        Timestamp tsExact = Timestamp.nextVersion();
        pk.commitWrite(binaryRow, tsExact);

        Timestamp tsAfter = Timestamp.nextVersion();

        // Row is invisible at the time before writing.
        assertNull(pk.read(binaryRow, tsBefore));

        // Row is valid at the time during and after writing.
        assertEquals(value, value(pk.read(binaryRow, null)));
        assertEquals(value, value(pk.read(binaryRow, tsExact)));
        assertEquals(value, value(pk.read(binaryRow, tsAfter)));

        TestValue newValue = new TestValue(30, "duh");

        pk.addWrite(binaryRow(key, newValue), UUID.randomUUID());

        // Same checks, but now there are two different versions.
        assertNull(pk.read(binaryRow, tsBefore));

        assertEquals(newValue, value(pk.read(binaryRow, null)));

        assertEquals(value, value(pk.read(binaryRow, tsExact)));
        assertEquals(value, value(pk.read(binaryRow, tsAfter)));
        assertEquals(value, value(pk.read(binaryRow, Timestamp.nextVersion())));

        pk.commitWrite(binaryKey(key), Timestamp.nextVersion());

        assertEquals(newValue, value(pk.read(binaryRow, null)));
        assertEquals(newValue, value(pk.read(binaryRow, Timestamp.nextVersion())));
    }

    /**
     * Tests basic invariants of {@link MvPartitionStorage#scan(Predicate, Timestamp)}.
     */
    @Test
    public void testScan() throws Exception {
        MvPartitionStorage pk = partitionStorage();

        TestKey key1 = new TestKey(1, "1");
        TestValue value1 = new TestValue(10, "xxx");

        TestKey key2 = new TestKey(2, "2");
        TestValue value2 = new TestValue(20, "yyy");

        pk.addWrite(binaryRow(key1, value1), UUID.randomUUID());
        pk.addWrite(binaryRow(key2, value2), UUID.randomUUID());

        // Scan with and without filters.
        assertEquals(List.of(value1, value2), convert(pk.scan(row -> true, null)));
        assertEquals(List.of(value1), convert(pk.scan(row -> key(row).intKey == 1, null)));
        assertEquals(List.of(value2), convert(pk.scan(row -> key(row).intKey == 2, null)));

        Timestamp ts1 = Timestamp.nextVersion();

        Timestamp ts2 = Timestamp.nextVersion();
        pk.commitWrite(binaryKey(key1), ts2);

        Timestamp ts3 = Timestamp.nextVersion();

        Timestamp ts4 = Timestamp.nextVersion();
        pk.commitWrite(binaryKey(key2), ts4);

        Timestamp ts5 = Timestamp.nextVersion();

        // Full scan with various timestamp values.
        assertEquals(List.of(), convert(pk.scan(row -> true, ts1)));

        assertEquals(List.of(value1), convert(pk.scan(row -> true, ts2)));
        assertEquals(List.of(value1), convert(pk.scan(row -> true, ts3)));

        assertEquals(List.of(value1, value2), convert(pk.scan(row -> true, ts4)));
        assertEquals(List.of(value1, value2), convert(pk.scan(row -> true, ts5)));
    }

    private List<TestValue> convert(Cursor<BinaryRow> cursor) throws Exception {
        try (cursor) {
            List<TestValue> list = StreamSupport.stream(cursor.spliterator(), false)
                    .map(this::value)
                    .collect(Collectors.toList());

            Collections.sort(list);

            return list;
        }
    }
}
