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

import static java.util.stream.Collectors.toList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Comparator;
import java.util.List;
import java.util.UUID;
import java.util.stream.StreamSupport;
import org.apache.ignite.internal.schema.BinaryRow;
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

        IgniteRowId rowId = UuidIgniteRowId.randomRowId();

        // Read.
        assertNull(pk.read(rowId, null));
        assertNull(pk.read(rowId, Timestamp.nextVersion()));

        // Scan.
        assertEquals(List.of(), convert(pk.scan(null)));
        assertEquals(List.of(), convert(pk.scan(Timestamp.nextVersion())));
    }

    /**
     * Tests basic invariants of {@link MvPartitionStorage#addWrite(BinaryRow, UUID)}.
     */
    @Test
    public void testAddWrite() throws Exception {
        MvPartitionStorage pk = partitionStorage();

        TestKey key = new TestKey(10, "foo");
        TestValue value = new TestValue(20, "bar");
        IgniteRowId rowId = UuidIgniteRowId.randomRowId();

        BinaryRow binaryRow = binaryRow(key, value);

        pk.addWrite(rowId, binaryRow, UUID.randomUUID());

        // Attempt to write from another transaction.
        assertThrows(TxIdMismatchException.class, () -> pk.addWrite(rowId, binaryRow, UUID.randomUUID()));

        // Read without timestamp returns uncommited row.
        assertEquals(value, value(pk.read(rowId, null)));

        // Read with timestamp returns null.
        assertNull(pk.read(rowId, Timestamp.nextVersion()));
    }

    /**
     * Tests basic invariants of {@link MvPartitionStorage#abortWrite(BinaryRow)}.
     */
    @Test
    public void testAbortWrite() throws Exception {
        MvPartitionStorage pk = partitionStorage();

        TestKey key = new TestKey(10, "foo");
        TestValue value = new TestValue(20, "bar");
        IgniteRowId rowId = UuidIgniteRowId.randomRowId();

        pk.addWrite(rowId, binaryRow(key, value), UUID.randomUUID());

        pk.abortWrite(rowId);

        // Aborted row can't be read.
        assertNull(pk.read(rowId, null));
    }

    /**
     * Tests basic invariants of {@link MvPartitionStorage#commitWrite(BinaryRow, Timestamp)}.
     */
    @Test
    public void testCommitWrite() throws Exception {
        MvPartitionStorage pk = partitionStorage();

        TestKey key = new TestKey(10, "foo");
        TestValue value = new TestValue(20, "bar");
        IgniteRowId rowId = UuidIgniteRowId.randomRowId();

        BinaryRow binaryRow = binaryRow(key, value);

        pk.addWrite(rowId, binaryRow, UUID.randomUUID());

        Timestamp tsBefore = Timestamp.nextVersion();

        Timestamp tsExact = Timestamp.nextVersion();
        pk.commitWrite(rowId, tsExact);

        Timestamp tsAfter = Timestamp.nextVersion();

        // Row is invisible at the time before writing.
        assertNull(pk.read(rowId, tsBefore));

        // Row is valid at the time during and after writing.
        assertEquals(value, value(pk.read(rowId, null)));
        assertEquals(value, value(pk.read(rowId, tsExact)));
        assertEquals(value, value(pk.read(rowId, tsAfter)));

        TestValue newValue = new TestValue(30, "duh");

        pk.addWrite(rowId, binaryRow(key, newValue), UUID.randomUUID());

        // Same checks, but now there are two different versions.
        assertNull(pk.read(rowId, tsBefore));

        assertEquals(newValue, value(pk.read(rowId, null)));

        assertEquals(value, value(pk.read(rowId, tsExact)));
        assertEquals(value, value(pk.read(rowId, tsAfter)));
        assertEquals(value, value(pk.read(rowId, Timestamp.nextVersion())));

        // Only latest time behavior changes after commit.
        pk.commitWrite(rowId, Timestamp.nextVersion());

        assertEquals(newValue, value(pk.read(rowId, null)));

        assertEquals(value, value(pk.read(rowId, tsExact)));
        assertEquals(value, value(pk.read(rowId, tsAfter)));

        assertEquals(newValue, value(pk.read(rowId, Timestamp.nextVersion())));

        // Remove.
        pk.addWrite(rowId, null, UUID.randomUUID());

        assertNull(pk.read(rowId, tsBefore));

        assertNull(pk.read(rowId, null));

        assertEquals(value, value(pk.read(rowId, tsExact)));
        assertEquals(value, value(pk.read(rowId, tsAfter)));

        assertEquals(newValue, value(pk.read(rowId, Timestamp.nextVersion())));

        // Commit remove.
        Timestamp removeTs = Timestamp.nextVersion();
        pk.commitWrite(rowId, removeTs);

        assertNull(pk.read(rowId, tsBefore));

        assertNull(pk.read(rowId, null));
        assertNull(pk.read(rowId, removeTs));
        assertNull(pk.read(rowId, Timestamp.nextVersion()));

        assertEquals(value, value(pk.read(rowId, tsExact)));
        assertEquals(value, value(pk.read(rowId, tsAfter)));
    }

    /**
     * Tests basic invariants of {@link MvPartitionStorage#scan(Timestamp)}.
     */
    @Test
    public void testScan() throws Exception {
        MvPartitionStorage pk = partitionStorage();

        TestKey key1 = new TestKey(1, "1");
        TestValue value1 = new TestValue(10, "xxx");
        IgniteRowId rowId1 = UuidIgniteRowId.randomRowId();

        TestKey key2 = new TestKey(2, "2");
        TestValue value2 = new TestValue(20, "yyy");
        IgniteRowId rowId2 = UuidIgniteRowId.randomRowId();

        pk.addWrite(rowId1, binaryRow(key1, value1), UUID.randomUUID());
        pk.addWrite(rowId2, binaryRow(key2, value2), UUID.randomUUID());

        // Scan.
        assertEquals(List.of(value1, value2), convert(pk.scan(null)));

        Timestamp ts1 = Timestamp.nextVersion();

        Timestamp ts2 = Timestamp.nextVersion();
        pk.commitWrite(rowId1, ts2);

        Timestamp ts3 = Timestamp.nextVersion();

        Timestamp ts4 = Timestamp.nextVersion();
        pk.commitWrite(rowId2, ts4);

        Timestamp ts5 = Timestamp.nextVersion();

        // Full scan with various timestamp values.
        assertEquals(List.of(), convert(pk.scan(ts1)));

        assertEquals(List.of(value1), convert(pk.scan(ts2)));
        assertEquals(List.of(value1), convert(pk.scan(ts3)));

        assertEquals(List.of(value1, value2), convert(pk.scan(ts4)));
        assertEquals(List.of(value1, value2), convert(pk.scan(ts5)));
    }

    private List<TestValue> convert(Cursor<BinaryRow> cursor) throws Exception {
        try (cursor) {
            return StreamSupport.stream(cursor.spliterator(), false)
                    .map(BaseMvStoragesTest::value)
                    .sorted(Comparator.nullsFirst(Comparator.naturalOrder()))
                    .collect(toList());
        }
    }
}
