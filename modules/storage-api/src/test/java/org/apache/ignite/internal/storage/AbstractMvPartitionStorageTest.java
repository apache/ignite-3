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
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasItems;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.NoSuchElementException;
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

        RowId rowId = pk.insert(binaryRow(new TestKey(1, "1"), new TestValue(1, "1")), UUID.randomUUID());

        pk.abortWrite(rowId);

        assertEquals(0, rowId.partitionId());

        // Read.
        assertNull(pk.read(rowId, UUID.randomUUID()));
        assertNull(pk.read(rowId, Timestamp.nextVersion()));

        // Scan.
        assertEquals(List.of(), convert(pk.scan(row -> true, UUID.randomUUID())));
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

        UUID txId = UUID.randomUUID();

        RowId rowId = pk.insert(binaryRow, txId);

        // Attempt to write from another transaction.
        assertThrows(TxIdMismatchException.class, () -> pk.addWrite(rowId, binaryRow, UUID.randomUUID()));

        // Write from the same transaction.
        pk.addWrite(rowId, binaryRow, txId);

        // Read without timestamp returns uncommited row.
        assertEquals(value, value(pk.read(rowId, txId)));

        // Read with wrong transaction id should throw exception.
        assertThrows(TxIdMismatchException.class, () -> pk.read(rowId, UUID.randomUUID()));

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

        UUID txId = UUID.randomUUID();

        RowId rowId = pk.insert(binaryRow(key, value), txId);

        pk.abortWrite(rowId);

        // Aborted row can't be read.
        assertNull(pk.read(rowId, txId));
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

        UUID txId = UUID.randomUUID();

        RowId rowId = pk.insert(binaryRow, txId);

        Timestamp tsBefore = Timestamp.nextVersion();

        Timestamp tsExact = Timestamp.nextVersion();
        pk.commitWrite(rowId, tsExact);

        Timestamp tsAfter = Timestamp.nextVersion();

        // Row is invisible at the time before writing.
        assertNull(pk.read(rowId, tsBefore));

        // Row is valid at the time during and after writing.
        assertEquals(value, value(pk.read(rowId, txId)));
        assertEquals(value, value(pk.read(rowId, tsExact)));
        assertEquals(value, value(pk.read(rowId, tsAfter)));

        TestValue newValue = new TestValue(30, "duh");

        UUID newTxId = UUID.randomUUID();

        pk.addWrite(rowId, binaryRow(key, newValue), newTxId);

        // Same checks, but now there are two different versions.
        assertNull(pk.read(rowId, tsBefore));

        assertEquals(newValue, value(pk.read(rowId, newTxId)));

        assertEquals(value, value(pk.read(rowId, tsExact)));
        assertEquals(value, value(pk.read(rowId, tsAfter)));
        assertEquals(value, value(pk.read(rowId, Timestamp.nextVersion())));

        // Only latest time behavior changes after commit.
        pk.commitWrite(rowId, Timestamp.nextVersion());

        assertEquals(newValue, value(pk.read(rowId, newTxId)));

        assertEquals(value, value(pk.read(rowId, tsExact)));
        assertEquals(value, value(pk.read(rowId, tsAfter)));

        assertEquals(newValue, value(pk.read(rowId, Timestamp.nextVersion())));

        // Remove.
        UUID removeTxId = UUID.randomUUID();

        pk.addWrite(rowId, null, removeTxId);

        assertNull(pk.read(rowId, tsBefore));

        assertNull(pk.read(rowId, removeTxId));

        assertEquals(value, value(pk.read(rowId, tsExact)));
        assertEquals(value, value(pk.read(rowId, tsAfter)));

        assertEquals(newValue, value(pk.read(rowId, Timestamp.nextVersion())));

        // Commit remove.
        Timestamp removeTs = Timestamp.nextVersion();
        pk.commitWrite(rowId, removeTs);

        assertNull(pk.read(rowId, tsBefore));

        assertNull(pk.read(rowId, removeTxId));
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

        TestKey key2 = new TestKey(2, "2");
        TestValue value2 = new TestValue(20, "yyy");

        UUID txId1 = UUID.randomUUID();
        RowId rowId1 = pk.insert(binaryRow(key1, value1), txId1);

        UUID txId2 = UUID.randomUUID();
        RowId rowId2 = pk.insert(binaryRow(key2, value2), txId2);

        // Scan with and without filters.
        assertThrows(TxIdMismatchException.class, () -> convert(pk.scan(row -> true, txId1)));
        assertThrows(TxIdMismatchException.class, () -> convert(pk.scan(row -> true, txId2)));

        assertEquals(List.of(value1), convert(pk.scan(row -> key(row).intKey == 1, txId1)));
        assertEquals(List.of(value2), convert(pk.scan(row -> key(row).intKey == 2, txId2)));

        Timestamp ts1 = Timestamp.nextVersion();

        Timestamp ts2 = Timestamp.nextVersion();
        pk.commitWrite(rowId1, ts2);

        Timestamp ts3 = Timestamp.nextVersion();

        Timestamp ts4 = Timestamp.nextVersion();
        pk.commitWrite(rowId2, ts4);

        Timestamp ts5 = Timestamp.nextVersion();

        // Full scan with various timestamp values.
        assertEquals(List.of(), convert(pk.scan(row -> true, ts1)));

        assertEquals(List.of(value1), convert(pk.scan(row -> true, ts2)));
        assertEquals(List.of(value1), convert(pk.scan(row -> true, ts3)));

        assertEquals(List.of(value1, value2), convert(pk.scan(row -> true, ts4)));
        assertEquals(List.of(value1, value2), convert(pk.scan(row -> true, ts5)));
    }

    @Test
    public void testScanCursorInvariants() {
        MvPartitionStorage pk = partitionStorage();

        TestValue value1 = new TestValue(10, "xxx");

        TestValue value2 = new TestValue(20, "yyy");

        UUID txId = UUID.randomUUID();

        RowId rowId1 = pk.insert(binaryRow(new TestKey(1, "1"), value1), txId);
        pk.commitWrite(rowId1, Timestamp.nextVersion());

        RowId rowId2 = pk.insert(binaryRow(new TestKey(2, "2"), value2), txId);
        pk.commitWrite(rowId2, Timestamp.nextVersion());

        Cursor<BinaryRow> cursor = pk.scan(row -> true, txId);

        assertTrue(cursor.hasNext());
        assertTrue(cursor.hasNext());

        List<TestValue> res = new ArrayList<>();

        res.add(value(cursor.next()));

        assertTrue(cursor.hasNext());
        assertTrue(cursor.hasNext());

        res.add(value(cursor.next()));

        assertFalse(cursor.hasNext());
        assertFalse(cursor.hasNext());

        assertThrows(NoSuchElementException.class, () -> cursor.next());

        assertThat(res, hasItems(value1, value2));
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
