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
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
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
import java.util.function.Predicate;
import java.util.stream.StreamSupport;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.tx.Timestamp;
import org.apache.ignite.internal.util.Cursor;
import org.junit.jupiter.api.Test;

/**
 * Base test for MV partition storages.
 */
public abstract class AbstractMvPartitionStorageTest<S extends MvPartitionStorage> extends BaseMvStoragesTest {
    protected S storage;

    protected final UUID txId = newTransactionId();

    private final TestKey key = new TestKey(10, "foo");
    private final TestValue value = new TestValue(20, "bar");
    protected final BinaryRow binaryRow = binaryRow(key, value);
    private final TestValue value2 = new TestValue(21, "bar2");
    protected final BinaryRow binaryRow2 = binaryRow(key, value2);

    /**
     * Tests that reads from empty storage return empty results.
     */
    @Test
    public void testReadsFromEmpty() {
        RowId rowId = freshRowId(partitionId());
        assertEquals(partitionId(), rowId.partitionId());

        assertNull(storage.read(rowId, newTransactionId()));
        assertNull(storage.read(rowId, Timestamp.nextVersion()));
    }

    @Test
    public void testScanOverEmpty() throws Exception {
        assertEquals(List.of(), convert(storage.scan(row -> true, newTransactionId())));
        assertEquals(List.of(), convert(storage.scan(row -> true, Timestamp.nextVersion())));
    }

    protected int partitionId() {
        return 0;
    }

    protected UUID newTransactionId() {
        return UUID.randomUUID();
    }

    /**
     * Tests basic invariants of {@link MvPartitionStorage#addWrite(RowId, BinaryRow, UUID)}.
     */
    @Test
    public void testAddWrite() {
        RowId rowId = storage.insert(binaryRow, txId);

        // Attempt to write from another transaction.
        assertThrows(TxIdMismatchException.class, () -> storage.addWrite(rowId, binaryRow, newTransactionId()));

        // Write from the same transaction.
        storage.addWrite(rowId, binaryRow, txId);

        // Read without timestamp returns uncommited row.
        assertEquals(value, value(storage.read(rowId, txId)));

        // Read with wrong transaction id should throw exception.
        assertThrows(TxIdMismatchException.class, () -> storage.read(rowId, newTransactionId()));

        // Read with timestamp returns null.
        assertNull(storage.read(rowId, Timestamp.nextVersion()));
    }

    /**
     * Tests basic invariants of {@link MvPartitionStorage#abortWrite(RowId)}.
     */
    @Test
    public void testAbortWrite() {
        RowId rowId = storage.insert(binaryRow(key, value), txId);

        storage.abortWrite(rowId);

        // Aborted row can't be read.
        assertNull(storage.read(rowId, txId));
    }

    /**
     * Tests basic invariants of {@link MvPartitionStorage#commitWrite(RowId, Timestamp)}.
     */
    @Test
    public void testCommitWrite() {
        RowId rowId = storage.insert(binaryRow, txId);

        Timestamp tsBefore = Timestamp.nextVersion();

        Timestamp tsExact = Timestamp.nextVersion();
        storage.commitWrite(rowId, tsExact);

        Timestamp tsAfter = Timestamp.nextVersion();

        // Row is invisible at the time before writing.
        assertNull(storage.read(rowId, tsBefore));

        // Row is valid at the time during and after writing.
        assertEquals(value, value(storage.read(rowId, txId)));
        assertEquals(value, value(storage.read(rowId, tsExact)));
        assertEquals(value, value(storage.read(rowId, tsAfter)));

        TestValue newValue = new TestValue(30, "duh");

        UUID newTxId = newTransactionId();

        storage.addWrite(rowId, binaryRow(key, newValue), newTxId);

        // Same checks, but now there are two different versions.
        assertNull(storage.read(rowId, tsBefore));

        assertEquals(newValue, value(storage.read(rowId, newTxId)));

        assertEquals(value, value(storage.read(rowId, tsExact)));
        assertEquals(value, value(storage.read(rowId, tsAfter)));
        assertEquals(value, value(storage.read(rowId, Timestamp.nextVersion())));

        // Only latest time behavior changes after commit.
        storage.commitWrite(rowId, Timestamp.nextVersion());

        assertEquals(newValue, value(storage.read(rowId, newTxId)));

        assertEquals(value, value(storage.read(rowId, tsExact)));
        assertEquals(value, value(storage.read(rowId, tsAfter)));

        assertEquals(newValue, value(storage.read(rowId, Timestamp.nextVersion())));

        // Remove.
        UUID removeTxId = newTransactionId();

        storage.addWrite(rowId, null, removeTxId);

        assertNull(storage.read(rowId, tsBefore));

        assertNull(storage.read(rowId, removeTxId));

        assertEquals(value, value(storage.read(rowId, tsExact)));
        assertEquals(value, value(storage.read(rowId, tsAfter)));

        assertEquals(newValue, value(storage.read(rowId, Timestamp.nextVersion())));

        // Commit remove.
        Timestamp removeTs = Timestamp.nextVersion();
        storage.commitWrite(rowId, removeTs);

        assertNull(storage.read(rowId, tsBefore));

        assertNull(storage.read(rowId, removeTxId));
        assertNull(storage.read(rowId, removeTs));
        assertNull(storage.read(rowId, Timestamp.nextVersion()));

        assertEquals(value, value(storage.read(rowId, tsExact)));
        assertEquals(value, value(storage.read(rowId, tsAfter)));
    }

    /**
     * Tests basic invariants of {@link MvPartitionStorage#scan(Predicate, Timestamp)}.
     */
    @Test
    public void testScan() throws Exception {
        TestKey key1 = new TestKey(1, "1");
        TestValue value1 = new TestValue(10, "xxx");

        TestKey key2 = new TestKey(2, "2");
        TestValue value2 = new TestValue(20, "yyy");

        UUID txId1 = newTransactionId();
        RowId rowId1 = storage.insert(binaryRow(key1, value1), txId1);

        UUID txId2 = newTransactionId();
        RowId rowId2 = storage.insert(binaryRow(key2, value2), txId2);

        // Scan with and without filters.
        assertThrows(TxIdMismatchException.class, () -> convert(storage.scan(row -> true, txId1)));
        assertThrows(TxIdMismatchException.class, () -> convert(storage.scan(row -> true, txId2)));

        assertEquals(List.of(value1), convert(storage.scan(row -> key(row).intKey == 1, txId1)));
        assertEquals(List.of(value2), convert(storage.scan(row -> key(row).intKey == 2, txId2)));

        Timestamp ts1 = Timestamp.nextVersion();

        Timestamp ts2 = Timestamp.nextVersion();
        storage.commitWrite(rowId1, ts2);

        Timestamp ts3 = Timestamp.nextVersion();

        Timestamp ts4 = Timestamp.nextVersion();
        storage.commitWrite(rowId2, ts4);

        Timestamp ts5 = Timestamp.nextVersion();

        // Full scan with various timestamp values.
        assertEquals(List.of(), convert(storage.scan(row -> true, ts1)));

        assertEquals(List.of(value1), convert(storage.scan(row -> true, ts2)));
        assertEquals(List.of(value1), convert(storage.scan(row -> true, ts3)));

        assertEquals(List.of(value1, value2), convert(storage.scan(row -> true, ts4)));
        assertEquals(List.of(value1, value2), convert(storage.scan(row -> true, ts5)));
    }

    @Test
    public void testScanCursorInvariants() {
        TestValue value1 = new TestValue(10, "xxx");

        TestValue value2 = new TestValue(20, "yyy");

        RowId rowId1 = storage.insert(binaryRow(new TestKey(1, "1"), value1), txId);
        storage.commitWrite(rowId1, Timestamp.nextVersion());

        RowId rowId2 = storage.insert(binaryRow(new TestKey(2, "2"), value2), txId);
        storage.commitWrite(rowId2, Timestamp.nextVersion());

        Cursor<BinaryRow> cursor = storage.scan(row -> true, txId);

        assertTrue(cursor.hasNext());
        //noinspection ConstantConditions
        assertTrue(cursor.hasNext());

        List<TestValue> res = new ArrayList<>();

        res.add(value(cursor.next()));

        assertTrue(cursor.hasNext());
        //noinspection ConstantConditions
        assertTrue(cursor.hasNext());

        res.add(value(cursor.next()));

        assertFalse(cursor.hasNext());
        //noinspection ConstantConditions
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

    @Test
    void readOfUncommittedRowWithCorrespondingTransactionIdReturnsTheRow() {
        RowId rowId = storage.insert(binaryRow, txId);

        BinaryRow foundRow = storage.read(rowId, txId);

        assertThat(foundRow, is(notNullValue()));
        assertThat(foundRow.bytes(), is(equalTo(binaryRow.bytes())));
    }

    @Test
    void readOfUncommittedRowWithDifferentTransactionIdThrows() {
        RowId rowId = storage.insert(binaryRow, txId);

        assertThrows(TxIdMismatchException.class, () -> storage.read(rowId, newTransactionId()));
    }

    @Test
    void readOfCommittedRowWithAnyTransactionIdReturnsTheRow() {
        RowId rowId = storage.insert(binaryRow, txId);
        storage.commitWrite(rowId, Timestamp.nextVersion());

        BinaryRow foundRow = storage.read(rowId, newTransactionId());

        assertThat(foundRow, is(notNullValue()));
        assertThat(foundRow.bytes(), is(equalTo(binaryRow.bytes())));
    }

    @Test
    void readsUncommittedVersionEvenWhenThereIsCommittedVersionBeforeIt() {
        RowId rowId1 = storage.insert(binaryRow, txId);
        storage.commitWrite(rowId1, Timestamp.nextVersion());

        RowId rowId2 = storage.insert(binaryRow2, txId);

        BinaryRow foundRow = storage.read(rowId2, txId);

        assertThat(foundRow, is(notNullValue()));
        assertThat(foundRow.bytes(), is(equalTo(binaryRow2.bytes())));
    }

    @Test
    void readsCommittedVersionEvenWhenThereIsCommittedVersionBeforeIt() {
        RowId rowId1 = storage.insert(binaryRow, txId);
        storage.commitWrite(rowId1, Timestamp.nextVersion());

        RowId rowId2 = storage.insert(binaryRow2, txId);
        storage.commitWrite(rowId2, Timestamp.nextVersion());

        BinaryRow foundRow = storage.read(rowId2, txId);

        assertThat(foundRow, is(notNullValue()));
        assertThat(foundRow.bytes(), is(equalTo(binaryRow2.bytes())));
    }

    @Test
    void readByExactlyCommitTimestampFindsRow() {
        RowId rowId = storage.insert(binaryRow, txId);
        Timestamp commitTimestamp = Timestamp.nextVersion();
        storage.commitWrite(rowId, commitTimestamp);

        BinaryRow foundRow = storage.read(rowId, commitTimestamp);

        assertThat(foundRow, is(notNullValue()));
        assertThat(foundRow.bytes(), is(equalTo(binaryRow.bytes())));
    }

    @Test
    void readByTimestampAfterCommitTimestampFindsRow() {
        RowId rowId = storage.insert(binaryRow, txId);
        Timestamp commitTimestamp = Timestamp.nextVersion();
        storage.commitWrite(rowId, commitTimestamp);

        Timestamp afterCommit = Timestamp.nextVersion();
        BinaryRow foundRow = storage.read(rowId, afterCommit);

        assertThat(foundRow, is(notNullValue()));
        assertThat(foundRow.bytes(), is(equalTo(binaryRow.bytes())));
    }

    @Test
    void readByTimestampBeforeFirstVersionCommitTimestampFindsNothing() {
        Timestamp beforeCommit = Timestamp.nextVersion();

        RowId rowId = storage.insert(binaryRow, txId);
        Timestamp commitTimestamp = Timestamp.nextVersion();
        storage.commitWrite(rowId, commitTimestamp);

        BinaryRow foundRow = storage.read(rowId, beforeCommit);

        assertThat(foundRow, is(nullValue()));
    }

    @Test
    void readByTimestampOfLastVersionFindsLastVersion() {
        RowId rowId = storage.insert(binaryRow, txId);
        Timestamp firstVersionTs = Timestamp.nextVersion();
        storage.commitWrite(rowId, firstVersionTs);

        storage.addWrite(rowId, binaryRow2, newTransactionId());
        Timestamp secondVersionTs = Timestamp.nextVersion();
        storage.commitWrite(rowId, secondVersionTs);

        BinaryRow foundRow = storage.read(rowId, secondVersionTs);

        assertThat(foundRow, is(notNullValue()));
        assertThat(foundRow.bytes(), is(equalTo(binaryRow2.bytes())));
    }

    @Test
    void readByTimestampOfPreviousVersionFindsPreviousVersion() {
        RowId rowId = storage.insert(binaryRow, txId);
        Timestamp firstVersionTs = Timestamp.nextVersion();
        storage.commitWrite(rowId, firstVersionTs);

        storage.addWrite(rowId, binaryRow2, newTransactionId());
        storage.commitWrite(rowId, Timestamp.nextVersion());

        BinaryRow foundRow = storage.read(rowId, firstVersionTs);

        assertThat(foundRow, is(notNullValue()));
        assertThat(foundRow.bytes(), is(equalTo(binaryRow.bytes())));
    }

    @Test
    void readByTimestampBetweenVersionsFindsPreviousVersion() {
        RowId rowId = storage.insert(binaryRow, txId);
        Timestamp firstVersionTs = Timestamp.nextVersion();
        storage.commitWrite(rowId, firstVersionTs);

        Timestamp tsInBetween = Timestamp.nextVersion();

        storage.addWrite(rowId, binaryRow2, newTransactionId());
        storage.commitWrite(rowId, Timestamp.nextVersion());

        BinaryRow foundRow = storage.read(rowId, tsInBetween);

        assertThat(foundRow, is(notNullValue()));
        assertThat(foundRow.bytes(), is(equalTo(binaryRow.bytes())));
    }

    @Test
    void readByTimestampIgnoresUncommittedVersion() {
        RowId rowId = storage.insert(binaryRow, newTransactionId());
        storage.commitWrite(rowId, Timestamp.nextVersion());

        storage.addWrite(rowId, binaryRow2, newTransactionId());

        Timestamp latestTs = Timestamp.nextVersion();
        BinaryRow foundRow = storage.read(rowId, latestTs);

        assertThat(foundRow, is(notNullValue()));
        assertThat(foundRow.bytes(), is(equalTo(binaryRow.bytes())));
    }

    @Test
    void addWriteWithDifferentTxIdThrows() {
        RowId rowId = storage.insert(binaryRow, txId);

        assertThrows(TxIdMismatchException.class, () -> storage.addWrite(rowId, binaryRow2, newTransactionId()));
    }

    @Test
    void secondUncommittedWriteWithSameTxIdReplacesExistingUncommittedWrite() {
        RowId rowId = storage.insert(binaryRow, txId);

        storage.addWrite(rowId, binaryRow2, txId);

        BinaryRow foundRow = storage.read(rowId, txId);

        assertThat(foundRow, is(notNullValue()));
        assertThat(foundRow.bytes(), is(equalTo(binaryRow2.bytes())));
    }

    @Test
    void addWriteReturnsUncommittedVersionIfItExists() {
        RowId rowId = storage.insert(binaryRow, txId);

        BinaryRow returnedRow = storage.addWrite(rowId, binaryRow2, txId);

        assertThat(returnedRow, is(notNullValue()));
        assertThat(returnedRow.bytes(), is(equalTo(binaryRow.bytes())));
    }

    @Test
    void addWriteReturnsNullIfNoUncommittedVersionExists() {
        RowId rowId = storage.insert(binaryRow, newTransactionId());
        storage.commitWrite(rowId, Timestamp.nextVersion());

        BinaryRow returnedRow = storage.addWrite(rowId, binaryRow2, txId);

        assertThat(returnedRow, is(nullValue()));
    }

    @Test
    void afterRemovalReadWithTxIdFindsNothing() {
        RowId rowId = storage.insert(binaryRow, newTransactionId());
        storage.commitWrite(rowId, Timestamp.nextVersion());

        storage.addWrite(rowId, null, txId);

        BinaryRow foundRow = storage.read(rowId, txId);

        assertThat(foundRow, is(nullValue()));
    }

    @Test
    void afterRemovalReadByLatestTimestampFindsNothing() {
        RowId rowId = storage.insert(binaryRow, newTransactionId());
        storage.commitWrite(rowId, Timestamp.nextVersion());

        storage.addWrite(rowId, null, newTransactionId());
        storage.commitWrite(rowId, Timestamp.nextVersion());

        BinaryRow foundRow = storage.read(rowId, Timestamp.nextVersion());

        assertThat(foundRow, is(nullValue()));
    }

    @Test
    void afterRemovalPreviousVersionRemainsAccessibleByTimestamp() {
        RowId rowId = storage.insert(binaryRow, newTransactionId());
        Timestamp firstTimestamp = Timestamp.nextVersion();
        storage.commitWrite(rowId, firstTimestamp);

        storage.addWrite(rowId, null, newTransactionId());
        storage.commitWrite(rowId, Timestamp.nextVersion());

        BinaryRow foundRow = storage.read(rowId, firstTimestamp);

        assertThat(foundRow, is(notNullValue()));
        assertThat(foundRow.bytes(), is(equalTo(binaryRow.bytes())));
    }

    @Test
    void removalReturnsUncommittedRowVersionIfItExists() {
        RowId rowId = storage.insert(binaryRow, txId);

        BinaryRow rowFromRemoval = storage.addWrite(rowId, null, txId);

        assertThat(rowFromRemoval, is(notNullValue()));
        assertThat(rowFromRemoval.bytes(), is(equalTo(binaryRow.bytes())));
    }

    @Test
    void removalReturnsNullIfNoUncommittedVersionExists() {
        RowId rowId = storage.insert(binaryRow, newTransactionId());
        storage.commitWrite(rowId, Timestamp.nextVersion());

        BinaryRow rowFromRemoval = storage.addWrite(rowId, null, newTransactionId());

        assertThat(rowFromRemoval, is(nullValue()));
    }

    @Test
    void commitWriteMakesVersionAvailableToReadByTimestamp() {
        RowId rowId = storage.insert(binaryRow, txId);

        storage.commitWrite(rowId, Timestamp.nextVersion());

        BinaryRow foundRow = storage.read(rowId, Timestamp.nextVersion());

        assertThat(foundRow, is(notNullValue()));
        assertThat(foundRow.bytes(), is(equalTo(binaryRow.bytes())));
    }

    @Test
    void abortWriteFailsIfNoUncommittedVersionExists() {
        RowId rowId = storage.insert(binaryRow, newTransactionId());
        storage.commitWrite(rowId, Timestamp.nextVersion());

        assertThrows(NoUncommittedVersionException.class, () -> storage.abortWrite(rowId));
    }

    @Test
    void abortWriteRemovesUncommittedVersion() {
        RowId rowId = storage.insert(binaryRow, newTransactionId());
        storage.commitWrite(rowId, Timestamp.nextVersion());

        storage.addWrite(rowId, binaryRow2, txId);

        storage.abortWrite(rowId);

        BinaryRow foundRow = storage.read(rowId, txId);

        assertThat(foundRow, is(notNullValue()));
        assertThat(foundRow.bytes(), is(equalTo(binaryRow.bytes())));
    }

    @Test
    void abortOfInsertMakesRowNonExistentForReadByTimestamp() {
        RowId rowId = storage.insert(binaryRow, newTransactionId());

        storage.abortWrite(rowId);

        BinaryRow foundRow = storage.read(rowId, Timestamp.nextVersion());

        assertThat(foundRow, is(nullValue()));
    }

    @Test
    void abortOfInsertMakesRowNonExistentForReadWithTxId() {
        RowId rowId = storage.insert(binaryRow, txId);

        storage.abortWrite(rowId);

        BinaryRow foundRow = storage.read(rowId, txId);

        assertThat(foundRow, is(nullValue()));
    }

    @Test
    void abortWriteReturnsTheRemovedVersion() {
        RowId rowId = storage.insert(binaryRow, txId);

        BinaryRow returnedRow = storage.abortWrite(rowId);

        assertThat(returnedRow, is(notNullValue()));
        assertThat(returnedRow.bytes(), is(equalTo(binaryRow.bytes())));
    }

    @Test
    void scanWithTxIdThrowsWhenOtherTransactionHasUncommittedChanges() {
        storage.insert(binaryRow, txId);

        Cursor<BinaryRow> cursor = storage.scan(k -> true, newTransactionId());

        assertThrows(TxIdMismatchException.class, cursor::next);
    }
}
