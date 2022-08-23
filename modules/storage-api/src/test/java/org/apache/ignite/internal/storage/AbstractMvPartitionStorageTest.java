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
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
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
import java.util.concurrent.CompletableFuture;
import java.util.function.Predicate;
import java.util.stream.StreamSupport;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.tx.Timestamp;
import org.apache.ignite.internal.util.Cursor;
import org.junit.jupiter.api.Test;

/**
 * Base test for MV partition storages.
 */
public abstract class AbstractMvPartitionStorageTest extends BaseMvStoragesTest {
    /** A partition id that should be used to create a partition instance. */
    protected static final int PARTITION_ID = 1;

    protected MvPartitionStorage storage;

    protected final UUID txId = newTransactionId();

    protected final TestKey key = new TestKey(10, "foo");
    private final TestValue value = new TestValue(20, "bar");
    protected final BinaryRow binaryRow = binaryRow(key, value);
    private final TestValue value2 = new TestValue(21, "bar2");
    protected final BinaryRow binaryRow2 = binaryRow(key, value2);

    /**
     * Reads a row inside of consistency closure.
     */
    protected BinaryRow read(RowId rowId, UUID txId) {
        return storage.runConsistently(() -> storage.read(rowId, txId));
    }

    /**
     * Reads a row inside of consistency closure.
     */
    protected BinaryRow read(RowId rowId, Timestamp timestamp) {
        return storage.runConsistently(() -> storage.read(rowId, timestamp));
    }

    /**
     * Scans partition inside of consistency closure.
     */
    protected Cursor<BinaryRow> scan(Predicate<BinaryRow> filter, Timestamp timestamp) {
        return storage.runConsistently(() -> storage.scan(filter, timestamp));
    }

    /**
     * Scans partition inside of consistency closure.
     */
    protected Cursor<BinaryRow> scan(Predicate<BinaryRow> filter, UUID txId) {
        return storage.runConsistently(() -> storage.scan(filter, txId));
    }

    /**
     * Inserts a row inside of consistency closure.
     */
    protected RowId insert(BinaryRow binaryRow, UUID txId) {
        return storage.runConsistently(() -> storage.insert(binaryRow, txId));
    }

    /**
     * Adds/updates a write-intent inside of consistency closure.
     */
    protected BinaryRow addWrite(RowId rowId, BinaryRow binaryRow, UUID txId) {
        return storage.runConsistently(() -> storage.addWrite(rowId, binaryRow, txId));
    }

    /**
     * Commits write-intent inside of consistency closure.
     */
    protected void commitWrite(RowId rowId, Timestamp tsExact) {
        storage.runConsistently(() -> {
            storage.commitWrite(rowId, tsExact);

            return null;
        });
    }

    /**
     * Aborts write-intent inside of consistency closure.
     */
    protected BinaryRow abortWrite(RowId rowId) {
        return storage.runConsistently(() -> storage.abortWrite(rowId));
    }

    /**
     * Creates a new transaction id.
     */
    protected UUID newTransactionId() {
        return Timestamp.nextVersion().toUuid();
    }

    /**
     * Tests that reads from empty storage return empty results.
     */
    @Test
    public void testReadsFromEmpty() {
        RowId rowId = new RowId(PARTITION_ID);

        assertEquals(PARTITION_ID, rowId.partitionId());

        assertNull(read(rowId, newTransactionId()));
        assertNull(read(rowId, Timestamp.nextVersion()));
    }

    @Test
    public void testScanOverEmpty() throws Exception {
        new RowId(PARTITION_ID);

        assertEquals(List.of(), convert(scan(row -> true, newTransactionId())));
        assertEquals(List.of(), convert(scan(row -> true, Timestamp.nextVersion())));
    }

    /**
     * Tests basic invariants of {@link MvPartitionStorage#addWrite(RowId, BinaryRow, UUID)}.
     */
    @Test
    public void testAddWrite() {
        RowId rowId = insert(binaryRow, txId);

        // Attempt to write from another transaction.
        assertThrows(TxIdMismatchException.class, () -> addWrite(rowId, binaryRow, newTransactionId()));

        // Write from the same transaction.
        addWrite(rowId, binaryRow, txId);

        // Read without timestamp returns uncommited row.
        assertRowMatches(read(rowId, txId), binaryRow);

        // Read with wrong transaction id should throw exception.
        assertThrows(TxIdMismatchException.class, () -> read(rowId, newTransactionId()));

        // Read with timestamp returns null.
        assertNull(read(rowId, Timestamp.nextVersion()));
    }

    /**
     * Tests basic invariants of {@link MvPartitionStorage#abortWrite(RowId)}.
     */
    @Test
    public void testAbortWrite() {
        RowId rowId = insert(binaryRow(key, value), txId);

        abortWrite(rowId);

        // Aborted row can't be read.
        assertNull(read(rowId, txId));
    }

    /**
     * Tests basic invariants of {@link MvPartitionStorage#commitWrite(RowId, Timestamp)}.
     */
    @Test
    public void testCommitWrite() {
        RowId rowId = insert(binaryRow, txId);

        Timestamp tsBefore = Timestamp.nextVersion();

        Timestamp tsExact = Timestamp.nextVersion();
        commitWrite(rowId, tsExact);

        Timestamp tsAfter = Timestamp.nextVersion();

        // Row is invisible at the time before writing.
        assertNull(read(rowId, tsBefore));

        // Row is valid at the time during and after writing.
        assertRowMatches(read(rowId, tsExact), binaryRow);
        assertRowMatches(read(rowId, tsAfter), binaryRow);

        TestValue newValue = new TestValue(30, "duh");

        UUID newTxId = newTransactionId();

        BinaryRow newRow = binaryRow(key, newValue);
        addWrite(rowId, newRow, newTxId);

        // Same checks, but now there are two different versions.
        assertNull(read(rowId, tsBefore));

        assertRowMatches(read(rowId, newTxId), newRow);

        assertRowMatches(read(rowId, tsExact), binaryRow);
        assertRowMatches(read(rowId, tsAfter), binaryRow);
        assertRowMatches(read(rowId, Timestamp.nextVersion()), binaryRow);

        // Only latest time behavior changes after commit.
        commitWrite(rowId, Timestamp.nextVersion());

        assertRowMatches(read(rowId, newTxId), newRow);

        assertRowMatches(read(rowId, tsExact), binaryRow);
        assertRowMatches(read(rowId, tsAfter), binaryRow);

        assertRowMatches(read(rowId, Timestamp.nextVersion()), newRow);

        // Remove.
        UUID removeTxId = newTransactionId();

        addWrite(rowId, null, removeTxId);

        assertNull(read(rowId, tsBefore));

        assertNull(read(rowId, removeTxId));

        assertRowMatches(read(rowId, tsExact), binaryRow);
        assertRowMatches(read(rowId, tsAfter), binaryRow);

        assertRowMatches(read(rowId, Timestamp.nextVersion()), newRow);

        // Commit remove.
        Timestamp removeTs = Timestamp.nextVersion();
        commitWrite(rowId, removeTs);

        assertNull(read(rowId, tsBefore));

        assertNull(read(rowId, removeTxId));
        assertNull(read(rowId, removeTs));
        assertNull(read(rowId, Timestamp.nextVersion()));

        assertRowMatches(read(rowId, tsExact), binaryRow);
        assertRowMatches(read(rowId, tsAfter), binaryRow);
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
        RowId rowId1 = insert(binaryRow(key1, value1), txId1);

        UUID txId2 = newTransactionId();
        RowId rowId2 = insert(binaryRow(key2, value2), txId2);

        // Scan with and without filters.
        assertThrows(TxIdMismatchException.class, () -> convert(scan(row -> true, txId1)));
        assertThrows(TxIdMismatchException.class, () -> convert(scan(row -> true, txId2)));

        assertEquals(List.of(value1), convert(storage.scan(row -> key(row).intKey == 1, txId1)));
        assertEquals(List.of(value2), convert(storage.scan(row -> key(row).intKey == 2, txId2)));

        Timestamp ts1 = Timestamp.nextVersion();

        Timestamp ts2 = Timestamp.nextVersion();
        commitWrite(rowId1, ts2);

        Timestamp ts3 = Timestamp.nextVersion();

        Timestamp ts4 = Timestamp.nextVersion();
        commitWrite(rowId2, ts4);

        Timestamp ts5 = Timestamp.nextVersion();

        // Full scan with various timestamp values.
        assertEquals(List.of(), convert(scan(row -> true, ts1)));

        assertEquals(List.of(value1), convert(scan(row -> true, ts2)));
        assertEquals(List.of(value1), convert(scan(row -> true, ts3)));

        assertEquals(List.of(value1, value2), convert(scan(row -> true, ts4)));
        assertEquals(List.of(value1, value2), convert(scan(row -> true, ts5)));
    }

    @Test
    public void testScanCursorInvariants() {
        TestValue value1 = new TestValue(10, "xxx");

        TestValue value2 = new TestValue(20, "yyy");

        RowId rowId1 = insert(binaryRow(new TestKey(1, "1"), value1), txId);
        commitWrite(rowId1, Timestamp.nextVersion());

        RowId rowId2 = insert(binaryRow(new TestKey(2, "2"), value2), txId);
        commitWrite(rowId2, Timestamp.nextVersion());

        Cursor<BinaryRow> cursor = scan(row -> true, txId);

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
        RowId rowId = insert(binaryRow, txId);

        BinaryRow foundRow = read(rowId, txId);

        assertRowMatches(foundRow, binaryRow);
    }

    protected final void assertRowMatches(BinaryRow rowUnderQuestion, BinaryRow expectedRow) {
        assertThat(rowUnderQuestion, is(notNullValue()));
        assertThat(rowUnderQuestion.bytes(), is(equalTo(expectedRow.bytes())));
    }

    @Test
    void readOfUncommittedRowWithDifferentTransactionIdThrows() {
        RowId rowId = insert(binaryRow, txId);

        assertThrows(TxIdMismatchException.class, () -> read(rowId, newTransactionId()));
    }

    @Test
    void readOfCommittedRowWithAnyTransactionIdReturnsTheRow() {
        RowId rowId = insert(binaryRow, txId);
        commitWrite(rowId, Timestamp.nextVersion());

        BinaryRow foundRow = read(rowId, newTransactionId());

        assertRowMatches(foundRow, binaryRow);
    }

    @Test
    void readsUncommittedVersionEvenWhenThereIsCommittedVersionBeforeIt() {
        RowId rowId1 = insert(binaryRow, txId);
        commitWrite(rowId1, Timestamp.nextVersion());

        RowId rowId2 = insert(binaryRow2, txId);

        BinaryRow foundRow = read(rowId2, txId);

        assertRowMatches(foundRow, binaryRow2);
    }

    @Test
    void readsCommittedVersionEvenWhenThereIsCommittedVersionBeforeIt() {
        RowId rowId1 = insert(binaryRow, txId);
        commitWrite(rowId1, Timestamp.nextVersion());

        RowId rowId2 = insert(binaryRow2, txId);
        commitWrite(rowId2, Timestamp.nextVersion());

        BinaryRow foundRow = read(rowId2, txId);

        assertRowMatches(foundRow, binaryRow2);
    }

    @Test
    void readByExactlyCommitTimestampFindsRow() {
        RowId rowId = insert(binaryRow, txId);
        Timestamp commitTimestamp = Timestamp.nextVersion();
        commitWrite(rowId, commitTimestamp);

        BinaryRow foundRow = read(rowId, commitTimestamp);

        assertRowMatches(foundRow, binaryRow);
    }

    @Test
    void readByTimestampAfterCommitTimestampFindsRow() {
        RowId rowId = insert(binaryRow, txId);
        Timestamp commitTimestamp = Timestamp.nextVersion();
        commitWrite(rowId, commitTimestamp);

        Timestamp afterCommit = Timestamp.nextVersion();
        BinaryRow foundRow = read(rowId, afterCommit);

        assertRowMatches(foundRow, binaryRow);
    }

    @Test
    void readByTimestampBeforeFirstVersionCommitTimestampFindsNothing() {
        Timestamp beforeCommit = Timestamp.nextVersion();

        RowId rowId = insert(binaryRow, txId);
        Timestamp commitTimestamp = Timestamp.nextVersion();
        commitWrite(rowId, commitTimestamp);

        BinaryRow foundRow = read(rowId, beforeCommit);

        assertThat(foundRow, is(nullValue()));
    }

    @Test
    void readByTimestampOfLastVersionFindsLastVersion() {
        RowId rowId = insert(binaryRow, txId);
        Timestamp firstVersionTs = Timestamp.nextVersion();
        commitWrite(rowId, firstVersionTs);

        addWrite(rowId, binaryRow2, newTransactionId());
        Timestamp secondVersionTs = Timestamp.nextVersion();
        commitWrite(rowId, secondVersionTs);

        BinaryRow foundRow = read(rowId, secondVersionTs);

        assertRowMatches(foundRow, binaryRow2);
    }

    @Test
    void readByTimestampOfPreviousVersionFindsPreviousVersion() {
        RowId rowId = insert(binaryRow, txId);
        Timestamp firstVersionTs = Timestamp.nextVersion();
        commitWrite(rowId, firstVersionTs);

        addWrite(rowId, binaryRow2, newTransactionId());
        commitWrite(rowId, Timestamp.nextVersion());

        BinaryRow foundRow = read(rowId, firstVersionTs);

        assertRowMatches(foundRow, binaryRow);
    }

    @Test
    void readByTimestampBetweenVersionsFindsPreviousVersion() {
        RowId rowId = insert(binaryRow, txId);
        Timestamp firstVersionTs = Timestamp.nextVersion();
        commitWrite(rowId, firstVersionTs);

        Timestamp tsInBetween = Timestamp.nextVersion();

        addWrite(rowId, binaryRow2, newTransactionId());
        commitWrite(rowId, Timestamp.nextVersion());

        BinaryRow foundRow = read(rowId, tsInBetween);

        assertRowMatches(foundRow, binaryRow);
    }

    @Test
    void readByTimestampIgnoresUncommittedVersion() {
        RowId rowId = insert(binaryRow, newTransactionId());
        commitWrite(rowId, Timestamp.nextVersion());

        addWrite(rowId, binaryRow2, newTransactionId());

        Timestamp latestTs = Timestamp.nextVersion();
        BinaryRow foundRow = read(rowId, latestTs);

        assertRowMatches(foundRow, binaryRow);
    }

    @Test
    void addWriteWithDifferentTxIdThrows() {
        RowId rowId = insert(binaryRow, txId);

        assertThrows(TxIdMismatchException.class, () -> addWrite(rowId, binaryRow2, newTransactionId()));
    }

    @Test
    void secondUncommittedWriteWithSameTxIdReplacesExistingUncommittedWrite() {
        RowId rowId = insert(binaryRow, txId);

        addWrite(rowId, binaryRow2, txId);

        BinaryRow foundRow = read(rowId, txId);

        assertRowMatches(foundRow, binaryRow2);
    }

    @Test
    void addWriteReturnsUncommittedVersionIfItExists() {
        RowId rowId = insert(binaryRow, txId);

        BinaryRow returnedRow = addWrite(rowId, binaryRow2, txId);

        assertRowMatches(returnedRow, binaryRow);
    }

    @Test
    void addWriteReturnsNullIfNoUncommittedVersionExists() {
        RowId rowId = insert(binaryRow, newTransactionId());
        commitWrite(rowId, Timestamp.nextVersion());

        BinaryRow returnedRow = addWrite(rowId, binaryRow2, txId);

        assertThat(returnedRow, is(nullValue()));
    }

    @Test
    void afterRemovalReadWithTxIdFindsNothing() {
        RowId rowId = insert(binaryRow, newTransactionId());
        commitWrite(rowId, Timestamp.nextVersion());

        addWrite(rowId, null, txId);

        BinaryRow foundRow = read(rowId, txId);

        assertThat(foundRow, is(nullValue()));
    }

    @Test
    void afterRemovalReadByLatestTimestampFindsNothing() {
        RowId rowId = insert(binaryRow, newTransactionId());
        commitWrite(rowId, Timestamp.nextVersion());

        addWrite(rowId, null, newTransactionId());
        commitWrite(rowId, Timestamp.nextVersion());

        BinaryRow foundRow = read(rowId, Timestamp.nextVersion());

        assertThat(foundRow, is(nullValue()));
    }

    @Test
    void afterRemovalPreviousVersionRemainsAccessibleByTimestamp() {
        RowId rowId = insert(binaryRow, newTransactionId());
        Timestamp firstTimestamp = Timestamp.nextVersion();
        commitWrite(rowId, firstTimestamp);

        addWrite(rowId, null, newTransactionId());
        commitWrite(rowId, Timestamp.nextVersion());

        BinaryRow foundRow = read(rowId, firstTimestamp);

        assertRowMatches(foundRow, binaryRow);
    }

    @Test
    void removalReturnsUncommittedRowVersionIfItExists() {
        RowId rowId = insert(binaryRow, txId);

        BinaryRow rowFromRemoval = addWrite(rowId, null, txId);

        assertRowMatches(rowFromRemoval, binaryRow);
    }

    @Test
    void removalReturnsNullIfNoUncommittedVersionExists() {
        RowId rowId = insert(binaryRow, newTransactionId());
        commitWrite(rowId, Timestamp.nextVersion());

        BinaryRow rowFromRemoval = addWrite(rowId, null, newTransactionId());

        assertThat(rowFromRemoval, is(nullValue()));
    }

    @Test
    void commitWriteMakesVersionAvailableToReadByTimestamp() {
        RowId rowId = insert(binaryRow, txId);

        commitWrite(rowId, Timestamp.nextVersion());

        BinaryRow foundRow = read(rowId, Timestamp.nextVersion());

        assertRowMatches(foundRow, binaryRow);
    }

    @Test
    void commitAndAbortWriteNoOpIfNoUncommittedVersionExists() {
        RowId rowId = insert(binaryRow, newTransactionId());
        commitWrite(rowId, Timestamp.nextVersion());

        abortWrite(rowId);

        assertRowMatches(read(rowId, newTransactionId()), binaryRow);

        commitWrite(rowId, Timestamp.nextVersion());

        assertRowMatches(read(rowId, newTransactionId()), binaryRow);
    }

    @Test
    void abortWriteRemovesUncommittedVersion() {
        RowId rowId = insert(binaryRow, newTransactionId());
        commitWrite(rowId, Timestamp.nextVersion());

        addWrite(rowId, binaryRow2, txId);

        abortWrite(rowId);

        BinaryRow foundRow = read(rowId, txId);

        assertRowMatches(foundRow, binaryRow);
    }

    @Test
    void abortOfInsertMakesRowNonExistentForReadByTimestamp() {
        RowId rowId = insert(binaryRow, newTransactionId());

        abortWrite(rowId);

        BinaryRow foundRow = read(rowId, Timestamp.nextVersion());

        assertThat(foundRow, is(nullValue()));
    }

    @Test
    void abortOfInsertMakesRowNonExistentForReadWithTxId() {
        RowId rowId = new RowId(PARTITION_ID);

        BinaryRow foundRow = read(rowId, txId);

        assertThat(foundRow, is(nullValue()));
    }

    @Test
    void abortWriteReturnsTheRemovedVersion() {
        RowId rowId = insert(binaryRow, txId);

        BinaryRow returnedRow = abortWrite(rowId);

        assertRowMatches(returnedRow, binaryRow);
    }

    @Test
    void scanWithTxIdThrowsWhenOtherTransactionHasUncommittedChanges() {
        insert(binaryRow, txId);

        Cursor<BinaryRow> cursor = scan(row -> true, newTransactionId());

        assertThrows(TxIdMismatchException.class, cursor::next);
    }

    @Test
    void readByTimestampWorksCorrectlyAfterCommitAndAbortFollowedByUncommittedWrite() {
        RowId rowId = commitAbortAndAddUncommitted();

        BinaryRow foundRow = storage.read(rowId, Timestamp.nextVersion());

        assertRowMatches(foundRow, binaryRow);
    }

    private RowId commitAbortAndAddUncommitted() {
        return storage.runConsistently(() -> {
            RowId rowId = storage.insert(binaryRow, txId);

            storage.commitWrite(rowId, Timestamp.nextVersion());

            storage.addWrite(rowId, binaryRow2, newTransactionId());
            storage.abortWrite(rowId);

            BinaryRow binaryRow3 = binaryRow(key, new TestValue(22, "bar3"));

            storage.addWrite(rowId, binaryRow3, newTransactionId());

            return rowId;
        });
    }

    @Test
    void scanByTimestampWorksCorrectlyAfterCommitAndAbortFollowedByUncommittedWrite() throws Exception {
        commitAbortAndAddUncommitted();

        try (Cursor<BinaryRow> cursor = storage.scan(k -> true, Timestamp.nextVersion())) {
            BinaryRow foundRow = cursor.next();

            assertRowMatches(foundRow, binaryRow);

            assertFalse(cursor.hasNext());
        }
    }

    @Test
    void readByTimestampWorksCorrectlyIfNoUncommittedValueExists() {
        RowId rowId = insert(binaryRow, txId);

        BinaryRow foundRow = read(rowId, Timestamp.nextVersion());

        assertThat(foundRow, is(nullValue()));
    }

    /**
     * Tests that changed {@link MvPartitionStorage#lastAppliedIndex()} can be successfully read and that it's returned from
     * {@link MvPartitionStorage#persistedIndex()} after the {@link MvPartitionStorage#flush()}.
     */
    @Test
    void testAppliedIndex() {
        storage.runConsistently(() -> {
            assertEquals(0, storage.lastAppliedIndex());
            assertEquals(0, storage.persistedIndex());

            storage.lastAppliedIndex(1);

            assertEquals(1, storage.lastAppliedIndex());
            assertThat(storage.persistedIndex(), is(lessThanOrEqualTo(1L)));

            return null;
        });

        CompletableFuture<Void> flushFuture = storage.flush();

        assertThat(flushFuture, willCompleteSuccessfully());

        assertEquals(1, storage.persistedIndex());
    }
}
