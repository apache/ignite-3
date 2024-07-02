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

package org.apache.ignite.internal.storage;

import static java.util.stream.Collectors.toList;
import static org.apache.ignite.internal.schema.BinaryRowMatcher.equalToRow;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.lang.IgniteBiTuple;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.util.Cursor;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/**
 * Base test for MV partition storages.
 */
public abstract class AbstractMvPartitionStorageTest extends BaseMvPartitionStorageTest {
    protected final UUID txId = newTransactionId();

    protected final TestKey key = new TestKey(10, "foo");
    private final TestValue value = new TestValue(20, "bar");
    protected final BinaryRow binaryRow = binaryRow(key, value);
    private final TestValue value2 = new TestValue(21, "bar2");
    private final BinaryRow binaryRow2 = binaryRow(key, value2);
    private final BinaryRow binaryRow3 = binaryRow(key, new TestValue(22, "bar3"));

    /**
     * Tests that reads from empty storage return empty results.
     */
    @Test
    public void testReadsFromEmpty() {
        RowId rowId = new RowId(PARTITION_ID);

        assertEquals(PARTITION_ID, rowId.partitionId());

        assertNull(read(rowId, clock.now()));
    }

    @ParameterizedTest
    @EnumSource
    public void testScanOverEmpty(ScanTimestampProvider tsProvider) {
        assertEquals(List.of(), convert(scan(tsProvider.scanTimestamp(clock))));
    }

    /**
     * Tests basic invariants of {@link MvPartitionStorage#addWrite(RowId, BinaryRow, UUID, int, int)}.
     */
    @Test
    public void testAddWrite() {
        RowId rowId = insert(binaryRow, txId);

        // Attempt to write from another transaction.
        assertThrows(TxIdMismatchException.class, () -> addWrite(rowId, binaryRow, newTransactionId()));

        // Write from the same transaction.
        addWrite(rowId, binaryRow, txId);

        // Read with timestamp returns write-intent.
        assertThat(read(rowId, clock.now()), is(equalToRow(binaryRow)));

        // Remove write.
        addWrite(rowId, null, txId);

        // Removed row can't be read.
        assertNull(read(rowId, HybridTimestamp.MAX_VALUE));

        // Remove write once again.
        addWrite(rowId, null, txId);

        // Still can't be read.
        assertNull(read(rowId, HybridTimestamp.MAX_VALUE));
    }

    /**
     * Tests basic invariants of {@link MvPartitionStorage#abortWrite(RowId)}.
     */
    @Test
    public void testAbortWrite() {
        RowId rowId = insert(binaryRow(key, value), txId);

        abortWrite(rowId);

        // Aborted row can't be read.
        assertNull(read(rowId, HybridTimestamp.MAX_VALUE));
    }

    /**
     * Tests basic invariants of {@link MvPartitionStorage#commitWrite(RowId, HybridTimestamp)}.
     */
    @Test
    public void testCommitWrite() {
        RowId rowId = insert(binaryRow, txId);

        HybridTimestamp tsBefore = clock.now();

        HybridTimestamp tsExact = clock.now();
        commitWrite(rowId, tsExact);

        HybridTimestamp tsAfter = clock.now();

        // Row is invisible at the time before writing.
        assertNull(read(rowId, tsBefore));

        // Row is valid at the time during and after writing.
        assertThat(read(rowId, tsExact), is(equalToRow(binaryRow)));
        assertThat(read(rowId, tsAfter), is(equalToRow(binaryRow)));

        TestValue newValue = new TestValue(30, "duh");

        UUID newTxId = newTransactionId();

        BinaryRow newRow = binaryRow(key, newValue);
        addWrite(rowId, newRow, newTxId);

        // Same checks, but now there are two different versions.
        assertNull(read(rowId, tsBefore));

        assertThat(read(rowId, HybridTimestamp.MAX_VALUE), is(equalToRow(newRow)));

        assertThat(read(rowId, tsExact), is(equalToRow(binaryRow)));
        assertThat(read(rowId, tsAfter), is(equalToRow(newRow)));
        assertThat(read(rowId, clock.now()), is(equalToRow(newRow)));

        // Only latest time behavior changes after commit.
        HybridTimestamp newRowCommitTs = clock.now();
        commitWrite(rowId, newRowCommitTs);

        assertThat(read(rowId, HybridTimestamp.MAX_VALUE), is(equalToRow(newRow)));

        assertThat(read(rowId, tsExact), is(equalToRow(binaryRow)));
        assertThat(read(rowId, tsAfter), is(equalToRow(binaryRow)));

        assertThat(read(rowId, clock.now()), is(equalToRow(newRow)));

        // Remove.
        UUID removeTxId = newTransactionId();

        addWrite(rowId, null, removeTxId);

        assertNull(read(rowId, tsBefore));

        assertNull(read(rowId, HybridTimestamp.MAX_VALUE));

        assertThat(read(rowId, tsExact), is(equalToRow(binaryRow)));
        assertThat(read(rowId, tsAfter), is(equalToRow(binaryRow)));
        assertThat(read(rowId, newRowCommitTs), is(equalToRow(newRow)));

        assertNull(read(rowId, clock.now()));

        // Commit remove.
        HybridTimestamp removeTs = clock.now();
        commitWrite(rowId, removeTs);

        assertNull(read(rowId, tsBefore));

        assertNull(read(rowId, HybridTimestamp.MAX_VALUE));
        assertNull(read(rowId, removeTs));
        assertNull(read(rowId, clock.now()));

        assertThat(read(rowId, tsExact), is(equalToRow(binaryRow)));
        assertThat(read(rowId, tsAfter), is(equalToRow(binaryRow)));
    }

    /**
     * Tests basic invariants of {@link MvPartitionStorage#scan(HybridTimestamp)}.
     */
    @Test
    public void testScan() {
        TestKey key1 = new TestKey(1, "1");
        TestValue value1 = new TestValue(10, "xxx");

        TestKey key2 = new TestKey(2, "2");
        TestValue value2 = new TestValue(20, "yyy");

        UUID txId = newTransactionId();
        RowId rowId1 = insert(binaryRow(key1, value1), txId);
        RowId rowId2 = insert(binaryRow(key2, value2), txId);

        HybridTimestamp ts1 = clock.now();

        HybridTimestamp ts2 = clock.now();
        commitWrite(rowId1, ts2);

        HybridTimestamp ts3 = clock.now();

        HybridTimestamp ts4 = clock.now();
        commitWrite(rowId2, ts4);

        HybridTimestamp ts5 = clock.now();

        // Full scan with various timestamp values.
        assertEquals(List.of(), convert(scan(ts1)));

        assertEquals(List.of(value1), convert(scan(ts2)));
        assertEquals(List.of(value1), convert(scan(ts3)));

        assertEquals(List.of(value1, value2), convert(scan(ts4)));
        assertEquals(List.of(value1, value2), convert(scan(ts5)));

        assertEquals(List.of(value1, value2), convert(scan(HybridTimestamp.MAX_VALUE)));
    }

    @SuppressWarnings("ConstantConditions")
    @Test
    public void testTransactionScanCursorInvariants() {
        TestValue value1 = new TestValue(10, "xxx");

        TestValue value2 = new TestValue(20, "yyy");

        RowId rowId1 = insert(binaryRow(new TestKey(1, "1"), value1), txId);
        commitWrite(rowId1, clock.now());

        RowId rowId2 = insert(binaryRow(new TestKey(2, "2"), value2), txId);
        commitWrite(rowId2, clock.now());

        try (PartitionTimestampCursor cursor = scan(HybridTimestamp.MAX_VALUE)) {
            assertTrue(cursor.hasNext());
            assertTrue(cursor.hasNext());

            List<TestValue> res = new ArrayList<>();

            res.add(value(cursor.next().binaryRow()));

            assertTrue(cursor.hasNext());
            assertTrue(cursor.hasNext());

            res.add(value(cursor.next().binaryRow()));

            assertFalse(cursor.hasNext());
            assertFalse(cursor.hasNext());

            assertThrows(NoSuchElementException.class, cursor::next);

            assertThat(res, containsInAnyOrder(value1, value2));
        }
    }

    @SuppressWarnings("ConstantConditions")
    @Test
    public void testTimestampScanCursorInvariants() {
        TestValue value11 = new TestValue(10, "xxx");
        TestValue value12 = new TestValue(11, "xxx");

        TestValue value21 = new TestValue(20, "yyy");
        TestValue value22 = new TestValue(21, "yyy");

        RowId rowId1 = new RowId(PARTITION_ID, 10, 10);
        RowId rowId2 = new RowId(PARTITION_ID, 10, 20);

        TestKey key1 = new TestKey(1, "1");
        BinaryRow binaryRow11 = binaryRow(key1, value11);
        BinaryRow binaryRow12 = binaryRow(key1, value12);

        addWrite(rowId1, binaryRow11, txId);
        HybridTimestamp commitTs1 = clock.now();
        commitWrite(rowId1, commitTs1);

        addWrite(rowId1, binaryRow12, newTransactionId());

        TestKey key2 = new TestKey(2, "2");
        BinaryRow binaryRow21 = binaryRow(key2, value21);
        BinaryRow binaryRow22 = binaryRow(key2, value22);

        addWrite(rowId2, binaryRow21, txId);
        HybridTimestamp commitTs2 = clock.now();
        commitWrite(rowId2, commitTs2);

        addWrite(rowId2, binaryRow22, newTransactionId());

        try (PartitionTimestampCursor cursor = scan(clock.now())) {
            assertThrows(IllegalStateException.class, () -> cursor.committed(commitTs1));

            assertTrue(cursor.hasNext());
            while (cursor.hasNext()) {
                assertTrue(cursor.hasNext());

                ReadResult res = cursor.next();

                assertNotNull(res);
                assertTrue(res.isWriteIntent());
                assertFalse(res.isEmpty());

                BinaryRow expectedRow1;
                BinaryRow expectedRow2;
                HybridTimestamp commitTs;

                TestKey readKey = key(res.binaryRow());

                if (readKey.equals(key1)) {
                    expectedRow1 = binaryRow11;
                    expectedRow2 = binaryRow12;
                    commitTs = commitTs1;
                } else {
                    expectedRow1 = binaryRow21;
                    expectedRow2 = binaryRow22;
                    commitTs = commitTs2;
                }

                assertThat(res.binaryRow(), is(equalToRow(expectedRow2)));

                BinaryRow previousRow = cursor.committed(commitTs);

                assertNotNull(previousRow);
                assertThat(previousRow, is(equalToRow(expectedRow1)));
            }

            assertFalse(cursor.hasNext());
            assertFalse(cursor.hasNext());

            assertThrows(NoSuchElementException.class, cursor::next);

            assertThrows(IllegalStateException.class, () -> cursor.committed(commitTs1));
        }
    }

    private static List<TestValue> convert(PartitionTimestampCursor cursor) {
        try (cursor) {
            return cursor.stream()
                    .map(rs -> value(rs.binaryRow()))
                    .sorted(Comparator.nullsFirst(Comparator.naturalOrder()))
                    .collect(toList());
        }
    }

    @Test
    void readOfUncommittedRowReturnsTheRow() {
        RowId rowId = insert(binaryRow, txId);

        ReadResult foundResult = storage.read(rowId, HybridTimestamp.MAX_VALUE);

        assertThat(foundResult.rowId(), is(rowId));
        assertThat(foundResult.binaryRow(), is(equalToRow(binaryRow)));
    }

    @Test
    void readOfCommittedRowReturnsTheRow() {
        RowId rowId = insert(binaryRow, txId);
        commitWrite(rowId, clock.now());

        ReadResult foundResult = storage.read(rowId, HybridTimestamp.MAX_VALUE);

        assertThat(foundResult.rowId(), is(rowId));
        assertThat(foundResult.binaryRow(), is(equalToRow(binaryRow)));
    }

    @Test
    void readsUncommittedVersionEvenWhenThereIsCommittedVersionBeforeIt() {
        RowId rowId1 = insert(binaryRow, txId);
        commitWrite(rowId1, clock.now());

        RowId rowId2 = insert(binaryRow2, txId);

        ReadResult foundResult = storage.read(rowId2, HybridTimestamp.MAX_VALUE);

        assertThat(foundResult.rowId(), is(rowId2));
        assertThat(foundResult.binaryRow(), is(equalToRow(binaryRow2)));
    }

    @Test
    void readsCommittedVersionEvenWhenThereIsCommittedVersionBeforeIt() {
        RowId rowId1 = insert(binaryRow, txId);
        commitWrite(rowId1, clock.now());

        RowId rowId2 = insert(binaryRow2, txId);
        commitWrite(rowId2, clock.now());

        ReadResult foundResult = storage.read(rowId2, HybridTimestamp.MAX_VALUE);

        assertThat(foundResult.rowId(), is(rowId2));
        assertThat(foundResult.binaryRow(), is(equalToRow(binaryRow2)));
    }

    @Test
    void readByExactlyCommitTimestampFindsRow() {
        RowId rowId = insert(binaryRow, txId);
        HybridTimestamp commitTimestamp = clock.now();
        commitWrite(rowId, commitTimestamp);

        BinaryRow foundRow = read(rowId, commitTimestamp);

        assertThat(foundRow, is(equalToRow(binaryRow)));
    }

    @Test
    void readByTimestampAfterCommitTimestampFindsRow() {
        RowId rowId = insert(binaryRow, txId);
        HybridTimestamp commitTimestamp = clock.now();
        commitWrite(rowId, commitTimestamp);

        HybridTimestamp afterCommit = clock.now();
        BinaryRow foundRow = read(rowId, afterCommit);

        assertThat(foundRow, is(equalToRow(binaryRow)));
    }

    @Test
    void readByTimestampBeforeFirstVersionCommitTimestampFindsNothing() {
        HybridTimestamp beforeCommit = clock.now();

        RowId rowId = insert(binaryRow, txId);
        HybridTimestamp commitTimestamp = clock.now();
        commitWrite(rowId, commitTimestamp);

        BinaryRow foundRow = read(rowId, beforeCommit);

        assertThat(foundRow, is(nullValue()));
    }

    @Test
    void readByTimestampOfLastVersionFindsLastVersion() {
        RowId rowId = insert(binaryRow, txId);
        HybridTimestamp firstVersionTs = clock.now();
        commitWrite(rowId, firstVersionTs);

        addWrite(rowId, binaryRow2, newTransactionId());
        HybridTimestamp secondVersionTs = clock.now();
        commitWrite(rowId, secondVersionTs);

        BinaryRow foundRow = read(rowId, secondVersionTs);

        assertThat(foundRow, is(equalToRow(binaryRow2)));
    }

    @Test
    void readByTimestampOfPreviousVersionFindsPreviousVersion() {
        RowId rowId = insert(binaryRow, txId);
        HybridTimestamp firstVersionTs = clock.now();
        commitWrite(rowId, firstVersionTs);

        addWrite(rowId, binaryRow2, newTransactionId());
        commitWrite(rowId, clock.now());

        BinaryRow foundRow = read(rowId, firstVersionTs);

        assertThat(foundRow, is(equalToRow(binaryRow)));
    }

    @Test
    void readByTimestampBetweenVersionsFindsPreviousVersion() {
        RowId rowId = insert(binaryRow, txId);
        HybridTimestamp firstVersionTs = clock.now();
        commitWrite(rowId, firstVersionTs);

        HybridTimestamp tsInBetween = clock.now();

        addWrite(rowId, binaryRow2, newTransactionId());
        commitWrite(rowId, clock.now());

        BinaryRow foundRow = read(rowId, tsInBetween);

        assertThat(foundRow, is(equalToRow(binaryRow)));
    }

    @Test
    void readByTimestampAfterWriteFindsUncommittedVersion() {
        RowId rowId = new RowId(PARTITION_ID);

        addWrite(rowId, binaryRow, newTransactionId());

        HybridTimestamp latestTs = clock.now();
        BinaryRow foundRow = read(rowId, latestTs);

        assertThat(foundRow, is(equalToRow(binaryRow)));
    }

    @Test
    void readByTimestampAfterCommitAndWriteFindsUncommittedVersion() {
        RowId rowId = insert(binaryRow, newTransactionId());
        commitWrite(rowId, clock.now());

        addWrite(rowId, binaryRow2, newTransactionId());

        HybridTimestamp latestTs = clock.now();
        BinaryRow foundRow = read(rowId, latestTs);

        assertThat(foundRow, is(equalToRow(binaryRow2)));
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

        BinaryRow foundRow = read(rowId, HybridTimestamp.MAX_VALUE);

        assertThat(foundRow, is(equalToRow(binaryRow2)));
    }

    @Test
    void addWriteReturnsUncommittedVersionIfItExists() {
        RowId rowId = insert(binaryRow, txId);

        BinaryRow returnedRow = addWrite(rowId, binaryRow2, txId);

        assertThat(returnedRow, is(equalToRow(binaryRow)));
    }

    @Test
    void addWriteReturnsNullIfNoUncommittedVersionExists() {
        RowId rowId = insert(binaryRow, newTransactionId());
        commitWrite(rowId, clock.now());

        BinaryRow returnedRow = addWrite(rowId, binaryRow2, txId);

        assertThat(returnedRow, is(nullValue()));
    }

    @Test
    void addWriteCommittedTombstone() {
        addWriteCommitted(ROW_ID, binaryRow, clock.now());
        assertThat(read(ROW_ID, HybridTimestamp.MAX_VALUE), is(equalToRow(binaryRow)));

        addWriteCommitted(ROW_ID, null, clock.now());
        assertNull(read(ROW_ID, HybridTimestamp.MAX_VALUE));
    }

    @Test
    void addWriteCreatesUncommittedVersion() {
        RowId rowId = insert(binaryRow, txId);

        ReadResult readResult = storage.read(rowId, clock.now());

        assertTrue(readResult.isWriteIntent());
    }

    @Test
    void afterRemovalReadWithTxIdFindsNothing() {
        RowId rowId = insert(binaryRow, newTransactionId());
        commitWrite(rowId, clock.now());

        addWrite(rowId, null, txId);

        BinaryRow foundRow = read(rowId, HybridTimestamp.MAX_VALUE);

        assertThat(foundRow, is(nullValue()));
    }

    @Test
    void afterRemovalReadByLatestTimestampFindsNothing() {
        RowId rowId = insert(binaryRow, newTransactionId());
        commitWrite(rowId, clock.now());

        addWrite(rowId, null, newTransactionId());
        commitWrite(rowId, clock.now());

        BinaryRow foundRow = read(rowId, clock.now());

        assertThat(foundRow, is(nullValue()));
    }

    @Test
    void afterRemovalPreviousVersionRemainsAccessibleByTimestamp() {
        RowId rowId = insert(binaryRow, newTransactionId());
        HybridTimestamp firstTimestamp = clock.now();
        commitWrite(rowId, firstTimestamp);

        addWrite(rowId, null, newTransactionId());
        commitWrite(rowId, clock.now());

        BinaryRow foundRow = read(rowId, firstTimestamp);

        assertThat(foundRow, is(equalToRow(binaryRow)));
    }

    @Test
    void removalReturnsUncommittedRowVersionIfItExists() {
        RowId rowId = insert(binaryRow, txId);

        BinaryRow rowFromRemoval = addWrite(rowId, null, txId);

        assertThat(rowFromRemoval, is(equalToRow(binaryRow)));
    }

    @Test
    void removalReturnsNullIfNoUncommittedVersionExists() {
        RowId rowId = insert(binaryRow, newTransactionId());
        commitWrite(rowId, clock.now());

        BinaryRow rowFromRemoval = addWrite(rowId, null, newTransactionId());

        assertThat(rowFromRemoval, is(nullValue()));
    }

    @Test
    void commitWriteCommitsWriteIntentVersion() {
        RowId rowId = insert(binaryRow, txId);
        commitWrite(rowId, clock.now());

        ReadResult readResult = storage.read(rowId, clock.now());

        assertFalse(readResult.isWriteIntent());
    }

    @Test
    void commitWriteMakesVersionAvailableToReadByTimestamp() {
        RowId rowId = insert(binaryRow, txId);

        commitWrite(rowId, clock.now());

        BinaryRow foundRow = read(rowId, clock.now());

        assertThat(foundRow, is(equalToRow(binaryRow)));
    }

    @Test
    void commitAndAbortWriteNoOpIfNoUncommittedVersionExists() {
        RowId rowId = insert(binaryRow, newTransactionId());
        commitWrite(rowId, clock.now());

        abortWrite(rowId);

        assertThat(read(rowId, HybridTimestamp.MAX_VALUE), is(equalToRow(binaryRow)));

        commitWrite(rowId, clock.now());

        assertThat(read(rowId, HybridTimestamp.MAX_VALUE), is(equalToRow(binaryRow)));
    }

    @Test
    void abortWriteRemovesUncommittedVersion() {
        RowId rowId = insert(binaryRow, newTransactionId());
        commitWrite(rowId, clock.now());

        addWrite(rowId, binaryRow2, txId);

        abortWrite(rowId);

        BinaryRow foundRow = read(rowId, HybridTimestamp.MAX_VALUE);

        assertThat(foundRow, is(equalToRow(binaryRow)));
    }

    @Test
    void abortOfInsertMakesRowNonExistentForReadByTimestamp() {
        RowId rowId = insert(binaryRow, newTransactionId());

        abortWrite(rowId);

        BinaryRow foundRow = read(rowId, clock.now());

        assertThat(foundRow, is(nullValue()));
    }

    @Test
    void abortOfInsertMakesRowNonExistentForReadWithTxId() {
        RowId rowId = new RowId(PARTITION_ID);

        BinaryRow foundRow = read(rowId, HybridTimestamp.MAX_VALUE);

        assertThat(foundRow, is(nullValue()));
    }

    @Test
    void abortWriteReturnsTheRemovedVersion() {
        RowId rowId = insert(binaryRow, txId);

        BinaryRow returnedRow = abortWrite(rowId);

        assertThat(returnedRow, is(equalToRow(binaryRow)));
    }

    @Test
    void readByTimestampWorksCorrectlyAfterCommitAndAbortFollowedByUncommittedWrite() {
        RowId rowId = commitAbortAndAddUncommitted();

        ReadResult foundResult = storage.read(rowId, clock.now());

        // We see the uncommitted row.
        assertThat(foundResult.rowId(), is(rowId));
        assertThat(foundResult.binaryRow(), is(equalToRow(binaryRow3)));
    }

    @Test
    void readByTimestampBeforeAndAfterUncommittedWrite() {
        RowId rowId = new RowId(PARTITION_ID);

        HybridTimestamp commitTs = clock.now();

        storage.runConsistently(locker -> {
            addWrite(rowId, binaryRow, txId);

            commitWrite(rowId, commitTs);

            return null;
        });

        UUID txId2 = UUID.randomUUID();

        storage.runConsistently(locker -> {
            addWrite(rowId, binaryRow2, txId2);

            return null;
        });

        ReadResult res = storage.read(rowId, commitTs);

        assertNotNull(res);

        assertNull(res.transactionId());
        assertNull(res.commitTableId());
        assertEquals(ReadResult.UNDEFINED_COMMIT_PARTITION_ID, res.commitPartitionId());
        assertThat(res.binaryRow(), is(equalToRow(binaryRow)));

        res = storage.read(rowId, clock.now());

        assertNotNull(res);

        assertEquals(txId2, res.transactionId());
        assertEquals(COMMIT_TABLE_ID, res.commitTableId());
        assertEquals(PARTITION_ID, res.commitPartitionId());
        assertThat(res.binaryRow(), is(equalToRow(binaryRow2)));
    }

    private RowId commitAbortAndAddUncommitted() {
        return storage.runConsistently(locker -> {
            RowId rowId = new RowId(PARTITION_ID);

            locker.lock(rowId);

            storage.addWrite(rowId, binaryRow, txId, 999, 0);
            commitWrite(rowId, clock.now());

            addWrite(rowId, binaryRow2, newTransactionId());
            storage.abortWrite(rowId);

            addWrite(rowId, binaryRow3, newTransactionId());

            return rowId;
        });
    }

    @ParameterizedTest
    @EnumSource(ScanTimestampProvider.class)
    void scanWorksCorrectlyAfterCommitAndAbortFollowedByUncommittedWrite(ScanTimestampProvider tsProvider) {
        RowId rowId = commitAbortAndAddUncommitted();

        try (Cursor<ReadResult> cursor = storage.scan(tsProvider.scanTimestamp(clock))) {
            ReadResult result = cursor.next();

            assertThat(result.rowId(), is(rowId));
            assertThat(result.binaryRow(), is(equalToRow(binaryRow3)));

            assertFalse(cursor.hasNext());
        }
    }

    @Test
    void readByTimestampWorksCorrectlyIfNoUncommittedValueExists() {
        RowId rowId = insert(binaryRow, txId);

        BinaryRow foundRow = read(rowId, clock.now());

        assertThat(foundRow, is(equalToRow(binaryRow)));
    }

    /**
     * Tests that changed {@link MvPartitionStorage#lastAppliedIndex()} can be successfully read back.
     */
    @Test
    void testAppliedIndex() {
        storage.runConsistently(locker -> {
            assertEquals(0, storage.lastAppliedIndex());
            assertEquals(0, storage.lastAppliedTerm());

            storage.lastApplied(1, 1);

            assertEquals(1, storage.lastAppliedIndex());
            assertEquals(1, storage.lastAppliedTerm());

            return null;
        });

        CompletableFuture<Void> flushFuture = storage.flush();

        assertThat(flushFuture, willCompleteSuccessfully());
    }

    @Test
    void testReadWithinBeforeAndAfterTwoCommits() {
        HybridTimestamp before = clock.now();

        RowId rowId = new RowId(PARTITION_ID);

        HybridTimestamp first = clock.now();

        storage.runConsistently(locker -> {
            addWrite(rowId, binaryRow, newTransactionId());

            commitWrite(rowId, first);
            return null;
        });

        HybridTimestamp betweenCommits = clock.now();

        HybridTimestamp second = clock.now();

        storage.runConsistently(locker -> {
            addWrite(rowId, binaryRow2, newTransactionId());

            commitWrite(rowId, second);
            return null;
        });

        storage.runConsistently(locker -> {
            addWrite(rowId, binaryRow3, newTransactionId());

            return null;
        });

        HybridTimestamp after = clock.now();

        // Read before commits.
        ReadResult res = storage.read(rowId, before);
        assertNull(res.binaryRow());

        // Read at exact time of first commit.
        res = storage.read(rowId, first);

        assertNotNull(res);
        assertNull(res.newestCommitTimestamp());
        assertThat(res.binaryRow(), is(equalToRow(binaryRow)));

        // Read between two commits.
        res = storage.read(rowId, betweenCommits);

        assertNotNull(res);
        assertNull(res.newestCommitTimestamp());
        assertThat(res.binaryRow(), is(equalToRow(binaryRow)));

        // Read at exact time of second commit.
        res = storage.read(rowId, second);

        assertNotNull(res);
        assertNull(res.newestCommitTimestamp());
        assertThat(res.binaryRow(), is(equalToRow(binaryRow2)));

        // Read after second commit (write intent).
        res = storage.read(rowId, after);

        assertNotNull(res);
        assertNotNull(res.newestCommitTimestamp());
        assertEquals(second, res.newestCommitTimestamp());
        assertThat(res.binaryRow(), is(equalToRow(binaryRow3)));
    }

    @Test
    void testWrongPartition() {
        RowId rowId = commitAbortAndAddUncommitted();

        var row = new RowId(rowId.partitionId() + 1, rowId.mostSignificantBits(), rowId.leastSignificantBits());

        assertThrows(IllegalArgumentException.class, () -> read(row, clock.now()));
    }

    @Test
    void testReadingNothingWithLowerRowIdIfHigherRowIdWritesExist() {
        RowId rowId = commitAbortAndAddUncommitted();

        RowId lowerRowId = decrement(rowId);

        assertNull(read(lowerRowId, clock.now()));
    }

    @Test
    void testReadingNothingByTxIdWithLowerRowId() {
        RowId higherRowId = new RowId(PARTITION_ID);
        RowId lowerRowId = decrement(higherRowId);

        UUID txId = UUID.randomUUID();

        storage.runConsistently(locker -> {
            addWrite(higherRowId, binaryRow, txId);

            return null;
        });

        assertNull(read(lowerRowId, HybridTimestamp.MAX_VALUE));
    }

    @Test
    void testReadingCorrectWriteIntentByTimestampIfLowerRowIdWriteIntentExists() {
        RowId higherRowId = new RowId(PARTITION_ID);
        RowId lowerRowId = decrement(higherRowId);

        storage.runConsistently(locker -> {
            addWrite(lowerRowId, binaryRow2, newTransactionId());
            addWrite(higherRowId, binaryRow, newTransactionId());

            commitWrite(higherRowId, clock.now());

            return null;
        });

        assertThat(read(higherRowId, clock.now()), is(equalToRow(binaryRow)));
    }

    @Test
    void testReadingCorrectWriteIntentByTimestampIfHigherRowIdWriteIntentExists() {
        RowId higherRowId = new RowId(PARTITION_ID);
        RowId lowerRowId = decrement(higherRowId);

        storage.runConsistently(locker -> {
            addWrite(lowerRowId, binaryRow, newTransactionId());
            addWrite(higherRowId, binaryRow2, newTransactionId());

            return null;
        });

        assertThat(read(lowerRowId, clock.now()), is(equalToRow(binaryRow)));
    }

    @Test
    void testReadingTombstoneIfPreviousCommitExists() {
        RowId rowId = new RowId(PARTITION_ID);

        HybridTimestamp commitTs = clock.now();

        storage.runConsistently(locker -> {
            addWrite(rowId, binaryRow, newTransactionId());
            commitWrite(rowId, commitTs);

            addWrite(rowId, null, newTransactionId());

            return null;
        });

        ReadResult res = storage.read(rowId, clock.now());

        assertNotNull(res);
        assertNull(res.binaryRow());
        assertEquals(commitTs, res.newestCommitTimestamp());
    }

    @Test
    void testReadingTombstoneIfPreviousCommitNotExists() {
        RowId rowId = new RowId(PARTITION_ID);

        storage.runConsistently(locker -> {
            addWrite(rowId, null, newTransactionId());

            return null;
        });

        ReadResult res = storage.read(rowId, clock.now());

        assertNotNull(res);
        assertNull(res.binaryRow());
        assertNull(res.newestCommitTimestamp());
    }

    @Test
    void testScanVersions() {
        RowId rowId = new RowId(PARTITION_ID, 100, 0);

        // Populate storage with several versions for the same row id.
        List<TestValue> values = new ArrayList<>();

        values.add(value);
        values.add(value2);
        values.add(null); // A tombstone.

        for (TestValue value : values) {
            BinaryRow row = value == null ? null : binaryRow(key, value);

            addWrite(rowId, row, newTransactionId());

            commitWrite(rowId, clock.now());
        }

        // Put rows before and after.
        RowId lowRowId = new RowId(PARTITION_ID, 99, 0);
        RowId highRowId = new RowId(PARTITION_ID, 101, 0);

        List.of(lowRowId, highRowId).forEach(newRowId ->  {
            addWrite(newRowId, binaryRow(key, value), newTransactionId());

            commitWrite(newRowId, clock.now());
        });

        // Reverse expected values to simplify comparison - they are returned in reversed order, newest to oldest.
        Collections.reverse(values);

        List<IgniteBiTuple<TestKey, TestValue>> list = storage.runConsistently(locker -> {
            locker.lock(rowId);

            return drainVerifyingRowIdMatch(storage.scanVersions(rowId), rowId);
        });

        assertEquals(values.size(), list.size());

        for (int i = 0; i < list.size(); i++) {
            IgniteBiTuple<TestKey, TestValue> kv = list.get(i);

            if (kv == null) {
                assertThat(values.get(i), is(nullValue()));
            } else {
                assertThat(key, is(kv.getKey()));

                assertThat(values.get(i), is(kv.getValue()));
            }
        }
    }

    private static List<IgniteBiTuple<TestKey, TestValue>> drainVerifyingRowIdMatch(Cursor<ReadResult> cursor, RowId rowId) {
        try (cursor) {
            return cursor.stream()
                    .peek(result -> assertThat(result.rowId(), is(rowId)))
                    .map(BaseMvStoragesTest::unwrap)
                    .collect(toList());
        }
    }

    @ParameterizedTest
    @EnumSource(ScanTimestampProvider.class)
    void testScanWithWriteIntent(ScanTimestampProvider tsProvider) {
        IgniteBiTuple<RowId, HybridTimestamp> rowIdAndCommitTs = addCommittedVersionAndWriteIntent();

        try (PartitionTimestampCursor cursor = storage.scan(tsProvider.scanTimestamp(clock))) {
            assertTrue(cursor.hasNext());

            ReadResult next = cursor.next();

            assertTrue(next.isWriteIntent());

            assertThat(next.rowId(), is(rowIdAndCommitTs.get1()));
            assertThat(next.binaryRow(), is(equalToRow(binaryRow2)));

            BinaryRow committedRow = cursor.committed(rowIdAndCommitTs.get2());

            assertThat(committedRow, is(equalToRow(binaryRow)));
        }
    }

    private IgniteBiTuple<RowId, HybridTimestamp> addCommittedVersionAndWriteIntent() {
        RowId rowId = new RowId(PARTITION_ID);

        HybridTimestamp commitTs = clock.now();

        storage.runConsistently(locker -> {
            addWrite(rowId, binaryRow, newTransactionId());

            commitWrite(rowId, commitTs);

            addWrite(rowId, binaryRow2, newTransactionId());

            return null;
        });

        return new IgniteBiTuple<>(rowId, commitTs);
    }

    @Test
    void testScanVersionsWithWriteIntent() {
        RowId rowId = new RowId(PARTITION_ID, 100, 0);

        addWrite(rowId, binaryRow(key, value), newTransactionId());

        commitWrite(rowId, clock.now());

        addWrite(rowId, binaryRow(key, value2), newTransactionId());

        // Put rows before and after.
        RowId lowRowId = new RowId(PARTITION_ID, 99, 0);
        RowId highRowId = new RowId(PARTITION_ID, 101, 0);

        List.of(lowRowId, highRowId).forEach(newRowId ->  {
            addWrite(newRowId, binaryRow(key, value), newTransactionId());

            commitWrite(newRowId, clock.now());
        });

        List<IgniteBiTuple<TestKey, TestValue>> list = storage.runConsistently(locker -> {
            locker.lock(rowId);

            return drainVerifyingRowIdMatch(storage.scanVersions(rowId), rowId);
        });

        assertEquals(2, list.size());

        for (IgniteBiTuple<TestKey, TestValue> objects : list) {
            assertEquals(key, objects.getKey());
        }

        assertEquals(value2, list.get(0).getValue());

        assertEquals(value, list.get(1).getValue());
    }

    @Test
    void testClosestRowId() {
        RowId rowId0 = new RowId(PARTITION_ID, 1, -1);
        RowId rowId1 = new RowId(PARTITION_ID, 1, 0);
        RowId rowId2 = new RowId(PARTITION_ID, 1, 1);

        addWrite(rowId1, binaryRow, txId);
        addWrite(rowId2, binaryRow2, txId);

        assertEquals(rowId1, storage.closestRowId(rowId0));
        assertEquals(rowId1, storage.closestRowId(rowId0.increment()));

        assertEquals(rowId1, storage.closestRowId(rowId1));

        assertEquals(rowId2, storage.closestRowId(rowId2));

        assertNull(storage.closestRowId(rowId2.increment()));
    }

    @Test
    public void addWriteCommittedAddsCommittedVersion() {
        RowId rowId = new RowId(PARTITION_ID);

        addWriteCommitted(rowId, binaryRow, clock.now());

        // Read with timestamp returns write-intent.
        assertThat(storage.read(rowId, clock.now()).binaryRow(), is(equalToRow(binaryRow)));
    }

    @Test
    public void addWriteCommittedLeavesExistingCommittedVersionsUntouched() {
        RowId rowId = new RowId(PARTITION_ID);

        HybridTimestamp ts1 = clock.now();

        addWriteCommitted(rowId, binaryRow, ts1);
        addWriteCommitted(rowId, binaryRow2, clock.now());

        assertThat(storage.read(rowId, clock.now()).binaryRow(), is(equalToRow(binaryRow2)));
        assertThat(storage.read(rowId, ts1).binaryRow(), is(equalToRow(binaryRow)));
    }

    @Test
    public void addWriteCommittedThrowsIfUncommittedVersionExists() {
        RowId rowId = insert(binaryRow, txId);

        StorageException ex = assertThrows(StorageException.class, () -> addWriteCommitted(rowId, binaryRow2, clock.now()));
        assertThat(ex.getMessage(), allOf(containsString("Write intent exists"), containsString(rowId.toString())));
    }

    @Test
    public void scanVersionsReturnsUncommittedVersionsAsUncommitted() {
        RowId rowId = insert(binaryRow, txId);
        commitWrite(rowId, clock.now());
        addWrite(rowId, binaryRow2, newTransactionId());

        storage.runConsistently(locker -> {
            locker.lock(rowId);

            try (Cursor<ReadResult> cursor = storage.scanVersions(rowId)) {
                ReadResult result = cursor.next();

                assertThat(result.rowId(), is(rowId));
                assertTrue(result.isWriteIntent());
                assertThat(result.commitPartitionId(), is(not(ReadResult.UNDEFINED_COMMIT_PARTITION_ID)));
                assertThat(result.commitTableId(), is(notNullValue()));
                assertThat(result.transactionId(), is(notNullValue()));
                assertThat(result.commitTimestamp(), is(nullValue()));
                assertThat(result.newestCommitTimestamp(), is(nullValue()));
            }

            return null;
        });
    }

    @Test
    public void scanVersionsReturnsCommittedVersionsAsCommitted() {
        RowId rowId = insert(binaryRow, txId);
        commitWrite(rowId, clock.now());

        storage.runConsistently(locker -> {
            locker.lock(rowId);

            try (Cursor<ReadResult> cursor = storage.scanVersions(rowId)) {
                ReadResult result = cursor.next();

                assertThat(result.rowId(), is(rowId));
                assertFalse(result.isWriteIntent());
                assertThat(result.commitPartitionId(), is(ReadResult.UNDEFINED_COMMIT_PARTITION_ID));
                assertThat(result.commitTableId(), is(nullValue()));
                assertThat(result.transactionId(), is(nullValue()));
                assertThat(result.commitTimestamp(), is(notNullValue()));
                assertThat(result.newestCommitTimestamp(), is(nullValue()));
            }

            return null;
        });
    }

    @ParameterizedTest
    @EnumSource(ScanTimestampProvider.class)
    public void scanCursorHasNextReturnsFalseEachTimeAfterExhaustion(ScanTimestampProvider tsProvider) {
        RowId rowId = insert(binaryRow, txId);
        commitWrite(rowId, clock.now());

        try (PartitionTimestampCursor cursor = scan(tsProvider.scanTimestamp(clock))) {
            cursor.next();

            assertFalse(cursor.hasNext());
            //noinspection ConstantConditions
            assertFalse(cursor.hasNext());
        }
    }

    @ParameterizedTest
    @EnumSource(ScanTimestampProvider.class)
    public void scanSeesTombstonesWhenTombstoneIsNotCommitted(ScanTimestampProvider tsProvider) {
        RowId rowId = insert(binaryRow, txId);
        HybridTimestamp commitTs = clock.now();
        commitWrite(rowId, commitTs);

        addWrite(rowId, null, newTransactionId());

        try (PartitionTimestampCursor cursor = scan(tsProvider.scanTimestamp(clock))) {
            assertTrue(cursor.hasNext());

            ReadResult next = cursor.next();

            assertThat(next.rowId(), is(rowId));
            assertNull(next.binaryRow());
            assertEquals(commitTs, next.newestCommitTimestamp());

            assertFalse(cursor.hasNext());
        }
    }

    @ParameterizedTest
    @EnumSource(ScanTimestampProvider.class)
    public void scanDoesNotSeeTombstonesWhenTombstoneIsCommitted(ScanTimestampProvider tsProvider) {
        RowId rowId = insert(binaryRow, txId);
        commitWrite(rowId, clock.now());

        addWrite(rowId, null, newTransactionId());
        commitWrite(rowId, clock.now());

        try (PartitionTimestampCursor cursor = scan(tsProvider.scanTimestamp(clock))) {
            assertFalse(cursor.hasNext());
        }
    }

    @ParameterizedTest
    @EnumSource(ScanTimestampProvider.class)
    void committedMethodCallDoesNotInterfereWithIteratingOverScanCursor(ScanTimestampProvider scanTsProvider) {
        RowId rowId1 = new RowId(PARTITION_ID, 0, 0);
        addWrite(rowId1, binaryRow, txId);

        HybridTimestamp commitTs1 = clock.now();
        commitWrite(rowId1, commitTs1);

        addWrite(new RowId(PARTITION_ID, 0, 1), binaryRow2, txId);

        try (PartitionTimestampCursor cursor = scan(scanTsProvider.scanTimestamp(clock))) {
            cursor.next();

            cursor.committed(commitTs1);

            ReadResult result2 = cursor.next();
            assertThat(result2.binaryRow(), is(equalToRow(binaryRow2)));

            assertFalse(cursor.hasNext());
        }
    }

    @Test
    void groupConfigurationOnEmptyStorageIsNull() {
        assertThat(storage.committedGroupConfiguration(), is(nullValue()));
    }

    @Test
    void groupConfigurationIsSaved() {
        byte[] configToSave = {1, 2, 3};

        storage.runConsistently(locker -> {
            storage.committedGroupConfiguration(configToSave);

            return null;
        });

        byte[] returnedConfig = storage.committedGroupConfiguration();

        assertThat(returnedConfig, is(notNullValue()));
        assertThat(returnedConfig, is(equalTo(configToSave)));
    }

    @Test
    void groupConfigurationIsUpdated() {
        byte[] firstConfig = {1, 2, 3};

        storage.runConsistently(locker -> {
            storage.committedGroupConfiguration(firstConfig);

            return null;
        });

        byte[] secondConfig = {3, 2, 1};

        storage.runConsistently(locker -> {
            storage.committedGroupConfiguration(secondConfig);

            return null;
        });

        byte[] returnedConfig = storage.committedGroupConfiguration();

        assertThat(returnedConfig, is(notNullValue()));
        assertThat(returnedConfig, is(equalTo(secondConfig)));
    }

    @Test
    public void testLease() {
        storage.runConsistently(locker -> {
            long lst0 = 1000;

            long lst1 = 2000;

            storage.updateLease(lst0);

            assertEquals(lst0, storage.leaseStartTime());

            storage.updateLease(lst1);

            assertEquals(lst1, storage.leaseStartTime());

            storage.updateLease(0);

            assertEquals(lst1, storage.leaseStartTime());

            return null;
        });
    }

    @Test
    public void estimatedSizeUsingWriteIntents() {
        assertThat(storage.estimatedSize(), is(0L));

        // Adding a Write Intent should not increase the size.
        addWrite(ROW_ID, binaryRow, newTransactionId());

        assertThat(storage.estimatedSize(), is(0L));

        // Committing a row increases the size.
        commitWrite(ROW_ID, clock.now());

        assertThat(storage.estimatedSize(), is(1L));

        // Adding a Write Intent with a tombstone does not decrease the size.
        addWrite(ROW_ID, null, newTransactionId());

        assertThat(storage.estimatedSize(), is(1L));

        // Committing a tombstone decreases the size.
        commitWrite(ROW_ID, clock.now());

        assertThat(storage.estimatedSize(), is(0L));
    }

    @Test
    public void estimatedSizeUsingCommittedWrites() {
        assertThat(storage.estimatedSize(), is(0L));

        addWriteCommitted(ROW_ID, binaryRow, clock.now());

        assertThat(storage.estimatedSize(), is(1L));

        addWriteCommitted(ROW_ID, null, clock.now());

        assertThat(storage.estimatedSize(), is(0L));
    }

    @Test
    public void estimatedSizeNeverFallsBelowZero() {
        assertThat(storage.estimatedSize(), is(0L));

        addWriteCommitted(ROW_ID, null, clock.now());

        assertThat(storage.estimatedSize(), is(0L));

        addWriteCommitted(ROW_ID, binaryRow, clock.now());

        assertThat(storage.estimatedSize(), is(1L));

        addWriteCommitted(ROW_ID, null, clock.now());

        assertThat(storage.estimatedSize(), is(0L));

        addWriteCommitted(ROW_ID, null, clock.now());

        assertThat(storage.estimatedSize(), is(0L));
    }

    @Test
    public void estimatedSizeShowsLatestRowsNumber() {
        assertThat(storage.estimatedSize(), is(0L));

        var rowId1 = new RowId(PARTITION_ID);
        var rowId2 = new RowId(PARTITION_ID);

        addWriteCommitted(rowId1, binaryRow, clock.now());
        addWriteCommitted(rowId2, binaryRow, clock.now());

        assertThat(storage.estimatedSize(), is(2L));

        // Overwrite an existing row.
        addWriteCommitted(rowId1, binaryRow, clock.now());

        assertThat(storage.estimatedSize(), is(2L));
    }

    @Test
    public void estimatedSizeIsNotAffectedByGarbageTombstones() {
        assertThat(storage.estimatedSize(), is(0L));

        var rowId1 = new RowId(PARTITION_ID);
        var rowId2 = new RowId(PARTITION_ID);

        addWriteCommitted(rowId1, binaryRow, clock.now());

        assertThat(storage.estimatedSize(), is(1L));

        // Remove a non-existing row.
        addWriteCommitted(rowId2, null, clock.now());

        assertThat(storage.estimatedSize(), is(1L));
    }

    @Test
    public void estimatedSizeHandlesTransactionAborts() {
        UUID transactionId = newTransactionId();

        addWriteCommitted(ROW_ID, binaryRow, clock.now());

        addWrite(ROW_ID, binaryRow, transactionId);

        abortWrite(ROW_ID);

        assertThat(storage.estimatedSize(), is(1L));
    }

    /**
     * Returns row id that is lexicographically smaller (by the value of one) than the argument.
     *
     * @param value Row id.
     * @return Row id value minus 1.
     */
    private static RowId decrement(RowId value) {
        long msb = value.mostSignificantBits();
        long lsb = value.leastSignificantBits();

        if (--lsb == Long.MAX_VALUE) {
            if (--msb == Long.MAX_VALUE) {
                throw new IllegalArgumentException();
            }
        }

        return new RowId(value.partitionId(), msb, lsb);
    }

    private enum ScanTimestampProvider {
        NOW {
            @Override
            HybridTimestamp scanTimestamp(HybridClock clock) {
                return clock.now();
            }
        },
        MAX_VALUE {
            @Override
            HybridTimestamp scanTimestamp(HybridClock clock) {
                return HybridTimestamp.MAX_VALUE;
            }
        };

        abstract HybridTimestamp scanTimestamp(HybridClock clock);
    }
}
