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
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
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
import org.apache.ignite.internal.schema.TableRow;
import org.apache.ignite.internal.util.Cursor;
import org.apache.ignite.lang.IgniteBiTuple;
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
    protected final TableRow tableRow = tableRow(key, value);
    private final TestValue value2 = new TestValue(21, "bar2");
    private final TableRow tableRow2 = tableRow(key, value2);
    private final TableRow tableRow3 = tableRow(key, new TestValue(22, "bar3"));

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
     * Tests basic invariants of {@link MvPartitionStorage#addWrite(RowId, TableRow, UUID, UUID, int)}.
     */
    @Test
    public void testAddWrite() {
        RowId rowId = insert(tableRow, txId);

        // Attempt to write from another transaction.
        assertThrows(TxIdMismatchException.class, () -> addWrite(rowId, tableRow, newTransactionId()));

        // Write from the same transaction.
        addWrite(rowId, tableRow, txId);

        // Read with timestamp returns write-intent.
        assertRowMatches(read(rowId, clock.now()), tableRow);

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
        RowId rowId = insert(tableRow(key, value), txId);

        abortWrite(rowId);

        // Aborted row can't be read.
        assertNull(read(rowId, HybridTimestamp.MAX_VALUE));
    }

    /**
     * Tests basic invariants of {@link MvPartitionStorage#commitWrite(RowId, HybridTimestamp)}.
     */
    @Test
    public void testCommitWrite() {
        RowId rowId = insert(tableRow, txId);

        HybridTimestamp tsBefore = clock.now();

        HybridTimestamp tsExact = clock.now();
        commitWrite(rowId, tsExact);

        HybridTimestamp tsAfter = clock.now();

        // Row is invisible at the time before writing.
        assertNull(read(rowId, tsBefore));

        // Row is valid at the time during and after writing.
        assertRowMatches(read(rowId, tsExact), tableRow);
        assertRowMatches(read(rowId, tsAfter), tableRow);

        TestValue newValue = new TestValue(30, "duh");

        UUID newTxId = newTransactionId();

        TableRow newRow = tableRow(key, newValue);
        addWrite(rowId, newRow, newTxId);

        // Same checks, but now there are two different versions.
        assertNull(read(rowId, tsBefore));

        assertRowMatches(read(rowId, HybridTimestamp.MAX_VALUE), newRow);

        assertRowMatches(read(rowId, tsExact), tableRow);
        assertRowMatches(read(rowId, tsAfter), newRow);
        assertRowMatches(read(rowId, clock.now()), newRow);

        // Only latest time behavior changes after commit.
        HybridTimestamp newRowCommitTs = clock.now();
        commitWrite(rowId, newRowCommitTs);

        assertRowMatches(read(rowId, HybridTimestamp.MAX_VALUE), newRow);

        assertRowMatches(read(rowId, tsExact), tableRow);
        assertRowMatches(read(rowId, tsAfter), tableRow);

        assertRowMatches(read(rowId, clock.now()), newRow);

        // Remove.
        UUID removeTxId = newTransactionId();

        addWrite(rowId, null, removeTxId);

        assertNull(read(rowId, tsBefore));

        assertNull(read(rowId, HybridTimestamp.MAX_VALUE));

        assertRowMatches(read(rowId, tsExact), tableRow);
        assertRowMatches(read(rowId, tsAfter), tableRow);
        assertRowMatches(read(rowId, newRowCommitTs), newRow);

        assertNull(read(rowId, clock.now()));

        // Commit remove.
        HybridTimestamp removeTs = clock.now();
        commitWrite(rowId, removeTs);

        assertNull(read(rowId, tsBefore));

        assertNull(read(rowId, HybridTimestamp.MAX_VALUE));
        assertNull(read(rowId, removeTs));
        assertNull(read(rowId, clock.now()));

        assertRowMatches(read(rowId, tsExact), tableRow);
        assertRowMatches(read(rowId, tsAfter), tableRow);
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
        RowId rowId1 = insert(tableRow(key1, value1), txId);
        RowId rowId2 = insert(tableRow(key2, value2), txId);

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

        RowId rowId1 = insert(tableRow(new TestKey(1, "1"), value1), txId);
        commitWrite(rowId1, clock.now());

        RowId rowId2 = insert(tableRow(new TestKey(2, "2"), value2), txId);
        commitWrite(rowId2, clock.now());

        try (PartitionTimestampCursor cursor = scan(HybridTimestamp.MAX_VALUE)) {
            assertTrue(cursor.hasNext());
            assertTrue(cursor.hasNext());

            List<TestValue> res = new ArrayList<>();

            res.add(value(cursor.next().tableRow()));

            assertTrue(cursor.hasNext());
            assertTrue(cursor.hasNext());

            res.add(value(cursor.next().tableRow()));

            assertFalse(cursor.hasNext());
            assertFalse(cursor.hasNext());

            assertThrows(NoSuchElementException.class, () -> cursor.next());

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
        TableRow tableRow11 = tableRow(key1, value11);
        TableRow tableRow12 = tableRow(key1, value12);

        addWrite(rowId1, tableRow11, txId);
        HybridTimestamp commitTs1 = clock.now();
        commitWrite(rowId1, commitTs1);

        addWrite(rowId1, tableRow12, newTransactionId());

        TestKey key2 = new TestKey(2, "2");
        TableRow tableRow21 = tableRow(key2, value21);
        TableRow tableRow22 = tableRow(key2, value22);

        addWrite(rowId2, tableRow21, txId);
        HybridTimestamp commitTs2 = clock.now();
        commitWrite(rowId2, commitTs2);

        addWrite(rowId2, tableRow22, newTransactionId());

        try (PartitionTimestampCursor cursor = scan(clock.now())) {
            assertThrows(IllegalStateException.class, () -> cursor.committed(commitTs1));

            assertTrue(cursor.hasNext());
            while (cursor.hasNext()) {
                assertTrue(cursor.hasNext());

                ReadResult res = cursor.next();

                assertNotNull(res);
                assertTrue(res.isWriteIntent());
                assertFalse(res.isEmpty());

                TableRow expectedRow1;
                TableRow expectedRow2;
                HybridTimestamp commitTs;

                TestKey readKey = key(res.tableRow());

                if (readKey.equals(key1)) {
                    expectedRow1 = tableRow11;
                    expectedRow2 = tableRow12;
                    commitTs = commitTs1;
                } else {
                    expectedRow1 = tableRow21;
                    expectedRow2 = tableRow22;
                    commitTs = commitTs2;
                }

                assertRowMatches(res.tableRow(), expectedRow2);

                TableRow previousRow = cursor.committed(commitTs);

                assertNotNull(previousRow);
                assertRowMatches(previousRow, expectedRow1);
            }

            assertFalse(cursor.hasNext());
            assertFalse(cursor.hasNext());

            assertThrows(NoSuchElementException.class, () -> cursor.next());

            assertThrows(IllegalStateException.class, () -> cursor.committed(commitTs1));
        }
    }

    private List<TestValue> convert(PartitionTimestampCursor cursor) {
        try (cursor) {
            return cursor.stream()
                    .map((ReadResult rs) -> BaseMvStoragesTest.value(rs.tableRow()))
                    .sorted(Comparator.nullsFirst(Comparator.naturalOrder()))
                    .collect(toList());
        }
    }

    @Test
    void readOfUncommittedRowReturnsTheRow() {
        RowId rowId = insert(tableRow, txId);

        ReadResult foundResult = storage.read(rowId, HybridTimestamp.MAX_VALUE);

        assertThat(foundResult.rowId(), is(rowId));
        assertRowMatches(foundResult.tableRow(), tableRow);
    }

    @Test
    void readOfCommittedRowReturnsTheRow() {
        RowId rowId = insert(tableRow, txId);
        commitWrite(rowId, clock.now());

        ReadResult foundResult = storage.read(rowId, HybridTimestamp.MAX_VALUE);

        assertThat(foundResult.rowId(), is(rowId));
        assertRowMatches(foundResult.tableRow(), tableRow);
    }

    @Test
    void readsUncommittedVersionEvenWhenThereIsCommittedVersionBeforeIt() {
        RowId rowId1 = insert(tableRow, txId);
        commitWrite(rowId1, clock.now());

        RowId rowId2 = insert(tableRow2, txId);

        ReadResult foundResult = storage.read(rowId2, HybridTimestamp.MAX_VALUE);

        assertThat(foundResult.rowId(), is(rowId2));
        assertRowMatches(foundResult.tableRow(), tableRow2);
    }

    @Test
    void readsCommittedVersionEvenWhenThereIsCommittedVersionBeforeIt() {
        RowId rowId1 = insert(tableRow, txId);
        commitWrite(rowId1, clock.now());

        RowId rowId2 = insert(tableRow2, txId);
        commitWrite(rowId2, clock.now());

        ReadResult foundResult = storage.read(rowId2, HybridTimestamp.MAX_VALUE);

        assertThat(foundResult.rowId(), is(rowId2));
        assertRowMatches(foundResult.tableRow(), tableRow2);
    }

    @Test
    void readByExactlyCommitTimestampFindsRow() {
        RowId rowId = insert(tableRow, txId);
        HybridTimestamp commitTimestamp = clock.now();
        commitWrite(rowId, commitTimestamp);

        TableRow foundRow = read(rowId, commitTimestamp);

        assertRowMatches(foundRow, tableRow);
    }

    @Test
    void readByTimestampAfterCommitTimestampFindsRow() {
        RowId rowId = insert(tableRow, txId);
        HybridTimestamp commitTimestamp = clock.now();
        commitWrite(rowId, commitTimestamp);

        HybridTimestamp afterCommit = clock.now();
        TableRow foundRow = read(rowId, afterCommit);

        assertRowMatches(foundRow, tableRow);
    }

    @Test
    void readByTimestampBeforeFirstVersionCommitTimestampFindsNothing() {
        HybridTimestamp beforeCommit = clock.now();

        RowId rowId = insert(tableRow, txId);
        HybridTimestamp commitTimestamp = clock.now();
        commitWrite(rowId, commitTimestamp);

        TableRow foundRow = read(rowId, beforeCommit);

        assertThat(foundRow, is(nullValue()));
    }

    @Test
    void readByTimestampOfLastVersionFindsLastVersion() {
        RowId rowId = insert(tableRow, txId);
        HybridTimestamp firstVersionTs = clock.now();
        commitWrite(rowId, firstVersionTs);

        addWrite(rowId, tableRow2, newTransactionId());
        HybridTimestamp secondVersionTs = clock.now();
        commitWrite(rowId, secondVersionTs);

        TableRow foundRow = read(rowId, secondVersionTs);

        assertRowMatches(foundRow, tableRow2);
    }

    @Test
    void readByTimestampOfPreviousVersionFindsPreviousVersion() {
        RowId rowId = insert(tableRow, txId);
        HybridTimestamp firstVersionTs = clock.now();
        commitWrite(rowId, firstVersionTs);

        addWrite(rowId, tableRow2, newTransactionId());
        commitWrite(rowId, clock.now());

        TableRow foundRow = read(rowId, firstVersionTs);

        assertRowMatches(foundRow, tableRow);
    }

    @Test
    void readByTimestampBetweenVersionsFindsPreviousVersion() {
        RowId rowId = insert(tableRow, txId);
        HybridTimestamp firstVersionTs = clock.now();
        commitWrite(rowId, firstVersionTs);

        HybridTimestamp tsInBetween = clock.now();

        addWrite(rowId, tableRow2, newTransactionId());
        commitWrite(rowId, clock.now());

        TableRow foundRow = read(rowId, tsInBetween);

        assertRowMatches(foundRow, tableRow);
    }

    @Test
    void readByTimestampAfterWriteFindsUncommittedVersion() {
        RowId rowId = new RowId(PARTITION_ID);

        addWrite(rowId, tableRow, newTransactionId());

        HybridTimestamp latestTs = clock.now();
        TableRow foundRow = read(rowId, latestTs);

        assertRowMatches(foundRow, tableRow);
    }

    @Test
    void readByTimestampAfterCommitAndWriteFindsUncommittedVersion() {
        RowId rowId = insert(tableRow, newTransactionId());
        commitWrite(rowId, clock.now());

        addWrite(rowId, tableRow2, newTransactionId());

        HybridTimestamp latestTs = clock.now();
        TableRow foundRow = read(rowId, latestTs);

        assertRowMatches(foundRow, tableRow2);
    }

    @Test
    void addWriteWithDifferentTxIdThrows() {
        RowId rowId = insert(tableRow, txId);

        assertThrows(TxIdMismatchException.class, () -> addWrite(rowId, tableRow2, newTransactionId()));
    }

    @Test
    void secondUncommittedWriteWithSameTxIdReplacesExistingUncommittedWrite() {
        RowId rowId = insert(tableRow, txId);

        addWrite(rowId, tableRow2, txId);

        TableRow foundRow = read(rowId, HybridTimestamp.MAX_VALUE);

        assertRowMatches(foundRow, tableRow2);
    }

    @Test
    void addWriteReturnsUncommittedVersionIfItExists() {
        RowId rowId = insert(tableRow, txId);

        TableRow returnedRow = addWrite(rowId, tableRow2, txId);

        assertRowMatches(returnedRow, tableRow);
    }

    @Test
    void addWriteReturnsNullIfNoUncommittedVersionExists() {
        RowId rowId = insert(tableRow, newTransactionId());
        commitWrite(rowId, clock.now());

        TableRow returnedRow = addWrite(rowId, tableRow2, txId);

        assertThat(returnedRow, is(nullValue()));
    }

    @Test
    void addWriteCreatesUncommittedVersion() {
        RowId rowId = insert(tableRow, txId);

        ReadResult readResult = storage.read(rowId, clock.now());

        assertTrue(readResult.isWriteIntent());
    }

    @Test
    void afterRemovalReadWithTxIdFindsNothing() {
        RowId rowId = insert(tableRow, newTransactionId());
        commitWrite(rowId, clock.now());

        addWrite(rowId, null, txId);

        TableRow foundRow = read(rowId, HybridTimestamp.MAX_VALUE);

        assertThat(foundRow, is(nullValue()));
    }

    @Test
    void afterRemovalReadByLatestTimestampFindsNothing() {
        RowId rowId = insert(tableRow, newTransactionId());
        commitWrite(rowId, clock.now());

        addWrite(rowId, null, newTransactionId());
        commitWrite(rowId, clock.now());

        TableRow foundRow = read(rowId, clock.now());

        assertThat(foundRow, is(nullValue()));
    }

    @Test
    void afterRemovalPreviousVersionRemainsAccessibleByTimestamp() {
        RowId rowId = insert(tableRow, newTransactionId());
        HybridTimestamp firstTimestamp = clock.now();
        commitWrite(rowId, firstTimestamp);

        addWrite(rowId, null, newTransactionId());
        commitWrite(rowId, clock.now());

        TableRow foundRow = read(rowId, firstTimestamp);

        assertRowMatches(foundRow, tableRow);
    }

    @Test
    void removalReturnsUncommittedRowVersionIfItExists() {
        RowId rowId = insert(tableRow, txId);

        TableRow rowFromRemoval = addWrite(rowId, null, txId);

        assertRowMatches(rowFromRemoval, tableRow);
    }

    @Test
    void removalReturnsNullIfNoUncommittedVersionExists() {
        RowId rowId = insert(tableRow, newTransactionId());
        commitWrite(rowId, clock.now());

        TableRow rowFromRemoval = addWrite(rowId, null, newTransactionId());

        assertThat(rowFromRemoval, is(nullValue()));
    }

    @Test
    void commitWriteCommitsWriteIntentVersion() {
        RowId rowId = insert(tableRow, txId);
        commitWrite(rowId, clock.now());

        ReadResult readResult = storage.read(rowId, clock.now());

        assertFalse(readResult.isWriteIntent());
    }

    @Test
    void commitWriteMakesVersionAvailableToReadByTimestamp() {
        RowId rowId = insert(tableRow, txId);

        commitWrite(rowId, clock.now());

        TableRow foundRow = read(rowId, clock.now());

        assertRowMatches(foundRow, tableRow);
    }

    @Test
    void commitAndAbortWriteNoOpIfNoUncommittedVersionExists() {
        RowId rowId = insert(tableRow, newTransactionId());
        commitWrite(rowId, clock.now());

        abortWrite(rowId);

        assertRowMatches(read(rowId, HybridTimestamp.MAX_VALUE), tableRow);

        commitWrite(rowId, clock.now());

        assertRowMatches(read(rowId, HybridTimestamp.MAX_VALUE), tableRow);
    }

    @Test
    void abortWriteRemovesUncommittedVersion() {
        RowId rowId = insert(tableRow, newTransactionId());
        commitWrite(rowId, clock.now());

        addWrite(rowId, tableRow2, txId);

        abortWrite(rowId);

        TableRow foundRow = read(rowId, HybridTimestamp.MAX_VALUE);

        assertRowMatches(foundRow, tableRow);
    }

    @Test
    void abortOfInsertMakesRowNonExistentForReadByTimestamp() {
        RowId rowId = insert(tableRow, newTransactionId());

        abortWrite(rowId);

        TableRow foundRow = read(rowId, clock.now());

        assertThat(foundRow, is(nullValue()));
    }

    @Test
    void abortOfInsertMakesRowNonExistentForReadWithTxId() {
        RowId rowId = new RowId(PARTITION_ID);

        TableRow foundRow = read(rowId, HybridTimestamp.MAX_VALUE);

        assertThat(foundRow, is(nullValue()));
    }

    @Test
    void abortWriteReturnsTheRemovedVersion() {
        RowId rowId = insert(tableRow, txId);

        TableRow returnedRow = abortWrite(rowId);

        assertRowMatches(returnedRow, tableRow);
    }

    @Test
    void readByTimestampWorksCorrectlyAfterCommitAndAbortFollowedByUncommittedWrite() {
        RowId rowId = commitAbortAndAddUncommitted();

        ReadResult foundResult = storage.read(rowId, clock.now());

        // We see the uncommitted row.
        assertThat(foundResult.rowId(), is(rowId));
        assertRowMatches(foundResult.tableRow(), tableRow3);
    }

    @Test
    void readByTimestampBeforeAndAfterUncommittedWrite() {
        RowId rowId = new RowId(PARTITION_ID);

        HybridTimestamp commitTs = clock.now();

        storage.runConsistently(() -> {
            addWrite(rowId, tableRow, txId);

            commitWrite(rowId, commitTs);

            return null;
        });

        UUID txId2 = UUID.randomUUID();

        storage.runConsistently(() -> {
            addWrite(rowId, tableRow2, txId2);

            return null;
        });

        ReadResult res = storage.read(rowId, commitTs);

        assertNotNull(res);

        assertNull(res.transactionId());
        assertNull(res.commitTableId());
        assertEquals(ReadResult.UNDEFINED_COMMIT_PARTITION_ID, res.commitPartitionId());
        assertRowMatches(res.tableRow(), tableRow);

        res = storage.read(rowId, clock.now());

        assertNotNull(res);

        assertEquals(txId2, res.transactionId());
        assertEquals(COMMIT_TABLE_ID, res.commitTableId());
        assertEquals(PARTITION_ID, res.commitPartitionId());
        assertRowMatches(res.tableRow(), tableRow2);
    }

    private RowId commitAbortAndAddUncommitted() {
        return storage.runConsistently(() -> {
            RowId rowId = new RowId(PARTITION_ID);

            storage.addWrite(rowId, tableRow, txId, UUID.randomUUID(), 0);
            commitWrite(rowId, clock.now());

            addWrite(rowId, tableRow2, newTransactionId());
            storage.abortWrite(rowId);

            addWrite(rowId, tableRow3, newTransactionId());

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
            assertRowMatches(result.tableRow(), tableRow3);

            assertFalse(cursor.hasNext());
        }
    }

    @Test
    void readByTimestampWorksCorrectlyIfNoUncommittedValueExists() {
        RowId rowId = insert(tableRow, txId);

        TableRow foundRow = read(rowId, clock.now());

        assertRowMatches(foundRow, tableRow);
    }

    /**
     * Tests that changed {@link MvPartitionStorage#lastAppliedIndex()} can be successfully read and that it's returned from
     * {@link MvPartitionStorage#persistedIndex()} after the {@link MvPartitionStorage#flush()}.
     */
    @Test
    void testAppliedIndex() {
        storage.runConsistently(() -> {
            assertEquals(0, storage.lastAppliedIndex());
            assertEquals(0, storage.lastAppliedTerm());
            assertEquals(0, storage.persistedIndex());

            storage.lastApplied(1, 1);

            assertEquals(1, storage.lastAppliedIndex());
            assertEquals(1, storage.lastAppliedTerm());
            assertThat(storage.persistedIndex(), is(lessThanOrEqualTo(1L)));

            return null;
        });

        CompletableFuture<Void> flushFuture = storage.flush();

        assertThat(flushFuture, willCompleteSuccessfully());

        assertEquals(1, storage.persistedIndex());
    }

    @Test
    void testReadWithinBeforeAndAfterTwoCommits() {
        HybridTimestamp before = clock.now();

        RowId rowId = new RowId(PARTITION_ID);

        HybridTimestamp first = clock.now();

        storage.runConsistently(() -> {
            addWrite(rowId, tableRow, newTransactionId());

            commitWrite(rowId, first);
            return null;
        });

        HybridTimestamp betweenCommits = clock.now();

        HybridTimestamp second = clock.now();

        storage.runConsistently(() -> {
            addWrite(rowId, tableRow2, newTransactionId());

            commitWrite(rowId, second);
            return null;
        });

        storage.runConsistently(() -> {
            addWrite(rowId, tableRow3, newTransactionId());

            return null;
        });

        HybridTimestamp after = clock.now();

        // Read before commits.
        ReadResult res = storage.read(rowId, before);
        assertNull(res.tableRow());

        // Read at exact time of first commit.
        res = storage.read(rowId, first);

        assertNotNull(res);
        assertNull(res.newestCommitTimestamp());
        assertRowMatches(res.tableRow(), tableRow);

        // Read between two commits.
        res = storage.read(rowId, betweenCommits);

        assertNotNull(res);
        assertNull(res.newestCommitTimestamp());
        assertRowMatches(res.tableRow(), tableRow);

        // Read at exact time of second commit.
        res = storage.read(rowId, second);

        assertNotNull(res);
        assertNull(res.newestCommitTimestamp());
        assertRowMatches(res.tableRow(), tableRow2);

        // Read after second commit (write intent).
        res = storage.read(rowId, after);

        assertNotNull(res);
        assertNotNull(res.newestCommitTimestamp());
        assertEquals(second, res.newestCommitTimestamp());
        assertRowMatches(res.tableRow(), tableRow3);
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

        storage.runConsistently(() -> {
            addWrite(higherRowId, tableRow, txId);

            return null;
        });

        assertNull(read(lowerRowId, HybridTimestamp.MAX_VALUE));
    }

    @Test
    void testReadingCorrectWriteIntentByTimestampIfLowerRowIdWriteIntentExists() {
        RowId higherRowId = new RowId(PARTITION_ID);
        RowId lowerRowId = decrement(higherRowId);

        storage.runConsistently(() -> {
            addWrite(lowerRowId, tableRow2, newTransactionId());
            addWrite(higherRowId, tableRow, newTransactionId());

            commitWrite(higherRowId, clock.now());

            return null;
        });

        assertRowMatches(read(higherRowId, clock.now()), tableRow);
    }

    @Test
    void testReadingCorrectWriteIntentByTimestampIfHigherRowIdWriteIntentExists() {
        RowId higherRowId = new RowId(PARTITION_ID);
        RowId lowerRowId = decrement(higherRowId);

        storage.runConsistently(() -> {
            addWrite(lowerRowId, tableRow, newTransactionId());
            addWrite(higherRowId, tableRow2, newTransactionId());

            return null;
        });

        assertRowMatches(read(lowerRowId, clock.now()), tableRow);
    }

    @Test
    void testReadingTombstoneIfPreviousCommitExists() {
        RowId rowId = new RowId(PARTITION_ID);

        HybridTimestamp commitTs = clock.now();

        storage.runConsistently(() -> {
            addWrite(rowId, tableRow, newTransactionId());
            commitWrite(rowId, commitTs);

            addWrite(rowId, null, newTransactionId());

            return null;
        });

        ReadResult res = storage.read(rowId, clock.now());

        assertNotNull(res);
        assertNull(res.tableRow());
        assertEquals(commitTs, res.newestCommitTimestamp());
    }

    @Test
    void testReadingTombstoneIfPreviousCommitNotExists() {
        RowId rowId = new RowId(PARTITION_ID);

        storage.runConsistently(() -> {
            addWrite(rowId, null, newTransactionId());

            return null;
        });

        ReadResult res = storage.read(rowId, clock.now());

        assertNotNull(res);
        assertNull(res.tableRow());
        assertNull(res.newestCommitTimestamp());
    }

    @Test
    void testScanVersions() {
        RowId rowId = new RowId(PARTITION_ID, 100, 0);

        // Populate storage with several versions for the same row id.
        List<TestValue> values = new ArrayList<>(List.of(value, value2));

        for (TestValue value : values) {
            addWrite(rowId, tableRow(key, value), newTransactionId());

            commitWrite(rowId, clock.now());
        }

        // Put rows before and after.
        RowId lowRowId = new RowId(PARTITION_ID, 99, 0);
        RowId highRowId = new RowId(PARTITION_ID, 101, 0);

        List.of(lowRowId, highRowId).forEach(newRowId ->  {
            addWrite(newRowId, tableRow(key, value), newTransactionId());

            commitWrite(newRowId, clock.now());
        });

        // Reverse expected values to simplify comparison - they are returned in reversed order, newest to oldest.
        Collections.reverse(values);

        List<IgniteBiTuple<TestKey, TestValue>> list = drainVerifyingRowIdMatch(storage.scanVersions(rowId), rowId);

        assertEquals(values.size(), list.size());

        for (int i = 0; i < list.size(); i++) {
            IgniteBiTuple<TestKey, TestValue> kv = list.get(i);

            assertEquals(key, kv.getKey());

            assertEquals(values.get(i), kv.getValue());
        }
    }

    private static List<IgniteBiTuple<TestKey, TestValue>> drainVerifyingRowIdMatch(Cursor<ReadResult> cursor, RowId rowId) {
        try (Cursor<ReadResult> c = cursor) {
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
            assertRowMatches(next.tableRow(), tableRow2);

            TableRow committedRow = cursor.committed(rowIdAndCommitTs.get2());

            assertRowMatches(committedRow, tableRow);
        }
    }

    private IgniteBiTuple<RowId, HybridTimestamp> addCommittedVersionAndWriteIntent() {
        RowId rowId = new RowId(PARTITION_ID);

        HybridTimestamp commitTs = clock.now();

        storage.runConsistently(() -> {
            addWrite(rowId, tableRow, newTransactionId());

            commitWrite(rowId, commitTs);

            addWrite(rowId, tableRow2, newTransactionId());

            return null;
        });

        return new IgniteBiTuple<>(rowId, commitTs);
    }

    @Test
    void testScanVersionsWithWriteIntent() {
        RowId rowId = new RowId(PARTITION_ID, 100, 0);

        addWrite(rowId, tableRow(key, value), newTransactionId());

        commitWrite(rowId, clock.now());

        addWrite(rowId, tableRow(key, value2), newTransactionId());

        // Put rows before and after.
        RowId lowRowId = new RowId(PARTITION_ID, 99, 0);
        RowId highRowId = new RowId(PARTITION_ID, 101, 0);

        List.of(lowRowId, highRowId).forEach(newRowId ->  {
            addWrite(newRowId, tableRow(key, value), newTransactionId());

            commitWrite(newRowId, clock.now());
        });

        List<IgniteBiTuple<TestKey, TestValue>> list = drainVerifyingRowIdMatch(storage.scanVersions(rowId), rowId);

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

        addWrite(rowId1, tableRow, txId);
        addWrite(rowId2, tableRow2, txId);

        assertEquals(rowId1, storage.closestRowId(rowId0));
        assertEquals(rowId1, storage.closestRowId(rowId0.increment()));

        assertEquals(rowId1, storage.closestRowId(rowId1));

        assertEquals(rowId2, storage.closestRowId(rowId2));

        assertNull(storage.closestRowId(rowId2.increment()));
    }

    @Test
    public void addWriteCommittedAddsCommittedVersion() {
        RowId rowId = new RowId(PARTITION_ID);

        addWriteCommitted(rowId, tableRow, clock.now());

        // Read with timestamp returns write-intent.
        assertRowMatches(storage.read(rowId, clock.now()).tableRow(), tableRow);
    }

    @Test
    public void addWriteCommittedLeavesExistingCommittedVersionsUntouched() {
        RowId rowId = new RowId(PARTITION_ID);

        HybridTimestamp ts1 = clock.now();

        addWriteCommitted(rowId, tableRow, ts1);
        addWriteCommitted(rowId, tableRow2, clock.now());

        assertRowMatches(storage.read(rowId, clock.now()).tableRow(), tableRow2);
        assertRowMatches(storage.read(rowId, ts1).tableRow(), tableRow);
    }

    @Test
    public void addWriteCommittedThrowsIfUncommittedVersionExists() {
        RowId rowId = insert(tableRow, txId);

        StorageException ex = assertThrows(StorageException.class, () -> addWriteCommitted(rowId, tableRow2, clock.now()));
        assertThat(ex.getMessage(), containsString("Write intent exists for " + rowId));
    }

    @Test
    public void scanVersionsReturnsUncommittedVersionsAsUncommitted() {
        RowId rowId = insert(tableRow, txId);
        commitWrite(rowId, clock.now());
        addWrite(rowId, tableRow2, newTransactionId());

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
    }

    @Test
    public void scanVersionsReturnsCommittedVersionsAsCommitted() {
        RowId rowId = insert(tableRow, txId);
        commitWrite(rowId, clock.now());

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
    }

    @ParameterizedTest
    @EnumSource(ScanTimestampProvider.class)
    public void scanCursorHasNextReturnsFalseEachTimeAfterExhaustion(ScanTimestampProvider tsProvider) {
        RowId rowId = insert(tableRow, txId);
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
        RowId rowId = insert(tableRow, txId);
        HybridTimestamp commitTs = clock.now();
        commitWrite(rowId, commitTs);

        addWrite(rowId, null, newTransactionId());

        try (PartitionTimestampCursor cursor = scan(tsProvider.scanTimestamp(clock))) {
            assertTrue(cursor.hasNext());

            ReadResult next = cursor.next();

            assertThat(next.rowId(), is(rowId));
            assertNull(next.tableRow());
            assertEquals(commitTs, next.newestCommitTimestamp());

            assertFalse(cursor.hasNext());
        }
    }

    @ParameterizedTest
    @EnumSource(ScanTimestampProvider.class)
    public void scanDoesNotSeeTombstonesWhenTombstoneIsCommitted(ScanTimestampProvider tsProvider) {
        RowId rowId = insert(tableRow, txId);
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
        RowId rowId1 = insert(tableRow, txId, new UUID(0, 0));
        HybridTimestamp commitTs1 = clock.now();
        commitWrite(rowId1, commitTs1);

        insert(tableRow2, txId, new UUID(0, 1));

        try (PartitionTimestampCursor cursor = scan(scanTsProvider.scanTimestamp(clock))) {
            cursor.next();

            cursor.committed(commitTs1);

            ReadResult result2 = cursor.next();
            assertRowMatches(result2.tableRow(), tableRow2);

            assertFalse(cursor.hasNext());
        }
    }

    @Test
    void groupConfigurationOnEmptyStorageIsNull() {
        assertThat(storage.committedGroupConfiguration(), is(nullValue()));
    }

    @Test
    void groupConfigurationIsSaved() {
        RaftGroupConfiguration configToSave = new RaftGroupConfiguration(
                List.of("peer1", "peer2"),
                List.of("learner1", "learner2"),
                List.of("old-peer1", "old-peer2"),
                List.of("old-learner1", "old-learner2")
        );

        storage.runConsistently(() -> {
            storage.committedGroupConfiguration(configToSave);

            return null;
        });

        RaftGroupConfiguration returnedConfig = storage.committedGroupConfiguration();

        assertThat(returnedConfig, is(notNullValue()));
        assertThat(returnedConfig, is(equalTo(configToSave)));
    }

    @Test
    void groupConfigurationIsUpdated() {
        RaftGroupConfiguration firstConfig = new RaftGroupConfiguration(
                List.of("peer1", "peer2"),
                List.of("learner1", "learner2"),
                List.of("old-peer1", "old-peer2"),
                List.of("old-learner1", "old-learner2")
        );

        storage.runConsistently(() -> {
            storage.committedGroupConfiguration(firstConfig);

            return null;
        });

        RaftGroupConfiguration secondConfig = new RaftGroupConfiguration(
                List.of("peer3", "peer4"),
                List.of("learner3", "learner4"),
                List.of("old-peer3", "old-peer4"),
                List.of("old-learner3", "old-learner4")
        );

        storage.runConsistently(() -> {
            storage.committedGroupConfiguration(secondConfig);

            return null;
        });

        RaftGroupConfiguration returnedConfig = storage.committedGroupConfiguration();

        assertThat(returnedConfig, is(notNullValue()));
        assertThat(returnedConfig, is(equalTo(secondConfig)));
    }

    /**
     * Returns row id that is lexicographically smaller (by the value of one) than the argument.
     *
     * @param value Row id.
     * @return Row id value minus 1.
     */
    private RowId decrement(RowId value) {
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
