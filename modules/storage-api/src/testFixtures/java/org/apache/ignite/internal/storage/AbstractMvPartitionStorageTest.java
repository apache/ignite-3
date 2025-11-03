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
import static org.apache.ignite.internal.schema.BinaryRowMatcher.isRow;
import static org.apache.ignite.internal.storage.AbortResultMatcher.equalsToAbortResult;
import static org.apache.ignite.internal.storage.AddWriteCommittedResultMatcher.equalsToAddWriteCommittedResult;
import static org.apache.ignite.internal.storage.AddWriteResultMatcher.equalsToAddWriteResult;
import static org.apache.ignite.internal.storage.CommitResultMatcher.equalsToCommitResult;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
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
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.lang.IgniteBiTuple;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.storage.lease.LeaseInfo;
import org.apache.ignite.internal.testframework.IgniteTestUtils;
import org.apache.ignite.internal.util.Cursor;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

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
        assertThat(
                addWrite(rowId, binaryRow, newTransactionId()),
                equalsToAddWriteResult(AddWriteResult.txMismatch(txId, null))
        );

        // Write from the same transaction.
        assertThat(
                addWrite(rowId, binaryRow, txId),
                equalsToAddWriteResult(AddWriteResult.success(binaryRow))
        );

        // Read with timestamp returns write-intent.
        assertThat(read(rowId, clock.now()), isRow(binaryRow));

        // Remove write.
        addWrite(rowId, null, txId);

        // Removed row can't be read.
        assertNull(read(rowId, HybridTimestamp.MAX_VALUE));

        // Remove write once again.
        addWrite(rowId, null, txId);

        // Still can't be read.
        assertNull(read(rowId, HybridTimestamp.MAX_VALUE));
    }

    @Test
    void testAddWriteForEmptyVersionChain() {
        assertThat(
                addWrite(new RowId(PARTITION_ID), binaryRow, txId),
                equalsToAddWriteResult(AddWriteResult.success(null))
        );
    }

    @Test
    void testAddWriteForVersionChainWithCommittedRowVersion() {
        RowId rowId = insert(binaryRow, txId);
        commitWrite(rowId, clock.now(), txId);

        assertThat(
                addWrite(rowId, binaryRow2, newTransactionId()),
                equalsToAddWriteResult(AddWriteResult.success(null))
        );
    }

    @Test
    void testAddWriteReplaceWriteIntent() {
        RowId rowId = insert(binaryRow, txId);

        assertThat(
                addWrite(rowId, binaryRow2, txId),
                equalsToAddWriteResult(AddWriteResult.success(binaryRow))
        );
    }

    @Test
    void testAddWriteReplaceWriteIntentWithCommittedRowVersion() {
        RowId rowId = insert(binaryRow, txId);
        commitWrite(rowId, clock.now(), txId);

        UUID newTxId = newTransactionId();

        addWrite(rowId, binaryRow2, newTxId);

        assertThat(
                addWrite(rowId, binaryRow3, newTxId),
                equalsToAddWriteResult(AddWriteResult.success(binaryRow2))
        );
    }

    @Test
    void testAddWriteWithDifferentTransaction() {
        RowId rowId = insert(binaryRow, txId);

        assertThat(
                addWrite(rowId, binaryRow2, newTransactionId()),
                equalsToAddWriteResult(AddWriteResult.txMismatch(txId, null))
        );
    }

    @Test
    void testAddWriteWithDifferentTransactionAndCommittedRow() {
        RowId rowId = insert(binaryRow, txId);

        HybridTimestamp commitTimestamp = clock.now();
        commitWrite(rowId, commitTimestamp, txId);

        UUID newTxId = newTransactionId();
        addWrite(rowId, binaryRow2, newTxId);

        assertThat(
                addWrite(rowId, binaryRow2, newTransactionId()),
                equalsToAddWriteResult(AddWriteResult.txMismatch(newTxId, commitTimestamp))
        );
    }

    @Test
    void testAddWriteWithDifferentTransactionAndMultipleCommittedRow() {
        RowId rowId = insert(binaryRow, txId);
        commitWrite(rowId, clock.now(), txId);

        UUID newTxId = newTransactionId();
        HybridTimestamp newCommitTimestamp = clock.now();
        addWrite(rowId, binaryRow2, newTxId);
        commitWrite(rowId, newCommitTimestamp, newTxId);

        UUID newTxIdLatest = newTransactionId();
        addWrite(rowId, binaryRow3, newTxIdLatest);

        assertThat(
                addWrite(rowId, binaryRow3, newTransactionId()),
                equalsToAddWriteResult(AddWriteResult.txMismatch(newTxIdLatest, newCommitTimestamp))
        );
    }

    @Test
    void testAddWriteCommittedForEmptyVersionChain() {
        assertThat(
                addWriteCommitted(new RowId(PARTITION_ID), binaryRow, clock.now()),
                equalsToAddWriteCommittedResult(AddWriteCommittedResult.success())
        );
    }

    @Test
    void testAddWriteCommittedWithExistsCommittedVersion() {
        var rowId = new RowId(PARTITION_ID);

        addWriteCommitted(rowId, binaryRow, clock.now());

        assertThat(
                addWriteCommitted(rowId, binaryRow2, clock.now()),
                equalsToAddWriteCommittedResult(AddWriteCommittedResult.success())
        );
    }

    @Test
    void testAddWriteCommittedWithWriteIntent() {
        var rowId = new RowId(PARTITION_ID);

        addWrite(rowId, binaryRow, txId);

        assertThat(
                addWriteCommitted(rowId, binaryRow2, clock.now()),
                equalsToAddWriteCommittedResult(AddWriteCommittedResult.writeIntentExists(txId, null))
        );
    }

    @Test
    void testAddWriteCommittedWithWriteIntentAndCommittedVersion() {
        var rowId = new RowId(PARTITION_ID);
        HybridTimestamp commitTimestamp = clock.now();

        addWriteCommitted(rowId, binaryRow, commitTimestamp);
        addWrite(rowId, binaryRow2, txId);

        assertThat(
                addWriteCommitted(rowId, binaryRow3, clock.now()),
                equalsToAddWriteCommittedResult(AddWriteCommittedResult.writeIntentExists(txId, commitTimestamp))
        );
    }

    @Test
    void testAddWriteCommittedWithWriteIntentAndMultipleCommittedVersion() {
        var rowId = new RowId(PARTITION_ID);

        addWriteCommitted(rowId, binaryRow, clock.now());

        HybridTimestamp newCommitTimestamp = clock.now();
        addWriteCommitted(rowId, binaryRow2, newCommitTimestamp);

        addWrite(rowId, binaryRow3, txId);

        assertThat(
                addWriteCommitted(rowId, binaryRow3, clock.now()),
                equalsToAddWriteCommittedResult(AddWriteCommittedResult.writeIntentExists(txId, newCommitTimestamp))
        );
    }

    /**
     * Tests handling values of a decently large size.
     */
    @Test
    public void testWriteLongValues() {
        Random random = new Random();

        for (int length = 10_000; length < 20_000; length++) {
            BinaryRow insertedValue = binaryRow(key, new TestValue(length, IgniteTestUtils.randomString(random, length)));

            RowId rowId = insert(insertedValue, txId);
            commitWrite(rowId, clock.now(), txId);

            BinaryRow valueFromStorage = read(rowId, clock.now());

            assertNotNull(valueFromStorage);
            assertEquals(insertedValue.tupleSlice(), valueFromStorage.tupleSlice(), "Value mismatch at length " + length);
        }
    }

    /**
     * Tests basic invariants of {@link MvPartitionStorage#abortWrite}.
     */
    @Test
    public void testAbortWrite() {
        RowId rowId = insert(binaryRow(key, value), txId);

        abortWrite(rowId, txId);

        // Aborted row can't be read.
        assertNull(read(rowId, HybridTimestamp.MAX_VALUE));
    }

    @Test
    void testAbortWriteSuccessfully() {
        RowId rowId = insert(binaryRow, txId);

        HybridTimestamp beforeAbortTimestamp = clock.now();

        assertThat(
                abortWrite(rowId, txId),
                equalsToAbortResult(AbortResult.success(binaryRow))
        );

        assertNull(read(rowId, beforeAbortTimestamp.subtractPhysicalTime(1)));
        assertNull(read(rowId, beforeAbortTimestamp));
        assertNull(read(rowId, beforeAbortTimestamp.addPhysicalTime(1)));
    }

    @Test
    void testAbortWriteForNotExistingVersionChain() {
        HybridTimestamp beforeAbortTimestamp = clock.now();

        assertThat(
                abortWrite(ROW_ID, txId),
                equalsToAbortResult(AbortResult.noWriteIntent())
        );

        assertNull(read(ROW_ID, beforeAbortTimestamp.subtractPhysicalTime(1)));
        assertNull(read(ROW_ID, beforeAbortTimestamp));
        assertNull(read(ROW_ID, beforeAbortTimestamp.addPhysicalTime(1)));
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testAbortWriteForAlreadyCommittedWrite(boolean useDifferentTxId) {
        RowId rowId = insert(binaryRow, txId);

        HybridTimestamp commitTimestamp = clock.now();

        commitWrite(rowId, commitTimestamp, txId);

        HybridTimestamp beforeAbortTimestamp = clock.now();

        assertThat(
                abortWrite(rowId, useDifferentTxId ? newTransactionId() : txId),
                equalsToAbortResult(AbortResult.noWriteIntent())
        );

        assertThat(read(rowId, commitTimestamp), isRow(binaryRow));
        assertThat(read(rowId, beforeAbortTimestamp), isRow(binaryRow));
        assertThat(read(rowId, beforeAbortTimestamp.addPhysicalTime(1)), isRow(binaryRow));

        assertThat(storage.read(rowId, commitTimestamp).commitTimestamp(), equalTo(commitTimestamp));
        assertThat(storage.read(rowId, beforeAbortTimestamp).commitTimestamp(), equalTo(commitTimestamp));
        assertThat(storage.read(rowId, beforeAbortTimestamp.addPhysicalTime(1)).commitTimestamp(), equalTo(commitTimestamp));
    }

    @Test
    void testAbortWriteWithDifferentTxId() {
        RowId rowId = insert(binaryRow, txId);

        HybridTimestamp beforeAbortTimestamp = clock.now();

        assertThat(
                abortWrite(rowId, newTransactionId()),
                equalsToAbortResult(AbortResult.txMismatch(txId))
        );

        assertTrue(storage.read(rowId, beforeAbortTimestamp.subtractPhysicalTime(1)).isWriteIntent());
        assertTrue(storage.read(rowId, beforeAbortTimestamp).isWriteIntent());
        assertTrue(storage.read(rowId, beforeAbortTimestamp.addPhysicalTime(1)).isWriteIntent());
    }

    /**
     * Tests basic invariants of {@link MvPartitionStorage#commitWrite}.
     */
    @Test
    public void testCommitWrite() {
        RowId rowId = insert(binaryRow, txId);

        HybridTimestamp tsBefore = clock.now();

        HybridTimestamp tsExact = clock.now();
        commitWrite(rowId, tsExact, txId);

        HybridTimestamp tsAfter = clock.now();

        // Row is invisible at the time before writing.
        assertNull(read(rowId, tsBefore));

        // Row is valid at the time during and after writing.
        assertThat(read(rowId, tsExact), isRow(binaryRow));
        assertThat(read(rowId, tsAfter), isRow(binaryRow));

        TestValue newValue = new TestValue(30, "duh");

        UUID newTxId = newTransactionId();

        BinaryRow newRow = binaryRow(key, newValue);
        addWrite(rowId, newRow, newTxId);

        // Same checks, but now there are two different versions.
        assertNull(read(rowId, tsBefore));

        assertThat(read(rowId, HybridTimestamp.MAX_VALUE), isRow(newRow));

        assertThat(read(rowId, tsExact), isRow(binaryRow));
        assertThat(read(rowId, tsAfter), isRow(newRow));
        assertThat(read(rowId, clock.now()), isRow(newRow));

        // Only latest time behavior changes after commit.
        HybridTimestamp newRowCommitTs = clock.now();
        commitWrite(rowId, newRowCommitTs, newTxId);

        assertThat(read(rowId, HybridTimestamp.MAX_VALUE), isRow(newRow));

        assertThat(read(rowId, tsExact), isRow(binaryRow));
        assertThat(read(rowId, tsAfter), isRow(binaryRow));

        assertThat(read(rowId, clock.now()), isRow(newRow));

        // Remove.
        UUID removeTxId = newTransactionId();

        addWrite(rowId, null, removeTxId);

        assertNull(read(rowId, tsBefore));

        assertNull(read(rowId, HybridTimestamp.MAX_VALUE));

        assertThat(read(rowId, tsExact), isRow(binaryRow));
        assertThat(read(rowId, tsAfter), isRow(binaryRow));
        assertThat(read(rowId, newRowCommitTs), isRow(newRow));

        assertNull(read(rowId, clock.now()));

        // Commit remove.
        HybridTimestamp removeTs = clock.now();
        commitWrite(rowId, removeTs, removeTxId);

        assertNull(read(rowId, tsBefore));

        assertNull(read(rowId, HybridTimestamp.MAX_VALUE));
        assertNull(read(rowId, removeTs));
        assertNull(read(rowId, clock.now()));

        assertThat(read(rowId, tsExact), isRow(binaryRow));
        assertThat(read(rowId, tsAfter), isRow(binaryRow));
    }

    @Test
    void testCommitWriteSuccessfully() {
        RowId rowId = insert(binaryRow, txId);

        HybridTimestamp commitTimestamp = clock.now();

        assertThat(
                commitWrite(rowId, commitTimestamp, txId),
                equalsToCommitResult(CommitResult.success())
        );

        assertNull(read(rowId, commitTimestamp.subtractPhysicalTime(1)));
        assertThat(read(rowId, commitTimestamp), isRow(binaryRow));
        assertThat(read(rowId, commitTimestamp.addPhysicalTime(1)), isRow(binaryRow));
    }

    @Test
    void testCommitWriteForNotExistingVersionChain() {
        HybridTimestamp commitTimestamp = clock.now();

        assertThat(
                commitWrite(ROW_ID, commitTimestamp, txId),
                equalsToCommitResult(CommitResult.noWriteIntent())
        );

        assertNull(read(ROW_ID, commitTimestamp.subtractPhysicalTime(1)));
        assertNull(read(ROW_ID, commitTimestamp));
        assertNull(read(ROW_ID, commitTimestamp.addPhysicalTime(1)));
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testCommitWriteForAlreadyCommittedWrite(boolean useDifferentTxId) {
        RowId rowId = insert(binaryRow, txId);

        HybridTimestamp commitTimestamp = clock.now();
        HybridTimestamp newCommitTimestamp = clock.now();

        commitWrite(rowId, commitTimestamp, txId);

        assertThat(
                commitWrite(rowId, newCommitTimestamp, useDifferentTxId ? newTransactionId() : txId),
                equalsToCommitResult(CommitResult.noWriteIntent())
        );

        assertNull(read(rowId, commitTimestamp.subtractPhysicalTime(1)));
        assertThat(read(rowId, commitTimestamp), isRow(binaryRow));
        assertThat(read(rowId, commitTimestamp.addPhysicalTime(1)), isRow(binaryRow));

        assertThat(storage.read(rowId, commitTimestamp).commitTimestamp(), equalTo(commitTimestamp));
        assertThat(storage.read(rowId, newCommitTimestamp).commitTimestamp(), equalTo(commitTimestamp));
    }

    @Test
    void testCommitWriteWithDifferentTxId() {
        RowId rowId = insert(binaryRow, txId);

        HybridTimestamp commitTimestamp = clock.now();

        assertThat(
                commitWrite(rowId, commitTimestamp, newTransactionId()),
                equalsToCommitResult(CommitResult.txMismatch(txId))
        );

        assertTrue(storage.read(rowId, commitTimestamp.subtractPhysicalTime(1)).isWriteIntent());
        assertTrue(storage.read(rowId, commitTimestamp).isWriteIntent());
        assertTrue(storage.read(rowId, commitTimestamp.addPhysicalTime(1)).isWriteIntent());
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
        commitWrite(rowId1, ts2, txId);

        HybridTimestamp ts3 = clock.now();

        HybridTimestamp ts4 = clock.now();
        commitWrite(rowId2, ts4, txId);

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
        commitWrite(rowId1, clock.now(), txId);

        RowId rowId2 = insert(binaryRow(new TestKey(2, "2"), value2), txId);
        commitWrite(rowId2, clock.now(), txId);

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
        BinaryRow binaryRow1Commited = binaryRow(key1, value11);
        BinaryRow binaryRow1WriteIntent = binaryRow(key1, value12);

        addWrite(rowId1, binaryRow1Commited, txId);
        HybridTimestamp commitTs1 = clock.now();
        commitWrite(rowId1, commitTs1, txId);

        addWrite(rowId1, binaryRow1WriteIntent, newTransactionId());

        TestKey key2 = new TestKey(2, "2");
        BinaryRow binaryRow2Commited = binaryRow(key2, value21);
        BinaryRow binaryRow2WriteIntent = binaryRow(key2, value22);

        addWrite(rowId2, binaryRow2Commited, txId);
        HybridTimestamp commitTs2 = clock.now();
        commitWrite(rowId2, commitTs2, txId);

        addWrite(rowId2, binaryRow2WriteIntent, newTransactionId());

        try (PartitionTimestampCursor cursor = scan(clock.now())) {
            assertThrows(IllegalStateException.class, () -> cursor.committed(commitTs1));

            {
                // First row
                assertTrue(cursor.hasNext());
                ReadResult res = cursor.next();

                assertNotNull(res);
                assertTrue(res.isWriteIntent());
                assertFalse(res.isEmpty());

                // Since scan doesn't ends exactly on the commit timestamp, write-intent row is returned.
                assertThat(res.binaryRow(), isRow(binaryRow1WriteIntent));

                // Committed row is visible for exact commit timestamp.
                assertThat(cursor.committed(commitTs1), isRow(binaryRow1Commited));

                // No row because there is a commited row after given cursor timestamp.
                assertThat(cursor.committed(before(commitTs1)), isRow(null));

                // Write-intent row because a given cursor timestamp is after the most recent commit.
                assertThat(cursor.committed(after(commitTs1)), isRow(binaryRow1WriteIntent));
            }

            {
                // Second row
                assertTrue(cursor.hasNext());
                ReadResult res = cursor.next();

                assertNotNull(res);
                assertTrue(res.isWriteIntent());
                assertFalse(res.isEmpty());

                // Since scan doesn't ends exactly on the commit timestamp, write-intent row is returned.
                assertThat(res.binaryRow(), isRow(binaryRow2WriteIntent));

                // Committed row is visible for exact commit timestamp.
                assertThat(cursor.committed(commitTs2), isRow(binaryRow2Commited));

                // No row because there is a commited row after given cursor timestamp.
                assertThat(cursor.committed(before(commitTs2)), isRow(null));

                // Write-intent row because a given cursor timestamp is after the most recent commit.
                assertThat(cursor.committed(after(commitTs2)), isRow(binaryRow2WriteIntent));
            }

            // Cursor is exhausted.
            assertFalse(cursor.hasNext());
            assertFalse(cursor.hasNext());

            assertThrows(NoSuchElementException.class, cursor::next);

            assertThrows(IllegalStateException.class, () -> cursor.committed(commitTs1));
        }
    }

    private static HybridTimestamp before(HybridTimestamp ts) {
        return ts.subtractPhysicalTime(1);
    }

    private static HybridTimestamp after(HybridTimestamp ts) {
        return ts.addPhysicalTime(1);
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
        assertThat(foundResult.binaryRow(), isRow(binaryRow));
    }

    @Test
    void readOfCommittedRowReturnsTheRow() {
        RowId rowId = insert(binaryRow, txId);
        commitWrite(rowId, clock.now(), txId);

        ReadResult foundResult = storage.read(rowId, HybridTimestamp.MAX_VALUE);

        assertThat(foundResult.rowId(), is(rowId));
        assertThat(foundResult.binaryRow(), isRow(binaryRow));
    }

    @Test
    void readsUncommittedVersionEvenWhenThereIsCommittedVersionBeforeIt() {
        RowId rowId1 = insert(binaryRow, txId);
        commitWrite(rowId1, clock.now(), txId);

        RowId rowId2 = insert(binaryRow2, txId);

        ReadResult foundResult = storage.read(rowId2, HybridTimestamp.MAX_VALUE);

        assertThat(foundResult.rowId(), is(rowId2));
        assertThat(foundResult.binaryRow(), isRow(binaryRow2));
    }

    @Test
    void readsCommittedVersionEvenWhenThereIsCommittedVersionBeforeIt() {
        RowId rowId1 = insert(binaryRow, txId);
        commitWrite(rowId1, clock.now(), txId);

        RowId rowId2 = insert(binaryRow2, txId);
        commitWrite(rowId2, clock.now(), txId);

        ReadResult foundResult = storage.read(rowId2, HybridTimestamp.MAX_VALUE);

        assertThat(foundResult.rowId(), is(rowId2));
        assertThat(foundResult.binaryRow(), isRow(binaryRow2));
    }

    @Test
    void readByExactlyCommitTimestampFindsRow() {
        RowId rowId = insert(binaryRow, txId);
        HybridTimestamp commitTimestamp = clock.now();
        commitWrite(rowId, commitTimestamp, txId);

        BinaryRow foundRow = read(rowId, commitTimestamp);

        assertThat(foundRow, isRow(binaryRow));
    }

    @Test
    void readByTimestampAfterCommitTimestampFindsRow() {
        RowId rowId = insert(binaryRow, txId);
        HybridTimestamp commitTimestamp = clock.now();
        commitWrite(rowId, commitTimestamp, txId);

        HybridTimestamp afterCommit = clock.now();
        BinaryRow foundRow = read(rowId, afterCommit);

        assertThat(foundRow, isRow(binaryRow));
    }

    @Test
    void readByTimestampBeforeFirstVersionCommitTimestampFindsNothing() {
        HybridTimestamp beforeCommit = clock.now();

        RowId rowId = insert(binaryRow, txId);
        HybridTimestamp commitTimestamp = clock.now();
        commitWrite(rowId, commitTimestamp, txId);

        BinaryRow foundRow = read(rowId, beforeCommit);

        assertThat(foundRow, isRow(null));
    }

    @Test
    void readByTimestampOfLastVersionFindsLastVersion() {
        RowId rowId = insert(binaryRow, txId);
        HybridTimestamp firstVersionTs = clock.now();
        commitWrite(rowId, firstVersionTs, txId);

        UUID newTxId = newTransactionId();
        addWrite(rowId, binaryRow2, newTxId);
        HybridTimestamp secondVersionTs = clock.now();
        commitWrite(rowId, secondVersionTs, newTxId);

        BinaryRow foundRow = read(rowId, secondVersionTs);

        assertThat(foundRow, isRow(binaryRow2));
    }

    @Test
    void readByTimestampOfPreviousVersionFindsPreviousVersion() {
        RowId rowId = insert(binaryRow, txId);
        HybridTimestamp firstVersionTs = clock.now();
        commitWrite(rowId, firstVersionTs, txId);

        UUID newTxId = newTransactionId();
        addWrite(rowId, binaryRow2, newTxId);
        commitWrite(rowId, clock.now(), newTxId);

        BinaryRow foundRow = read(rowId, firstVersionTs);

        assertThat(foundRow, isRow(binaryRow));
    }

    @Test
    void readByTimestampBetweenVersionsFindsPreviousVersion() {
        RowId rowId = insert(binaryRow, txId);
        HybridTimestamp firstVersionTs = clock.now();
        commitWrite(rowId, firstVersionTs, txId);

        HybridTimestamp tsInBetween = clock.now();

        UUID newTxId = newTransactionId();
        addWrite(rowId, binaryRow2, newTxId);
        commitWrite(rowId, clock.now(), newTxId);

        BinaryRow foundRow = read(rowId, tsInBetween);

        assertThat(foundRow, isRow(binaryRow));
    }

    @Test
    void readByTimestampAfterWriteFindsUncommittedVersion() {
        RowId rowId = new RowId(PARTITION_ID);

        addWrite(rowId, binaryRow, newTransactionId());

        HybridTimestamp latestTs = clock.now();
        BinaryRow foundRow = read(rowId, latestTs);

        assertThat(foundRow, isRow(binaryRow));
    }

    @Test
    void readByTimestampAfterCommitAndWriteFindsUncommittedVersion() {
        RowId rowId = insert(binaryRow, txId);
        commitWrite(rowId, clock.now(), txId);

        addWrite(rowId, binaryRow2, newTransactionId());

        HybridTimestamp latestTs = clock.now();
        BinaryRow foundRow = read(rowId, latestTs);

        assertThat(foundRow, isRow(binaryRow2));
    }

    @Test
    void addWriteWithDifferentTxIdThrows() {
        RowId rowId = insert(binaryRow, txId);

        assertThat(
                addWrite(rowId, binaryRow2, newTransactionId()),
                equalsToAddWriteResult(AddWriteResult.txMismatch(txId, null))
        );
    }

    @Test
    void secondUncommittedWriteWithSameTxIdReplacesExistingUncommittedWrite() {
        RowId rowId = insert(binaryRow, txId);

        addWrite(rowId, binaryRow2, txId);

        BinaryRow foundRow = read(rowId, HybridTimestamp.MAX_VALUE);

        assertThat(foundRow, isRow(binaryRow2));
    }

    @Test
    void addWriteReturnsUncommittedVersionIfItExists() {
        RowId rowId = insert(binaryRow, txId);

        BinaryRow returnedRow = addWrite(rowId, binaryRow2, txId).previousWriteIntent();

        assertThat(returnedRow, isRow(binaryRow));
    }

    @Test
    void addWriteReturnsNullIfNoUncommittedVersionExists() {
        RowId rowId = insert(binaryRow, txId);
        commitWrite(rowId, clock.now(), txId);

        BinaryRow returnedRow = addWrite(rowId, binaryRow2, newTransactionId()).previousWriteIntent();

        assertThat(returnedRow, isRow(null));
    }

    @Test
    void addWriteCommittedTombstone() {
        addWriteCommitted(ROW_ID, binaryRow, clock.now());
        assertThat(read(ROW_ID, HybridTimestamp.MAX_VALUE), isRow(binaryRow));

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
        RowId rowId = insert(binaryRow, txId);
        commitWrite(rowId, clock.now(), txId);

        addWrite(rowId, null, newTransactionId());

        BinaryRow foundRow = read(rowId, HybridTimestamp.MAX_VALUE);

        assertThat(foundRow, isRow(null));
    }

    @Test
    void afterRemovalReadByLatestTimestampFindsNothing() {
        RowId rowId = insert(binaryRow, txId);
        commitWrite(rowId, clock.now(), txId);

        UUID newTxId = newTransactionId();
        addWrite(rowId, null, newTxId);
        commitWrite(rowId, clock.now(), newTxId);

        BinaryRow foundRow = read(rowId, clock.now());

        assertThat(foundRow, isRow(null));
    }

    @Test
    void afterRemovalPreviousVersionRemainsAccessibleByTimestamp() {
        RowId rowId = insert(binaryRow, txId);
        HybridTimestamp firstTimestamp = clock.now();
        commitWrite(rowId, firstTimestamp, txId);

        UUID newTxId = newTransactionId();
        addWrite(rowId, null, newTxId);
        commitWrite(rowId, clock.now(), newTxId);

        BinaryRow foundRow = read(rowId, firstTimestamp);

        assertThat(foundRow, isRow(binaryRow));
    }

    @Test
    void removalReturnsUncommittedRowVersionIfItExists() {
        RowId rowId = insert(binaryRow, txId);

        BinaryRow rowFromRemoval = addWrite(rowId, null, txId).previousWriteIntent();

        assertThat(rowFromRemoval, isRow(binaryRow));
    }

    @Test
    void removalReturnsNullIfNoUncommittedVersionExists() {
        RowId rowId = insert(binaryRow, txId);
        commitWrite(rowId, clock.now(), txId);

        BinaryRow rowFromRemoval = addWrite(rowId, null, newTransactionId()).previousWriteIntent();

        assertThat(rowFromRemoval, isRow(null));
    }

    @Test
    void commitWriteCommitsWriteIntentVersion() {
        RowId rowId = insert(binaryRow, txId);
        commitWrite(rowId, clock.now(), txId);

        ReadResult readResult = storage.read(rowId, clock.now());

        assertFalse(readResult.isWriteIntent());
    }

    @Test
    void commitWriteMakesVersionAvailableToReadByTimestamp() {
        RowId rowId = insert(binaryRow, txId);

        commitWrite(rowId, clock.now(), txId);

        BinaryRow foundRow = read(rowId, clock.now());

        assertThat(foundRow, isRow(binaryRow));
    }

    @Test
    void commitAndAbortWriteNoOpIfNoUncommittedVersionExists() {
        RowId rowId = insert(binaryRow, txId);
        commitWrite(rowId, clock.now(), txId);

        abortWrite(rowId, txId);

        assertThat(read(rowId, HybridTimestamp.MAX_VALUE), isRow(binaryRow));

        commitWrite(rowId, clock.now(), txId);

        assertThat(read(rowId, HybridTimestamp.MAX_VALUE), isRow(binaryRow));
    }

    @Test
    void abortWriteRemovesUncommittedVersion() {
        RowId rowId = insert(binaryRow, txId);
        commitWrite(rowId, clock.now(), txId);

        UUID newTxId = newTransactionId();

        addWrite(rowId, binaryRow2, newTxId);

        abortWrite(rowId, newTxId);

        BinaryRow foundRow = read(rowId, HybridTimestamp.MAX_VALUE);

        assertThat(foundRow, isRow(binaryRow));
    }

    @Test
    void abortOfInsertMakesRowNonExistentForReadByTimestamp() {
        RowId rowId = insert(binaryRow, txId);

        abortWrite(rowId, txId);

        BinaryRow foundRow = read(rowId, clock.now());

        assertThat(foundRow, isRow(null));
    }

    @Test
    void abortOfInsertMakesRowNonExistentForReadWithTxId() {
        RowId rowId = new RowId(PARTITION_ID);

        BinaryRow foundRow = read(rowId, HybridTimestamp.MAX_VALUE);

        assertThat(foundRow, isRow(null));
    }

    @Test
    void abortWriteReturnsTheRemovedVersion() {
        RowId rowId = insert(binaryRow, txId);

        BinaryRow returnedRow = abortWrite(rowId, txId).previousWriteIntent();

        assertThat(returnedRow, isRow(binaryRow));
    }

    @Test
    void readByTimestampWorksCorrectlyAfterCommitAndAbortFollowedByUncommittedWrite() {
        RowId rowId = commitAbortAndAddUncommitted();

        ReadResult foundResult = storage.read(rowId, clock.now());

        // We see the uncommitted row.
        assertThat(foundResult.rowId(), is(rowId));
        assertThat(foundResult.binaryRow(), isRow(binaryRow3));
    }

    @Test
    void readByTimestampBeforeAndAfterUncommittedWrite() {
        RowId rowId = new RowId(PARTITION_ID);

        HybridTimestamp commitTs = clock.now();

        storage.runConsistently(locker -> {
            addWrite(rowId, binaryRow, txId);

            commitWrite(rowId, commitTs, txId);

            return null;
        });

        UUID txId2 = newTransactionId();

        storage.runConsistently(locker -> {
            addWrite(rowId, binaryRow2, txId2);

            return null;
        });

        ReadResult res = storage.read(rowId, commitTs);

        assertNotNull(res);

        assertNull(res.transactionId());
        assertNull(res.commitTableOrZoneId());
        assertEquals(ReadResult.UNDEFINED_COMMIT_PARTITION_ID, res.commitPartitionId());
        assertThat(res.binaryRow(), isRow(binaryRow));

        res = storage.read(rowId, clock.now());

        assertNotNull(res);

        assertEquals(txId2, res.transactionId());
        assertEquals(COMMIT_TABLE_ID, res.commitTableOrZoneId());
        assertEquals(PARTITION_ID, res.commitPartitionId());
        assertThat(res.binaryRow(), isRow(binaryRow2));
    }

    private RowId commitAbortAndAddUncommitted() {
        return storage.runConsistently(locker -> {
            RowId rowId = new RowId(PARTITION_ID);

            locker.lock(rowId);

            storage.addWrite(rowId, binaryRow, txId, 999, 0);
            commitWrite(rowId, clock.now(), txId);

            UUID newTxId = newTransactionId();
            addWrite(rowId, binaryRow2, newTxId);
            storage.abortWrite(rowId, newTxId);

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
            assertThat(result.binaryRow(), isRow(binaryRow3));

            assertFalse(cursor.hasNext());
        }
    }

    @Test
    void readByTimestampWorksCorrectlyIfNoUncommittedValueExists() {
        RowId rowId = insert(binaryRow, txId);

        BinaryRow foundRow = read(rowId, clock.now());

        assertThat(foundRow, isRow(binaryRow));
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
            UUID txId = newTransactionId();

            addWrite(rowId, binaryRow, txId);

            commitWrite(rowId, first, txId);
            return null;
        });

        HybridTimestamp betweenCommits = clock.now();

        HybridTimestamp second = clock.now();

        storage.runConsistently(locker -> {
            UUID txId = newTransactionId();

            addWrite(rowId, binaryRow2, txId);

            commitWrite(rowId, second, txId);
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
        assertThat(res.binaryRow(), isRow(binaryRow));

        // Read between two commits.
        res = storage.read(rowId, betweenCommits);

        assertNotNull(res);
        assertNull(res.newestCommitTimestamp());
        assertThat(res.binaryRow(), isRow(binaryRow));

        // Read at exact time of second commit.
        res = storage.read(rowId, second);

        assertNotNull(res);
        assertNull(res.newestCommitTimestamp());
        assertThat(res.binaryRow(), isRow(binaryRow2));

        // Read after second commit (write intent).
        res = storage.read(rowId, after);

        assertNotNull(res);
        assertNotNull(res.newestCommitTimestamp());
        assertEquals(second, res.newestCommitTimestamp());
        assertThat(res.binaryRow(), isRow(binaryRow3));
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
            addWrite(lowerRowId, binaryRow2, txId);
            addWrite(higherRowId, binaryRow, txId);

            commitWrite(higherRowId, clock.now(), txId);

            return null;
        });

        assertThat(read(higherRowId, clock.now()), isRow(binaryRow));
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

        assertThat(read(lowerRowId, clock.now()), isRow(binaryRow));
    }

    @Test
    void testReadingTombstoneIfLatestCommitExists() {
        RowId rowId = new RowId(PARTITION_ID);

        HybridTimestamp commitTs = clock.now();

        storage.runConsistently(locker -> {
            addWrite(rowId, binaryRow, txId);
            commitWrite(rowId, commitTs, txId);

            addWrite(rowId, null, newTransactionId());

            return null;
        });

        ReadResult res = storage.read(rowId, clock.now());

        assertNotNull(res);
        assertNull(res.binaryRow());
        assertEquals(commitTs, res.newestCommitTimestamp());
    }

    @Test
    void testReadingTombstoneIfLatestCommitNotExists() {
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

            UUID newTxId = newTransactionId();

            addWrite(rowId, row, newTxId);

            commitWrite(rowId, clock.now(), newTxId);
        }

        // Put rows before and after.
        RowId lowRowId = new RowId(PARTITION_ID, 99, 0);
        RowId highRowId = new RowId(PARTITION_ID, 101, 0);

        List.of(lowRowId, highRowId).forEach(newRowId -> {
            UUID newTxId = newTransactionId();

            addWrite(newRowId, binaryRow(key, value), newTxId);

            commitWrite(newRowId, clock.now(), newTxId);
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
            assertThat(next.binaryRow(), isRow(binaryRow2));

            BinaryRow committedRow = cursor.committed(rowIdAndCommitTs.get2());

            assertThat(committedRow, isRow(binaryRow));
        }
    }

    private IgniteBiTuple<RowId, HybridTimestamp> addCommittedVersionAndWriteIntent() {
        RowId rowId = new RowId(PARTITION_ID);

        HybridTimestamp commitTs = clock.now();

        storage.runConsistently(locker -> {
            UUID txId = newTransactionId();

            addWrite(rowId, binaryRow, txId);

            commitWrite(rowId, commitTs, txId);

            addWrite(rowId, binaryRow2, newTransactionId());

            return null;
        });

        return new IgniteBiTuple<>(rowId, commitTs);
    }

    @Test
    void testScanVersionsWithWriteIntent() {
        RowId rowId = new RowId(PARTITION_ID, 100, 0);

        addWrite(rowId, binaryRow(key, value), txId);

        commitWrite(rowId, clock.now(), txId);

        addWrite(rowId, binaryRow(key, value2), newTransactionId());

        // Put rows before and after.
        RowId lowRowId = new RowId(PARTITION_ID, 99, 0);
        RowId highRowId = new RowId(PARTITION_ID, 101, 0);

        List.of(lowRowId, highRowId).forEach(newRowId -> {
            UUID newTxId = newTransactionId();

            addWrite(newRowId, binaryRow(key, value), newTxId);

            commitWrite(newRowId, clock.now(), newTxId);
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
    void testClosestRow() {
        RowId rowId0 = new RowId(PARTITION_ID, 1, -1);
        RowId rowId1 = new RowId(PARTITION_ID, 1, 0);
        RowId rowId2 = new RowId(PARTITION_ID, 1, 1);

        RowMeta expectedRowMeta1 = new RowMeta(rowId1, txId, COMMIT_TABLE_ID, PARTITION_ID);
        RowMeta expectedRowMeta2 = new RowMeta(rowId2, txId, COMMIT_TABLE_ID, PARTITION_ID);

        addWrite(rowId1, binaryRow, txId);
        addWrite(rowId2, binaryRow2, txId);

        assertRowMetaEquals(expectedRowMeta1, storage.closestRow(rowId0));
        assertRowMetaEquals(expectedRowMeta1, storage.closestRow(rowId0.increment()));

        assertRowMetaEquals(expectedRowMeta1, storage.closestRow(rowId1));

        assertRowMetaEquals(expectedRowMeta2, storage.closestRow(rowId2));

        assertNull(storage.closestRow(rowId2.increment()));
    }

    private static void assertRowMetaEquals(RowMeta expected, RowMeta actual) {
        assertNotNull(actual);

        assertEquals(expected.rowId(), actual.rowId());
        assertEquals(expected.transactionId(), actual.transactionId());
        assertEquals(expected.commitTableOrZoneId(), actual.commitTableOrZoneId());
        assertEquals(expected.commitPartitionId(), actual.commitPartitionId());
    }

    @Test
    void testClosestRowReconstruction() {
        RowId rowId = new RowId(PARTITION_ID, 0x1234567890ABCDEFL, 0xFEDCBA0987654321L);

        RowMeta expectedRowMeta = new RowMeta(rowId, txId, COMMIT_TABLE_ID, PARTITION_ID);

        addWrite(rowId, binaryRow, txId);

        assertRowMetaEquals(expectedRowMeta, storage.closestRow(RowId.lowestRowId(PARTITION_ID)));
    }

    @Test
    public void addWriteCommittedAddsCommittedVersion() {
        RowId rowId = new RowId(PARTITION_ID);

        addWriteCommitted(rowId, binaryRow, clock.now());

        // Read with timestamp returns write-intent.
        assertThat(storage.read(rowId, clock.now()).binaryRow(), isRow(binaryRow));
    }

    @Test
    public void addWriteCommittedLeavesExistingCommittedVersionsUntouched() {
        RowId rowId = new RowId(PARTITION_ID);

        HybridTimestamp ts1 = clock.now();

        addWriteCommitted(rowId, binaryRow, ts1);
        addWriteCommitted(rowId, binaryRow2, clock.now());

        assertThat(storage.read(rowId, clock.now()).binaryRow(), isRow(binaryRow2));
        assertThat(storage.read(rowId, ts1).binaryRow(), isRow(binaryRow));
    }

    @Test
    public void scanVersionsReturnsUncommittedVersionsAsUncommitted() {
        RowId rowId = insert(binaryRow, txId);
        commitWrite(rowId, clock.now(), txId);
        addWrite(rowId, binaryRow2, newTransactionId());

        storage.runConsistently(locker -> {
            locker.lock(rowId);

            try (Cursor<ReadResult> cursor = storage.scanVersions(rowId)) {
                ReadResult result = cursor.next();

                assertThat(result.rowId(), is(rowId));
                assertTrue(result.isWriteIntent());
                assertThat(result.commitPartitionId(), is(not(ReadResult.UNDEFINED_COMMIT_PARTITION_ID)));
                assertThat(result.commitTableOrZoneId(), is(notNullValue()));
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
        commitWrite(rowId, clock.now(), txId);

        storage.runConsistently(locker -> {
            locker.lock(rowId);

            try (Cursor<ReadResult> cursor = storage.scanVersions(rowId)) {
                ReadResult result = cursor.next();

                assertThat(result.rowId(), is(rowId));
                assertFalse(result.isWriteIntent());
                assertThat(result.commitPartitionId(), is(ReadResult.UNDEFINED_COMMIT_PARTITION_ID));
                assertThat(result.commitTableOrZoneId(), is(nullValue()));
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
        commitWrite(rowId, clock.now(), txId);

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
        commitWrite(rowId, commitTs, txId);

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
        commitWrite(rowId, clock.now(), txId);

        UUID newTxId = newTransactionId();
        addWrite(rowId, null, newTxId);
        commitWrite(rowId, clock.now(), newTxId);

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
        commitWrite(rowId1, commitTs1, txId);

        addWrite(new RowId(PARTITION_ID, 0, 1), binaryRow2, txId);

        try (PartitionTimestampCursor cursor = scan(scanTsProvider.scanTimestamp(clock))) {
            cursor.next();

            assertDoesNotThrow(() -> cursor.committed(commitTs1));

            ReadResult result2 = cursor.next();
            assertThat(result2.binaryRow(), isRow(binaryRow2));

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
            UUID nodeId0 = UUID.randomUUID();

            long lst1 = 2000;
            UUID nodeId1 = UUID.randomUUID();

            var leaseInfo = new LeaseInfo(lst0, nodeId0, nodeId0 + "name");

            storage.updateLease(leaseInfo);

            assertEquals(leaseInfo, storage.leaseInfo());

            leaseInfo = new LeaseInfo(lst1, nodeId1, nodeId1 + "name");

            storage.updateLease(leaseInfo);

            assertEquals(leaseInfo, storage.leaseInfo());

            return null;
        });
    }

    @Test
    public void estimatedSizeUsingWriteIntents() {
        assertThat(storage.estimatedSize(), is(0L));

        // Adding a Write Intent should not increase the size.
        addWrite(ROW_ID, binaryRow, txId);

        assertThat(storage.estimatedSize(), is(0L));

        // Committing a row increases the size.
        commitWrite(ROW_ID, clock.now(), txId);

        assertThat(storage.estimatedSize(), is(1L));

        // Adding a Write Intent with a tombstone does not decrease the size.
        UUID newTxId = newTransactionId();
        addWrite(ROW_ID, null, newTxId);

        assertThat(storage.estimatedSize(), is(1L));

        // Committing a tombstone decreases the size.
        commitWrite(ROW_ID, clock.now(), newTxId);

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
    public void estimatedSizeNeverFallsBelowZeroUsingWriteCommitted() {
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
    public void estimatedSizeNeverFallsBelowZeroUsingCommitWrite() {
        assertThat(storage.estimatedSize(), is(0L));

        addWrite(ROW_ID, null, txId);

        assertThat(storage.estimatedSize(), is(0L));

        commitWrite(ROW_ID, clock.now(), txId);

        assertThat(storage.estimatedSize(), is(0L));

        addWriteCommitted(ROW_ID, binaryRow, clock.now());

        assertThat(storage.estimatedSize(), is(1L));

        addWrite(ROW_ID, null, txId);

        assertThat(storage.estimatedSize(), is(1L));

        commitWrite(ROW_ID, clock.now(), txId);

        assertThat(storage.estimatedSize(), is(0L));

        addWrite(ROW_ID, null, txId);

        assertThat(storage.estimatedSize(), is(0L));

        commitWrite(ROW_ID, clock.now(), txId);

        assertThat(storage.estimatedSize(), is(0L));
    }

    @Test
    public void estimatedSizeIncreasedAfterTombstoneUsingWriteCommitted() {
        addWriteCommitted(ROW_ID, binaryRow, clock.now());

        assertThat(storage.estimatedSize(), is(1L));

        addWriteCommitted(ROW_ID, null, clock.now());

        assertThat(storage.estimatedSize(), is(0L));

        addWriteCommitted(ROW_ID, binaryRow, clock.now());

        assertThat(storage.estimatedSize(), is(1L));
    }

    @Test
    public void estimatedSizeIncreasedAfterTombstoneUsingCommiteWrite() {
        addWrite(ROW_ID, binaryRow, txId);
        commitWrite(ROW_ID, clock.now(), txId);

        assertThat(storage.estimatedSize(), is(1L));

        addWrite(ROW_ID, null, txId);
        commitWrite(ROW_ID, clock.now(), txId);

        assertThat(storage.estimatedSize(), is(0L));

        addWrite(ROW_ID, binaryRow, txId);
        commitWrite(ROW_ID, clock.now(), txId);

        assertThat(storage.estimatedSize(), is(1L));
    }

    @Test
    public void estimatedSizeShowsLatestRowsNumberUsingWriteCommited() {
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
    public void estimatedSizeShowsLatestRowsNumberUsingCommitWrite() {
        assertThat(storage.estimatedSize(), is(0L));

        var rowId1 = new RowId(PARTITION_ID);
        var rowId2 = new RowId(PARTITION_ID);

        addWrite(rowId1, binaryRow, txId);
        addWrite(rowId2, binaryRow, txId);

        assertThat(storage.estimatedSize(), is(0L));

        commitWrite(rowId1, clock.now(), txId);
        commitWrite(rowId2, clock.now(), txId);

        assertThat(storage.estimatedSize(), is(2L));

        // Overwrite an existing row.
        addWrite(rowId1, binaryRow, txId);

        assertThat(storage.estimatedSize(), is(2L));

        commitWrite(rowId1, clock.now(), txId);

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
        addWriteCommitted(ROW_ID, binaryRow, clock.now());

        addWrite(ROW_ID, binaryRow, txId);

        abortWrite(ROW_ID, txId);

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
