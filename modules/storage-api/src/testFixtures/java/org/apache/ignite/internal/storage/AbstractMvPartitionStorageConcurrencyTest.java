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

import static org.apache.ignite.internal.testframework.IgniteTestUtils.runRace;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.schema.TableRow;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/**
 * Test to check for race conditions in MV partition storage.
 */
public abstract class AbstractMvPartitionStorageConcurrencyTest extends BaseMvPartitionStorageTest {
    /** To be used in a loop. {@link RepeatedTest} has a smaller failure rate due to recreating the storage every time. */
    private static final int REPEATS = 100;

    @Test
    void testAbortAndRead() {
        for (int i = 0; i < REPEATS; i++) {
            addWrite(ROW_ID, TABLE_ROW, TX_ID);

            runRace(
                    () -> abortWrite(ROW_ID),
                    () -> read(ROW_ID, clock.now()),
                    () -> scanFirstEntry(clock.now()),
                    () -> scanFirstVersion(ROW_ID)
            );

            assertNull(read(ROW_ID, clock.now()));
        }
    }

    @Test
    void testCommitAndRead() {
        for (int i = 0; i < REPEATS; i++) {
            addWrite(ROW_ID, TABLE_ROW, TX_ID);

            runRace(
                    () -> commitWrite(ROW_ID, clock.now()),
                    () -> read(ROW_ID, clock.now()),
                    () -> scanFirstEntry(clock.now()),
                    () -> scanFirstVersion(ROW_ID)
            );

            assertRowMatches(read(ROW_ID, clock.now()), TABLE_ROW);
        }
    }

    @Test
    void testUpdateAndRead() {
        for (int i = 0; i < REPEATS; i++) {
            addWrite(ROW_ID, TABLE_ROW, TX_ID);

            runRace(
                    () -> addWrite(ROW_ID, TABLE_ROW2, TX_ID),
                    () -> read(ROW_ID, clock.now()),
                    () -> scanFirstEntry(clock.now()),
                    () -> scanFirstVersion(ROW_ID)
            );

            assertRowMatches(read(ROW_ID, clock.now()), TABLE_ROW2);
        }
    }

    @ParameterizedTest
    @EnumSource(AddAndCommit.class)
    public void testRegularGcAndRead(AddAndCommit addAndCommit) {
        for (int i = 0; i < REPEATS; i++) {
            HybridTimestamp firstCommitTs = addAndCommit(TABLE_ROW);

            addAndCommit.perform(this, TABLE_ROW2);

            runRace(
                    () -> pollForVacuum(HybridTimestamp.MAX_VALUE),
                    () -> read(ROW_ID, firstCommitTs),
                    () -> scanFirstEntry(firstCommitTs)
            );

            assertNull(pollForVacuum(HybridTimestamp.MAX_VALUE));

            cleanup();
        }
    }

    @ParameterizedTest
    @EnumSource(AddAndCommit.class)
    public void testTombstoneGcAndRead(AddAndCommit addAndCommit) {
        for (int i = 0; i < REPEATS; i++) {
            HybridTimestamp firstCommitTs = addAndCommit.perform(this, TABLE_ROW);

            addAndCommit.perform(this, null);

            runRace(
                    () -> pollForVacuum(HybridTimestamp.MAX_VALUE),
                    () -> read(ROW_ID, firstCommitTs),
                    () -> scanFirstEntry(firstCommitTs)
            );

            assertNull(pollForVacuum(HybridTimestamp.MAX_VALUE));
        }
    }

    @ParameterizedTest
    @EnumSource(AddAndCommit.class)
    public void testTombstoneGcAndAddWrite(AddAndCommit addAndCommit) {
        for (int i = 0; i < REPEATS; i++) {
            addAndCommit.perform(this, TABLE_ROW);

            addAndCommit.perform(this, null);

            runRace(
                    () -> pollForVacuum(HybridTimestamp.MAX_VALUE),
                    () -> addWrite(ROW_ID, TABLE_ROW2, TX_ID)
            );

            assertRowMatches(read(ROW_ID, HybridTimestamp.MAX_VALUE), TABLE_ROW2);

            abortWrite(ROW_ID);

            assertNull(storage.closestRowId(ROW_ID));
        }
    }

    @ParameterizedTest
    @EnumSource(AddAndCommit.class)
    public void testTombstoneGcAndCommitWrite(AddAndCommit addAndCommit) {
        for (int i = 0; i < REPEATS; i++) {
            addAndCommit.perform(this, TABLE_ROW);

            addAndCommit.perform(this, null);

            addWrite(ROW_ID, TABLE_ROW2, TX_ID);

            runRace(
                    () -> pollForVacuum(HybridTimestamp.MAX_VALUE),
                    () -> commitWrite(ROW_ID, clock.now())
            );

            assertRowMatches(read(ROW_ID, HybridTimestamp.MAX_VALUE), TABLE_ROW2);

            assertNull(pollForVacuum(HybridTimestamp.MAX_VALUE));

            cleanup();
        }
    }

    @ParameterizedTest
    @EnumSource(AddAndCommit.class)
    public void testTombstoneGcAndAbortWrite(AddAndCommit addAndCommit) {
        for (int i = 0; i < REPEATS; i++) {
            addAndCommit.perform(this, TABLE_ROW);

            addAndCommit.perform(this, null);

            addWrite(ROW_ID, TABLE_ROW2, TX_ID);

            runRace(
                    () -> pollForVacuum(HybridTimestamp.MAX_VALUE),
                    () -> abortWrite(ROW_ID)
            );

            assertNull(storage.closestRowId(ROW_ID));
        }
    }

    @ParameterizedTest
    @EnumSource(AddAndCommit.class)
    public void testConcurrentGc(AddAndCommit addAndCommit) {
        for (int i = 0; i < REPEATS; i++) {
            addAndCommit.perform(this, TABLE_ROW);

            addAndCommit.perform(this, null);

            runRace(
                    () -> pollForVacuum(HybridTimestamp.MAX_VALUE),
                    () -> pollForVacuum(HybridTimestamp.MAX_VALUE),
                    () -> pollForVacuum(HybridTimestamp.MAX_VALUE),
                    () -> pollForVacuum(HybridTimestamp.MAX_VALUE)
            );

            assertNull(storage.closestRowId(ROW_ID));
        }
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    private void scanFirstEntry(HybridTimestamp firstCommitTs) {
        try (var cursor = scan(firstCommitTs)) {
            cursor.hasNext();
        }
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    private void scanFirstVersion(RowId rowId) {
        try (var cursor = scan(rowId)) {
            cursor.hasNext();
        }
    }

    /**
     * Adds a tombstone and cleans a GC queue until nothing's there.
     */
    private void cleanup() {
        addAndCommit(null);

        TableRowAndRowId row;

        do {
            row = pollForVacuum(HybridTimestamp.MAX_VALUE);
        } while (row != null);
    }

    /**
     * Perform add write operation.
     */
    protected enum AddAndCommit {
        ATOMIC {
            @Override
            HybridTimestamp perform(AbstractMvPartitionStorageConcurrencyTest test, @Nullable TableRow tableRow) {
                HybridTimestamp ts = test.clock.now();

                test.addWriteCommitted(ROW_ID, tableRow, ts);

                return ts;
            }
        },
        NON_ATOMIC {
            @Override
            HybridTimestamp perform(AbstractMvPartitionStorageConcurrencyTest test, @Nullable TableRow tableRow) {
                return test.addAndCommit(tableRow);
            }
        };

        abstract HybridTimestamp perform(AbstractMvPartitionStorageConcurrencyTest test, @Nullable TableRow tableRow);
    }
}
