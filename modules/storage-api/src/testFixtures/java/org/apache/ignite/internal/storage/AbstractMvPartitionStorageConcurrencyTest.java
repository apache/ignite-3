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
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.storage.impl.TestStorageEngine;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;

/**
 * Test to check for race conditions in MV partition storage.
 */
public abstract class AbstractMvPartitionStorageConcurrencyTest extends BaseMvPartitionStorageTest {
    /** To be used in a loop. {@link RepeatedTest} has a smaller failure rate due to recreating the storage every time. */
    private static final int REPEATS = 100;

    @Test
    void testAbortAndRead() throws Exception {
        for (int i = 0; i < REPEATS; i++) {
            addWrite(ROW_ID, BINARY_ROW, TX_ID);

            runRace(
                    () -> abortWrite(ROW_ID),
                    () -> read(ROW_ID, clock.now()),
                    () -> scanFirstEntry(clock.now())
            );

            assertNull(read(ROW_ID, clock.now()));
        }
    }

    @Test
    void testCommitAndRead() throws Exception {
        for (int i = 0; i < REPEATS; i++) {
            addWrite(ROW_ID, BINARY_ROW, TX_ID);

            runRace(
                    () -> commitWrite(ROW_ID, clock.now()),
                    () -> read(ROW_ID, clock.now()),
                    () -> scanFirstEntry(clock.now())
            );

            assertRowMatches(read(ROW_ID, clock.now()), BINARY_ROW);
        }
    }

    @Test
    void testUpdateAndRead() throws Exception {
        for (int i = 0; i < REPEATS; i++) {
            addWrite(ROW_ID, BINARY_ROW, TX_ID);

            runRace(
                    () -> addWrite(ROW_ID, BINARY_ROW2, TX_ID),
                    () -> read(ROW_ID, clock.now()),
                    () -> scanFirstEntry(clock.now())
            );

            assertRowMatches(read(ROW_ID, clock.now()), BINARY_ROW2);
        }
    }

    @Test
    void testRegularGcAndRead() throws Exception {
        //TODO https://issues.apache.org/jira/browse/IGNITE-18020
        assumeTrue(engine instanceof TestStorageEngine);

        for (int i = 0; i < REPEATS; i++) {
            HybridTimestamp firstCommitTs = addAndCommit(BINARY_ROW);

            addAndCommit(BINARY_ROW2);

            runRace(
                    () -> pollForVacuum(HybridTimestamp.MAX_VALUE),
                    () -> read(ROW_ID, firstCommitTs),
                    () -> scanFirstEntry(firstCommitTs)
            );

            assertNull(pollForVacuum(HybridTimestamp.MAX_VALUE));

            cleanup();
        }
    }

    @Test
    void testTombstoneGcAndRead() throws Exception {
        //TODO https://issues.apache.org/jira/browse/IGNITE-18020
        assumeTrue(engine instanceof TestStorageEngine);

        for (int i = 0; i < REPEATS; i++) {
            HybridTimestamp firstCommitTs = addAndCommit(BINARY_ROW);

            addAndCommit(null);

            runRace(
                    () -> pollForVacuum(HybridTimestamp.MAX_VALUE),
                    () -> read(ROW_ID, firstCommitTs),
                    () -> scanFirstEntry(firstCommitTs)
            );

            assertNull(pollForVacuum(HybridTimestamp.MAX_VALUE));
        }
    }

    @Test
    void testTombstoneGcAndAddWrite() throws Exception {
        //TODO https://issues.apache.org/jira/browse/IGNITE-18020
        assumeTrue(engine instanceof TestStorageEngine);

        for (int i = 0; i < REPEATS; i++) {
            addAndCommit(BINARY_ROW);

            addAndCommit(null);

            runRace(
                    () -> pollForVacuum(HybridTimestamp.MAX_VALUE),
                    () -> addWrite(ROW_ID, BINARY_ROW2, TX_ID)
            );

            assertRowMatches(read(ROW_ID, HybridTimestamp.MAX_VALUE), BINARY_ROW2);

            abortWrite(ROW_ID);

            assertNull(storage.closestRowId(ROW_ID));
        }
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    private void scanFirstEntry(HybridTimestamp firstCommitTs) {
        try (var cursor = scan(firstCommitTs)) {
            cursor.hasNext();
        }
    }

    /**
     * Adds a tombstone and cleans a GC queue until nothing's there.
     */
    private void cleanup() {
        addAndCommit(null);

        BinaryRowAndRowId row;

        do {
            row = pollForVacuum(HybridTimestamp.MAX_VALUE);
        } while (row != null);
    }
}
