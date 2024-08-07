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

import static org.apache.ignite.internal.schema.BinaryRowMatcher.isRow;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.junit.jupiter.api.Test;

/**
 * Abstract test for MV partition storage GC.
 */
public abstract class AbstractMvPartitionStorageGcTest extends BaseMvPartitionStorageTest {
    @Test
    void testEmptyStorage() {
        assertNull(pollForVacuum(CLOCK.now()));
    }

    @Test
    void testSingleValueStorage() {
        addAndCommit(TABLE_ROW);

        assertNull(pollForVacuum(CLOCK.now()));
    }

    @Test
    void testRegularPoll() {
        HybridTimestamp firstCommitTs = addAndCommit(TABLE_ROW);

        HybridTimestamp tsBetweenCommits = CLOCK.now();

        HybridTimestamp secondCommitTs = addAndCommit(TABLE_ROW2);

        // Data is still visible for older timestamps.
        assertNull(pollForVacuum(firstCommitTs));

        assertNull(pollForVacuum(tsBetweenCommits));

        // Once a low watermark value becomes equal to second commit timestamp, previous value
        // becomes completely inaccessible and should be purged.
        BinaryRowAndRowId gcedRow = pollForVacuum(secondCommitTs);

        assertNotNull(gcedRow);

        assertThat(gcedRow.binaryRow(), isRow(TABLE_ROW));

        // Read from the old timestamp should return null.
        assertNull(read(ROW_ID, firstCommitTs));

        // Read from the newer timestamp should return last value.
        assertThat(read(ROW_ID, secondCommitTs), isRow(TABLE_ROW2));
    }

    @Test
    void testPollFromUnderTombstone() {
        addAndCommit(TABLE_ROW);
        HybridTimestamp secondCommitTs = addAndCommit(null);

        BinaryRowAndRowId row = pollForVacuum(secondCommitTs);

        assertNotNull(row);
        assertThat(row.binaryRow(), isRow(TABLE_ROW));

        assertNull(read(ROW_ID, secondCommitTs));

        // Check that tombstone is also deleted from the partition. It must be empty at this point.
        assertNull(pollForVacuum(HybridTimestamp.MAX_VALUE));

        // Let's check that the storage is empty.
        assertNull(storage.closestRowId(ROW_ID));
    }

    @Test
    void testDoubleTombstone() {
        addAndCommit(TABLE_ROW);
        addAndCommit(null);
        HybridTimestamp lastCommitTs = addAndCommit(null);

        BinaryRowAndRowId row = pollForVacuum(lastCommitTs);

        assertNotNull(row);
        assertThat(row.binaryRow(), isRow(TABLE_ROW));

        assertNull(read(ROW_ID, lastCommitTs));

        // Check that all tombstones are deleted from the partition. It must be empty at this point.
        assertNull(pollForVacuum(HybridTimestamp.MAX_VALUE));

        // Let's check that the storage is empty.
        assertNull(storage.closestRowId(ROW_ID));
    }

    @Test
    void testManyOldVersions() {
        addAndCommit(TABLE_ROW);

        addAndCommit(TABLE_ROW2);

        HybridTimestamp lowWatermark = addAndCommit(null);

        // Poll the oldest row.
        BinaryRowAndRowId row = pollForVacuum(lowWatermark);

        assertNotNull(row);
        assertThat(row.binaryRow(), isRow(TABLE_ROW));

        // Poll the next oldest row.
        row = pollForVacuum(lowWatermark);

        assertNotNull(row);
        assertThat(row.binaryRow(), isRow(TABLE_ROW2));

        // Nothing else to poll.
        assertNull(pollForVacuum(lowWatermark));
    }

    @Test
    void testVacuumsSecondRowIfTombstoneIsFirst() {
        addAndCommit(null);

        addAndCommit(TABLE_ROW);

        addAndCommit(TABLE_ROW2);

        BinaryRowAndRowId row = pollForVacuum(HybridTimestamp.MAX_VALUE);

        assertNotNull(row);
        assertThat(row.binaryRow(), isRow(TABLE_ROW));
    }

    @Test
    void testVacuumsSecondRowIfTombstoneIsFirstAddCommitted() {
        addWriteCommitted(ROW_ID, null, CLOCK.now());

        addWriteCommitted(ROW_ID, TABLE_ROW, CLOCK.now());

        addWriteCommitted(ROW_ID, TABLE_ROW2, CLOCK.now());

        BinaryRowAndRowId row = pollForVacuum(HybridTimestamp.MAX_VALUE);

        assertNotNull(row);
        assertThat(row.binaryRow(), isRow(TABLE_ROW));
    }
}
