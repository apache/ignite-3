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

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.schema.BinaryRow;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Abstract test for MV partition storage GC.
 */
@ExtendWith(ConfigurationExtension.class)
public abstract class AbstractMvPartitionStorageGcTest extends BaseMvPartitionStorageTest {
    @Test
    void testEmptyStorage() {
        assertNull(storage.pollForVacuum(clock.now()));
    }

    @Test
    void testSingleValueStorage() {
        addAndCommit(BINARY_ROW);

        assertNull(storage.pollForVacuum(clock.now()));
    }

    @Test
    void testRegularPoll() {
        HybridTimestamp firstCommitTs = addAndCommit(BINARY_ROW);

        HybridTimestamp tsBetweenCommits = clock.now();

        HybridTimestamp secondCommitTs = addAndCommit(BINARY_ROW);

        // Data is still visible for older timestamps.
        assertNull(storage.pollForVacuum(firstCommitTs));

        assertNull(storage.pollForVacuum(tsBetweenCommits));

        // Once a low watermark value becomes equal to second commit timestamp, previous value
        // becomes completely inaccessible and should be purged.
        BinaryRowWithRowId row = storage.pollForVacuum(secondCommitTs);

        assertNotNull(row);

        assertRowMatches(row.binaryRow(), BINARY_ROW);

        // Read from the old timestamp should return null.
        assertNull(read(ROW_ID, firstCommitTs));

        // Read from the newer timestamp should return last value.
        assertRowMatches(read(ROW_ID, secondCommitTs), BINARY_ROW);
    }

    @Test
    void testPollFromUnderTombstone() {
        addAndCommit(BINARY_ROW);
        HybridTimestamp secondCommitTs = addAndCommit(null);

        BinaryRowWithRowId row = storage.pollForVacuum(secondCommitTs);

        assertNotNull(row);
        assertRowMatches(row.binaryRow(), BINARY_ROW);

        assertNull(read(ROW_ID, secondCommitTs));

        // Check that tombstone is also deleted from the partition. It must be empty at this point.
        assertNull(storage.closestRowId(ROW_ID));
    }

    @Test
    void testDoubleTombstone() {
        addAndCommit(BINARY_ROW);
        addAndCommit(null);
        HybridTimestamp lastCommitTs = addAndCommit(null);

        BinaryRowWithRowId row = storage.pollForVacuum(lastCommitTs);

        assertNotNull(row);
        assertRowMatches(row.binaryRow(), BINARY_ROW);

        assertNull(read(ROW_ID, lastCommitTs));

        // Check that all tombstones are deleted from the partition. It must be empty at this point.
        assertNull(storage.closestRowId(ROW_ID));
    }

    @Test
    void testManyOldVersions() {
        addAndCommit(BINARY_ROW);

        BinaryRow binaryRow2 = binaryRow(KEY, new TestValue(50, "50"));

        addAndCommit(binaryRow2);

        HybridTimestamp lowWatermark = addAndCommit(null);

        // Poll the oldest row.
        BinaryRowWithRowId row = pollForVacuum(lowWatermark);

        assertNotNull(row);
        assertRowMatches(row.binaryRow(), BINARY_ROW);

        // Poll the next oldest row.
        row = pollForVacuum(lowWatermark);

        assertNotNull(row);
        assertRowMatches(row.binaryRow(), binaryRow2);

        // Nothing else to poll.
        assertNull(pollForVacuum(lowWatermark));
    }
}
