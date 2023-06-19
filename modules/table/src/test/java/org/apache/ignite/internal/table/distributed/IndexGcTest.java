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

package org.apache.ignite.internal.table.distributed;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.UUID;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.storage.RowId;
import org.junit.jupiter.api.Test;

/** Tests indexes cleaning up on garbage collection. */
public class IndexGcTest extends IndexBaseTest {
    @Test
    void testRemoveStaleEntryWithSameIndex() {
        UUID rowUuid = UUID.randomUUID();
        RowId rowId = new RowId(PARTITION_ID, rowUuid);

        BinaryRow row = defaultRow();

        addWrite(storageUpdateHandler, rowUuid, row);
        commitWrite(rowId);

        addWrite(storageUpdateHandler, rowUuid, row);
        commitWrite(rowId);

        assertTrue(gcUpdateHandler.vacuumBatch(now(), 1, true));

        assertEquals(1, getRowVersions(rowId).size());
        // Newer entry has the same index value, so it should not be removed.
        assertTrue(inAllIndexes(row));
    }

    @Test
    void testRemoveStaleEntriesWithDifferentIndexes() {
        UUID rowUuid = UUID.randomUUID();
        RowId rowId = new RowId(PARTITION_ID, rowUuid);

        var key = new TestKey(1, "foo");

        BinaryRow row1 = binaryRow(key, new TestValue(2, "bar"));
        BinaryRow row2 = binaryRow(key, new TestValue(5, "baz"));

        addWrite(storageUpdateHandler, rowUuid, row1);
        commitWrite(rowId);

        addWrite(storageUpdateHandler, rowUuid, row1);
        commitWrite(rowId);

        addWrite(storageUpdateHandler, rowUuid, row2);
        commitWrite(rowId);

        HybridTimestamp afterCommits = now();

        assertTrue(gcUpdateHandler.vacuumBatch(afterCommits, 1, true));

        // row1 should still be in the index, because second write was identical to the first.
        assertTrue(inAllIndexes(row1));

        assertTrue(gcUpdateHandler.vacuumBatch(afterCommits, 1, true));
        assertFalse(gcUpdateHandler.vacuumBatch(afterCommits, 1, true));

        assertEquals(1, getRowVersions(rowId).size());
        // Older entries have different indexes, should be removed.
        assertTrue(inIndexes(row1, true, false));
        assertTrue(inAllIndexes(row2));
    }

    @Test
    void testRemoveTombstonesRowNullNull() {
        UUID rowUuid = UUID.randomUUID();
        RowId rowId = new RowId(PARTITION_ID, rowUuid);

        BinaryRow row = defaultRow();

        addWrite(storageUpdateHandler, rowUuid, row);
        commitWrite(rowId);

        addWrite(storageUpdateHandler, rowUuid, null);
        commitWrite(rowId);

        // Second tombstone won't be actually put into the storage, but still, let's check.
        addWrite(storageUpdateHandler, rowUuid, null);
        commitWrite(rowId);

        HybridTimestamp afterCommits = now();

        assertTrue(gcUpdateHandler.vacuumBatch(afterCommits, 1, true));
        assertFalse(gcUpdateHandler.vacuumBatch(afterCommits, 1, true));

        assertEquals(0, getRowVersions(rowId).size());
        // The last entry was a tombstone, so no indexes should be left.
        assertTrue(notInAnyIndex(row));
    }

    @Test
    void testRemoveTombstonesRowNullRow() {
        UUID rowUuid = UUID.randomUUID();
        RowId rowId = new RowId(PARTITION_ID, rowUuid);

        BinaryRow row = defaultRow();

        addWrite(storageUpdateHandler, rowUuid, row);
        commitWrite(rowId);

        addWrite(storageUpdateHandler, rowUuid, null);
        commitWrite(rowId);

        addWrite(storageUpdateHandler, rowUuid, row);
        commitWrite(rowId);

        HybridTimestamp afterCommits = now();

        assertTrue(gcUpdateHandler.vacuumBatch(afterCommits, 1, true));
        assertFalse(gcUpdateHandler.vacuumBatch(afterCommits, 1, true));

        assertEquals(1, getRowVersions(rowId).size());
        assertTrue(inAllIndexes(row));
    }

    @Test
    void testRemoveTombstonesRowRowNull() {
        UUID rowUuid = UUID.randomUUID();
        RowId rowId = new RowId(PARTITION_ID, rowUuid);

        BinaryRow row = defaultRow();

        addWrite(storageUpdateHandler, rowUuid, row);
        commitWrite(rowId);

        addWrite(storageUpdateHandler, rowUuid, row);
        commitWrite(rowId);

        addWrite(storageUpdateHandler, rowUuid, null);
        commitWrite(rowId);

        HybridTimestamp afterCommits = now();

        assertTrue(gcUpdateHandler.vacuumBatch(afterCommits, 1, true));
        assertTrue(gcUpdateHandler.vacuumBatch(afterCommits, 1, true));
        assertFalse(gcUpdateHandler.vacuumBatch(afterCommits, 1, true));

        assertEquals(0, getRowVersions(rowId).size());
        assertTrue(notInAnyIndex(row));
    }
}
