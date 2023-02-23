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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
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
        RowId rowId = new RowId(1, rowUuid);

        BinaryRow row = defaultRow();

        addWrite(storageUpdateHandler, rowUuid, row);
        commitWrite(rowId);

        addWrite(storageUpdateHandler, rowUuid, row);
        commitWrite(rowId);

        assertEquals(2, getRowVersions(rowId).size());
        assertThat(pkInnerStorage.allRowsIds(), contains(rowId));
        assertThat(sortedInnerStorage.allRowsIds(), contains(rowId));
        assertThat(hashInnerStorage.allRowsIds(), contains(rowId));

        assertTrue(storageUpdateHandler.vacuum(now()));

        assertEquals(1, getRowVersions(rowId).size());
        // Newer entry has the same index value, so it should not be removed.
        assertTrue(inIndex(row));
    }

    @Test
    void testRemoveStaleEntriesWithDifferentIndexes() {
        UUID rowUuid = UUID.randomUUID();
        RowId rowId = new RowId(1, rowUuid);

        var key1 = new TestKey(1, "foo");
        var value1 = new TestValue(2, "bar");
        BinaryRow tableRow1 = binaryRow(key1, value1);

        var key2 = new TestKey(1, "foo");
        var value2 = new TestValue(5, "baz");
        BinaryRow tableRow2 = binaryRow(key2, value2);

        addWrite(storageUpdateHandler, rowUuid, tableRow1);
        commitWrite(rowId);

        addWrite(storageUpdateHandler, rowUuid, tableRow1);
        commitWrite(rowId);

        addWrite(storageUpdateHandler, rowUuid, tableRow2);
        commitWrite(rowId);

        assertEquals(3, getRowVersions(rowId).size());
        assertThat(pkInnerStorage.allRowsIds(), contains(rowId));
        assertThat(sortedInnerStorage.allRowsIds(), contains(rowId));
        assertThat(hashInnerStorage.allRowsIds(), contains(rowId));

        HybridTimestamp afterCommits = now();

        assertTrue(storageUpdateHandler.vacuum(afterCommits));
        assertTrue(storageUpdateHandler.vacuum(afterCommits));
        assertFalse(storageUpdateHandler.vacuum(afterCommits));

        assertEquals(1, getRowVersions(rowId).size());
        // Older entries have different indexes, should be removed.
        assertFalse(inIndex(tableRow1));
        assertTrue(inIndex(tableRow2));
    }

    @Test
    void testRemoveTombstonesRowNullNull() {
        UUID rowUuid = UUID.randomUUID();
        RowId rowId = new RowId(1, rowUuid);

        BinaryRow row = defaultRow();

        addWrite(storageUpdateHandler, rowUuid, row);
        commitWrite(rowId);

        addWrite(storageUpdateHandler, rowUuid, null);
        commitWrite(rowId);

        // Second tombstone won't be actually put into the storage, but still, let's check.
        addWrite(storageUpdateHandler, rowUuid, null);
        commitWrite(rowId);

        assertEquals(2, getRowVersions(rowId).size());
        assertThat(pkInnerStorage.allRowsIds(), contains(rowId));
        assertThat(sortedInnerStorage.allRowsIds(), contains(rowId));
        assertThat(hashInnerStorage.allRowsIds(), contains(rowId));

        HybridTimestamp afterCommits = now();

        assertTrue(storageUpdateHandler.vacuum(afterCommits));
        assertFalse(storageUpdateHandler.vacuum(afterCommits));

        assertEquals(0, getRowVersions(rowId).size());
        // The last entry was a tombstone, so no indexes should be left.
        assertFalse(inIndex(row));
    }

    @Test
    void testRemoveTombstonesNullRowRow() {
        UUID rowUuid = UUID.randomUUID();
        RowId rowId = new RowId(1, rowUuid);

        BinaryRow row = defaultRow();

        addWrite(storageUpdateHandler, rowUuid, null);
        commitWrite(rowId);

        addWrite(storageUpdateHandler, rowUuid, row);
        commitWrite(rowId);

        addWrite(storageUpdateHandler, rowUuid, row);
        commitWrite(rowId);

        assertEquals(3, getRowVersions(rowId).size());
        assertThat(pkInnerStorage.allRowsIds(), contains(rowId));
        assertThat(sortedInnerStorage.allRowsIds(), contains(rowId));
        assertThat(hashInnerStorage.allRowsIds(), contains(rowId));

        HybridTimestamp afterCommits = now();

        assertTrue(storageUpdateHandler.vacuum(afterCommits));
        assertTrue(storageUpdateHandler.vacuum(afterCommits));
        assertFalse(storageUpdateHandler.vacuum(afterCommits));

        assertEquals(1, getRowVersions(rowId).size());
        assertTrue(inIndex(row));
    }

    @Test
    void testRemoveTombstonesRowNullRow() {
        UUID rowUuid = UUID.randomUUID();
        RowId rowId = new RowId(1, rowUuid);

        BinaryRow row = defaultRow();

        addWrite(storageUpdateHandler, rowUuid, row);
        commitWrite(rowId);

        addWrite(storageUpdateHandler, rowUuid, null);
        commitWrite(rowId);

        addWrite(storageUpdateHandler, rowUuid, row);
        commitWrite(rowId);

        assertEquals(3, getRowVersions(rowId).size());
        assertThat(pkInnerStorage.allRowsIds(), contains(rowId));
        assertThat(sortedInnerStorage.allRowsIds(), contains(rowId));
        assertThat(hashInnerStorage.allRowsIds(), contains(rowId));

        HybridTimestamp afterCommits = now();

        assertTrue(storageUpdateHandler.vacuum(afterCommits));
        assertFalse(storageUpdateHandler.vacuum(afterCommits));

        assertEquals(1, getRowVersions(rowId).size());
        assertTrue(inIndex(row));
    }

    @Test
    void testRemoveTombstonesRowRowNull() {
        UUID rowUuid = UUID.randomUUID();
        RowId rowId = new RowId(1, rowUuid);

        BinaryRow row = defaultRow();

        addWrite(storageUpdateHandler, rowUuid, row);
        commitWrite(rowId);

        addWrite(storageUpdateHandler, rowUuid, row);
        commitWrite(rowId);

        addWrite(storageUpdateHandler, rowUuid, null);
        commitWrite(rowId);

        assertEquals(3, getRowVersions(rowId).size());
        assertThat(pkInnerStorage.allRowsIds(), contains(rowId));
        assertThat(sortedInnerStorage.allRowsIds(), contains(rowId));
        assertThat(hashInnerStorage.allRowsIds(), contains(rowId));

        HybridTimestamp afterCommits = now();

        assertTrue(storageUpdateHandler.vacuum(afterCommits));
        assertTrue(storageUpdateHandler.vacuum(afterCommits));
        assertFalse(storageUpdateHandler.vacuum(afterCommits));

        assertEquals(0, getRowVersions(rowId).size());
        assertFalse(inIndex(row));
    }
}
