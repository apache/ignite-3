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
import static org.hamcrest.Matchers.not;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Set;
import java.util.UUID;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.storage.RowId;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/** Tests indexes cleaning up on data removal and transaction abortions. */
public class IndexCleanupTest extends IndexBaseTest {
    @ParameterizedTest
    @EnumSource(AddWrite.class)
    void testAbort(AddWrite writer) {
        UUID rowUuid = UUID.randomUUID();
        RowId rowId = new RowId(PARTITION_ID, rowUuid);

        BinaryRow row = defaultRow();

        writer.addWrite(storageUpdateHandler, rowUuid, row);

        assertThat(pkInnerStorage.allRowsIds(), contains(rowId));
        assertThat(sortedInnerStorage.allRowsIds(), contains(rowId));
        assertThat(hashInnerStorage.allRowsIds(), contains(rowId));

        storageUpdateHandler.switchWriteIntents(getTxId(), false, null, null);

        assertTrue(pkInnerStorage.allRowsIds().isEmpty());
        assertTrue(sortedInnerStorage.allRowsIds().isEmpty());
        assertTrue(hashInnerStorage.allRowsIds().isEmpty());
    }

    @ParameterizedTest
    @EnumSource(AddWrite.class)
    void testTombstoneCleansUpIndexes(AddWrite writer) {
        UUID rowUuid = UUID.randomUUID();
        RowId rowId = new RowId(PARTITION_ID, rowUuid);

        BinaryRow row = defaultRow();

        writer.addWrite(storageUpdateHandler, rowUuid, row);

        // Write intent is in the storage.
        assertTrue(storage.read(rowId, HybridTimestamp.MAX_VALUE).isWriteIntent());

        // Indexes are already in the storage.
        assertThat(pkInnerStorage.allRowsIds(), contains(rowId));
        assertThat(sortedInnerStorage.allRowsIds(), contains(rowId));
        assertThat(hashInnerStorage.allRowsIds(), contains(rowId));

        writer.addWrite(storageUpdateHandler, rowUuid, null);

        // Write intent is in the storage.
        assertTrue(storage.read(rowId, HybridTimestamp.MAX_VALUE).isWriteIntent());

        // But indexes are removed.
        assertTrue(pkInnerStorage.allRowsIds().isEmpty());
        assertTrue(sortedInnerStorage.allRowsIds().isEmpty());
        assertTrue(hashInnerStorage.allRowsIds().isEmpty());
    }

    @ParameterizedTest
    @EnumSource(AddWrite.class)
    void testAbortTombstone(AddWrite writer) {
        UUID rowUuid = UUID.randomUUID();

        BinaryRow row = defaultRow();

        writer.addWrite(storageUpdateHandler, rowUuid, row);
        writer.addWrite(storageUpdateHandler, rowUuid, null);

        storageUpdateHandler.switchWriteIntents(getTxId(), false, null, null);

        assertTrue(pkInnerStorage.allRowsIds().isEmpty());
        assertTrue(sortedInnerStorage.allRowsIds().isEmpty());
        assertTrue(hashInnerStorage.allRowsIds().isEmpty());
    }

    @ParameterizedTest
    @EnumSource(AddWrite.class)
    void testAbortConsecutiveTxWithMatchingIndexes(AddWrite writer) {
        UUID rowUuid1 = UUID.randomUUID();
        UUID rowUuid2 = UUID.randomUUID();
        RowId rowId1 = new RowId(PARTITION_ID, rowUuid1);
        RowId rowId2 = new RowId(PARTITION_ID, rowUuid2);

        var key1 = new TestKey(1, "foo");
        var key2 = new TestKey(2, "baz");
        var value = new TestValue(2, "bar");

        writer.addWrite(storageUpdateHandler, rowUuid1, binaryRow(key1, value));
        commitWrite(rowId1);

        writer.addWrite(storageUpdateHandler, rowUuid2, binaryRow(key2, value));

        storageUpdateHandler.switchWriteIntents(getTxId(), false, null, null);

        Set<RowId> pkRows = pkInnerStorage.allRowsIds();
        Set<RowId> sortedRows = sortedInnerStorage.allRowsIds();
        Set<RowId> hashRows = hashInnerStorage.allRowsIds();

        assertThat(pkRows, contains(rowId1));
        assertThat(sortedRows, contains(rowId1));
        assertThat(hashRows, contains(rowId1));

        assertThat(pkRows, not(contains(rowId2)));
        assertThat(sortedRows, not(contains(rowId2)));
        assertThat(hashRows, not(contains(rowId2)));
    }

    @ParameterizedTest
    @EnumSource(AddWrite.class)
    void testAbortConsecutiveTxWithMatchingIndexesSameRow(AddWrite writer) {
        UUID rowUuid = UUID.randomUUID();
        RowId rowId = new RowId(PARTITION_ID, rowUuid);

        var key = new TestKey(PARTITION_ID, "foo");

        BinaryRow row1 = binaryRow(key, new TestValue(2, "bar"));
        BinaryRow row2 = binaryRow(key, new TestValue(3, "baz"));

        writer.addWrite(storageUpdateHandler, rowUuid, row1);
        commitWrite(rowId);

        writer.addWrite(storageUpdateHandler, rowUuid, row2);

        storageUpdateHandler.switchWriteIntents(getTxId(), false, null, null);

        assertTrue(inAllIndexes(row1));
        assertTrue(inIndexes(row2, true, false));
    }

    @ParameterizedTest
    @EnumSource(AddWrite.class)
    void testIndexNotRemovedOnTombstone(AddWrite writer) {
        UUID rowUuid = UUID.randomUUID();
        RowId rowId = new RowId(PARTITION_ID, rowUuid);

        BinaryRow row = defaultRow();

        writer.addWrite(storageUpdateHandler, rowUuid, row);
        commitWrite(rowId);

        writer.addWrite(storageUpdateHandler, rowUuid, null);

        storageUpdateHandler.switchWriteIntents(getTxId(), false, null, null);

        assertThat(pkInnerStorage.allRowsIds(), contains(rowId));
        assertThat(sortedInnerStorage.allRowsIds(), contains(rowId));
        assertThat(hashInnerStorage.allRowsIds(), contains(rowId));
    }

    @ParameterizedTest
    @EnumSource(AddWrite.class)
    void testIndexNotRemovedWhileAbortingIfPreviousVersionExists(AddWrite writer) {
        UUID rowUuid = UUID.randomUUID();
        RowId rowId = new RowId(PARTITION_ID, rowUuid);

        BinaryRow row = defaultRow();

        writer.addWrite(storageUpdateHandler, rowUuid, row);
        commitWrite(rowId);

        writer.addWrite(storageUpdateHandler, rowUuid, row);

        storageUpdateHandler.switchWriteIntents(getTxId(), false, null, null);

        assertThat(pkInnerStorage.allRowsIds(), contains(rowId));
        assertThat(sortedInnerStorage.allRowsIds(), contains(rowId));
        assertThat(hashInnerStorage.allRowsIds(), contains(rowId));
    }

    @ParameterizedTest
    @EnumSource(AddWrite.class)
    void testIndexNotRemovedWhileWritingIfPreviousVersionExists(AddWrite writer) {
        UUID rowUuid = UUID.randomUUID();
        RowId rowId = new RowId(PARTITION_ID, rowUuid);

        BinaryRow row = defaultRow();

        writer.addWrite(storageUpdateHandler, rowUuid, row);
        commitWrite(rowId);

        writer.addWrite(storageUpdateHandler, rowUuid, row);
        writer.addWrite(storageUpdateHandler, rowUuid, null);

        assertThat(pkInnerStorage.allRowsIds(), contains(rowId));
        assertThat(sortedInnerStorage.allRowsIds(), contains(rowId));
        assertThat(hashInnerStorage.allRowsIds(), contains(rowId));
    }

    @ParameterizedTest
    @EnumSource(AddWrite.class)
    void testIndexNotRemovedWhileWritingIfMultiplePreviousVersionsExists(AddWrite writer) {
        UUID rowUuid = UUID.randomUUID();
        RowId rowId = new RowId(PARTITION_ID, rowUuid);

        BinaryRow row = defaultRow();

        writer.addWrite(storageUpdateHandler, rowUuid, row);
        commitWrite(rowId);

        writer.addWrite(storageUpdateHandler, rowUuid, row);
        commitWrite(rowId);

        writer.addWrite(storageUpdateHandler, rowUuid, row);
        writer.addWrite(storageUpdateHandler, rowUuid, null);

        assertThat(pkInnerStorage.allRowsIds(), contains(rowId));
        assertThat(sortedInnerStorage.allRowsIds(), contains(rowId));
        assertThat(hashInnerStorage.allRowsIds(), contains(rowId));
    }
}
