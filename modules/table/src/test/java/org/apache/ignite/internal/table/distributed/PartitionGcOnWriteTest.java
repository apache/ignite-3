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

import static java.util.Collections.singletonMap;
import static java.util.stream.Collectors.toList;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.distributed.TestPartitionDataStorage;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.configuration.storage.DataStorageConfiguration;
import org.apache.ignite.internal.storage.BaseMvStoragesTest;
import org.apache.ignite.internal.storage.ReadResult;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.storage.impl.TestMvPartitionStorage;
import org.apache.ignite.internal.table.distributed.replicator.TablePartitionId;
import org.apache.ignite.internal.table.impl.DummyInternalTableImpl;
import org.apache.ignite.internal.util.Cursor;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/** Tests for cooperative GC (GC that is executed on write). */
@ExtendWith(ConfigurationExtension.class)
public class PartitionGcOnWriteTest extends BaseMvStoragesTest {
    private static final int WRITES_COUNT = 10;
    private static final int GC_BATCH_SIZE = 7;
    private static final UUID TX_ID = UUID.randomUUID();
    private static final int PARTITION_ID = 1;
    private static final TablePartitionId TABLE_PARTITION_ID = new TablePartitionId(UUID.randomUUID(), PARTITION_ID);

    private TestMvPartitionStorage storage;
    private StorageUpdateHandler storageUpdateHandler;

    @BeforeEach
    void setUp(@InjectConfiguration("mock.gcOnUpdateBatchSize=" + GC_BATCH_SIZE) DataStorageConfiguration dsCfg) {
        storage = new TestMvPartitionStorage(1);

        storageUpdateHandler = new StorageUpdateHandler(
                1,
                new TestPartitionDataStorage(storage),
                DummyInternalTableImpl.createTableIndexStoragesSupplier(Map.of()),
                dsCfg
        );
    }

    @ParameterizedTest
    @EnumSource(AddWriteWithGc.class)
    void testNullLwm(AddWriteWithGc writer) {
        RowId rowId = fillWithDataForGc();

        writeWithGc(writer, null);

        assertEquals(WRITES_COUNT, getRowVersions(rowId).size());
    }

    @ParameterizedTest
    @EnumSource(AddWriteWithGc.class)
    void testOlderLwm(AddWriteWithGc writer) {
        HybridTimestamp older = clock.now();

        HybridTimestamp newer = clock.now();

        writeWithGc(writer, newer);

        RowId rowId = fillWithDataForGc();

        writeWithGc(writer, older);

        assertEquals(WRITES_COUNT, getRowVersions(rowId).size());
    }

    @ParameterizedTest
    @EnumSource(AddWriteWithGc.class)
    void testNewerLwm(AddWriteWithGc writer) {
        RowId rowId = fillWithDataForGc();

        writeWithGc(writer, clock.now());

        assertEquals(WRITES_COUNT - GC_BATCH_SIZE, getRowVersions(rowId).size());
    }

    private List<ReadResult> getRowVersions(RowId rowId) {
        try (Cursor<ReadResult> readResults = storage.scanVersions(rowId)) {
            return readResults.stream().collect(toList());
        }
    }

    private void writeWithGc(AddWriteWithGc writer, @Nullable HybridTimestamp lwm) {
        UUID rowUuid = UUID.randomUUID();

        TestKey key = new TestKey(1337, "leet");
        BinaryRow row = binaryRow(key, new TestValue(999, "bar"));

        writer.addWrite(storageUpdateHandler, rowUuid, row, lwm);
    }

    private RowId fillWithDataForGc() {
        UUID rowUuid = UUID.randomUUID();
        var rowId = new RowId(PARTITION_ID, rowUuid);

        TestKey key = new TestKey(1, "foo");

        for (int i = 0; i < WRITES_COUNT; i++) {
            BinaryRow row = binaryRow(key, new TestValue(i, "bar" + i));

            addWrite(storageUpdateHandler, rowUuid, row);

            commitWrite(rowId);
        }

        return rowId;
    }

    private static void addWrite(StorageUpdateHandler handler, UUID rowUuid, @Nullable BinaryRow row) {
        handler.handleUpdate(
                TX_ID,
                rowUuid,
                TABLE_PARTITION_ID,
                row == null ? null : row.byteBuffer(),
                (unused) -> {}
        );
    }

    private void commitWrite(RowId rowId) {
        storage.runConsistently(() -> {
            storage.commitWrite(rowId, clock.now());

            return null;
        });
    }

    /** Enum that encapsulates update API. */
    enum AddWriteWithGc {
        /** Uses update api. */
        USE_UPDATE {
            @Override
            void addWrite(StorageUpdateHandler handler, TablePartitionId partitionId, UUID rowUuid, @Nullable BinaryRow row,
                    @Nullable HybridTimestamp lwm) {
                handler.handleUpdate(
                        TX_ID,
                        rowUuid,
                        partitionId,
                        row == null ? null : row.byteBuffer(),
                        (unused) -> {},
                        lwm
                );
            }
        },
        /** Uses updateAll api. */
        USE_UPDATE_ALL {
            @Override
            void addWrite(StorageUpdateHandler handler, TablePartitionId partitionId, UUID rowUuid, @Nullable BinaryRow row,
                    @Nullable HybridTimestamp lwm) {
                handler.handleUpdateAll(
                        TX_ID,
                        singletonMap(rowUuid, row == null ? null : row.byteBuffer()),
                        partitionId,
                        (unused) -> {},
                        lwm
                );
            }
        };

        void addWrite(StorageUpdateHandler handler, UUID rowUuid, @Nullable BinaryRow row, @Nullable HybridTimestamp lwm) {
            TablePartitionId tablePartitionId = new TablePartitionId(UUID.randomUUID(), 1);

            addWrite(handler, tablePartitionId, rowUuid, row, lwm);
        }

        abstract void addWrite(StorageUpdateHandler handler, TablePartitionId partitionId, UUID rowUuid, @Nullable BinaryRow row,
                @Nullable HybridTimestamp lwm);
    }
}
