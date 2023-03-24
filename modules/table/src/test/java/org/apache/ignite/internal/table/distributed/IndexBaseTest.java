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

import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.distributed.TestPartitionDataStorage;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.BinaryRowConverter;
import org.apache.ignite.internal.schema.BinaryTuple;
import org.apache.ignite.internal.schema.BinaryTupleSchema;
import org.apache.ignite.internal.schema.NativeTypes;
import org.apache.ignite.internal.schema.configuration.storage.DataStorageConfiguration;
import org.apache.ignite.internal.storage.BaseMvStoragesTest;
import org.apache.ignite.internal.storage.ReadResult;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.storage.impl.TestMvPartitionStorage;
import org.apache.ignite.internal.storage.index.HashIndexDescriptor;
import org.apache.ignite.internal.storage.index.HashIndexDescriptor.HashIndexColumnDescriptor;
import org.apache.ignite.internal.storage.index.SortedIndexDescriptor;
import org.apache.ignite.internal.storage.index.SortedIndexDescriptor.SortedIndexColumnDescriptor;
import org.apache.ignite.internal.storage.index.impl.TestHashIndexStorage;
import org.apache.ignite.internal.storage.index.impl.TestSortedIndexStorage;
import org.apache.ignite.internal.table.distributed.replicator.TablePartitionId;
import org.apache.ignite.internal.util.Cursor;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Base test for indexes. Sets up a table with (int, string) key and (int, string) value and
 * three indexes: primary key, hash index over value columns and sorted index over value columns.
 */
@ExtendWith(ConfigurationExtension.class)
public abstract class IndexBaseTest extends BaseMvStoragesTest {
    private static final int PARTITION_ID = 0;

    private static final BinaryTupleSchema TUPLE_SCHEMA = BinaryTupleSchema.createRowSchema(schemaDescriptor);

    private static final BinaryTupleSchema PK_INDEX_SCHEMA = BinaryTupleSchema.createKeySchema(schemaDescriptor);

    private static final BinaryRowConverter PK_INDEX_BINARY_TUPLE_CONVERTER = new BinaryRowConverter(TUPLE_SCHEMA, PK_INDEX_SCHEMA);

    private static final int[] USER_INDEX_COLS = {
            schemaDescriptor.column("INTVAL").schemaIndex(),
            schemaDescriptor.column("STRVAL").schemaIndex()
    };

    private static final BinaryTupleSchema USER_INDEX_SCHEMA = BinaryTupleSchema.createSchema(schemaDescriptor, USER_INDEX_COLS);

    private static final BinaryRowConverter USER_INDEX_BINARY_TUPLE_CONVERTER = new BinaryRowConverter(TUPLE_SCHEMA, USER_INDEX_SCHEMA);

    private static final UUID TX_ID = UUID.randomUUID();

    TestHashIndexStorage pkInnerStorage;
    TestSortedIndexStorage sortedInnerStorage;
    TestHashIndexStorage hashInnerStorage;
    TestMvPartitionStorage storage;
    StorageUpdateHandler storageUpdateHandler;

    @BeforeEach
    void setUp(@InjectConfiguration DataStorageConfiguration dsCfg) {
        UUID pkIndexId = UUID.randomUUID();
        UUID sortedIndexId = UUID.randomUUID();
        UUID hashIndexId = UUID.randomUUID();

        pkInnerStorage = new TestHashIndexStorage(PARTITION_ID, null);

        TableSchemaAwareIndexStorage pkStorage = new TableSchemaAwareIndexStorage(
                pkIndexId,
                pkInnerStorage,
                PK_INDEX_BINARY_TUPLE_CONVERTER::toTuple
        );

        sortedInnerStorage = new TestSortedIndexStorage(PARTITION_ID, new SortedIndexDescriptor(sortedIndexId, List.of(
                new SortedIndexColumnDescriptor("INTVAL", NativeTypes.INT32, false, true),
                new SortedIndexColumnDescriptor("STRVAL", NativeTypes.STRING, false, true)
        )));

        TableSchemaAwareIndexStorage sortedIndexStorage = new TableSchemaAwareIndexStorage(
                sortedIndexId,
                sortedInnerStorage,
                USER_INDEX_BINARY_TUPLE_CONVERTER::toTuple
        );

        hashInnerStorage = new TestHashIndexStorage(PARTITION_ID, new HashIndexDescriptor(hashIndexId, List.of(
                new HashIndexColumnDescriptor("INTVAL", NativeTypes.INT32, false),
                new HashIndexColumnDescriptor("STRVAL", NativeTypes.STRING, false)
        )));

        TableSchemaAwareIndexStorage hashIndexStorage = new TableSchemaAwareIndexStorage(
                hashIndexId,
                hashInnerStorage,
                USER_INDEX_BINARY_TUPLE_CONVERTER::toTuple
        );

        storage = new TestMvPartitionStorage(PARTITION_ID);

        storageUpdateHandler = new StorageUpdateHandler(PARTITION_ID, new TestPartitionDataStorage(storage),
                () -> Map.of(
                        pkIndexId, pkStorage,
                        sortedIndexId, sortedIndexStorage,
                        hashIndexId, hashIndexStorage
                ),
                dsCfg
        );
    }

    List<ReadResult> getRowVersions(RowId rowId) {
        try (Cursor<ReadResult> readResults = storage.scanVersions(rowId)) {
            return readResults.stream().collect(toList());
        }
    }

    static void addWrite(StorageUpdateHandler handler, UUID rowUuid, @Nullable BinaryRow row) {
        TablePartitionId partitionId = new TablePartitionId(UUID.randomUUID(), PARTITION_ID);

        handler.handleUpdate(
                TX_ID,
                rowUuid,
                partitionId,
                row == null ? null : row.byteBuffer(),
                (unused) -> {}
        );
    }

    static BinaryRow defaultRow() {
        var key = new TestKey(1, "foo");
        var value = new TestValue(2, "bar");

        return binaryRow(key, value);
    }

    boolean inAllIndexes(BinaryRow row) {
        return inIndexes(row, true, true);
    }

    boolean notInAnyIndex(BinaryRow row) {
        return inIndexes(row, false, false);
    }

    boolean inIndexes(BinaryRow row, boolean mustBeInPk, boolean mustBeInUser) {
        BinaryTuple pkIndexValue = PK_INDEX_BINARY_TUPLE_CONVERTER.toTuple(row);
        BinaryTuple userIndexValue = USER_INDEX_BINARY_TUPLE_CONVERTER.toTuple(row);

        assert pkIndexValue != null;
        assert userIndexValue != null;

        try (Cursor<RowId> pkCursor = pkInnerStorage.get(pkIndexValue)) {
            if (pkCursor.hasNext() != mustBeInPk) {
                return false;
            }
        }

        try (Cursor<RowId> sortedIdxCursor = sortedInnerStorage.get(userIndexValue)) {
            if (sortedIdxCursor.hasNext() != mustBeInUser) {
                return false;
            }
        }

        try (Cursor<RowId> hashIdxCursor = hashInnerStorage.get(userIndexValue)) {
            return hashIdxCursor.hasNext() == mustBeInUser;
        }
    }

    HybridTimestamp now() {
        return clock.now();
    }

    void commitWrite(RowId rowId) {
        storage.runConsistently(() -> {
            storage.commitWrite(rowId, now());

            return null;
        });
    }

    /** Enum that encapsulates update API. */
    enum AddWrite {
        /** Uses update api. */
        USE_UPDATE {
            @Override
            void addWrite(StorageUpdateHandler handler, TablePartitionId partitionId, UUID rowUuid, @Nullable BinaryRow row) {
                handler.handleUpdate(
                        TX_ID,
                        rowUuid,
                        partitionId,
                        row == null ? null : row.byteBuffer(),
                        (unused) -> {}
                );
            }
        },
        /** Uses updateAll api. */
        USE_UPDATE_ALL {
            @Override
            void addWrite(StorageUpdateHandler handler, TablePartitionId partitionId, UUID rowUuid, @Nullable BinaryRow row) {
                handler.handleUpdateAll(
                        TX_ID,
                        singletonMap(rowUuid, row == null ? null : row.byteBuffer()),
                        partitionId,
                        (unused) -> {}
                );
            }
        };

        void addWrite(StorageUpdateHandler handler, UUID rowUuid, @Nullable BinaryRow row) {
            TablePartitionId tablePartitionId = new TablePartitionId(UUID.randomUUID(), PARTITION_ID);

            addWrite(handler, tablePartitionId, rowUuid, row);
        }

        abstract void addWrite(StorageUpdateHandler handler, TablePartitionId partitionId, UUID rowUuid, @Nullable BinaryRow row);
    }
}
