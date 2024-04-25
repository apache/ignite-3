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
import org.apache.ignite.internal.catalog.descriptors.CatalogIndexStatus;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.BinaryRowConverter;
import org.apache.ignite.internal.schema.BinaryTuple;
import org.apache.ignite.internal.schema.BinaryTupleSchema;
import org.apache.ignite.internal.schema.ColumnsExtractor;
import org.apache.ignite.internal.schema.configuration.StorageUpdateConfiguration;
import org.apache.ignite.internal.storage.BaseMvStoragesTest;
import org.apache.ignite.internal.storage.ReadResult;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.storage.TestStorageUtils;
import org.apache.ignite.internal.storage.impl.TestMvPartitionStorage;
import org.apache.ignite.internal.storage.index.StorageHashIndexDescriptor;
import org.apache.ignite.internal.storage.index.StorageHashIndexDescriptor.StorageHashIndexColumnDescriptor;
import org.apache.ignite.internal.storage.index.StorageSortedIndexDescriptor;
import org.apache.ignite.internal.storage.index.StorageSortedIndexDescriptor.StorageSortedIndexColumnDescriptor;
import org.apache.ignite.internal.storage.index.impl.TestHashIndexStorage;
import org.apache.ignite.internal.storage.index.impl.TestSortedIndexStorage;
import org.apache.ignite.internal.table.distributed.gc.GcUpdateHandler;
import org.apache.ignite.internal.table.distributed.index.IndexUpdateHandler;
import org.apache.ignite.internal.table.distributed.replication.request.BinaryRowMessage;
import org.apache.ignite.internal.table.distributed.replicator.TimedBinaryRow;
import org.apache.ignite.internal.table.impl.DummyInternalTableImpl;
import org.apache.ignite.internal.type.NativeTypes;
import org.apache.ignite.internal.util.Cursor;
import org.apache.ignite.internal.util.PendingComparableValuesTracker;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.BeforeEach;

/**
 * Base test for indexes. Sets up a table with (int, string) key and (int, string) value and three indexes: primary key, hash index over
 * value columns and sorted index over value columns.
 */
public abstract class IndexBaseTest extends BaseMvStoragesTest {
    protected static final int PARTITION_ID = 0;

    private static final TableMessagesFactory MSG_FACTORY = new TableMessagesFactory();

    private static final BinaryTupleSchema TUPLE_SCHEMA = BinaryTupleSchema.createRowSchema(SCHEMA_DESCRIPTOR);

    private static final BinaryTupleSchema PK_INDEX_SCHEMA = BinaryTupleSchema.createKeySchema(SCHEMA_DESCRIPTOR);

    private static final ColumnsExtractor PK_INDEX_BINARY_TUPLE_CONVERTER = new BinaryRowConverter(TUPLE_SCHEMA, PK_INDEX_SCHEMA);

    private static final int[] USER_INDEX_COLS = {
            SCHEMA_DESCRIPTOR.column("INTVAL").positionInRow(),
            SCHEMA_DESCRIPTOR.column("STRVAL").positionInRow()
    };

    @InjectConfiguration
    private StorageUpdateConfiguration storageUpdateConfiguration;

    private static final BinaryTupleSchema USER_INDEX_SCHEMA = BinaryTupleSchema.createSchema(SCHEMA_DESCRIPTOR, USER_INDEX_COLS);

    private static final ColumnsExtractor USER_INDEX_BINARY_TUPLE_CONVERTER = new BinaryRowConverter(TUPLE_SCHEMA, USER_INDEX_SCHEMA);

    private static final UUID TX_ID = UUID.randomUUID();

    TestHashIndexStorage pkInnerStorage;
    TestSortedIndexStorage sortedInnerStorage;
    TestHashIndexStorage hashInnerStorage;
    TestMvPartitionStorage storage;
    StorageUpdateHandler storageUpdateHandler;

    GcUpdateHandler gcUpdateHandler;

    public static UUID getTxId() {
        return TX_ID;
    }

    @BeforeEach
    void setUp() {
        int tableId = 1;
        int pkIndexId = 2;
        int sortedIndexId = 3;
        int hashIndexId = 4;

        pkInnerStorage = new TestHashIndexStorage(
                PARTITION_ID,
                new StorageHashIndexDescriptor(
                        pkIndexId,
                        List.of(
                                new StorageHashIndexColumnDescriptor("INTKEY", NativeTypes.INT32, false),
                                new StorageHashIndexColumnDescriptor("STRKEY", NativeTypes.STRING, false)
                        ),
                        true
                )
        );

        TableSchemaAwareIndexStorage pkStorage = new TableSchemaAwareIndexStorage(
                pkIndexId,
                pkInnerStorage,
                PK_INDEX_BINARY_TUPLE_CONVERTER
        );

        sortedInnerStorage = new TestSortedIndexStorage(
                PARTITION_ID,
                new StorageSortedIndexDescriptor(
                        sortedIndexId,
                        List.of(
                                new StorageSortedIndexColumnDescriptor("INTVAL", NativeTypes.INT32, false, true),
                                new StorageSortedIndexColumnDescriptor("STRVAL", NativeTypes.STRING, false, true)
                        ),
                        false
                ),
                indexId -> CatalogIndexStatus.AVAILABLE
        );

        TableSchemaAwareIndexStorage sortedIndexStorage = new TableSchemaAwareIndexStorage(
                sortedIndexId,
                sortedInnerStorage,
                USER_INDEX_BINARY_TUPLE_CONVERTER
        );

        hashInnerStorage = new TestHashIndexStorage(
                PARTITION_ID,
                new StorageHashIndexDescriptor(
                        hashIndexId,
                        List.of(
                                new StorageHashIndexColumnDescriptor("INTVAL", NativeTypes.INT32, false),
                                new StorageHashIndexColumnDescriptor("STRVAL", NativeTypes.STRING, false)
                        ),
                        false
                )
        );

        TableSchemaAwareIndexStorage hashIndexStorage = new TableSchemaAwareIndexStorage(
                hashIndexId,
                hashInnerStorage,
                USER_INDEX_BINARY_TUPLE_CONVERTER
        );

        storage = new TestMvPartitionStorage(PARTITION_ID);

        Map<Integer, TableSchemaAwareIndexStorage> indexes = Map.of(
                pkIndexId, pkStorage,
                sortedIndexId, sortedIndexStorage,
                hashIndexId, hashIndexStorage
        );

        TestPartitionDataStorage partitionDataStorage = new TestPartitionDataStorage(tableId, PARTITION_ID, storage);

        IndexUpdateHandler indexUpdateHandler = new IndexUpdateHandler(DummyInternalTableImpl.createTableIndexStoragesSupplier(indexes));

        gcUpdateHandler = new GcUpdateHandler(
                partitionDataStorage,
                new PendingComparableValuesTracker<>(HybridTimestamp.MAX_VALUE),
                indexUpdateHandler
        );

        storageUpdateHandler = new StorageUpdateHandler(
                PARTITION_ID,
                partitionDataStorage,
                indexUpdateHandler,
                storageUpdateConfiguration
        );

        TestStorageUtils.completeBuiltIndexes(storage, hashInnerStorage, sortedInnerStorage);
    }

    List<ReadResult> getRowVersions(RowId rowId) {
        try (Cursor<ReadResult> readResults = storage.scanVersions(rowId)) {
            return readResults.stream().collect(toList());
        }
    }

    static void addWrite(StorageUpdateHandler handler, UUID rowUuid, @Nullable BinaryRow row) {
        addWrite(handler, rowUuid, row, null);
    }

    static void addWrite(StorageUpdateHandler handler, UUID rowUuid, @Nullable BinaryRow row, @Nullable HybridTimestamp lastCommitTime) {
        TablePartitionId partitionId = new TablePartitionId(333, PARTITION_ID);

        handler.handleUpdate(TX_ID, rowUuid, partitionId, row, false, null, null, lastCommitTime, null);
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
        BinaryTuple pkIndexValue = PK_INDEX_BINARY_TUPLE_CONVERTER.extractColumns(row);
        BinaryTuple userIndexValue = USER_INDEX_BINARY_TUPLE_CONVERTER.extractColumns(row);

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
        storage.runConsistently(locker -> {
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
                // TODO: perhaps need to pass last commit time as a param
                handler.handleUpdate(TX_ID, rowUuid, partitionId, row, true, null, null, null, null);
            }
        },
        /** Uses updateAll api. */
        USE_UPDATE_ALL {
            @Override
            void addWrite(StorageUpdateHandler handler, TablePartitionId partitionId, UUID rowUuid, @Nullable BinaryRow row) {
                BinaryRowMessage rowMessage = row == null
                        ? null
                        : MSG_FACTORY.binaryRowMessage()
                                .binaryTuple(row.tupleSlice())
                                .schemaVersion(row.schemaVersion())
                                .build();

                handler.handleUpdateAll(
                        TX_ID,
                        singletonMap(rowUuid, new TimedBinaryRow(rowMessage == null ? null : rowMessage.asBinaryRow(), null)),
                        partitionId,
                        true,
                        null,
                        null,
                        null
                );
            }
        };

        void addWrite(StorageUpdateHandler handler, UUID rowUuid, @Nullable BinaryRow row) {
            TablePartitionId tablePartitionId = new TablePartitionId(444, PARTITION_ID);

            addWrite(handler, tablePartitionId, rowUuid, row);
        }

        abstract void addWrite(StorageUpdateHandler handler, TablePartitionId partitionId, UUID rowUuid, @Nullable BinaryRow row);
    }
}
