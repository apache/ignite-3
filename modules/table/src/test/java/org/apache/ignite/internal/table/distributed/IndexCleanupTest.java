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
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.not;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.ignite.distributed.TestPartitionDataStorage;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.BinaryRowConverter;
import org.apache.ignite.internal.schema.BinaryTupleSchema;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.NativeTypes;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.marshaller.KvMarshaller;
import org.apache.ignite.internal.schema.marshaller.MarshallerException;
import org.apache.ignite.internal.schema.marshaller.MarshallerFactory;
import org.apache.ignite.internal.schema.marshaller.reflection.ReflectionMarshallerFactory;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.storage.impl.TestMvPartitionStorage;
import org.apache.ignite.internal.storage.index.HashIndexDescriptor;
import org.apache.ignite.internal.storage.index.HashIndexDescriptor.HashIndexColumnDescriptor;
import org.apache.ignite.internal.storage.index.SortedIndexDescriptor;
import org.apache.ignite.internal.storage.index.SortedIndexDescriptor.SortedIndexColumnDescriptor;
import org.apache.ignite.internal.storage.index.impl.TestHashIndexStorage;
import org.apache.ignite.internal.storage.index.impl.TestSortedIndexStorage;
import org.apache.ignite.internal.table.distributed.replicator.TablePartitionId;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/** Tests indexes cleaning up on data removal and transaction abortions. */
public class IndexCleanupTest {
    /** Default reflection marshaller factory. */
    private static final MarshallerFactory MARSHALLER_FACTORY = new ReflectionMarshallerFactory();

    private static final SchemaDescriptor SCHEMA_DESCRIPTOR = new SchemaDescriptor(1, new Column[]{
            new Column("INTKEY", NativeTypes.INT32, false),
            new Column("STRKEY", NativeTypes.STRING, false),
    }, new Column[]{
            new Column("INTVAL", NativeTypes.INT32, false),
            new Column("STRVAL", NativeTypes.STRING, false),
    });

    private static final BinaryTupleSchema TUPLE_SCHEMA = BinaryTupleSchema.createRowSchema(SCHEMA_DESCRIPTOR);

    private static final BinaryTupleSchema PK_INDEX_SCHEMA = BinaryTupleSchema.createKeySchema(SCHEMA_DESCRIPTOR);

    private static final BinaryRowConverter PK_INDEX_BINARY_TUPLE_CONVERTER = new BinaryRowConverter(TUPLE_SCHEMA, PK_INDEX_SCHEMA);

    private static final int[] USER_INDEX_COLS = {
            SCHEMA_DESCRIPTOR.column("INTVAL").schemaIndex(),
            SCHEMA_DESCRIPTOR.column("STRVAL").schemaIndex()
    };

    private static final BinaryTupleSchema USER_INDEX_SCHEMA = BinaryTupleSchema.createSchema(SCHEMA_DESCRIPTOR, USER_INDEX_COLS);

    private static final BinaryRowConverter USER_INDEX_BINARY_TUPLE_CONVERTER = new BinaryRowConverter(TUPLE_SCHEMA, USER_INDEX_SCHEMA);

    /** Key-value marshaller for tests. */
    private static final KvMarshaller<TestKey, TestValue> KV_MARSHALLER
            = MARSHALLER_FACTORY.create(SCHEMA_DESCRIPTOR, TestKey.class, TestValue.class);

    private static final UUID TX_ID = UUID.randomUUID();

    private static final HybridClock CLOCK = new HybridClockImpl();

    private TestHashIndexStorage pkInnerStorage;
    private TestSortedIndexStorage sortedInnerStorage;
    private TestHashIndexStorage hashInnerStorage;
    private TestMvPartitionStorage storage;
    private StorageUpdateHandler storageUpdateHandler;

    @BeforeEach
    void setUp() {
        UUID pkIndexId = UUID.randomUUID();
        UUID sortedIndexId = UUID.randomUUID();
        UUID hashIndexId = UUID.randomUUID();

        pkInnerStorage = new TestHashIndexStorage(null);

        TableSchemaAwareIndexStorage pkStorage = new TableSchemaAwareIndexStorage(
                pkIndexId,
                pkInnerStorage,
                PK_INDEX_BINARY_TUPLE_CONVERTER::toTuple
        );

        sortedInnerStorage = new TestSortedIndexStorage(new SortedIndexDescriptor(sortedIndexId, List.of(
                new SortedIndexColumnDescriptor("INTVAL", NativeTypes.INT32, false, true),
                new SortedIndexColumnDescriptor("STRVAL", NativeTypes.STRING, false, true)
        )));

        TableSchemaAwareIndexStorage sortedIndexStorage = new TableSchemaAwareIndexStorage(
                sortedIndexId,
                sortedInnerStorage,
                USER_INDEX_BINARY_TUPLE_CONVERTER::toTuple
        );

        hashInnerStorage = new TestHashIndexStorage(new HashIndexDescriptor(hashIndexId, List.of(
                new HashIndexColumnDescriptor("INTVAL", NativeTypes.INT32, false),
                new HashIndexColumnDescriptor("STRVAL", NativeTypes.STRING, false)
        )));

        TableSchemaAwareIndexStorage hashIndexStorage = new TableSchemaAwareIndexStorage(
                hashIndexId,
                hashInnerStorage,
                USER_INDEX_BINARY_TUPLE_CONVERTER::toTuple
        );

        storage = new TestMvPartitionStorage(1);

        storageUpdateHandler = new StorageUpdateHandler(1, new TestPartitionDataStorage(storage),
                () -> Map.of(
                        pkIndexId, pkStorage,
                        sortedIndexId, sortedIndexStorage,
                        hashIndexId, hashIndexStorage
                )
        );
    }

    @ParameterizedTest
    @EnumSource(AddWrite.class)
    void testAbort(AddWrite writer) {
        UUID rowUuid = UUID.randomUUID();
        RowId rowId = new RowId(1, rowUuid);

        var key = new TestKey(1, "foo");
        var value = new TestValue(2, "bar");
        BinaryRow tableRow = binaryRow(key, value);

        writer.addWrite(storageUpdateHandler, rowUuid, tableRow);

        assertEquals(1, storage.rowsCount());
        assertThat(pkInnerStorage.allRowsIds(), contains(rowId));
        assertThat(sortedInnerStorage.allRowsIds(), contains(rowId));
        assertThat(hashInnerStorage.allRowsIds(), contains(rowId));

        storageUpdateHandler.handleTransactionAbortion(Set.of(rowId), () -> {});

        assertEquals(0, storage.rowsCount());
        assertTrue(pkInnerStorage.allRowsIds().isEmpty());
        assertTrue(sortedInnerStorage.allRowsIds().isEmpty());
        assertTrue(hashInnerStorage.allRowsIds().isEmpty());
    }

    @ParameterizedTest
    @EnumSource(AddWrite.class)
    void testTombstoneCleansUpIndexes(AddWrite writer) {
        UUID rowUuid = UUID.randomUUID();
        RowId rowId = new RowId(1, rowUuid);

        var key = new TestKey(1, "foo");
        var value = new TestValue(2, "bar");
        BinaryRow tableRow = binaryRow(key, value);

        writer.addWrite(storageUpdateHandler, rowUuid, tableRow);

        // Write intent is in the storage.
        assertEquals(1, storage.rowsCount());

        // Indexes are already in the storage.
        assertThat(pkInnerStorage.allRowsIds(), contains(rowId));
        assertThat(sortedInnerStorage.allRowsIds(), contains(rowId));
        assertThat(hashInnerStorage.allRowsIds(), contains(rowId));

        writer.addWrite(storageUpdateHandler, rowUuid, null);

        // Write intent is in the storage.
        assertEquals(1, storage.rowsCount());

        // But indexes are removed.
        assertTrue(pkInnerStorage.allRowsIds().isEmpty());
        assertTrue(sortedInnerStorage.allRowsIds().isEmpty());
        assertTrue(hashInnerStorage.allRowsIds().isEmpty());
    }

    @ParameterizedTest
    @EnumSource(AddWrite.class)
    void testAbortTombstone(AddWrite writer) {
        UUID rowUuid = UUID.randomUUID();
        RowId rowId = new RowId(1, rowUuid);

        var key = new TestKey(1, "foo");
        var value = new TestValue(2, "bar");
        BinaryRow tableRow = binaryRow(key, value);

        writer.addWrite(storageUpdateHandler, rowUuid, tableRow);
        writer.addWrite(storageUpdateHandler, rowUuid, null);

        storageUpdateHandler.handleTransactionAbortion(Set.of(rowId), () -> {});

        assertEquals(0, storage.rowsCount());
        assertTrue(pkInnerStorage.allRowsIds().isEmpty());
        assertTrue(sortedInnerStorage.allRowsIds().isEmpty());
        assertTrue(hashInnerStorage.allRowsIds().isEmpty());
    }

    @ParameterizedTest
    @EnumSource(AddWrite.class)
    void testAbortConsecutiveTxWithMatchingIndexes(AddWrite writer) {
        UUID rowUuid1 = UUID.randomUUID();
        UUID rowUuid2 = UUID.randomUUID();
        RowId rowId1 = new RowId(1, rowUuid1);
        RowId rowId2 = new RowId(1, rowUuid2);

        var key1 = new TestKey(1, "foo");
        var key2 = new TestKey(2, "baz");
        var value = new TestValue(2, "bar");

        writer.addWrite(storageUpdateHandler, rowUuid1, binaryRow(key1, value));
        commitWrite(rowId1);

        writer.addWrite(storageUpdateHandler, rowUuid2, binaryRow(key2, value));

        storageUpdateHandler.handleTransactionAbortion(Set.of(rowId2), () -> {});

        assertEquals(1, storage.rowsCount());

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
    void testIndexNotRemovedOnTombstone(AddWrite writer) {
        UUID rowUuid = UUID.randomUUID();
        RowId rowId = new RowId(1, rowUuid);

        var key = new TestKey(1, "foo");
        var value = new TestValue(2, "bar");
        BinaryRow tableRow = binaryRow(key, value);

        writer.addWrite(storageUpdateHandler, rowUuid, tableRow);
        commitWrite(rowId);

        writer.addWrite(storageUpdateHandler, rowUuid, null);

        storageUpdateHandler.handleTransactionAbortion(Set.of(rowId), () -> {});

        assertEquals(1, storage.rowsCount());
        assertThat(pkInnerStorage.allRowsIds(), contains(rowId));
        assertThat(sortedInnerStorage.allRowsIds(), contains(rowId));
        assertThat(hashInnerStorage.allRowsIds(), contains(rowId));
    }

    @ParameterizedTest
    @EnumSource(AddWrite.class)
    void testIndexNotRemovedWhileAbortingIfPreviousVersionExists(AddWrite writer) {
        UUID rowUuid = UUID.randomUUID();
        RowId rowId = new RowId(1, rowUuid);

        var key = new TestKey(1, "foo");
        var value = new TestValue(2, "bar");
        BinaryRow tableRow = binaryRow(key, value);

        writer.addWrite(storageUpdateHandler, rowUuid, tableRow);
        commitWrite(rowId);

        writer.addWrite(storageUpdateHandler, rowUuid, tableRow);

        storageUpdateHandler.handleTransactionAbortion(Set.of(rowId), () -> {});

        assertEquals(1, storage.rowsCount());
        assertThat(pkInnerStorage.allRowsIds(), contains(rowId));
        assertThat(sortedInnerStorage.allRowsIds(), contains(rowId));
        assertThat(hashInnerStorage.allRowsIds(), contains(rowId));
    }

    @ParameterizedTest
    @EnumSource(AddWrite.class)
    void testIndexNotRemovedWhileWritingIfPreviousVersionExists(AddWrite writer) {
        UUID rowUuid = UUID.randomUUID();
        RowId rowId = new RowId(1, rowUuid);

        var key = new TestKey(1, "foo");
        var value = new TestValue(2, "bar");
        BinaryRow tableRow = binaryRow(key, value);

        writer.addWrite(storageUpdateHandler, rowUuid, tableRow);
        commitWrite(rowId);

        writer.addWrite(storageUpdateHandler, rowUuid, tableRow);
        writer.addWrite(storageUpdateHandler, rowUuid, null);

        assertEquals(1, storage.rowsCount());
        assertThat(pkInnerStorage.allRowsIds(), contains(rowId));
        assertThat(sortedInnerStorage.allRowsIds(), contains(rowId));
        assertThat(hashInnerStorage.allRowsIds(), contains(rowId));
    }

    @ParameterizedTest
    @EnumSource(AddWrite.class)
    void testIndexNotRemovedWhileWritingIfMultiplePreviousVersionsExists(AddWrite writer) {
        UUID rowUuid = UUID.randomUUID();
        RowId rowId = new RowId(1, rowUuid);

        var key = new TestKey(1, "foo");
        var value = new TestValue(2, "bar");
        BinaryRow tableRow = binaryRow(key, value);

        writer.addWrite(storageUpdateHandler, rowUuid, tableRow);
        commitWrite(rowId);

        writer.addWrite(storageUpdateHandler, rowUuid, tableRow);
        commitWrite(rowId);

        writer.addWrite(storageUpdateHandler, rowUuid, tableRow);
        writer.addWrite(storageUpdateHandler, rowUuid, null);

        assertEquals(1, storage.rowsCount());
        assertThat(pkInnerStorage.allRowsIds(), contains(rowId));
        assertThat(sortedInnerStorage.allRowsIds(), contains(rowId));
        assertThat(hashInnerStorage.allRowsIds(), contains(rowId));
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
            TablePartitionId tablePartitionId = new TablePartitionId(UUID.randomUUID(), 1);

            addWrite(handler, tablePartitionId, rowUuid, row);
        }

        abstract void addWrite(StorageUpdateHandler handler, TablePartitionId partitionId, UUID rowUuid, @Nullable BinaryRow row);
    }

    private static BinaryRow binaryRow(TestKey key, TestValue value) {
        try {
            return KV_MARSHALLER.marshal(key, value);
        } catch (MarshallerException e) {
            throw new RuntimeException(e);
        }
    }

    private void commitWrite(RowId rowId) {
        storage.runConsistently(() -> {
            storage.commitWrite(rowId, CLOCK.now());

            return null;
        });
    }

    private static class TestKey {
        int intKey;

        String strKey;

        TestKey() {
        }

        TestKey(int intKey, String strKey) {
            this.intKey = intKey;
            this.strKey = strKey;
        }
    }

    private static class TestValue {
        Integer intVal;

        String strVal;

        TestValue() {
        }

        TestValue(Integer intVal, String strVal) {
            this.intVal = intVal;
            this.strVal = strVal;
        }
    }
}
