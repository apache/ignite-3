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

import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toList;
import static org.apache.ignite.internal.storage.MvPartitionStorage.REBALANCE_IN_PROGRESS;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.binarytuple.BinaryTupleBuilder;
import org.apache.ignite.internal.configuration.util.ConfigurationUtil;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.schema.BinaryConverter;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.BinaryTuple;
import org.apache.ignite.internal.schema.BinaryTupleSchema;
import org.apache.ignite.internal.schema.BinaryTupleSchema.Element;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.NativeTypes;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.SchemaUtils;
import org.apache.ignite.internal.schema.configuration.TableConfiguration;
import org.apache.ignite.internal.schema.configuration.TablesConfiguration;
import org.apache.ignite.internal.schema.configuration.index.TableIndexView;
import org.apache.ignite.internal.schema.marshaller.KvMarshaller;
import org.apache.ignite.internal.schema.marshaller.MarshallerException;
import org.apache.ignite.internal.schema.marshaller.reflection.ReflectionMarshallerFactory;
import org.apache.ignite.internal.schema.row.Row;
import org.apache.ignite.internal.schema.testutils.SchemaConfigurationConverter;
import org.apache.ignite.internal.schema.testutils.builder.SchemaBuilders;
import org.apache.ignite.internal.schema.testutils.definition.ColumnType;
import org.apache.ignite.internal.schema.testutils.definition.TableDefinition;
import org.apache.ignite.internal.schema.testutils.definition.index.IndexDefinition;
import org.apache.ignite.internal.storage.engine.MvTableStorage;
import org.apache.ignite.internal.storage.impl.TestMvPartitionStorage;
import org.apache.ignite.internal.storage.index.HashIndexStorage;
import org.apache.ignite.internal.storage.index.IndexDescriptor;
import org.apache.ignite.internal.storage.index.IndexRow;
import org.apache.ignite.internal.storage.index.IndexRowImpl;
import org.apache.ignite.internal.storage.index.IndexStorage;
import org.apache.ignite.internal.storage.index.SortedIndexStorage;
import org.apache.ignite.internal.tostring.S;
import org.apache.ignite.internal.util.Cursor;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lang.IgniteTuple3;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

/**
 * Abstract class that contains tests for {@link MvTableStorage} implementations.
 */
public abstract class AbstractMvTableStorageTest {
    private static final String SORTED_INDEX_NAME = "SORTED_IDX";

    private static final String HASH_INDEX_NAME = "HASH_IDX";

    protected static final int PARTITION_ID = 0;

    /** Partition id for 0 storage. */
    protected static final int PARTITION_ID_0 = 42;

    /** Partition id for 1 storage. */
    protected static final int PARTITION_ID_1 = 1 << 8;

    /** Hybrid clock to generate timestamps. */
    protected final HybridClock clock = new HybridClockImpl();

    private MvTableStorage tableStorage;

    private TableIndexView sortedIdx;

    private TableIndexView hashIdx;

    private SchemaDescriptor schemaDescriptor;

    /**
     * Initializes the internal structures needed for tests.
     *
     * <p>This method *MUST* always be called in either subclass' constructor or setUp method.
     */
    protected final void initialize(MvTableStorage tableStorage, TablesConfiguration tablesCfg) {
        createTestTable(tableStorage.configuration());
        createTestIndexes(tablesCfg);

        this.tableStorage = tableStorage;

        sortedIdx = tablesCfg.indexes().get(SORTED_INDEX_NAME).value();
        hashIdx = tablesCfg.indexes().get(HASH_INDEX_NAME).value();

        schemaDescriptor = SchemaUtils.prepareSchemaDescriptor(1, tableStorage.configuration().value());
    }

    /**
     * Tests that {@link MvTableStorage#getMvPartition(int)} correctly returns an existing partition.
     */
    @Test
    void testCreatePartition() {
        MvPartitionStorage absentStorage = tableStorage.getMvPartition(0);

        assertThat(absentStorage, is(nullValue()));

        MvPartitionStorage partitionStorage = tableStorage.getOrCreateMvPartition(0);

        assertThat(partitionStorage, is(notNullValue()));

        assertThat(partitionStorage, is(sameInstance(tableStorage.getMvPartition(0))));
    }

    /**
     * Tests that partition data does not overlap.
     */
    @Test
    void testPartitionIndependence() throws Exception {
        MvPartitionStorage partitionStorage0 = tableStorage.getOrCreateMvPartition(PARTITION_ID_0);
        // Using a shifted ID value to test a multibyte scenario.
        MvPartitionStorage partitionStorage1 = tableStorage.getOrCreateMvPartition(PARTITION_ID_1);

        var testData0 = binaryRow(new TestKey(1), new TestValue(10));

        UUID txId = UUID.randomUUID();

        RowId rowId0 = new RowId(PARTITION_ID_0);

        partitionStorage0.runConsistently(() -> partitionStorage0.addWrite(rowId0, testData0, txId, UUID.randomUUID(), 0));

        assertThat(unwrap(partitionStorage0.read(rowId0, HybridTimestamp.MAX_VALUE)), is(equalTo(unwrap(testData0))));
        assertThrows(IllegalArgumentException.class, () -> partitionStorage1.read(rowId0, HybridTimestamp.MAX_VALUE));

        var testData1 = binaryRow(new TestKey(2), new TestValue(20));

        RowId rowId1 = new RowId(PARTITION_ID_1);

        partitionStorage1.runConsistently(() -> partitionStorage1.addWrite(rowId1, testData1, txId, UUID.randomUUID(), 0));

        assertThrows(IllegalArgumentException.class, () -> partitionStorage0.read(rowId1, HybridTimestamp.MAX_VALUE));
        assertThat(unwrap(partitionStorage1.read(rowId1, HybridTimestamp.MAX_VALUE)), is(equalTo(unwrap(testData1))));

        assertThat(
                getAll(partitionStorage0.scan(HybridTimestamp.MAX_VALUE)).stream().map(this::unwrap).collect(toList()),
                contains(unwrap(testData0))
        );

        assertThat(
                getAll(partitionStorage1.scan(HybridTimestamp.MAX_VALUE)).stream().map(this::unwrap).collect(toList()),
                contains(unwrap(testData1))
        );
    }

    /**
     * Tests the {@link MvTableStorage#getOrCreateIndex} method.
     */
    @Test
    public void testCreateIndex() {
        assertThrows(StorageException.class, () -> tableStorage.getOrCreateIndex(PARTITION_ID, sortedIdx.id()));
        assertThrows(StorageException.class, () -> tableStorage.getOrCreateIndex(PARTITION_ID, hashIdx.id()));

        // Index should only be available after the associated partition has been created.
        tableStorage.getOrCreateMvPartition(PARTITION_ID);

        assertThat(tableStorage.getOrCreateIndex(PARTITION_ID, sortedIdx.id()), is(instanceOf(SortedIndexStorage.class)));
        assertThat(tableStorage.getOrCreateIndex(PARTITION_ID, hashIdx.id()), is(instanceOf(HashIndexStorage.class)));

        assertThrows(StorageException.class, () -> tableStorage.getOrCreateIndex(PARTITION_ID, UUID.randomUUID()));
    }

    /**
     * Test creating a Sorted Index.
     */
    @Test
    public void testCreateSortedIndex() {
        assertThrows(StorageException.class, () -> tableStorage.getOrCreateSortedIndex(PARTITION_ID, sortedIdx.id()));

        // Index should only be available after the associated partition has been created.
        tableStorage.getOrCreateMvPartition(PARTITION_ID);

        assertThat(tableStorage.getOrCreateSortedIndex(PARTITION_ID, sortedIdx.id()), is(notNullValue()));
    }

    /**
     * Test creating a Hash Index.
     */
    @Test
    public void testCreateHashIndex() {
        assertThrows(StorageException.class, () -> tableStorage.getOrCreateHashIndex(PARTITION_ID, hashIdx.id()));

        // Index should only be available after the associated partition has been created.
        tableStorage.getOrCreateMvPartition(PARTITION_ID);

        assertThat(tableStorage.getOrCreateHashIndex(PARTITION_ID, hashIdx.id()), is(notNullValue()));
    }

    /**
     * Tests destroying an index.
     */
    @Disabled("https://issues.apache.org/jira/browse/IGNITE-17626")
    @Test
    public void testDestroyIndex() {
        MvPartitionStorage partitionStorage = tableStorage.getOrCreateMvPartition(PARTITION_ID);

        assertThat(tableStorage.getOrCreateSortedIndex(PARTITION_ID, sortedIdx.id()), is(notNullValue()));
        assertThat(tableStorage.getOrCreateHashIndex(PARTITION_ID, hashIdx.id()), is(notNullValue()));

        CompletableFuture<Void> destroySortedIndexFuture = tableStorage.destroyIndex(sortedIdx.id());
        CompletableFuture<Void> destroyHashIndexFuture = tableStorage.destroyIndex(hashIdx.id());

        assertThat(partitionStorage.flush(), willCompleteSuccessfully());
        assertThat(destroySortedIndexFuture, willCompleteSuccessfully());
        assertThat(destroyHashIndexFuture, willCompleteSuccessfully());
    }

    @Test
    public void testHashIndexIndependence() {
        MvPartitionStorage partitionStorage1 = tableStorage.getOrCreateMvPartition(PARTITION_ID);

        assertThat(tableStorage.getOrCreateHashIndex(PARTITION_ID, hashIdx.id()), is(notNullValue()));
        assertThrows(StorageException.class, () -> tableStorage.getOrCreateHashIndex(PARTITION_ID + 1, hashIdx.id()));

        MvPartitionStorage partitionStorage2 = tableStorage.getOrCreateMvPartition(PARTITION_ID + 1);

        HashIndexStorage storage1 = tableStorage.getOrCreateHashIndex(PARTITION_ID, hashIdx.id());
        HashIndexStorage storage2 = tableStorage.getOrCreateHashIndex(PARTITION_ID + 1, hashIdx.id());

        assertThat(storage1, is(notNullValue()));
        assertThat(storage2, is(notNullValue()));

        var rowId1 = new RowId(PARTITION_ID);
        var rowId2 = new RowId(PARTITION_ID + 1);

        BinaryTupleSchema schema = BinaryTupleSchema.create(new Element[]{
                new Element(NativeTypes.INT32, false),
                new Element(NativeTypes.INT32, false)
        });

        ByteBuffer buffer = new BinaryTupleBuilder(schema.elementCount(), schema.hasNullableElements())
                .appendInt(1)
                .appendInt(2)
                .build();

        BinaryTuple tuple = new BinaryTuple(schema, buffer);

        partitionStorage1.runConsistently(() -> {
            storage1.put(new IndexRowImpl(tuple, rowId1));

            return null;
        });

        partitionStorage2.runConsistently(() -> {
            storage2.put(new IndexRowImpl(tuple, rowId2));

            return null;
        });

        assertThat(getAll(storage1.get(tuple)), contains(rowId1));
        assertThat(getAll(storage2.get(tuple)), contains(rowId2));
    }

    /**
     * Tests that exceptions are thrown if indices are not configured correctly.
     */
    @Test
    public void testMisconfiguredIndices() {
        Exception e = assertThrows(
                StorageException.class,
                () -> tableStorage.getOrCreateSortedIndex(PARTITION_ID, sortedIdx.id())
        );

        assertThat(e.getMessage(), is("Partition ID " + PARTITION_ID + " does not exist"));

        e = assertThrows(
                StorageException.class,
                () -> tableStorage.getOrCreateHashIndex(PARTITION_ID, hashIdx.id())
        );

        assertThat(e.getMessage(), is("Partition ID " + PARTITION_ID + " does not exist"));

        tableStorage.getOrCreateMvPartition(PARTITION_ID);

        UUID invalidUuid = UUID.randomUUID();

        e = assertThrows(
                StorageException.class,
                () -> tableStorage.getOrCreateHashIndex(PARTITION_ID, invalidUuid)
        );

        assertThat(e.getMessage(), is(String.format("Index configuration for \"%s\" could not be found", invalidUuid)));

        e = assertThrows(
                StorageException.class,
                () -> tableStorage.getOrCreateHashIndex(PARTITION_ID, sortedIdx.id())
        );

        assertThat(
                e.getMessage(),
                is(String.format("Index \"%s\" is not configured as a Hash Index. Actual type: SORTED", sortedIdx.id()))
        );

        e = assertThrows(
                StorageException.class,
                () -> tableStorage.getOrCreateSortedIndex(PARTITION_ID, hashIdx.id())
        );

        assertThat(
                e.getMessage(),
                is(String.format("Index \"%s\" is not configured as a Sorted Index. Actual type: HASH", hashIdx.id()))
        );
    }

    @Test
    public void testDestroyPartition() throws Exception {
        assertThrows(IllegalArgumentException.class, () -> tableStorage.destroyPartition(getPartitionIdOutOfRange()));

        MvPartitionStorage mvPartitionStorage = tableStorage.getOrCreateMvPartition(PARTITION_ID);
        HashIndexStorage hashIndexStorage = tableStorage.getOrCreateHashIndex(PARTITION_ID, hashIdx.id());
        SortedIndexStorage sortedIndexStorage = tableStorage.getOrCreateSortedIndex(PARTITION_ID, sortedIdx.id());

        RowId rowId = new RowId(PARTITION_ID);

        BinaryRow binaryRow = binaryRow(new TestKey(0), new TestValue(1));

        IndexRow hashIndexRow = indexRow(hashIndexStorage.indexDescriptor(), binaryRow, rowId);
        IndexRow sortedIndexRow = indexRow(sortedIndexStorage.indexDescriptor(), binaryRow, rowId);

        mvPartitionStorage.runConsistently(() -> {
            mvPartitionStorage.addWriteCommitted(rowId, binaryRow, clock.now());

            hashIndexStorage.put(hashIndexRow);

            sortedIndexStorage.put(sortedIndexRow);

            return null;
        });

        Cursor<ReadResult> scanVersionsCursor = mvPartitionStorage.scanVersions(rowId);
        PartitionTimestampCursor scanTimestampCursor = mvPartitionStorage.scan(clock.now());

        Cursor<RowId> getFromHashIndexCursor = hashIndexStorage.get(hashIndexRow.indexColumns());

        Cursor<RowId> getFromSortedIndexCursor = sortedIndexStorage.get(hashIndexRow.indexColumns());
        Cursor<IndexRow> scanFromSortedIndexCursor = sortedIndexStorage.scan(null, null, 0);

        tableStorage.destroyPartition(PARTITION_ID).get(1, SECONDS);

        // Let's check that we won't get destroyed storages.
        assertNull(tableStorage.getMvPartition(PARTITION_ID));
        assertThrows(StorageException.class, () -> tableStorage.getOrCreateHashIndex(PARTITION_ID, hashIdx.id()));
        assertThrows(StorageException.class, () -> tableStorage.getOrCreateSortedIndex(PARTITION_ID, sortedIdx.id()));

        checkStorageDestroyed(mvPartitionStorage);
        checkStorageDestroyed(hashIndexStorage);
        checkStorageDestroyed(sortedIndexStorage);

        assertThrows(StorageClosedException.class, () -> getAll(scanVersionsCursor));
        assertThrows(StorageClosedException.class, () -> getAll(scanTimestampCursor));

        assertThrows(StorageClosedException.class, () -> getAll(getFromHashIndexCursor));

        assertThrows(StorageClosedException.class, () -> getAll(getFromSortedIndexCursor));
        assertThrows(StorageClosedException.class, () -> getAll(scanFromSortedIndexCursor));

        // Let's check that nothing will happen if we try to destroy a non-existing partition.
        assertThat(tableStorage.destroyPartition(PARTITION_ID), willCompleteSuccessfully());
    }

    @Test
    public void testReCreatePartition() throws Exception {
        MvPartitionStorage mvPartitionStorage = tableStorage.getOrCreateMvPartition(PARTITION_ID);

        RowId rowId = new RowId(PARTITION_ID);

        BinaryRow binaryRow = binaryRow(new TestKey(0), new TestValue(1));

        mvPartitionStorage.runConsistently(() -> {
            mvPartitionStorage.addWriteCommitted(rowId, binaryRow, clock.now());

            return null;
        });

        tableStorage.destroyPartition(PARTITION_ID).get(1, SECONDS);

        MvPartitionStorage newMvPartitionStorage = tableStorage.getOrCreateMvPartition(PARTITION_ID);

        assertThat(getAll(newMvPartitionStorage.scanVersions(rowId)), empty());
    }

    @Test
    public void testSuccessRebalance() throws Exception {
        MvPartitionStorage mvPartitionStorage = tableStorage.getOrCreateMvPartition(PARTITION_ID);
        HashIndexStorage hashIndexStorage = tableStorage.getOrCreateHashIndex(PARTITION_ID, hashIdx.id());
        SortedIndexStorage sortedIndexStorage = tableStorage.getOrCreateSortedIndex(PARTITION_ID, sortedIdx.id());

        // Error because reblance has not yet started for the partition.
        assertThrows(StorageRebalanceException.class, () -> tableStorage.finishRebalancePartition(PARTITION_ID, 100, 500));

        List<IgniteTuple3<RowId, BinaryRow, HybridTimestamp>> rowsBeforeRebalanceStart = List.of(
                new IgniteTuple3<>(new RowId(PARTITION_ID), binaryRow(new TestKey(0), new TestValue(0)), clock.now()),
                new IgniteTuple3<>(new RowId(PARTITION_ID), binaryRow(new TestKey(1), new TestValue(1)), clock.now())
        );

        startRebalanceWithChecks(
                PARTITION_ID,
                mvPartitionStorage,
                hashIndexStorage,
                sortedIndexStorage,
                rowsBeforeRebalanceStart
        );

        // Let's fill the storages with fresh data on rebalance.
        List<IgniteTuple3<RowId, BinaryRow, HybridTimestamp>> rowsOnRebalance = List.of(
                new IgniteTuple3<>(new RowId(PARTITION_ID), binaryRow(new TestKey(2), new TestValue(2)), clock.now()),
                new IgniteTuple3<>(new RowId(PARTITION_ID), binaryRow(new TestKey(3), new TestValue(3)), clock.now())
        );

        fillStorages(mvPartitionStorage, hashIndexStorage, sortedIndexStorage, rowsOnRebalance);

        // Let's finish rebalancing.

        // Partition is out of configuration range.
        assertThrows(IllegalArgumentException.class, () -> tableStorage.finishRebalancePartition(getPartitionIdOutOfRange(), 100, 500));

        // Partition does not exist.
        assertThrows(StorageRebalanceException.class, () -> tableStorage.finishRebalancePartition(1, 100, 500));

        assertThat(tableStorage.finishRebalancePartition(PARTITION_ID, 10, 20), willCompleteSuccessfully());

        // Let's check the storages after success finish rebalance.
        checkForMissingRows(mvPartitionStorage, hashIndexStorage, sortedIndexStorage, rowsBeforeRebalanceStart);
        checkForPresenceRows(mvPartitionStorage, hashIndexStorage, sortedIndexStorage, rowsOnRebalance);

        assertEquals(10, mvPartitionStorage.lastAppliedIndex());
        assertEquals(10, mvPartitionStorage.persistedIndex());
        assertEquals(20, mvPartitionStorage.lastAppliedTerm());
    }

    @Test
    public void testFailRebalance() throws Exception {
        MvPartitionStorage mvPartitionStorage = tableStorage.getOrCreateMvPartition(PARTITION_ID);
        HashIndexStorage hashIndexStorage = tableStorage.getOrCreateHashIndex(PARTITION_ID, hashIdx.id());
        SortedIndexStorage sortedIndexStorage = tableStorage.getOrCreateSortedIndex(PARTITION_ID, sortedIdx.id());

        // Nothing will happen because rebalancing has not started.
        tableStorage.abortRebalancePartition(PARTITION_ID).get(1, SECONDS);

        List<IgniteTuple3<RowId, BinaryRow, HybridTimestamp>> rowsBeforeRebalanceStart = List.of(
                new IgniteTuple3<>(new RowId(PARTITION_ID), binaryRow(new TestKey(0), new TestValue(0)), clock.now()),
                new IgniteTuple3<>(new RowId(PARTITION_ID), binaryRow(new TestKey(1), new TestValue(1)), clock.now())
        );

        startRebalanceWithChecks(
                PARTITION_ID,
                mvPartitionStorage,
                hashIndexStorage,
                sortedIndexStorage,
                rowsBeforeRebalanceStart
        );

        // Let's fill the storages with fresh data on rebalance.
        List<IgniteTuple3<RowId, BinaryRow, HybridTimestamp>> rowsOnRebalance = List.of(
                new IgniteTuple3<>(new RowId(PARTITION_ID), binaryRow(new TestKey(2), new TestValue(2)), clock.now()),
                new IgniteTuple3<>(new RowId(PARTITION_ID), binaryRow(new TestKey(3), new TestValue(3)), clock.now())
        );

        fillStorages(mvPartitionStorage, hashIndexStorage, sortedIndexStorage, rowsOnRebalance);

        // Let's abort rebalancing.

        // Partition is out of configuration range.
        assertThrows(IllegalArgumentException.class, () -> tableStorage.abortRebalancePartition(getPartitionIdOutOfRange()));

        assertThat(tableStorage.abortRebalancePartition(PARTITION_ID), willCompleteSuccessfully());

        // Let's check the storages after abort rebalance.
        checkForMissingRows(mvPartitionStorage, hashIndexStorage, sortedIndexStorage, rowsBeforeRebalanceStart);
        checkForMissingRows(mvPartitionStorage, hashIndexStorage, sortedIndexStorage, rowsOnRebalance);

        assertEquals(0, mvPartitionStorage.lastAppliedIndex());
        assertEquals(0, mvPartitionStorage.persistedIndex());
        assertEquals(0, mvPartitionStorage.lastAppliedTerm());
    }

    private static void createTestIndexes(TablesConfiguration tablesConfig) {
        List<IndexDefinition> indexDefinitions = List.of(
                SchemaBuilders.sortedIndex(SORTED_INDEX_NAME)
                        .addIndexColumn("COLUMN0").done()
                        .build(),
                SchemaBuilders.hashIndex(HASH_INDEX_NAME)
                        .withColumns("COLUMN0")
                        .build()
        );

        UUID tableId = ConfigurationUtil.internalId(tablesConfig.tables().value(), "foo");

        CompletableFuture<Void> indexCreateFut = tablesConfig.indexes().change(ch ->
                indexDefinitions.forEach(idxDef -> ch.create(idxDef.name(),
                        c -> SchemaConfigurationConverter.addIndex(idxDef, tableId, c)
                ))
        );

        assertThat(indexCreateFut, willCompleteSuccessfully());
    }

    private static void createTestTable(TableConfiguration tableConfig) {
        TableDefinition tableDefinition = SchemaBuilders.tableBuilder("PUBLIC", "foo")
                .columns(
                        SchemaBuilders.column("ID", ColumnType.INT32).build(),
                        SchemaBuilders.column("COLUMN0", ColumnType.INT32).build()
                )
                .withPrimaryKey("ID")
                .build();

        CompletableFuture<Void> createTableFuture = tableConfig.change(
                tableChange -> SchemaConfigurationConverter.convert(tableDefinition, tableChange)
        );

        assertThat(createTableFuture, willCompleteSuccessfully());
    }

    private static <T> List<T> getAll(Cursor<T> cursor) {
        try (cursor) {
            return cursor.stream().collect(toList());
        }
    }

    private void checkStorageDestroyed(MvPartitionStorage storage) throws Exception {
        int partId = PARTITION_ID;

        assertThrows(StorageClosedException.class, () -> storage.runConsistently(() -> null));

        assertThrows(StorageClosedException.class, storage::flush);

        assertThrows(StorageClosedException.class, storage::lastAppliedIndex);
        assertThrows(StorageClosedException.class, storage::lastAppliedTerm);
        assertThrows(StorageClosedException.class, storage::persistedIndex);
        assertThrows(StorageClosedException.class, storage::committedGroupConfiguration);

        RowId rowId = new RowId(partId);

        HybridTimestamp timestamp = clock.now();

        assertThrows(StorageClosedException.class, () -> storage.read(new RowId(PARTITION_ID), timestamp));

        BinaryRow binaryRow = binaryRow(new TestKey(0), new TestValue(1));

        assertThrows(StorageClosedException.class, () -> storage.addWrite(rowId, binaryRow, UUID.randomUUID(), UUID.randomUUID(), partId));
        assertThrows(StorageClosedException.class, () -> storage.commitWrite(rowId, timestamp));
        assertThrows(StorageClosedException.class, () -> storage.abortWrite(rowId));
        assertThrows(StorageClosedException.class, () -> storage.addWriteCommitted(rowId, binaryRow, timestamp));

        assertThrows(StorageClosedException.class, () -> storage.scan(timestamp));
        assertThrows(StorageClosedException.class, () -> storage.scanVersions(rowId));
        assertThrows(StorageClosedException.class, () -> storage.scanVersions(rowId));

        assertThrows(StorageClosedException.class, () -> storage.closestRowId(rowId));

        assertThrows(StorageClosedException.class, storage::rowsCount);
    }

    private void checkStorageDestroyed(SortedIndexStorage storage) {
        checkStorageDestroyed((IndexStorage) storage);

        assertThrows(StorageClosedException.class, () -> storage.scan(null, null, 0));
    }

    private void checkStorageDestroyed(IndexStorage storage) {
        assertThrows(StorageClosedException.class, () -> storage.get(mock(BinaryTuple.class)));

        assertThrows(StorageClosedException.class, () -> storage.put(mock(IndexRow.class)));

        assertThrows(StorageClosedException.class, () -> storage.remove(mock(IndexRow.class)));
    }

    private int getPartitionIdOutOfRange() {
        return tableStorage.configuration().partitions().value();
    }

    private void startRebalanceWithChecks(
            int partitionId,
            MvPartitionStorage mvPartitionStorage,
            HashIndexStorage hashIndexStorage,
            SortedIndexStorage sortedIndexStorage,
            List<IgniteTuple3<RowId, BinaryRow, HybridTimestamp>> rowsBeforeRebalanceStart
    ) {
        assertThat(rowsBeforeRebalanceStart, hasSize(greaterThanOrEqualTo(2)));

        fillStorages(mvPartitionStorage, hashIndexStorage, sortedIndexStorage, rowsBeforeRebalanceStart);

        // Let's open the cursors before start rebalance.
        IgniteTuple3<RowId, BinaryRow, HybridTimestamp> rowForCursors = rowsBeforeRebalanceStart.get(0);

        Cursor<?> mvPartitionStorageScanVersionsCursor = mvPartitionStorage.scanVersions(rowForCursors.get1());
        Cursor<?> mvPartitionStorageScanCursor = mvPartitionStorage.scan(rowForCursors.get3());

        IndexRow hashIndexRow = indexRow(hashIndexStorage.indexDescriptor(), rowForCursors.get2(), rowForCursors.get1());
        IndexRow sortedIndexRow = indexRow(sortedIndexStorage.indexDescriptor(), rowForCursors.get2(), rowForCursors.get1());

        Cursor<?> hashIndexStorageGetCursor = hashIndexStorage.get(hashIndexRow.indexColumns());

        Cursor<?> sortedIndexStorageGetCursor = sortedIndexStorage.get(sortedIndexRow.indexColumns());
        Cursor<?> sortedIndexStorageScanCursor = sortedIndexStorage.scan(null, null, 0);

        // Partition is out of configuration range.
        assertThrows(IllegalArgumentException.class, () -> tableStorage.startRebalancePartition(getPartitionIdOutOfRange()));

        // Partition does not exist.
        assertThrows(StorageRebalanceException.class, () -> tableStorage.startRebalancePartition(partitionId + 1));

        // Let's start rebalancing of the partition.
        assertThat(tableStorage.startRebalancePartition(partitionId), willCompleteSuccessfully());

        // Once again, rebalancing of the partition cannot be started.
        assertThrows(StorageRebalanceException.class, () -> tableStorage.startRebalancePartition(partitionId));

        checkMvPartitionStorageMethodsAfterStartRebalance(mvPartitionStorage);
        checkHashIndexStorageMethodsAfterStartRebalance(hashIndexStorage);
        checkSortedIndexStorageMethodsAfterStartRebalance(sortedIndexStorage);

        checkCursorAfterStartRebalance(mvPartitionStorageScanVersionsCursor);
        checkCursorAfterStartRebalance(mvPartitionStorageScanCursor);

        checkCursorAfterStartRebalance(hashIndexStorageGetCursor);

        checkCursorAfterStartRebalance(sortedIndexStorageGetCursor);
        checkCursorAfterStartRebalance(sortedIndexStorageScanCursor);
    }

    private void checkMvPartitionStorageMethodsAfterStartRebalance(MvPartitionStorage storage) {
        assertEquals(REBALANCE_IN_PROGRESS, storage.lastAppliedIndex());
        assertEquals(REBALANCE_IN_PROGRESS, storage.persistedIndex());
        assertEquals(REBALANCE_IN_PROGRESS, storage.lastAppliedTerm());

        assertDoesNotThrow(() -> storage.committedGroupConfiguration());

        storage.runConsistently(() -> {
            assertThrows(StorageRebalanceException.class, () -> storage.lastApplied(100, 500));

            assertThrows(
                    StorageRebalanceException.class,
                    () -> storage.committedGroupConfiguration(mock(RaftGroupConfiguration.class))
            );

            RowId rowId = new RowId(PARTITION_ID);

            assertThrows(StorageRebalanceException.class, () -> storage.read(rowId, clock.now()));
            assertThrows(StorageRebalanceException.class, () -> storage.scanVersions(rowId));
            assertThrows(StorageRebalanceException.class, () -> storage.scan(clock.now()));
            assertThrows(StorageRebalanceException.class, () -> storage.closestRowId(rowId));
            assertThrows(StorageRebalanceException.class, storage::rowsCount);

            // TODO: IGNITE-18020 Add check
            // TODO: IGNITE-18023 Add check
            if (storage instanceof TestMvPartitionStorage) {
                assertThrows(StorageRebalanceException.class, () -> storage.pollForVacuum(clock.now()));
            }

            return null;
        });
    }

    private static void checkHashIndexStorageMethodsAfterStartRebalance(HashIndexStorage storage) {
        assertDoesNotThrow(storage::indexDescriptor);

        assertThrows(StorageRebalanceException.class, () -> storage.get(mock(BinaryTuple.class)));
    }

    private static void checkSortedIndexStorageMethodsAfterStartRebalance(SortedIndexStorage storage) {
        assertDoesNotThrow(storage::indexDescriptor);

        assertThrows(StorageRebalanceException.class, () -> storage.get(mock(BinaryTuple.class)));
        assertThrows(StorageRebalanceException.class, () -> storage.scan(null, null, 0));
    }

    private static void checkCursorAfterStartRebalance(Cursor<?> cursor) {
        assertDoesNotThrow(cursor::close);

        assertThrows(StorageRebalanceException.class, cursor::hasNext);
        assertThrows(StorageRebalanceException.class, cursor::next);
    }

    private void fillStorages(
            MvPartitionStorage mvPartitionStorage,
            HashIndexStorage hashIndexStorage,
            SortedIndexStorage sortedIndexStorage,
            List<IgniteTuple3<RowId, BinaryRow, HybridTimestamp>> rows
    ) {
        for (int i = 0; i < rows.size(); i++) {
            int finalI = i;

            IgniteTuple3<RowId, BinaryRow, HybridTimestamp> row = rows.get(i);

            RowId rowId = row.get1();
            BinaryRow binaryRow = row.get2();
            HybridTimestamp timestamp = row.get3();

            IndexRow hashIndexRow = indexRow(hashIndexStorage.indexDescriptor(), binaryRow, rowId);
            IndexRow sortedIndexRow = indexRow(sortedIndexStorage.indexDescriptor(), binaryRow, rowId);

            mvPartitionStorage.runConsistently(() -> {
                // If even.
                if ((finalI & 1) == 0) {
                    mvPartitionStorage.addWrite(rowId, binaryRow, UUID.randomUUID(), UUID.randomUUID(), rowId.partitionId());

                    mvPartitionStorage.commitWrite(rowId, timestamp);
                } else {
                    mvPartitionStorage.addWriteCommitted(rowId, binaryRow, timestamp);
                }

                hashIndexStorage.put(hashIndexRow);

                sortedIndexStorage.put(sortedIndexRow);

                return null;
            });
        }
    }

    private void checkForMissingRows(
            MvPartitionStorage mvPartitionStorage,
            HashIndexStorage hashIndexStorage,
            SortedIndexStorage sortedIndexStorage,
            List<IgniteTuple3<RowId, BinaryRow, HybridTimestamp>> rows
    ) {
        for (IgniteTuple3<RowId, BinaryRow, HybridTimestamp> row : rows) {
            assertThat(getAll(mvPartitionStorage.scanVersions(row.get1())), is(empty()));

            IndexRow hashIndexRow = indexRow(hashIndexStorage.indexDescriptor(), row.get2(), row.get1());
            IndexRow sortedIndexRow = indexRow(sortedIndexStorage.indexDescriptor(), row.get2(), row.get1());

            assertThat(getAll(hashIndexStorage.get(hashIndexRow.indexColumns())), is(empty()));
            assertThat(getAll(sortedIndexStorage.get(sortedIndexRow.indexColumns())), is(empty()));
        }
    }

    private void checkForPresenceRows(
            MvPartitionStorage mvPartitionStorage,
            HashIndexStorage hashIndexStorage,
            SortedIndexStorage sortedIndexStorage,
            List<IgniteTuple3<RowId, BinaryRow, HybridTimestamp>> rows
    ) {
        for (IgniteTuple3<RowId, BinaryRow, HybridTimestamp> row : rows) {
            assertThat(
                    getAll(mvPartitionStorage.scanVersions(row.get1())).stream().map(ReadResult::binaryRow).collect(toList()),
                    containsInAnyOrder(row.get2())
            );

            IndexRow hashIndexRow = indexRow(hashIndexStorage.indexDescriptor(), row.get2(), row.get1());
            IndexRow sortedIndexRow = indexRow(sortedIndexStorage.indexDescriptor(), row.get2(), row.get1());

            assertThat(getAll(hashIndexStorage.get(hashIndexRow.indexColumns())), contains(row.get1()));
            assertThat(getAll(sortedIndexStorage.get(sortedIndexRow.indexColumns())), contains(row.get1()));
        }
    }

    protected BinaryRow binaryRow(TestKey key, TestValue value) throws Exception {
        KvMarshaller<TestKey, TestValue> kvMarshaller = new ReflectionMarshallerFactory().create(
                schemaDescriptor,
                TestKey.class,
                TestValue.class
        );

        return kvMarshaller.marshal(key, value);
    }

    protected IndexRow indexRow(IndexDescriptor indexDescriptor, BinaryRow binaryRow, RowId rowId) {
        int[] columnIndexes = indexDescriptor.columns().stream()
                .mapToInt(indexColumnDescriptor -> {
                    Column column = schemaDescriptor.column(indexColumnDescriptor.name());

                    assertNotNull(column, column.name());

                    return column.schemaIndex();
                })
                .toArray();

        BinaryTupleSchema binaryTupleSchema = BinaryTupleSchema.createSchema(schemaDescriptor, columnIndexes);

        BinaryConverter binaryTupleConverter = new BinaryConverter(schemaDescriptor, binaryTupleSchema, false);

        return new IndexRowImpl(binaryTupleConverter.toTuple(binaryRow), rowId);
    }

    protected @Nullable IgniteBiTuple<TestKey, TestValue> unwrap(@Nullable BinaryRow binaryRow) {
        if (binaryRow == null) {
            return null;
        }

        KvMarshaller<TestKey, TestValue> kvMarshaller = new ReflectionMarshallerFactory().create(
                schemaDescriptor,
                TestKey.class,
                TestValue.class
        );

        Row row = new Row(schemaDescriptor, binaryRow);

        try {
            return new IgniteBiTuple<>(kvMarshaller.unmarshalKey(row), kvMarshaller.unmarshalValue(row));
        } catch (MarshallerException e) {
            throw new RuntimeException(e);
        }
    }

    protected @Nullable IgniteBiTuple<TestKey, TestValue> unwrap(@Nullable ReadResult readResult) {
        if (readResult == null) {
            return null;
        }

        return unwrap(readResult.binaryRow());
    }

    /**
     * Test pojo key.
     */
    protected static class TestKey {
        public int id;

        protected TestKey() {
        }

        public TestKey(int id) {
            this.id = id;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }

            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            TestKey other = (TestKey) o;

            return id == other.id;
        }

        @Override
        public int hashCode() {
            return Objects.hash(id);
        }

        @Override
        public String toString() {
            return S.toString(TestKey.class, this);
        }
    }

    /**
     * Test pojo value.
     */
    protected static class TestValue {
        public int column0;

        protected TestValue() {
        }

        public TestValue(int column0) {
            this.column0 = column0;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }

            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            TestValue other = (TestValue) o;

            return column0 == other.column0;
        }

        @Override
        public int hashCode() {
            return Objects.hash(column0);
        }

        @Override
        public String toString() {
            return S.toString(TestValue.class, this);
        }
    }
}
