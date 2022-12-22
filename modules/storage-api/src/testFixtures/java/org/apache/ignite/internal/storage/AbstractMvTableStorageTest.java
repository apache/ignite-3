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

import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.ignite.internal.binarytuple.BinaryTupleBuilder;
import org.apache.ignite.internal.configuration.util.ConfigurationUtil;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.BinaryTuple;
import org.apache.ignite.internal.schema.BinaryTuplePrefix;
import org.apache.ignite.internal.schema.BinaryTupleSchema;
import org.apache.ignite.internal.schema.BinaryTupleSchema.Element;
import org.apache.ignite.internal.schema.NativeTypes;
import org.apache.ignite.internal.schema.configuration.TableConfiguration;
import org.apache.ignite.internal.schema.configuration.TablesConfiguration;
import org.apache.ignite.internal.schema.configuration.index.TableIndexView;
import org.apache.ignite.internal.schema.testutils.SchemaConfigurationConverter;
import org.apache.ignite.internal.schema.testutils.builder.SchemaBuilders;
import org.apache.ignite.internal.schema.testutils.definition.ColumnType;
import org.apache.ignite.internal.schema.testutils.definition.TableDefinition;
import org.apache.ignite.internal.schema.testutils.definition.index.IndexDefinition;
import org.apache.ignite.internal.storage.engine.MvTableStorage;
import org.apache.ignite.internal.storage.index.HashIndexStorage;
import org.apache.ignite.internal.storage.index.IndexRow;
import org.apache.ignite.internal.storage.index.IndexRowImpl;
import org.apache.ignite.internal.storage.index.IndexStorage;
import org.apache.ignite.internal.storage.index.SortedIndexStorage;
import org.apache.ignite.internal.util.Cursor;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

/**
 * Abstract class that contains tests for {@link MvTableStorage} implementations.
 */
public abstract class AbstractMvTableStorageTest extends BaseMvStoragesTest {
    private static final String SORTED_INDEX_NAME = "SORTED_IDX";

    private static final String HASH_INDEX_NAME = "HASH_IDX";

    protected static final int PARTITION_ID = 0;

    /** Partition id for 0 storage. */
    protected static final int PARTITION_ID_0 = 42;

    /** Partition id for 1 storage. */
    protected static final int PARTITION_ID_1 = 1 << 8;

    private MvTableStorage tableStorage;

    private TableIndexView sortedIdx;

    private TableIndexView hashIdx;

    private final HybridClock clock = new HybridClockImpl();

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
    void testPartitionIndependence() {
        MvPartitionStorage partitionStorage0 = tableStorage.getOrCreateMvPartition(PARTITION_ID_0);
        // Using a shifted ID value to test a multibyte scenario.
        MvPartitionStorage partitionStorage1 = tableStorage.getOrCreateMvPartition(PARTITION_ID_1);

        var testData0 = binaryRow(new TestKey(1, "1"), new TestValue(10, "10"));

        UUID txId = UUID.randomUUID();

        RowId rowId0 = new RowId(PARTITION_ID_0);

        partitionStorage0.runConsistently(() -> partitionStorage0.addWrite(rowId0, testData0, txId, UUID.randomUUID(), 0));

        assertThat(unwrap(partitionStorage0.read(rowId0, HybridTimestamp.MAX_VALUE)), is(equalTo(unwrap(testData0))));
        assertThrows(IllegalArgumentException.class, () -> partitionStorage1.read(rowId0, HybridTimestamp.MAX_VALUE));

        var testData1 = binaryRow(new TestKey(2, "2"), new TestValue(20, "20"));

        RowId rowId1 = new RowId(PARTITION_ID_1);

        partitionStorage1.runConsistently(() -> partitionStorage1.addWrite(rowId1, testData1, txId, UUID.randomUUID(), 0));

        assertThrows(IllegalArgumentException.class, () -> partitionStorage0.read(rowId1, HybridTimestamp.MAX_VALUE));
        assertThat(unwrap(partitionStorage1.read(rowId1, HybridTimestamp.MAX_VALUE)), is(equalTo(unwrap(testData1))));

        assertThat(drainToList(partitionStorage0.scan(HybridTimestamp.MAX_VALUE)), contains(unwrap(testData0)));
        assertThat(drainToList(partitionStorage1.scan(HybridTimestamp.MAX_VALUE)), contains(unwrap(testData1)));
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
    public void testHashIndexIndependence() throws Exception {
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
    public void testStartRebalanceMvPartition() throws Exception {
        MvPartitionStorage partitionStorage = tableStorage.getOrCreateMvPartition(PARTITION_ID);

        partitionStorage.runConsistently(() -> {
            partitionStorage.addWriteCommitted(
                    new RowId(PARTITION_ID),
                    binaryRow(new TestKey(0, "0"), new TestValue(1, "1")),
                    clock.now()
            );

            partitionStorage.lastApplied(100, 10);

            partitionStorage.committedGroupConfiguration(new RaftGroupConfiguration(List.of("peer"), List.of("learner"), null, null));

            return null;
        });

        partitionStorage.flush().get(1, TimeUnit.SECONDS);

        tableStorage.startFullRebalancePartition(PARTITION_ID).get(1, TimeUnit.SECONDS);

        MvPartitionStorage newPartitionStorage0 = tableStorage.getMvPartition(PARTITION_ID);

        assertNotNull(newPartitionStorage0);
        assertNotSame(partitionStorage, newPartitionStorage0);

        assertEquals(0L, newPartitionStorage0.lastAppliedIndex());
        assertEquals(0L, newPartitionStorage0.lastAppliedTerm());
        assertNull(newPartitionStorage0.committedGroupConfiguration());
        assertEquals(0L, newPartitionStorage0.persistedIndex());
        assertEquals(0, newPartitionStorage0.rowsCount());

        tableStorage.startFullRebalancePartition(PARTITION_ID).get(1, TimeUnit.SECONDS);

        MvPartitionStorage newPartitionStorage1 = tableStorage.getMvPartition(PARTITION_ID);

        assertSame(newPartitionStorage0, newPartitionStorage1);
    }

    @Test
    public void testAbortRebalanceMvPartition() throws Exception {
        assertDoesNotThrow(() -> tableStorage.abortFullRebalancePartition(PARTITION_ID).get(1, TimeUnit.SECONDS));

        MvPartitionStorage partitionStorage = tableStorage.getOrCreateMvPartition(PARTITION_ID);

        tableStorage.startFullRebalancePartition(PARTITION_ID).get(1, TimeUnit.SECONDS);

        tableStorage.abortFullRebalancePartition(PARTITION_ID).get(1, TimeUnit.SECONDS);

        assertSame(partitionStorage, tableStorage.getMvPartition(PARTITION_ID));

        assertDoesNotThrow(() -> tableStorage.abortFullRebalancePartition(PARTITION_ID).get(1, TimeUnit.SECONDS));
    }

    @Test
    public void testFinishRebalanceMvPartition() throws Exception {
        assertDoesNotThrow(() -> tableStorage.finishFullRebalancePartition(PARTITION_ID).get(1, TimeUnit.SECONDS));

        tableStorage.getOrCreateMvPartition(PARTITION_ID);

        tableStorage.startFullRebalancePartition(PARTITION_ID).get(1, TimeUnit.SECONDS);

        MvPartitionStorage newPartitionStorage = tableStorage.getMvPartition(PARTITION_ID);

        tableStorage.finishFullRebalancePartition(PARTITION_ID).get(1, TimeUnit.SECONDS);

        assertSame(newPartitionStorage, tableStorage.getMvPartition(PARTITION_ID));

        assertDoesNotThrow(() -> tableStorage.finishFullRebalancePartition(PARTITION_ID).get(1, TimeUnit.SECONDS));
    }

    @Test
    public void testDestroyPartition() throws Exception {
        assertThrows(
                IllegalArgumentException.class,
                () -> tableStorage.destroyPartition(tableStorage.configuration().partitions().value())
        );

        MvPartitionStorage mvPartitionStorage = tableStorage.getOrCreateMvPartition(PARTITION_ID);
        HashIndexStorage hashIndexStorage = tableStorage.getOrCreateHashIndex(PARTITION_ID, hashIdx.id());
        SortedIndexStorage sortedIndexStorage = tableStorage.getOrCreateSortedIndex(PARTITION_ID, sortedIdx.id());

        RowId rowId = new RowId(PARTITION_ID);

        BinaryRow binaryRow = binaryRow(new TestKey(0, "0"), new TestValue(1, "1"));

        IndexRow indexRow = indexRow(binaryRow, rowId);

        mvPartitionStorage.runConsistently(() -> {
            mvPartitionStorage.addWriteCommitted(rowId, binaryRow, clock.now());

            hashIndexStorage.put(indexRow);

            sortedIndexStorage.put(indexRow);

            return null;
        });

        Cursor<ReadResult> scanVersionsCursor = mvPartitionStorage.scanVersions(rowId);
        PartitionTimestampCursor scanTimestampCursor = mvPartitionStorage.scan(clock.now());

        Cursor<RowId> getFromHashIndexCursor = hashIndexStorage.get(indexRow.indexColumns());

        Cursor<RowId> getFromSortedIndexCursor = sortedIndexStorage.get(indexRow.indexColumns());
        Cursor<IndexRow> scanFromSortedIndexCursor = sortedIndexStorage.scan(null, null, 0);

        tableStorage.destroyPartition(PARTITION_ID).get(1, TimeUnit.SECONDS);

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
        assertDoesNotThrow(() -> tableStorage.destroyPartition(PARTITION_ID).get(1, TimeUnit.SECONDS));
    }

    @Test
    public void testReCreatePartition() throws Exception {
        MvPartitionStorage mvPartitionStorage = tableStorage.getOrCreateMvPartition(PARTITION_ID);

        RowId rowId = new RowId(PARTITION_ID);

        BinaryRow binaryRow = binaryRow(new TestKey(0, "0"), new TestValue(1, "1"));

        mvPartitionStorage.runConsistently(() -> {
            mvPartitionStorage.addWriteCommitted(rowId, binaryRow, clock.now());

            return null;
        });

        tableStorage.destroyPartition(PARTITION_ID).get(1, TimeUnit.SECONDS);

        MvPartitionStorage newMvPartitionStorage = tableStorage.getOrCreateMvPartition(PARTITION_ID);

        assertThat(getAll(newMvPartitionStorage.scanVersions(rowId)), empty());
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
            return cursor.stream().collect(Collectors.toList());
        }
    }

    private void checkStorageDestroyed(MvPartitionStorage storage) {
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

        BinaryRow binaryRow = binaryRow(new TestKey(0, "0"), new TestValue(1, "1"));

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

        BinaryTuple indexKey = indexKey(binaryRow(new TestKey(0, "0"), new TestValue(1, "1")));

        assertThrows(
                StorageClosedException.class,
                () -> storage.scan(BinaryTuplePrefix.fromBinaryTuple(indexKey), BinaryTuplePrefix.fromBinaryTuple(indexKey), 0)
        );
    }

    private void checkStorageDestroyed(IndexStorage storage) {
        IndexRow indexRow = indexRow(binaryRow(new TestKey(0, "0"), new TestValue(1, "1")), new RowId(PARTITION_ID));

        assertThrows(StorageClosedException.class, () -> storage.get(indexRow.indexColumns()));

        assertThrows(StorageClosedException.class, () -> storage.put(indexRow));

        assertThrows(StorageClosedException.class, () -> storage.remove(indexRow));
    }
}
