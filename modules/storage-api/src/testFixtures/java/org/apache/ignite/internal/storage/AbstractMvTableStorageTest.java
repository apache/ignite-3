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
import static org.apache.ignite.internal.storage.MvPartitionStorage.FULL_REBALANCE_IN_PROGRESS;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
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
        assertThrows(IllegalArgumentException.class, () -> tableStorage.destroyPartition(getOutConfigRangePartitionId()));

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
        assertDoesNotThrow(() -> tableStorage.destroyPartition(PARTITION_ID).get(1, SECONDS));
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

        tableStorage.destroyPartition(PARTITION_ID).get(1, SECONDS);

        MvPartitionStorage newMvPartitionStorage = tableStorage.getOrCreateMvPartition(PARTITION_ID);

        assertThat(getAll(newMvPartitionStorage.scanVersions(rowId)), empty());
    }

    @Test
    public void testSuccessFullRebalance() throws Exception {
        MvPartitionStorage mvPartitionStorage = tableStorage.getOrCreateMvPartition(PARTITION_ID);
        HashIndexStorage hashIndexStorage = tableStorage.getOrCreateHashIndex(PARTITION_ID, hashIdx.id());
        SortedIndexStorage sortedIndexStorage = tableStorage.getOrCreateSortedIndex(PARTITION_ID, sortedIdx.id());

        // Error because the full reblance has not yet started for the partition.
        assertThrows(StorageFullRebalanceException.class, () -> tableStorage.finishFullRebalancePartition(PARTITION_ID, 100, 500));

        // Let's fill the storages before a full rebalance start.

        RowId rowIdBeforeFullRebalanceStart0 = new RowId(PARTITION_ID);
        RowId rowIdBeforeFullRebalanceStart1 = new RowId(PARTITION_ID);

        BinaryRow binaryRowBeforeFullRebalanceStart0 = binaryRow(new TestKey(0, "0"), new TestValue(0, "0"));
        BinaryRow binaryRowBeforeFullRebalanceStart1 = binaryRow(new TestKey(1, "1"), new TestValue(1, "1"));

        HybridTimestamp timestampBeforeFullRebalanceStart0 = clock.now();
        HybridTimestamp timestampBeforeFullRebalanceStart1 = clock.now();

        IndexRow indexRowBeforeFullRebalanceStart0 = indexRow(binaryRowBeforeFullRebalanceStart0, rowIdBeforeFullRebalanceStart0);
        IndexRow indexRowBeforeFullRebalanceStart1 = indexRow(binaryRowBeforeFullRebalanceStart1, rowIdBeforeFullRebalanceStart1);

        mvPartitionStorage.runConsistently(() -> {
            mvPartitionStorage.addWriteCommitted(
                    rowIdBeforeFullRebalanceStart0,
                    binaryRowBeforeFullRebalanceStart0,
                    timestampBeforeFullRebalanceStart0
            );

            mvPartitionStorage.addWriteCommitted(
                    rowIdBeforeFullRebalanceStart1,
                    binaryRowBeforeFullRebalanceStart1,
                    timestampBeforeFullRebalanceStart1
            );

            hashIndexStorage.put(indexRowBeforeFullRebalanceStart0);
            hashIndexStorage.put(indexRowBeforeFullRebalanceStart1);

            sortedIndexStorage.put(indexRowBeforeFullRebalanceStart0);
            sortedIndexStorage.put(indexRowBeforeFullRebalanceStart1);

            return null;
        });

        // Let's open the cursors.

        Cursor<ReadResult> mvPartitionCursorBeforeFullRebalanceStart0 = mvPartitionStorage.scanVersions(rowIdBeforeFullRebalanceStart0);
        Cursor<ReadResult> mvPartitionCursorBeforeFullRebalanceStart1 = mvPartitionStorage.scan(timestampBeforeFullRebalanceStart0);

        Cursor<RowId> hashIndexCursorBeforeFullRebalanceStart = hashIndexStorage.get(indexRowBeforeFullRebalanceStart0.indexColumns());

        Cursor<RowId> sortedIndexCursorBeforeFullRebalanceStart0 = sortedIndexStorage.get(indexRowBeforeFullRebalanceStart0.indexColumns());
        Cursor<IndexRow> sortedIndexCursorBeforeFullRebalanceStart1 = sortedIndexStorage.scan(null, null, 0);

        // Partition is out of configuration range.
        assertThrows(IllegalArgumentException.class, () -> tableStorage.startFullRebalancePartition(getOutConfigRangePartitionId()));

        // Partition does not exist.
        assertThrows(StorageFullRebalanceException.class, () -> tableStorage.startFullRebalancePartition(1));

        // Let's start a full rebalancing of the partition.
        tableStorage.startFullRebalancePartition(PARTITION_ID).get(1, SECONDS);

        // Once again, a full rebalancing of the partition cannot be started.
        assertThrows(StorageFullRebalanceException.class, () -> tableStorage.startFullRebalancePartition(PARTITION_ID));

        checkMvPartitionStorageMethodsAfterStartFullRebalance(mvPartitionStorage);
        checkHashIndexStorageMethodsAfterStartFullRebalance(hashIndexStorage);
        checkSortedIndexStorageMethodsAfterStartFullRebalance(sortedIndexStorage);

        checkCursorAfterStartFullRebalance(mvPartitionCursorBeforeFullRebalanceStart0);
        checkCursorAfterStartFullRebalance(mvPartitionCursorBeforeFullRebalanceStart1);
        checkCursorAfterStartFullRebalance(hashIndexCursorBeforeFullRebalanceStart);
        checkCursorAfterStartFullRebalance(sortedIndexCursorBeforeFullRebalanceStart0);
        checkCursorAfterStartFullRebalance(sortedIndexCursorBeforeFullRebalanceStart1);

        // Let's fill the storages with fresh data on rebalance.

        RowId rowIdOnFullRebalance0 = new RowId(PARTITION_ID);
        RowId rowIdOnFullRebalance1 = new RowId(PARTITION_ID);

        BinaryRow binaryRowOnFullRebalance0 = binaryRow(new TestKey(2, "2"), new TestValue(2, "2"));
        BinaryRow binaryRowOnFullRebalance1 = binaryRow(new TestKey(3, "3"), new TestValue(3, "3"));

        HybridTimestamp timestampOnFullRebalance0 = clock.now();
        HybridTimestamp timestampOnFullRebalance1 = clock.now();

        IndexRow indexRowOnFullRebalance0 = indexRow(binaryRowOnFullRebalance0, rowIdOnFullRebalance0);
        IndexRow indexRowOnFullRebalance1 = indexRow(binaryRowOnFullRebalance1, rowIdOnFullRebalance1);

        mvPartitionStorage.runConsistently(() -> {
            mvPartitionStorage.addWriteCommitted(
                    rowIdOnFullRebalance0,
                    binaryRowOnFullRebalance0,
                    timestampOnFullRebalance0
            );

            mvPartitionStorage.addWrite(
                    rowIdOnFullRebalance1,
                    binaryRowOnFullRebalance1,
                    UUID.randomUUID(),
                    UUID.randomUUID(),
                    PARTITION_ID
            );

            mvPartitionStorage.commitWrite(
                    rowIdOnFullRebalance1,
                    timestampOnFullRebalance1
            );

            hashIndexStorage.put(indexRowOnFullRebalance0);
            hashIndexStorage.put(indexRowOnFullRebalance1);

            sortedIndexStorage.put(indexRowOnFullRebalance0);
            sortedIndexStorage.put(indexRowOnFullRebalance1);

            return null;
        });

        // Let's finish a full rebalancing.

        // Partition is out of configuration range.
        assertThrows(
                IllegalArgumentException.class,
                () -> tableStorage.finishFullRebalancePartition(getOutConfigRangePartitionId(), 100, 500)
        );

        // Partition does not exist.
        assertThrows(
                StorageFullRebalanceException.class,
                () -> tableStorage.finishFullRebalancePartition(1, 100, 500)
        );

        tableStorage.finishFullRebalancePartition(PARTITION_ID, 10, 20).get(1, SECONDS);

        // Let's check the storages after success finish a full rebalance.

        assertThat(getAll(mvPartitionStorage.scanVersions(rowIdBeforeFullRebalanceStart0)), is(empty()));
        assertThat(getAll(mvPartitionStorage.scanVersions(rowIdBeforeFullRebalanceStart1)), is(empty()));

        assertThat(
                getAll(mvPartitionStorage.scanVersions(rowIdOnFullRebalance0)).stream().map(ReadResult::binaryRow).collect(toList()),
                containsInAnyOrder(binaryRowOnFullRebalance0)
        );

        assertThat(
                getAll(mvPartitionStorage.scanVersions(rowIdOnFullRebalance1)).stream().map(ReadResult::binaryRow).collect(toList()),
                containsInAnyOrder(binaryRowOnFullRebalance1)
        );

        assertThat(getAll(hashIndexStorage.get(indexRowBeforeFullRebalanceStart0.indexColumns())), is(empty()));
        assertThat(getAll(hashIndexStorage.get(indexRowBeforeFullRebalanceStart1.indexColumns())), is(empty()));

        assertThat(getAll(hashIndexStorage.get(indexRowOnFullRebalance0.indexColumns())), contains(rowIdOnFullRebalance0));
        assertThat(getAll(hashIndexStorage.get(indexRowOnFullRebalance1.indexColumns())), contains(rowIdOnFullRebalance1));

        assertThat(
                getAll(sortedIndexStorage.scan(null, null, 0)).stream().map(IndexRow::rowId).collect(toList()),
                containsInAnyOrder(rowIdOnFullRebalance0, rowIdOnFullRebalance1)
        );

        assertEquals(10, mvPartitionStorage.lastAppliedIndex());
        assertEquals(10, mvPartitionStorage.persistedIndex());
        assertEquals(20, mvPartitionStorage.lastAppliedTerm());
    }

    @Test
    public void testFailFullRebalance() throws Exception {
        MvPartitionStorage mvPartitionStorage = tableStorage.getOrCreateMvPartition(PARTITION_ID);
        HashIndexStorage hashIndexStorage = tableStorage.getOrCreateHashIndex(PARTITION_ID, hashIdx.id());
        SortedIndexStorage sortedIndexStorage = tableStorage.getOrCreateSortedIndex(PARTITION_ID, sortedIdx.id());

        // Nothing will happen because the full rebalancing has not started.
        tableStorage.abortFullRebalancePartition(PARTITION_ID).get(1, SECONDS);

        // Let's fill the storages before a full rebalance start.

        RowId rowIdBeforeFullRebalanceStart0 = new RowId(PARTITION_ID);
        RowId rowIdBeforeFullRebalanceStart1 = new RowId(PARTITION_ID);

        BinaryRow binaryRowBeforeFullRebalanceStart0 = binaryRow(new TestKey(0, "0"), new TestValue(0, "0"));
        BinaryRow binaryRowBeforeFullRebalanceStart1 = binaryRow(new TestKey(1, "1"), new TestValue(1, "1"));

        HybridTimestamp timestampBeforeFullRebalanceStart0 = clock.now();
        HybridTimestamp timestampBeforeFullRebalanceStart1 = clock.now();

        IndexRow indexRowBeforeFullRebalanceStart0 = indexRow(binaryRowBeforeFullRebalanceStart0, rowIdBeforeFullRebalanceStart0);
        IndexRow indexRowBeforeFullRebalanceStart1 = indexRow(binaryRowBeforeFullRebalanceStart1, rowIdBeforeFullRebalanceStart1);

        mvPartitionStorage.runConsistently(() -> {
            mvPartitionStorage.addWriteCommitted(
                    rowIdBeforeFullRebalanceStart0,
                    binaryRowBeforeFullRebalanceStart0,
                    timestampBeforeFullRebalanceStart0
            );

            mvPartitionStorage.addWriteCommitted(
                    rowIdBeforeFullRebalanceStart1,
                    binaryRowBeforeFullRebalanceStart1,
                    timestampBeforeFullRebalanceStart1
            );

            hashIndexStorage.put(indexRowBeforeFullRebalanceStart0);
            hashIndexStorage.put(indexRowBeforeFullRebalanceStart1);

            sortedIndexStorage.put(indexRowBeforeFullRebalanceStart0);
            sortedIndexStorage.put(indexRowBeforeFullRebalanceStart1);

            return null;
        });

        // Let's open the cursors.

        Cursor<ReadResult> mvPartitionCursorBeforeFullRebalanceStart0 = mvPartitionStorage.scanVersions(rowIdBeforeFullRebalanceStart0);
        Cursor<ReadResult> mvPartitionCursorBeforeFullRebalanceStart1 = mvPartitionStorage.scan(timestampBeforeFullRebalanceStart0);

        Cursor<RowId> hashIndexCursorBeforeFullRebalanceStart = hashIndexStorage.get(indexRowBeforeFullRebalanceStart0.indexColumns());

        Cursor<RowId> sortedIndexCursorBeforeFullRebalanceStart0 = sortedIndexStorage.get(indexRowBeforeFullRebalanceStart0.indexColumns());
        Cursor<IndexRow> sortedIndexCursorBeforeFullRebalanceStart1 = sortedIndexStorage.scan(null, null, 0);

        // Partition is out of configuration range.
        assertThrows(IllegalArgumentException.class, () -> tableStorage.startFullRebalancePartition(getOutConfigRangePartitionId()));

        // Partition does not exist.
        assertThrows(StorageFullRebalanceException.class, () -> tableStorage.startFullRebalancePartition(1));

        // Let's start a full rebalancing of the partition.
        tableStorage.startFullRebalancePartition(PARTITION_ID).get(1, SECONDS);

        // Once again, a full rebalancing of the partition cannot be started.
        assertThrows(StorageFullRebalanceException.class, () -> tableStorage.startFullRebalancePartition(PARTITION_ID));

        checkMvPartitionStorageMethodsAfterStartFullRebalance(mvPartitionStorage);
        checkHashIndexStorageMethodsAfterStartFullRebalance(hashIndexStorage);
        checkSortedIndexStorageMethodsAfterStartFullRebalance(sortedIndexStorage);

        checkCursorAfterStartFullRebalance(mvPartitionCursorBeforeFullRebalanceStart0);
        checkCursorAfterStartFullRebalance(mvPartitionCursorBeforeFullRebalanceStart1);
        checkCursorAfterStartFullRebalance(hashIndexCursorBeforeFullRebalanceStart);
        checkCursorAfterStartFullRebalance(sortedIndexCursorBeforeFullRebalanceStart0);
        checkCursorAfterStartFullRebalance(sortedIndexCursorBeforeFullRebalanceStart1);

        // Let's fill the storages with fresh data on rebalance.

        RowId rowIdOnFullRebalance0 = new RowId(PARTITION_ID);
        RowId rowIdOnFullRebalance1 = new RowId(PARTITION_ID);

        BinaryRow binaryRowOnFullRebalance0 = binaryRow(new TestKey(2, "2"), new TestValue(2, "2"));
        BinaryRow binaryRowOnFullRebalance1 = binaryRow(new TestKey(3, "3"), new TestValue(3, "3"));

        HybridTimestamp timestampOnFullRebalance0 = clock.now();
        HybridTimestamp timestampOnFullRebalance1 = clock.now();

        IndexRow indexRowOnFullRebalance0 = indexRow(binaryRowOnFullRebalance0, rowIdOnFullRebalance0);
        IndexRow indexRowOnFullRebalance1 = indexRow(binaryRowOnFullRebalance1, rowIdOnFullRebalance1);

        mvPartitionStorage.runConsistently(() -> {
            mvPartitionStorage.addWriteCommitted(
                    rowIdOnFullRebalance0,
                    binaryRowOnFullRebalance0,
                    timestampOnFullRebalance0
            );

            mvPartitionStorage.addWrite(
                    rowIdOnFullRebalance1,
                    binaryRowOnFullRebalance1,
                    UUID.randomUUID(),
                    UUID.randomUUID(),
                    PARTITION_ID
            );

            mvPartitionStorage.commitWrite(
                    rowIdOnFullRebalance1,
                    timestampOnFullRebalance1
            );

            hashIndexStorage.put(indexRowOnFullRebalance0);
            hashIndexStorage.put(indexRowOnFullRebalance1);

            sortedIndexStorage.put(indexRowOnFullRebalance0);
            sortedIndexStorage.put(indexRowOnFullRebalance1);

            return null;
        });

        // Let's abort a full rebalancing.

        // Partition is out of configuration range.
        assertThrows(IllegalArgumentException.class, () -> tableStorage.abortFullRebalancePartition(getOutConfigRangePartitionId()));

        tableStorage.abortFullRebalancePartition(PARTITION_ID).get(1, SECONDS);

        // Let's check the storages after abort a full rebalance.

        assertThat(getAll(mvPartitionStorage.scanVersions(rowIdBeforeFullRebalanceStart0)), is(empty()));
        assertThat(getAll(mvPartitionStorage.scanVersions(rowIdBeforeFullRebalanceStart1)), is(empty()));
        assertThat(getAll(mvPartitionStorage.scanVersions(rowIdOnFullRebalance0)), is(empty()));
        assertThat(getAll(mvPartitionStorage.scanVersions(rowIdOnFullRebalance1)), is(empty()));

        assertThat(getAll(hashIndexStorage.get(indexRowBeforeFullRebalanceStart0.indexColumns())), is(empty()));
        assertThat(getAll(hashIndexStorage.get(indexRowBeforeFullRebalanceStart1.indexColumns())), is(empty()));
        assertThat(getAll(hashIndexStorage.get(indexRowOnFullRebalance0.indexColumns())), is(empty()));
        assertThat(getAll(hashIndexStorage.get(indexRowOnFullRebalance1.indexColumns())), is(empty()));

        assertThat(getAll(sortedIndexStorage.scan(null, null, 0)), is(empty()));

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

    private int getOutConfigRangePartitionId() {
        return tableStorage.configuration().partitions().value();
    }

    private void checkMvPartitionStorageMethodsAfterStartFullRebalance(MvPartitionStorage storage) {
        assertEquals(FULL_REBALANCE_IN_PROGRESS, storage.lastAppliedIndex());
        assertEquals(FULL_REBALANCE_IN_PROGRESS, storage.persistedIndex());
        assertEquals(FULL_REBALANCE_IN_PROGRESS, storage.lastAppliedTerm());

        assertDoesNotThrow(() -> storage.committedGroupConfiguration());

        storage.runConsistently(() -> {
            assertThrows(StorageFullRebalanceException.class, () -> storage.lastApplied(100, 500));

            assertThrows(
                    StorageFullRebalanceException.class,
                    () -> storage.committedGroupConfiguration(mock(RaftGroupConfiguration.class))
            );

            RowId rowId = new RowId(PARTITION_ID);

            assertThrows(StorageFullRebalanceException.class, () -> storage.read(rowId, clock.now()));
            assertThrows(StorageFullRebalanceException.class, () -> storage.scanVersions(rowId));
            assertThrows(StorageFullRebalanceException.class, () -> storage.scan(clock.now()));
            assertThrows(StorageFullRebalanceException.class, () -> storage.closestRowId(rowId));
            assertThrows(StorageFullRebalanceException.class, storage::rowsCount);

            return null;
        });
    }

    private static void checkHashIndexStorageMethodsAfterStartFullRebalance(HashIndexStorage storage) {
        assertDoesNotThrow(storage::indexDescriptor);

        assertThrows(StorageFullRebalanceException.class, () -> storage.get(mock(BinaryTuple.class)));
    }

    private static void checkSortedIndexStorageMethodsAfterStartFullRebalance(SortedIndexStorage storage) {
        assertDoesNotThrow(storage::indexDescriptor);

        assertThrows(StorageFullRebalanceException.class, () -> storage.get(mock(BinaryTuple.class)));
        assertThrows(StorageFullRebalanceException.class, () -> storage.scan(null, null, 0));
    }

    private static void checkCursorAfterStartFullRebalance(Cursor<?> cursor) {
        assertDoesNotThrow(cursor::close);

        assertThrows(StorageFullRebalanceException.class, cursor::hasNext);
        assertThrows(StorageFullRebalanceException.class, cursor::next);
    }
}
