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
import static org.apache.ignite.internal.catalog.CatalogManagerImpl.INITIAL_CAUSALITY_TOKEN;
import static org.apache.ignite.internal.catalog.descriptors.CatalogColumnCollation.ASC_NULLS_LAST;
import static org.apache.ignite.internal.schema.BinaryRowMatcher.equalToRow;
import static org.apache.ignite.internal.storage.MvPartitionStorage.REBALANCE_IN_PROGRESS;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureExceptionMatcher.willThrowFast;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.sql.ColumnType.INT32;
import static org.apache.ignite.sql.ColumnType.STRING;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.binarytuple.BinaryTupleBuilder;
import org.apache.ignite.internal.catalog.CatalogService;
import org.apache.ignite.internal.catalog.commands.CatalogUtils;
import org.apache.ignite.internal.catalog.commands.ColumnParams;
import org.apache.ignite.internal.catalog.descriptors.CatalogHashIndexDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogIndexColumnDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogIndexDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogSortedIndexDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableDescriptor;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.lang.IgniteTuple3;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.BinaryTuple;
import org.apache.ignite.internal.schema.BinaryTupleSchema;
import org.apache.ignite.internal.schema.BinaryTupleSchema.Element;
import org.apache.ignite.internal.schema.NativeTypes;
import org.apache.ignite.internal.storage.engine.MvTableStorage;
import org.apache.ignite.internal.storage.index.HashIndexStorage;
import org.apache.ignite.internal.storage.index.IndexRow;
import org.apache.ignite.internal.storage.index.IndexRowImpl;
import org.apache.ignite.internal.storage.index.IndexStorage;
import org.apache.ignite.internal.storage.index.PeekCursor;
import org.apache.ignite.internal.storage.index.SortedIndexStorage;
import org.apache.ignite.internal.storage.index.StorageHashIndexDescriptor;
import org.apache.ignite.internal.storage.index.StorageIndexDescriptor;
import org.apache.ignite.internal.storage.index.StorageSortedIndexDescriptor;
import org.apache.ignite.internal.util.Cursor;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

/**
 * Abstract class that contains tests for {@link MvTableStorage} implementations.
 */
public abstract class AbstractMvTableStorageTest extends BaseMvStoragesTest {
    private static final String TABLE_NAME = "foo";

    private static final String SORTED_INDEX_NAME = "SORTED_IDX";

    private static final String HASH_INDEX_NAME = "HASH_IDX";

    protected static final int PARTITION_ID = 0;

    /** Partition id for 0 storage. */
    protected static final int PARTITION_ID_0 = 10;

    /** Partition id for 1 storage. */
    protected static final int PARTITION_ID_1 = 9;

    protected static final int COMMIT_TABLE_ID = 999;

    protected MvTableStorage tableStorage;

    protected StorageSortedIndexDescriptor sortedIdx;

    protected StorageHashIndexDescriptor hashIdx;

    protected final CatalogService catalogService = mock(CatalogService.class);

    /**
     * Initializes the internal structures needed for tests.
     *
     * <p>This method *MUST* always be called in either subclass' constructor or setUp method.
     */
    protected final void initialize() {
        createTestTableAndIndexes(catalogService);

        this.tableStorage = createMvTableStorage();
        this.tableStorage.start();

        CatalogTableDescriptor catalogTableDescriptor = catalogService.table(TABLE_NAME, clock.nowLong());

        CatalogIndexDescriptor catalogSortedIndexDescriptor = catalogService.index(SORTED_INDEX_NAME, clock.nowLong());
        CatalogIndexDescriptor catalogHashIndexDescriptor = catalogService.index(HASH_INDEX_NAME, clock.nowLong());

        sortedIdx = new StorageSortedIndexDescriptor(catalogTableDescriptor, (CatalogSortedIndexDescriptor) catalogSortedIndexDescriptor);
        hashIdx = new StorageHashIndexDescriptor(catalogTableDescriptor, (CatalogHashIndexDescriptor) catalogHashIndexDescriptor);
    }

    @AfterEach
    protected void tearDown() throws Exception {
        if (tableStorage != null) {
            tableStorage.close();
        }
    }

    protected abstract MvTableStorage createMvTableStorage();

    /**
     * Tests that {@link MvTableStorage#getMvPartition(int)} correctly returns an existing partition.
     */
    @Test
    void testCreatePartition() {
        assertThrows(IllegalArgumentException.class, () -> tableStorage.createMvPartition(getPartitionIdOutOfRange()));

        MvPartitionStorage absentStorage = tableStorage.getMvPartition(0);

        assertThat(absentStorage, is(nullValue()));

        MvPartitionStorage partitionStorage = getOrCreateMvPartition(0);

        assertThat(partitionStorage, is(notNullValue()));

        assertThat(partitionStorage, is(sameInstance(tableStorage.getMvPartition(0))));

        StorageException exception = assertThrows(StorageException.class, () -> tableStorage.createMvPartition(0));

        assertThat(exception.getMessage(), containsString("Storage already exists"));
    }

    /**
     * Tests that partition data does not overlap.
     */
    @Test
    void testPartitionIndependence() {
        MvPartitionStorage partitionStorage0 = getOrCreateMvPartition(PARTITION_ID_0);
        // Using a shifted ID value to test a multibyte scenario.
        MvPartitionStorage partitionStorage1 = getOrCreateMvPartition(PARTITION_ID_1);

        var testData0 = binaryRow(new TestKey(1, "0"), new TestValue(10, "10"));

        UUID txId = UUID.randomUUID();

        RowId rowId0 = new RowId(PARTITION_ID_0);

        partitionStorage0.runConsistently(locker -> {
            locker.lock(rowId0);

            return partitionStorage0.addWrite(rowId0, testData0, txId, COMMIT_TABLE_ID, 0);
        });

        assertThat(unwrap(partitionStorage0.read(rowId0, HybridTimestamp.MAX_VALUE)), is(equalTo(unwrap(testData0))));
        assertThrows(IllegalArgumentException.class, () -> partitionStorage1.read(rowId0, HybridTimestamp.MAX_VALUE));

        var testData1 = binaryRow(new TestKey(2, "2"), new TestValue(20, "20"));

        RowId rowId1 = new RowId(PARTITION_ID_1);

        partitionStorage1.runConsistently(locker -> {
            locker.lock(rowId1);

            return partitionStorage1.addWrite(rowId1, testData1, txId, COMMIT_TABLE_ID, 0);
        });

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
        assertThrows(StorageException.class, () -> tableStorage.getOrCreateIndex(PARTITION_ID, sortedIdx));
        assertThrows(StorageException.class, () -> tableStorage.getOrCreateIndex(PARTITION_ID, hashIdx));

        // Index should only be available after the associated partition has been created.
        tableStorage.createMvPartition(PARTITION_ID);

        assertThat(tableStorage.getOrCreateIndex(PARTITION_ID, sortedIdx), is(instanceOf(SortedIndexStorage.class)));
        assertThat(tableStorage.getOrCreateIndex(PARTITION_ID, hashIdx), is(instanceOf(HashIndexStorage.class)));

        assertThrows(StorageException.class, () -> tableStorage.getOrCreateIndex(PARTITION_ID, mock(StorageIndexDescriptor.class)));
    }

    /**
     * Test creating a Sorted Index.
     */
    @Test
    public void testCreateSortedIndex() {
        assertThrows(StorageException.class, () -> tableStorage.getOrCreateSortedIndex(PARTITION_ID, sortedIdx));

        // Index should only be available after the associated partition has been created.
        tableStorage.createMvPartition(PARTITION_ID);

        assertThat(tableStorage.getOrCreateSortedIndex(PARTITION_ID, sortedIdx), is(notNullValue()));
    }

    /**
     * Test creating a Hash Index.
     */
    @Test
    public void testCreateHashIndex() {
        assertThrows(StorageException.class, () -> tableStorage.getOrCreateHashIndex(PARTITION_ID, hashIdx));

        // Index should only be available after the associated partition has been created.
        tableStorage.createMvPartition(PARTITION_ID);

        assertThat(tableStorage.getOrCreateHashIndex(PARTITION_ID, hashIdx), is(notNullValue()));
    }

    /**
     * Tests destroying an index.
     */
    @Disabled("https://issues.apache.org/jira/browse/IGNITE-17626")
    @Test
    public void testDestroyIndex() {
        MvPartitionStorage partitionStorage = getOrCreateMvPartition(PARTITION_ID);

        assertThat(tableStorage.getOrCreateSortedIndex(PARTITION_ID, sortedIdx), is(notNullValue()));
        assertThat(tableStorage.getOrCreateHashIndex(PARTITION_ID, hashIdx), is(notNullValue()));

        CompletableFuture<Void> destroySortedIndexFuture = tableStorage.destroyIndex(sortedIdx.id());
        CompletableFuture<Void> destroyHashIndexFuture = tableStorage.destroyIndex(hashIdx.id());

        assertThat(partitionStorage.flush(), willCompleteSuccessfully());
        assertThat(destroySortedIndexFuture, willCompleteSuccessfully());
        assertThat(destroyHashIndexFuture, willCompleteSuccessfully());
    }

    @Test
    public void testHashIndexIndependence() {
        MvPartitionStorage partitionStorage1 = getOrCreateMvPartition(PARTITION_ID);

        assertThat(tableStorage.getOrCreateHashIndex(PARTITION_ID, hashIdx), is(notNullValue()));
        assertThrows(StorageException.class, () -> tableStorage.getOrCreateHashIndex(PARTITION_ID + 1, hashIdx));

        MvPartitionStorage partitionStorage2 = getOrCreateMvPartition(PARTITION_ID + 1);

        HashIndexStorage storage1 = tableStorage.getOrCreateHashIndex(PARTITION_ID, hashIdx);
        HashIndexStorage storage2 = tableStorage.getOrCreateHashIndex(PARTITION_ID + 1, hashIdx);

        assertThat(storage1, is(notNullValue()));
        assertThat(storage2, is(notNullValue()));

        var rowId1 = new RowId(PARTITION_ID);
        var rowId2 = new RowId(PARTITION_ID + 1);

        BinaryTupleSchema schema = BinaryTupleSchema.create(new Element[]{
                new Element(NativeTypes.INT32, false),
                new Element(NativeTypes.INT32, false)
        });

        ByteBuffer buffer = new BinaryTupleBuilder(schema.elementCount())
                .appendInt(1)
                .appendInt(2)
                .build();

        BinaryTuple tuple = new BinaryTuple(schema.elementCount(), buffer);

        partitionStorage1.runConsistently(locker -> {
            storage1.put(new IndexRowImpl(tuple, rowId1));

            return null;
        });

        partitionStorage2.runConsistently(locker -> {
            storage2.put(new IndexRowImpl(tuple, rowId2));

            return null;
        });

        assertThat(getAll(storage1.get(tuple)), contains(rowId1));
        assertThat(getAll(storage2.get(tuple)), contains(rowId2));
    }

    @Test
    public void testDestroyPartition() throws Exception {
        assertThrows(IllegalArgumentException.class, () -> tableStorage.destroyPartition(getPartitionIdOutOfRange()));

        MvPartitionStorage mvPartitionStorage = getOrCreateMvPartition(PARTITION_ID);
        HashIndexStorage hashIndexStorage = tableStorage.getOrCreateHashIndex(PARTITION_ID, hashIdx);
        SortedIndexStorage sortedIndexStorage = tableStorage.getOrCreateSortedIndex(PARTITION_ID, sortedIdx);

        RowId rowId = new RowId(PARTITION_ID);

        BinaryRow binaryRow = binaryRow(new TestKey(0, "0"), new TestValue(1, "1"));

        IndexRow hashIndexRow = indexRow(hashIndexStorage.indexDescriptor(), binaryRow, rowId);
        IndexRow sortedIndexRow = indexRow(sortedIndexStorage.indexDescriptor(), binaryRow, rowId);

        mvPartitionStorage.runConsistently(locker -> {
            locker.lock(rowId);

            mvPartitionStorage.addWriteCommitted(rowId, binaryRow, clock.now());

            hashIndexStorage.put(hashIndexRow);

            sortedIndexStorage.put(sortedIndexRow);

            return null;
        });

        PartitionTimestampCursor scanTimestampCursor = mvPartitionStorage.scan(clock.now());

        Cursor<RowId> getFromHashIndexCursor = hashIndexStorage.get(hashIndexRow.indexColumns());

        Cursor<RowId> getFromSortedIndexCursor = sortedIndexStorage.get(hashIndexRow.indexColumns());
        Cursor<IndexRow> scanFromSortedIndexCursor = sortedIndexStorage.scan(null, null, 0);

        tableStorage.destroyPartition(PARTITION_ID).get(1, SECONDS);

        // Let's check that we won't get destroyed storages.
        assertNull(tableStorage.getMvPartition(PARTITION_ID));
        assertThrows(StorageException.class, () -> tableStorage.getOrCreateHashIndex(PARTITION_ID, hashIdx));
        assertThrows(StorageException.class, () -> tableStorage.getOrCreateSortedIndex(PARTITION_ID, sortedIdx));

        checkStorageDestroyed(mvPartitionStorage);
        checkStorageDestroyed(hashIndexStorage);
        checkStorageDestroyed(sortedIndexStorage);

        assertThrows(StorageClosedException.class, () -> getAll(scanTimestampCursor));

        assertThrows(StorageClosedException.class, () -> getAll(getFromHashIndexCursor));

        assertThrows(StorageClosedException.class, () -> getAll(getFromSortedIndexCursor));
        assertThrows(StorageClosedException.class, () -> getAll(scanFromSortedIndexCursor));

        // What happens if there is no partition?
        assertThrows(StorageException.class, () -> tableStorage.destroyPartition(PARTITION_ID));
    }

    @Test
    public void testReCreatePartition() throws Exception {
        MvPartitionStorage mvPartitionStorage = getOrCreateMvPartition(PARTITION_ID);

        RowId rowId = new RowId(PARTITION_ID);

        BinaryRow binaryRow = binaryRow(new TestKey(0, "0"), new TestValue(1, "1"));

        mvPartitionStorage.runConsistently(locker -> {
            locker.lock(rowId);

            mvPartitionStorage.addWriteCommitted(rowId, binaryRow, clock.now());

            return null;
        });

        tableStorage.destroyPartition(PARTITION_ID).get(1, SECONDS);

        MvPartitionStorage newMvPartitionStorage = getOrCreateMvPartition(PARTITION_ID);

        List<ReadResult> allVersions = newMvPartitionStorage.runConsistently(locker -> {
            locker.lock(rowId);

            return getAll(newMvPartitionStorage.scanVersions(rowId));
        });

        assertThat(allVersions, empty());
    }

    @Test
    public void testSuccessRebalance() {
        MvPartitionStorage mvPartitionStorage = getOrCreateMvPartition(PARTITION_ID);
        HashIndexStorage hashIndexStorage = tableStorage.getOrCreateHashIndex(PARTITION_ID, hashIdx);
        SortedIndexStorage sortedIndexStorage = tableStorage.getOrCreateSortedIndex(PARTITION_ID, sortedIdx);

        // Error because reblance has not yet started for the partition.
        assertThrows(
                StorageRebalanceException.class,
                () -> tableStorage.finishRebalancePartition(PARTITION_ID, 100, 500, new byte[0])
        );

        List<IgniteTuple3<RowId, BinaryRow, HybridTimestamp>> rowsBeforeRebalanceStart = List.of(
                new IgniteTuple3<>(new RowId(PARTITION_ID), binaryRow(new TestKey(0, "0"), new TestValue(0, "0")), clock.now()),
                new IgniteTuple3<>(new RowId(PARTITION_ID), binaryRow(new TestKey(1, "1"), new TestValue(1, "1")), clock.now())
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
                new IgniteTuple3<>(new RowId(PARTITION_ID), binaryRow(new TestKey(2, "2"), new TestValue(2, "2")), clock.now()),
                new IgniteTuple3<>(new RowId(PARTITION_ID), binaryRow(new TestKey(3, "3"), new TestValue(3, "3")), clock.now())
        );

        fillStorages(mvPartitionStorage, hashIndexStorage, sortedIndexStorage, rowsOnRebalance);

        checkLastApplied(mvPartitionStorage, REBALANCE_IN_PROGRESS, REBALANCE_IN_PROGRESS);
        assertNull(mvPartitionStorage.committedGroupConfiguration());

        // Let's finish rebalancing.

        // Partition is out of configuration range.
        assertThrows(
                IllegalArgumentException.class,
                () -> tableStorage.finishRebalancePartition(getPartitionIdOutOfRange(), 100, 500, new byte[0])
        );

        // Partition does not exist.
        assertThrows(
                StorageRebalanceException.class,
                () -> tableStorage.finishRebalancePartition(1, 100, 500, new byte[0])
        );

        byte[] raftGroupConfig = createRandomRaftGroupConfiguration();

        assertThat(tableStorage.finishRebalancePartition(PARTITION_ID, 10, 20, raftGroupConfig), willCompleteSuccessfully());

        // Let's check the storages after success finish rebalance.
        checkForMissingRows(mvPartitionStorage, hashIndexStorage, sortedIndexStorage, rowsBeforeRebalanceStart);
        checkForPresenceRows(mvPartitionStorage, hashIndexStorage, sortedIndexStorage, rowsOnRebalance);

        checkLastApplied(mvPartitionStorage, 10, 20);
        checkRaftGroupConfigs(raftGroupConfig, mvPartitionStorage.committedGroupConfiguration());
    }

    @Test
    public void testFailRebalance() throws Exception {
        MvPartitionStorage mvPartitionStorage = getOrCreateMvPartition(PARTITION_ID);
        HashIndexStorage hashIndexStorage = tableStorage.getOrCreateHashIndex(PARTITION_ID, hashIdx);
        SortedIndexStorage sortedIndexStorage = tableStorage.getOrCreateSortedIndex(PARTITION_ID, sortedIdx);

        // Nothing will happen because rebalancing has not started.
        tableStorage.abortRebalancePartition(PARTITION_ID).get(1, SECONDS);

        List<IgniteTuple3<RowId, BinaryRow, HybridTimestamp>> rowsBeforeRebalanceStart = List.of(
                new IgniteTuple3<>(new RowId(PARTITION_ID), binaryRow(new TestKey(0, "0"), new TestValue(0, "0")), clock.now()),
                new IgniteTuple3<>(new RowId(PARTITION_ID), binaryRow(new TestKey(1, "1"), new TestValue(1, "1")), clock.now())
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
                new IgniteTuple3<>(new RowId(PARTITION_ID), binaryRow(new TestKey(2, "2"), new TestValue(2, "2")), clock.now()),
                new IgniteTuple3<>(new RowId(PARTITION_ID), binaryRow(new TestKey(3, "3"), new TestValue(3, "3")), clock.now())
        );

        fillStorages(mvPartitionStorage, hashIndexStorage, sortedIndexStorage, rowsOnRebalance);

        checkLastApplied(mvPartitionStorage, REBALANCE_IN_PROGRESS, REBALANCE_IN_PROGRESS);

        // Let's abort rebalancing.

        // Partition is out of configuration range.
        assertThrows(IllegalArgumentException.class, () -> tableStorage.abortRebalancePartition(getPartitionIdOutOfRange()));

        assertThat(tableStorage.abortRebalancePartition(PARTITION_ID), willCompleteSuccessfully());

        // Let's check the storages after abort rebalance.
        checkForMissingRows(mvPartitionStorage, hashIndexStorage, sortedIndexStorage, rowsBeforeRebalanceStart);
        checkForMissingRows(mvPartitionStorage, hashIndexStorage, sortedIndexStorage, rowsOnRebalance);

        checkLastApplied(mvPartitionStorage, 0, 0);
        assertNull(mvPartitionStorage.committedGroupConfiguration());
    }

    @Test
    public void testStartRebalanceForClosedPartition() {
        MvPartitionStorage mvPartitionStorage = getOrCreateMvPartition(PARTITION_ID);

        mvPartitionStorage.close();

        assertThat(tableStorage.startRebalancePartition(PARTITION_ID), willThrowFast(StorageRebalanceException.class));
    }

    @Test
    public void testDestroyTableStorage() {
        MvPartitionStorage mvPartitionStorage = getOrCreateMvPartition(PARTITION_ID);
        HashIndexStorage hashIndexStorage = tableStorage.getOrCreateHashIndex(PARTITION_ID, hashIdx);
        SortedIndexStorage sortedIndexStorage = tableStorage.getOrCreateSortedIndex(PARTITION_ID, sortedIdx);

        List<IgniteTuple3<RowId, BinaryRow, HybridTimestamp>> rows = List.of(
                new IgniteTuple3<>(new RowId(PARTITION_ID), binaryRow(new TestKey(0, "0"), new TestValue(0, "0")), clock.now()),
                new IgniteTuple3<>(new RowId(PARTITION_ID), binaryRow(new TestKey(1, "1"), new TestValue(1, "1")), clock.now())
        );

        fillStorages(mvPartitionStorage, hashIndexStorage, sortedIndexStorage, rows);

        PartitionTimestampCursor scanTimestampCursor = mvPartitionStorage.scan(clock.now());

        IndexRow hashIndexRow = indexRow(hashIndexStorage.indexDescriptor(), rows.get(0).get2(), rows.get(0).get1());
        IndexRow sortedIndexRow = indexRow(sortedIndexStorage.indexDescriptor(), rows.get(0).get2(), rows.get(0).get1());

        Cursor<RowId> getFromHashIndexCursor = hashIndexStorage.get(hashIndexRow.indexColumns());

        Cursor<RowId> getFromSortedIndexCursor = sortedIndexStorage.get(sortedIndexRow.indexColumns());
        Cursor<IndexRow> scanFromSortedIndexCursor = sortedIndexStorage.scan(null, null, 0);

        assertThat(tableStorage.destroy(), willCompleteSuccessfully());

        checkStorageDestroyed(mvPartitionStorage);
        checkStorageDestroyed(hashIndexStorage);
        checkStorageDestroyed(sortedIndexStorage);

        assertThrows(StorageClosedException.class, () -> getAll(scanTimestampCursor));

        assertThrows(StorageClosedException.class, () -> getAll(getFromHashIndexCursor));

        assertThrows(StorageClosedException.class, () -> getAll(getFromSortedIndexCursor));
        assertThrows(StorageClosedException.class, () -> getAll(scanFromSortedIndexCursor));

        // Let's check that nothing will happen if we try to destroy it again.
        assertThat(tableStorage.destroy(), willCompleteSuccessfully());

        // Let's check that after restarting the table we will have an empty partition.
        tableStorage = createMvTableStorage();

        tableStorage.start();

        mvPartitionStorage = getOrCreateMvPartition(PARTITION_ID);
        hashIndexStorage = tableStorage.getOrCreateHashIndex(PARTITION_ID, hashIdx);
        sortedIndexStorage = tableStorage.getOrCreateSortedIndex(PARTITION_ID, sortedIdx);

        checkForMissingRows(mvPartitionStorage, hashIndexStorage, sortedIndexStorage, rows);
    }

    @Test
    public void testRestartStoragesInTheMiddleOfRebalance() {
        MvPartitionStorage mvPartitionStorage = getOrCreateMvPartition(PARTITION_ID);
        HashIndexStorage hashIndexStorage = tableStorage.getOrCreateHashIndex(PARTITION_ID, hashIdx);
        SortedIndexStorage sortedIndexStorage = tableStorage.getOrCreateSortedIndex(PARTITION_ID, sortedIdx);

        List<IgniteTuple3<RowId, BinaryRow, HybridTimestamp>> rows = List.of(
                new IgniteTuple3<>(new RowId(PARTITION_ID), binaryRow(new TestKey(0, "0"), new TestValue(0, "0")), clock.now()),
                new IgniteTuple3<>(new RowId(PARTITION_ID), binaryRow(new TestKey(1, "1"), new TestValue(1, "1")), clock.now())
        );

        fillStorages(mvPartitionStorage, hashIndexStorage, sortedIndexStorage, rows);

        // Since it is not possible to close storages in middle of rebalance, we will shorten path a bit by updating only lastApplied*.
        MvPartitionStorage finalMvPartitionStorage = mvPartitionStorage;

        mvPartitionStorage.runConsistently(locker -> {
            finalMvPartitionStorage.lastApplied(REBALANCE_IN_PROGRESS, REBALANCE_IN_PROGRESS);

            return null;
        });

        assertThat(mvPartitionStorage.flush(), willCompleteSuccessfully());

        // Restart storages.
        tableStorage.stop();

        tableStorage = createMvTableStorage();

        tableStorage.start();

        mvPartitionStorage = getOrCreateMvPartition(PARTITION_ID);
        hashIndexStorage = tableStorage.getOrCreateHashIndex(PARTITION_ID, hashIdx);
        sortedIndexStorage = tableStorage.getOrCreateSortedIndex(PARTITION_ID, sortedIdx);

        if (tableStorage.isVolatile()) {
            // Let's check the repositories: they should be empty.
            checkForMissingRows(mvPartitionStorage, hashIndexStorage, sortedIndexStorage, rows);

            checkLastApplied(mvPartitionStorage, 0, 0);
        } else {
            checkForPresenceRows(mvPartitionStorage, hashIndexStorage, sortedIndexStorage, rows);

            checkLastApplied(mvPartitionStorage, REBALANCE_IN_PROGRESS, REBALANCE_IN_PROGRESS);
        }
    }

    @Test
    void testClear() {
        assertThrows(IllegalArgumentException.class, () -> tableStorage.clearPartition(getPartitionIdOutOfRange()));

        // Let's check that there will be an error for a non-existent partition.
        assertThrows(StorageException.class, () -> tableStorage.clearPartition(PARTITION_ID));

        MvPartitionStorage mvPartitionStorage = getOrCreateMvPartition(PARTITION_ID);
        HashIndexStorage hashIndexStorage = tableStorage.getOrCreateHashIndex(PARTITION_ID, hashIdx);
        SortedIndexStorage sortedIndexStorage = tableStorage.getOrCreateSortedIndex(PARTITION_ID, sortedIdx);

        // Let's check the cleanup for an empty partition.
        assertThat(tableStorage.clearPartition(PARTITION_ID), willCompleteSuccessfully());

        checkLastApplied(mvPartitionStorage, 0, 0);
        assertNull(mvPartitionStorage.committedGroupConfiguration());

        // Let's fill the storages and clean them.
        List<IgniteTuple3<RowId, BinaryRow, HybridTimestamp>> rows = List.of(
                new IgniteTuple3<>(new RowId(PARTITION_ID), binaryRow(new TestKey(0, "0"), new TestValue(0, "0")), clock.now()),
                new IgniteTuple3<>(new RowId(PARTITION_ID), binaryRow(new TestKey(1, "1"), new TestValue(1, "1")), clock.now())
        );

        byte[] raftGroupConfig = createRandomRaftGroupConfiguration();

        fillStorages(mvPartitionStorage, hashIndexStorage, sortedIndexStorage, rows);

        mvPartitionStorage.runConsistently(locker -> {
            mvPartitionStorage.lastApplied(100, 500);

            mvPartitionStorage.committedGroupConfiguration(raftGroupConfig);

            return null;
        });

        // Let's clear the storages and check them out.
        assertThat(tableStorage.clearPartition(PARTITION_ID), willCompleteSuccessfully());

        checkLastApplied(mvPartitionStorage, 0, 0);
        assertNull(mvPartitionStorage.committedGroupConfiguration());

        checkForMissingRows(mvPartitionStorage, hashIndexStorage, sortedIndexStorage, rows);
    }

    @Test
    void testClearForCanceledOrRebalancedPartition() {
        MvPartitionStorage mvPartitionStorage0 = getOrCreateMvPartition(PARTITION_ID);
        getOrCreateMvPartition(PARTITION_ID + 1);

        mvPartitionStorage0.close();
        assertThat(tableStorage.startRebalancePartition(PARTITION_ID + 1), willCompleteSuccessfully());

        try {
            assertThat(tableStorage.clearPartition(PARTITION_ID), willThrowFast(StorageClosedException.class));
            assertThat(tableStorage.clearPartition(PARTITION_ID + 1), willThrowFast(StorageRebalanceException.class));
        } finally {
            assertThat(tableStorage.abortRebalancePartition(PARTITION_ID + 1), willCompleteSuccessfully());
        }
    }

    @Test
    void testCloseStartedRebalance() {
        MvPartitionStorage mvPartitionStorage = getOrCreateMvPartition(PARTITION_ID);

        assertThat(tableStorage.startRebalancePartition(PARTITION_ID), willCompleteSuccessfully());

        assertDoesNotThrow(mvPartitionStorage::close);
    }

    @Test
    void testDestroyStartedRebalance() {
        getOrCreateMvPartition(PARTITION_ID);

        assertThat(tableStorage.startRebalancePartition(PARTITION_ID), willCompleteSuccessfully());

        assertThat(tableStorage.destroyPartition(PARTITION_ID), willCompleteSuccessfully());
    }

    @Test
    void testNextRowIdToBuilt() {
        MvPartitionStorage mvPartitionStorage = getOrCreateMvPartition(PARTITION_ID);

        IndexStorage hashIndexStorage = tableStorage.getOrCreateIndex(PARTITION_ID, hashIdx);
        IndexStorage sortedIndexStorage = tableStorage.getOrCreateIndex(PARTITION_ID, sortedIdx);

        assertThat(hashIndexStorage.getNextRowIdToBuild(), equalTo(RowId.lowestRowId(PARTITION_ID)));
        assertThat(sortedIndexStorage.getNextRowIdToBuild(), equalTo(RowId.lowestRowId(PARTITION_ID)));

        RowId rowId0 = new RowId(PARTITION_ID);
        RowId rowId1 = new RowId(PARTITION_ID);

        mvPartitionStorage.runConsistently(locker -> {
            hashIndexStorage.setNextRowIdToBuild(rowId0);
            sortedIndexStorage.setNextRowIdToBuild(rowId1);

            return null;
        });

        assertThat(hashIndexStorage.getNextRowIdToBuild(), equalTo(rowId0));
        assertThat(sortedIndexStorage.getNextRowIdToBuild(), equalTo(rowId1));
    }

    @Test
    void testNextRowIdToBuiltAfterRestart() {
        MvPartitionStorage mvPartitionStorage = getOrCreateMvPartition(PARTITION_ID);

        IndexStorage hashIndexStorage = tableStorage.getOrCreateIndex(PARTITION_ID, hashIdx);
        IndexStorage sortedIndexStorage = tableStorage.getOrCreateIndex(PARTITION_ID, sortedIdx);

        RowId rowId0 = new RowId(PARTITION_ID);
        RowId rowId1 = new RowId(PARTITION_ID);

        mvPartitionStorage.runConsistently(locker -> {
            hashIndexStorage.setNextRowIdToBuild(rowId0);
            sortedIndexStorage.setNextRowIdToBuild(rowId1);

            return null;
        });

        assertThat(mvPartitionStorage.flush(), willCompleteSuccessfully());

        // Restart storages.
        tableStorage.stop();

        tableStorage = createMvTableStorage();

        tableStorage.start();

        getOrCreateMvPartition(PARTITION_ID);

        IndexStorage hashIndexStorageRestarted = tableStorage.getOrCreateIndex(PARTITION_ID, hashIdx);
        IndexStorage sortedIndexStorageRestarted = tableStorage.getOrCreateIndex(PARTITION_ID, sortedIdx);

        if (tableStorage.isVolatile()) {
            assertThat(hashIndexStorageRestarted.getNextRowIdToBuild(), equalTo(RowId.lowestRowId(PARTITION_ID)));
            assertThat(sortedIndexStorageRestarted.getNextRowIdToBuild(), equalTo(RowId.lowestRowId(PARTITION_ID)));
        } else {
            assertThat(hashIndexStorageRestarted.getNextRowIdToBuild(), equalTo(rowId0));
            assertThat(sortedIndexStorageRestarted.getNextRowIdToBuild(), equalTo(rowId1));
        }
    }

    private static void createTestTableAndIndexes(CatalogService catalogService) {
        int id = 0;

        int tableId = id++;
        int zoneId = id++;
        int sortedIndexId = id++;
        int hashIndexId = id++;

        CatalogTableDescriptor tableDescriptor = new CatalogTableDescriptor(
                tableId,
                hashIndexId,
                TABLE_NAME,
                zoneId,
                1,
                List.of(
                        CatalogUtils.fromParams(ColumnParams.builder().name("INTKEY").type(INT32).build()),
                        CatalogUtils.fromParams(ColumnParams.builder().name("STRKEY").length(100).type(STRING).build()),
                        CatalogUtils.fromParams(ColumnParams.builder().name("INTVAL").type(INT32).build()),
                        CatalogUtils.fromParams(ColumnParams.builder().name("STRVAL").length(100).type(STRING).build())
                ),
                List.of("INTKEY"),
                null,
                INITIAL_CAUSALITY_TOKEN
        );

        CatalogSortedIndexDescriptor sortedIndex = new CatalogSortedIndexDescriptor(
                sortedIndexId,
                SORTED_INDEX_NAME,
                tableId,
                false,
                List.of(new CatalogIndexColumnDescriptor("STRKEY", ASC_NULLS_LAST))
        );

        CatalogHashIndexDescriptor hashIndex = new CatalogHashIndexDescriptor(
                hashIndexId,
                HASH_INDEX_NAME,
                tableId,
                true,
                List.of("STRKEY")
        );

        when(catalogService.table(eq(TABLE_NAME), anyLong())).thenReturn(tableDescriptor);
        when(catalogService.index(eq(SORTED_INDEX_NAME), anyLong())).thenReturn(sortedIndex);
        when(catalogService.index(eq(HASH_INDEX_NAME), anyLong())).thenReturn(hashIndex);

        when(catalogService.table(eq(tableId), anyInt())).thenReturn(tableDescriptor);
        when(catalogService.index(eq(sortedIndexId), anyInt())).thenReturn(sortedIndex);
        when(catalogService.index(eq(hashIndexId), anyInt())).thenReturn(hashIndex);
    }

    private static <T> List<T> getAll(Cursor<T> cursor) {
        try (cursor) {
            return cursor.stream().collect(toList());
        }
    }

    private void checkStorageDestroyed(MvPartitionStorage storage) {
        int partId = PARTITION_ID;

        assertThrows(StorageClosedException.class, () -> storage.runConsistently(locker -> null));

        assertThrows(StorageClosedException.class, storage::flush);

        assertThrows(StorageClosedException.class, storage::lastAppliedIndex);
        assertThrows(StorageClosedException.class, storage::lastAppliedTerm);
        assertThrows(StorageClosedException.class, storage::committedGroupConfiguration);

        RowId rowId = new RowId(partId);

        HybridTimestamp timestamp = clock.now();

        assertThrows(StorageClosedException.class, () -> storage.read(new RowId(PARTITION_ID), timestamp));

        BinaryRow binaryRow = binaryRow(new TestKey(0, "0"), new TestValue(1, "1"));

        assertThrows(StorageClosedException.class, () -> storage.addWrite(rowId, binaryRow, UUID.randomUUID(), COMMIT_TABLE_ID, partId));
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
        return tableStorage.getTableDescriptor().getPartitions();
    }

    private void startRebalanceWithChecks(
            int partitionId,
            MvPartitionStorage mvPartitionStorage,
            HashIndexStorage hashIndexStorage,
            SortedIndexStorage sortedIndexStorage,
            List<IgniteTuple3<RowId, BinaryRow, HybridTimestamp>> rowsBeforeRebalanceStart
    ) {
        fillStorages(mvPartitionStorage, hashIndexStorage, sortedIndexStorage, rowsBeforeRebalanceStart);

        // Let's open the cursors before start rebalance.
        IgniteTuple3<RowId, BinaryRow, HybridTimestamp> rowForCursors = rowsBeforeRebalanceStart.get(0);

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

        checkCursorAfterStartRebalance(mvPartitionStorageScanCursor);

        checkCursorAfterStartRebalance(hashIndexStorageGetCursor);

        checkCursorAfterStartRebalance(sortedIndexStorageGetCursor);
        checkCursorAfterStartRebalance(sortedIndexStorageScanCursor);
    }

    private void checkMvPartitionStorageMethodsAfterStartRebalance(MvPartitionStorage storage) {
        checkLastApplied(storage, REBALANCE_IN_PROGRESS, REBALANCE_IN_PROGRESS);

        assertNull(storage.committedGroupConfiguration());

        assertDoesNotThrow(() -> storage.committedGroupConfiguration());

        storage.runConsistently(locker -> {
            assertThrows(StorageRebalanceException.class, () -> storage.lastApplied(100, 500));
            assertThrows(StorageRebalanceException.class, () -> storage.committedGroupConfiguration(new byte[0]));

            assertThrows(
                    StorageRebalanceException.class,
                    () -> storage.committedGroupConfiguration(new byte[0])
            );

            RowId rowId = new RowId(PARTITION_ID);

            locker.lock(rowId);

            assertThrows(StorageRebalanceException.class, () -> storage.read(rowId, clock.now()));
            assertThrows(StorageRebalanceException.class, () -> storage.abortWrite(rowId));
            assertThrows(StorageRebalanceException.class, () -> storage.scanVersions(rowId));
            assertThrows(StorageRebalanceException.class, () -> storage.scan(clock.now()));
            assertThrows(StorageRebalanceException.class, () -> storage.closestRowId(rowId));
            assertThrows(StorageRebalanceException.class, storage::rowsCount);

            return null;
        });
    }

    private static void checkHashIndexStorageMethodsAfterStartRebalance(HashIndexStorage storage) {
        assertDoesNotThrow(storage::indexDescriptor);

        assertThrows(StorageRebalanceException.class, () -> storage.get(mock(BinaryTuple.class)));
        assertThrows(StorageRebalanceException.class, () -> storage.remove(mock(IndexRow.class)));
    }

    private static void checkSortedIndexStorageMethodsAfterStartRebalance(SortedIndexStorage storage) {
        assertDoesNotThrow(storage::indexDescriptor);

        assertThrows(StorageRebalanceException.class, () -> storage.get(mock(BinaryTuple.class)));
        assertThrows(StorageRebalanceException.class, () -> storage.remove(mock(IndexRow.class)));
        assertThrows(StorageRebalanceException.class, () -> storage.scan(null, null, 0));
    }

    private static void checkCursorAfterStartRebalance(Cursor<?> cursor) {
        assertDoesNotThrow(cursor::close);

        assertThrows(StorageRebalanceException.class, cursor::hasNext);
        assertThrows(StorageRebalanceException.class, cursor::next);

        if (cursor instanceof PeekCursor) {
            assertThrows(StorageRebalanceException.class, ((PeekCursor<?>) cursor)::peek);
        }
    }

    private void fillStorages(
            MvPartitionStorage mvPartitionStorage,
            HashIndexStorage hashIndexStorage,
            SortedIndexStorage sortedIndexStorage,
            List<IgniteTuple3<RowId, BinaryRow, HybridTimestamp>> rows
    ) {
        assertThat(rows, hasSize(greaterThanOrEqualTo(2)));

        for (int i = 0; i < rows.size(); i++) {
            int finalI = i;

            IgniteTuple3<RowId, BinaryRow, HybridTimestamp> row = rows.get(i);

            RowId rowId = row.get1();
            BinaryRow binaryRow = row.get2();
            HybridTimestamp timestamp = row.get3();

            IndexRow hashIndexRow = indexRow(hashIndexStorage.indexDescriptor(), binaryRow, rowId);
            IndexRow sortedIndexRow = indexRow(sortedIndexStorage.indexDescriptor(), binaryRow, rowId);

            mvPartitionStorage.runConsistently(locker -> {
                locker.lock(rowId);

                if ((finalI % 2) == 0) {
                    mvPartitionStorage.addWrite(rowId, binaryRow, UUID.randomUUID(), COMMIT_TABLE_ID, rowId.partitionId());

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
            List<ReadResult> allVersions = mvPartitionStorage.runConsistently(locker -> {
                locker.lock(row.get1());

                return getAll(mvPartitionStorage.scanVersions(row.get1()));
            });
            assertThat(allVersions, is(empty()));

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
            List<BinaryRow> allVersions = mvPartitionStorage.runConsistently(locker -> {
                locker.lock(row.get1());

                return toListOfBinaryRows(mvPartitionStorage.scanVersions(row.get1()));
            });

            assertThat(allVersions, contains(equalToRow(row.get2())));

            IndexRow hashIndexRow = indexRow(hashIndexStorage.indexDescriptor(), row.get2(), row.get1());
            IndexRow sortedIndexRow = indexRow(sortedIndexStorage.indexDescriptor(), row.get2(), row.get1());

            assertThat(getAll(hashIndexStorage.get(hashIndexRow.indexColumns())), contains(row.get1()));
            assertThat(getAll(sortedIndexStorage.get(sortedIndexRow.indexColumns())), contains(row.get1()));
        }
    }

    private static void checkLastApplied(
            MvPartitionStorage storage,
            long expLastAppliedIndex,
            long expLastAppliedTerm
    ) {
        assertEquals(expLastAppliedIndex, storage.lastAppliedIndex());
        assertEquals(expLastAppliedTerm, storage.lastAppliedTerm());
    }

    private static List<BinaryRow> toListOfBinaryRows(Cursor<ReadResult> cursor) {
        try (cursor) {
            return cursor.stream().map(ReadResult::binaryRow).collect(toList());
        }
    }

    private static byte[] createRandomRaftGroupConfiguration() {
        Random random = new Random(System.currentTimeMillis());

        byte[] bytes = new byte[100];

        random.nextBytes(bytes);

        return bytes;
    }

    private static void checkRaftGroupConfigs(byte @Nullable [] exp, byte @Nullable [] act) {
        assertNotNull(exp);
        assertNotNull(act);

        assertArrayEquals(exp, act);
    }

    /**
     * Retrieves or creates a multi-versioned partition storage.
     */
    protected MvPartitionStorage getOrCreateMvPartition(int partitionId) {
        return getOrCreateMvPartition(tableStorage, partitionId);
    }
}
