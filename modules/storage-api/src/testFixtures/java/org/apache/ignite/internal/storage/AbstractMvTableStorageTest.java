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
import static org.apache.ignite.internal.catalog.CatalogService.DEFAULT_STORAGE_PROFILE;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.pkIndexName;
import static org.apache.ignite.internal.catalog.descriptors.CatalogColumnCollation.ASC_NULLS_LAST;
import static org.apache.ignite.internal.catalog.descriptors.CatalogIndexStatus.AVAILABLE;
import static org.apache.ignite.internal.schema.BinaryRowMatcher.equalToRow;
import static org.apache.ignite.internal.storage.MvPartitionStorage.REBALANCE_IN_PROGRESS;
import static org.apache.ignite.internal.storage.index.SortedIndexStorage.GREATER;
import static org.apache.ignite.internal.storage.util.StorageUtils.initialRowIdToBuild;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureExceptionMatcher.willThrowFast;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.util.ArrayUtils.BYTE_EMPTY_ARRAY;
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
import static org.junit.jupiter.api.Assumptions.assumeFalse;
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
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.BinaryTuple;
import org.apache.ignite.internal.schema.BinaryTupleSchema;
import org.apache.ignite.internal.schema.BinaryTupleSchema.Element;
import org.apache.ignite.internal.storage.engine.MvTableStorage;
import org.apache.ignite.internal.storage.index.HashIndexStorage;
import org.apache.ignite.internal.storage.index.IndexRow;
import org.apache.ignite.internal.storage.index.IndexRowImpl;
import org.apache.ignite.internal.storage.index.IndexStorage;
import org.apache.ignite.internal.storage.index.PeekCursor;
import org.apache.ignite.internal.storage.index.SortedIndexStorage;
import org.apache.ignite.internal.storage.index.StorageHashIndexDescriptor;
import org.apache.ignite.internal.storage.index.StorageIndexDescriptor;
import org.apache.ignite.internal.storage.index.StorageIndexDescriptorSupplier;
import org.apache.ignite.internal.storage.index.StorageSortedIndexDescriptor;
import org.apache.ignite.internal.storage.index.impl.BinaryTupleRowSerializer;
import org.apache.ignite.internal.type.NativeTypes;
import org.apache.ignite.internal.util.Cursor;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Abstract class that contains tests for {@link MvTableStorage} implementations.
 */
@SuppressWarnings("JUnitTestMethodInProductSource")
public abstract class AbstractMvTableStorageTest extends BaseMvStoragesTest {
    private static final String TABLE_NAME = "FOO";

    private static final String PK_INDEX_NAME = pkIndexName(TABLE_NAME);

    private static final String SORTED_INDEX_NAME = "SORTED_IDX";

    private static final String HASH_INDEX_NAME = "HASH_IDX";

    protected static final int PARTITION_ID = 0;

    private static final RowId INITIAL_ROW_ID_TO_BUILD = initialRowIdToBuild(PARTITION_ID);

    /** Partition id for 0 storage. */
    protected static final int PARTITION_ID_0 = 10;

    /** Partition id for 1 storage. */
    protected static final int PARTITION_ID_1 = 9;

    protected static final int COMMIT_TABLE_ID = 999;

    protected MvTableStorage tableStorage;

    protected StorageSortedIndexDescriptor sortedIdx;

    protected StorageHashIndexDescriptor hashIdx;

    protected StorageIndexDescriptor pkIdx;

    private final CatalogService catalogService = mock(CatalogService.class);

    protected final StorageIndexDescriptorSupplier indexDescriptorSupplier = new StorageIndexDescriptorSupplier() {
        @Override
        public @Nullable StorageIndexDescriptor get(int indexId) {
            int catalogVersion = catalogService.latestCatalogVersion();

            CatalogIndexDescriptor indexDescriptor = catalogService.index(indexId, catalogVersion);

            if (indexDescriptor == null) {
                return null;
            }

            CatalogTableDescriptor tableDescriptor = catalogService.table(indexDescriptor.tableId(), catalogVersion);

            assertThat(tableDescriptor, is(notNullValue()));

            return StorageIndexDescriptor.create(tableDescriptor, indexDescriptor);
        }
    };

    private class TestRow {
        final RowId rowId;

        final BinaryRow row;

        final HybridTimestamp timestamp;

        TestRow(RowId rowId, BinaryRow row) {
            this.rowId = rowId;
            this.row = row;
            this.timestamp = clock.now();
        }
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
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testDestroyIndex(boolean waitForDestroyFuture) throws Exception {
        MvPartitionStorage partitionStorage = getOrCreateMvPartition(PARTITION_ID);

        SortedIndexStorage sortedIndexStorage = tableStorage.getOrCreateSortedIndex(PARTITION_ID, sortedIdx);
        assertThat(sortedIndexStorage, is(notNullValue()));

        HashIndexStorage hashIndexStorage = tableStorage.getOrCreateHashIndex(PARTITION_ID, hashIdx);
        assertThat(hashIndexStorage, is(notNullValue()));

        CompletableFuture<Void> destroySortedIndexFuture = tableStorage.destroyIndex(sortedIdx.id());
        CompletableFuture<Void> destroyHashIndexFuture = tableStorage.destroyIndex(hashIdx.id());

        Runnable waitForDestroy = () -> {
            assertThat(partitionStorage.flush(), willCompleteSuccessfully());
            assertThat(destroySortedIndexFuture, willCompleteSuccessfully());
            assertThat(destroyHashIndexFuture, willCompleteSuccessfully());
        };

        if (waitForDestroyFuture) {
            waitForDestroy.run();
        }

        checkStorageDestroyed(sortedIndexStorage);
        checkStorageDestroyed(hashIndexStorage);

        // Make sure the destroy finishes before we recreate the storage.
        if (!waitForDestroyFuture) {
            waitForDestroy.run();
        }

        tableStorage.close();

        tableStorage = createMvTableStorage();

        getOrCreateMvPartition(PARTITION_ID);

        assertThat(tableStorage.getIndex(PARTITION_ID, sortedIdx.id()), is(nullValue()));
        assertThat(tableStorage.getIndex(PARTITION_ID, hashIdx.id()), is(nullValue()));
    }

    /**
     * Tests that removing one Sorted Index does not affect the data in the other.
     */
    @Test
    public void testDestroySortedIndexIndependence() {
        CatalogTableDescriptor catalogTableDescriptor = catalogService.table(TABLE_NAME, clock.nowLong());

        var catalogSortedIndex1 = new CatalogSortedIndexDescriptor(
                200,
                "TEST_INDEX_1",
                catalogTableDescriptor.id(),
                false,
                AVAILABLE,
                catalogService.latestCatalogVersion(),
                List.of(new CatalogIndexColumnDescriptor("STRKEY", ASC_NULLS_LAST))
        );

        var catalogSortedIndex2 = new CatalogSortedIndexDescriptor(
                201,
                "TEST_INDEX_2",
                catalogTableDescriptor.id(),
                false,
                AVAILABLE,
                catalogService.latestCatalogVersion(),
                List.of(new CatalogIndexColumnDescriptor("STRKEY", ASC_NULLS_LAST))
        );

        var sortedIndexDescriptor1 = new StorageSortedIndexDescriptor(catalogTableDescriptor, catalogSortedIndex1);
        var sortedIndexDescriptor2 = new StorageSortedIndexDescriptor(catalogTableDescriptor, catalogSortedIndex2);

        MvPartitionStorage partitionStorage = getOrCreateMvPartition(PARTITION_ID);

        SortedIndexStorage sortedIndexStorage1 = tableStorage.getOrCreateSortedIndex(PARTITION_ID, sortedIndexDescriptor1);
        SortedIndexStorage sortedIndexStorage2 = tableStorage.getOrCreateSortedIndex(PARTITION_ID, sortedIndexDescriptor2);

        List<TestRow> rows = List.of(
                new TestRow(new RowId(PARTITION_ID), binaryRow(new TestKey(0, "0"), new TestValue(0, "0"))),
                new TestRow(new RowId(PARTITION_ID), binaryRow(new TestKey(1, "1"), new TestValue(1, "1")))
        );

        fillStorages(partitionStorage, null, sortedIndexStorage1, rows);
        fillStorages(partitionStorage, null, sortedIndexStorage2, rows);

        checkForPresenceRows(null, null, sortedIndexStorage1, rows);
        checkForPresenceRows(null, null, sortedIndexStorage2, rows);

        assertThat(tableStorage.destroyIndex(sortedIndexDescriptor1.id()), willCompleteSuccessfully());

        assertThat(tableStorage.getIndex(PARTITION_ID, sortedIndexDescriptor1.id()), is(nullValue()));

        checkForPresenceRows(null, null, sortedIndexStorage2, rows);
    }

    /**
     * Tests that removing one Hash Index does not affect the data in the other.
     */
    @Test
    public void testDestroyHashIndexIndependence() {
        CatalogTableDescriptor catalogTableDescriptor = catalogService.table(TABLE_NAME, clock.nowLong());

        var catalogHashIndex1 = new CatalogHashIndexDescriptor(
                200,
                "TEST_INDEX_1",
                catalogTableDescriptor.id(),
                true,
                AVAILABLE,
                catalogService.latestCatalogVersion(),
                List.of("STRKEY")
        );

        var catalogHashIndex2 = new CatalogHashIndexDescriptor(
                201,
                "TEST_INDEX_2",
                catalogTableDescriptor.id(),
                true,
                AVAILABLE,
                catalogService.latestCatalogVersion(),
                List.of("STRKEY")
        );

        var hashIndexDescriptor1 = new StorageHashIndexDescriptor(catalogTableDescriptor, catalogHashIndex1);
        var hashIndexDescriptor2 = new StorageHashIndexDescriptor(catalogTableDescriptor, catalogHashIndex2);

        MvPartitionStorage partitionStorage = getOrCreateMvPartition(PARTITION_ID);

        HashIndexStorage hashIndexStorage1 = tableStorage.getOrCreateHashIndex(PARTITION_ID, hashIndexDescriptor1);
        HashIndexStorage hashIndexStorage2 = tableStorage.getOrCreateHashIndex(PARTITION_ID, hashIndexDescriptor2);

        List<TestRow> rows = List.of(
                new TestRow(new RowId(PARTITION_ID), binaryRow(new TestKey(0, "0"), new TestValue(0, "0"))),
                new TestRow(new RowId(PARTITION_ID), binaryRow(new TestKey(1, "1"), new TestValue(1, "1")))
        );

        fillStorages(partitionStorage, hashIndexStorage1, null, rows);
        fillStorages(partitionStorage, hashIndexStorage2, null, rows);

        checkForPresenceRows(null, hashIndexStorage1, null, rows);
        checkForPresenceRows(null, hashIndexStorage2, null, rows);

        assertThat(tableStorage.destroyIndex(hashIndexDescriptor1.id()), willCompleteSuccessfully());

        assertThat(tableStorage.getIndex(PARTITION_ID, hashIndexDescriptor1.id()), is(nullValue()));

        checkForPresenceRows(null, hashIndexStorage2, null, rows);
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
    public void testPutIndexAndVersionRowToMemory() {
        MvPartitionStorage partitionStorage = getOrCreateMvPartition(PARTITION_ID);
        HashIndexStorage indexStorage = tableStorage.getOrCreateHashIndex(PARTITION_ID, hashIdx);

        // Should be large enough to exceed inline size.
        byte[] bytes = new byte[100];
        new Random().nextBytes(bytes);
        String str = new String(bytes);

        BinaryRow binaryRow = binaryRow(new TestKey(10, "foo"), new TestValue(20, str));

        var serializer = new BinaryTupleRowSerializer(indexStorage.indexDescriptor());

        IndexRow indexRow = serializer.serializeRow(new Object[]{str}, new RowId(PARTITION_ID));
        partitionStorage.runConsistently(locker -> {
            indexStorage.put(indexRow);

            return null;
        });

        partitionStorage.runConsistently(locker -> {
            RowId rowId = new RowId(PARTITION_ID);
            locker.tryLock(rowId);

            partitionStorage.addWrite(rowId, binaryRow, UUID.randomUUID(), COMMIT_TABLE_ID, PARTITION_ID);

            return null;
        });
    }

    private static void checkStorageDestroyed(IndexStorage storage) {
        assertThrows(StorageDestroyedException.class, () -> storage.get(mock(BinaryTuple.class)));

        assertThrows(StorageDestroyedException.class, () -> storage.put(mock(IndexRow.class)));

        assertThrows(StorageDestroyedException.class, () -> storage.remove(mock(IndexRow.class)));
    }

    @SuppressWarnings("resource")
    private static void checkStorageDestroyed(SortedIndexStorage storage) {
        checkStorageDestroyed((IndexStorage) storage);

        assertThrows(StorageDestroyedException.class, () -> storage.scan(null, null, GREATER));
    }

    @SuppressWarnings({"resource", "deprecation"})
    private void checkStorageDestroyed(MvPartitionStorage storage) {
        int partId = PARTITION_ID;

        assertThrows(StorageDestroyedException.class, () -> storage.runConsistently(locker -> null));

        assertThrows(StorageDestroyedException.class, storage::flush);

        assertThrows(StorageDestroyedException.class, storage::lastAppliedIndex);
        assertThrows(StorageDestroyedException.class, storage::lastAppliedTerm);
        assertThrows(StorageDestroyedException.class, storage::committedGroupConfiguration);

        RowId rowId = new RowId(partId);

        HybridTimestamp timestamp = clock.now();

        assertThrows(StorageDestroyedException.class, () -> storage.read(new RowId(PARTITION_ID), timestamp));

        BinaryRow binaryRow = binaryRow(new TestKey(0, "0"), new TestValue(1, "1"));

        assertThrows(StorageDestroyedException.class, () -> storage.addWrite(rowId, binaryRow, UUID.randomUUID(), COMMIT_TABLE_ID, partId));
        assertThrows(StorageDestroyedException.class, () -> storage.commitWrite(rowId, timestamp));
        assertThrows(StorageDestroyedException.class, () -> storage.abortWrite(rowId));
        assertThrows(StorageDestroyedException.class, () -> storage.addWriteCommitted(rowId, binaryRow, timestamp));

        assertThrows(StorageDestroyedException.class, () -> storage.scan(timestamp));
        assertThrows(StorageDestroyedException.class, () -> storage.scanVersions(rowId));
        assertThrows(StorageDestroyedException.class, () -> storage.scanVersions(rowId));

        assertThrows(StorageDestroyedException.class, () -> storage.closestRowId(rowId));

        assertThrows(StorageDestroyedException.class, storage::rowsCount);
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

        // Error because rebalance has not yet started for the partition.
        assertThrows(
                StorageRebalanceException.class,
                () -> tableStorage.finishRebalancePartition(PARTITION_ID, 100, 500, BYTE_EMPTY_ARRAY)
        );

        List<TestRow> rowsBeforeRebalanceStart = List.of(
                new TestRow(new RowId(PARTITION_ID), binaryRow(new TestKey(0, "0"), new TestValue(0, "0"))),
                new TestRow(new RowId(PARTITION_ID), binaryRow(new TestKey(1, "1"), new TestValue(1, "1")))
        );

        startRebalanceWithChecks(
                PARTITION_ID,
                mvPartitionStorage,
                hashIndexStorage,
                sortedIndexStorage,
                rowsBeforeRebalanceStart
        );

        // Let's fill the storages with fresh data on rebalance.
        List<TestRow> rowsOnRebalance = List.of(
                new TestRow(new RowId(PARTITION_ID), binaryRow(new TestKey(2, "2"), new TestValue(2, "2"))),
                new TestRow(new RowId(PARTITION_ID), binaryRow(new TestKey(3, "3"), new TestValue(3, "3")))
        );

        fillStorages(mvPartitionStorage, hashIndexStorage, sortedIndexStorage, rowsOnRebalance);

        checkLastApplied(mvPartitionStorage, REBALANCE_IN_PROGRESS, REBALANCE_IN_PROGRESS);
        assertNull(mvPartitionStorage.committedGroupConfiguration());

        // Let's finish rebalancing.

        // Partition is out of configuration range.
        assertThrows(
                IllegalArgumentException.class,
                () -> tableStorage.finishRebalancePartition(getPartitionIdOutOfRange(), 100, 500, BYTE_EMPTY_ARRAY)
        );

        // Partition does not exist.
        assertThrows(
                StorageRebalanceException.class,
                () -> tableStorage.finishRebalancePartition(1, 100, 500, BYTE_EMPTY_ARRAY)
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

        List<TestRow> rowsBeforeRebalanceStart = List.of(
                new TestRow(new RowId(PARTITION_ID), binaryRow(new TestKey(0, "0"), new TestValue(0, "0"))),
                new TestRow(new RowId(PARTITION_ID), binaryRow(new TestKey(1, "1"), new TestValue(1, "1")))
        );

        startRebalanceWithChecks(
                PARTITION_ID,
                mvPartitionStorage,
                hashIndexStorage,
                sortedIndexStorage,
                rowsBeforeRebalanceStart
        );

        // Let's fill the storages with fresh data on rebalance.
        List<TestRow> rowsOnRebalance = List.of(
                new TestRow(new RowId(PARTITION_ID), binaryRow(new TestKey(2, "2"), new TestValue(2, "2"))),
                new TestRow(new RowId(PARTITION_ID), binaryRow(new TestKey(3, "3"), new TestValue(3, "3")))
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

    @SuppressWarnings("resource")
    private static void checkSortedIndexStorageMethodsAfterStartRebalance(SortedIndexStorage storage) {
        assertDoesNotThrow(storage::indexDescriptor);

        assertThrows(StorageRebalanceException.class, () -> storage.get(mock(BinaryTuple.class)));
        assertThrows(StorageRebalanceException.class, () -> storage.remove(mock(IndexRow.class)));
        assertThrows(StorageRebalanceException.class, () -> storage.scan(null, null, GREATER));
    }

    @Test
    public void testRestartStoragesInTheMiddleOfRebalance() throws Exception {
        MvPartitionStorage mvPartitionStorage = getOrCreateMvPartition(PARTITION_ID);
        HashIndexStorage hashIndexStorage = tableStorage.getOrCreateHashIndex(PARTITION_ID, hashIdx);
        SortedIndexStorage sortedIndexStorage = tableStorage.getOrCreateSortedIndex(PARTITION_ID, sortedIdx);

        List<TestRow> rows = List.of(
                new TestRow(new RowId(PARTITION_ID), binaryRow(new TestKey(0, "0"), new TestValue(0, "0"))),
                new TestRow(new RowId(PARTITION_ID), binaryRow(new TestKey(1, "1"), new TestValue(1, "1")))
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
        tableStorage.close();

        tableStorage = createMvTableStorage();

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
        List<TestRow> rows = List.of(
                new TestRow(new RowId(PARTITION_ID), binaryRow(new TestKey(0, "0"), new TestValue(0, "0"))),
                new TestRow(new RowId(PARTITION_ID), binaryRow(new TestKey(1, "1"), new TestValue(1, "1")))
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
    void testNextRowIdToBuildAfterRestart() throws Exception {
        MvPartitionStorage mvPartitionStorage = getOrCreateMvPartition(PARTITION_ID);

        IndexStorage hashIndexStorage = tableStorage.getOrCreateIndex(PARTITION_ID, hashIdx);
        IndexStorage sortedIndexStorage = tableStorage.getOrCreateIndex(PARTITION_ID, sortedIdx);
        IndexStorage pkIndexStorage = tableStorage.getOrCreateIndex(PARTITION_ID, pkIdx);

        RowId rowId0 = new RowId(PARTITION_ID);
        RowId rowId1 = new RowId(PARTITION_ID);
        RowId rowId2 = new RowId(PARTITION_ID);

        mvPartitionStorage.runConsistently(locker -> {
            hashIndexStorage.setNextRowIdToBuild(rowId0);
            sortedIndexStorage.setNextRowIdToBuild(rowId1);
            pkIndexStorage.setNextRowIdToBuild(rowId2);

            return null;
        });

        assertThat(mvPartitionStorage.flush(), willCompleteSuccessfully());

        // Restart storages.
        tableStorage.close();

        tableStorage = createMvTableStorage();

        getOrCreateMvPartition(PARTITION_ID);

        IndexStorage hashIndexStorageRestarted = tableStorage.getOrCreateIndex(PARTITION_ID, hashIdx);
        IndexStorage sortedIndexStorageRestarted = tableStorage.getOrCreateIndex(PARTITION_ID, sortedIdx);
        IndexStorage pkIndexStorageRestarted = tableStorage.getOrCreateIndex(PARTITION_ID, pkIdx);

        if (tableStorage.isVolatile()) {
            assertThat(hashIndexStorageRestarted.getNextRowIdToBuild(), equalTo(INITIAL_ROW_ID_TO_BUILD));
            assertThat(sortedIndexStorageRestarted.getNextRowIdToBuild(), equalTo(INITIAL_ROW_ID_TO_BUILD));
            assertThat(pkIndexStorageRestarted.getNextRowIdToBuild(), nullValue());
        } else {
            assertThat(hashIndexStorageRestarted.getNextRowIdToBuild(), equalTo(rowId0));
            assertThat(sortedIndexStorageRestarted.getNextRowIdToBuild(), equalTo(rowId1));
            assertThat(pkIndexStorageRestarted.getNextRowIdToBuild(), equalTo(rowId2));
        }
    }

    @Test
    void testNextRowIdToBuildAfterRebalance() throws Exception {
        testNextRowIdToBuildAfterOperation(() -> {
            assertThat(tableStorage.startRebalancePartition(PARTITION_ID), willCompleteSuccessfully());
            assertThat(tableStorage.finishRebalancePartition(PARTITION_ID, 100, 100, BYTE_EMPTY_ARRAY), willCompleteSuccessfully());
        });
    }

    @Test
    void testNextRowIdToBuildAfterClearPartition() throws Exception {
        testNextRowIdToBuildAfterOperation(() -> assertThat(tableStorage.clearPartition(PARTITION_ID), willCompleteSuccessfully()));
    }

    @Test
    void testNextRowIdToBuildAfterDestroyPartition() throws Exception {
        testNextRowIdToBuildAfterOperation(() -> {
            assertThat(tableStorage.destroyPartition(PARTITION_ID), willCompleteSuccessfully());

            getOrCreateMvPartition(PARTITION_ID);
        });
    }

    @Test
    void testNextRowIdToBuildAfterDestroyTable() throws Exception {
        testNextRowIdToBuildAfterOperation(() -> {
            assertThat(tableStorage.destroy(), willCompleteSuccessfully());

            tableStorage.close();

            tableStorage = createMvTableStorage();

            getOrCreateMvPartition(PARTITION_ID);
        });
    }

    @Test
    public void testIndexDestructionOnRecovery() throws Exception {
        assumeFalse(tableStorage.isVolatile(), "Volatile storages do not support index recovery");

        MvPartitionStorage mvPartitionStorage = getOrCreateMvPartition(PARTITION_ID);
        HashIndexStorage hashIndexStorage = tableStorage.getOrCreateHashIndex(PARTITION_ID, hashIdx);
        SortedIndexStorage sortedIndexStorage = tableStorage.getOrCreateSortedIndex(PARTITION_ID, sortedIdx);

        List<TestRow> rows = List.of(
                new TestRow(new RowId(PARTITION_ID), binaryRow(new TestKey(0, "0"), new TestValue(0, "0"))),
                new TestRow(new RowId(PARTITION_ID), binaryRow(new TestKey(1, "1"), new TestValue(1, "1")))
        );

        fillStorages(mvPartitionStorage, hashIndexStorage, sortedIndexStorage, rows);

        assertThat(mvPartitionStorage.flush(), willCompleteSuccessfully());

        // Restart storages.
        tableStorage.close();

        // Emulate a situation when indexes have been removed from the catalog. We then expect them to be removed upon startup.
        when(catalogService.index(eq(hashIdx.id()), anyInt())).thenReturn(null);
        when(catalogService.index(eq(sortedIdx.id()), anyInt())).thenReturn(null);

        tableStorage = createMvTableStorage();

        mvPartitionStorage = getOrCreateMvPartition(PARTITION_ID);
        hashIndexStorage = (HashIndexStorage) tableStorage.getIndex(PARTITION_ID, hashIdx.id());
        sortedIndexStorage = (SortedIndexStorage) tableStorage.getIndex(PARTITION_ID, sortedIdx.id());

        // Data should remain in the partition storages, but the indexes must be cleaned up.
        checkForPresenceRows(mvPartitionStorage, null, null, rows);
        assertThat(hashIndexStorage, is(nullValue()));
        assertThat(sortedIndexStorage, is(nullValue()));
    }

    private static void createTestTableAndIndexes(CatalogService catalogService) {
        int id = 0;

        int schemaId = id++;
        int tableId = id++;
        int zoneId = id++;
        int sortedIndexId = id++;
        int hashIndexId = id++;
        int pkIndexId = id++;

        String pkColumnName = "INTKEY";

        CatalogTableDescriptor tableDescriptor = new CatalogTableDescriptor(
                tableId,
                schemaId,
                pkIndexId,
                TABLE_NAME,
                zoneId,
                List.of(
                        CatalogUtils.fromParams(ColumnParams.builder().name(pkColumnName).type(INT32).build()),
                        CatalogUtils.fromParams(ColumnParams.builder().name("STRKEY").length(100).type(STRING).build()),
                        CatalogUtils.fromParams(ColumnParams.builder().name("INTVAL").type(INT32).build()),
                        CatalogUtils.fromParams(ColumnParams.builder().name("STRVAL").length(100).type(STRING).build())
                ),
                List.of(pkColumnName),
                null,
                DEFAULT_STORAGE_PROFILE
        );

        CatalogSortedIndexDescriptor sortedIndex = new CatalogSortedIndexDescriptor(
                sortedIndexId,
                SORTED_INDEX_NAME,
                tableId,
                false,
                AVAILABLE,
                catalogService.latestCatalogVersion(),
                List.of(new CatalogIndexColumnDescriptor("STRKEY", ASC_NULLS_LAST))
        );

        CatalogHashIndexDescriptor hashIndex = new CatalogHashIndexDescriptor(
                hashIndexId,
                HASH_INDEX_NAME,
                tableId,
                true,
                AVAILABLE,
                catalogService.latestCatalogVersion(),
                List.of("STRKEY")
        );

        CatalogIndexDescriptor pkIndex = new CatalogHashIndexDescriptor(
                pkIndexId,
                PK_INDEX_NAME,
                tableId,
                true,
                AVAILABLE,
                catalogService.latestCatalogVersion(),
                List.of(pkColumnName)
        );

        when(catalogService.table(eq(TABLE_NAME), anyLong())).thenReturn(tableDescriptor);
        when(catalogService.aliveIndex(eq(SORTED_INDEX_NAME), anyLong())).thenReturn(sortedIndex);
        when(catalogService.aliveIndex(eq(HASH_INDEX_NAME), anyLong())).thenReturn(hashIndex);
        when(catalogService.aliveIndex(eq(PK_INDEX_NAME), anyLong())).thenReturn(pkIndex);

        when(catalogService.table(eq(tableId), anyInt())).thenReturn(tableDescriptor);
        when(catalogService.index(eq(sortedIndexId), anyInt())).thenReturn(sortedIndex);
        when(catalogService.index(eq(hashIndexId), anyInt())).thenReturn(hashIndex);
        when(catalogService.index(eq(pkIndexId), anyInt())).thenReturn(pkIndex);
    }

    private static <T> List<T> getAll(Cursor<T> cursor) {
        try (cursor) {
            return cursor.stream().collect(toList());
        }
    }

    private static void checkCursorAfterStartRebalance(Cursor<?> cursor) {
        assertDoesNotThrow(cursor::close);

        assertThrows(StorageRebalanceException.class, cursor::hasNext);
        assertThrows(StorageRebalanceException.class, cursor::next);

        if (cursor instanceof PeekCursor) {
            assertThrows(StorageRebalanceException.class, ((PeekCursor<?>) cursor)::peek);
        }
    }

    private static void fillStorages(
            MvPartitionStorage mvPartitionStorage,
            @Nullable HashIndexStorage hashIndexStorage,
            @Nullable SortedIndexStorage sortedIndexStorage,
            List<TestRow> rows
    ) {
        assertThat(rows, hasSize(greaterThanOrEqualTo(2)));

        for (int i = 0; i < rows.size(); i++) {
            int finalI = i;

            TestRow row = rows.get(i);

            RowId rowId = row.rowId;
            BinaryRow binaryRow = row.row;
            HybridTimestamp timestamp = row.timestamp;

            assertNotNull(rowId);
            assertNotNull(binaryRow);
            assertNotNull(timestamp);

            mvPartitionStorage.runConsistently(locker -> {
                locker.lock(rowId);

                if ((finalI % 2) == 0) {
                    mvPartitionStorage.addWrite(rowId, binaryRow, UUID.randomUUID(), COMMIT_TABLE_ID, rowId.partitionId());

                    mvPartitionStorage.commitWrite(rowId, timestamp);
                } else {
                    mvPartitionStorage.addWriteCommitted(rowId, binaryRow, timestamp);
                }

                if (hashIndexStorage != null) {
                    IndexRow hashIndexRow = indexRow(hashIndexStorage.indexDescriptor(), binaryRow, rowId);

                    hashIndexStorage.put(hashIndexRow);
                }

                if (sortedIndexStorage != null) {
                    IndexRow sortedIndexRow = indexRow(sortedIndexStorage.indexDescriptor(), binaryRow, rowId);

                    sortedIndexStorage.put(sortedIndexRow);
                }

                return null;
            });
        }
    }

    private static void checkForMissingRows(
            @Nullable MvPartitionStorage mvPartitionStorage,
            @Nullable HashIndexStorage hashIndexStorage,
            @Nullable SortedIndexStorage sortedIndexStorage,
            List<TestRow> rows
    ) {
        for (TestRow row : rows) {
            if (mvPartitionStorage != null) {
                List<ReadResult> allVersions = mvPartitionStorage.runConsistently(locker -> {
                    locker.lock(row.rowId);

                    return getAll(mvPartitionStorage.scanVersions(row.rowId));
                });

                assertThat(allVersions, is(empty()));
            }

            if (hashIndexStorage != null) {
                IndexRow hashIndexRow = indexRow(hashIndexStorage.indexDescriptor(), row.row, row.rowId);

                assertThat(getAll(hashIndexStorage.get(hashIndexRow.indexColumns())), is(empty()));
            }

            if (sortedIndexStorage != null) {
                IndexRow sortedIndexRow = indexRow(sortedIndexStorage.indexDescriptor(), row.row, row.rowId);

                assertThat(getAll(sortedIndexStorage.get(sortedIndexRow.indexColumns())), is(empty()));
            }
        }
    }

    private int getPartitionIdOutOfRange() {
        return tableStorage.getTableDescriptor().getPartitions();
    }

    private static void checkForPresenceRows(
            @Nullable MvPartitionStorage mvPartitionStorage,
            @Nullable HashIndexStorage hashIndexStorage,
            @Nullable SortedIndexStorage sortedIndexStorage,
            List<TestRow> rows
    ) {
        for (TestRow row : rows) {
            if (mvPartitionStorage != null) {
                List<BinaryRow> allVersions = mvPartitionStorage.runConsistently(locker -> {
                    locker.lock(row.rowId);

                    return toListOfBinaryRows(mvPartitionStorage.scanVersions(row.rowId));
                });

                assertThat(allVersions, contains(equalToRow(row.row)));
            }

            if (hashIndexStorage != null) {
                IndexRow hashIndexRow = indexRow(hashIndexStorage.indexDescriptor(), row.row, row.rowId);

                assertThat(getAll(hashIndexStorage.get(hashIndexRow.indexColumns())), contains(row.rowId));
            }

            if (sortedIndexStorage != null) {
                IndexRow sortedIndexRow = indexRow(sortedIndexStorage.indexDescriptor(), row.row, row.rowId);

                assertThat(getAll(sortedIndexStorage.get(sortedIndexRow.indexColumns())), contains(row.rowId));
            }
        }
    }

    /**
     * Initializes the internal structures needed for tests.
     *
     * <p>This method *MUST* always be called in either subclass' constructor or setUp method.
     */
    protected final void initialize() {
        createTestTableAndIndexes(catalogService);

        this.tableStorage = createMvTableStorage();

        CatalogTableDescriptor catalogTableDescriptor = catalogService.table(TABLE_NAME, clock.nowLong());
        assertNotNull(catalogTableDescriptor);

        CatalogIndexDescriptor catalogSortedIndexDescriptor = catalogService.aliveIndex(SORTED_INDEX_NAME, clock.nowLong());
        CatalogIndexDescriptor catalogHashIndexDescriptor = catalogService.aliveIndex(HASH_INDEX_NAME, clock.nowLong());
        CatalogIndexDescriptor catalogPkIndexDescriptor = catalogService.aliveIndex(PK_INDEX_NAME, clock.nowLong());

        assertNotNull(catalogSortedIndexDescriptor);
        assertNotNull(catalogHashIndexDescriptor);
        assertNotNull(catalogPkIndexDescriptor);

        sortedIdx = new StorageSortedIndexDescriptor(catalogTableDescriptor, (CatalogSortedIndexDescriptor) catalogSortedIndexDescriptor);
        hashIdx = new StorageHashIndexDescriptor(catalogTableDescriptor, (CatalogHashIndexDescriptor) catalogHashIndexDescriptor);
        pkIdx = StorageIndexDescriptor.create(catalogTableDescriptor, catalogPkIndexDescriptor);
    }

    private static void checkHashIndexStorageMethodsAfterStartRebalance(HashIndexStorage storage) {
        assertDoesNotThrow(storage::indexDescriptor);

        assertThrows(StorageRebalanceException.class, () -> storage.get(mock(BinaryTuple.class)));
        assertThrows(StorageRebalanceException.class, () -> storage.remove(mock(IndexRow.class)));
    }

    @SuppressWarnings("resource")
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testDestroyPartition(boolean waitForDestroyFuture) {
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

        PartitionTimestampCursor scanAtTimestampCursor = mvPartitionStorage.scan(clock.now());
        PartitionTimestampCursor scanLatestCursor = mvPartitionStorage.scan(HybridTimestamp.MAX_VALUE);

        Cursor<ReadResult> scanVersionsCursor = mvPartitionStorage.runConsistently(locker -> {
            locker.lock(rowId);

            return mvPartitionStorage.scanVersions(rowId);
        });

        Cursor<RowId> getFromHashIndexCursor = hashIndexStorage.get(hashIndexRow.indexColumns());

        Cursor<RowId> getFromSortedIndexCursor = sortedIndexStorage.get(hashIndexRow.indexColumns());
        Cursor<IndexRow> scanFromSortedIndexCursor = sortedIndexStorage.scan(null, null, GREATER);

        CompletableFuture<Void> destroyFuture = tableStorage.destroyPartition(PARTITION_ID);
        if (waitForDestroyFuture) {
            assertThat(destroyFuture, willCompleteSuccessfully());
        }

        // Let's check that we won't get destroyed storages.
        assertNull(tableStorage.getMvPartition(PARTITION_ID));
        assertThrows(StorageException.class, () -> tableStorage.getOrCreateHashIndex(PARTITION_ID, hashIdx));
        assertThrows(StorageException.class, () -> tableStorage.getOrCreateSortedIndex(PARTITION_ID, sortedIdx));

        checkStorageDestroyed(mvPartitionStorage);
        checkStorageDestroyed(hashIndexStorage);
        checkStorageDestroyed(sortedIndexStorage);

        assertThrows(StorageDestroyedException.class, () -> getAll(scanAtTimestampCursor));
        assertThrows(StorageDestroyedException.class, () -> getAll(scanLatestCursor));

        assertThrows(StorageDestroyedException.class, () -> getAll(scanVersionsCursor));

        assertThrows(StorageDestroyedException.class, () -> getAll(getFromHashIndexCursor));

        assertThrows(StorageDestroyedException.class, () -> getAll(getFromSortedIndexCursor));
        assertThrows(StorageDestroyedException.class, () -> getAll(scanFromSortedIndexCursor));

        // What happens if there is no partition?
        assertThrows(StorageException.class, () -> tableStorage.destroyPartition(PARTITION_ID));
    }

    @SuppressWarnings("resource")
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testDestroyTableStorage(boolean waitForDestroyFuture) {
        MvPartitionStorage mvPartitionStorage = getOrCreateMvPartition(PARTITION_ID);
        HashIndexStorage hashIndexStorage = tableStorage.getOrCreateHashIndex(PARTITION_ID, hashIdx);
        SortedIndexStorage sortedIndexStorage = tableStorage.getOrCreateSortedIndex(PARTITION_ID, sortedIdx);

        List<TestRow> rows = List.of(
                new TestRow(new RowId(PARTITION_ID), binaryRow(new TestKey(0, "0"), new TestValue(0, "0"))),
                new TestRow(new RowId(PARTITION_ID), binaryRow(new TestKey(1, "1"), new TestValue(1, "1")))
        );

        fillStorages(mvPartitionStorage, hashIndexStorage, sortedIndexStorage, rows);

        PartitionTimestampCursor scanTimestampCursor = mvPartitionStorage.scan(clock.now());

        IndexRow hashIndexRow = indexRow(hashIndexStorage.indexDescriptor(), rows.get(0).row, rows.get(0).rowId);
        IndexRow sortedIndexRow = indexRow(sortedIndexStorage.indexDescriptor(), rows.get(0).row, rows.get(0).rowId);

        Cursor<RowId> getFromHashIndexCursor = hashIndexStorage.get(hashIndexRow.indexColumns());

        Cursor<RowId> getFromSortedIndexCursor = sortedIndexStorage.get(sortedIndexRow.indexColumns());
        Cursor<IndexRow> scanFromSortedIndexCursor = sortedIndexStorage.scan(null, null, GREATER);

        CompletableFuture<Void> destroyFuture = tableStorage.destroy();

        Runnable waitForDestroy = () -> assertThat(destroyFuture, willCompleteSuccessfully());

        if (waitForDestroyFuture) {
            waitForDestroy.run();
        }

        checkStorageDestroyed(mvPartitionStorage);
        checkStorageDestroyed(hashIndexStorage);
        checkStorageDestroyed(sortedIndexStorage);

        assertThrows(StorageDestroyedException.class, () -> getAll(scanTimestampCursor));

        assertThrows(StorageDestroyedException.class, () -> getAll(getFromHashIndexCursor));

        assertThrows(StorageDestroyedException.class, () -> getAll(getFromSortedIndexCursor));
        assertThrows(StorageDestroyedException.class, () -> getAll(scanFromSortedIndexCursor));

        // Let's check that nothing will happen if we try to destroy it again.
        assertThat(tableStorage.destroy(), willCompleteSuccessfully());

        // Make sure the destroy finishes before we recreate the storage.
        if (!waitForDestroyFuture) {
            waitForDestroy.run();
        }

        // Let's check that after restarting the table we will have an empty partition.
        tableStorage = createMvTableStorage();

        mvPartitionStorage = getOrCreateMvPartition(PARTITION_ID);
        hashIndexStorage = tableStorage.getOrCreateHashIndex(PARTITION_ID, hashIdx);
        sortedIndexStorage = tableStorage.getOrCreateSortedIndex(PARTITION_ID, sortedIdx);

        checkForMissingRows(mvPartitionStorage, hashIndexStorage, sortedIndexStorage, rows);
    }

    private void startRebalanceWithChecks(
            int partitionId,
            MvPartitionStorage mvPartitionStorage,
            HashIndexStorage hashIndexStorage,
            SortedIndexStorage sortedIndexStorage,
            List<TestRow> rowsBeforeRebalanceStart
    ) {
        fillStorages(mvPartitionStorage, hashIndexStorage, sortedIndexStorage, rowsBeforeRebalanceStart);

        // Let's open the cursors before start rebalance.
        TestRow rowForCursors = rowsBeforeRebalanceStart.get(0);

        Cursor<?> mvPartitionStorageScanCursor = mvPartitionStorage.scan(rowForCursors.timestamp);

        IndexRow hashIndexRow = indexRow(hashIndexStorage.indexDescriptor(), rowForCursors.row, rowForCursors.rowId);
        IndexRow sortedIndexRow = indexRow(sortedIndexStorage.indexDescriptor(), rowForCursors.row, rowForCursors.rowId);

        Cursor<?> hashIndexStorageGetCursor = hashIndexStorage.get(hashIndexRow.indexColumns());

        Cursor<?> sortedIndexStorageGetCursor = sortedIndexStorage.get(sortedIndexRow.indexColumns());
        Cursor<?> sortedIndexStorageScanCursor = sortedIndexStorage.scan(null, null, GREATER);

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

    @SuppressWarnings({"resource", "deprecation"})
    private void checkMvPartitionStorageMethodsAfterStartRebalance(MvPartitionStorage storage) {
        checkLastApplied(storage, REBALANCE_IN_PROGRESS, REBALANCE_IN_PROGRESS);

        assertNull(storage.committedGroupConfiguration());

        assertDoesNotThrow(() -> storage.committedGroupConfiguration());

        storage.runConsistently(locker -> {
            assertThrows(StorageRebalanceException.class, () -> storage.lastApplied(100, 500));
            assertThrows(StorageRebalanceException.class, () -> storage.committedGroupConfiguration(BYTE_EMPTY_ARRAY));

            assertThrows(
                    StorageRebalanceException.class,
                    () -> storage.committedGroupConfiguration(BYTE_EMPTY_ARRAY)
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

    void testNextRowIdToBuildAfterOperation(Operation operation) throws Exception {
        MvPartitionStorage mvPartitionStorage = getOrCreateMvPartition(PARTITION_ID);

        IndexStorage hashIndexStorage = tableStorage.getOrCreateIndex(PARTITION_ID, hashIdx);
        IndexStorage sortedIndexStorage = tableStorage.getOrCreateIndex(PARTITION_ID, sortedIdx);
        IndexStorage pkIndexStorage = tableStorage.getOrCreateIndex(PARTITION_ID, pkIdx);

        RowId rowId0 = new RowId(PARTITION_ID);
        RowId rowId1 = new RowId(PARTITION_ID);
        RowId rowId2 = new RowId(PARTITION_ID);

        mvPartitionStorage.runConsistently(locker -> {
            hashIndexStorage.setNextRowIdToBuild(rowId0);
            sortedIndexStorage.setNextRowIdToBuild(rowId1);
            pkIndexStorage.setNextRowIdToBuild(rowId2);

            return null;
        });

        assertThat(mvPartitionStorage.flush(), willCompleteSuccessfully());

        // We expect that nextRowIdToBuild will be reverted to its default value after the operation.
        operation.doOperation();

        mvPartitionStorage = getOrCreateMvPartition(PARTITION_ID);

        IndexStorage recreatedHashIndexStorage = tableStorage.getOrCreateIndex(PARTITION_ID, hashIdx);
        IndexStorage recreatedSortedIndexStorage = tableStorage.getOrCreateIndex(PARTITION_ID, sortedIdx);
        IndexStorage recreatedPkIndexStorage = tableStorage.getOrCreateIndex(PARTITION_ID, pkIdx);

        assertThat(recreatedHashIndexStorage.getNextRowIdToBuild(), is(equalTo(INITIAL_ROW_ID_TO_BUILD)));
        assertThat(recreatedSortedIndexStorage.getNextRowIdToBuild(), is(equalTo(INITIAL_ROW_ID_TO_BUILD)));
        assertThat(recreatedPkIndexStorage.getNextRowIdToBuild(), nullValue());

        // Check that rowId can be set successfully.
        mvPartitionStorage.runConsistently(locker -> {
            recreatedHashIndexStorage.setNextRowIdToBuild(rowId0);
            recreatedSortedIndexStorage.setNextRowIdToBuild(rowId1);
            recreatedPkIndexStorage.setNextRowIdToBuild(rowId2);

            return null;
        });

        assertThat(recreatedHashIndexStorage.getNextRowIdToBuild(), is(equalTo(rowId0)));
        assertThat(recreatedSortedIndexStorage.getNextRowIdToBuild(), is(equalTo(rowId1)));
        assertThat(recreatedPkIndexStorage.getNextRowIdToBuild(), is(equalTo(rowId2)));
    }

    private interface Operation {
        void doOperation() throws Exception;
    }
}
