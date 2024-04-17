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

package org.apache.ignite.internal.storage.engine;

import static java.util.stream.Collectors.toList;
import static org.apache.ignite.internal.catalog.CatalogService.DEFAULT_STORAGE_PROFILE;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assumptions.assumeFalse;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.stream.IntStream;
import org.apache.ignite.internal.binarytuple.BinaryTupleBuilder;
import org.apache.ignite.internal.components.LogSyncer;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.BinaryTuple;
import org.apache.ignite.internal.schema.BinaryTupleSchema;
import org.apache.ignite.internal.schema.BinaryTupleSchema.Element;
import org.apache.ignite.internal.storage.AbstractMvTableStorageTest;
import org.apache.ignite.internal.storage.BaseMvStoragesTest;
import org.apache.ignite.internal.storage.MvPartitionStorage;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.storage.index.HashIndexStorage;
import org.apache.ignite.internal.storage.index.IndexRow;
import org.apache.ignite.internal.storage.index.IndexRowImpl;
import org.apache.ignite.internal.storage.index.IndexStorage;
import org.apache.ignite.internal.storage.index.SortedIndexStorage;
import org.apache.ignite.internal.storage.index.StorageHashIndexDescriptor;
import org.apache.ignite.internal.storage.index.StorageHashIndexDescriptor.StorageHashIndexColumnDescriptor;
import org.apache.ignite.internal.storage.index.StorageIndexDescriptor;
import org.apache.ignite.internal.storage.index.StorageIndexDescriptorSupplier;
import org.apache.ignite.internal.storage.index.StorageSortedIndexDescriptor;
import org.apache.ignite.internal.storage.index.StorageSortedIndexDescriptor.StorageSortedIndexColumnDescriptor;
import org.apache.ignite.internal.storage.index.impl.BinaryTupleRowSerializer;
import org.apache.ignite.internal.type.NativeTypes;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * Tests basic functionality of storage engines. Allows for more complex scenarios than {@link AbstractMvTableStorageTest}, because it
 * doesn't limit the usage of the engine with a single table.
 */
@ExtendWith(MockitoExtension.class)
public abstract class AbstractStorageEngineTest extends BaseMvStoragesTest {
    /** Engine instance. */
    private StorageEngine storageEngine;

    protected LogSyncer logSyncer = mock(LogSyncer.class);

    @BeforeEach
    void createEngineBeforeTest() {
        storageEngine = createEngine();

        storageEngine.start();
    }

    @AfterEach
    void stopEngineAfterTest() {
        if (storageEngine != null) {
            storageEngine.stop();
        }
    }

    /**
     * Creates a new storage engine instance. For persistent engines, the instances within a single test method should point to the same
     * directory.
     */
    protected abstract StorageEngine createEngine();

    /**
     * Tests that explicitly flushed data remains persistent on the device, when the engine is restarted.
     */
    @Test
    void testRestartAfterFlush() throws Exception {
        assumeFalse(storageEngine.isVolatile());

        int tableId = 1;
        int lastAppliedIndex = 10;
        int lastAppliedTerm = 20;

        createMvTableWithPartitionAndFill(tableId, lastAppliedIndex, lastAppliedTerm);

        // Restart.
        stopEngineAfterTest();
        createEngineBeforeTest();

        checkMvTableStorageWithPartitionAfterRestart(tableId, lastAppliedIndex, lastAppliedTerm);
    }

    /**
     * Tests that write-ahead log is synced before flush.
     */
    @Test
    void testSyncWalBeforeFlush() throws Exception {
        assumeFalse(storageEngine.isVolatile());

        int tableId = 1;
        int lastAppliedIndex = 10;
        int lastAppliedTerm = 20;

        createMvTableWithPartitionAndFill(tableId, lastAppliedIndex, lastAppliedTerm);

        verify(logSyncer, atLeastOnce()).sync();
    }

    @Test
    void testDropMvTableOnRecovery() throws Exception {
        assumeFalse(storageEngine.isVolatile());

        int tableId = 1;

        // Table does not exist.
        assertDoesNotThrow(() -> storageEngine.dropMvTable(tableId));

        createMvTableWithPartitionAndFill(tableId, 10, 20);

        // Restart.
        stopEngineAfterTest();
        createEngineBeforeTest();

        assertDoesNotThrow(() -> storageEngine.dropMvTable(tableId));

        checkMvTableStorageWithPartitionAfterRestart(tableId, 0, 0);
    }

    /**
     * Tests indexes recovery that happens on table storage start for a scenario with multiple tables.
     */
    @Test
    void testIndexRecoveryForMultipleTables(@Mock StorageIndexDescriptorSupplier indexDescriptorSupplier) {
        int partitionId = 0;

        int numTables = 2;

        var sortedIndexColumnDescriptor = new StorageSortedIndexColumnDescriptor("foo", NativeTypes.INT64, true, true);

        var hashIndexColumnDescriptor = new StorageHashIndexColumnDescriptor("foo", NativeTypes.INT64, true);

        var indexDescriptorMap = new HashMap<Integer, StorageIndexDescriptor>();

        for (int i = 0; i < numTables; i++) {
            int sortedIndexId = numTables + i * 2;
            int hashIndexId = sortedIndexId + 1;

            indexDescriptorMap.put(
                    sortedIndexId,
                    new StorageSortedIndexDescriptor(sortedIndexId, List.of(sortedIndexColumnDescriptor), false)
            );
            indexDescriptorMap.put(
                    hashIndexId,
                    new StorageHashIndexDescriptor(hashIndexId, List.of(hashIndexColumnDescriptor), false)
            );
        }

        when(indexDescriptorSupplier.get(anyInt()))
                .thenAnswer(invocationOnMock -> indexDescriptorMap.get(invocationOnMock.getArgument(0, Integer.class)));

        // Create a test data row to fill the indexes with.
        BinaryTupleSchema schema = BinaryTupleSchema.create(new Element[]{new Element(NativeTypes.INT64, true)});

        ByteBuffer buffer = new BinaryTupleBuilder(schema.elementCount())
                .appendLong(1)
                .build();

        var tuple = new BinaryTuple(schema.elementCount(), buffer);

        var indexRow = new IndexRowImpl(tuple, new RowId(partitionId));

        // Create and fill tables and indexes.
        List<MvTableStorage> tableStorages = IntStream.range(0, numTables)
                .mapToObj(i -> {
                    // Page Memory doesn't like table IDs equal to 0.
                    StorageTableDescriptor tableDescriptor = new StorageTableDescriptor(i + 1, 1, DEFAULT_STORAGE_PROFILE);

                    MvTableStorage tableStorage = storageEngine.createMvTable(tableDescriptor, indexDescriptorSupplier);

                    CompletableFuture<MvPartitionStorage> future = tableStorage.createMvPartition(partitionId);

                    assertThat(future, willCompleteSuccessfully());

                    MvPartitionStorage partitionStorage = future.join();

                    int indexId = numTables + i * 2;

                    SortedIndexStorage sortedIndexStorage = tableStorage.getOrCreateSortedIndex(
                            partitionId, (StorageSortedIndexDescriptor) indexDescriptorMap.get(indexId)
                    );

                    HashIndexStorage hashIndexStorage = tableStorage.getOrCreateHashIndex(
                            partitionId, (StorageHashIndexDescriptor) indexDescriptorMap.get(indexId + 1)
                    );

                    partitionStorage.runConsistently(locker -> {
                        sortedIndexStorage.put(indexRow);
                        hashIndexStorage.put(indexRow);

                        return null;
                    });

                    assertThat(partitionStorage.flush(), willCompleteSuccessfully());

                    return tableStorage;
                })
                .collect(toList());

        // Check that every table contains its own indexes.
        for (int i = 0; i < numTables; i++) {
            MvTableStorage tableStorage = tableStorages.get(i);

            int sortedIndexId = numTables + i * 2;
            int hashIndexId = sortedIndexId + 1;

            for (int indexId = numTables; indexId < numTables * 2; indexId++) {
                IndexStorage indexStorage = tableStorage.getIndex(partitionId, indexId);

                if (indexId == sortedIndexId || indexId == hashIndexId) {
                    assertThat(indexStorage, is(notNullValue()));
                } else {
                    assertThat(indexStorage, is(nullValue()));
                }
            }
        }

        // Restart.
        stopEngineAfterTest();
        createEngineBeforeTest();

        // Re-create the tables.
        tableStorages = IntStream.range(0, numTables)
                .mapToObj(i -> {
                    StorageTableDescriptor tableDescriptor = new StorageTableDescriptor(i + 1, 1, DEFAULT_STORAGE_PROFILE);

                    MvTableStorage tableStorage = storageEngine.createMvTable(tableDescriptor, indexDescriptorSupplier);

                    CompletableFuture<MvPartitionStorage> future = tableStorage.createMvPartition(partitionId);

                    assertThat(future, willCompleteSuccessfully());

                    return tableStorage;
                })
                .collect(toList());

        // Check that indexes have been restored.
        for (int i = 0; i < numTables; i++) {
            MvTableStorage tableStorage = tableStorages.get(i);

            int sortedIndexId = numTables + i * 2;
            int hashIndexId = sortedIndexId + 1;

            for (int indexId = numTables; indexId < numTables * 2; indexId++) {
                IndexStorage indexStorage = tableStorage.getIndex(partitionId, indexId);

                if (indexId == sortedIndexId || indexId == hashIndexId) {
                    assertThat(indexStorage, is(notNullValue()));
                } else {
                    assertThat(indexStorage, is(nullValue()));
                }
            }
        }
    }

    @Test
    public void testPutBigRow(@Mock StorageIndexDescriptorSupplier indexDescriptorSupplier) {
        int indexId = 0;
        int partitionId = 0;

        StorageTableDescriptor tableDescriptor = new StorageTableDescriptor(1, 1, DEFAULT_STORAGE_PROFILE);

        MvTableStorage tableStorage = storageEngine.createMvTable(tableDescriptor, indexDescriptorSupplier);

        CompletableFuture<MvPartitionStorage> mvPartitionFut;
        mvPartitionFut = tableStorage.createMvPartition(0);

        assertThat(mvPartitionFut, willCompleteSuccessfully());

        MvPartitionStorage mvPartition = mvPartitionFut.join();

        StorageSortedIndexColumnDescriptor indexColumnDescriptor =
                new StorageSortedIndexColumnDescriptor("foo", NativeTypes.STRING, true, true);
        StorageSortedIndexDescriptor indexDescriptor = new StorageSortedIndexDescriptor(indexId,
                List.of(indexColumnDescriptor), false);
        SortedIndexStorage indexStorage = tableStorage.getOrCreateSortedIndex(partitionId, indexDescriptor);

        var serializer = new BinaryTupleRowSerializer(indexDescriptor);

        byte[] bytes = new byte[Integer.MAX_VALUE / 1000];
        new Random().nextBytes(bytes);
        String str = new String(bytes);

        IndexRow indexRow = serializer.serializeRow(new Object[]{str}, new RowId(0));
        mvPartition.runConsistently(locker -> {
            indexStorage.put(indexRow);

            return null;
        });

        BinaryRow binaryRow = binaryRow(new TestKey(10, "foo"), new TestValue(20, str));

        mvPartition.runConsistently(locker -> {
            RowId rowId = new RowId(0);
            locker.tryLock(rowId);

            mvPartition.addWrite(rowId, binaryRow, UUID.randomUUID(), 999, 0);

            return null;
        });
    }

    private void createMvTableWithPartitionAndFill(int tableId, int lastAppliedIndex, int lastAppliedTerm) throws Exception {
        StorageTableDescriptor tableDescriptor = new StorageTableDescriptor(tableId, 1, DEFAULT_STORAGE_PROFILE);
        StorageIndexDescriptorSupplier indexSupplier = mock(StorageIndexDescriptorSupplier.class);

        MvTableStorage mvTableStorage = storageEngine.createMvTable(tableDescriptor, indexSupplier);

        try (AutoCloseable ignored0 = mvTableStorage::close) {
            CompletableFuture<MvPartitionStorage> mvPartitionStorageFuture = mvTableStorage.createMvPartition(0);

            assertThat(mvPartitionStorageFuture, willCompleteSuccessfully());
            MvPartitionStorage mvPartitionStorage = mvPartitionStorageFuture.join();

            try (AutoCloseable ignored1 = mvPartitionStorage::close) {
                // Flush. Persist the table itself, not the update.
                assertThat(mvPartitionStorage.flush(), willCompleteSuccessfully());

                mvPartitionStorage.runConsistently(locker -> {
                    // Update of basic storage data.
                    mvPartitionStorage.lastApplied(lastAppliedIndex, lastAppliedTerm);

                    return null;
                });

                // Flush.
                assertThat(mvPartitionStorage.flush(), willCompleteSuccessfully());
            }
        }
    }

    private void checkMvTableStorageWithPartitionAfterRestart(
            int tableId,
            int expLastAppliedIndex,
            int expLastAppliedTerm
    ) throws Exception {
        StorageTableDescriptor tableDescriptor = new StorageTableDescriptor(tableId, 1, DEFAULT_STORAGE_PROFILE);
        StorageIndexDescriptorSupplier indexSupplier = mock(StorageIndexDescriptorSupplier.class);

        MvTableStorage mvTableStorage = storageEngine.createMvTable(tableDescriptor, indexSupplier);

        try (AutoCloseable ignored0 = mvTableStorage::close) {
            CompletableFuture<MvPartitionStorage> mvPartitionStorageFuture = mvTableStorage.createMvPartition(0);

            assertThat(mvPartitionStorageFuture, willCompleteSuccessfully());
            MvPartitionStorage mvPartitionStorage = mvPartitionStorageFuture.join();

            try (AutoCloseable ignored1 = mvPartitionStorage::close) {
                // Check that data has been persisted.
                assertEquals(expLastAppliedIndex, mvPartitionStorage.lastAppliedIndex());
                assertEquals(expLastAppliedTerm, mvPartitionStorage.lastAppliedTerm());
            }
        }
    }
}
