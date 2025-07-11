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
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureExceptionMatcher.willTimeoutFast;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.IntStream;
import org.apache.ignite.internal.binarytuple.BinaryTupleBuilder;
import org.apache.ignite.internal.schema.BinaryTuple;
import org.apache.ignite.internal.schema.BinaryTupleSchema;
import org.apache.ignite.internal.schema.BinaryTupleSchema.Element;
import org.apache.ignite.internal.storage.AbstractMvTableStorageTest;
import org.apache.ignite.internal.storage.MvPartitionStorage;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.storage.index.HashIndexStorage;
import org.apache.ignite.internal.storage.index.IndexRowImpl;
import org.apache.ignite.internal.storage.index.IndexStorage;
import org.apache.ignite.internal.storage.index.SortedIndexStorage;
import org.apache.ignite.internal.storage.index.StorageHashIndexDescriptor;
import org.apache.ignite.internal.storage.index.StorageHashIndexDescriptor.StorageHashIndexColumnDescriptor;
import org.apache.ignite.internal.storage.index.StorageIndexDescriptor;
import org.apache.ignite.internal.storage.index.StorageIndexDescriptorSupplier;
import org.apache.ignite.internal.storage.index.StorageSortedIndexDescriptor;
import org.apache.ignite.internal.storage.index.StorageSortedIndexDescriptor.StorageSortedIndexColumnDescriptor;
import org.apache.ignite.internal.type.NativeTypes;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;

/**
 * Tests basic functionality of persistent storage engines. Allows for more complex scenarios than {@link AbstractMvTableStorageTest},
 * because it doesn't limit the usage of the engine with a single table.
 */
public abstract class AbstractPersistentStorageEngineTest extends AbstractStorageEngineTest {
    /** Makes sure that table destruction is persisted durably. */
    protected abstract void persistTableDestructionIfNeeded();

    @Test
    void isNonVolatile() {
        assertFalse(storageEngine.isVolatile());
    }

    /**
     * Tests that explicitly flushed data remains persistent on the device, when the engine is restarted.
     */
    @Test
    void testRestartAfterFlush() throws Exception {
        int tableId = 1;
        int lastAppliedIndex = 10;
        int lastAppliedTerm = 20;

        createMvTableWithPartitionAndFill(tableId, lastAppliedIndex, lastAppliedTerm);

        restartEngine();

        checkMvTableStorageWithPartitionAfterRestart(tableId, lastAppliedIndex, lastAppliedTerm);
    }

    private void restartEngine() {
        stopEngineAfterTest();
        createEngineBeforeTest();
    }

    /**
     * Tests that write-ahead log is synced before flush.
     */
    @Test
    void testSyncWalBeforeFlush() throws Exception {
        int tableId = 1;
        int lastAppliedIndex = 10;
        int lastAppliedTerm = 20;

        createMvTableWithPartitionAndFill(tableId, lastAppliedIndex, lastAppliedTerm);

        verify(logSyncer, atLeastOnce()).sync();
    }

    @Test
    void testDestroyMvTableOnRecovery() throws Exception {
        int tableId = 1;

        // Table does not exist.
        assertDoesNotThrow(() -> storageEngine.destroyMvTable(tableId));

        createMvTableWithPartitionAndFill(tableId, 10, 20);

        restartEngine();

        assertDoesNotThrow(() -> storageEngine.destroyMvTable(tableId));

        checkMvTableStorageWithPartitionAfterRestart(tableId, 0, 0);
    }

    /**
     * Tests indexes recovery that happens on table storage start for a scenario with multiple tables.
     */
    @Test
    void testIndexRecoveryForMultipleTables(@Mock StorageIndexDescriptorSupplier indexDescriptorSupplier) {
        int partitionId = 0;

        int numTables = 2;

        var sortedIndexColumnDescriptor = new StorageSortedIndexColumnDescriptor(
                "foo", NativeTypes.INT64, true, true, false
        );

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

                    StorageIndexDescriptor sortedIndexDescriptor = indexDescriptorMap.get(indexId);
                    tableStorage.createSortedIndex(partitionId, (StorageSortedIndexDescriptor) sortedIndexDescriptor);
                    SortedIndexStorage sortedIndexStorage = (SortedIndexStorage) tableStorage.getIndex(
                            partitionId, sortedIndexDescriptor.id()
                    );

                    StorageIndexDescriptor hashIndexDescriptor = indexDescriptorMap.get(indexId + 1);
                    tableStorage.createHashIndex(partitionId, (StorageHashIndexDescriptor) hashIndexDescriptor);
                    HashIndexStorage hashIndexStorage = (HashIndexStorage) tableStorage.getIndex(partitionId, hashIndexDescriptor.id());

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

        restartEngine();

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

    /**
     * Ensures that {@code flush(false)} does not trigger a flush explicitly, but only subscribes to the next scheduled one.
     */
    @Test
    void testSubscribeToFlush() throws Exception {
        int tableId = 1;
        int lastAppliedIndex = 10;
        int lastAppliedTerm = 20;

        StorageTableDescriptor tableDescriptor = new StorageTableDescriptor(tableId, 1, DEFAULT_STORAGE_PROFILE);
        StorageIndexDescriptorSupplier indexSupplier = mock(StorageIndexDescriptorSupplier.class);

        MvTableStorage mvTableStorage = storageEngine.createMvTable(tableDescriptor, indexSupplier);

        try (AutoCloseable ignored0 = mvTableStorage::close) {
            CompletableFuture<MvPartitionStorage> mvPartitionStorageFuture = mvTableStorage.createMvPartition(0);

            assertThat(mvPartitionStorageFuture, willCompleteSuccessfully());
            MvPartitionStorage mvPartitionStorage = mvPartitionStorageFuture.join();

            try (AutoCloseable ignored1 = mvPartitionStorage::close) {
                assertThat(mvPartitionStorage.flush(), willCompleteSuccessfully());

                mvPartitionStorage.runConsistently(locker -> {
                    mvPartitionStorage.lastApplied(lastAppliedIndex, lastAppliedTerm);

                    return null;
                });

                CompletableFuture<Void> subscribeFuture = mvPartitionStorage.flush(false);
                assertThat(subscribeFuture, willTimeoutFast());

                CompletableFuture<Void> flushFuture = mvPartitionStorage.flush();
                assertSame(subscribeFuture, flushFuture);

                assertThat(flushFuture, willCompleteSuccessfully());
            }
        }
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

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    protected void remembersCreatedTableIdsOnDisk(boolean restart) {
        createTableStorageLeavingTraceOnDisk(1);
        createTableStorageLeavingTraceOnDisk(3);

        if (restart) {
            restartEngine();
        }

        assertThat(storageEngine.tableIdsOnDisk(), containsInAnyOrder(1, 3));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    protected void tableIdsOnDiskGetRemovedOnPersistedDestruction(boolean restart) {
        MvTableStorage tableStorage1 = createTableStorageLeavingTraceOnDisk(1);
        createTableStorageLeavingTraceOnDisk(3);

        assertThat(tableStorage1.destroy(), willCompleteSuccessfully());

        if (restart) {
            persistTableDestructionIfNeeded();
            restartEngine();
        }

        assertThat(storageEngine.tableIdsOnDisk(), contains(3));
    }

    private MvTableStorage createTableStorageLeavingTraceOnDisk(int tableId) {
        StorageTableDescriptor tableDescriptor = new StorageTableDescriptor(tableId, 1, DEFAULT_STORAGE_PROFILE);
        StorageIndexDescriptorSupplier indexSupplier = mock(StorageIndexDescriptorSupplier.class);

        MvTableStorage tableStorage = storageEngine.createMvTable(tableDescriptor, indexSupplier);

        CompletableFuture<MvPartitionStorage> partitionStorageFuture = tableStorage.createMvPartition(0);
        assertThat(partitionStorageFuture, willCompleteSuccessfully());

        MvPartitionStorage partitionStorage = partitionStorageFuture.join();

        partitionStorage.runConsistently(locker -> {
            partitionStorage.committedGroupConfiguration(new byte[]{1, 2, 3});
            partitionStorage.lastApplied(1, 1);
            return null;
        });

        assertThat(partitionStorage.flush(), willCompleteSuccessfully());

        return tableStorage;
    }
}
