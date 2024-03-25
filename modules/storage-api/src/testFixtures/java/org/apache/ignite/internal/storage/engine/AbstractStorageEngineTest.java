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

import static org.apache.ignite.internal.catalog.CatalogService.DEFAULT_STORAGE_PROFILE;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assumptions.assumeFalse;
import static org.mockito.Mockito.mock;

import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.storage.AbstractMvTableStorageTest;
import org.apache.ignite.internal.storage.BaseMvStoragesTest;
import org.apache.ignite.internal.storage.MvPartitionStorage;
import org.apache.ignite.internal.storage.index.StorageIndexDescriptorSupplier;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests basic functionality of storage engines. Allows for more complex scenarios than {@link AbstractMvTableStorageTest}, because it
 * doesn't limit the usage of the engine with a single table.
 */
public abstract class AbstractStorageEngineTest extends BaseMvStoragesTest {
    /** Engine instance. */
    private StorageEngine storageEngine;

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

    @Test
    protected void testDropMvTableOnRecovery() throws Exception {
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
