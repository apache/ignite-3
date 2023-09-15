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

import static org.apache.ignite.internal.catalog.commands.CatalogUtils.DEFAULT_DATA_REGION;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
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
     * Tests that explicitly flushed data remeins persistent on the device, when the engine is restarted.
     */
    @Test
    void testRestartAfterFlush() throws Exception {
        assumeFalse(storageEngine.isVolatile());

        StorageTableDescriptor tableDescriptor = new StorageTableDescriptor(1, 1, DEFAULT_DATA_REGION);
        StorageIndexDescriptorSupplier indexSupplier = mock(StorageIndexDescriptorSupplier.class);

        MvTableStorage mvTableStorage = storageEngine.createMvTable(tableDescriptor, indexSupplier);

        mvTableStorage.start();
        try (AutoCloseable ignored0 = mvTableStorage::stop) {
            CompletableFuture<MvPartitionStorage> mvPartitionStorageFuture = mvTableStorage.createMvPartition(0);

            assertThat(mvPartitionStorageFuture, willCompleteSuccessfully());
            MvPartitionStorage mvPartitionStorage = mvPartitionStorageFuture.join();

            try (AutoCloseable ignored1 = mvTableStorage::stop) {
                // Flush. Persist the table itself, not the update.
                CompletableFuture<Void> flushFuture = mvPartitionStorage.flush();
                assertThat(flushFuture, willCompleteSuccessfully());

                mvPartitionStorage.runConsistently(locker -> {
                    // Update of basic storage data.
                    mvPartitionStorage.lastApplied(10, 20);

                    return null;
                });

                // Flush.
                flushFuture = mvPartitionStorage.flush();
                assertThat(flushFuture, willCompleteSuccessfully());
            }
        }

        // Restart.
        stopEngineAfterTest();
        createEngineBeforeTest();

        mvTableStorage = storageEngine.createMvTable(tableDescriptor, indexSupplier);

        mvTableStorage.start();
        try (AutoCloseable ignored0 = mvTableStorage::close) {
            CompletableFuture<MvPartitionStorage> mvPartitionStorageFuture = mvTableStorage.createMvPartition(0);

            assertThat(mvPartitionStorageFuture, willCompleteSuccessfully());
            MvPartitionStorage mvPartitionStorage = mvPartitionStorageFuture.join();

            try (AutoCloseable ignored1 = mvTableStorage::stop) {
                // Check that data has been persisted.
                assertEquals(10, mvPartitionStorage.lastAppliedIndex());
                assertEquals(20, mvPartitionStorage.lastAppliedTerm());
            }
        }
    }
}
