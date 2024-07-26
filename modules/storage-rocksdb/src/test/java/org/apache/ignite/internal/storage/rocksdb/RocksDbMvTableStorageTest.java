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

package org.apache.ignite.internal.storage.rocksdb;

import static org.apache.ignite.internal.catalog.commands.CatalogUtils.DEFAULT_PARTITION_COUNT;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mock;

import java.nio.file.Path;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.internal.components.LogSyncer;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.storage.AbstractMvTableStorageTest;
import org.apache.ignite.internal.storage.MvPartitionStorage;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.storage.configurations.StorageConfiguration;
import org.apache.ignite.internal.storage.engine.MvTableStorage;
import org.apache.ignite.internal.storage.engine.StorageTableDescriptor;
import org.apache.ignite.internal.storage.rocksdb.configuration.schema.RocksDbStorageEngineConfiguration;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.util.LazyPath;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Tests for the {@link RocksDbTableStorage}.
 */
@ExtendWith(WorkDirectoryExtension.class)
public class RocksDbMvTableStorageTest extends AbstractMvTableStorageTest {
    private RocksDbStorageEngine engine;

    @BeforeEach
    void setUp(
            @WorkDirectory Path workDir,
            @InjectConfiguration("mock.flushDelayMillis = 0")
            RocksDbStorageEngineConfiguration engineConfig,
            @InjectConfiguration("mock.profiles.default {engine = rocksdb, size = 16777216, writeBufferSize = 16777216}")
            StorageConfiguration storageConfiguration
    ) {
        engine = new RocksDbStorageEngine("test", engineConfig, storageConfiguration, LazyPath.create(workDir), mock(LogSyncer.class));

        engine.start();

        initialize();
    }

    @Override
    @AfterEach
    protected void tearDown() throws Exception {
        super.tearDown();

        IgniteUtils.closeAllManually(engine == null ? null : engine::stop);
    }

    @Override
    protected MvTableStorage createMvTableStorage() {
        return engine.createMvTable(
                new StorageTableDescriptor(1, DEFAULT_PARTITION_COUNT, "default"),
                indexDescriptorSupplier
        );
    }

    /**
     * Tests that dropping a partition does not remove extra data.
     */
    @Test
    void testDropPartition() throws Exception {
        var testData = binaryRow(new TestKey(1, "1"), new TestValue(10, "10"));

        UUID txId = UUID.randomUUID();

        MvPartitionStorage partitionStorage0 = getOrCreateMvPartition(PARTITION_ID_0);

        RowId rowId0 = new RowId(PARTITION_ID_0);

        partitionStorage0.runConsistently(locker -> {
            locker.lock(rowId0);

            return partitionStorage0.addWrite(rowId0, testData, txId, COMMIT_TABLE_ID, 0);
        });

        MvPartitionStorage partitionStorage1 = getOrCreateMvPartition(PARTITION_ID_1);

        RowId rowId1 = new RowId(PARTITION_ID_1);

        partitionStorage1.runConsistently(locker -> {
            locker.lock(rowId1);

            return partitionStorage1.addWrite(rowId1, testData, txId, COMMIT_TABLE_ID, 0);
        });

        tableStorage.destroyPartition(PARTITION_ID_0).get(1, TimeUnit.SECONDS);

        // Partition destruction doesn't enforce flush.
        ((RocksDbTableStorage) tableStorage).awaitFlush(true);

        assertThat(tableStorage.getMvPartition(PARTITION_ID_0), is(nullValue()));
        assertThat(getOrCreateMvPartition(PARTITION_ID_0).read(rowId0, HybridTimestamp.MAX_VALUE).binaryRow(),
                is(nullValue()));
        assertThat(unwrap(tableStorage.getMvPartition(PARTITION_ID_1).read(rowId1, HybridTimestamp.MAX_VALUE).binaryRow()),
                is(equalTo(unwrap(testData))));
    }

    /**
     * Tests that restarting the storage does not result in data loss.
     */
    @Test
    void testRestart() throws Exception {
        var testData = binaryRow(new TestKey(1, "1"), new TestValue(10, "10"));

        UUID txId = UUID.randomUUID();

        MvPartitionStorage partitionStorage0 = getOrCreateMvPartition(PARTITION_ID);

        RowId rowId0 = new RowId(PARTITION_ID);

        partitionStorage0.runConsistently(locker -> {
            locker.lock(rowId0);

            return partitionStorage0.addWrite(rowId0, testData, txId, COMMIT_TABLE_ID, 0);
        });

        tableStorage.close();

        tableStorage = createMvTableStorage();

        assertThat(tableStorage.getMvPartition(PARTITION_ID), is(nullValue()));

        assertThat(tableStorage.createMvPartition(PARTITION_ID), willCompleteSuccessfully());

        assertThat(tableStorage.getMvPartition(PARTITION_ID_0), is(nullValue()));
        assertThat(tableStorage.getMvPartition(PARTITION_ID_1), is(nullValue()));
        assertThat(unwrap(tableStorage.getMvPartition(PARTITION_ID).read(rowId0, HybridTimestamp.MAX_VALUE).binaryRow()),
                is(equalTo(unwrap(testData))));
    }

    @Test
    void storageAdvertisesItIsPersistent() {
        assertThat(tableStorage.isVolatile(), is(false));
    }
}
