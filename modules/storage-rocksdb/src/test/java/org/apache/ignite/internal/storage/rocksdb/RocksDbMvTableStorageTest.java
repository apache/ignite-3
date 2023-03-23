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

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;

import java.nio.file.Path;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.distributionzones.configuration.DistributionZoneConfiguration;
import org.apache.ignite.internal.distributionzones.configuration.DistributionZonesConfiguration;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.schema.configuration.TablesConfiguration;
import org.apache.ignite.internal.storage.AbstractMvTableStorageTest;
import org.apache.ignite.internal.storage.MvPartitionStorage;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.storage.rocksdb.configuration.schema.RocksDbStorageEngineConfiguration;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Tests for the {@link RocksDbTableStorage}.
 */
@ExtendWith(WorkDirectoryExtension.class)
@ExtendWith(ConfigurationExtension.class)
public class RocksDbMvTableStorageTest extends AbstractMvTableStorageTest {
    @BeforeEach
    void setUp(
            @WorkDirectory Path workDir,
            @InjectConfiguration("mock {flushDelayMillis = 0, defaultRegion {size = 16536, writeBufferSize = 16536}}")
            RocksDbStorageEngineConfiguration rocksDbEngineConfig,
            @InjectConfiguration(
                    "mock.tables.foo{ dataStorage.name = " + RocksDbStorageEngine.ENGINE_NAME + "}"
            )
            TablesConfiguration tablesConfig,
            @InjectConfiguration("mock.partitions = 512")
            DistributionZoneConfiguration distributionZoneConfiguration
    ) {
        initialize(new RocksDbStorageEngine(rocksDbEngineConfig, workDir),
                tablesConfig, distributionZoneConfiguration);
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

        partitionStorage0.runConsistently(() -> partitionStorage0.addWrite(rowId0, testData, txId, UUID.randomUUID(), 0));

        MvPartitionStorage partitionStorage1 = getOrCreateMvPartition(PARTITION_ID_1);

        RowId rowId1 = new RowId(PARTITION_ID_1);

        partitionStorage1.runConsistently(() -> partitionStorage1.addWrite(rowId1, testData, txId, UUID.randomUUID(), 0));

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
    void testRestart() {
        var testData = binaryRow(new TestKey(1, "1"), new TestValue(10, "10"));

        UUID txId = UUID.randomUUID();

        MvPartitionStorage partitionStorage0 = getOrCreateMvPartition(PARTITION_ID);

        RowId rowId0 = new RowId(PARTITION_ID);

        partitionStorage0.runConsistently(() -> partitionStorage0.addWrite(rowId0, testData, txId, UUID.randomUUID(), 0));

        tableStorage.stop();

        tableStorage = createMvTableStorage(tableStorage.tablesConfiguration(), tableStorage.distributionZoneConfiguration());

        tableStorage.start();

        assertThat(tableStorage.getMvPartition(PARTITION_ID), is(notNullValue()));
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
