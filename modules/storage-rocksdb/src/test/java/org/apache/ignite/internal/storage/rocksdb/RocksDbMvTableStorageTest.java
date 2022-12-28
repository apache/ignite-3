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
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;

import java.nio.file.Path;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.schema.configuration.TablesConfiguration;
import org.apache.ignite.internal.storage.AbstractMvTableStorageTest;
import org.apache.ignite.internal.storage.MvPartitionStorage;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.storage.engine.MvTableStorage;
import org.apache.ignite.internal.storage.rocksdb.configuration.schema.RocksDbStorageEngineConfiguration;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.apache.ignite.internal.util.IgniteUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Tests for the {@link RocksDbTableStorage}.
 */
@ExtendWith(WorkDirectoryExtension.class)
@ExtendWith(ConfigurationExtension.class)
public class RocksDbMvTableStorageTest extends AbstractMvTableStorageTest {
    @InjectConfiguration(
            value = "mock.tables.foo{ partitions = 512, dataStorage.name = " + RocksDbStorageEngine.ENGINE_NAME + "}"
    )
    private TablesConfiguration tablesConfig;

    private RocksDbStorageEngine engine;

    private MvTableStorage tableStorage;

    @BeforeEach
    void setUp(
            @WorkDirectory Path workDir,
            @InjectConfiguration("mock {flushDelayMillis = 0, defaultRegion {size = 16536, writeBufferSize = 16536}}")
            RocksDbStorageEngineConfiguration rocksDbEngineConfig
    ) {
        engine = new RocksDbStorageEngine(rocksDbEngineConfig, workDir);

        engine.start();

        tableStorage = engine.createMvTable(tablesConfig.tables().get("foo"), tablesConfig);

        assertThat(tableStorage, is(instanceOf(RocksDbTableStorage.class)));

        tableStorage.start();

        initialize(tableStorage, tablesConfig);
    }

    @AfterEach
    void tearDown() throws Exception {
        IgniteUtils.closeAll(
                tableStorage == null ? null : tableStorage::stop,
                engine == null ? null : engine::stop
        );
    }

    /**
     * Tests that dropping a partition does not remove extra data.
     */
    @Test
    void testDropPartition() throws Exception {
        var testData = binaryRow(new TestKey(1, "1"), new TestValue(10, "10"));

        UUID txId = UUID.randomUUID();

        MvPartitionStorage partitionStorage0 = tableStorage.getOrCreateMvPartition(PARTITION_ID_0);

        RowId rowId0 = new RowId(PARTITION_ID_0);

        partitionStorage0.runConsistently(() -> partitionStorage0.addWrite(rowId0, testData, txId, UUID.randomUUID(), 0));

        MvPartitionStorage partitionStorage1 = tableStorage.getOrCreateMvPartition(PARTITION_ID_1);

        RowId rowId1 = new RowId(PARTITION_ID_1);

        partitionStorage1.runConsistently(() -> partitionStorage1.addWrite(rowId1, testData, txId, UUID.randomUUID(), 0));

        tableStorage.destroyPartition(PARTITION_ID_0).get(1, TimeUnit.SECONDS);

        // Partition destruction doesn't enforce flush.
        ((RocksDbTableStorage) tableStorage).awaitFlush(true);

        assertThat(tableStorage.getMvPartition(PARTITION_ID_0), is(nullValue()));
        assertThat(tableStorage.getOrCreateMvPartition(PARTITION_ID_0).read(rowId0, HybridTimestamp.MAX_VALUE).binaryRow(),
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

        MvPartitionStorage partitionStorage0 = tableStorage.getOrCreateMvPartition(PARTITION_ID);

        RowId rowId0 = new RowId(PARTITION_ID);

        partitionStorage0.runConsistently(() -> partitionStorage0.addWrite(rowId0, testData, txId, UUID.randomUUID(), 0));

        tableStorage.stop();

        tableStorage = engine.createMvTable(tablesConfig.tables().get("foo"), tablesConfig);

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

    @Disabled("https://issues.apache.org/jira/browse/IGNITE-18180")
    @Override
    public void testDestroyPartition() throws Exception {
        super.testDestroyPartition();
    }

    @Disabled("https://issues.apache.org/jira/browse/IGNITE-18180")
    @Override
    public void testReCreatePartition() throws Exception {
        super.testReCreatePartition();
    }

    @Disabled("https://issues.apache.org/jira/browse/IGNITE-18027")
    @Override
    public void testSuccessRebalance() throws Exception {
        super.testSuccessRebalance();
    }

    @Disabled("https://issues.apache.org/jira/browse/IGNITE-18027")
    @Override
    public void testFailRebalance() throws Exception {
        super.testFailRebalance();
    }

    @Disabled("https://issues.apache.org/jira/browse/IGNITE-18027")
    @Override
    public void testStartRebalanceForClosedPartition() {
        super.testStartRebalanceForClosedPartition();
    }
}
