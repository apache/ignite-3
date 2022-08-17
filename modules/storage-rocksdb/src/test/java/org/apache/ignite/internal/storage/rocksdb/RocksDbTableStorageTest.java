/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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

import static org.apache.ignite.internal.storage.rocksdb.configuration.schema.RocksDbStorageEngineConfigurationSchema.DEFAULT_DATA_REGION_NAME;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willBe;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.sameInstance;

import java.nio.file.Path;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.ignite.configuration.schemas.store.UnknownDataStorageConfigurationSchema;
import org.apache.ignite.configuration.schemas.table.ConstantValueDefaultConfigurationSchema;
import org.apache.ignite.configuration.schemas.table.FunctionCallDefaultConfigurationSchema;
import org.apache.ignite.configuration.schemas.table.HashIndexConfigurationSchema;
import org.apache.ignite.configuration.schemas.table.NullValueDefaultConfigurationSchema;
import org.apache.ignite.configuration.schemas.table.TableConfiguration;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.storage.BaseMvStoragesTest;
import org.apache.ignite.internal.storage.MvPartitionStorage;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.storage.rocksdb.configuration.schema.RocksDbDataStorageChange;
import org.apache.ignite.internal.storage.rocksdb.configuration.schema.RocksDbDataStorageConfigurationSchema;
import org.apache.ignite.internal.storage.rocksdb.configuration.schema.RocksDbDataStorageView;
import org.apache.ignite.internal.storage.rocksdb.configuration.schema.RocksDbStorageEngineConfiguration;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.apache.ignite.internal.util.Cursor;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.lang.IgniteBiTuple;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Tests for the {@link RocksDbTableStorage}.
 */
@ExtendWith(WorkDirectoryExtension.class)
@ExtendWith(ConfigurationExtension.class)
public class RocksDbTableStorageTest extends BaseMvStoragesTest {
    private RocksDbStorageEngine engine;

    private RocksDbTableStorage storage;

    @BeforeEach
    public void setUp(
            @WorkDirectory Path workDir,
            @InjectConfiguration(
                    value = "mock {flushDelayMillis = 0, defaultRegion {size = 16536, writeBufferSize = 16536}}"
            ) RocksDbStorageEngineConfiguration rocksDbEngineConfig,
            @InjectConfiguration(
                    name = "table",
                    value = "mock.partitions = 512",
                    polymorphicExtensions = {
                            HashIndexConfigurationSchema.class,
                            UnknownDataStorageConfigurationSchema.class,
                            RocksDbDataStorageConfigurationSchema.class,
                            ConstantValueDefaultConfigurationSchema.class,
                            FunctionCallDefaultConfigurationSchema.class,
                            NullValueDefaultConfigurationSchema.class,
                    }
            ) TableConfiguration tableCfg
    ) throws Exception {
        CompletableFuture<Void> changeDataStorageFuture = tableCfg.dataStorage().change(c -> c.convert(RocksDbDataStorageChange.class));

        assertThat(changeDataStorageFuture, willBe(nullValue(Void.class)));

        assertThat(((RocksDbDataStorageView) tableCfg.dataStorage().value()).dataRegion(), equalTo(DEFAULT_DATA_REGION_NAME));

        engine = new RocksDbStorageEngine(rocksDbEngineConfig, workDir);

        engine.start();

        storage = engine.createMvTable(tableCfg);

        assertThat(storage, is(instanceOf(RocksDbTableStorage.class)));

        storage.start();
    }

    @AfterEach
    public void tearDown() throws Exception {
        IgniteUtils.closeAll(
                storage == null ? null : storage::stop,
                engine == null ? null : engine::stop
        );
    }

    /**
     * Tests that {@link RocksDbTableStorage#getMvPartition(int)} correctly returns an existing partition.
     */
    @Test
    void testCreatePartition() {
        MvPartitionStorage absentStorage = storage.getMvPartition(0);

        assertThat(absentStorage, is(nullValue()));

        MvPartitionStorage partitionStorage = storage.getOrCreateMvPartition(0);

        assertThat(partitionStorage, is(notNullValue()));

        var testData = binaryRow(new TestKey(1, "1"), new TestValue(10, "10"));

        partitionStorage.runConsistently(() -> {
            UUID txId = UUID.randomUUID();

            RowId rowId = partitionStorage.insert(testData, txId);

            assertThat(partitionStorage, is(sameInstance(storage.getMvPartition(0))));

            assertThat(unwrap(partitionStorage.read(rowId, txId)), is(equalTo(unwrap(testData))));

            return null;
        });
    }

    /**
     * Tests that partition data does not overlap.
     */
    @Test
    void testPartitionIndependence() throws Exception {
        MvPartitionStorage partitionStorage0 = storage.getOrCreateMvPartition(42);
        // Using a shifted ID value to test a multibyte scenario.
        MvPartitionStorage partitionStorage1 = storage.getOrCreateMvPartition(1 << 8);

        var testData0 = binaryRow(new TestKey(1, "1"), new TestValue(10, "10"));

        UUID txId = UUID.randomUUID();

        RowId rowId0 = partitionStorage0.runConsistently(() -> partitionStorage0.insert(testData0, txId));

        assertThat(unwrap(partitionStorage0.read(rowId0, txId)), is(equalTo(unwrap(testData0))));
        assertThat(partitionStorage1.read(rowId0, txId), is(nullValue()));

        var testData1 = binaryRow(new TestKey(2, "2"), new TestValue(20, "20"));

        RowId rowId1 = partitionStorage1.runConsistently(() -> partitionStorage1.insert(testData1, txId));

        assertThat(partitionStorage0.read(rowId1, txId), is(nullValue()));
        assertThat(unwrap(partitionStorage1.read(rowId1, txId)), is(equalTo(unwrap(testData1))));

        assertThat(toList(partitionStorage0.scan(row -> true, txId)), contains(unwrap(testData0)));
        assertThat(toList(partitionStorage1.scan(row -> true, txId)), contains(unwrap(testData1)));
    }

    private List<IgniteBiTuple<TestKey, TestValue>> toList(Cursor<BinaryRow> cursor) throws Exception {
        try (cursor) {
            return cursor.stream().map(RocksDbTableStorageTest::unwrap).collect(Collectors.toList());
        }
    }

    /**
     * Tests that dropping a partition does not remove extra data.
     */
    @Test
    void testDropPartition() {
        var testData = binaryRow(new TestKey(1, "1"), new TestValue(10, "10"));

        UUID txId = UUID.randomUUID();

        MvPartitionStorage partitionStorage0 = storage.getOrCreateMvPartition(42);

        RowId rowId0 = partitionStorage0.runConsistently(() -> partitionStorage0.insert(testData, txId));

        MvPartitionStorage partitionStorage1 = storage.getOrCreateMvPartition(1 << 8);

        RowId rowId1 = partitionStorage1.runConsistently(() -> partitionStorage1.insert(testData, txId));

        CompletableFuture<Void> destroyFuture = storage.destroyPartition(42);

        // Partition desctuction doesn't enforce flush.
        storage.scheduleFlush();

        assertThat(destroyFuture, willCompleteSuccessfully());

        assertThat(storage.getMvPartition(42), is(nullValue()));
        assertThat(storage.getOrCreateMvPartition(42).read(rowId0, txId), is(nullValue()));
        assertThat(unwrap(storage.getMvPartition(1 << 8).read(rowId1, txId)), is(equalTo(unwrap(testData))));
    }

    /**
     * Tests that restarting the storage does not result in data loss.
     */
    @Test
    void testRestart(
            @InjectConfiguration(
                    name = "table",
                    polymorphicExtensions = {HashIndexConfigurationSchema.class, RocksDbDataStorageConfigurationSchema.class}
            ) TableConfiguration tableCfg
    ) throws Exception {
        var testData = binaryRow(new TestKey(1, "1"), new TestValue(10, "10"));

        UUID txId = UUID.randomUUID();

        MvPartitionStorage partitionStorage0 = storage.getOrCreateMvPartition(0);

        RowId rowId0 = partitionStorage0.runConsistently(() -> partitionStorage0.insert(testData, txId));

        storage.stop();

        tableCfg.dataStorage().change(c -> c.convert(RocksDbDataStorageChange.class)).get(1, TimeUnit.SECONDS);

        storage = engine.createMvTable(tableCfg);

        storage.start();

        assertThat(storage.getMvPartition(0), is(notNullValue()));
        assertThat(storage.getMvPartition(1), is(nullValue()));
        assertThat(unwrap(storage.getMvPartition(0).read(rowId0, txId)), is(equalTo(unwrap(testData))));
    }

    @Test
    void storageAdvertisesItIsPersistent() {
        assertThat(storage.isVolatile(), is(false));
    }

    private static @Nullable IgniteBiTuple<TestKey, TestValue> unwrap(@Nullable BinaryRow binaryRow) {
        if (binaryRow == null) {
            return null;
        }

        return new IgniteBiTuple<>(key(binaryRow), value(binaryRow));
    }
}
