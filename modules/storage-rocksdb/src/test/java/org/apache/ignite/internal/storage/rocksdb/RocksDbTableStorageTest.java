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
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.ignite.configuration.schemas.store.UnknownDataStorageConfigurationSchema;
import org.apache.ignite.configuration.schemas.table.HashIndexConfigurationSchema;
import org.apache.ignite.configuration.schemas.table.TableConfiguration;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.storage.PartitionStorage;
import org.apache.ignite.internal.storage.basic.SimpleDataRow;
import org.apache.ignite.internal.storage.engine.StorageEngine;
import org.apache.ignite.internal.storage.engine.TableStorage;
import org.apache.ignite.internal.storage.rocksdb.configuration.schema.RocksDbDataStorageChange;
import org.apache.ignite.internal.storage.rocksdb.configuration.schema.RocksDbDataStorageConfigurationSchema;
import org.apache.ignite.internal.storage.rocksdb.configuration.schema.RocksDbDataStorageView;
import org.apache.ignite.internal.storage.rocksdb.configuration.schema.RocksDbStorageEngineConfiguration;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.apache.ignite.internal.util.Cursor;
import org.apache.ignite.internal.util.IgniteUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Tests for the {@link RocksDbTableStorage}.
 */
@ExtendWith(WorkDirectoryExtension.class)
@ExtendWith(ConfigurationExtension.class)
public class RocksDbTableStorageTest {
    @WorkDirectory
    private Path workDir;

    private StorageEngine engine;

    private TableStorage storage;

    @BeforeEach
    public void setUp(
            @InjectConfiguration RocksDbStorageEngineConfiguration rocksDbEngineConfig,
            @InjectConfiguration(
                    name = "table",
                    polymorphicExtensions = {
                            HashIndexConfigurationSchema.class,
                            UnknownDataStorageConfigurationSchema.class,
                            RocksDbDataStorageConfigurationSchema.class
                    }
            ) TableConfiguration tableCfg
    ) throws Exception {
        CompletableFuture<Void> changeDataStorageFuture = tableCfg.dataStorage().change(c -> c.convert(RocksDbDataStorageChange.class));

        assertThat(changeDataStorageFuture, willBe(nullValue(Void.class)));

        assertThat(((RocksDbDataStorageView) tableCfg.dataStorage().value()).dataRegion(), equalTo(DEFAULT_DATA_REGION_NAME));

        CompletableFuture<Void> changeEngineFuture = rocksDbEngineConfig.defaultRegion()
                .change(c -> c.changeSize(16 * 1024).changeWriteBufferSize(16 * 1024));

        assertThat(changeEngineFuture, willBe(nullValue(Void.class)));

        changeEngineFuture = tableCfg.change(cfg -> cfg.changePartitions(512));

        assertThat(changeEngineFuture, willBe(nullValue(Void.class)));

        engine = new RocksDbStorageEngine(rocksDbEngineConfig, workDir);

        engine.start();

        storage = engine.createTable(tableCfg);

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
     * Tests that {@link RocksDbTableStorage#getPartition} correctly returns an existing partition.
     */
    @Test
    void testCreatePartition() {
        PartitionStorage partitionStorage = storage.getPartition(0);

        assertThat(partitionStorage, is(nullValue()));

        partitionStorage = storage.getOrCreatePartition(0);

        assertThat(partitionStorage, is(notNullValue()));

        var testData = new SimpleDataRow("foo".getBytes(StandardCharsets.UTF_8), "bar".getBytes(StandardCharsets.UTF_8));

        partitionStorage.write(testData);

        partitionStorage = storage.getPartition(0);

        assertThat(partitionStorage, is(notNullValue()));

        assertThat(partitionStorage.read(testData), is(equalTo(testData)));
    }

    /**
     * Tests that partition data does not overlap.
     */
    @Test
    void testPartitionIndependence() throws Exception {
        PartitionStorage partitionStorage0 = storage.getOrCreatePartition(42);
        // using a shifted ID value to test a multi-byte scenario
        PartitionStorage partitionStorage1 = storage.getOrCreatePartition(1 << 8);

        var testData = new SimpleDataRow("foo".getBytes(StandardCharsets.UTF_8), "bar".getBytes(StandardCharsets.UTF_8));

        partitionStorage0.write(testData);

        assertThat(partitionStorage0.read(testData), is(equalTo(testData)));
        assertThat(partitionStorage1.read(testData), is(nullValue()));

        var testData2 = new SimpleDataRow("baz".getBytes(StandardCharsets.UTF_8), "quux".getBytes(StandardCharsets.UTF_8));

        partitionStorage1.write(testData2);

        assertThat(partitionStorage0.read(testData2), is(nullValue()));
        assertThat(partitionStorage1.read(testData2), is(equalTo(testData2)));

        assertThat(toList(partitionStorage0.scan(row -> true)), contains(testData));
        assertThat(toList(partitionStorage1.scan(row -> true)), contains(testData2));
    }

    private static <T> List<T> toList(Cursor<T> cursor) throws Exception {
        try (cursor) {
            return cursor.stream().collect(Collectors.toList());
        }
    }

    /**
     * Tests that dropping a partition does not remove extra data.
     */
    @Test
    void testDropPartition() {
        var testData = new SimpleDataRow("foo".getBytes(StandardCharsets.UTF_8), "bar".getBytes(StandardCharsets.UTF_8));

        storage.getOrCreatePartition(42).write(testData);
        storage.getOrCreatePartition(1 << 8).write(testData);

        storage.dropPartition(42);

        assertThat(storage.getPartition(42), is(nullValue()));
        assertThat(storage.getOrCreatePartition(42).read(testData), is(nullValue()));
        assertThat(storage.getPartition(1 << 8).read(testData), is(equalTo(testData)));
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
        var testData = new SimpleDataRow("foo".getBytes(StandardCharsets.UTF_8), "bar".getBytes(StandardCharsets.UTF_8));

        storage.getOrCreatePartition(0).write(testData);

        storage.stop();

        tableCfg.dataStorage().change(c -> c.convert(RocksDbDataStorageChange.class)).get(1, TimeUnit.SECONDS);

        storage = engine.createTable(tableCfg);

        storage.start();

        assertThat(storage.getPartition(0), is(notNullValue()));
        assertThat(storage.getPartition(1), is(nullValue()));
        assertThat(storage.getPartition(0).read(testData), is(equalTo(testData)));
    }

    /**
     * Tests that restoring a snapshot clears all previous data.
     */
    @Test
    void testRestoreSnapshot() {
        PartitionStorage partitionStorage = storage.getOrCreatePartition(0);

        var testData1 = new SimpleDataRow("foo".getBytes(StandardCharsets.UTF_8), "bar".getBytes(StandardCharsets.UTF_8));
        var testData2 = new SimpleDataRow("baz".getBytes(StandardCharsets.UTF_8), "quux".getBytes(StandardCharsets.UTF_8));

        Path snapshotDir = workDir.resolve("snapshot");

        partitionStorage.write(testData1);

        assertThat(partitionStorage.snapshot(snapshotDir), willBe(nullValue(Void.class)));

        partitionStorage.write(testData2);

        partitionStorage.restoreSnapshot(snapshotDir);

        assertThat(partitionStorage.read(testData1), is(testData1));
        assertThat(partitionStorage.read(testData2), is(nullValue()));
    }

    /**
     * Tests that loading snapshots for one partition does not influence data in another.
     */
    @Test
    void testSnapshotIndependence() {
        PartitionStorage partitionStorage1 = storage.getOrCreatePartition(0);
        PartitionStorage partitionStorage2 = storage.getOrCreatePartition(1);

        var testData1 = new SimpleDataRow("foo".getBytes(StandardCharsets.UTF_8), "bar".getBytes(StandardCharsets.UTF_8));
        var testData2 = new SimpleDataRow("baz".getBytes(StandardCharsets.UTF_8), "quux".getBytes(StandardCharsets.UTF_8));

        partitionStorage1.writeAll(List.of(testData1, testData2));
        partitionStorage2.writeAll(List.of(testData1, testData2));

        // take a snapshot of the first partition
        assertThat(partitionStorage1.snapshot(workDir.resolve("snapshot")), willBe(nullValue(Void.class)));

        // remove all data from partitions
        partitionStorage1.removeAll(List.of(testData1, testData2));
        partitionStorage2.removeAll(List.of(testData1, testData2));

        assertThat(partitionStorage1.readAll(List.of(testData1, testData2)), is(empty()));
        assertThat(partitionStorage2.readAll(List.of(testData1, testData2)), is(empty()));

        // restore a snapshot and check that only the first partition has data
        partitionStorage1.restoreSnapshot(workDir.resolve("snapshot"));

        assertThat(partitionStorage1.readAll(List.of(testData1, testData2)), containsInAnyOrder(testData1, testData2));
        assertThat(partitionStorage2.readAll(List.of(testData1, testData2)), is(empty()));
    }

    /**
     * Tests that loading snapshots for one partition does not influence data in another when overwriting existing keys.
     */
    @Test
    void testSnapshotIndependenceOverwritesKeys() {
        PartitionStorage partitionStorage1 = storage.getOrCreatePartition(0);
        PartitionStorage partitionStorage2 = storage.getOrCreatePartition(1);

        var testData = new SimpleDataRow("foo".getBytes(StandardCharsets.UTF_8), "bar".getBytes(StandardCharsets.UTF_8));

        partitionStorage1.write(testData);
        partitionStorage2.write(testData);

        assertThat(partitionStorage2.snapshot(workDir.resolve("snapshot")), willBe(nullValue(Void.class)));

        // key is intentionally the same as testData
        var overwriteData = new SimpleDataRow(testData.keyBytes(), "new value".getBytes(StandardCharsets.UTF_8));

        // test that snapshot restoration overrides existing keys
        partitionStorage2.write(overwriteData);

        partitionStorage2.restoreSnapshot(workDir.resolve("snapshot"));

        assertThat(partitionStorage1.read(overwriteData), is(testData));
        assertThat(partitionStorage2.read(overwriteData), is(testData));
    }
}
