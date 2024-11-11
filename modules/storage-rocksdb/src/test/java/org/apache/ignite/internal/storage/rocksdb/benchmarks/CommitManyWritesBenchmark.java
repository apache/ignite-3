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

package org.apache.ignite.internal.storage.rocksdb.benchmarks;

import static org.apache.ignite.internal.util.IgniteUtils.capacity;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.IntStream;
import org.apache.ignite.configuration.ConfigurationValue;
import org.apache.ignite.configuration.NamedConfigurationTree;
import org.apache.ignite.configuration.NamedListView;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.BinaryRowImpl;
import org.apache.ignite.internal.storage.MvPartitionStorage;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.storage.configurations.StorageConfiguration;
import org.apache.ignite.internal.storage.engine.StorageTableDescriptor;
import org.apache.ignite.internal.storage.rocksdb.RocksDbMvPartitionStorage;
import org.apache.ignite.internal.storage.rocksdb.RocksDbStorageEngine;
import org.apache.ignite.internal.storage.rocksdb.RocksDbTableStorage;
import org.apache.ignite.internal.storage.rocksdb.configuration.schema.RocksDbProfileView;
import org.apache.ignite.internal.storage.rocksdb.configuration.schema.RocksDbStorageEngineConfiguration;
import org.apache.ignite.internal.tx.TransactionIds;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

/**
 * Benchmark for measuring the performance of {@link RocksDbMvPartitionStorage} in a scenario, when many entries are added in one big
 * transaction and are then committed.
 */
@Fork(1)
@Threads(5)
@State(Scope.Benchmark)
@BenchmarkMode(Mode.SingleShotTime)
public class CommitManyWritesBenchmark {
    private static final String STORAGE_PROFILE_NAME = "test";
    private static final int TABLE_ID = 1;
    private static final int NUM_PARTITIONS = 50;
    private static final int NUM_ROWS = 100_000;
    private static final int ROW_LENGTH = 10_000;

    private final HybridClock clock = new HybridClockImpl();

    private RocksDbStorageEngine storageEngine;

    private RocksDbTableStorage tableStorage;

    /** Setup method. */
    @Setup
    public void setUp() throws IOException {
        Path workDir = Files.createTempDirectory(CommitManyWritesBenchmark.class.getSimpleName());

        storageEngine = new RocksDbStorageEngine(
                "test",
                engineConfiguration(),
                storageConfiguration(),
                workDir,
                () -> {}
        );

        storageEngine.start();

        var tableDescriptor = new StorageTableDescriptor(TABLE_ID, NUM_PARTITIONS, STORAGE_PROFILE_NAME);

        tableStorage = storageEngine.createMvTable(tableDescriptor, indexId -> null);

        CompletableFuture<?>[] createFutures = IntStream.range(0, NUM_PARTITIONS)
                .mapToObj(tableStorage::createMvPartition)
                .toArray(CompletableFuture[]::new);

        CompletableFuture.allOf(createFutures).join();
    }

    /** Tear down method. */
    @TearDown
    public void tearDown() {
        tableStorage.destroy().join();

        storageEngine.stop();
    }

    private static int randomPartitionId() {
        return ThreadLocalRandom.current().nextInt(NUM_PARTITIONS);
    }

    private static Map<RowId, BinaryRow> randomRows(int partitionId) {
        ThreadLocalRandom random = ThreadLocalRandom.current();

        var rows = new HashMap<RowId, BinaryRow>(capacity(NUM_ROWS));

        for (int i = 0; i < NUM_ROWS; i++) {
            var rowId = new RowId(partitionId);

            BinaryRow row = randomRow(random);

            rows.put(rowId, row);
        }

        return rows;
    }

    private static BinaryRow randomRow(ThreadLocalRandom random) {
        ByteBuffer buffer = ByteBuffer.allocate(ROW_LENGTH);

        random.nextBytes(buffer.array());

        return new BinaryRowImpl(0, buffer);
    }

    /** Randomly generated rows for a partition. */
    @State(Scope.Thread)
    public static class DataToAddAndCommit {
        final int partitionId = randomPartitionId();

        final Map<RowId, BinaryRow> rows = randomRows(partitionId);
    }

    /** Randomly generated rows for a partition which has also been written to the storage. */
    @State(Scope.Thread)
    public static class DataToCommit {
        final int partitionId = randomPartitionId();

        final Map<RowId, BinaryRow> rows = randomRows(partitionId);

        /** Setup method. */
        @Setup
        public void setUp(CommitManyWritesBenchmark benchmark) {
            UUID txId = TransactionIds.transactionId(benchmark.clock.now(), 0);

            MvPartitionStorage partitionStorage = benchmark.tableStorage.getMvPartition(partitionId);

            partitionStorage.runConsistently(locker -> {
                rows.forEach((rowId, row) -> partitionStorage.addWrite(rowId, row, txId, TABLE_ID, partitionId));

                return null;
            });
        }
    }

    private static RocksDbStorageEngineConfiguration engineConfiguration() {
        RocksDbStorageEngineConfiguration config = mock(RocksDbStorageEngineConfiguration.class);

        ConfigurationValue<Integer> flushDelayMillis = mock(ConfigurationValue.class);

        when(flushDelayMillis.value()).thenReturn(100);

        when(config.flushDelayMillis()).thenReturn(flushDelayMillis);

        return config;
    }

    private static StorageConfiguration storageConfiguration() {
        StorageConfiguration config = mock(StorageConfiguration.class);

        NamedConfigurationTree profilesTree = mock(NamedConfigurationTree.class);
        NamedListView profilesView = mock(NamedListView.class);
        RocksDbProfileView rocksDbProfileView = mock(RocksDbProfileView.class);

        when(rocksDbProfileView.name()).thenReturn(STORAGE_PROFILE_NAME);
        when(rocksDbProfileView.size()).thenReturn(16777216L);
        when(rocksDbProfileView.writeBufferSize()).thenReturn(16777216L);

        when(config.profiles()).thenReturn(profilesTree);
        when(profilesTree.value()).thenReturn(profilesView);
        when(profilesView.iterator()).thenReturn(List.of(rocksDbProfileView).iterator());

        return config;
    }

    /** Benchmark for the combination of {@link MvPartitionStorage#addWrite} and {@link MvPartitionStorage#commitWrite} methods. */
    @Benchmark
    public void addAndCommitManyWrites(DataToAddAndCommit data) {
        MvPartitionStorage partitionStorage = tableStorage.getMvPartition(data.partitionId);

        UUID txId = TransactionIds.transactionId(clock.now(), 0);

        partitionStorage.runConsistently(locker -> {
            data.rows.forEach((rowId, row) -> partitionStorage.addWrite(rowId, row, txId, TABLE_ID, data.partitionId));

            return null;
        });

        HybridTimestamp commitTs = clock.now();

        partitionStorage.runConsistently(locker -> {
            data.rows.keySet().forEach(rowId -> partitionStorage.commitWrite(rowId, commitTs));

            return null;
        });
    }

    /** Benchmark for calling {@link MvPartitionStorage#addWrite} many times. */
    @Benchmark
    public void addManyWrites(DataToAddAndCommit data) {
        MvPartitionStorage partitionStorage = tableStorage.getMvPartition(data.partitionId);

        UUID txId = TransactionIds.transactionId(clock.now(), 0);

        partitionStorage.runConsistently(locker -> {
            data.rows.forEach((rowId, row) -> partitionStorage.addWrite(rowId, row, txId, TABLE_ID, data.partitionId));

            return null;
        });
    }

    /** Benchmark for calling {@link MvPartitionStorage#commitWrite} many times. */
    @Benchmark
    public void commitManyWrites(DataToCommit data) {
        MvPartitionStorage partitionStorage = tableStorage.getMvPartition(data.partitionId);

        HybridTimestamp commitTs = clock.now();

        partitionStorage.runConsistently(locker -> {
            data.rows.keySet().forEach(rowId -> partitionStorage.commitWrite(rowId, commitTs));

            return null;
        });
    }

    /** Main method. */
    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(CommitManyWritesBenchmark.class.getSimpleName())
                .build();

        new Runner(opt).run();
    }
}
