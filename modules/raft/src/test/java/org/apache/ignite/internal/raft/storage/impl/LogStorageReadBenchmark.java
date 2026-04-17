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

package org.apache.ignite.internal.raft.storage.impl;

import static org.apache.ignite.internal.raft.storage.impl.LogStorageBenchmarkUtils.PEER_INDEX;
import static org.apache.ignite.internal.raft.storage.impl.LogStorageBenchmarkUtils.ROCKSDB;
import static org.apache.ignite.internal.raft.storage.impl.LogStorageBenchmarkUtils.SEGSTORE;
import static org.apache.ignite.internal.raft.storage.impl.LogStorageBenchmarkUtils.STARTING_ENTRIES;
import static org.apache.ignite.internal.raft.storage.impl.LogStorageBenchmarkUtils.createLogStorageManager;
import static org.apache.ignite.internal.raft.storage.impl.LogStorageBenchmarkUtils.seedGroup;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.internal.manager.ComponentContext;
import org.apache.ignite.internal.raft.storage.LogStorageManager;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.raft.jraft.entity.LogEntry;
import org.apache.ignite.raft.jraft.storage.LogStorage;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.ThreadParams;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

/**
 * JMH read throughput benchmark for {@link LogStorage} implementations.
 *
 * <p>Each thread operates on its own {@link LogStorage} instance (simulating independent raft groups),
 * so the benchmark measures how well the underlying storage manager handles concurrent reads from multiple groups.
 * The seeded working set fits in the page cache, so this measures the warm-cache path.
 *
 * <p>Storage implementation is selected via the {@code storageType} parameter.
 *
 * <p>Thread count is configured via the {@code -t} JMH flag.
 */
@Fork(1)
@State(Scope.Benchmark)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 2, time = 5)
@Measurement(iterations = 20, time = 10)
@Threads(4)
public class LogStorageReadBenchmark {
    private static final int READ_OBJECT_ID = 43;

    /** Batch size used to seed the storage. Reads do not depend on it, so it is fixed. */
    private static final int SEED_BATCH_SIZE = 100;

    /** Entry payload size used to seed the storage. */
    private static final int SEED_LOG_SIZE = 1024;

    /** Reads do not need durable writes during seeding. */
    private static final boolean SEED_FSYNC = false;

    @Param({SEGSTORE, ROCKSDB})
    private String storageType;

    private LogStorageManager logStorageManager;

    private Path testPath;

    private byte[] data;

    /** Setup method. */
    @Setup(Level.Trial)
    public void setUp() throws IOException {
        testPath = Files.createTempDirectory("log-storage-benchmark");

        data = new byte[SEED_LOG_SIZE];
        ThreadLocalRandom.current().nextBytes(data);

        logStorageManager = createLogStorageManager(storageType, testPath, SEED_FSYNC);
        logStorageManager.startAsync(new ComponentContext()).join();
    }

    /** Tear down method. */
    @TearDown(Level.Trial)
    public void tearDown() {
        logStorageManager.stopAsync(new ComponentContext()).join();
        IgniteUtils.deleteIfExists(testPath);
    }

    /** Per-thread state for {@link #getEntry}. */
    @State(Scope.Thread)
    public static class ReadState {
        LogStorage logStorage;

        long nextReadIndex;

        /** Setup method. */
        @Setup(Level.Trial)
        public void setUp(LogStorageReadBenchmark benchmark, ThreadParams threadParams) {
            String groupId = READ_OBJECT_ID + "_part_" + threadParams.getThreadIndex() + "-" + PEER_INDEX;
            logStorage = seedGroup(benchmark.logStorageManager, groupId, SEED_FSYNC, SEED_BATCH_SIZE, benchmark.data);
        }

        /** Tear down method. */
        @TearDown(Level.Trial)
        public void tearDown() {
            logStorage.shutdown();
        }
    }

    /** Reads a log entry by index. Working set fits in the page cache, so this measures the warm-cache path. */
    @Benchmark
    public LogEntry getEntry(ReadState state) {
        long index = state.nextReadIndex++ % STARTING_ENTRIES;
        return state.logStorage.getEntry(index);
    }

    /** Runs the read benchmark with all configured parameter combinations. */
    public static void main(String[] args) throws RunnerException {
        Options opts = new OptionsBuilder()
                .include(LogStorageReadBenchmark.class.getSimpleName())
                .build();
        new Runner(opts).run();
    }
}
