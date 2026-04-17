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
import static org.apache.ignite.internal.raft.storage.impl.LogStorageBenchmarkUtils.TERM;
import static org.apache.ignite.internal.raft.storage.impl.LogStorageBenchmarkUtils.createLogStorageManager;
import static org.apache.ignite.internal.raft.storage.impl.LogStorageBenchmarkUtils.seedGroup;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.internal.manager.ComponentContext;
import org.apache.ignite.internal.raft.storage.LogStorageManager;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.raft.jraft.entity.EnumOutter;
import org.apache.ignite.raft.jraft.entity.LogEntry;
import org.apache.ignite.raft.jraft.entity.LogId;
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
 * JMH write throughput benchmark for {@link LogStorage} implementations.
 *
 * <p>Each thread operates on its own {@link LogStorage} instance (simulating independent raft groups),
 * so the benchmark measures how well the underlying storage manager handles concurrent appends from multiple groups.
 * Throughput results are reported in {@code batches/sec}; multiply by {@code batchSize} for entries/sec.
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
public class LogStorageWriteBenchmark {
    private static final int APPEND_OBJECT_ID = 42;

    @Param({SEGSTORE, ROCKSDB})
    private String storageType;

    @Param({"100"})
    private int batchSize;

    @Param({"1024"})
    private int logSize;

    @Param({"false", "true"})
    private boolean fsync;

    private LogStorageManager logStorageManager;

    private Path testPath;

    private byte[] data;

    /** Setup method. */
    @Setup(Level.Trial)
    public void setUp() throws IOException {
        testPath = Files.createTempDirectory("log-storage-benchmark");

        data = new byte[logSize];
        ThreadLocalRandom.current().nextBytes(data);

        logStorageManager = createLogStorageManager(storageType, testPath, fsync);
        logStorageManager.startAsync(new ComponentContext()).join();
    }

    /** Tear down method. */
    @TearDown(Level.Trial)
    public void tearDown() {
        logStorageManager.stopAsync(new ComponentContext()).join();
        IgniteUtils.deleteIfExists(testPath);
    }

    /** Per-thread state for {@link #appendEntries}. */
    @State(Scope.Thread)
    public static class AppendState {
        LogStorage logStorage;

        long firstKept;

        long nextIndex;

        /** Setup method. */
        @Setup(Level.Trial)
        public void setUp(LogStorageWriteBenchmark benchmark, ThreadParams threadParams) {
            String groupId = APPEND_OBJECT_ID + "_part_" + threadParams.getThreadIndex() + "-" + PEER_INDEX;
            logStorage = seedGroup(benchmark.logStorageManager, groupId, benchmark.fsync, benchmark.batchSize, benchmark.data);
            nextIndex = STARTING_ENTRIES;
        }

        /** Restores the live window to the baseline. */
        @TearDown(Level.Invocation)
        public void resetWindow() {
            if (nextIndex - firstKept > STARTING_ENTRIES) {
                long newFirstKept = nextIndex - STARTING_ENTRIES;
                logStorage.truncatePrefix(newFirstKept);
                firstKept = newFirstKept;
            }
        }

        /** Tear down method. */
        @TearDown(Level.Trial)
        public void tearDown() {
            logStorage.shutdown();
        }
    }

    /** Benchmarks appending a batch of log entries. */
    @Benchmark
    public int appendEntries(AppendState state) {
        List<LogEntry> entries = new ArrayList<>(batchSize);
        for (int i = 0; i < batchSize; i++) {
            LogEntry entry = new LogEntry(EnumOutter.EntryType.ENTRY_TYPE_NO_OP);
            entry.setId(new LogId(state.nextIndex + i, TERM));
            entry.setData(ByteBuffer.wrap(data));
            entries.add(entry);
        }
        int ret = state.logStorage.appendEntries(entries);
        state.nextIndex += batchSize;
        return ret;
    }

    /** Runs the write benchmark with all configured parameter combinations. */
    public static void main(String[] args) throws RunnerException {
        Options opts = new OptionsBuilder()
                .include(LogStorageWriteBenchmark.class.getSimpleName())
                .build();
        new Runner(opts).run();
    }
}
