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

package org.apache.ignite.raft.jraft.storage.impl;

import static org.apache.ignite.internal.raft.configuration.LogStorageConfigurationSchema.DEFAULT_MAX_CHECKPOINT_QUEUE_SIZE;
import static org.apache.ignite.internal.raft.configuration.LogStorageConfigurationSchema.DEFAULT_SEGMENT_FILE_SIZE_BYTES;
import static org.apache.ignite.internal.raft.configuration.LogStorageConfigurationSchema.DEFAULT_SOFT_LOG_SIZE_LIMIT_BYTES;
import static org.apache.ignite.internal.raft.configuration.LogStorageConfigurationSchema.UNSPECIFIED_MAX_LOG_ENTRY_SIZE;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.internal.failure.NoOpFailureManager;
import org.apache.ignite.internal.manager.ComponentContext;
import org.apache.ignite.internal.raft.configuration.LogStorageConfiguration;
import org.apache.ignite.internal.raft.configuration.LogStorageView;
import org.apache.ignite.internal.raft.storage.LogStorageManager;
import org.apache.ignite.internal.raft.storage.impl.DefaultLogStorageManager;
import org.apache.ignite.internal.raft.storage.impl.RocksDbLogStorageOptions;
import org.apache.ignite.internal.raft.storage.segstore.SegmentLogStorageManager;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.raft.jraft.conf.ConfigurationManager;
import org.apache.ignite.raft.jraft.entity.EnumOutter;
import org.apache.ignite.raft.jraft.entity.LogEntry;
import org.apache.ignite.raft.jraft.entity.LogId;
import org.apache.ignite.raft.jraft.entity.codec.v1.LogEntryV1CodecFactory;
import org.apache.ignite.raft.jraft.option.LogStorageOptions;
import org.apache.ignite.raft.jraft.option.RaftOptions;
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
 * JMH benchmark for {@link LogStorage} implementations.
 *
 * <p>Each thread operates on its own {@link LogStorage} instance (simulating independent raft groups),
 * so the benchmark measures how well the underlying storage manager handles concurrent access from multiple groups.
 * Throughput results are reported in {@code batches/sec}; multiply by {@code batchSize} for entries/sec.
 *
 * <p>Storage implementation is selected via the {@code storageType} parameter. To add a new implementation,
 * add a case to {@link #createLogStorageManager}.
 *
 * <p>Thread count is configured via the {@code -t} JMH flag.
 *
 * <p>{@link #main} runs {@code appendEntries} with all parameter combinations and {@code getEntry} only with
 * {@code storageType} varying — read throughput is independent of {@code batchSize}, {@code logSize} and {@code fsync}.
 */
@Fork(1)
@State(Scope.Benchmark)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 2, time = 5)
@Measurement(iterations = 20, time = 10)
@Threads(4)
public class LogStorageBenchmark {
    private static final String ROCKSDB = "rocksdb";

    private static final String SEGSTORE = "segstore";

    /** Pre-written during setup; extra values truncated after each invocation. */
    private static final int STARTING_ENTRIES = 30_000;

    /** Matches {@code DisruptorConfigurationSchema.DEFAULT_LOG_MANAGER_STRIPES_COUNT} used in production. */
    private static final int SEGSTORE_STRIPES = 4;

    private static final long TERM = 1;

    private static final int PEER_INDEX = 1;

    private static final int APPEND_OBJECT_ID = 42;

    private static final int READ_OBJECT_ID = 43;

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

    private static LogStorageManager createLogStorageManager(String storageType, Path path, boolean fsync) throws IOException {
        switch (storageType) {
            case ROCKSDB:
                return new DefaultLogStorageManager("test", "test", path, fsync, RocksDbLogStorageOptions.defaults());

            case SEGSTORE:
                return new SegmentLogStorageManager(
                        "test",
                        "test",
                        path,
                        SEGSTORE_STRIPES,
                        new NoOpFailureManager(),
                        fsync,
                        mockSegstoreConfig()
                );

            default:
                throw new IllegalArgumentException("Unsupported storage type: " + storageType);
        }
    }

    private static LogStorageConfiguration mockSegstoreConfig() {
        LogStorageConfiguration config = mock(LogStorageConfiguration.class);
        LogStorageView view = mock(LogStorageView.class);

        when(config.value()).thenReturn(view);
        when(view.maxCheckpointQueueSize()).thenReturn(DEFAULT_MAX_CHECKPOINT_QUEUE_SIZE);
        when(view.segmentFileSizeBytes()).thenReturn((long) DEFAULT_SEGMENT_FILE_SIZE_BYTES);
        when(view.maxLogEntrySizeBytes()).thenReturn(UNSPECIFIED_MAX_LOG_ENTRY_SIZE);
        when(view.softLogSizeLimitBytes()).thenReturn(DEFAULT_SOFT_LOG_SIZE_LIMIT_BYTES);

        return config;
    }

    private LogStorage createAndInitLogStorage(String groupId) {
        RaftOptions raftOptions = new RaftOptions();
        raftOptions.setSync(fsync);

        LogStorage logStorage = logStorageManager.createLogStorage(groupId, raftOptions);

        LogStorageOptions opts = new LogStorageOptions();
        opts.setConfigurationManager(new ConfigurationManager());
        opts.setLogEntryCodecFactory(LogEntryV1CodecFactory.getInstance());
        logStorage.init(opts);

        return logStorage;
    }

    private static LogStorage seedGroup(LogStorageBenchmark benchmark, String groupId) {
        LogStorage logStorage = benchmark.createAndInitLogStorage(groupId);

        List<LogEntry> batch = new ArrayList<>(benchmark.batchSize);
        for (int i = 0; i < STARTING_ENTRIES; i += benchmark.batchSize) {
            batch.clear();
            int end = Math.min(i + benchmark.batchSize, STARTING_ENTRIES);
            for (int j = i; j < end; j++) {
                LogEntry entry = new LogEntry(EnumOutter.EntryType.ENTRY_TYPE_NO_OP);
                entry.setId(new LogId(j, TERM));
                entry.setData(ByteBuffer.wrap(benchmark.data));
                batch.add(entry);
            }
            logStorage.appendEntries(batch);
        }

        return logStorage;
    }

    /** Per-thread state for {@link #appendEntries}. */
    @State(Scope.Thread)
    public static class AppendState {
        LogStorage logStorage;

        long firstKept;

        long nextIndex;

        /** Setup method. */
        @Setup(Level.Trial)
        public void setUp(LogStorageBenchmark benchmark, ThreadParams threadParams) {
            String groupId = APPEND_OBJECT_ID + "_part_" + threadParams.getThreadIndex() + "-" + PEER_INDEX;
            logStorage = seedGroup(benchmark, groupId);
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

    /** Per-thread state for {@link #getEntry}. */
    @State(Scope.Thread)
    public static class ReadState {
        LogStorage logStorage;

        long nextReadIndex;

        /** Setup method. */
        @Setup(Level.Trial)
        public void setUp(LogStorageBenchmark benchmark, ThreadParams threadParams) {
            String groupId = READ_OBJECT_ID + "_part_" + threadParams.getThreadIndex() + "-" + PEER_INDEX;
            logStorage = seedGroup(benchmark, groupId);
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

    /** Reads a log entry by index. Working set fits in the page cache, so this measures the warm-cache path. */
    @Benchmark
    public LogEntry getEntry(ReadState state) {
        long index = state.nextReadIndex++ % STARTING_ENTRIES;
        return state.logStorage.getEntry(index);
    }

    public static void main(String[] args) throws RunnerException {
        // appendEntries varies all params.
        Options writeOpts = new OptionsBuilder()
                .include(LogStorageBenchmark.class.getSimpleName() + "\\.appendEntries")
                .build();
        new Runner(writeOpts).run();

        Options readOpts = new OptionsBuilder()
                .include(LogStorageBenchmark.class.getSimpleName() + "\\.getEntry")
                .param("batchSize", "100")
                .param("logSize", "1024")
                .param("fsync", "false")
                .build();
        new Runner(readOpts).run();
    }
}
