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

import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.ignite.internal.raft.storage.LogStorageFactory;
import org.apache.ignite.internal.raft.storage.logit.LogitLogStorageFactory;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.raft.jraft.conf.ConfigurationManager;
import org.apache.ignite.raft.jraft.entity.EnumOutter;
import org.apache.ignite.raft.jraft.entity.LogEntry;
import org.apache.ignite.raft.jraft.entity.LogId;
import org.apache.ignite.raft.jraft.entity.codec.v1.LogEntryV1CodecFactory;
import org.apache.ignite.raft.jraft.option.LogStorageOptions;
import org.apache.ignite.raft.jraft.option.RaftOptions;
import org.apache.ignite.raft.jraft.storage.LogStorage;
import org.apache.ignite.raft.jraft.storage.logit.option.StoreOptions;
import org.apache.ignite.raft.jraft.util.SystemPropertyUtil;
import org.apache.ignite.raft.jraft.util.Utils;

public class LogStorageBenchmark {
    private static final int WARMUP_LOG_ENTRIES = 10000;

    private final LogStorage logStorage;

    private final int logSize;

    private final int totalLogs;

    private final int batchSize;

    private final byte[] bytes;

    public LogStorageBenchmark(final LogStorage logStorage, final int logSize, final int totalLogs,
        final int batchSize) {
        super();
        this.logStorage = logStorage;
        this.logSize = logSize;
        this.totalLogs = totalLogs;
        this.batchSize = batchSize;
        this.bytes = new byte[logSize];
        ThreadLocalRandom.current().nextBytes(bytes);
    }

    private void write(final int batchSize, final int logSize, final int totalLogs, int offset) {
        List<LogEntry> entries = new ArrayList<>(batchSize);
        for (int i = offset; i < totalLogs; i += batchSize) {
            for (int j = i; j < i + batchSize; j++) {
                LogEntry entry = new LogEntry(EnumOutter.EntryType.ENTRY_TYPE_NO_OP);
                entry.setId(new LogId(j, j));
                entry.setData(ByteBuffer.wrap(bytes)); // Reuse the same data for benchmark purity.

                entries.add(entry);
            }
            int ret = this.logStorage.appendEntries(entries);
            if (ret != batchSize) {
                System.err.println("Fatal error: write failures, expect " + batchSize + ", but was " + ret);
                System.exit(1);
            }
            entries.clear(); //reuse it
        }
    }

    private static void assertNotNull(final Object obj) {
        if (obj == null) {
            System.err.println("Null object");
            System.exit(1);
        }
    }

    private static void assertEquals(final long x, final long y) {
        if (x != y) {
            System.err.println("Expect " + x + " but was " + y);
            System.exit(1);
        }
    }

    private void read(final int logSize, final int totalLogs) {
        for (int i = 0; i < totalLogs; i++) {
            LogEntry log = this.logStorage.getEntry(i);
            assertNotNull(log);
            assertEquals(i, log.getId().getIndex());
            assertEquals(i, log.getId().getTerm());
            assertEquals(logSize, log.getData().remaining());
        }
    }

    private void report(final String op, final long cost) {
        System.out.println("Test " + op + ":");
        System.out.println("  Log number      : " + this.totalLogs);
        System.out.println("  Log Size        : " + this.logSize);
        System.out.println("  Batch Size      : " + this.batchSize);
        System.out.println("  Cost time(s)    : " + cost / 1000.0f);
        System.out.println("  Total size      : " + (long) this.totalLogs * this.logSize);
        System.out.println("  Throughput(bps) : " + 1000L * this.totalLogs * this.logSize / cost);
        System.out.println("  Throughput(rps) : " + 1000L * this.totalLogs / cost);
    }

    private void doTest() {
        System.out.println("Begin test...");
        {
            System.out.println("Warm up...");
            write(this.batchSize, this.logSize, WARMUP_LOG_ENTRIES, 0);
            read(this.logSize, WARMUP_LOG_ENTRIES);
        }

        System.out.println("Start test...");
        {
            long start = Utils.monotonicMs();
            write(this.batchSize, this.logSize, this.totalLogs, WARMUP_LOG_ENTRIES);
            long cost = Utils.monotonicMs() - start;
            report("write", cost);
        }

        {
            long start = Utils.monotonicMs();
            read(this.logSize, this.totalLogs);
            long cost = Utils.monotonicMs() - start;
            report("read", cost);
        }
        System.out.println("Test done!");
    }

    public static void main(final String[] args) throws Exception {
        Path testPath = Paths.get(SystemPropertyUtil.get("user.dir"), "log_storage");
        IgniteUtils.deleteIfExists(testPath);

        System.out.println("Test log storage path: " + testPath);
        int batchSize = 100;
        int logSize = 16 * 1024;
        int totalLogs = 100 * 1024;

//        LogStorageFactory logStorageFactory = new DefaultLogStorageFactory(testPath);
        LogStorageFactory logStorageFactory = new LogitLogStorageFactory("test", new StoreOptions(), () -> testPath);
        assertThat(logStorageFactory.startAsync(), willCompleteSuccessfully());

        try {
            RaftOptions raftOptions = new RaftOptions();
            raftOptions.setSync(false);

            LogStorage logStorage = logStorageFactory.createLogStorage("test", raftOptions);

            LogStorageOptions opts = new LogStorageOptions();
            opts.setConfigurationManager(new ConfigurationManager());
            opts.setLogEntryCodecFactory(LogEntryV1CodecFactory.getInstance());
            logStorage.init(opts);

            try (AutoCloseable log = logStorage::shutdown) {
                new LogStorageBenchmark(logStorage, logSize, totalLogs, batchSize).doTest();
            }
        } finally {
            assertThat(logStorageFactory.stopAsync(), willCompleteSuccessfully());
        }
    }
}
