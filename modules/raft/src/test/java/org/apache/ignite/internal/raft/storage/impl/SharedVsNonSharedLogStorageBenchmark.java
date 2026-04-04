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

import static java.util.stream.Collectors.toList;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.util.IgniteUtils.deleteIfExists;
import static org.hamcrest.MatcherAssert.assertThat;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.IntStream;
import org.apache.ignite.internal.manager.ComponentContext;
import org.apache.ignite.internal.raft.storage.LogStorageManager;
import org.apache.ignite.raft.jraft.Lifecycle;
import org.apache.ignite.raft.jraft.conf.ConfigurationManager;
import org.apache.ignite.raft.jraft.entity.LogEntry;
import org.apache.ignite.raft.jraft.entity.codec.v1.LogEntryV1CodecFactory;
import org.apache.ignite.raft.jraft.option.LogStorageOptions;
import org.apache.ignite.raft.jraft.option.RaftOptions;
import org.apache.ignite.raft.jraft.storage.LogStorage;
import org.apache.ignite.raft.jraft.storage.impl.RocksDBLogStorage;
import org.apache.ignite.raft.jraft.test.TestUtils;
import org.apache.ignite.raft.jraft.util.Utils;

/**
 * Benchmark for shared versus non-shared log storage.
 */
public class SharedVsNonSharedLogStorageBenchmark {

    private ExecutorService svc = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors() * 2);

    private final List<LogStorage> logStorages;

    private final int logSize;

    private final int totalLogs;

    private final int batchSize;

    private SharedVsNonSharedLogStorageBenchmark(List<LogStorage> logStorages, int logSize, int totalLogs, int batchSize) {
        this.logStorages = logStorages;
        this.logSize = logSize;
        this.totalLogs = totalLogs;
        this.batchSize = batchSize;
    }

    private void write(final int batchSize, final int logSize, final int totalLogs) {
        var futs = logStorages.stream().map(storage -> {
            return svc.submit(() -> {
                List<LogEntry> entries = new ArrayList<>(batchSize);
                for (int i = 0; i < totalLogs; i += batchSize) {
                    for (int j = i; j < i + batchSize; j++) {
                        entries.add(TestUtils.mockEntry(j, j, logSize));
                    }
                    int ret = storage.appendEntries(entries);
                    if (ret != batchSize) {
                        System.err.println("Fatal error: write failures, expect " + batchSize + ", but was " + ret);
                        System.exit(1);
                    }
                    entries.clear(); // Reuse it.
                }
            });
        }).collect(toList());

        futs.forEach(future -> {
            try {
                future.get();
            } catch (InterruptedException | ExecutionException e) {
                throw new RuntimeException("BOOM");
            }
        });
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
        var futs = logStorages.stream().map(storage -> {
            return svc.submit(() -> {
                for (int i = 0; i < totalLogs; i++) {
                    LogEntry log = storage.getEntry(i);
                    assertNotNull(log);
                    assertEquals(i, log.getId().getIndex());
                    assertEquals(i, log.getId().getTerm());
                    assertEquals(logSize, log.getData().remaining());
                }
            });
        }).collect(toList());

        futs.forEach(future -> {
            try {
                future.get();
            } catch (InterruptedException | ExecutionException e) {
                throw new RuntimeException("BOOM");
            }
        });
    }

    private void report(final String op, final long cost) {
        System.out.println("Test " + op + ":");
        System.out.println("  Store count                 :" + this.logStorages.size());
        System.out.println("  Log number                  :" + this.totalLogs);
        System.out.println("  Log Size                    :" + this.logSize);
        System.out.println("  Batch Size                  :" + this.batchSize);
        System.out.println("  Total size (per storage)    :" + (long) this.totalLogs * this.logSize);
        System.out.println("  Cost time(s)                :" + cost / 1000);
    }

    private void doTest() {
        System.out.println("Begin test...");
        {
            System.out.println("Warm up...");
            write(10, 64, 10000);
            read(64, 10000);
        }

        System.out.println("Start test...");
        {
            long start = Utils.monotonicMs();
            write(this.batchSize, this.logSize, this.totalLogs);
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

        svc.shutdown();
    }

    /** Run benchmark. */
    public static void main(final String[] args) throws Exception {
        int batchSize = 100;
        int logSize = 16 * 1024;
        int totalLogs = 30 * 1024;

        int groups = 50;

        String randomUuid = UUID.randomUUID().toString();
        List<String> grps = IntStream.range(0, groups).mapToObj(cnt -> randomUuid + "_part_" + cnt).collect(toList());

        testShared(batchSize, logSize, totalLogs, grps);
        testIsolated(batchSize, logSize, totalLogs, grps);
    }

    private static void testShared(int batchSize, int logSize, int totalLogs, List<String> grps) throws Exception {
        System.out.println(">>> Testing shared");

        Path benchmarkPath = Files.createTempDirectory("storage_benchmark_shared");
        String testPath = benchmarkPath.toString();

        System.out.println("Test log storage path: " + testPath);

        LogStorageManager provider = new DefaultLogStorageManager(benchmarkPath);
        assertThat(provider.startAsync(new ComponentContext()), willCompleteSuccessfully());

        List<LogStorage> sharedStorages = grps.stream()
                .map(grp -> {
                    LogStorage storage = provider.createLogStorage(grp, new RaftOptions());

                    LogStorageOptions opts = new LogStorageOptions();
                    opts.setConfigurationManager(new ConfigurationManager());
                    opts.setLogEntryCodecFactory(LogEntryV1CodecFactory.getInstance());

                    storage.init(opts);

                    return storage;
                }).collect(toList());

        new SharedVsNonSharedLogStorageBenchmark(sharedStorages, logSize, totalLogs, batchSize).doTest();

        sharedStorages.forEach(Lifecycle::shutdown);
        assertThat(provider.stopAsync(new ComponentContext()), willCompleteSuccessfully());

        deleteIfExists(benchmarkPath);
    }

    private static void testIsolated(int batchSize, int logSize, int totalLogs, List<String> grps) throws IOException {
        System.out.println(">>> Testing isolated");

        Path benchmarkPath = Files.createTempDirectory("storage_benchmark_isolated");
        String testPath = benchmarkPath.toString();

        System.out.println("Test log storage path: " + testPath);

        List<LogStorage> isolatedStorages = grps.stream()
                .map(grp -> {
                    var storage = new RocksDBLogStorage(benchmarkPath.resolve(grp).toString(), new RaftOptions());

                    LogStorageOptions opts = new LogStorageOptions();
                    opts.setConfigurationManager(new ConfigurationManager());
                    opts.setLogEntryCodecFactory(LogEntryV1CodecFactory.getInstance());

                    storage.init(opts);

                    return storage;
                }).collect(toList());

        new SharedVsNonSharedLogStorageBenchmark(isolatedStorages, logSize, totalLogs, batchSize).doTest();

        isolatedStorages.forEach(Lifecycle::shutdown);

        deleteIfExists(benchmarkPath);
    }
}
