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

package org.apache.ignite.internal.benchmark;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.internal.TestHybridClock;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.tx.LockKey;
import org.apache.ignite.internal.tx.LockManager;
import org.apache.ignite.internal.tx.LockMode;
import org.apache.ignite.internal.tx.impl.HeapLockManager;
import org.apache.ignite.internal.tx.impl.TransactionIdGenerator;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

/**
 * Benchmark lock manager.
 */
@State(Scope.Benchmark)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class LockManagerBenchmark {
    private LockManager lockManager;
    private TransactionIdGenerator generator;
    private HybridClock clock;

    /**
     * Initializes session and statement.
     */
    @Setup
    public void setUp() {
        lockManager = new HeapLockManager();
        generator = new TransactionIdGenerator(0);
        clock = new TestHybridClock(() -> 0L);
    }

    /**
     * Closes resources.
     */
    @TearDown
    public void tearDown() throws Exception {
        if (!lockManager.isEmpty()) {
            throw new AssertionError("Invalid lock manager state");
        }
    }

    /**
     * Concurrent active transactions.
     */
    @Param({"200"})
    private int concTxns;

    /**
     * Take and release some locks.
     */
    @Benchmark
    @Warmup(iterations = 1, time = 3)
    @Measurement(iterations = 1, time = 10)
    public void lockCommit() {
        List<UUID> ids = new ArrayList<>(concTxns);

        int c = 0;

        for (int i = 0; i < concTxns; i++) {
            UUID txId = generator.transactionIdFor(clock.now());
            ids.add(txId);
            lockManager.acquire(txId, new LockKey(0, new RowId(0, new UUID(0, c++))), LockMode.X).join();
        }

        for (UUID id : ids) {
            lockManager.releaseAll(id);
        }
    }

    /**
     * Benchmark's entry point.
     */
    public static void main(String[] args) throws RunnerException {
        // TODO JVM args
        Options opt = new OptionsBuilder()
                .include(".*" + LockManagerBenchmark.class.getSimpleName() + ".*")
                .forks(1)
                .threads(1)
                .mode(Mode.AverageTime)
                .build();

        new Runner(opt).run();
    }
}
