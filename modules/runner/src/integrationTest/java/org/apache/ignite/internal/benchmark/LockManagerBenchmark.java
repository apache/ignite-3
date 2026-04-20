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

import static org.apache.ignite.internal.tx.impl.HeapLockManager.DEFAULT_SLOTS;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.internal.TestHybridClock;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.tx.DeadlockPreventionPolicy;
import org.apache.ignite.internal.tx.LockKey;
import org.apache.ignite.internal.tx.LockManager;
import org.apache.ignite.internal.tx.LockMode;
import org.apache.ignite.internal.tx.impl.HeapLockManager;
import org.apache.ignite.internal.tx.impl.TransactionIdGenerator;
import org.apache.ignite.internal.tx.impl.VolatileTxStateMetaStorage;
import org.apache.ignite.internal.tx.impl.WaitDieDeadlockPreventionPolicy;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Group;
import org.openjdk.jmh.annotations.GroupThreads;
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
    private LockManager noOpLockManager;
    private TransactionIdGenerator generator;
    private HybridClock clock;

    /**
     * Initializes session and statement.
     */
    @Setup
    public void setUp() {
        VolatileTxStateMetaStorage txStateVolatileStorage = VolatileTxStateMetaStorage.createStarted();
        lockManager = new HeapLockManager(DEFAULT_SLOTS, txStateVolatileStorage);
        lockManager.start(new WaitDieDeadlockPreventionPolicy());

        VolatileTxStateMetaStorage noOpTxStateStorage = VolatileTxStateMetaStorage.createStarted();
        noOpLockManager = new HeapLockManager(DEFAULT_SLOTS, noOpTxStateStorage);
        noOpLockManager.start(DeadlockPreventionPolicy.NO_OP);

        generator = new TransactionIdGenerator(0);
        clock = new TestHybridClock(() -> 0L);
    }

    /**
     * Closes resources.
     */
    @TearDown
    public void tearDown() throws Exception {
        if (!lockManager.isEmpty()) {
            throw new AssertionError("Invalid lockManager state");
        }
        if (!noOpLockManager.isEmpty()) {
            throw new AssertionError("Invalid noOpLockManager state");
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

    /** Shared key for the contention benchmarks. */
    private static final LockKey SHARED_KEY = new LockKey(0, new RowId(0, new UUID(0, -1L)));

    /** Shared key for the release-wakeup benchmark. */
    private static final LockKey RELEASE_WAKEUP_KEY = new LockKey(0, new RowId(0, new UUID(0, -2L)));

    /**
     * Contended S-mode acquire/release on a shared key from many threads.
     * Stresses the synchronized section of the lock slot and the {@code isWaiterReadyToNotify} path on the acquire side.
     */
    @Benchmark
    @Warmup(iterations = 2, time = 3)
    @Measurement(iterations = 3, time = 5)
    @Threads(16)
    public void contendedSharedKeyS() {
        UUID txId = generator.transactionIdFor(clock.now());
        lockManager.acquire(txId, SHARED_KEY, LockMode.S).join();
        lockManager.releaseAll(txId);
    }

    /**
     * Mixed X/S workload on a shared key: one X-writer forces S-readers to queue with pending intents, so every X release
     * triggers {@code unlockCompatibleWaiters} over a batch of waiting S-readers. Uses {@link DeadlockPreventionPolicy#NO_OP}
     * so waiters actually block instead of aborting on conflict.
     */
    @Benchmark
    @Warmup(iterations = 2, time = 3)
    @Measurement(iterations = 3, time = 5)
    @Group("releaseWakeupMixed")
    @GroupThreads(1)
    public void releaseWakeupMixedX() {
        UUID txId = generator.transactionIdFor(clock.now());
        noOpLockManager.acquire(txId, RELEASE_WAKEUP_KEY, LockMode.X).join();
        noOpLockManager.releaseAll(txId);
    }

    /**
     * S-reader side of {@link #releaseWakeupMixedX}.
     */
    @Benchmark
    @Warmup(iterations = 2, time = 3)
    @Measurement(iterations = 3, time = 5)
    @Group("releaseWakeupMixed")
    @GroupThreads(15)
    public void releaseWakeupMixedS() {
        UUID txId = generator.transactionIdFor(clock.now());
        noOpLockManager.acquire(txId, RELEASE_WAKEUP_KEY, LockMode.S).join();
        noOpLockManager.releaseAll(txId);
    }

    /**
     * Benchmark's entry point.
     */
    public static void main(String[] args) throws RunnerException {
        // TODO: https://issues.apache.org/jira/browse/IGNITE-28128 JVM args.
        Options opt = new OptionsBuilder()
                .include(".*" + LockManagerBenchmark.class.getSimpleName() + ".*")
                .forks(1)
                .mode(Mode.AverageTime)
                .build();

        new Runner(opt).run();
    }
}
