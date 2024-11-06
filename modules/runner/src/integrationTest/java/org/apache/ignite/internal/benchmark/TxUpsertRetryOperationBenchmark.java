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

import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.table.RecordView;
import org.apache.ignite.table.Tuple;
import org.apache.ignite.tx.IgniteTransactions;
import org.apache.ignite.tx.MismatchingTransactionOutcomeException;
import org.apache.ignite.tx.Transaction;
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
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

/**
 * Benchmark that tests lock conflicts on concurrent upserts.
 */
@State(Scope.Benchmark)
@Fork(1)
@Threads(8)
@Warmup(iterations = 3, time = 10)
@Measurement(iterations = 2, time = 30)
@BenchmarkMode({Mode.AverageTime, Mode.Throughput})
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class TxUpsertRetryOperationBenchmark extends AbstractMultiNodeBenchmark {
    private static final IgniteLogger LOG = Loggers.forClass(TxBalanceRetryOperationBenchmark.class);

    private static RecordView<Tuple> recordView;

    private static IgniteTransactions transactions;

    private Set<UUID> txns = ConcurrentHashMap.newKeySet();

    @Param({"false"})
    private boolean fsync;

    @Param({"100", "1000"})
    private int keysUpperBound;

    @Param({"waitTimeout", "replicaOperationRetry"})
    private String txRetryMode;

    @Override
    protected String clusterConfiguration() {
        if (txRetryMode.equals("waitTimeout")) {
            return "transaction: { deadlockPreventionPolicy: { waitTimeout: 1000, txIdComparator: NATURAL } },"
                    + "replication: { replicaOperationRetryInterval: 0 }";
        } else {
            assert txRetryMode.equals("replicaOperationRetry");

            return "transaction: { deadlockPreventionPolicy: { waitTimeout: 0, txIdComparator: NATURAL } },"
                    + "replication: { replicaOperationRetryInterval: 10 }";
        }
    }

    /**
     * Benchmark's entry point.
     */
    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(".*" + TxUpsertRetryOperationBenchmark.class.getSimpleName() + ".*")
                .build();

        new Runner(opt).run();
    }

    @Setup(Level.Trial)
    public void setup() {
        recordView = publicIgnite.tables().table(TABLE_NAME).recordView();
        transactions = publicIgnite.transactions();

        Transaction tx = transactions.begin();

        for (int i = 0; i < keysUpperBound; i++) {
            recordView.insert(tx, Tuple.create().set("id", i).set("amount", 1000.0f));
        }

        tx.commit();
    }

    @TearDown(Level.Trial)
    public void checkTxnsCompleted() {
        assert txns.isEmpty();
    }

    @Override
    protected void createTable(String tableName) {
        createTable(tableName,
                List.of(
                        "id int",
                        "amount float"
                ),
                List.of("id"),
                List.of()
        );
    }

    @Benchmark
    public void doTx(BenchmarkState state) {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        Transaction tx = state.tx;

        int key = random.nextInt(keysUpperBound);

        try {
            recordView.upsert(tx, Tuple.create().set("id", key).set("amount", random.nextInt(1000)));
        } catch (Exception e) {
            state.toBeRolledBack = true;
        }
    }

    @State(Scope.Thread)
    public static class BenchmarkState {
        InternalTransaction tx;
        boolean toBeRolledBack;

        @Setup(Level.Invocation)
        public void startTx(TxnCounters counters) {
            tx = (InternalTransaction) transactions.begin();
            toBeRolledBack = false;
            counters.txnCounter.incrementAndGet();
        }

        @TearDown(Level.Invocation)
        public void finishTx(TxnCounters counters) {
            if (toBeRolledBack) {
                try {
                    tx.rollback();
                } catch (MismatchingTransactionOutcomeException ex) {
                    // No-op.
                }

                counters.rollbackCounter.incrementAndGet();
            }
            try {
                tx.commit();
            } catch (MismatchingTransactionOutcomeException ex) {
                counters.rollbackCounter.incrementAndGet();
            }
        }
    }

    @State(Scope.Benchmark)
    public static class TxnCounters {
        AtomicInteger txnCounter = new AtomicInteger();
        AtomicInteger rollbackCounter = new AtomicInteger();
    }
}
