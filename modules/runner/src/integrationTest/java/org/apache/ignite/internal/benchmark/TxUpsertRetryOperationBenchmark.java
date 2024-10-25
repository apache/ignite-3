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

import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
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

@State(Scope.Benchmark)
@Fork(1)
@Threads(8)
@Warmup(iterations = 3, time = 5)
@Measurement(iterations = 3, time = 10)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class TxUpsertRetryOperationBenchmark extends AbstractMultiNodeBenchmark {
    private static final int KEYS_UPPER_BOUND = 100;

    private static RecordView<Tuple> recordView;

    private static IgniteTransactions transactions;

    private Set<UUID> txns = ConcurrentHashMap.newKeySet();

    private InternalTransaction tx;

    /**
     * Benchmark's entry point.
     */
    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(".*" + TxUpsertRetryOperationBenchmark.class.getSimpleName() + ".*")
                .build();

        new Runner(opt).run();
    }

    @Setup(Level.Invocation)
    public void startTx() {
        tx = (InternalTransaction) transactions.begin();
        txns.add(tx.id());
    }

    @TearDown(Level.Invocation)
    public void finishTx() {
        try {
            tx.commit();
        } catch (MismatchingTransactionOutcomeException ex) {

        } finally {
            txns.remove(tx.id());
        }
    }

    @Setup(Level.Trial)
    public void setup() {
        recordView = publicIgnite.tables().table(TABLE_NAME).recordView();
        transactions = publicIgnite.transactions();
    }

    @TearDown(Level.Trial)
    public void checkTxnsCompleted() {
        assert txns.isEmpty();
    }

    @Benchmark
    public void doTx() {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        Transaction tx = transactions.begin();

        int from = random.nextInt(KEYS_UPPER_BOUND);
        int to = from;
        while (to == from) {
            to = random.nextInt(KEYS_UPPER_BOUND);
        }

        try {
            float amountFrom = recordView.get(tx, Tuple.create().set("id", from)).value("amount");
            float amountTo = recordView.get(tx, Tuple.create().set("id", to)).value("amount");

            recordView.upsert(tx, Tuple.create().set("id", from).set("amount", amountFrom - 10));
            recordView.upsert(tx, Tuple.create().set("id", to).set("amount", amountTo + 10));
        } catch (Exception e) {
            System.out.println("qqq " + e);

            boolean rolledBack = true;
            try {
                tx.rollback();
            } catch (MismatchingTransactionOutcomeException ex) {
                rolledBack = false;
            }
        }
    }
}
