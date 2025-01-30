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

import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.openjdk.jmh.annotations.Mode.AverageTime;
import static org.openjdk.jmh.annotations.Mode.Throughput;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.table.KeyValueView;
import org.apache.ignite.table.Tuple;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

/**
 * This benchmark allows to measure reading key value pairs via KeyValue API for tables that share the same distribution zone.
 * TODO: https://issues.apache.org/jira/browse/IGNITE-22522 remove this benchmark.
 */
@Fork(1)
@State(Scope.Benchmark)
public class ColocationSelectBenchmark extends AbstractColocationBenchmark {
    private static final int TABLE_SIZE = 30_000;

    private final AtomicInteger counter = new AtomicInteger();

    /**
     * Fills the tables with data.
     */
    @Setup
    public void fillTables() {
        Tuple value = Tuple.create().set("company", "Apache");
        CompletableFuture[] futures = new CompletableFuture[tableViews.size()];

        for (int k = 0; k < tableCount(); ++k) {
            KeyValueView<Tuple, Tuple> view = tableViews.get(k);

            futures[k] = CompletableFuture.supplyAsync(() -> {
                for (int i = 0; i < TABLE_SIZE; i++) {
                    Tuple key = Tuple.create().set("id", i);

                    publicIgnite.transactions().runInTransaction(tx -> {
                        view.putAll(tx, Map.of(key, value));
                    });
                }

                return null;
            });
        }

        CompletableFuture.allOf(futures).join();
    }

    /**
     * Measures throughput of key-value api for tables that share the same distribution zone.
     */
    @Benchmark
    @Threads(Threads.MAX)
    @Warmup(iterations = 5, time = 2)
    @Measurement(iterations = 10, time = 2)
    @BenchmarkMode(Throughput)
    @OutputTimeUnit(SECONDS)
    public void getKeyValueApiThroughput(Blackhole hole) {
        doGet(hole);
    }

    /**
     * Measures average time of inserting a key-value pair for tables that share the same distribution zone.
     */
    @Benchmark
    @Threads(1)
    @Warmup(iterations = 5, time = 2)
    @Measurement(iterations = 10, time = 2)
    @BenchmarkMode(AverageTime)
    @OutputTimeUnit(MICROSECONDS)
    public void getKeyValueApiAverage(Blackhole hole) {
        doGet(hole);
    }

    private void doGet(Blackhole hole) {
        ThreadLocalRandom random = ThreadLocalRandom.current();

        int tableIdx = counter.getAndIncrement() % tableViews.size();

        KeyValueView<Tuple, Tuple> kvView = tableViews.get(tableIdx);

        Tuple value = publicIgnite.transactions().runInTransaction(tx -> {
            return kvView.get(tx, Tuple.create().set("id", random.nextInt(TABLE_SIZE)));
        });

        hole.consume(value);
    }

    /**
     * Benchmark's entry point.
     */
    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(".*" + ColocationSelectBenchmark.class.getSimpleName() + ".*")
                .build();

        new Runner(opt).run();
    }
}
