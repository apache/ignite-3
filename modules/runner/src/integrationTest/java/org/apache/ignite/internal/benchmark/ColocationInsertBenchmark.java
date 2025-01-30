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
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.table.KeyValueView;
import org.apache.ignite.table.Tuple;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

/**
 * This benchmark allows to measure inserting key value pairs via KeyValue API for tables that share the same distribution zone.
 * TODO: https://issues.apache.org/jira/browse/IGNITE-22522 remove this benchmark.
 */
@Fork(1)
@State(Scope.Benchmark)
public class ColocationInsertBenchmark extends AbstractColocationBenchmark {
    private final Tuple tuple = Tuple.create().set("company", "Apache");

    private final AtomicInteger counter = new AtomicInteger();

    /**
     * Measures throughput of key-value api for tables that share the same distribution zone.
     */
    @Benchmark
    @Threads(Threads.MAX)
    @Warmup(iterations = 5, time = 2)
    @Measurement(iterations = 10, time = 2)
    @BenchmarkMode(Throughput)
    @OutputTimeUnit(SECONDS)
    public void insertKeyValueApiThroughput() {
        doPut();
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
    public void insertKeyValueApiAverage() {
        doPut();
    }

    private void doPut() {
        int currentId = counter.getAndIncrement();
        int tableIdx = currentId % tableViews.size();

        KeyValueView<Tuple, Tuple> kvView = tableViews.get(tableIdx);

        publicIgnite.transactions().runInTransaction(tx -> {
            kvView.putAll(tx, Map.of(Tuple.create().set("id", currentId), tuple));
        });
    }

    /**
     * Benchmark's entry point.
     */
    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(".*" + ColocationInsertBenchmark.class.getSimpleName() + ".*")
                .build();

        new Runner(opt).run();
    }
}
