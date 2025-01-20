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

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.openjdk.jmh.annotations.Mode.AverageTime;

import java.util.concurrent.atomic.AtomicInteger;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

/**
 * This benchmark measures creating a new table in the default distribution zone.
 */
@Fork(1)
@State(Scope.Benchmark)
public class CreatingTableBenchmark extends AbstractMultiNodeBenchmark {
    @Param({"3"})
    private int clusterSize;

    @Param({"1", "8"})
    private int partitionCount;

    @Param({"1", "3"})
    private int replicaCount;

    /** Tables counter. */
    private AtomicInteger cnt = new AtomicInteger();

    @Override
    protected int nodes() {
        return clusterSize;
    }

    @Override
    protected int partitionCount() {
        return partitionCount;
    }

    @Override
    protected int replicaCount() {
        return replicaCount;
    }

    @Override
    protected void createDefaultTableOnStartup() {
        // There is no need to create a table on start-up.
    }

    @Benchmark
    @Threads(1)
    @Warmup(iterations = 5, time = 5)
    @Measurement(iterations = 5, time = 5)
    @BenchmarkMode(AverageTime)
    @OutputTimeUnit(MILLISECONDS)
    public void createTableInDefaultZone() {
        String tableName = "table_test_" + cnt.incrementAndGet();

        createTable(tableName);

        populateTable(tableName, 1, 1);
    }

    /**
     * Benchmark's entry point.
     */
    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(".*" + CreatingTableBenchmark.class.getSimpleName() + ".*")
                .build();

        new Runner(opt).run();
    }
}
