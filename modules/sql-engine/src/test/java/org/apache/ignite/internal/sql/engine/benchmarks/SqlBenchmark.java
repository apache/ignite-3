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

package org.apache.ignite.internal.sql.engine.benchmarks;

import static org.apache.ignite.internal.testframework.IgniteTestUtils.await;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.ignite.internal.sql.engine.framework.DataProvider;
import org.apache.ignite.internal.sql.engine.framework.TestBuilders;
import org.apache.ignite.internal.sql.engine.framework.TestCluster;
import org.apache.ignite.internal.sql.engine.framework.TestNode;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

/**
 * A micro-benchmark of sql execution.
 */
@Warmup(iterations = 20, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 20, time = 1, timeUnit = TimeUnit.SECONDS)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Fork(1)
@Threads(1)
@State(Scope.Benchmark)
public class SqlBenchmark {
    private final DataProvider<Object[]> dataProvider = DataProvider.fromRow(
            new Object[]{42, UUID.randomUUID().toString()}, 3_333
    );

    // @formatter:off
    private final TestCluster cluster = TestBuilders.cluster()
            .nodes("N1", "N2", "N3")
            .build();
    // @formatter:on

    private final TestNode gatewayNode = cluster.node("N1");

    /** Starts the cluster and prepares the plan of the query. */
    @Setup
    public void setUp() {
        cluster.start();

        //noinspection ConcatenationWithEmptyString
        cluster.node("N1").initSchema(""
                + "CREATE ZONE test_zone WITH partitions=3, storage_profiles='Default';"
                + "CREATE TABLE t1 (id INT PRIMARY KEY, val VARCHAR(64)) ZONE test_zone");

        cluster.setAssignmentsProvider("T1", (partitionCount, b) -> {
            assert partitionCount == 3;

            return Stream.of("N1", "N2", "N3")
                    .map(List::of)
                    .collect(Collectors.toList());
        });
        cluster.setDataProvider("T1", TestBuilders.tableScan(dataProvider));
    }

    /** Stops the cluster. */
    @TearDown
    public void tearDown() throws Exception {
        cluster.stop();
    }

    /** Very simple test to measure performance of minimal possible distributed query. */
    @Benchmark
    public void selectAllSimple(Blackhole bh) {
        for (var row : await(gatewayNode.executeQuery("SELECT * FROM t1").requestNextAsync(10_000)).items()) {
            bh.consume(row);
        }
    }

    /**
     * Runs the benchmark.
     *
     * @param args args
     * @throws Exception if something goes wrong
     */
    public static void main(String[] args) throws Exception {
        Options build = new OptionsBuilder()
                // .addProfiler("gc")
                .include(SqlBenchmark.class.getName())
                .build();

        new Runner(build).run();
    }
}
