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

import java.util.concurrent.TimeUnit;
import org.apache.ignite.internal.sql.engine.framework.TestBuilders;
import org.apache.ignite.internal.sql.engine.framework.TestCluster;
import org.apache.ignite.internal.sql.engine.framework.TestNode;
import org.apache.ignite.internal.sql.engine.sql.ParsedResult;
import org.apache.ignite.internal.sql.engine.sql.ParserServiceImpl;
import org.apache.ignite.internal.sql.engine.util.tpch.TpchHelper;
import org.apache.ignite.internal.sql.engine.util.tpch.TpchTables;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

/**
 * Benchmarks derived from <a href="http://www.tpc.org/tpch/">TPC-H</a>.
 */
@Warmup(iterations = 10, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 10, time = 1, timeUnit = TimeUnit.SECONDS)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Fork(1)
@State(Scope.Benchmark)
public class TpchPrepareBenchmark {

    /**
     * Identifiers of TPC-H queries. See {@link TpchHelper#getQuery(String)}.
     */
    @Param({
            "1", "2", "3", "4", "5", "6", "7", "8", "8v", "9", "10", "11", "12", "12v",
            "13", "14", "14v", "15", "16", "17", "18", "19", "20", "21", "22"
    })
    private String queryId;

    private TestCluster testCluster;

    private TestNode gatewayNode;

    private ParsedResult parsedResult;

    /** Starts the cluster and prepares the plan of the query. */
    @Setup
    public void setUp() {
        testCluster = TestBuilders.cluster().nodes("N1").build();

        testCluster.start();

        gatewayNode = testCluster.node("N1");

        for (TpchTables table : TpchTables.values()) {
            gatewayNode.initSchema(table.ddlScript());
        }

        String query = TpchHelper.getQuery(queryId);
        parsedResult = new ParserServiceImpl().parse(query);
    }

    /** Stops the cluster. */
    @TearDown
    public void tearDown() throws Exception {
        testCluster.stop();
    }

    /**
     * Benchmark that measures the time it takes to prepare a {@code TPC-H query}.
     *
     * <p>The result includes the time to complete validation and planning.
     */
    @Benchmark
    public void prepareQuery(Blackhole bh) {
        bh.consume(gatewayNode.prepare(parsedResult));
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
                .include(TpchPrepareBenchmark.class.getName())
                .build();

        new Runner(build).run();
    }
}
