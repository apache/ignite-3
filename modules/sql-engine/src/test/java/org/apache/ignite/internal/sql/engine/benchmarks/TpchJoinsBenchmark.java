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

import com.google.common.io.CharStreams;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.internal.sql.engine.framework.TestBuilders;
import org.apache.ignite.internal.sql.engine.framework.TestCluster;
import org.apache.ignite.internal.sql.engine.framework.TestNode;
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
 * {@code TPC-H} based benchmark.
 */
@Warmup(iterations = 20, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 20, time = 1, timeUnit = TimeUnit.SECONDS)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Fork(3)
@State(Scope.Benchmark)
public class TpchJoinsBenchmark {

    // The numeric query id or <numeric_id>v for variant queries.
    @Param({
            "1", "2", "3", "4", "5", "6", "7", "8", "8v", "9", "10", "11", "12", "12v",
            "13", "14", "14v", "15", "16", "17", "18", "19", "20", "21", "22"
    })
    private String queryId;

    private TestCluster testCluster;

    private TestNode gatewayNode;

    private String queryString;

    /** Starts the cluster and prepares the plan of the query. */
    @Setup
    public void setUp() {
        var clusterBuilder = TestBuilders.cluster().nodes("N1");
        TpchSchema.registerTables(clusterBuilder, 1, 10);

        testCluster = clusterBuilder.build();

        testCluster.start();
        gatewayNode = testCluster.node("N1");

        // variant query ends with "v"
        if (queryId.endsWith("v")) {
            String qId = queryId.substring(0, queryId.length() - 1);
            queryString = loadQuery(Integer.parseInt(qId), true);
        } else {
            queryString = loadQuery(Integer.parseInt(queryId), false);
        }
    }

    /** Stops the cluster. */
    @TearDown
    public void tearDown() throws Exception {
        testCluster.stop();
    }

    /**
     * Runs {@code TPC-H query}.
     */
    @Benchmark
    public void runQuery(Blackhole bh) {
        bh.consume(gatewayNode.prepare(queryString));
    }

    /**
     * Runs the benchmark.
     *
     * @param args args
     * @throws Exception if something goes wrong
     */
    public static void main(String[] args) throws Exception {
        Options build = new OptionsBuilder()
                //.addProfiler("gc")
                .include(TpchJoinsBenchmark.class.getName())
                .build();

        new Runner(build).run();
    }

    private static String loadQuery(int id, boolean variant){
        if (variant) {
            var variantQueryFile = String.format("tpch/variant_q%d.sql", id);
            return loadFromResource(variantQueryFile);
        } else {
            var queryFile = String.format("tpch/q%s.sql", id);
            return loadFromResource(queryFile);
        }
    }

    private static String loadFromResource(String resource) {
        try (InputStream is = TpchJoinsBenchmark.class.getClassLoader().getResourceAsStream(resource)) {
            if (is == null) {
                throw new IllegalArgumentException("Resource does not exist: " + resource);
            }
            try (InputStreamReader reader = new InputStreamReader(is, StandardCharsets.UTF_8)) {
                return CharStreams.toString(reader);
            }
        } catch (IOException e) {
            throw new UncheckedIOException("I/O operation failed: " + resource, e);
        }
    }
}
