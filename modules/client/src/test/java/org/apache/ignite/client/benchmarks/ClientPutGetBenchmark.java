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

package org.apache.ignite.client.benchmarks;

import io.netty.util.ResourceLeakDetector;
import io.netty.util.ResourceLeakDetector.Level;
import org.apache.ignite.Ignite;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.client.TestServer;
import org.apache.ignite.client.fakes.FakeIgnite;
import org.apache.ignite.client.fakes.FakeIgniteTables;
import org.apache.ignite.table.RecordView;
import org.apache.ignite.table.Tuple;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.TimeValue;

/**
 * Basic table benchmark.
 *
 * <p>Results on i9-12900H, openjdk 11.0.18, Ubuntu 22.04:
 * Benchmark                                                Mode  Cnt      Score       Error   Units
 * ClientPutGetBenchmark.get                               thrpt    3  49418.080 ± 17639.810   ops/s (before IGNITE-18899)
 * ClientPutGetBenchmark.get                               thrpt    3  50305.929 ± 13924.407   ops/s (after IGNITE-18899)
 * ClientPutGetBenchmark.get:·gc.alloc.rate                thrpt    3    666.616 ±  1242.450  MB/sec (before IGNITE-18899)
 * ClientPutGetBenchmark.get:·gc.alloc.rate                thrpt    3    480.666 ±   829.508  MB/sec (after IGNITE-18899)
 * ClientPutGetBenchmark.get:·gc.alloc.rate.norm           thrpt    3  15572.822 ± 25042.204    B/op (before IGNITE-18899)
 * ClientPutGetBenchmark.get:·gc.alloc.rate.norm           thrpt    3  11046.571 ± 21170.502    B/op (after IGNITE-18899)
 * ClientPutGetBenchmark.get:·gc.churn.G1_Eden_Space       thrpt    3    716.282 ±   529.130  MB/sec (before IGNITE-18899)
 * ClientPutGetBenchmark.get:·gc.churn.G1_Eden_Space       thrpt    3    519.416 ±   555.076  MB/sec (after IGNITE-18899)
 * ClientPutGetBenchmark.get:·gc.churn.G1_Eden_Space.norm  thrpt    3  16757.093 ± 17212.675    B/op (before IGNITE-18899)
 * ClientPutGetBenchmark.get:·gc.churn.G1_Eden_Space.norm  thrpt    3  11928.927 ± 13109.114    B/op (after IGNITE-18899)
 * ClientPutGetBenchmark.get:·gc.churn.G1_Old_Gen          thrpt    3      0.005 ±     0.046  MB/sec (before IGNITE-18899)
 * ClientPutGetBenchmark.get:·gc.churn.G1_Old_Gen          thrpt    3      0.001 ±     0.001  MB/sec (after IGNITE-18899)
 * ClientPutGetBenchmark.get:·gc.churn.G1_Old_Gen.norm     thrpt    3      0.113 ±     1.101    B/op (before IGNITE-18899)
 * ClientPutGetBenchmark.get:·gc.churn.G1_Old_Gen.norm     thrpt    3      0.025 ±     0.006    B/op (after IGNITE-18899)
 * ClientPutGetBenchmark.get:·gc.count                     thrpt    3     40.000              counts (before IGNITE-18899)
 * ClientPutGetBenchmark.get:·gc.count                     thrpt    3     29.000              counts (after IGNITE-18899)
 * ClientPutGetBenchmark.get:·gc.time                      thrpt    3     15.000                  ms (before IGNITE-18899)
 * ClientPutGetBenchmark.get:·gc.time                      thrpt    3     12.000                  ms (after IGNITE-18899)
 */
@State(Scope.Benchmark)
public class ClientPutGetBenchmark {
    private static final String DEFAULT_TABLE = "default_test_table";

    private TestServer testServer;

    private Ignite ignite;

    private IgniteClient client;

    private RecordView<Tuple> recordView;

    private Tuple key;

    /**
     * Init.
     */
    @Setup
    public void init() {
        ignite = new FakeIgnite("server-1");
        ((FakeIgniteTables) ignite.tables()).createTable(DEFAULT_TABLE);

        testServer = new TestServer(1000, ignite);

        client = IgniteClient.builder()
                .addresses("127.0.0.1:" + testServer.port())
                .build();

        key = Tuple.create().set("id", 1L);

        Tuple rec = Tuple.create()
                .set("id", 1L)
                .set("name", "John".repeat(1000));

        recordView = client.tables().table(DEFAULT_TABLE).recordView();
        recordView.upsert(null, rec);
    }

    /**
     * Tear down.
     */
    @TearDown
    public void tearDown() throws Exception {
        client.close();
        testServer.close();
    }

    /**
     * Get benchmark.
     */
    @Benchmark
    public void get() {
        recordView.get(null, key);
    }

    /**
     * Runner.
     *
     * @param args Arguments.
     * @throws RunnerException Exception.
     */
    public static void main(String[] args) throws RunnerException {
        ResourceLeakDetector.setLevel(Level.DISABLED);

        Options opt = new OptionsBuilder()
                .include(ClientPutGetBenchmark.class.getSimpleName())
                .addProfiler("gc")
                .warmupIterations(3)
                .warmupTime(TimeValue.seconds(5))
                .measurementIterations(3)
                .measurementTime(TimeValue.seconds(5))
                .forks(1)
                .build();

        new Runner(opt).run();
    }
}
