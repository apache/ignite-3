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

package org.apache.ignite.client;

import org.apache.ignite.Ignite;
import org.apache.ignite.client.fakes.FakeIgnite;
import org.apache.ignite.client.fakes.FakeIgniteTables;
import org.apache.ignite.table.RecordView;
import org.apache.ignite.table.Table;
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
 * <p>Results
 * Benchmark                   Mode  Cnt      Score      Error  Units
 * ClientPutGetBenchmark.get  thrpt    3  53183.778 ± 8726.747  ops/s
 */
@State(Scope.Benchmark)
public class ClientPutGetBenchmark {
    private static final String DEFAULT_TABLE = "default_test_table";

    private TestServer testServer;

    private Ignite ignite;

    private IgniteClient client;

    private Table table;

    private RecordView<Tuple> recordView;

    private Tuple key;

    @Setup
    public void init() {
        ignite = new FakeIgnite("server-1");
        ((FakeIgniteTables) ignite.tables()).createTable(DEFAULT_TABLE);

        testServer = new TestServer(10800, 10, 1000, ignite);

        client = IgniteClient.builder()
                .addresses("127.0.0.1:" + testServer.port())
                .build();

        table = client.tables().table(DEFAULT_TABLE);
        recordView = table.recordView();

        key = Tuple.create().set("id", 1L);
        recordView.upsert(null, Tuple.create().set("id", 1L).set("name", "John"));
    }

    @TearDown
    public void tearDown() throws Exception {
        client.close();
        testServer.close();
        ignite.close();
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
        Options opt = new OptionsBuilder()
                .include(ClientPutGetBenchmark.class.getSimpleName())
                .warmupIterations(3)
                .warmupTime(TimeValue.seconds(5))
                .measurementIterations(3)
                .measurementTime(TimeValue.seconds(5))
                .forks(1)
                .build();

        new Runner(opt).run();
    }
}
