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

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.internal.sql.engine.util.tpch.TpchHelper;
import org.apache.ignite.sql.Session;
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
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

/**
 * Benchmark that runs sql queries from TPC-H suite via embedded client.
 */
@State(Scope.Benchmark)
@Fork(1)
@Threads(1)
@Warmup(iterations = 10, time = 2)
@Measurement(iterations = 20, time = 2)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@SuppressWarnings({"WeakerAccess", "unused"})
public class TpchBenchmark extends AbstractMultiNodeBenchmark {
    private final String[] tablesToInit = {
            "customer", "lineitem", "nation", "orders",
            "part", "partsupp", "region", "supplier"
    };

    @Param({
            "1", "2", "3", "4", "5", "6", "7", "8", "8v", "9", "10", "11", "12", "12v",
            "13", "14", "14v", "15", "16", "17", "18", "19", "20", "21", "22"
    })
    private String queryId;

    private Session session;
    private String queryString;

    /** Initializes a schema and fills tables with data. */
    @Setup
    public void setUp() throws IOException {
        session = clusterNode.sql().createSession();

        System.out.println("Going to create schema...");
        session.executeScript(TpchHelper.getSchemaDefinitionScript());
        System.out.println("Done");

        for (String tableName : tablesToInit) {
            fillTable(session, tableName);
        }

        queryString = TpchHelper.getQuery(queryId);
    }

    /** Benchmark that measures performance of queries from TPC-H suite. */
    @Benchmark
    public void run(Blackhole bh) {
        try (var rs = session.execute(null, queryString)) {
            while (rs.hasNext()) {
                bh.consume(rs.next());
            }
        }
    }

    /**
     * Benchmark's entry point.
     */
    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(".*" + TpchBenchmark.class.getSimpleName() + ".*")
                .build();

        new Runner(opt).run();
    }

    private static void fillTable(Session session, String tableName) {
        System.out.println("Going to fill table \"" + tableName + "\"...");
        long start = System.nanoTime();
        session.execute(null, TpchHelper.getDmlForTable(tableName));
        System.out.println("Done in " + Duration.ofNanos(System.nanoTime() - start));
    }
}


