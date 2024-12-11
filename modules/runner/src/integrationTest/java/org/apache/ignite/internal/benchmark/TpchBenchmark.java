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

import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.internal.sql.engine.util.TpcTable;
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
public class TpchBenchmark extends AbstractTpcBenchmark {
    /*
        Minimal configuration of this benchmark requires specifying pathToDataset. Latest known location
        of dataset is https://github.com/cmu-db/benchbase/tree/main/data/tpch-sf0.01 for scale factor 0.01
        and https://github.com/cmu-db/benchbase/tree/main/data/tpch-sf0.1 for scale factor 0.1. Dataset
        is set of CSV files with name `{$tableName}.tbl` per each table and character `|` as separator.

        By default, cluster's work directory will be created as a temporary folder. This implies,
        that all data generated by benchmark will be cleared automatically. However, this also implies
        that cluster will be recreated on EVERY RUN. Given there are 25 queries and 2 different modes
        (fsync on/off), it results in 50 schema initialization and data upload cycles. To initialize
        cluster once and then reuse it state override `AbstractMultiNodeBenchmark.workDir()` method.
        Don't forget to clear that directory afterwards.
     */

    @Override
    TpcTable[] tablesToInit() {
        return TpchTables.values();
    }

    @Override
    Path pathToDataset() {
        throw new RuntimeException("Provide path to directory containing <table_name>.tbl files");
    }

    @Param({
            "1", "2", "3", "4", "5", "6", "7", "8", "8v", "9", "10", "11", "12", "12v",
            "13", "14", "14v", "15", "16", "17", "18", "19", "20", "21", "22"
    })
    private String queryId;

    private String queryString;

    /** Initializes a query string. */
    @Setup
    public void setUp() throws Exception {
        try {
            queryString = TpchHelper.getQuery(queryId);
        } catch (Exception e) {
            nodeTearDown();

            throw e;
        }
    }

    /** Benchmark that measures performance of queries from TPC-H suite. */
    @Benchmark
    public void run(Blackhole bh) {
        try (var rs = sql.execute(null, queryString)) {
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
}
