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

import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.sql.IgniteSql;
import org.apache.ignite.sql.ResultSet;
import org.apache.ignite.sql.SqlRow;
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
 * Benchmark that runs sql queries via embedded client on clusters of different size.
 */
@State(Scope.Benchmark)
@Fork(1)
@Threads(1)
@Warmup(iterations = 10, time = 2)
@Measurement(iterations = 20, time = 2)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@SuppressWarnings({"WeakerAccess", "unused"})
public class UpgradeRowBenchmark extends AbstractMultiNodeBenchmark {
    private static final int TABLE_SIZE = 300_000;

    private IgniteSql sql;

    @Param({"1"})
    private int clusterSize;

    /** Fills the table with data. */
    @Setup
    public void setUp() throws IOException {
        populateTable(TABLE_NAME, TABLE_SIZE, 1_000);

        sql = publicIgnite.sql();

        try (ResultSet<SqlRow> rs = sql.execute(format("ALTER TABLE {} ADD COLUMN new_col INT DEFAULT 42", TABLE_NAME));) {
            while (rs.hasNext()) {
                rs.next();
            }
        }
    }

    /** Benchmark that measures performance of `SELECT *` query over entire table. */
    @Benchmark
    public void selectAll(Blackhole bh) {
        try (var rs = sql.execute("SELECT * FROM usertable")) {
            while (rs.hasNext()) {
                bh.consume(rs.next());
            }
        }
    }

    /** Benchmark that measures performance of `SELECT` query with projection over entire table. */
    @Benchmark
    public void selectAllProjected(Blackhole bh) {
        try (var rs = sql.execute("SELECT field1, field2, field3 FROM usertable")) {
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
                .include(".*" + UpgradeRowBenchmark.class.getSimpleName() + ".*")
                .build();

        new Runner(opt).run();
    }

    @Override
    protected int nodes() {
        return clusterSize;
    }
}
