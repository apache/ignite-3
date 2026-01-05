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
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.sql.IgniteSql;
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
 * Benchmark that runs sql queries over system views to measure its performance.
 */
@State(Scope.Benchmark)
@Fork(1)
@Threads(1)
@Warmup(iterations = 10, time = 2)
@Measurement(iterations = 20, time = 2)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@SuppressWarnings({"WeakerAccess", "unused"})
public class SystemViewsBenchmark extends AbstractMultiNodeBenchmark {
    private IgniteSql sql;

    @Param("3")
    private int clusterSize;

    /** Fills the table with data. */
    @Setup
    public void setUp() throws IOException {
        sql = publicIgnite.sql();
    }

    /** Benchmark to measure performance of queries over TABLE-COLUMNS system view. */
    @Benchmark
    @OutputTimeUnit(TimeUnit.MICROSECONDS)
    public void getPrimaryKeys(FiveHundredTablesState state, Blackhole bh) {
        try (var rs = sql.execute(null, "SELECT column_name\n"
                + "FROM SYSTEM.table_columns\n"
                + "WHERE table_name = ?\n"
                + "AND pk_column_ordinal IS NOT NULL;", state.tableName())) {
            while (rs.hasNext()) {
                bh.consume(rs.next());
            }
        }
    }

    /** Benchmark to measure performance of queries over TABLE-COLUMNS system view. */
    @Benchmark
    @OutputTimeUnit(TimeUnit.MICROSECONDS)
    public void getColumnsType(FiveHundredTablesState state, Blackhole bh) {
        try (var rs = sql.execute(null, "SELECT column_name,column_type\n"
                + "FROM SYSTEM.table_columns\n"
                + "WHERE table_name = ?;", state.tableName())) {
            while (rs.hasNext()) {
                bh.consume(rs.next());
            }
        }
    }

    /** State that creates 500 tables with 10 columns each and 2 columns primary key. */
    @State(Scope.Benchmark)
    public static class FiveHundredTablesState {
        private static final int TABLES_COUNT = 500;

        /** Creates necessary tables. */
        @Setup
        public void setUp() throws IOException {
            IgniteSql sql = publicIgnite.sql();

            int columnsCount = 10;

            StringBuilder scriptBuilder = new StringBuilder(
                    "CREATE ZONE my_zone (PARTITIONS 1, REPLICAS 1) STORAGE PROFILES ['default'];"
            ).append("ALTER ZONE my_zone SET DEFAULT;");

            for (int t = 0; t < TABLES_COUNT; t++) {
                scriptBuilder.append("CREATE TABLE my_table_").append(t).append("(");

                for (int c = 0; c < columnsCount; c++) {
                    if (c > 0) {
                        scriptBuilder.append(", ");
                    }

                    scriptBuilder.append("c_").append(c).append(" INT");
                }

                scriptBuilder.append(", PRIMARY KEY (c_1, c_2));");
            }

            sql.executeScript(scriptBuilder.toString());
        }

        @SuppressWarnings("MethodMayBeStatic")
        String tableName() {
            return "MY_TABLE_" + ThreadLocalRandom.current().nextInt(TABLES_COUNT);
        }
    }

    /**
     * Benchmark's entry point.
     */
    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(".*" + SystemViewsBenchmark.class.getSimpleName() + ".*")
                .build();

        new Runner(opt).run();
    }

    @Override
    protected void createDistributionZoneOnStartup() {
        // NO-OP
    }

    @Override
    protected void createTablesOnStartup() {
        // NO-OP
    }

    @Override
    protected int nodes() {
        return clusterSize;
    }
}
