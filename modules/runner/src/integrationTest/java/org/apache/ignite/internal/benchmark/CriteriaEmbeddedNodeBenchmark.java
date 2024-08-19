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

import static org.apache.ignite.table.criteria.Criteria.columnValue;
import static org.apache.ignite.table.criteria.Criteria.equalTo;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.lang.Cursor;
import org.apache.ignite.sql.IgniteSql;
import org.apache.ignite.sql.ResultSet;
import org.apache.ignite.sql.SqlRow;
import org.apache.ignite.table.RecordView;
import org.apache.ignite.table.Tuple;
import org.apache.ignite.table.criteria.Criteria;
import org.jetbrains.annotations.Nullable;
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
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

/**
 * Benchmark for criteria operation, comparing KV, SQL APIs over embedded node.
 */
@State(Scope.Benchmark)
@Fork(1)
@Threads(1)
@Warmup(iterations = 10, time = 2)
@Measurement(iterations = 20, time = 2)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@SuppressWarnings({"WeakerAccess", "unused"})
public class CriteriaEmbeddedNodeBenchmark extends AbstractMultiNodeBenchmark {
    private static final int TABLE_SIZE = 30_000;

    private static final String SELECT_ALL_FROM_USERTABLE = "select * from usertable where ycsb_key = ?";

    private final Random random = new Random();

    @Param("1")
    private int clusterSize;

    /** Fills the table with data. */
    @Setup
    public void setUp() throws IOException {
        populateTable(TABLE_NAME, TABLE_SIZE, 10_000);
    }

    /**
     * Benchmark for KV get via embedded node.
     */
    @Benchmark
    public void kvGet(IgniteState state) {
        state.get(Tuple.create().set("ycsb_key", random.nextInt(TABLE_SIZE)));
    }

    /**
     * Benchmark for SQL select via embedded node.
     */
    @Benchmark
    public void sqlGet(IgniteState state) {
        try (ResultSet<SqlRow> rs = state.sql(SELECT_ALL_FROM_USERTABLE, random.nextInt(TABLE_SIZE))) {
            rs.next();
        }
    }

    /**
     * Benchmark for Criteria get via embedded node.
     */
    @Benchmark
    public void criteriaGet(IgniteState state) {
        try (Cursor<Tuple> cur = state.query(columnValue("ycsb_key", equalTo(random.nextInt(TABLE_SIZE))))) {
            cur.next();
        }
    }

    /**
     * Benchmark's entry point.
     */
    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(".*" + CriteriaEmbeddedNodeBenchmark.class.getSimpleName() + ".*")
                .param("fsync", "false")
                .build();

        new Runner(opt).run();
    }

    /**
     * Benchmark state for {@link #kvGet(IgniteState)}, {@link #sqlGet(IgniteState)}, {@link #criteriaGet(IgniteState)}.
     *
     * <p>Holds {@link RecordView}, {@link Session}.
     */
    @State(Scope.Benchmark)
    public static class IgniteState {
        private RecordView<Tuple> view;

        private IgniteSql sql;

        /**
         * Initializes session and statement.
         */
        @Setup
        public void setUp() {
            view = publicIgnite.tables().table(TABLE_NAME).recordView();

            sql = publicIgnite.sql();
        }

        @Nullable Tuple get(Tuple key) {
            return view.get(null, key);
        }

        ResultSet<SqlRow> sql(String query, Object... args) {
            return sql.execute(null, query, args);
        }

        Cursor<Tuple> query(@Nullable Criteria criteria) {
            return view.query(null, criteria);
        }
    }

    @Override
    protected int nodes() {
        return clusterSize;
    }
}
