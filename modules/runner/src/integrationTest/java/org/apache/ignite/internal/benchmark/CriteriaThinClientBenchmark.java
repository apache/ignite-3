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
import java.util.stream.IntStream;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.lang.Cursor;
import org.apache.ignite.sql.IgniteSql;
import org.apache.ignite.sql.ResultSet;
import org.apache.ignite.sql.SqlRow;
import org.apache.ignite.table.RecordView;
import org.apache.ignite.table.Tuple;
import org.apache.ignite.table.criteria.Criteria;
import org.apache.ignite.tx.Transaction;
import org.jetbrains.annotations.Nullable;
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
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

/**
 * Benchmark for criteria operation, comparing KV, SQL APIs over thin client.
 */
@State(Scope.Benchmark)
@Fork(1)
@Threads(1)
@Warmup(iterations = 10, time = 2)
@Measurement(iterations = 20, time = 2)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@SuppressWarnings({"WeakerAccess", "unused"})
public class CriteriaThinClientBenchmark extends AbstractMultiNodeBenchmark {
    private static final int CLUSTER_SIZE = 2;

    private static final int TABLE_SIZE = 100_000;

    private static final String SELECT_FROM_USERTABLE = "select * from usertable where ycsb_key = ?";

    private static final String SELECT_ALL_FROM_USERTABLE = "select * from usertable";

    /** Fills the table with data. */
    @Setup
    public void setUp() throws IOException {
        populateTable(TABLE_NAME, TABLE_SIZE, 10_000);
    }

    /**
     * Benchmark for KV get via thin client.
     */
    @Benchmark
    public void kvGet(ThinClientState state) {
        state.get(makeKey(state.nextId()));
    }

    /**
     * Benchmark for get via thin client.
     */
    @Benchmark
    public void kvGetNonNullTxDisablesPartitionAwareness(NoPartitionAwarenessState state) {
        state.get(makeKey(state.nextId()));
    }

    /**
     * Benchmark for SQL select via thin client.
     */
    @Benchmark
    public void sqlGet(ThinClientState state) {
        try (ResultSet<SqlRow> rs = state.sql(SELECT_FROM_USERTABLE, state.nextId())) {
            rs.next();
        }
    }

    /**
     * Benchmark for SQL select via thin client.
     */
    @Benchmark
    public void sqlGetNonNullTxDisablesPartitionAwareness(NoPartitionAwarenessState state) {
        try (ResultSet<SqlRow> rs = state.sql(SELECT_FROM_USERTABLE, state.nextId())) {
            rs.next();
        }
    }

    /**
     * Benchmark for Criteria get via thin client.
     */
    @Benchmark
    public void criteriaGet(ThinClientState state) {
        try (Cursor<Tuple> cur = state.query(columnValue("ycsb_key", equalTo(state.nextId())))) {
            cur.next();
        }
    }

    /**
     * Benchmark for Criteria get via thin client.
     */
    @Benchmark
    public void criteriaGetNonNullTxDisablesPartitionAwareness(NoPartitionAwarenessState state) {
        try (Cursor<Tuple> cur = state.query(columnValue("ycsb_key", equalTo(state.nextId())))) {
            cur.next();
        }
    }

    /**
     * Benchmark for SQL select with iteration via thin client.
     */
    @Benchmark
    public void sqlIterate(ThinClientState state) {
        try (ResultSet<SqlRow> rs = state.sql(SELECT_ALL_FROM_USERTABLE)) {
            while (rs.hasNext()) {
                rs.next();
            }
        }
    }

    /**
     * Benchmark for Criteria with iteration via thin client.
     */
    @Benchmark
    public void criteriaIterate(ThinClientState state) {
        try (Cursor<Tuple> cur = state.query(null)) {
            while (cur.hasNext()) {
                cur.next();
            }
        }
    }

    /**
     * Makes a tuple containing key and value.
     *
     * @param id The id.
     * @return The value tuple.
     */
    static Tuple makeKey(int id) {
        return Tuple.create().set("ycsb_key", id);
    }

    /**
     * Benchmark's entry point.
     */
    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(".*" + CriteriaThinClientBenchmark.class.getSimpleName() + ".*")
                .param("fsync", "false")
                .build();

        new Runner(opt).run();
    }

    /**
     * Benchmark state for {@link #kvGet(ThinClientState)}, {@link #sqlGet(ThinClientState)}, {@link #criteriaGet(ThinClientState)},
     *      {@link #sqlIterate(ThinClientState)}, {@link #criteriaIterate(ThinClientState)}.
     *
     * <p>Holds {@link IgniteClient} and {@link Session}.
     */
    @State(Scope.Benchmark)
    public static class ThinClientState {
        protected final Random random = new Random();

        protected IgniteClient client;
        protected IgniteSql sql;
        protected RecordView<Tuple> view;

        @Nullable
        protected Transaction tx;

        /**
         * Initializes session and statement.
         */
        @Setup
        public void setUp() {
            client = IgniteClient.builder().addresses(getClientAddresses()).build();
            sql = client.sql();
            view = client.tables().table(TABLE_NAME).recordView();
        }

        /**
         * Gets client connector addresses for the specified nodes.
         *
         * @return Array of client addresses.
         */
        String[] getClientAddresses() {
            return IntStream.range(0, CLUSTER_SIZE)
                    .mapToObj(i -> "127.0.0.1:" + (BASE_CLIENT_PORT + i))
                    .toArray(String[]::new);
        }

        /**
         * Closes resources.
         */
        @TearDown
        public void tearDown() throws Exception {
            IgniteUtils.closeAll(client);
        }

        @Nullable Tuple get(Tuple key) {
            return view.get(tx, key);
        }

        ResultSet<SqlRow> sql(String query, Object... args) {
            return sql.execute(tx, query, args);
        }

        Cursor<Tuple> query(@Nullable Criteria criteria) {
            return view.query(tx, criteria);
        }

        int nextId() {
            return random.nextInt(TABLE_SIZE);
        }
    }

    /**
     * Benchmark state for {@link #kvGetNonNullTxDisablesPartitionAwareness(NoPartitionAwarenessState)},
     *      {@link #sqlGetNonNullTxDisablesPartitionAwareness(NoPartitionAwarenessState)},
     *      {@link #criteriaGetNonNullTxDisablesPartitionAwareness(NoPartitionAwarenessState)}
     *
     * <p>Holds {@link IgniteClient}, {@link Session} and {@link Transaction}.
     */
    @State(Scope.Benchmark)
    public static class NoPartitionAwarenessState extends ThinClientState {
        /**
         * Initializes session and statement.
         */
        @Setup
        @Override
        public void setUp() {
            super.setUp();

            tx = client.transactions().begin();
        }

        /**
         * Closes resources.
         */
        @TearDown
        @Override
        public void tearDown() throws Exception {
            if (tx != null) {
                tx.rollback();
            }

            IgniteUtils.closeAll(client);
        }
    }

    @Override
    protected int nodes() {
        return CLUSTER_SIZE;
    }
}
