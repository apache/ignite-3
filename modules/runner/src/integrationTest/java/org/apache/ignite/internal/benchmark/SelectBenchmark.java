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
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.sql.IgniteSql;
import org.apache.ignite.sql.Session;
import org.apache.ignite.sql.SqlRow;
import org.apache.ignite.table.KeyValueView;
import org.apache.ignite.table.Tuple;
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
 * Benchmark for reading operation, comparing KV, JDBC and SQL APIs.
 */
@State(Scope.Benchmark)
@Fork(1)
@Threads(1)
@Warmup(iterations = 10, time = 2)
@Measurement(iterations = 20, time = 2)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@SuppressWarnings({"WeakerAccess", "unused"})
public class SelectBenchmark extends AbstractOneNodeBenchmark {
    private static final int TABLE_SIZE = 30_000;
    private static final String SELECT_ALL_FROM_USERTABLE = "select * from usertable where ycsb_key = ?";

    private final Random random = new Random();

    private KeyValueView<Tuple, Tuple> keyValueView;

    /**
     * Fills the table with data.
     */
    @Setup
    public void setUp() throws IOException {
        int id = 0;

        keyValueView = clusterNode.tables().table(TABLE_NAME).keyValueView();

        for (int i = 0; i < TABLE_SIZE; i++) {
            Tuple t = Tuple.create();
            for (int j = 1; j <= 10; j++) {
                t.set("field" + j, FIELD_VAL);
            }

            keyValueView.put(null, Tuple.create().set("ycsb_key", id++), t);
        }
    }

    /**
     * Benchmark for SQL select via embedded client.
     */
    @Benchmark
    public void sqlGet(SqlState sqlState) {
        try (var rs = sqlState.sql(SELECT_ALL_FROM_USERTABLE, random.nextInt(TABLE_SIZE))) {
            rs.next();
        }
    }

    /**
     * Benchmark for SQL select via thin client.
     */
    @Benchmark
    public void sqlThinGet(SqlThinState sqlState) {
        try (var rs = sqlState.sql(SELECT_ALL_FROM_USERTABLE, random.nextInt(TABLE_SIZE))) {
            rs.next();
        }
    }

    /**
     * Benchmark for JDBC get.
     */
    @Benchmark
    public void jdbcGet(JdbcState state) throws SQLException {
        state.stmt.setInt(1, random.nextInt(TABLE_SIZE));
        try (ResultSet r = state.stmt.executeQuery()) {
            r.next();
        }
    }

    /**
     * Benchmark for KV get via embedded client.
     */
    @Benchmark
    public void kvGet() {
        keyValueView.get(null, Tuple.create().set("ycsb_key", random.nextInt(TABLE_SIZE)));
    }

    /**
     * Benchmark for KV get via thin client.
     */
    @Benchmark
    public void kvThinGet(KvThinState kvState) {
        kvState.kvView().get(null, Tuple.create().set("ycsb_key", random.nextInt(TABLE_SIZE)));
    }

    /**
     * Benchmark's entry point.
     */
    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(".*" + SelectBenchmark.class.getSimpleName() + ".*")
                .build();

        new Runner(opt).run();
    }

    /**
     * Benchmark state for {@link #sqlGet(SqlState)}.
     *
     * <p>Holds {@link Session}.
     */
    @State(Scope.Benchmark)
    public static class SqlState {
        private final Session session = clusterNode.sql().createSession();

        /**
         * Closes resources.
         */
        @TearDown
        public void tearDown() throws Exception {
            IgniteUtils.closeAll(session);
        }

        private org.apache.ignite.sql.ResultSet<SqlRow> sql(String sql, Object... args) {
            return session.execute(null, sql, args);
        }
    }

    /**
     * Benchmark state for {@link #sqlThinGet(SqlThinState)}.
     *
     * <p>Holds {@link IgniteClient} and {@link Session}.
     */
    @State(Scope.Benchmark)
    public static class SqlThinState {
        private IgniteClient client;
        private Session session;

        /**
         * Initializes session and statement.
         */
        @Setup
        public void setUp() {
            client = IgniteClient.builder().addresses("127.0.0.1:10800").build();

            IgniteSql sql = client.sql();

            session = sql.createSession();
        }

        /**
         * Closes resources.
         */
        @TearDown
        public void tearDown() throws Exception {
            IgniteUtils.closeAll(session, client);
        }

        org.apache.ignite.sql.ResultSet<SqlRow> sql(String query, Object... args) {
            return session.execute(null, query, args);
        }
    }

    /**
     * Benchmark state for {@link #jdbcGet(JdbcState)}.
     *
     * <p>Holds {@link Connection} and {@link PreparedStatement}.
     */
    @State(Scope.Benchmark)
    public static class JdbcState {
        Connection conn;

        PreparedStatement stmt;

        /**
         * Initializes connection and prepared statement.
         */
        @Setup
        public void setUp() {
            try {
                //noinspection CallToDriverManagerGetConnection
                conn = DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1:10800/");

                stmt = conn.prepareStatement(SELECT_ALL_FROM_USERTABLE);
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }

        @TearDown
        public void tearDown() throws Exception {
            IgniteUtils.closeAll(stmt, conn);
        }
    }

    /**
     * Benchmark state for {@link #kvThinGet(KvThinState)}.
     *
     * <p>Holds {@link IgniteClient} and {@link KeyValueView} for the table.
     */
    @State(Scope.Benchmark)
    public static class KvThinState {
        private IgniteClient client;
        private KeyValueView<Tuple, Tuple> kvView;

        /**
         * Creates the client.
         */
        @Setup
        public void setUp() {
            client = IgniteClient.builder().addresses("127.0.0.1:10800").build();
            kvView = client.tables().table(TABLE_NAME).keyValueView();
        }

        @TearDown
        public void tearDown() throws Exception {
            IgniteUtils.closeAll(client);
        }

        KeyValueView<Tuple, Tuple> kvView() {
            return kvView;
        }
    }
}


