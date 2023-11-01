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

import static java.util.stream.Collectors.joining;
import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.sql.IgniteSql;
import org.apache.ignite.sql.Session;
import org.apache.ignite.sql.Statement;
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
 * Benchmark for insertion operation, comparing KV, JDBC and SQL APIs.
 */
@State(Scope.Benchmark)
@Fork(1)
@Threads(1)
@Warmup(iterations = 10, time = 2)
@Measurement(iterations = 20, time = 2)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class InsertBenchmark extends AbstractOneNodeBenchmark {
    /**
     * Benchmark for SQL insert via embedded client.
     */
    @Benchmark
    public void sqlInsert(SqlState state) {
        state.executeQuery();
    }

    /**
     * Benchmark for KV insert via embedded client.
     */
    @Benchmark
    public void kvInsert(KvState state) {
        state.executeQuery();
    }

    /**
     * Benchmark for JDBC insert.
     */
    @Benchmark
    public void jdbcInsert(JdbcState state) throws SQLException {
        state.executeQuery();
    }

    /**
     * Benchmark for SQL insert via thin client.
     */
    @Benchmark
    public void sqlThinInsert(SqlThinState state) {
        state.executeQuery();
    }

    /**
     * Benchmark for KV insert via thin client.
     */
    @Benchmark
    public void kvThinInsert(KvThinState state) {
        state.executeQuery();
    }

    /**
     * Benchmark's entry point.
     */
    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(".*" + InsertBenchmark.class.getSimpleName() + ".*")
                .forks(1)
                .threads(1)
                .mode(Mode.AverageTime)
                .build();

        new Runner(opt).run();
    }

    /**
     * Benchmark state for {@link #sqlInsert(SqlState)}.
     *
     * <p>Holds {@link Session} and {@link Statement}.
     */
    @State(Scope.Benchmark)
    public static class SqlState {
        private Statement statement;
        private Session session;

        /**
         * Initializes session and statement.
         */
        @Setup
        public void setUp() {
            String queryStr = createInsertStatement();

            IgniteSql sql = clusterNode.sql();

            statement = sql.createStatement(queryStr);
            session = sql.createSession();
        }

        /**
         * Closes resources.
         */
        @TearDown
        public void tearDown() throws Exception {
            // statement.close() throws `UnsupportedOperationException("Not implemented yet.")`, that's why it's commented.
            IgniteUtils.closeAll(session/*, statement*/);
        }

        private int id = 0;

        void executeQuery() {
            session.execute(null, statement, id++);
        }
    }

    /**
     * Benchmark state for {@link #sqlThinInsert(SqlThinState)}.
     *
     * <p>Holds {@link Session}, {@link IgniteClient}, and {@link Statement}.
     */
    @State(Scope.Benchmark)
    public static class SqlThinState {
        private IgniteClient client;
        private Statement statement;
        private Session session;

        /**
         * Initializes session and statement.
         */
        @Setup
        public void setUp() {
            String queryStr = createInsertStatement();

            client = IgniteClient.builder().addresses("127.0.0.1:10800").build();

            IgniteSql sql = client.sql();

            statement = sql.createStatement(queryStr);
            session = sql.createSession();
        }

        /**
         * Closes resources.
         */
        @TearDown
        public void tearDown() throws Exception {
            // statement.close() throws `UnsupportedOperationException("Not implemented yet.")`, that's why it's commented.
            IgniteUtils.closeAll(session/*, statement*/, client);
        }

        private int id = 0;

        void executeQuery() {
            session.execute(null, statement, id++);
        }
    }

    /**
     * Benchmark state for {@link #jdbcInsert(JdbcState)}.
     *
     * <p>Holds {@link Connection} and {@link PreparedStatement}.
     */
    @State(Scope.Benchmark)
    public static class JdbcState {
        private Connection conn;

        private PreparedStatement stmt;

        private int id;

        /**
         * Initializes connection and prepared statement.
         */
        @Setup
        public void setUp() throws SQLException {
            String queryStr = createInsertStatement();

            //noinspection CallToDriverManagerGetConnection
            conn = DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1:10800/");

            stmt = conn.prepareStatement(queryStr);
        }

        /**
         * Closes resources.
         */
        @TearDown
        public void tearDown() throws Exception {
            IgniteUtils.closeAll(stmt, conn);
        }

        void executeQuery() throws SQLException {
            stmt.setInt(1, id++);
            stmt.executeUpdate();
        }
    }

    /**
     * Benchmark state for {@link #kvInsert(KvState)}.
     *
     * <p>Holds {@link Tuple} and {@link KeyValueView} for the table.
     */
    @State(Scope.Benchmark)
    public static class KvState {
        private final Tuple tuple = Tuple.create();

        private int id = 0;

        private final KeyValueView<Tuple, Tuple> kvView = clusterNode.tables().table(TABLE_NAME).keyValueView();

        /**
         * Initializes the tuple.
         */
        @Setup
        public void setUp() {
            for (int i = 1; i < 11; i++) {
                tuple.set("field" + i, FIELD_VAL);
            }
        }

        void executeQuery() {
            kvView.put(null, Tuple.create().set("ycsb_key", id++), tuple);
        }
    }

    /**
     * Benchmark state for {@link #kvThinInsert(KvThinState)}.
     *
     * <p>Holds {@link Tuple}, {@link IgniteClient}, and {@link KeyValueView} for the table.
     */
    @State(Scope.Benchmark)
    public static class KvThinState {
        private final Tuple tuple = Tuple.create();

        private IgniteClient client;
        private KeyValueView<Tuple, Tuple> kvView;

        private int id = 0;

        /**
         * Initializes the tuple.
         */
        @Setup
        public void setUp() {
            for (int i = 1; i < 11; i++) {
                tuple.set("field" + i, FIELD_VAL);
            }

            client = IgniteClient.builder().addresses("127.0.0.1:10800").build();
            kvView = client.tables().table(TABLE_NAME).keyValueView();
        }

        @TearDown
        public void tearDown() throws Exception {
            IgniteUtils.closeAll(client);
        }

        void executeQuery() {
            kvView.put(null, Tuple.create().set("ycsb_key", id++), tuple);
        }
    }

    private static String createInsertStatement() {
        String insertQueryTemplate = "insert into {}({}, {}) values(?, {})";

        String fieldsQ = IntStream.range(1, 11).mapToObj(i -> "field" + i).collect(joining(","));
        String valQ = IntStream.range(1, 11).mapToObj(i -> "'" + FIELD_VAL + "'").collect(joining(","));

        return format(insertQueryTemplate, TABLE_NAME, "ycsb_key", fieldsQ, valQ);
    }
}
