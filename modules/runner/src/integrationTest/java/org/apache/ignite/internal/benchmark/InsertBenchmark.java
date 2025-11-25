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
import static org.apache.ignite.internal.util.IgniteUtils.closeAll;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.sql.IgniteSql;
import org.apache.ignite.sql.ResultSet;
import org.apache.ignite.sql.Statement;
import org.apache.ignite.table.KeyValueView;
import org.apache.ignite.table.Tuple;
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
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class InsertBenchmark extends AbstractMultiNodeBenchmark {
    @Param({"1", "2", "3"})
    private static int clusterSize;

    @Param({"1", "2", "4", "8", "16", "32"})
    private int partitionCount;

    @Param({"1", "2", "3"})
    private int replicaCount;

    /**
     * Benchmark for SQL insert via embedded client.
     */
    @Benchmark
    public void sqlPreparedInsert(SqlState state) {
        state.executeQuery();
    }

    /**
     * Benchmark for SQL insert via embedded client.
     */
    @Benchmark
    public void sqlInlinedInsert(SqlState state) {
        state.executeInlinedQuery();
    }

    /**
     * Benchmark for SQL multiple rows insert via embedded client.
     */
    @Benchmark
    public void sqlInsertMulti(SqlStateMultiValues state) {
        state.executeQuery();
    }

    /**
     * Benchmark for SQL script insert via embedded client.
     */
    @Benchmark
    public void sqlInsertScript(SqlState state) {
        state.executeScript();
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
     * Benchmark for JDBC script insert.
     */
    @Benchmark
    public void jdbcInsertScript(JdbcState state) throws SQLException {
        state.executeScript();
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
                .include(".*[.]" + InsertBenchmark.class.getSimpleName() + ".*")
                .build();

        new Runner(opt).run();
    }

    /**
     * Benchmark state for {@link #sqlPreparedInsert(SqlState)} and {@link #sqlInsertScript(SqlState)}.
     *
     * <p>Holds {@link Statement}.
     */
    @State(Scope.Benchmark)
    public static class SqlState {
        private Statement statement;
        private IgniteSql sql;

        /**
         * Initializes session and statement.
         */
        @Setup
        public void setUp() {
            String queryStr = createInsertStatement();

            sql = publicIgnite.sql();
            statement = sql.createStatement(queryStr);
        }

        private int id = 0;

        void executeQuery() {
            try (ResultSet<?> rs = sql.execute(null, statement, id++)) {
                // NO-OP
            }
        }

        void executeInlinedQuery() {
            try (ResultSet<?> rs = sql.execute(null, createInsertStatement(id++))) {
                // NO-OP
            }
        }

        void executeScript() {
            sql.executeScript(statement.query(), id++);
        }
    }

    /**
     * Benchmark state for {@link #sqlPreparedInsert(SqlState)} and {@link #sqlInsertScript(SqlState)}.
     *
     * <p>Holds {@link Statement}.
     */
    @State(Scope.Benchmark)
    public static class SqlStateMultiValues {
        private Statement statement;
        private IgniteSql sql;

        /**
         * Initializes session and statement.
         */
        @Setup
        public void setUp() {
            String queryStr = createMultiInsertStatement();

            sql = publicIgnite.sql();
            statement = sql.createStatement(queryStr);
        }

        private int id = 0;

        void executeQuery() {
            try (ResultSet<?> rs = sql.execute(null, statement, id + 1, id + 2);) {
                id += 2;
            }
        }
    }

    /**
     * Benchmark state for {@link #sqlThinInsert(SqlThinState)}.
     *
     * <p>Holds {@link IgniteClient} and {@link Statement}.
     */
    @State(Scope.Benchmark)
    public static class SqlThinState {
        private IgniteClient client;
        private Statement statement;
        private IgniteSql sql;

        /**
         * Initializes session and statement.
         */
        @Setup
        public void setUp() {
            String queryStr = createInsertStatement();

            String[] clientAddrs = getServerEndpoints(clusterSize);

            client = IgniteClient.builder().addresses(clientAddrs).build();

            sql = client.sql();

            statement = sql.createStatement(queryStr);
        }

        /**
         * Closes resources.
         */
        @TearDown
        public void tearDown() throws Exception {
            closeAll(client);
        }

        private int id = 0;

        void executeQuery() {
            sql.execute(null, statement, id++);
        }
    }

    /**
     * Benchmark state for {@link #jdbcInsert(JdbcState)} and {@link #jdbcInsertScript(JdbcState)}.
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
            conn = DriverManager.getConnection("jdbc:ignite:thin://" + String.join(",", getServerEndpoints(clusterSize)));

            stmt = conn.prepareStatement(queryStr);
        }

        /**
         * Closes resources.
         */
        @TearDown
        public void tearDown() throws Exception {
            closeAll(stmt, conn);
        }

        void executeQuery() throws SQLException {
            stmt.setInt(1, id++);
            stmt.executeUpdate();
        }

        void executeScript() throws SQLException {
            stmt.setInt(1, id++);
            stmt.execute();
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

        private final KeyValueView<Tuple, Tuple> kvView = publicIgnite.tables().table(TABLE_NAME).keyValueView();

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

            String[] clientAddrs = getServerEndpoints(clusterSize);

            client = IgniteClient.builder().addresses(clientAddrs).build();

            kvView = client.tables().table(TABLE_NAME).keyValueView();
        }

        @TearDown
        public void tearDown() throws Exception {
            closeAll(client);
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

    private static String createInsertStatement(int key) {
        String insertQueryTemplate = "insert into {}({}, {}) values({}, {})";

        String fieldsQ = IntStream.range(1, 11).mapToObj(i -> "field" + i).collect(joining(","));
        String valQ = IntStream.range(1, 11).mapToObj(i -> "'" + FIELD_VAL + "'").collect(joining(","));

        return format(insertQueryTemplate, TABLE_NAME, "ycsb_key", fieldsQ, key, valQ);
    }

    private static String createMultiInsertStatement() {
        String insertQueryTemplate = "insert into {}({}, {}) values(?, {}), (?, {})";

        String fieldsQ = IntStream.range(1, 11).mapToObj(i -> "field" + i).collect(joining(","));
        String valQ = IntStream.range(1, 11).mapToObj(i -> "'" + FIELD_VAL_WITH_SPACES + "'").collect(joining(","));

        return format(insertQueryTemplate, TABLE_NAME, "ycsb_key", fieldsQ, valQ, valQ);
    }

    @Override
    protected int nodes() {
        return clusterSize;
    }

    @Override
    protected int partitionCount() {
        return partitionCount;
    }

    @Override
    protected int replicaCount() {
        return replicaCount;
    }
}
