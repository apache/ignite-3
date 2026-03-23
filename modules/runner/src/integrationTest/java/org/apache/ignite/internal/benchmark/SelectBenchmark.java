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

import static org.apache.ignite.internal.util.IgniteUtils.closeAll;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.internal.sql.engine.AsyncSqlCursor;
import org.apache.ignite.internal.sql.engine.InternalSqlRow;
import org.apache.ignite.internal.sql.engine.QueryProcessor;
import org.apache.ignite.internal.sql.engine.SqlProperties;
import org.apache.ignite.internal.sql.engine.SqlQueryType;
import org.apache.ignite.internal.util.AsyncCursor.BatchedResult;
import org.apache.ignite.sql.IgniteSql;
import org.apache.ignite.sql.SqlRow;
import org.apache.ignite.sql.Statement;
import org.apache.ignite.table.KeyValueView;
import org.apache.ignite.table.Tuple;
import org.apache.ignite.tx.IgniteTransactions;
import org.apache.ignite.tx.Transaction;
import org.apache.ignite.tx.TransactionOptions;
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
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
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
public class SelectBenchmark extends AbstractMultiNodeBenchmark {
    private static final int TABLE_SIZE = 30_000;
    private static final String SELECT_ALL_FROM_USERTABLE = "select * from usertable where ycsb_key = ?";

    private final Random random = new Random();

    private KeyValueView<Tuple, Tuple> keyValueView;

    private IgniteTransactions transactions;

    private final TransactionOptions readOnlyTransactionOptions = new TransactionOptions().readOnly(true);

    @Param({"1", "2", "3"})
    private static int clusterSize;

    /**
     * Fills the table with data.
     */
    @Setup
    public void setUp() throws IOException {
        int id = 0;

        keyValueView = publicIgnite.tables().table(TABLE_NAME).keyValueView();

        for (int i = 0; i < TABLE_SIZE; i++) {
            Tuple t = Tuple.create();
            for (int j = 1; j <= 10; j++) {
                t.set("field" + j, FIELD_VAL);
            }

            keyValueView.put(null, Tuple.create().set("ycsb_key", id++), t);
        }

        transactions = publicIgnite.transactions();
    }

    /**
     * Benchmark for SQL select via embedded client.
     */
    @Benchmark
    public void sqlGet(SqlState sqlState, Blackhole bh) {
        try (var rs = sqlState.sql(SELECT_ALL_FROM_USERTABLE, random.nextInt(TABLE_SIZE))) {
            bh.consume(rs.next());
        }
    }

    /**
     * Benchmark for SQL select via embedded client using internal API.
     */
    @Benchmark
    public void sqlGetInternal(SqlInternalApiState sqlInternalApiState, Blackhole bh) {
        Iterator<InternalSqlRow> res = sqlInternalApiState.query(SELECT_ALL_FROM_USERTABLE, random.nextInt(TABLE_SIZE));

        bh.consume(res.next());
    }

    /**
     * Benchmark for SQL script select via embedded client using internal API.
     */
    @Benchmark
    public void sqlGetInternalScript(SqlInternalApiState sqlInternalApiState, Blackhole bh) {
        Iterator<InternalSqlRow> res = sqlInternalApiState.script(SELECT_ALL_FROM_USERTABLE, random.nextInt(TABLE_SIZE));

        bh.consume(res.next());
    }

    /**
     * Benchmark for SQL select via thin client.
     */
    @Benchmark
    public void sqlThinGet(SqlThinState sqlState, Blackhole bh) {
        try (var rs = sqlState.sql(SELECT_ALL_FROM_USERTABLE, random.nextInt(TABLE_SIZE))) {
            bh.consume(rs.next());
        }
    }

    /**
     * Benchmark for JDBC get.
     */
    @Benchmark
    public void jdbcGet(JdbcState state, Blackhole bh) throws SQLException {
        state.stmt.setInt(1, random.nextInt(TABLE_SIZE));
        try (ResultSet r = state.stmt.executeQuery()) {
            bh.consume(r.next());
        }
    }

    /**
     * Benchmark for JDBC script get.
     */
    @Benchmark
    public void jdbcGetScript(JdbcState state, Blackhole bh) throws SQLException {
        state.stmt.setInt(1, random.nextInt(TABLE_SIZE));
        state.stmt.execute();

        try (ResultSet r = state.stmt.getResultSet()) {
            bh.consume(r.next());
        }
    }

    /**
     * Benchmark for KV get via embedded client.
     */
    @Benchmark
    public void kvGet(Blackhole bh) {
        doKvGet(null, bh);
    }

    /**
     * Benchmark for KV get in RO transaction via embedded client.
     */
    @Benchmark
    public void kvGetInRoTransaction(Blackhole bh) {
        transactions.runInTransaction(tx -> {
            doKvGet(tx, bh);
        }, readOnlyTransactionOptions);
    }

    private void doKvGet(@Nullable Transaction tx, Blackhole bh) {
        Tuple val = keyValueView.get(tx, Tuple.create().set("ycsb_key", random.nextInt(TABLE_SIZE)));
        bh.consume(val);
    }

    /**
     * Benchmark for KV get via thin client.
     */
    @Benchmark
    public void kvThinGet(KvThinState kvState, Blackhole bh) {
        Tuple val = kvState.kvView().get(null, Tuple.create().set("ycsb_key", random.nextInt(TABLE_SIZE)));
        bh.consume(val);
    }

    /**
     * Benchmark's entry point.
     */
    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(".*\\." + SelectBenchmark.class.getSimpleName() + ".*")
                .build();

        new Runner(opt).run();
    }

    /**
     * Benchmark state for {@link #sqlGet(SqlState, Blackhole)}.
     *
     * <p>Holds {@link IgniteSql}.
     */
    @State(Scope.Benchmark)
    public static class SqlState {
        private final IgniteSql sql = publicIgnite.sql();

        private org.apache.ignite.sql.ResultSet<SqlRow> sql(String sql, Object... args) {
            return this.sql.execute(null, sql, args);
        }
    }

    /**
     * Benchmark state for {@link #sqlGetInternalScript(SqlInternalApiState, Blackhole)} and
     * {@link #sqlGetInternal(SqlInternalApiState, Blackhole)}.
     */
    @State(Scope.Benchmark)
    public static class SqlInternalApiState {
        private final SqlProperties properties = new SqlProperties()
                .allowedQueryTypes(SqlQueryType.SINGLE_STMT_TYPES)
                .allowMultiStatement(false);

        private final SqlProperties scriptProperties = new SqlProperties()
                .allowedQueryTypes(SqlQueryType.ALL)
                .allowMultiStatement(true);

        private final QueryProcessor queryProc = igniteImpl.queryEngine();
        private int pageSize;

        /** Initializes session. */
        @Setup
        public void setUp() throws Exception {
            Statement statement = publicIgnite.sql().createStatement("SELECT 1");

            pageSize = statement.pageSize();
        }

        private Iterator<InternalSqlRow> query(String sql, Object... args) {
            return handleFirstBatch(queryProc.queryAsync(properties, igniteImpl.observableTimeTracker(), null, null, sql, args));
        }

        private Iterator<InternalSqlRow> script(String sql, Object... args) {
            return handleFirstBatch(queryProc.queryAsync(scriptProperties, igniteImpl.observableTimeTracker(), null, null, sql, args));
        }

        private Iterator<InternalSqlRow> handleFirstBatch(CompletableFuture<AsyncSqlCursor<InternalSqlRow>> cursorFut) {
            AsyncSqlCursor<InternalSqlRow> cursor = cursorFut.join();
            BatchedResult<InternalSqlRow> res = cursor.requestNextAsync(pageSize).join();

            cursor.closeAsync();

            return res.items().iterator();
        }
    }

    /**
     * Benchmark state for {@link #sqlThinGet(SqlThinState, Blackhole)}.
     *
     * <p>Holds {@link IgniteClient} and {@link IgniteSql}.
     */
    @State(Scope.Benchmark)
    public static class SqlThinState {
        private IgniteClient client;
        private IgniteSql sql;

        /**
         * Initializes session and statement.
         */
        @Setup
        public void setUp() {
            String[] clientAddrs = getServerEndpoints(clusterSize);

            client = IgniteClient.builder().addresses(clientAddrs).build();

            sql = client.sql();
        }

        /**
         * Closes resources.
         */
        @TearDown
        public void tearDown() throws Exception {
            closeAll(client);
        }

        org.apache.ignite.sql.ResultSet<SqlRow> sql(String query, Object... args) {
            return sql.execute(null, query, args);
        }
    }

    /**
     * Benchmark state for {@link #jdbcGet(JdbcState, Blackhole)}.
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
                conn = DriverManager.getConnection("jdbc:ignite:thin://" + String.join(",", getServerEndpoints(clusterSize)));

                stmt = conn.prepareStatement(SELECT_ALL_FROM_USERTABLE);
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }

        @TearDown
        public void tearDown() throws Exception {
            closeAll(stmt, conn);
        }
    }

    /**
     * Benchmark state for {@link #kvThinGet(KvThinState, Blackhole)}.
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
            String[] clientAddrs = getServerEndpoints(clusterSize);

            client = IgniteClient.builder().addresses(clientAddrs).build();

            kvView = client.tables().table(TABLE_NAME).keyValueView();
        }

        @TearDown
        public void tearDown() throws Exception {
            closeAll(client);
        }

        KeyValueView<Tuple, Tuple> kvView() {
            return kvView;
        }
    }

    @Override
    protected int nodes() {
        return clusterSize;
    }
}


