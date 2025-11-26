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
import static org.apache.ignite.internal.util.IgniteUtils.closeAll;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.internal.sql.engine.SqlQueryProcessor;
import org.apache.ignite.internal.sql.engine.statistic.SqlStatisticManagerImpl;
import org.apache.ignite.internal.util.CompletableFutures;
import org.apache.ignite.sql.IgniteSql;
import org.apache.ignite.sql.ResultSet;
import org.apache.ignite.sql.Statement;
import org.apache.ignite.table.KeyValueView;
import org.apache.ignite.table.Tuple;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

/**
 * Benchmark for Deletion operation, comparing KV, JDBC and SQL APIs.
 */
@State(Scope.Benchmark)
@Fork(1)
@Threads(1)
@BenchmarkMode(Mode.SingleShotTime)
@OutputTimeUnit(TimeUnit.SECONDS)
public class BulkDeleteBenchmark extends AbstractMultiNodeBenchmark {
    private static final int TABLE_SIZE = 30_000;

    @Param({"1", "3"})
    private static int clusterSize;

    @Param({"32"})
    private int partitionCount;

    @Param({"1", "3"})
    private int replicaCount;

    /**
     * Fills the table with data.
     */
    @Setup
    public void setUp() throws IOException, ExecutionException, InterruptedException, TimeoutException {
        int id = 0;

        KeyValueView<Tuple, Tuple> keyValueView = publicIgnite.tables().table(TABLE_NAME).keyValueView();

        for (int i = 0; i < TABLE_SIZE; i++) {
            Tuple t = Tuple.create();
            for (int j = 1; j <= 10; j++) {
                t.set("field" + j, FIELD_VAL);
            }

            keyValueView.put(null, Tuple.create().set("ycsb_key", id++), t);
        }

        List<CompletableFuture<?>> futs = new ArrayList<>();
        for (int i = 0; i < clusterSize; i++) {
            SqlStatisticManagerImpl statisticManager = (SqlStatisticManagerImpl) ((SqlQueryProcessor) node(i).queryEngine())
                    .sqlStatisticManager();

            statisticManager.forceUpdateAll();
            futs.add(statisticManager.lastUpdateStatisticFuture());
        }

        CompletableFutures.allOf(futs).get(10, TimeUnit.SECONDS);
    }

    /**
     * Benchmark for SQL Delete via embedded client.
     */
    @Benchmark
    public void sqlPreparedDelete(SqlState state) {
        for (int i = TABLE_SIZE - 1; i >= 0; i--) {
            state.executeQuery(i);
        }
    }

    /**
     * Benchmark for SQL Delete via embedded client.
     */
    @Benchmark
    public void sqlInlinedDelete(SqlState state) {
        for (int i = TABLE_SIZE - 1; i >= 0; i--) {
            state.executeInlinedQuery(i);
        }
    }

    /**
     * Benchmark for KV Delete via embedded client.
     */
    @Benchmark
    public void kvDelete(KvState state) {
        for (int i = TABLE_SIZE - 1; i >= 0; i--) {
            state.executeQuery(i);
        }
    }

    /**
     * Benchmark for JDBC Delete.
     */
    @Benchmark
    public void jdbcDelete(JdbcState state) throws SQLException {
        for (int i = TABLE_SIZE - 1; i >= 0; i--) {
            state.executeQuery(i);
        }
    }

    /**
     * Benchmark for SQL Delete via thin client.
     */
    @Benchmark
    public void sqlThinDelete(SqlThinState state) {
        for (int i = TABLE_SIZE - 1; i >= 0; i--) {
            state.executeQuery(i);
        }
    }

    /**
     * Benchmark for KV Delete via thin client.
     */
    @Benchmark
    public void kvThinDelete(KvThinState state) {
        for (int i = TABLE_SIZE - 1; i >= 0; i--) {
            state.executeQuery(i);
        }
    }

    /**
     * Benchmark's entry point.
     */
    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(".*[.]" + BulkDeleteBenchmark.class.getSimpleName() + ".*")
                .build();

        new Runner(opt).run();
    }

    /**
     * Benchmark state for {@link #sqlPreparedDelete(SqlState)}.
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
            String queryStr = createDeleteStatement();

            sql = publicIgnite.sql();
            statement = sql.createStatement(queryStr);
        }

        void executeQuery(int key) {
            try (ResultSet<?> rs = sql.execute(null, statement, key)) {
                // NO-OP
            }
        }

        void executeInlinedQuery(int key) {
            try (ResultSet<?> rs = sql.execute(null, createDeleteStatement(key))) {
                // NO-OP
            }
        }
    }

    /**
     * Benchmark state for {@link #sqlThinDelete(SqlThinState)}.
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
            String queryStr = createDeleteStatement();

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

        void executeQuery(int key) {
            sql.execute(null, statement, key);
        }
    }

    /**
     * Benchmark state for {@link #jdbcDelete(JdbcState)}.
     *
     * <p>Holds {@link Connection} and {@link PreparedStatement}.
     */
    @State(Scope.Benchmark)
    public static class JdbcState {
        private Connection conn;

        private PreparedStatement stmt;

        /**
         * Initializes connection and prepared statement.
         */
        @Setup
        public void setUp() throws SQLException {
            String queryStr = createDeleteStatement();

            //noinspection CallToDriverManagerGetConnection
            conn = DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1:10800/");

            stmt = conn.prepareStatement(queryStr);
        }

        /**
         * Closes resources.
         */
        @TearDown
        public void tearDown() throws Exception {
            closeAll(stmt, conn);
        }

        void executeQuery(int key) throws SQLException {
            stmt.setInt(1, key);
            stmt.executeUpdate();
        }
    }

    /**
     * Benchmark state for {@link #kvDelete(KvState)}.
     *
     * <p>Holds {@link Tuple} and {@link KeyValueView} for the table.
     */
    @State(Scope.Benchmark)
    public static class KvState {
        private final KeyValueView<Tuple, Tuple> kvView = publicIgnite.tables().table(TABLE_NAME).keyValueView();

        void executeQuery(int key) {
            kvView.remove(null, Tuple.create().set("ycsb_key", key));
        }
    }

    /**
     * Benchmark state for {@link #kvThinDelete(KvThinState)}.
     *
     * <p>Holds {@link Tuple}, {@link IgniteClient}, and {@link KeyValueView} for the table.
     */
    @State(Scope.Benchmark)
    public static class KvThinState {
        private IgniteClient client;
        private KeyValueView<Tuple, Tuple> kvView;

        /**
         * Initializes the tuple.
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

        void executeQuery(int key) {
            kvView.remove(null, Tuple.create().set("ycsb_key", key));
        }
    }

    private static String createDeleteStatement() {
        String deleteQueryTemplate = "DELETE FROM {} WHERE ycsb_key = ?";

        return format(deleteQueryTemplate, TABLE_NAME, "ycsb_key");
    }

    private static String createDeleteStatement(int key) {
        String deleteQueryTemplate = "DELETE FROM {} WHERE ycsb_key = {}";

        return format(deleteQueryTemplate, TABLE_NAME, "ycsb_key", key);
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
