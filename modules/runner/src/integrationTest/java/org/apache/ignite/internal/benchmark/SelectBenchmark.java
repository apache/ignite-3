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

import static org.apache.ignite.internal.sql.engine.property.PropertiesHelper.newBuilder;
import static org.apache.ignite.internal.sql.engine.util.CursorUtils.getAllFromCursor;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.await;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.internal.sql.engine.QueryContext;
import org.apache.ignite.internal.sql.engine.QueryProcessor;
import org.apache.ignite.internal.sql.engine.QueryProperty;
import org.apache.ignite.internal.sql.engine.SqlQueryType;
import org.apache.ignite.internal.sql.engine.session.SessionId;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.table.KeyValueView;
import org.apache.ignite.table.Tuple;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

/**
 * Benchmark for reading operation, comparing KV, JDBC and SQL APIs.
 */
@State(Scope.Benchmark)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class SelectBenchmark extends AbstractOneNodeBenchmark {
    private final Random random = new Random();

    private KeyValueView<Tuple, Tuple> keyValueView;

    /**
     * Fills the table with data.
     */
    @Setup
    public void setUp() throws IOException {
        int id = 0;

        keyValueView = clusterNode.tables().table(TABLE_NAME).keyValueView();

        for (int i = 0; i < 1000; i++) {
            Tuple t = Tuple.create();
            for (int j = 1; j <= 10; j++) {
                t.set("field" + j, FIELD_VAL);
            }

            keyValueView.put(null, Tuple.create().set("ycsb_key", id++), t);
        }
    }

    /**
     * Benchmark for SQL select.
     */
    @Benchmark
    @Warmup(iterations = 1, time = 10)
    @Measurement(iterations = 1, time = 20)
    public void sqlGet(SqlState sqlState) {
        sqlState.sql("select * from usertable where ycsb_key = " + random.nextInt(1000));
    }

    /**
     * Benchmark for JDBC get.
     */
    @Benchmark
    @Warmup(iterations = 1, time = 10)
    @Measurement(iterations = 1, time = 20)
    public void jdbcGet(JdbcState state) throws SQLException {
        state.stmt.setInt(1, random.nextInt(1000));
        try (ResultSet r = state.stmt.executeQuery()) {
            r.next();
        }
    }

    /**
     * Benchmark for KV get.
     */
    @Benchmark
    @Warmup(iterations = 1, time = 10)
    @Measurement(iterations = 1, time = 20)
    public void kvGet() {
        keyValueView.get(null, Tuple.create().set("ycsb_key", random.nextInt(1000)));
    }

    /**
     * Benchmark's entry point.
     */
    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(".*" + SelectBenchmark.class.getSimpleName() + ".*")
                .forks(0)
                .threads(1)
                .mode(Mode.SampleTime)
                .build();

        new Runner(opt).run();
    }

    /**
     * Benchmark state for {@link #sqlGet(SqlState)}. Holds {@link QueryProcessor} and {@link SessionId}.
     */
    @State(Scope.Benchmark)
    public static class SqlState {
        QueryProcessor queryProcessor = clusterNode.queryEngine();

        SessionId sessionId;

        /**
         * Initializes session.
         */
        @Setup
        public void setUp() {
            sessionId = queryProcessor.createSession(newBuilder()
                    .set(QueryProperty.DEFAULT_SCHEMA, "PUBLIC")
                    .set(QueryProperty.QUERY_TIMEOUT, TimeUnit.SECONDS.toMillis(20))
                    .build()
            );
        }

        /**
         * Closes resources.
         */
        @TearDown
        public void tearDown() {
            queryProcessor.closeSession(sessionId);
        }

        private List<List<Object>> sql(String sql, Object... args) {
            var context = QueryContext.create(SqlQueryType.ALL);

            return getAllFromCursor(await(queryProcessor.querySingleAsync(sessionId, context, sql, args)));
        }
    }

    /**
     * Benchmark state for {@link #jdbcGet(JdbcState)}. Holds {@link Connection} and {@link PreparedStatement}.
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
            String queryStr = "select * from " + TABLE_NAME + " where ycsb_key = ?";

            try {
                //noinspection CallToDriverManagerGetConnection
                conn = DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1:10800/");

                stmt = conn.prepareStatement(queryStr);
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }

        @TearDown
        public void tearDown() throws Exception {
            IgniteUtils.closeAll(stmt, conn);
        }
    }
}
