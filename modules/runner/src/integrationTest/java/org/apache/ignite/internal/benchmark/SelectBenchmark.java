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

@State(Scope.Benchmark)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class SelectBenchmark extends AbstractOneNodeBenchmark {

    private Random r = new Random();

    private KeyValueView<Tuple, Tuple> keyValueView;

    @Setup
    public void setUp() throws IOException, ClassNotFoundException {
        int id = 0;

        keyValueView = clusterNode.tables().table(TABLE_NAME).keyValueView();

        for (int i = 0; i < 1000; i++) {
            Tuple t = Tuple.create();
            for (int j = 1; j <= 10; j++) {
                t.set("field" + j, fieldVal);
            }

            keyValueView.put(null, Tuple.create().set("ycsb_key", id++), t);
        }
    }

    @Benchmark
    @Warmup(iterations = 1, time = 10)
    @Measurement(iterations = 1, time = 20)
    public void sqlGet(SqlState sqlState) {
        sqlState.sql("select * from usertable where ycsb_key = " + r.nextInt(1000));
    }

    @Benchmark
    @Warmup(iterations = 1, time = 10)
    @Measurement(iterations = 1, time = 20)
    public void jdbcGet(JDBCState state) throws SQLException {
        state.stmt.setInt(1, r.nextInt(1000));
        try (ResultSet r = state.stmt.executeQuery()) {
            r.next();
        }
    }

    @Benchmark
    @Warmup(iterations = 1, time = 10)
    @Measurement(iterations = 1, time = 20)
    public void kvGet() {
        keyValueView.get(null, Tuple.create().set("ycsb_key", r.nextInt(1000)));
    }

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(".*" + SelectBenchmark.class.getSimpleName() + ".*")
                .forks(0)
                .threads(1)
                .mode(Mode.SampleTime)
                .build();

        new Runner(opt).run();
    }

    @State(Scope.Benchmark)
    public static class SqlState {
        QueryProcessor queryProcessor = clusterNode.queryEngine();
        SessionId sessionId = queryProcessor.createSession(newBuilder()
                .set(QueryProperty.DEFAULT_SCHEMA, "PUBLIC")
                .set(QueryProperty.QUERY_TIMEOUT, TimeUnit.SECONDS.toMillis(20))
                .build()
        );

        private List<List<Object>> sql(String sql, Object... args) {
            var context = QueryContext.create(SqlQueryType.ALL);

            return getAllFromCursor(
                    await(queryProcessor.querySingleAsync(sessionId, context, sql, args))
            );
        }
    }

    @State(Scope.Benchmark)
    public static class JDBCState {
        public String queryStr = "select * from " + TABLE_NAME + " where ycsb_key = ?";

        public Connection conn;

        public PreparedStatement stmt;

        @Setup
        public void setUp() {
            try {
                //noinspection CallToDriverManagerGetConnection
                conn = DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1:10800/");

                stmt = conn.prepareStatement(queryStr);
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }

        @TearDown
        public void tearDown() throws SQLException {
            stmt.close();
            conn.close();
        }
    }
}
