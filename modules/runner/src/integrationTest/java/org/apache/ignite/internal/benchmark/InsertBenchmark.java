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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.sql.IgniteSql;
import org.apache.ignite.sql.Session;
import org.apache.ignite.sql.Statement;
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
 * Benchmark for insertion operation, comparing KV, JDBC and SQL APIs.
 */
@State(Scope.Benchmark)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class InsertBenchmark extends AbstractOneNodeBenchmark {
    /**
     * Benchmark for SQL insert.
     */
    @Benchmark
    @Warmup(iterations = 1, time = 10)
    @Measurement(iterations = 1, time = 20)
    public void sqlInsert(SqlState state) {
        state.executeQuery();
    }

    /**
     * Benchmark for KV insert.
     */
    @Benchmark
    @Warmup(iterations = 1, time = 10)
    @Measurement(iterations = 1, time = 20)
    public void kvInsert(KvState state) {
        state.executeQuery();
    }

    /**
     * Benchmark for JDBC insert.
     */
    @Benchmark
    @Warmup(iterations = 1, time = 10)
    @Measurement(iterations = 1, time = 20)
    public void jdbcInsert(JdbcState state) throws SQLException {
        state.executeQuery();
    }

    /**
     * Benchmark's entry point.
     */
    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(".*" + InsertBenchmark.class.getSimpleName() + ".*")
                .forks(0)
                .threads(1)
                .mode(Mode.SampleTime)
                .build();

        new Runner(opt).run();
    }

    /**
     * Benchmark state for {@link #sqlInsert(SqlState)}. Holds {@link Session} and {@link Statement}.
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
            String fieldsQ = IntStream.range(1, 11).mapToObj(i -> "field" + i).collect(joining(","));
            String valQ = IntStream.range(1, 11).mapToObj(i -> "'" + FIELD_VAL + "'").collect(joining(","));

            String queryStr = String.format("insert into %s(%s, %s) values(?, %s);", TABLE_NAME, "ycsb_key", fieldsQ, valQ);

            IgniteSql sql = clusterNode.sql();

            statement = sql.createStatement(queryStr);
            session = sql.createSession();
        }

        /**
         * Closes resources.
         */
        @TearDown
        public void tearDow() throws Exception {
            IgniteUtils.closeAll(session/*, statement*/);
        }

        private int id = 0;

        void executeQuery() {
            session.execute(null, statement, id++);
        }
    }

    /**
     * Benchmark state for {@link #jdbcInsert(JdbcState)}. Holds {@link Connection} and {@link PreparedStatement}.
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
            String fieldsQ = IntStream.range(1, 11).mapToObj(i -> "field" + i).collect(joining(","));
            String valQ = IntStream.range(1, 11).mapToObj(i -> "'" + FIELD_VAL + "'").collect(joining(","));

            String queryStr = String.format("insert into %s(%s, %s) values(?, %s);", TABLE_NAME, "ycsb_key", fieldsQ, valQ);

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
     * Benchmark state for {@link #kvInsert(KvState)}. Holds {@link Tuple} and {@link KeyValueView} for the table.
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
}
