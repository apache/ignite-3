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
import org.apache.ignite.internal.sql.engine.QueryProcessor;
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

@State(Scope.Benchmark)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class InsertBenchmark extends AbstractOneNodeBenchmark {
    @Benchmark
    @Warmup(iterations = 1, time = 10)
    @Measurement(iterations = 1, time = 20)
    public void sqlInsert(SqlState state) {
        state.executeQuery();
    }

    @Benchmark
    @Warmup(iterations = 1, time = 10)
    @Measurement(iterations = 1, time = 20)
    public void kvInsert(KVState state) {
        state.executeQuery();
    }

    @Benchmark
    @Warmup(iterations = 1, time = 10)
    @Measurement(iterations = 1, time = 20)
    public void jdbcInsert(JDBCState state) throws SQLException {
        state.executeQuery();
    }

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(".*" + InsertBenchmark.class.getSimpleName() + ".*")
                .forks(0)
                .threads(1)
                .mode(Mode.SampleTime)
                .build();

        new Runner(opt).run();
    }

    @State(Scope.Benchmark)
    public static class SqlState {
        private String queryStr;
        private QueryProcessor q = clusterNode.queryEngine();

        @Setup
        public void setUp() {
            var fieldsQ = IntStream.range(1, 11).mapToObj(i -> "field" + i).collect(joining(","));
            var valQ = IntStream.range(1, 11).mapToObj(i -> "'" + fieldVal + "'").collect(joining(","));

            queryStr = String.format("insert into usertable(%s, %s)", "ycsb_key", fieldsQ) + "values(%s, " + String.format("%s);", valQ);
        }

        private int id = 0;

        public void executeQuery() {
            q.queryAsync("PUBLIC", String.format(queryStr, id++)).get(0).join();
        }
    }

    @State(Scope.Benchmark)
    public static class JDBCState {
        private String queryStr;

        private Connection conn;

        private PreparedStatement stmt;

        private int id;

        @Setup
        public void setUp() throws SQLException {
            var fieldsQ = IntStream.range(1, 11).mapToObj(i -> "field" + i).collect(joining(","));
            var valQ = IntStream.range(1, 11).mapToObj(i -> "'" + fieldVal + "'").collect(joining(","));

            queryStr = String.format("insert into usertable(%s, %s)", "ycsb_key", fieldsQ) + "values(?, " + String.format("%s);", valQ);

            conn = DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1:10800/");

            stmt = conn.prepareStatement(queryStr);
        }

        @TearDown
        public void tearDow() throws Exception {
            IgniteUtils.closeAll(stmt, conn);
        }

        public void executeQuery() throws SQLException {
            stmt.setInt(1, id++);
            stmt.executeUpdate();
        }
    }

    @State(Scope.Benchmark)
    public static class KVState {
        private Tuple tuple;

        private int id = 0;

        private KeyValueView<Tuple, Tuple> kvView1 = clusterNode.tables().table("usertable").keyValueView();

        @Setup
        public void setUp() {
            tuple = Tuple.create();
            for (int i = 0; i < 10; i++) {
                tuple.set("field" + 1, fieldVal);
            }
        }

        public void executeQuery() {
            kvView1.put(null, Tuple.create().set("ycsb_key", id++), tuple);
        }
    }
}
