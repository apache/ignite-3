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

import static org.apache.ignite.internal.util.IgniteUtils.capacity;
import static org.apache.ignite.table.criteria.Criteria.columnValue;
import static org.apache.ignite.table.criteria.Criteria.equalTo;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.lang.Cursor;
import org.apache.ignite.sql.ResultSet;
import org.apache.ignite.sql.Session;
import org.apache.ignite.sql.SqlRow;
import org.apache.ignite.table.KeyValueView;
import org.apache.ignite.table.Tuple;
import org.apache.ignite.table.criteria.Criteria;
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
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

/**
 * Benchmark for criteria operation, comparing KV, SQL APIs.
 */
@State(Scope.Benchmark)
@Fork(1)
@Threads(1)
@Warmup(iterations = 10, time = 2)
@Measurement(iterations = 10, time = 2)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@SuppressWarnings({"WeakerAccess", "unused"})
public class CriteriaMultiNodeBenchmark extends AbstractMultiNodeBenchmark {
    private static final IgniteLogger LOG = Loggers.forClass(CriteriaMultiNodeBenchmark.class);

    private static final int TABLE_SIZE = 10_000;

    private static final String SELECT_FROM_USERTABLE = "select * from usertable where ycsb_key = ?";

    private static final String SELECT_ALL_FROM_USERTABLE = "select * from usertable";

    private final Random random = new Random();

    @Param("2")
    private int clusterSize;

    /**
     * Fills the table with data.
     */
    @Setup
    public void setUp() {
        KeyValueView<Tuple, Tuple> keyValueView = clusterNode.tables().table(TABLE_NAME).keyValueView();

        Tuple payload = Tuple.create();
        for (int j = 1; j <= 10; j++) {
            payload.set("field" + j, FIELD_VAL);
        }

        int batchSize = 10_000;
        Map<Tuple, Tuple> batch = new HashMap<>(capacity(batchSize));
        for (int i = 0; i < TABLE_SIZE; i++) {
            batch.put(Tuple.create().set("ycsb_key", i), payload);

            if (batch.size() == batchSize) {
                keyValueView.putAll(null, batch);

                batch.clear();
            }
        }

        if (!batch.isEmpty()) {
            keyValueView.putAll(null, batch);

            batch.clear();
        }
    }

    /**
     * Benchmark for SQL select via thin client.
     */
    @Benchmark
    public void sqlGet(ThinClientState state) {
        try (var session = state.createSession()) {
            try (var rs = state.sql(session, SELECT_FROM_USERTABLE, random.nextInt(TABLE_SIZE))) {
                rs.next();
            }
        }
    }

    /**
     * Benchmark for Criteria get via thin client.
     */
    @Benchmark
    public void criteriaGet(ThinClientState state) {
        try (Cursor<Tuple> cur = state.query(columnValue("ycsb_key", equalTo(random.nextInt(TABLE_SIZE))))) {
            cur.next();
        }
    }

    /**
     * Benchmark for SQL select via embedded client.
     */
//    @Benchmark
    @Warmup(iterations = 1, time = 2)
    @Measurement(iterations = 1, time = 2)
    public void sqlIterate(ThinClientState state) {
        try (var session = state.createSession()) {
            try (var rs = state.sql(session, SELECT_ALL_FROM_USERTABLE)) {
                while (rs.hasNext()) {
                    rs.next();
                }
            }
        }
    }

    /**
     * Benchmark for Criteria get via embedded client.
     */
//    @Benchmark
    @Warmup(iterations = 1, time = 2)
    @Measurement(iterations = 1, time = 2)
    public void criteriaIterate(ThinClientState state) {
        try (Cursor<Tuple> cur = state.query(null)) {
            while (cur.hasNext()) {
                cur.next();
            }
        }
    }

    /**
     * Benchmark's entry point.
     */
    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(".*" + CriteriaMultiNodeBenchmark.class.getSimpleName() + ".*")
                .param("fsync", "false")
                .build();

        new Runner(opt).run();
    }

    /**
     * Benchmark state for {@link #sqlIterate(ThinClientState)} , {@link #criteriaIterate(ThinClientState)}.
     *
     * <p>Holds {@link IgniteClient} and {@link Session}.
     */
    @State(Scope.Benchmark)
    public static class ThinClientState {
        private IgniteClient client;

        /**
         * Initializes session and statement.
         */
        @Setup
        public void setUp() {
            client = IgniteClient.builder().addresses("127.0.0.1:10800").build();
        }

        @Nullable Tuple get(Tuple key) {
            return client.tables().table(TABLE_NAME).keyValueView().get(null, key);
        }

        Session createSession() {
            return client.sql().createSession();
        }

        ResultSet<SqlRow> sql(Session session, String query, Object... args) {
            return session.execute(null, query, args);
        }

        Cursor<Tuple> query(@Nullable Criteria criteria) {
            return client.tables().table(TABLE_NAME).recordView().query(null, criteria);
        }
    }

    @Override
    protected int nodes() {
        return clusterSize;
    }
}
