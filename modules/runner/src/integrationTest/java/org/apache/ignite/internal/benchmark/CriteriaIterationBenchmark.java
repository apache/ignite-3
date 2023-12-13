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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.sql.ResultSet;
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
public class CriteriaIterationBenchmark extends AbstractMultiNodeBenchmark {
    private static final IgniteLogger LOG = Loggers.forClass(CriteriaIterationBenchmark.class);

    private static final int TABLE_SIZE = 100_000;
    private static final String SELECT_ALL_FROM_USERTABLE = "select * from usertable";

    @Param("1")
    private int clusterSize;

    private KeyValueView<Tuple, Tuple> keyValueView;

    /**
     * Fills the table with data.
     */
    @Setup
    public void setUp() {
        keyValueView = clusterNode.tables().table(TABLE_NAME).keyValueView();

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
     * Benchmark for SQL select via embedded client.
     */
    @Benchmark
    public void sqlIterate(SqlState sqlState) {
        try (var rs = sqlState.sql(SELECT_ALL_FROM_USERTABLE)) {
            while (rs.hasNext())
                rs.next();
        }
    }

    /**
     * Benchmark for Criteria get via embedded client.
     */
    @Benchmark
    public void criteriaIterate() {
        try (var cur = keyValueView.queryCriteria(null, null)) {
            while (cur.hasNext())
                cur.next();
        }
    }

    /**
     * Benchmark's entry point.
     */
    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(".*" + CriteriaIterationBenchmark.class.getSimpleName() + ".*")
                .param("fsync", "false")
                .build();

        new Runner(opt).run();
    }

    /**
     * Benchmark state for {@link #sqlIterate(SqlState)}.
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

        private ResultSet<SqlRow> sql(String sql, Object... args) {
            return session.execute(null, sql, args);
        }
    }

    @Override
    protected int nodes() {
        return clusterSize;
    }
}
