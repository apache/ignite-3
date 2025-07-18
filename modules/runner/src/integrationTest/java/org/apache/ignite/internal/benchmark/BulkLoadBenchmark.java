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

import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.sql.IgniteSql;
import org.apache.ignite.sql.Statement;
import org.apache.ignite.table.KeyValueView;
import org.apache.ignite.table.Tuple;
import org.apache.ignite.tx.Transaction;
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
 * Benchmark to measure effect of direct transaction mapping.
 *
 * <p>Inserts the whole dataset row by row. New explicit transaction is started every {@code batch} insertion.
 */
@State(Scope.Benchmark)
@Fork(1)
@Threads(1)
@BenchmarkMode(Mode.SingleShotTime)
@OutputTimeUnit(TimeUnit.SECONDS)
public class BulkLoadBenchmark extends AbstractMultiNodeBenchmark {
    @Param("3")
    private int clusterSize;

    @Param("32")
    private int partitionCount;

    @Param("1")
    private int replicaCount;

    @Param("100000")
    private int count;

    @Param("5")
    private int batchSize;

    /**
     * Benchmark for SQL insert via thin client.
     */
    @Benchmark
    public void sqlThinInsert(SqlThinState state) {
        state.upload(count, batchSize);
    }

    /**
     * Benchmark for KV insert via thin client.
     */
    @Benchmark
    public void kvThinInsert(KvThinState state) {
        state.upload(count, batchSize);
    }

    /**
     * Benchmark's entry point.
     */
    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(".*[.]" + BulkLoadBenchmark.class.getSimpleName() + ".*")
                .build();

        new Runner(opt).run();
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

            client = IgniteClient.builder().addresses("127.0.0.1:10800").build();

            String[] clientAddrs = getServerEndpoints(client);

            client.close();

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

        void upload(int count, int batch) {
            Transaction tx = null;
            for (int i = 0; i < count; i++) {
                if (i % batch == 0) {
                    if (tx != null) {
                        tx.commit();
                    }

                    tx = client.transactions().begin();
                }

                sql.execute(tx, statement, i);
            }
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

        /**
         * Initializes the tuple.
         */
        @Setup
        public void setUp() {
            for (int i = 1; i < 11; i++) {
                tuple.set("field" + i, FIELD_VAL);
            }

            client = IgniteClient.builder().addresses("127.0.0.1:10800").build();

            String[] clientAddrs = getServerEndpoints(client);

            client.close();

            client = IgniteClient.builder().addresses(clientAddrs).build();

            kvView = client.tables().table(TABLE_NAME).keyValueView();
        }

        @TearDown
        public void tearDown() throws Exception {
            closeAll(client);
        }

        void upload(int count, int batch) {
            Transaction tx = null;
            for (int i = 0; i < count; i++) {
                if (i % batch == 0) {
                    if (tx != null) {
                        tx.commit();
                    }

                    tx = client.transactions().begin();
                }

                kvView.put(tx, Tuple.create().set("ycsb_key", i), tuple);
            }
        }
    }

    private static String createInsertStatement() {
        String insertQueryTemplate = "insert into {}({}, {}) values(?, {})";

        String fieldsQ = IntStream.range(1, 11).mapToObj(i -> "field" + i).collect(joining(","));
        String valQ = IntStream.range(1, 11).mapToObj(i -> "'" + FIELD_VAL + "'").collect(joining(","));

        return format(insertQueryTemplate, TABLE_NAME, "ycsb_key", fieldsQ, valQ);
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
