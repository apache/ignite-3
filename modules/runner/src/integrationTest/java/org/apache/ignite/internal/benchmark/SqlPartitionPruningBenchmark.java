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

import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.sql.IgniteSql;
import org.apache.ignite.sql.ResultSet;
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
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

/**
 * Benchmark that demonstrates performance gains of partition pruning.
 */
@State(Scope.Benchmark)
@Fork(1)
@Threads(1)
@Warmup(iterations = 10, time = 2)
@Measurement(iterations = 20, time = 2)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@SuppressWarnings({"unused"})
public class SqlPartitionPruningBenchmark extends AbstractMultiNodeBenchmark {

    private static final int TABLE_SIZE = 30_000;

    @Param({"1", "2", "3"})
    private int clusterSize;

    private IgniteSql sql;

    /** Creates tables. */
    @Setup
    public void createTables() {
        createTable("usertable2",
                List.of(
                        "key1 int",
                        "key2 int",
                        "field1   varchar(100)",
                        "field2   varchar(100)",
                        "field3   varchar(100)",
                        "field4   varchar(100)",
                        "field5   varchar(100)",
                        "field6   varchar(100)",
                        "field7   varchar(100)",
                        "field8   varchar(100)",
                        "field9   varchar(100)",
                        "field10  varchar(100)"
                ),
                List.of("key1", "key2"),
                List.of("key1")
        );

        createTable("usertable3",
                List.of(
                        "key1 int",
                        "key2 int",
                        "field1   varchar(100)"
                ),
                List.of("key1", "key2"),
                List.of("key1")
        );

        initTable("usertable2", 10);
        initTable("usertable3", 1);

        sql = publicIgnite.sql();
    }

    private static void initTable(String tableName, int fieldCount) {
        KeyValueView<Tuple, Tuple> keyValueView = publicIgnite.tables().table(tableName).keyValueView();

        String query = format("CREATE INDEX {}_sorted_idx ON {} USING SORTED (key1, key2)", tableName, tableName);
        try (var rs = publicIgnite.sql().execute(query)) {
            while (rs.hasNext()) {
                rs.next();
            }
        }

        int id = 0;

        for (int i = 0; i < TABLE_SIZE; i++) {
            Tuple t = Tuple.create();
            for (int j = 1; j <= fieldCount; j++) {
                t.set("field" + j, FIELD_VAL);
            }

            Tuple key = Tuple.create().set("key1", id).set("key2", id);
            id++;

            keyValueView.put(null, key, t);
        }
    }

    /** Select by key - should use key value plan. */
    @Benchmark
    public void selectByKey(Blackhole bh) {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        int key = random.nextInt(TABLE_SIZE);

        try (var rs = sql.execute("SELECT * FROM usertable2 WHERE key1=? and key2=?", key, key)) {
            expectSingleRecord(rs, bh);
        }
    }

    /** Select by a single colocation key - should use a scan with partition pruning. */
    @Benchmark
    public void selectWithPruning(Blackhole bh) {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        int key = random.nextInt(TABLE_SIZE);

        try (var rs = sql.execute("SELECT * FROM usertable2 WHERE key1=?", key)) {
            expectSingleRecord(rs, bh);
        }
    }

    /** Select by a single colocation key - should use a scan w/o partition pruning because such predicate is too complex. */
    @Benchmark
    public void selectWithNoPruning(Blackhole bh) {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        int key = random.nextInt(TABLE_SIZE);

        try (var rs = sql.execute("SELECT * FROM usertable2 WHERE key1 >= ? and key1 < ?", key, key + 1)) {
            expectSingleRecord(rs, bh);
        }
    }

    /** Correlated subquery with partition pruning .*/
    @Benchmark
    public void selectCorrelated(Blackhole bh) {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        int key = random.nextInt(TABLE_SIZE);

        String query = "SELECT * FROM usertable2 as cor WHERE EXISTS "
                + "(SELECT 1 FROM usertable3 WHERE usertable3.key1 = cor.key1) AND key1=?";

        try (var rs = sql.execute(query, key)) {
            expectSingleRecord(rs, bh);
        }
    }

    /** Correlated subquery without partition pruning .*/
    @Benchmark
    public void selectCorrelatedNoPruning(Blackhole bh) {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        int key = random.nextInt(TABLE_SIZE);

        String query = "SELECT * FROM usertable2 as cor WHERE EXISTS "
                + "(SELECT 1 FROM usertable3 WHERE usertable3.key1 >= cor.key1 AND usertable3.key1 < cor.key1 + 1) AND key1=?";

        try (var rs = sql.execute(query, key)) {
            expectSingleRecord(rs, bh);
        }
    }

    /**
     * Benchmark's entry point.
     */
    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(".*" + SqlPartitionPruningBenchmark.class.getSimpleName() + ".selectCorrelated*")
                .build();

        new Runner(opt).run();
    }

    @Override
    protected int nodes() {
        return clusterSize;
    }

    private static void expectSingleRecord(ResultSet<SqlRow> rs, Blackhole bh) {
        int i = 0;
        while (rs.hasNext()) {
            bh.consume(rs.next());
            i += 1;
        }
        if (i != 1) {
            throw new IllegalArgumentException("There should be exactly 1 output row but got " + i);
        }
    }
}
