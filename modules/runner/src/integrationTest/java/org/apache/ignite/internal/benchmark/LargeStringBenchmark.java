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
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.internal.testframework.IgniteTestUtils;
import org.apache.ignite.internal.util.Constants;
import org.apache.ignite.sql.IgniteSql;
import org.apache.ignite.sql.ResultSet;
import org.apache.ignite.sql.SqlRow;
import org.apache.ignite.table.IgniteTables;
import org.apache.ignite.table.KeyValueView;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Group;
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
 * Benchmarks for KV operations with large string fields.
 */
@State(Scope.Benchmark)
@Fork(1)
@Threads(1)
@Warmup(iterations = 10, time = 2)
@Measurement(iterations = 20, time = 2)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class LargeStringBenchmark extends AbstractMultiNodeBenchmark {

    private KeyValueView<Integer, String> keyValueView;

    private IgniteSql sql;

    private String localString;

    @Param({"1", "2", "4", "5", "10", "15"})
    private int valueSizeMb;

    @Param({"1", "3"})
    private int clusterSize;

    /**
     * Setup.
     */
    @Setup
    public void setUp() {
        sql = publicIgnite.sql();

        String stmt = format("CREATE TABLE t (id INT PRIMARY KEY, val VARCHAR({}))", Integer.MAX_VALUE);
        try (ResultSet<SqlRow> rs = sql.execute(stmt)) {
            assertTrue(rs.wasApplied());
        }

        int length = Constants.MiB * valueSizeMb;
        localString = IgniteTestUtils.randomString(new Random(), length);

        IgniteTables tables = publicIgnite.tables();
        keyValueView = tables.table("public.t").keyValueView(Integer.class, String.class);
    }

    // KV

    /**
     * Benchmark for read/writer KV workload. Writer threads.
     */
    @Group("kv_rw_ro")
    @Benchmark
    public void rwRoWriter(SharedState sharedState) {
        int id = sharedState.id.incrementAndGet();

        keyValueView.put(null, id, localString);
    }

    /**
     * Benchmark for read/writer KV workload. Reader threads.
     */
    @Group("kv_rw_ro")
    @Benchmark
    public void rwRoReader(SharedState sharedState, Blackhole bh) {
        int id = ThreadLocalRandom.current().nextInt(sharedState.id.get() + 1);
        bh.consume(keyValueView.getNullable(null, id));
    }

    /**
     * Benchmark for write only KV put workload.
     */
    @Benchmark
    public void rwOnly(SharedState sharedState) {
        int id = sharedState.id.incrementAndGet();

        keyValueView.put(null, id, localString);
    }

    // SQL

    /**
     * Benchmark for read/writer SQL workload. Writer threads.
     */
    @Group("sql_rw_ro")
    @Benchmark
    public void sqlRwRoWriter(SharedState sharedState, Blackhole bh) {
        int id = sharedState.id.incrementAndGet();

        try (ResultSet<SqlRow> rs = sql.execute("INSERT INTO t VALUES(?, ?)", id, localString)) {
            bh.consume(rs);
        }
    }

    /**
     * Benchmark for read/writer SQL workload. Reader threads.
     */
    @Group("sql_rw_ro")
    @Benchmark
    public void sqlRwRoReader(SharedState sharedState, Blackhole bh) {
        int id = ThreadLocalRandom.current().nextInt(sharedState.id.get() + 1);

        try (ResultSet<SqlRow> rs = sql.execute("SELECT val FROM t WHERE id=?", id)) {
            bh.consume(rs.hasNext());
        }
    }

    /**
     * Benchmark for write-only SQL insert workload.
     */
    @Benchmark
    public void sqlRwOnly(SharedState sharedState, Blackhole bh) {
        int id = sharedState.id.incrementAndGet();

        try (ResultSet<SqlRow> rs = sql.execute("INSERT INTO t VALUES(?, ?)", id, localString)) {
            bh.consume(rs);
        }
    }

    /**
     * Shared state.
     */
    @State(Scope.Benchmark)
    public static class SharedState {
        private final AtomicInteger id = new AtomicInteger();
    }

    /**
     * Benchmark's entry point.
     */
    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(".*" + LargeStringBenchmark.class.getSimpleName() + ".*")
                .build();

        new Runner(opt).run();
    }

    @Override
    protected int nodes() {
        return clusterSize;
    }
}
