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

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.sql.Session;
import org.apache.ignite.table.KeyValueView;
import org.apache.ignite.table.Tuple;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
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
 * Benchmark that runs sql queries via embedded client on single node cluster.
 */
@State(Scope.Benchmark)
@Fork(1)
@Threads(1)
@Warmup(iterations = 10, time = 2)
@Measurement(iterations = 20, time = 2)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@SuppressWarnings({"WeakerAccess", "unused"})
public class SqlOneNodeBenchmark extends AbstractOneNodeBenchmark {
    private static final int TABLE_SIZE = 30_000;

    private Session session;

    /** Fills the table with data. */
    @Setup
    public void setUp() throws IOException {
        KeyValueView<Tuple, Tuple> keyValueView = clusterNode.tables().table(TABLE_NAME).keyValueView();

        Tuple payload = Tuple.create();
        for (int j = 1; j <= 10; j++) {
            payload.set("field" + j, FIELD_VAL);
        }

        int batchSize = 1_000;
        Map<Tuple, Tuple> batch = new HashMap<>();
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

        session = clusterNode.sql().createSession();
    }

    /** Benchmark that measures performance of `SELECT count(*)` query over entire table. */
    @Benchmark
    public void countAll(Blackhole bh) {
        try (var rs = session.execute(null, "SELECT count(*) FROM usertable")) {
            bh.consume(rs.next());
        }
    }

    /** Benchmark that measures performance of `SELECT count(1)` query over entire table. */
    @Benchmark
    public void count1(Blackhole bh) {
        try (var rs = session.execute(null, "SELECT count(1) FROM usertable")) {
            bh.consume(rs.next());
        }
    }

    /** Benchmark that measures performance of `SELECT count(key)` query over entire table. */
    @Benchmark
    public void countKey(Blackhole bh) {
        try (var rs = session.execute(null, "SELECT count(ycsb_key) FROM usertable")) {
            bh.consume(rs.next());
        }
    }

    /** Benchmark that measures performance of `SELECT count(val)` query over entire table. */
    @Benchmark
    public void countVal(Blackhole bh) {
        try (var rs = session.execute(null, "SELECT count(field2) FROM usertable")) {
            bh.consume(rs.next());
        }
    }

    /** Benchmark that measures performance of `SELECT *` query over entire table. */
    @Benchmark
    public void selectAll(Blackhole bh) {
        try (var rs = session.execute(null, "SELECT * FROM usertable")) {
            while (rs.hasNext()) {
                bh.consume(rs.next());
            }
        }
    }

    /**
     * Benchmark to measure overhead of query initialisation.
     */
    @Benchmark
    @OutputTimeUnit(TimeUnit.MICROSECONDS)
    public void selectAllFromSystemRange(Blackhole bh) {
        try (var rs = session.execute(null, "SELECT * FROM TABLE(system_range(0, 1))")) {
            while (rs.hasNext()) {
                bh.consume(rs.next());
            }
        }
    }

    /**
     * Benchmark's entry point.
     */
    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(".*" + SqlOneNodeBenchmark.class.getSimpleName() + ".*")
                .build();

        new Runner(opt).run();
    }
}


