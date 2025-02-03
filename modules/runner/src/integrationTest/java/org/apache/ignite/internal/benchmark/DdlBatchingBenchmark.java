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
import static org.apache.ignite.internal.testframework.TestIgnitionManager.PRODUCTION_CLUSTER_CONFIG_STRING;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.sql.IgniteSql;
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
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

/**
 * Benchmark that measures execution of a ddl script on single-node cluster with default configuration.
 */
@State(Scope.Benchmark)
@Fork(1)
@Threads(1)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 7, time = 1)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.SECONDS)
@SuppressWarnings({"WeakerAccess", "unused"})
public class DdlBatchingBenchmark extends AbstractMultiNodeBenchmark {
    private static final String DROP_TABLE_TEMPLATE = "DROP TABLE IF EXISTS table_{};";
    private static final String CREATE_TABLE_TEMPLATE = "CREATE TABLE table_{} (id INT PRIMARY KEY, c_1 INT, c_2 INT, c_3 INT, c_4 INT);";
    private static final String CREATE_INDEX_TEMPLATE = "CREATE INDEX table_{}_c_{}_ind ON table_{} (c_{});";

    @Param({"1", "2", "3"})
    private int numberOfTables;

    @Param({"0", "2", "4"})
    private int numberOfSecondaryIndexesPerTable;

    private IgniteSql sql;
    private String scriptText;

    /** Initializes script text. */
    @Setup
    public void setUp() throws IOException {
        sql = publicIgnite.sql();

        StringBuilder sb = new StringBuilder();

        for (int t = 0; t < numberOfTables; t++) {
            sb.append(format(DROP_TABLE_TEMPLATE, t)).append(System.lineSeparator());
            sb.append(format(CREATE_TABLE_TEMPLATE, t)).append(System.lineSeparator());

            for (int i = 1; i <= numberOfSecondaryIndexesPerTable; i++) {
                sb.append(format(CREATE_INDEX_TEMPLATE, t, i, t, i)).append(System.lineSeparator());
            }
        }

        scriptText = sb.toString();
    }

    /** Benchmark's body. */
    @Benchmark
    public void test(Blackhole bh) {
        sql.executeScript(scriptText);
    }

    /**
     * Benchmark's entry point.
     */
    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(".*" + DdlBatchingBenchmark.class.getSimpleName() + ".*")
                .build();

        new Runner(opt).run();
    }

    @Override
    protected int nodes() {
        return 1;
    }

    @Override
    protected void createDistributionZoneOnStartup() {
        // no-op
    }

    @Override
    protected void createTablesOnStartup() {
        // no-op
    }

    @Override
    protected @Nullable String clusterConfiguration() {
        return PRODUCTION_CLUSTER_CONFIG_STRING;
    }
}


