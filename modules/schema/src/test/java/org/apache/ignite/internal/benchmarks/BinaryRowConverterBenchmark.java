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

package org.apache.ignite.internal.benchmarks;

import static java.util.Objects.requireNonNull;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.randomString;
import static org.apache.ignite.internal.type.NativeTypes.INT64;

import it.unimi.dsi.fastutil.ints.IntList;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.internal.binarytuple.BinaryTuple;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.BinaryRowConverter;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.row.RowAssembler;
import org.apache.ignite.internal.type.NativeTypes;
import org.apache.ignite.internal.util.Pair;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

/** Benchmark to measure {@link BinaryRowConverter}. */
@State(Scope.Benchmark)
@Warmup(time = 1, iterations = 10, timeUnit = TimeUnit.SECONDS)
@Measurement(time = 1, iterations = 60, timeUnit = TimeUnit.SECONDS)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Fork(1)
public class BinaryRowConverterBenchmark {
    private BinaryRowConverter converter;

    private int idx;
    private final List<Pair<BinaryTuple, BinaryRow>> dataset = new ArrayList<>();

    /** Initialize. */
    @Setup
    public void init() {
        long seed = System.currentTimeMillis();

        System.err.println("Seed: " + seed);

        Random rnd = new Random(seed);

        SchemaDescriptor schema = new SchemaDescriptor(
                1,
                List.of(
                        new Column("ID", INT64, false),
                        new Column("C1", INT64, true),
                        new Column("LONG_COL", INT64, true),
                        new Column("STRING_COL", NativeTypes.stringOf(256), true),
                        new Column("C4", INT64, true),
                        new Column("C5", INT64, true)
                ),
                IntList.of(0),
                null
        );

        converter = BinaryRowConverter.columnsExtractor(
                schema,
                requireNonNull(schema.column("STRING_COL")).positionInRow(),
                requireNonNull(schema.column("LONG_COL")).positionInRow()
        );

        int baseStringLength = 32;
        for (int i = 0; i < 10; i++) {
            BinaryRow row = new RowAssembler(schema, -1)
                    .appendLong(rnd.nextLong()) // ID
                    .appendLong(rnd.nextLong()) // C1
                    .appendLong(rnd.nextLong()) // LONG_COL
                    .appendString(randomString(rnd, baseStringLength + i)) // STRING_COL
                    .appendLong(rnd.nextLong()) // C4
                    .appendLong(rnd.nextLong()) // C5
                    .build();

            BinaryTuple indexColumns = converter.extractColumns(row);
            dataset.add(new Pair<>(indexColumns, row));
        }
    }

    /**
     * Measure matching avoiding tuple materialization.
     *
     * @param bh Black hole.
     */
    @Benchmark
    public void onlyMatch(Blackhole bh) {
        Pair<BinaryTuple, BinaryRow> pair = dataset.get(nextIdx());

        bh.consume(converter.columnsMatch(pair.getSecond(), pair.getFirst()));
    }

    /**
     * Measure matching through tuple materialization.
     *
     * @param bh Black hole.
     */
    @Benchmark
    public void extractAndMatch(Blackhole bh) {
        Pair<BinaryTuple, BinaryRow> pair = dataset.get(nextIdx());

        bh.consume(converter.extractColumns(pair.getSecond()).byteBuffer().equals(pair.getFirst().byteBuffer()));
    }

    private int nextIdx() {
        idx++;
        if (idx >= dataset.size()) {
            idx = 0;
        }

        return idx;
    }

    /**
     * Benchmark run method.
     */
    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .addProfiler("gc")
                .include(BinaryRowConverterBenchmark.class.getSimpleName())
                .build();

        new Runner(opt).run();
    }

}
