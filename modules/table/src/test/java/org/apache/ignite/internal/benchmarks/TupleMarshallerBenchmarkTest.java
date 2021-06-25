/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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

import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.IntStream;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.Columns;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.SchemaRegistry;
import org.apache.ignite.internal.schema.marshaller.TupleMarshaller;
import org.apache.ignite.internal.schema.row.Row;
import org.apache.ignite.internal.table.TupleBuilderImpl;
import org.apache.ignite.internal.table.TupleMarshallerImpl;
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
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import static org.apache.ignite.internal.schema.NativeTypes.BYTES;
import static org.apache.ignite.internal.schema.NativeTypes.LONG;

/**
 * Serializer benchmark.
 */
@State(Scope.Benchmark)
@Warmup(iterations = 1, time = 15, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 1, time = 30, timeUnit = TimeUnit.SECONDS)
@BenchmarkMode({Mode.Throughput, Mode.AverageTime})
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Fork(jvmArgs = "-Djava.lang.invoke.stringConcat=BC_SB" /* Workaround for Java 9+ */, value = 1)
public class TupleMarshallerBenchmarkTest {
    /** Random. */
    private Random rnd = new Random();

    /** Tuple marshaller. */
    private TupleMarshaller marshaller;

    /** Object fields count. */
    @Param({"1", "10", "100"})
    public int fieldsCount;

    /** Nullable cols. */
    @Param({"true", "false"})
    public boolean nullable;

    /** Fixed length. */
    @Param({"true", "false"})
    public boolean fixedLen;

    /** Schema descriptor. */
    private SchemaDescriptor schema;

    private Object[] vals;

    /**
     * Runner.
     */
    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
            .include(TupleMarshallerBenchmarkTest.class.getSimpleName())
            .build();

        new Runner(opt).run();
    }

    /**
     *
     */
    @Setup
    public void init() {
        long seed = System.currentTimeMillis();

        rnd = new Random(seed);

        schema = new SchemaDescriptor(
            UUID.randomUUID(),
            42,
            new Column[] {new Column("key", LONG, false)},
            IntStream.range(0, fieldsCount).boxed()
                .map(i -> new Column("col" + i, fixedLen ? LONG : BYTES, nullable))
                .toArray(Column[]::new)
        );

        marshaller = new TupleMarshallerImpl(new SchemaRegistry() {
            @Override public SchemaDescriptor schema() {
                return schema;
            }

            @Override public SchemaDescriptor schema(int ver) {
                return schema;
            }
        });

        Supplier<Object> gen = fixedLen ? () -> rnd.nextLong() :
            new Supplier<>() {
                private final byte[] bytes = new byte[8];

                @Override public Object get() {
                    rnd.nextBytes(bytes);

                    return bytes;
                }
            };

        vals = new Object[schema.valueColumns().length()];

        for (int i = 0; i < vals.length; i++)
            vals[i] = gen.get();
    }

    /**
     * Measure tuple build then marshall.
     *
     * @param bh Black hole.
     */
    @Benchmark
    public void measureTupleBuildAndMarshallerCost(Blackhole bh) {
        final Columns cols = schema.valueColumns();

        final TupleBuilderImpl valBld = new TupleBuilderImpl(schema);

        for (int i = 0; i < cols.length(); i++)
            valBld.set(cols.column(i).name(), vals[i]);

        Tuple keyTuple = new TupleBuilderImpl(schema).set("key", rnd.nextLong()).build();

        final Row row = marshaller.marshal(keyTuple, valBld.build());

        bh.consume(row);
    }
}
