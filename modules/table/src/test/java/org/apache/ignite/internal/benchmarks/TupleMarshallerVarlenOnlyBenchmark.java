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

import static org.apache.ignite.internal.type.NativeTypes.BYTES;
import static org.apache.ignite.internal.type.NativeTypes.INT64;
import static org.apache.ignite.internal.type.NativeTypes.STRING;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.DefaultValueProvider;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.marshaller.TupleMarshaller;
import org.apache.ignite.internal.schema.row.Row;
import org.apache.ignite.internal.table.KeyValueTestUtils;
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
import org.openjdk.jmh.runner.options.OptionsBuilder;

/**
 * Serializer benchmark.
 */
@State(Scope.Benchmark)
@Warmup(iterations = 10, time = 1)
@Measurement(iterations = 20, time = 1)
@BenchmarkMode({Mode.AverageTime})
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Fork(jvmArgs = "-Djava.lang.invoke.stringConcat=BC_SB" /* Workaround for Java 9+ */, value = 1)
@SuppressWarnings("InstanceVariableMayNotBeInitialized")
public class TupleMarshallerVarlenOnlyBenchmark {
    /** Type of the generated payload. */
    public enum Type {
        BYTES, PREDEFINED_STRING, RANDOM_STRING
    }

    /** Random. */
    private Random rnd;

    /** Tuple marshaller. */
    private TupleMarshaller marshaller;

    /** Object fields count. */
    @Param({"2", "10", "100"})
    public int fieldsCount;

    /** Payload size. */
    @Param({"250", "64000", "66000"})
    public int dataSize;

    /** Nullable cols. */
    //    @Param({"true", "false"})
    public boolean nullable = true;

    @Param({"a", "ж", "我"})
    public String character;

    /** Column types. */
    @Param({"PREDEFINED_STRING"})
    public Type type;

    /** Schema descriptor. */
    private SchemaDescriptor schema;

    /** Value. */
    private Object val;

    /**
     * Runner.
     */
    public static void main(String[] args) throws RunnerException {
        new Runner(
                new OptionsBuilder()
                        .include(TupleMarshallerVarlenOnlyBenchmark.class.getSimpleName())
                        .build()
        ).run();
    }

    /**
     * Setup.
     */
    @Setup
    public void init() {
        final long seed = System.currentTimeMillis();

        rnd = new Random(seed);

        schema = new SchemaDescriptor(
                42,
                new Column[]{new Column("KEY", INT64, false, DefaultValueProvider.constantProvider(0L))},
                IntStream.range(0, fieldsCount).boxed()
                        .map(i -> new Column("COL" + i, type == Type.BYTES ? BYTES : STRING, nullable))
                        .toArray(Column[]::new)
        );

        marshaller = KeyValueTestUtils.createMarshaller(schema);

        switch (type) {
            case RANDOM_STRING: {
                final byte[] data = new byte[dataSize / fieldsCount];

                for (int i = 0; i < data.length; i++) {
                    data[i] = (byte) (rnd.nextInt() & 0x7F);
                }

                val = new String(data, StandardCharsets.ISO_8859_1); // Latin1 string.

                break;
            }
            case PREDEFINED_STRING: {
                val = character.repeat(dataSize / fieldsCount / character.getBytes(StandardCharsets.UTF_8).length);

                break;
            }
            case BYTES: {
                rnd.nextBytes((byte[]) (val = new byte[dataSize / fieldsCount]));
                break;
            }
            default: throw new IllegalStateException("Unknown type " + type);
        }
    }

    /**
     * Measure tuple build then marshall.
     *
     * @param bh Black hole.
     */
    @Benchmark
    public void measureTupleBuildAndMarshallerCost(Blackhole bh) {
        List<Column> cols = schema.valueColumns();

        final Tuple valBld = Tuple.create(cols.size());

        for (int i = 0; i < cols.size(); i++) {
            valBld.set(cols.get(i).name(), val);
        }

        Tuple keyTuple = Tuple.create(1).set("key", rnd.nextLong());

        final Row row = marshaller.marshal(keyTuple, valBld);

        bh.consume(row);
    }
}
