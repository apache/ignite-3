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

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.internal.binarytuple.BinaryTupleBuilder;
import org.apache.ignite.internal.catalog.descriptors.CatalogColumnCollation;
import org.apache.ignite.internal.schema.PartialBinaryTupleMatcher;
import org.apache.ignite.internal.type.NativeTypes;
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
 * Benchmark for partial tuple comparator.
 *
 * <p>Results on 11th Gen Intel® Core™ i7-1165G7 @ 2.80GHz, OpenJDK 64-Bit Server VM, 21.0.7+6-LTS, Windows 10 Pro:
 * Benchmark                                         Mode  Cnt  Score    Error  Units
 * TupleComparatorBenchmark.testBytesFullCompare     avgt   10  0.050 ±  0.001  us/op
 * TupleComparatorBenchmark.testBytesPartialCompare  avgt   10  0.035 ±  0.001  us/op
 * TupleComparatorBenchmark.testStrFullCompare       avgt   10  0.734 ±  0.003  us/op
 * TupleComparatorBenchmark.testStrPartialCompare    avgt   10  0.390 ±  0.006  us/op
 * TupleComparatorBenchmark.testUuidFullCompare      avgt   10  0.018 ±  0.001  us/op
 * TupleComparatorBenchmark.testUuidPartialCompare   avgt   10  0.017 ±  0.001  us/op
 * TupleComparatorBenchmark.timestampFullCompare     avgt   10  0.017 ±  0.001  us/op
 * TupleComparatorBenchmark.timestampPartialCompare  avgt   10  0.017 ±  0.001  us/op
 */
@State(Scope.Benchmark)
@Fork(1)
@Threads(1)
@Warmup(iterations = 5, time = 2)
@Measurement(iterations = 10, time = 2)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class TupleComparatorBenchmark {
    PartialBinaryTupleMatcher strPartialBinaryTupleMatcher;
    PartialBinaryTupleMatcher bytePartialBinaryTupleMatcher;
    PartialBinaryTupleMatcher timestampPartialBinaryTupleMatcher;
    PartialBinaryTupleMatcher uuidPartialBinaryTupleMatcher;

    ByteBuffer strTupleReference;
    ByteBuffer strTuple;
    ByteBuffer strPartialTuple;

    ByteBuffer byteTupleReference;
    ByteBuffer byteTuple;
    ByteBuffer bytePartialTuple;

    ByteBuffer timestampTupleReference;
    ByteBuffer timestampTuple;
    ByteBuffer timestampPartialTuple;

    ByteBuffer uuidTupleReference;
    ByteBuffer uuidTuple;
    ByteBuffer uuidPartialTuple;

    /**
     * Prepare to start the benchmark.
     */
    @Setup
    public void setUp() {
        initForStr();
        initForBytes();
        initForTimestamp();
        initForUuid();
    }

    private void initForUuid() {
        uuidPartialBinaryTupleMatcher = new PartialBinaryTupleMatcher(
                List.of(CatalogColumnCollation.ASC_NULLS_LAST),
                List.of(NativeTypes.UUID)
        );

        uuidTupleReference = new BinaryTupleBuilder(1)
                .appendUuid(UUID.randomUUID())
                .build()
                .order(ByteOrder.LITTLE_ENDIAN);

        uuidTuple = ByteBuffer.allocateDirect(uuidTupleReference.capacity())
                .order(ByteOrder.LITTLE_ENDIAN)
                .put(uuidTupleReference);

        ByteBuffer tmpTuple = uuidTupleReference.rewind().limit(8).slice().order(ByteOrder.LITTLE_ENDIAN);

        uuidPartialTuple = ByteBuffer.allocateDirect(tmpTuple.capacity())
                .order(ByteOrder.LITTLE_ENDIAN)
                .put(tmpTuple);

        uuidTupleReference.clear();
        uuidTuple.clear();
        uuidPartialTuple.clear();
    }

    private void initForTimestamp() {
        timestampPartialBinaryTupleMatcher = new PartialBinaryTupleMatcher(
                List.of(CatalogColumnCollation.ASC_NULLS_LAST),
                List.of(NativeTypes.timestamp(3))
        );

        Clock clock = Clock.fixed(Instant.now().truncatedTo(ChronoUnit.SECONDS), ZoneId.systemDefault());
        Instant instant = clock.instant();

        timestampTupleReference = new BinaryTupleBuilder(1)
                .appendTimestamp(instant)
                .build()
                .order(ByteOrder.LITTLE_ENDIAN);

        timestampTuple = ByteBuffer.allocateDirect(timestampTupleReference.capacity())
                .order(ByteOrder.LITTLE_ENDIAN)
                .put(timestampTupleReference);

        ByteBuffer tmpTuple = timestampTupleReference.rewind().limit(8).slice().order(ByteOrder.LITTLE_ENDIAN);

        timestampPartialTuple = ByteBuffer.allocateDirect(tmpTuple.capacity())
                .order(ByteOrder.LITTLE_ENDIAN)
                .put(tmpTuple);

        timestampTupleReference.clear();
        timestampTuple.clear();
        timestampPartialTuple.clear();
    }

    private void initForBytes() {
        bytePartialBinaryTupleMatcher = new PartialBinaryTupleMatcher(
                List.of(CatalogColumnCollation.ASC_NULLS_LAST),
                List.of(NativeTypes.BYTES)
        );

        byte[] bytes = new byte[350];

        for (int i = 0; i < bytes.length; i++) {
            bytes[i] = (byte) (i % 10);
        }

        byteTupleReference = new BinaryTupleBuilder(1)
                .appendBytes(bytes)
                .build()
                .order(ByteOrder.LITTLE_ENDIAN);

        byteTuple = ByteBuffer.allocateDirect(byteTupleReference.capacity())
                .order(ByteOrder.LITTLE_ENDIAN)
                .put(byteTupleReference);

        ByteBuffer tmpTuple = byteTupleReference.rewind().limit(180).slice().order(ByteOrder.LITTLE_ENDIAN);

        bytePartialTuple = ByteBuffer.allocateDirect(tmpTuple.capacity())
                .order(ByteOrder.LITTLE_ENDIAN)
                .put(tmpTuple);

        byteTupleReference.clear();
        byteTuple.clear();
        bytePartialTuple.clear();
    }

    private void initForStr() {
        strPartialBinaryTupleMatcher = new PartialBinaryTupleMatcher(
                List.of(CatalogColumnCollation.ASC_NULLS_LAST),
                List.of(NativeTypes.STRING)
        );

        strTupleReference = new BinaryTupleBuilder(1)
                .appendString("Привет 你好".repeat(20))
                .build()
                .order(ByteOrder.LITTLE_ENDIAN);

        strTuple = ByteBuffer.allocateDirect(strTupleReference.capacity())
                .order(ByteOrder.LITTLE_ENDIAN)
                .put(strTupleReference);

        ByteBuffer tmpTuple = strTupleReference.rewind().limit(180).slice().order(ByteOrder.LITTLE_ENDIAN);

        strPartialTuple = ByteBuffer.allocateDirect(tmpTuple.capacity())
                .order(ByteOrder.LITTLE_ENDIAN)
                .put(tmpTuple);

        strTupleReference.clear();
        strTuple.clear();
        strPartialTuple.clear();
    }

    /**
     * Benchmarks the full comparison of string binary tuples using a match method from the strPartialBinaryTupleMatcher
     * instance. The result of the comparison is passed into a Blackhole to avoid dead code elimination during benchmarking.
     *
     * @param bh The Blackhole instance used to consume the comparison result for benchmarking purposes.
     */
    @Benchmark
    public void testStrFullCompare(Blackhole bh) {
        int r = strPartialBinaryTupleMatcher.match(strTuple, strTupleReference);

        bh.consume(r);
    }

    /**
     * Benchmarks the partial comparison of string binary tuples using the match method from the
     * `strPartialBinaryTupleMatcher` instance. The result of the comparison is passed into a Blackhole
     * to prevent dead code elimination and to measure performance accurately during benchmarking.
     *
     * @param bh The Blackhole instance used to consume the result of the comparison for benchmarking purposes.
     */
    @Benchmark
    public void testStrPartialCompare(Blackhole bh) {
        int r = strPartialBinaryTupleMatcher.match(strPartialTuple, strTupleReference);

        bh.consume(r);
    }

    /**
     * Benchmarks the full comparison of binary tuples represented as byte arrays using the `match` method
     * from the `bytePartialBinaryTupleMatcher` instance. The result of the comparison is passed into a
     * Blackhole to prevent dead code elimination and to ensure accurate benchmarking performance.
     *
     * @param bh The Blackhole instance used to consume the comparison result for benchmarking purposes.
     */
    @Benchmark
    public void testBytesFullCompare(Blackhole bh) {
        int r = bytePartialBinaryTupleMatcher.match(byteTuple, byteTupleReference);

        bh.consume(r);
    }

    /**
     * Benchmarks the partial comparison of binary tuples represented as byte arrays using the `match` method
     * from the `bytePartialBinaryTupleMatcher` instance. The benchmark evaluates the performance of comparing
     * tuple prefixes, types, and associated configurations up to a predefined schema limit. The result of the
     * comparison is consumed by a Blackhole instance to avoid dead code elimination and ensure accurate
     * performance measurement.
     *
     * @param bh The Blackhole instance used to consume the result of the comparison, ensuring it is used
     *           during the benchmark for accurate performance evaluation.
     */
    @Benchmark
    public void testBytesPartialCompare(Blackhole bh) {
        int r = bytePartialBinaryTupleMatcher.match(bytePartialTuple, byteTupleReference);

        bh.consume(r);
    }

    /**
     * Benchmarks the full comparison of binary tuples with UUIDs using the `match` method
     * from the `uuidPartialBinaryTupleMatcher` instance. The comparison evaluates the relative ordering
     * of two binary tuples represented as ByteBuffers, considering tuple prefixes, column types, and schema configurations.
     * This method measures the performance of the complete matching process and consumes
     * the result in a Blackhole to prevent dead code elimination during benchmarking.
     *
     * @param bh The Blackhole instance used to consume the result of the comparison for benchmarking purposes.
     */
    @Benchmark
    public void testUuidFullCompare(Blackhole bh) {
        int r = uuidPartialBinaryTupleMatcher.match(uuidTuple, uuidTupleReference);

        bh.consume(r);
    }

    /**
     * Benchmarks the partial comparison of binary tuples with UUIDs using the `match` method from
     * the `uuidPartialBinaryTupleMatcher` instance. This comparison evaluates the relative ordering
     * of two binary tuples represented as ByteBuffers, considering tuple prefixes, column types,
     * schema configurations, and the prefix matching rules. It measures the performance of comparing
     * only specific portions of the tuples, consuming the result in a Blackhole to avoid dead code
     * elimination during benchmarking.
     *
     * @param bh The Blackhole instance used to consume the result of the comparison, ensuring the
     *           comparison operation is not optimized out during benchmarking for accurate performance
     *           evaluation.
     */
    @Benchmark
    public void testUuidPartialCompare(Blackhole bh) {
        int r = uuidPartialBinaryTupleMatcher.match(uuidPartialTuple, uuidTupleReference);

        bh.consume(r);
    }

    /**
     * Benchmarks the full comparison of timestamp-based binary tuples using the `match` method
     * from the `timestampPartialBinaryTupleMatcher` instance. This method evaluates the
     * relative ordering of two binary tuples represented as `ByteBuffer` objects, considering
     * tuple prefixes, column types, and schema configurations. The comparison result is consumed
     * by a Blackhole instance to prevent dead code elimination during benchmarking and to
     * measure performance accurately.
     *
     * @param bh The Blackhole instance used to consume the result of the comparison, ensuring
     *           the operation is not optimized out during benchmarking for precise performance
     *           evaluation.
     */
    @Benchmark
    public void timestampFullCompare(Blackhole bh) {
        int r = timestampPartialBinaryTupleMatcher.match(timestampTuple, timestampTupleReference);

        bh.consume(r);
    }

    /**
     * Benchmarks the partial comparison of timestamp-based binary tuples using the `match` method
     * from the `timestampPartialBinaryTupleMatcher` instance. This method evaluates the relative
     * ordering of two binary tuples represented as `ByteBuffer` objects by comparing only a portion
     * of the tuples, considering tuple prefixes, column types, and schema configurations. The result
     * is consumed by a Blackhole to prevent dead code elimination during benchmarking and ensure
     * accurate performance measurement.
     *
     * @param bh The Blackhole instance used to consume the result of the comparison, ensuring the
     *           operation is not optimized out during benchmarking for precise performance evaluation.
     */
    @Benchmark
    public void timestampPartialCompare(Blackhole bh) {
        int r = timestampPartialBinaryTupleMatcher.match(timestampPartialTuple, timestampTupleReference);

        bh.consume(r);
    }

    /**
     * Benchmark's entry point.
     */
    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(".*" + TupleComparatorBenchmark.class.getSimpleName() + ".*")
                .build();

        new Runner(opt).run();
    }
}
