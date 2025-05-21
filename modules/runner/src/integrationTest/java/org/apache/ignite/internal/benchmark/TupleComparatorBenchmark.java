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
import java.util.List;
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
 * Benchmark for partial tuple comparator.
 */
@State(Scope.Benchmark)
@Fork(1)
@Threads(1)
@Warmup(iterations = 5, time = 2)
@Measurement(iterations = 10, time = 2)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class TupleComparatorBenchmark {
    PartialBinaryTupleMatcher partialBinaryTupleMatcher;
    PartialBinaryTupleMatcher bytePartialBinaryTupleMatcher;
    ByteBuffer tupleReference;
    ByteBuffer truncatedTuple;
    ByteBuffer truncatedTupleDirect;

    ByteBuffer tupleReferenceAscii;
    ByteBuffer truncatedTupleAscii;
    ByteBuffer truncatedTupleDirectAscii;

    ByteBuffer byteTupleReference;
    ByteBuffer byteTruncatedTuple;
    ByteBuffer byteTruncatedTupleDirect;

    @Param({"true", "false"})
    boolean useBuffer;

    /**
     * Prepare to start the benchmark.
     */
    @Setup
    public void setUp() {
        System.setProperty("IGNITE_USE_BUFFER", Boolean.toString(useBuffer));

        partialBinaryTupleMatcher = new PartialBinaryTupleMatcher(
                List.of(CatalogColumnCollation.ASC_NULLS_LAST),
                List.of(NativeTypes.STRING)
        );

        bytePartialBinaryTupleMatcher = new PartialBinaryTupleMatcher(
                List.of(CatalogColumnCollation.ASC_NULLS_LAST),
                List.of(NativeTypes.BYTES)
        );

        tupleReference = new BinaryTupleBuilder(1)
                .appendString("Привет 你好".repeat(20))
                .build()
                .order(ByteOrder.LITTLE_ENDIAN);

        truncatedTuple = new BinaryTupleBuilder(1)
                .appendString("Привет 你好".repeat(20))
                .build()
                .limit(180)
                .slice()
                .order(ByteOrder.LITTLE_ENDIAN);

        truncatedTupleDirect = ByteBuffer.allocateDirect(truncatedTuple.capacity())
                .order(ByteOrder.LITTLE_ENDIAN)
                .put(truncatedTuple);

        tupleReference.rewind();
        truncatedTuple.rewind();
        truncatedTupleDirect.rewind();

        tupleReferenceAscii = new BinaryTupleBuilder(1)
                 .appendString("qwertyuiop".repeat(20))
                .build()
                .order(ByteOrder.LITTLE_ENDIAN);

        truncatedTupleAscii = new BinaryTupleBuilder(1)
                 .appendString("qwertyuiop".repeat(20))
                .build()
                .limit(180)
                .slice()
                .order(ByteOrder.LITTLE_ENDIAN);

        truncatedTupleDirectAscii = ByteBuffer.allocateDirect(truncatedTuple.capacity())
                .order(ByteOrder.LITTLE_ENDIAN)
                .put(truncatedTuple);

        tupleReferenceAscii.rewind();
        truncatedTupleAscii.rewind();
        truncatedTupleDirectAscii.rewind();

        // bytes
        byte[] bytes = new byte[350];

        for (int i = 0; i < bytes.length; i++) {
            bytes[i] = (byte) (i % 10);
        }

        byteTupleReference = new BinaryTupleBuilder(1)
                .appendBytes(bytes)
                // .appendString("qwertyuiop".repeat(20))
                .build()
                .order(ByteOrder.LITTLE_ENDIAN);

        byteTruncatedTuple = new BinaryTupleBuilder(1)
                .appendBytes(bytes)
                // .appendString("qwertyuiop".repeat(20))
                .build()
                .limit(180)
                .slice()
                .order(ByteOrder.LITTLE_ENDIAN);

        byteTruncatedTupleDirect = ByteBuffer.allocateDirect(truncatedTuple.capacity())
                .order(ByteOrder.LITTLE_ENDIAN)
                .put(truncatedTuple);

        byteTupleReference.rewind();
        byteTruncatedTuple.rewind();
        byteTruncatedTupleDirect.rewind();
    }

    /**
     * Benchmarks the performance of a full comparison of two identical binary tuples using a partial binary tuple matcher.
     * This method invokes the comparison operation of {@code bytePartialBinaryTupleMatcher} and consumes the result using
     * a {@code Blackhole} to ensure the complete process is benchmarked and no optimizations are applied by the compiler.
     *
     * @param bh A Blackhole instance used to consume the output of the comparison and prevent optimizations during benchmarking.
     */
    @Benchmark
    public void testBytesFullCompare(Blackhole bh) {
        int r = bytePartialBinaryTupleMatcher.match(byteTupleReference, byteTupleReference);

        bh.consume(r);
    }

    /**
     * Benchmarks the performance of a partial comparison between two binary tuples.
     * This method invokes the comparison operation of {@code partialBinaryTupleMatcher} on a truncated binary tuple
     * and a tuple reference. The result of the comparison is then consumed using a {@code Blackhole} to ensure
     * the benchmarking process is not optimized away by the compiler.
     *
     * @param bh A Blackhole instance used to consume the output of the comparison and prevent optimizations during benchmarking.
     */
    @Benchmark
    public void testBytesPartialCompare(Blackhole bh) {
        int r = partialBinaryTupleMatcher.match(byteTruncatedTuple, byteTupleReference);

        bh.consume(r);
    }

    /**
     * Benchmarks the performance of directly performing a partial comparison between a truncated binary tuple
     * in direct byte buffer format and a tuple reference. This method uses the {@code partialBinaryTupleMatcher}
     * to execute the comparison and consumes the result using a {@code Blackhole} to ensure the entire process
     * is benchmarked and no optimizations are applied by the compiler.
     *
     * @param bh A Blackhole instance used to consume the comparison result and eliminate potential compiler optimizations
     *           during benchmarking.
     */
    @Benchmark
    public void testBytesPartialCompareDirect(Blackhole bh) {
        int r = partialBinaryTupleMatcher.match(byteTruncatedTupleDirect, byteTupleReference);

        bh.consume(r);
    }

    /**
     * Benchmarks the performance of comparing two identical binary tuples using a partial binary tuple comparator.
     * This method measures the time taken to execute the comparison operation
     * and consumes the result to ensure the entire process is benchmarked.
     *
     * @param bh A Blackhole instance used to consume the output of the comparison and prevent optimizations.
     */
    @Benchmark
    public void testFullCompare(Blackhole bh) {
        int r = partialBinaryTupleMatcher.match(tupleReference, tupleReference);

        bh.consume(r);
    }

    /**
     * Benchmarks the performance of a full comparison between two identical binary tuples encoded as ASCII strings.
     * This method utilizes the {@code partialBinaryTupleMatcher} to perform the comparison operation and then consumes
     * the result using a {@code Blackhole} to ensure that the benchmarking process is not optimized away by the compiler.
     *
     * @param bh A Blackhole instance used to consume the result of the comparison operation, ensuring accurate benchmarking
     *           and preventing compiler optimizations.
     */
    @Benchmark
    public void testFullCompareAscii(Blackhole bh) {
        int r = partialBinaryTupleMatcher.match(tupleReferenceAscii, tupleReferenceAscii);

        bh.consume(r);
    }

    /**
     * Benchmarks the performance of comparing a truncated binary tuple with a full binary tuple.
     * This method measures the execution time of the comparison and consumes the result
     * to ensure proper benchmarking and prevent potential optimizations by the compiler.
     *
     * @param bh A Blackhole instance used to consume the result of the comparison operation.
     */
    @Benchmark
    public void testPartialCompare(Blackhole bh) {
        int r = partialBinaryTupleMatcher.match(truncatedTuple, tupleReference);

        bh.consume(r);
    }

    /**
     * Benchmarks the performance of partially comparing binary tuples encoded as ASCII strings.
     * This method performs a partial comparison between two ASCII-encoded binary tuples using
     * a partial binary tuple matcher. The result of the comparison is consumed via a {@code Blackhole}
     * to ensure the full benchmarking process and prevent compiler optimizations.
     *
     * @param bh A Blackhole instance used to consume the comparison result, ensuring that the outcome
     *           is not optimized away by the compiler during benchmarking.
     */
    @Benchmark
    public void testPartialCompareAscii(Blackhole bh) {
        int r = partialBinaryTupleMatcher.match(truncatedTupleAscii, tupleReferenceAscii);

        bh.consume(r);
    }

    /**
     * Benchmarks the performance of directly comparing a truncated binary tuple with a tuple reference
     * using a partial binary tuple matcher. This method excludes middle-layer abstractions, aiming to
     * assess the raw comparison performance, and consumes the result to ensure proper benchmarking and
     * prevent compiler optimizations.
     *
     * @param bh A Blackhole instance used to consume the comparison result and eliminate any potential optimizations
     *           during benchmarking.
     */
    @Benchmark
    public void testPartialCompareDirect(Blackhole bh) {
        int r = partialBinaryTupleMatcher.match(truncatedTupleDirect, tupleReference);

        bh.consume(r);
    }

    /**
     * Benchmarks the performance of directly comparing a truncated binary tuple with a tuple reference
     * using ASCII encoding and a partial binary tuple matcher. This method avoids intermediate layers
     * to measure the raw performance of the comparison and consumes the result to ensure proper benchmarking
     * and prevent compiler optimizations.
     *
     * @param bh A Blackhole instance used to consume the comparison result, ensuring that the operation's outcome
     *           is not optimized away by the compiler during benchmarking.
     */
    @Benchmark
    public void testPartialCompareDirectAscii(Blackhole bh) {
        int r = partialBinaryTupleMatcher.match(truncatedTupleDirectAscii, tupleReferenceAscii);

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
