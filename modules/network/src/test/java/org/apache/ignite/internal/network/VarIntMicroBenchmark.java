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

package org.apache.ignite.internal.network;

import java.nio.ByteBuffer;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.internal.network.direct.stream.DirectByteBufferStream;
import org.apache.ignite.internal.network.direct.stream.DirectByteBufferStreamImplV1;
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
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

/**
 * A micro-benchmark of {@link DirectByteBufferStreamImplV1}'s var-length numbers encoding performance.
 */
@Warmup(iterations = 10, time = 500, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 10, time = 500, timeUnit = TimeUnit.MILLISECONDS)
@Fork(1)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Thread)
public class VarIntMicroBenchmark {
    /** Number of values in a benchmark round. */
    private static final int N = 16;

    private static final int MAX_VAR_LONG_BYTES = 10;

    @Param({"heap", "direct"})
    private String bufferType;

    /** A sequence of long values, where each consecutive sub-array of {@value N} elements requires one more byte to encode. */
    private final long[] values = new long[MAX_VAR_LONG_BYTES * N];

    private final DirectByteBufferStreamImplV1 stream = new DirectByteBufferStreamImplV1(new MessageSerializationRegistryImpl());

    private ByteBuffer bb;

    /** Initializes the benchmark state. */
    @Setup
    public void setup() {
        if ("heap".equals(bufferType)) {
            bb = ByteBuffer.allocate(10 * values.length);
        } else {
            bb = ByteBuffer.allocateDirect(10 * values.length);
        }

        stream.setBuffer(bb);

        Random rand = new Random(1337);
        long[] masks = {1L, 1L << 7, 1L << 14, 1L << 21, 1L << 28, 1L << 35, 1L << 42, 1L << 49, 1L << 56, 1L << 63};

        int ctr = 0;
        for (long mask : masks) {
            for (int i = 0; i < N; i++) {
                long val = (rand.nextLong() & (mask - 1)) + mask * rand.nextInt(1 << 7);

                values[ctr++] = (val | mask) - 1; // Extract 1 to account for internal increment in var-int encoding.
            }
        }
    }

    /** Benchmark for short values that will be encoded as 1 byte. */
    @Benchmark
    public DirectByteBufferStream varShort1() {
        bb.position(0);

        for (int i = 0; i < N; i++) {
            stream.writeShort((short) values[i]);
        }

        return stream;
    }

    /** Benchmark for short values that will be encoded as 2 bytes. */
    @Benchmark
    public DirectByteBufferStream varShort2() {
        bb.position(0);

        for (int i = N; i < 2 * N; i++) {
            stream.writeShort((short) values[i]);
        }

        return stream;
    }

    /** Benchmark for short values that will be encoded as 3 bytes. */
    @Benchmark
    public DirectByteBufferStream varShort3() {
        bb.position(0);

        for (int i = 2 * N; i < 3 * N; i++) {
            stream.writeShort((short) values[i]);
        }

        return stream;
    }

    /** Benchmark for short values. */
    @Benchmark
    public DirectByteBufferStream varShortAll() {
        bb.position(0);

        for (int i = 0; i < 3 * N; i++) {
            stream.writeShort((short) values[i]);
        }

        return stream;
    }

    /** Benchmark for int values that will be encoded as 1 byte. */
    @Benchmark
    public DirectByteBufferStream varInt1() {
        bb.position(0);

        for (int i = 0; i < N; i++) {
            stream.writeInt((int) values[i]);
        }

        return stream;
    }

    /** Benchmark for int values that will be encoded as 2 bytes. */
    @Benchmark
    public DirectByteBufferStream varInt2() {
        bb.position(0);

        for (int i = N; i < 2 * N; i++) {
            stream.writeInt((int) values[i]);
        }

        return stream;
    }

    /** Benchmark for int values that will be encoded as 3 bytes. */
    @Benchmark
    public DirectByteBufferStream varInt3() {
        bb.position(0);

        for (int i = 2 * N; i < 3 * N; i++) {
            stream.writeInt((int) values[i]);
        }

        return stream;
    }

    /** Benchmark for int values that will be encoded as 4 bytes. */
    @Benchmark
    public DirectByteBufferStream varInt4() {
        bb.position(0);

        for (int i = 3 * N; i < 4 * N; i++) {
            stream.writeInt((int) values[i]);
        }

        return stream;
    }

    /** Benchmark for int values that will be encoded as 5 bytes. */
    @Benchmark
    public DirectByteBufferStream varInt5() {
        bb.position(0);

        for (int i = 4 * N; i < 5 * N; i++) {
            stream.writeInt((int) values[i]);
        }

        return stream;
    }

    /** Benchmark for int values that. */
    @Benchmark
    public DirectByteBufferStream varIntAll() {
        bb.position(0);

        for (int i = 0; i < 5 * N; i++) {
            stream.writeInt((int) values[i]);
        }

        return stream;
    }

    /** Benchmark for long values that will be encoded as 1 byte. */
    @Benchmark
    public DirectByteBufferStream varLong1() {
        bb.position(0);

        for (int i = 0; i < N; i++) {
            stream.writeLong(values[i]);
        }

        return stream;
    }

    /** Benchmark for long values that will be encoded as 2 bytes. */
    @Benchmark
    public DirectByteBufferStream varLong2() {
        bb.position(0);

        for (int i = N; i < 2 * N; i++) {
            stream.writeLong(values[i]);
        }

        return stream;
    }

    /** Benchmark for long values that will be encoded as 3 bytes. */
    @Benchmark
    public DirectByteBufferStream varLong3() {
        bb.position(0);

        for (int i = 2 * N; i < 3 * N; i++) {
            stream.writeLong(values[i]);
        }

        return stream;
    }

    /** Benchmark for long values that will be encoded as 4 bytes. */
    @Benchmark
    public DirectByteBufferStream varLong4() {
        bb.position(0);

        for (int i = 3 * N; i < 4 * N; i++) {
            stream.writeLong(values[i]);
        }

        return stream;
    }

    /** Benchmark for long values that will be encoded as 5 bytes. */
    @Benchmark
    public DirectByteBufferStream varLong5() {
        bb.position(0);

        for (int i = 4 * N; i < 5 * N; i++) {
            stream.writeLong(values[i]);
        }

        return stream;
    }

    /** Benchmark for long values that will be encoded as 6 bytes. */
    @Benchmark
    public DirectByteBufferStream varLong6() {
        bb.position(0);

        for (int i = 5 * N; i < 6 * N; i++) {
            stream.writeLong(values[i]);
        }

        return stream;
    }

    /** Benchmark for long values that will be encoded as 7 bytes. */
    @Benchmark
    public DirectByteBufferStream varLong7() {
        bb.position(0);

        for (int i = 6 * N; i < 7 * N; i++) {
            stream.writeLong(values[i]);
        }

        return stream;
    }

    /** Benchmark for long values that will be encoded as 8 bytes. */
    @Benchmark
    public DirectByteBufferStream varLong8() {
        bb.position(0);

        for (int i = 7 * N; i < 8 * N; i++) {
            stream.writeLong(values[i]);
        }

        return stream;
    }

    /** Benchmark for long values that will be encoded as 9 bytes. */
    @Benchmark
    public DirectByteBufferStream varLong9() {
        bb.position(0);

        for (int i = 8 * N; i < 9 * N; i++) {
            stream.writeLong(values[i]);
        }

        return stream;
    }

    /** Benchmark for long values that will be encoded as 10 bytes. */
    @Benchmark
    public DirectByteBufferStream varLong10() {
        bb.position(0);

        for (int i = 9 * N; i < 10 * N; i++) {
            stream.writeLong(values[i]);
        }

        return stream;
    }

    /** Benchmark for long values. */
    @Benchmark
    public DirectByteBufferStream varLongAll() {
        bb.position(0);

        for (int i = 0; i < 10 * N; i++) {
            stream.writeLong(values[i]);
        }

        return stream;
    }

    /**
     * Runs the benchmark.
     *
     * @param args args
     * @throws Exception if something goes wrong
     */
    public static void main(String[] args) throws Exception {
        Options build = new OptionsBuilder()
                // .addProfiler("gc")
                .include(VarIntMicroBenchmark.class.getName() + ".*").build();

        new Runner(build).run();
    }
}
