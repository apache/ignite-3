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

package org.apache.ignite.internal.hlc.benchmarks;

import static org.apache.ignite.internal.hlc.HybridTimestamp.LOGICAL_TIME_BITS_SIZE;

import java.util.concurrent.TimeUnit;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.hlc.HybridTimestamp;
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
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

/**
 * Benchmark for HybridClock.
 */
@State(Scope.Benchmark)
@Fork(1)
@Warmup(iterations = 5, time = 2)
@Measurement(iterations = 10, time = 2)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
public class HybridClockBenchmark {
    /** Clock to benchmark. */
    private HybridClock clock;

    /**
     * Initializes the clock.
     */
    @Setup
    public void setUp() {
        clock = new HybridClockImpl();
    }

    @Benchmark
    @Threads(1)
    public void hybridClockNowSingleThread() {
        hybridClockNow();
    }

    @Benchmark
    @Threads(5)
    public void hybridClockNowFiveThreads() {
        hybridClockNow();
    }

    @Benchmark
    @Threads(10)
    public void hybridClockNowTenThreads() {
        hybridClockNow();
    }

    @Benchmark
    @Threads(1)
    public void hybridClockUpdateSingleThread() {
        hybridClockUpdate();
    }

    @Benchmark
    @Threads(5)
    public void hybridClockUpdateFiveThreads() {
        hybridClockUpdate();
    }

    @Benchmark
    @Threads(10)
    public void hybridClockUpdateTenThreads() {
        hybridClockUpdate();
    }

    private void hybridClockNow() {
        for (int i = 0; i < 1000; i++) {
            clock.now();
        }
    }

    private void hybridClockUpdate() {
        for (int i = 0; i < 1000; i++) {
            long ts = ((System.currentTimeMillis() + (i % 2 == 0 ? 5 : -5)) << LOGICAL_TIME_BITS_SIZE) + (i % 10);

            clock.update(HybridTimestamp.hybridTimestamp(ts));
        }
    }

    /**
     * Benchmark's entry point.
     */
    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(".*" + HybridClockBenchmark.class.getSimpleName() + ".*")
                .build();

        new Runner(opt).run();
    }
}
