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

package org.apache.ignite.internal.util;

import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

/**
 * Benchmark to compare performance of {@link VersatileReadWriteLock} with its derivatives.
 */
@State(Scope.Benchmark)
@Fork(1)
@Threads(8)
@Warmup(iterations = 10, time = 2)
@Measurement(iterations = 20, time = 2)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
public class VersatileReadWriteLockBenchmark {
    private final VersatileReadWriteLock simpleLock = new VersatileReadWriteLock(ForkJoinPool.commonPool());
    private final StripedVersatileReadWriteLock stripedLock = new StripedVersatileReadWriteLock(ForkJoinPool.commonPool());

    @Benchmark
    public void simpleSync() {
        simpleLock.readLock();
        simpleLock.readUnlock();
    }

    @Benchmark
    public void stripedSync() {
        stripedLock.readLock();
        stripedLock.readUnlock();
    }

    @Benchmark
    public Object simpleAsync() throws Exception {
        return simpleLock.inReadLockAsync(CompletableFutures::nullCompletedFuture).get();
    }

    @Benchmark
    public Object stripedAsync() throws Exception {
        return stripedLock.inReadLockAsync(CompletableFutures::nullCompletedFuture).get();
    }

    /**
     * Benchmark's entry point.
     */
    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(".*" + VersatileReadWriteLockBenchmark.class.getSimpleName() + ".*")
                // .jvmArgsAppend("-Djmh.executor=VIRTUAL")
                // .addProfiler(JavaFlightRecorderProfiler.class, "configName=profile.jfc")
                .build();

        new Runner(opt).run();
    }
}
