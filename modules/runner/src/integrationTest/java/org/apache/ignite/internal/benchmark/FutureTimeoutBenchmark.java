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

import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.apache.ignite.internal.util.IgniteUtils.awaitForWorkersStop;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.internal.future.timeout.TimeoutObject;
import org.apache.ignite.internal.future.timeout.TimeoutWorker;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.thread.IgniteThread;
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
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

/**
 * Future timeout benchmark - measures the latency of the assignment of the future timeout in two ways:
 * 1. Based on the embedded CompletableFuture#orTimeout.
 * 2. Based on the additional thread that is scanning collection and completing all the futures already have been explored.
 *
 * <p>Results on 11th Gen Intel® Core™ i7-1165G7 @ 2.80GHz, openjdk 11.0.24, Windows 10 Pro:
 * Benchmark                    (useFutureEmbeddedTimeout)  Mode  Cnt   Score    Error  Units
 * FutureTimeoutBenchmark.test                       false  avgt   20   0,760 ±  0,002  us/op
 * FutureTimeoutBenchmark.test                        true  avgt   20  36,123 ± 44,414  us/op
 */
@State(Scope.Benchmark)
@Fork(1)
@Threads(1)
@Warmup(iterations = 10, time = 2)
@Measurement(iterations = 20, time = 2)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class FutureTimeoutBenchmark {
    private static AtomicLong ID_GEN = new AtomicLong();

    /** Active operations. */
    public ConcurrentMap<Long, TimeoutObjectImpl> requestsMap;

    private TimeoutWorker timeoutWorker;

    private ConcurrentMap<Long, CompletableFuture<?>> futs;

    @Param({"false", "true"})
    private boolean useFutureEmbeddedTimeout;

    /**
     * Prepare to start the benchmark.
     */
    @Setup
    public void setUp() {
        if (useFutureEmbeddedTimeout) {
            futs = new ConcurrentHashMap<>();
        } else {
            requestsMap = new ConcurrentHashMap<>();
            this.timeoutWorker = new TimeoutWorker(
                    Loggers.forClass(FutureTimeoutBenchmark.class),
                    "test-node",
                    "FutureTimeoutBenchmark-timeout-worker",
                    requestsMap,
                    null
            );

            new IgniteThread(timeoutWorker).start();
        }
    }

    /**
     * Closes resources.
     */
    @TearDown
    public void tearDown() throws InterruptedException {
        if (useFutureEmbeddedTimeout) {
            for (CompletableFuture<?> fut : futs.values()) {
                if (!fut.isDone()) {
                    try {
                        fut.get(10, TimeUnit.SECONDS);
                    } catch (ExecutionException e) {
                        assert e.getCause() instanceof TimeoutException : "Unexpected exception type: " + e.getCause().getClass();
                    } catch (TimeoutException e) {
                        // Ignore exception.
                        break;
                    }
                }
            }

            futs.clear();
            futs = null;
        } else {
            assertTrue(waitForCondition(requestsMap::isEmpty, 10_000));

            awaitForWorkersStop(List.of(timeoutWorker), true, null);
        }
    }

    /**
     * Target method to benchmark.
     */
    @Benchmark
    public void test() {
        if (useFutureEmbeddedTimeout) {
            for (int i = 0; i < 10; i++) {
                var fut = new CompletableFuture<Void>();

                futs.put(ID_GEN.incrementAndGet(), fut);

                fut.orTimeout(10, TimeUnit.MILLISECONDS);
            }

            if (futs.size() > 100_000) {
                futs.clear();
            }
        } else {
            for (int i = 0; i < 10; i++) {
                requestsMap.put(ID_GEN.incrementAndGet(), new TimeoutObjectImpl(
                        System.currentTimeMillis() + 10,
                        new CompletableFuture()
                ));
            }

            if (requestsMap.size() > 100_000) {
                requestsMap.clear();
            }
        }
    }

    /**
     * Benchmark's entry point.
     */
    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(".*" + FutureTimeoutBenchmark.class.getSimpleName() + ".*")
                .build();

        new Runner(opt).run();
    }

    /**
     * Timeout object wrapper for the completable future.
     */
    private static class TimeoutObjectImpl implements TimeoutObject<Void> {
        /** End time (milliseconds since Unix epoch). */
        private final long endTime;

        /** Target future. */
        private final CompletableFuture<Void> fut;

        /**
         * Constructor.
         *
         * @param endTime End timestamp in milliseconds.
         * @param fut Target future.
         */
        public TimeoutObjectImpl(long endTime, CompletableFuture<Void> fut) {
            this.endTime = endTime;
            this.fut = fut;
        }

        @Override
        public long endTime() {
            return endTime;
        }

        @Override
        public CompletableFuture<Void> future() {
            return fut;
        }
    }
}
