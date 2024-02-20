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

package org.apache.ignite.internal.tracing.otel;

import static org.apache.ignite.internal.tracing.TracingManager.rootSpan;
import static org.apache.ignite.internal.tracing.TracingManager.span;
import static org.apache.ignite.internal.tracing.TracingManager.taskWrapping;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.tracing.TraceSpan;
import org.apache.ignite.internal.util.IgniteUtils;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

/**
 * Benchmark that runs get operation with tracing.
 */
@State(Scope.Benchmark)
@Warmup(iterations = 10, time = 10)
@Measurement(iterations = 10, time = 10)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Fork(1)
public class TracingBenchmark {
    /** Logger. */
    private static final IgniteLogger LOG = Loggers.forClass(TracingBenchmark.class);

    private ExecutorService executor;

    @Setup
    public void setup() {
        executor = taskWrapping(Executors.newSingleThreadExecutor());
    }

    /**
     * Tear down.
     */
    @TearDown
    public void tearDown() {
        IgniteUtils.shutdownAndAwaitTermination(executor, 1L, TimeUnit.SECONDS);
    }

    /**
     * ss.
     *
     * @throws Exception If failed.
     */
    @Benchmark
    public void tracingExecutorDisabled() throws Exception {
        executor.submit(() -> {
            try {
                Thread.sleep(100L);
            } catch (InterruptedException e) {
                LOG.error("Thread was interrupted", e);
            }
        }).get();
    }

    /**
     * ss.
     *
     * @throws Exception If failed.
     */
    @Benchmark
    public void tracingExecutorEnabled() throws Exception {
        rootSpan("root", (ignored) -> {
            return executor.submit(() -> {
                try (TraceSpan ignored1 = span("thread")) {
                    Thread.sleep(100L);
                } catch (InterruptedException e) {
                    LOG.error("Thread was interrupted", e);
                }
            });
        }).get();
    }

    /**
     * Runs the benchmark.
     *
     * @param args args
     * @throws Exception if something goes wrong
     */
    public static void main(String[] args) throws Exception {
        Options opt = new OptionsBuilder() //
                .include(TracingBenchmark.class.getSimpleName() + ".*")
                .build();

        new Runner(opt).run();
    }
}
