package org.apache.ignite.internal.benchmark;

import java.util.concurrent.TimeUnit;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridClockImpl;
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

@State(Scope.Benchmark)
@Fork(1)
@Warmup(iterations = 5, time = 2)
@Measurement(iterations = 10, time = 2)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
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

    private void hybridClockNow() {
        for (int i = 0; i < 1000; i++) {
            clock.now();
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
