package org.apache.ignite.example;

import static java.util.concurrent.TimeUnit.SECONDS;

import io.opentelemetry.instrumentation.annotations.WithSpan;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.thread.LogUncaughtExceptionHandler;
import org.apache.ignite.internal.thread.StripedThreadPoolExecutor;

/**
 * Tests for ExecutorService.
 * Add system property "-Dotel.instrumentation.executors.enabled=false" to disable instrumentation.
 */
public class ExecutorServiceExample {
    private static final IgniteLogger LOG = Loggers.forClass(ExecutorServiceExample.class);

    private final StripedThreadPoolExecutor stripedThreadPoolExecutor;

    public ExecutorServiceExample() {
        stripedThreadPoolExecutor = new StripedThreadPoolExecutor(
                2,
                "example-execution-pool",
                new LogUncaughtExceptionHandler(LOG),
                false,
                0
        );
    }

    @WithSpan
    private CompletableFuture<?> run() {
        return stripedThreadPoolExecutor.submit(this::process, 0);
    }

    @WithSpan
    private void process() {
        try {
            SECONDS.sleep(1L);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Main method of the example.
     *
     * @param args The command line arguments.
     */
    public static void main(String[] args) {
        var example = new ExecutorServiceExample();
        
        example.run().join();
        example.run().join();
    }
}
