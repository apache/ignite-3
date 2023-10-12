package org.apache.ignite.example;

import static java.util.Arrays.asList;
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
public class FutureExample {
    private static final IgniteLogger LOG = Loggers.forClass(FutureExample.class);

    private final StripedThreadPoolExecutor stripedThreadPoolExecutor;

    public FutureExample() {
        stripedThreadPoolExecutor = new StripedThreadPoolExecutor(
                2,
                "example-execution-pool",
                new LogUncaughtExceptionHandler(LOG),
                false,
                0
        );
    }

    @WithSpan
    private void start() {
        var futs = asList(submitToPool(), submitToPool());

        futs.forEach(CompletableFuture::join);
    }

    private CompletableFuture<?> submitToPool() {
        return stripedThreadPoolExecutor.submit(() -> process(1L), 0)
                .thenAccept(unused -> process(2L));
    }

    @WithSpan
    private void process(long delay) {
        try {
            SECONDS.sleep(delay);
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
        new FutureExample().start();
    }
}
