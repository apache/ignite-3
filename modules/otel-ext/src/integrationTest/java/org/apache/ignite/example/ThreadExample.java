package org.apache.ignite.example;

import static java.util.concurrent.TimeUnit.SECONDS;

import io.opentelemetry.context.Context;
import io.opentelemetry.instrumentation.annotations.WithSpan;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.testframework.IgniteTestUtils;

/**
 * Tests for Thread.
 */
public class ThreadExample {
    @WithSpan
    private static CompletableFuture<Void> process(Integer delay) {
        try {
            SECONDS.sleep(1L);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        return CompletableFuture.completedFuture(null);
    }

    @WithSpan
    private static void complete(CompletableFuture<Integer> f) {
        f.complete(10);
    }

    /**
     * Main method of the example.
     *
     * @param args The command line arguments.
     */
    @WithSpan
    public static void main(String[] args) throws Exception {
        var f = new CompletableFuture<Integer>();

        Context ctx = Context.current();

        IgniteTestUtils.runAsync(() -> f.thenCompose(ctx.wrapFunction(ThreadExample::process)), "handler-thread").join();

        SECONDS.sleep(2L);

        IgniteTestUtils.runAsync(() -> complete(f), "complete-future");
    }
}
