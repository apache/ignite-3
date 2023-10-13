package org.apache.ignite.example;

import static java.util.concurrent.TimeUnit.SECONDS;

import io.opentelemetry.context.Context;
import io.opentelemetry.instrumentation.annotations.WithSpan;
import java.util.concurrent.CompletableFuture;

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
    public static void main(String[] args) {
        var f = new CompletableFuture<Integer>();

        new Thread(() -> f.thenCompose(ThreadExample::process)).start();
        new Thread(Context.current().wrap(() -> complete(f))).start();
    }
}
