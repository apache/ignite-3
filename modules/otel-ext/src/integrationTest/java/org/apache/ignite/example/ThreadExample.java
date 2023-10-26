package org.apache.ignite.example;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.ignite.internal.tracing.OtelSpanManager.rootSpan;
import static org.apache.ignite.internal.tracing.OtelSpanManager.span;

import java.util.concurrent.CompletableFuture;

/**
 * Tests for propagating context between threads.
 */
public class ThreadExample {
    private static CompletableFuture<Integer> process(Integer delay) {
        try (var ignored = span("process")) {
            try {
                SECONDS.sleep(delay);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();

                throw new RuntimeException(e);
            }

            return completedFuture(10);
        }
    }

    private static void complete(CompletableFuture<Integer> f) {
        try (var ignored = span("complete")) {
            f.complete(1);
        }
    }

    /**
     * Main method of the example.
     *
     * @param args The command line arguments.
     */
    public static void main(String[] args) throws Exception {
        try (var ignored = span("main1")) {
            System.out.println(ignored);
        }

        try (var rootSpan = rootSpan("main")) {
            try (var ignored = span("main1")) {
                System.out.println(ignored);
            }

            var f = new CompletableFuture<Integer>();

            new Thread(() -> f.thenCompose(ThreadExample::process)).start();
            new Thread(rootSpan.wrap(() -> complete(f))).start();
        }
    }
}
