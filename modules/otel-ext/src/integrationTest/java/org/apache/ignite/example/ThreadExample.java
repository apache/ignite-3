package org.apache.ignite.example;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.ignite.internal.tracing.OtelSpanManager.asyncSpan;
import static org.apache.ignite.internal.tracing.OtelSpanManager.rootSpan;
import static org.apache.ignite.internal.tracing.OtelSpanManager.span;
import static org.apache.ignite.internal.tracing.OtelSpanManager.wrap;

import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.tracing.TraceSpan;

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
    public static void main(String[] args) {
        TraceSpan parent;

        try (var ignored = rootSpan("ClientTransactionBeginRequest.process")) {
            try (var parent0 = asyncSpan("main1")) {
                parent = parent0;
            }
        }

        asyncSpan("ClientTupleGetRequest.process", parent, (span) -> completedFuture(10)).join();

        try (var rootSpan = asyncSpan("main", parent)) {
            System.out.println(rootSpan);

            rootSpan.end();
            parent.end();
        }

        try (var rootSpan = rootSpan("main")) {
            try (var ignored = span("main1")) {
                System.out.println(ignored);
            }

            var f = new CompletableFuture<Integer>();

            new Thread(() -> f.thenCompose(ThreadExample::process)).start();
            new Thread(wrap(() -> complete(f))).start();
        }
    }
}
