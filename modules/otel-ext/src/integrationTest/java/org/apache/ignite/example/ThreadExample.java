package org.apache.ignite.example;

import static java.util.concurrent.TimeUnit.SECONDS;

import io.opentelemetry.context.Context;
import io.opentelemetry.instrumentation.annotations.WithSpan;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.testframework.IgniteTestUtils;

/**
 * Tests for Thread.
 */
public class ThreadExample {
    @WithSpan
    private CompletableFuture<?> run() {
        var callable = Context.current()
                .wrap(this::process);

        return IgniteTestUtils.runAsync(callable, "example-thread");
    }

    @WithSpan
    private boolean process() {
        try {
            SECONDS.sleep(1L);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        return true;
    }

    /**
     * Main method of the example.
     *
     * @param args The command line arguments.
     */
    public static void main(String[] args) {
        var example = new ThreadExample();
        
        example.run().join();
    }
}
