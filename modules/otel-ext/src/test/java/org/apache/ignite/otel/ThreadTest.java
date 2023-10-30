package org.apache.ignite.otel;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.ignite.internal.tracing.OtelSpanManager.asyncSpan;
import static org.apache.ignite.internal.tracing.OtelSpanManager.rootSpan;
import static org.apache.ignite.internal.tracing.OtelSpanManager.span;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.opentelemetry.context.Context;
import io.opentelemetry.sdk.autoconfigure.AutoConfiguredOpenTelemetrySdk;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.event.AbstractEventProducer;
import org.apache.ignite.internal.tracing.TraceSpan;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** For {@link AbstractEventProducer} testing. */
public class ThreadTest {
    @BeforeEach
    void setUp() {
        AutoConfiguredOpenTelemetrySdk.initialize();
    }

    @Test
    public void simpleScopeTest() {
        TraceSpan parent;

        assertEquals(Context.root(), Context.current());

        try (var ignored = rootSpan("root.request")) {
            try (var parent0 = asyncSpan("process")) {
                parent = parent0;
            }
        }

        assertEquals(Context.root(), Context.current());

        asyncSpan("process", parent, (span) -> {
            assertTrue(span.isValid());

            return null;
        });

        assertEquals(Context.root(), Context.current());
    }

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
}
