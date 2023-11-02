package org.apache.ignite.otel;

import static org.apache.ignite.internal.tracing.TracingManager.asyncSpan;
import static org.apache.ignite.internal.tracing.TracingManager.rootSpan;
import static org.apache.ignite.internal.tracing.TracingManager.spanWithResult;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.context.Context;
import io.opentelemetry.sdk.trace.ReadableSpan;
import io.opentelemetry.sdk.trace.data.SpanData;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import org.apache.ignite.internal.event.AbstractEventProducer;
import org.apache.ignite.internal.tracing.TraceSpan;
import org.junit.jupiter.api.Test;

/** For {@link AbstractEventProducer} testing. */
public class ScopeTest {
    @Test
    public void shouldEndedOnFutureError() {
        var actual = startInSpanClosure((span) -> {
            var fut = new CompletableFuture<>();
            fut.completeExceptionally(new RuntimeException());

            return fut;
        });

        assertEquals(StatusCode.ERROR, actual.getStatus().getStatusCode());
        assertTrue(actual.hasEnded());
    }

    @Test
    public void shouldEndedOnException() {
        var actual = startInSpanClosure((span) -> {
            throw new RuntimeException();
        });

        assertEquals(StatusCode.ERROR, actual.getStatus().getStatusCode());
        assertTrue(actual.hasEnded());
    }

    private static <R> SpanData startInSpanClosure(Function<TraceSpan, R> closure) {
        AtomicReference<ReadableSpan> otelSpan = new AtomicReference<>();

        try (var ignored = rootSpan("root.request")) {
            spanWithResult("process", (span) -> {
                otelSpan.set((ReadableSpan) Span.current());

                return closure.apply(span);
            });
        }
        catch (Exception ignored) {
            // No-op.
        }

        return otelSpan.get().toSpanData();
    }

    @Test
    public void parentScopeTest() {
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
}
