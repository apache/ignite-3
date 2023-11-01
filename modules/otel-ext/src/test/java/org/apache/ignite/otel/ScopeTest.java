package org.apache.ignite.otel;

import static org.apache.ignite.internal.tracing.TracingManager.asyncSpan;
import static org.apache.ignite.internal.tracing.TracingManager.rootSpan;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.opentelemetry.context.Context;
import io.opentelemetry.sdk.autoconfigure.AutoConfiguredOpenTelemetrySdk;
import org.apache.ignite.internal.event.AbstractEventProducer;
import org.apache.ignite.internal.tracing.TraceSpan;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** For {@link AbstractEventProducer} testing. */
public class ScopeTest {
    @BeforeEach
    void setUp() {
        AutoConfiguredOpenTelemetrySdk.initialize();
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
