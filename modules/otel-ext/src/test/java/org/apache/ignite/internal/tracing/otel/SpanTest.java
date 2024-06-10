/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.tracing.otel;

import static org.apache.ignite.internal.tracing.TracingManager.asyncSpan;
import static org.apache.ignite.internal.tracing.TracingManager.rootSpan;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.sdk.trace.ReadableSpan;
import io.opentelemetry.sdk.trace.data.SpanData;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher;
import org.apache.ignite.internal.tracing.GridTracingManager;
import org.apache.ignite.internal.tracing.TraceSpan;
import org.apache.ignite.internal.tracing.TracingManager;
import org.apache.ignite.internal.tracing.configuration.TracingConfiguration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/** For {@link Span} testing. */
@ExtendWith(ConfigurationExtension.class)
public class SpanTest {
    @InjectConfiguration
    private TracingConfiguration tracingConfiguration;

    @BeforeEach
    void before() {
        GridTracingManager.initialize("ignite-node-0", tracingConfiguration);

        assertThat(tracingConfiguration.change(tracingChange -> {
            tracingChange.changeRatio(1.);
        }), CompletableFutureMatcher.willCompleteSuccessfully());
    }

    @Test
    public void shouldEndedWithSpanClosure() {
        var actual = startInSpanWithResult((span) -> {
            var fut = new CompletableFuture<>();
            fut.completeExceptionally(new RuntimeException());

            return fut;
        });

        assertEquals(StatusCode.ERROR, actual.getStatus().getStatusCode());
        assertTrue(actual.hasEnded());

        var actual1 = startInSpanWithResult((span) -> {
            throw new RuntimeException();
        });

        assertEquals(StatusCode.ERROR, actual1.getStatus().getStatusCode());
        assertTrue(actual1.hasEnded());
    }

    @Test
    public void shouldEndedWithTryWithResources() {
        var actual = startInTryWithResources((span) -> {
            var fut = new CompletableFuture<>();
            fut.completeExceptionally(new RuntimeException());

            return fut;
        });

        assertEquals(StatusCode.ERROR, actual.getStatus().getStatusCode());
        assertTrue(actual.hasEnded());

        var actual1 = startInTryWithResources((span) -> {
            throw new RuntimeException();
        });

        assertEquals(StatusCode.ERROR, actual1.getStatus().getStatusCode());
        assertTrue(actual1.hasEnded());
    }

    private static <R> SpanData startInSpanWithResult(Function<TraceSpan, R> closure) {
        AtomicReference<ReadableSpan> otelSpan = new AtomicReference<>();

        try (var ignored = rootSpan("root.request")) {
            TracingManager.span("process", (span) -> {
                otelSpan.set((ReadableSpan) Span.current());

                return closure.apply(span);
            });
        } catch (Exception ignored) {
            // No-op.
        }

        return otelSpan.get().toSpanData();
    }

    private static <R> SpanData startInTryWithResources(Function<TraceSpan, R> closure) {
        AtomicReference<ReadableSpan> otelSpan = new AtomicReference<>();

        try (var ignored = rootSpan("root.request")) {
            var span = asyncSpan("process");

            try (span) {
                otelSpan.set((ReadableSpan) Span.current());

                span.endWhenComplete(closure.apply(span));
            } catch (Exception e) {
                span.recordException(e);
            }
        }

        return otelSpan.get().toSpanData();
    }
}
