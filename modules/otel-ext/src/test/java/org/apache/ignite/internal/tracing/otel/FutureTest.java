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

import static org.apache.ignite.internal.tracing.TracingManager.rootSpan;
import static org.apache.ignite.internal.tracing.TracingManager.span;
import static org.apache.ignite.internal.tracing.TracingManager.wrap;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import io.opentelemetry.api.trace.Span;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/** Tests for CompletableFuture wrapper. */
public class FutureTest {
    @BeforeAll
    static void setUp() {
        // TracingManager.initialize("ignite-node-0", 1.0d);
    }

    @Test
    public void preserveContextInFutureHandler() {
        var fut = new CompletableFuture<>();

        rootSpan("root", (parent) -> {
            var allOf = span("child", (span) -> {
                var childSpanId = ((OtelTraceSpan) span).span.getSpanContext().getSpanId();
                var wrappedFut = wrap(fut);

                return CompletableFuture.allOf(
                        wrappedFut.whenComplete((res, ex) -> {
                            assertEquals(childSpanId, Span.current().getSpanContext().getSpanId());
                        }),
                        wrappedFut.thenCompose((res) -> {
                            assertEquals(childSpanId, Span.current().getSpanContext().getSpanId());

                            return CompletableFuture.completedFuture(10L);
                        }),
                        wrappedFut.handle((res, ex) -> {
                            assertEquals(childSpanId, Span.current().getSpanContext().getSpanId());

                            return CompletableFuture.completedFuture(10L);
                        }),
                        fut.whenComplete((res, ex) -> {
                            assertNotEquals(childSpanId, Span.current().getSpanContext().getSpanId());
                        })
                );
            });

            span("child2", (ignored) -> {
                fut.complete(null);
            });

            allOf.join();

            return null;
        });
    }
}
