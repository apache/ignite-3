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

import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.apache.ignite.internal.tracing.TracingManager.asyncSpan;
import static org.apache.ignite.internal.tracing.TracingManager.rootSpan;
import static org.apache.ignite.internal.tracing.TracingManager.span;
import static org.apache.ignite.internal.tracing.TracingManager.spanWithResult;
import static org.apache.ignite.internal.tracing.TracingManager.wrap;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.opentelemetry.context.Context;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.internal.tracing.TraceSpan;
import org.apache.ignite.internal.tracing.TracingManager;
import org.junit.jupiter.api.Test;

/** For {@link io.opentelemetry.context.Scope} testing. */
public class ScopeTest {
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

        TracingManager.spanWithResult("process", parent, (span) -> {
            assertTrue(span.isValid());

            return null;
        });

        assertEquals(Context.root(), Context.current());
    }

    @Test
    public void shouldPassContextToChild() throws Exception {
        var f = new CompletableFuture<Integer>();
        AtomicReference<TraceSpan> processSpan = new AtomicReference<>();

        try (var rootSpan = rootSpan("main")) {
            var t1 = new Thread(() -> f.thenCompose((s) -> spanWithResult("process", (span) -> {
                processSpan.set(span);

                return completedFuture(2);
            })));
            t1.start();

            new Thread(wrap(() -> span("complete", (span) -> f.complete(1)))).start();
        }

        assertEquals(1, f.get());
        assertTrue(processSpan.get().isValid());
    }
}
