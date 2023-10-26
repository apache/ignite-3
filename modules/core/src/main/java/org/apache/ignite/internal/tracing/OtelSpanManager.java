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

package org.apache.ignite.internal.tracing;

import static io.opentelemetry.api.GlobalOpenTelemetry.getTracer;

import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import org.jetbrains.annotations.Nullable;

/**
 * Tracing context.
 */
public class OtelSpanManager {
    /**
     * ddd.
     *
     * @param spanName Span name to create.
     * @param parent Parent context.
     * @param rootSpan Root span.
     * @return Created span.
     */
    private static Span generateSpan(String spanName, @Nullable Span parent, boolean rootSpan, boolean sync) {
        if (!rootSpan && !io.opentelemetry.api.trace.Span.current().getSpanContext().isValid()) {
            return NoopSpan.INSTANCE;
        }

        if (parent instanceof OtelSpan) {
            ((OtelSpan) parent).span.makeCurrent();
        }

        var span = getTracer(null).spanBuilder(spanName).startSpan();
        var scope = span.makeCurrent();

        return new OtelSpan(scope, span, sync);
    }

    public static Span rootSpan(String spanName) {
        return generateSpan(spanName, null, true, true);
    }

    public static <R> R asyncRootSpan(String spanName, Function<Span, R> closure) {
        return asyncSpan(spanName, null, true, closure);
    }

    public static Span span(String spanName) {
        return generateSpan(spanName, null, false, true);
    }

    private static <R> R asyncSpan(String spanName, Span parent, boolean rootSpan, Function<Span, R> closure) {
        Span span = generateSpan(spanName, parent, rootSpan, false);

        try (span) {
            var res = closure.apply(span);

            if (res instanceof CompletableFuture) {
                ((CompletableFuture<?>) res).whenComplete(span::whenComplete);
            } else {
                span.end();
            }

            return res;
        } catch (Throwable ex) {
            span.recordException(ex);

            throw ex;
        }
    }

    public static <R> R asyncSpan(String spanName, Span parent, Function<Span, R> closure) {
        return asyncSpan(spanName, parent, true, closure);
    }

    public static <R> R asyncSpan(String spanName, Function<Span, R> closure) {
        return asyncSpan(spanName, null, false, closure);
    }

    public static Span asyncSpan(String spanName) {
        return generateSpan(spanName, NoopSpan.INSTANCE, false, false);
    }

    public static Span asyncSpan(String spanName, Span parent) {
        return generateSpan(spanName, parent, false, false);
    }
}
