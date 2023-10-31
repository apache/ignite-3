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

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.Function;
import org.jetbrains.annotations.Nullable;

/**
 * Tracing context.
 */
public class OtelSpanManager implements SpanManager {
    /** Instance. */
    private static final OtelSpanManager INSTANCE = new OtelSpanManager();

    /**
     * Creates Span with given name.
     *
     * @param spanName Span name to create.
     * @param parent Parent context.
     * @param rootSpan Root span.
     * @param endRequired Root span.
     * @return Created span.
     */
    @Override
    public TraceSpan createSpan(String spanName, @Nullable TraceSpan parent, boolean rootSpan, boolean endRequired) {
        boolean isBeginOfTrace = !Span.current().getSpanContext().isValid();

        if (isBeginOfTrace && !rootSpan && parent == null) {
            return NoopTraceSpan.INSTANCE;
        }

        var spanBuilder = getTracer(null).spanBuilder(spanName);

        if (parent != null) {
            spanBuilder.setParent(parent.getContext());
        }

        var span = spanBuilder.startSpan();
        var scope = span.makeCurrent();
        var ctx = Context.current();

        return new OtelTraceSpan(ctx, scope, span, endRequired);
    }

    /**
     * Creates Span with given name.
     *
     * @param spanName Span name to create.
     * @param parent Parent context.
     * @param rootSpan Root span.
     * @param closure Closure.
     * @return Created span.
     */
    private static <R> R createSpan(String spanName, @Nullable TraceSpan parent, boolean rootSpan, Function<TraceSpan, R> closure) {
        TraceSpan span = INSTANCE.createSpan(spanName, parent, rootSpan, false);

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

    /**
     * Creates span given name.
     *
     * @param spanName Name of span to create.
     * @return Created span.
     */
    public static TraceSpan rootSpan(String spanName) {
        return INSTANCE.createSpan(spanName, null, true, true);
    }

    /**
     * Creates span given name.
     *
     * @param spanName Name of span to create.
     * @param closure Closure.
     * @return Created span.
     */
    public static <R> R rootSpan(String spanName, Function<TraceSpan, R> closure) {
        return createSpan(spanName, null, true, closure);
    }

    /**
     * Creates span given name.
     *
     * @param spanName Name of span to create.
     * @return Created span.
     */
    public static TraceSpan span(String spanName) {
        return INSTANCE.createSpan(spanName, null, false, true);
    }

    /**
     * Call closure in span with given name.
     *
     * @param spanName Name of span to create.
     * @param closure Closure.
     */
    public static void span(String spanName, Consumer<TraceSpan> closure) {
        TraceSpan span = INSTANCE.createSpan(spanName, null, false, true);

        try (span) {
            closure.accept(span);
        } catch (Throwable ex) {
            span.recordException(ex);

            throw ex;
        }
    }

    /**
     * Call closure in span with given name.
     *
     * @param spanName Name of span to create.
     * @param closure Closure.
     * @return Closure result.
     */
    public static <R> R spanWithResult(String spanName, Function<TraceSpan, R> closure) {
        return createSpan(spanName, null, false, closure);
    }

    /**
     * Call closure in span with given name.
     *
     * @param spanName Name of span to create.
     * @return Created span.
     */
    public static TraceSpan asyncSpan(String spanName) {
        return INSTANCE.createSpan(spanName, null, false, false);
    }

    /**
     * Call closure in span with given name.
     *
     * @param spanName Name of span to create.
     * @param parent Parent context.
     * @return Created span.
     */
    public static TraceSpan asyncSpan(String spanName, TraceSpan parent) {
        return INSTANCE.createSpan(spanName, parent, false, false);
    }

    /**
     * Call closure in span with given name.
     *
     * @param spanName Name of span to create.
     * @param parent Parent context.
     * @param closure Closure.
     * @return Created span.
     */
    public static <R> R asyncSpan(String spanName, TraceSpan parent, Function<TraceSpan, R> closure) {
        return createSpan(spanName, parent, true, closure);
    }

    /**
     * Returns a {@link Runnable} that makes this the {@linkplain Context#current() current context}
     * and then invokes the input {@link Runnable}.
     */
    public static Runnable wrap(Runnable runnable) {
        return () -> {
            try (Scope ignored = Context.current().makeCurrent()) {
                runnable.run();
            }
        };
    }
}
