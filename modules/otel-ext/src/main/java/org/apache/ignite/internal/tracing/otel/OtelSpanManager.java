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

import static io.opentelemetry.api.GlobalOpenTelemetry.getPropagators;
import static org.apache.ignite.internal.util.IgniteUtils.capacity;

import com.google.auto.service.AutoService;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.api.trace.TracerProvider;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.propagation.TextMapGetter;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;
import java.util.function.Function;
import org.apache.ignite.internal.tracing.NoopSpan;
import org.apache.ignite.internal.tracing.SpanManager;
import org.apache.ignite.internal.tracing.TraceSpan;
import org.jetbrains.annotations.Nullable;

/**
 * Tracing manager.
 */
@AutoService(SpanManager.class)
public class OtelSpanManager implements SpanManager {
    private static final TextMapGetter<Map<String, String>> GETTER = new MapGetter();
    private volatile TracerProvider tracerProvider = TracerProvider.noop();
    private volatile Tracer tracer;

    /**
     * AA.
     */
    public OtelSpanManager() {
        // AutoConfiguredOpenTelemetrySdk telemetrySdk = AutoConfiguredOpenTelemetrySdk.builder().build();

        tracer = tracerProvider.get(null);
    }

    @Override
    public TraceSpan create(@Nullable TraceSpan parentSpan, String lb, boolean forceTracing, boolean endRequired) {
        boolean isBeginOfTrace = !Span.current().getSpanContext().isValid();
        boolean invalidParent = parentSpan == null || !parentSpan.isValid();

        if (isBeginOfTrace && !forceTracing && invalidParent) {
            return NoopSpan.INSTANCE;
        }

        var spanBuilder = tracer.spanBuilder(lb);

        if (!invalidParent) {
            spanBuilder.setParent(parentSpan.getContext());
        }

        var span = spanBuilder.startSpan();
        var scope = span.makeCurrent();
        Context ctx = Context.current();

        return new OtelTraceSpan(ctx, scope, span, endRequired);
    }

    @Override
    public <R> R create(@Nullable TraceSpan parentSpan, String lb, boolean forceTracing, Function<TraceSpan, R> closure) {
        TraceSpan span = create(parentSpan, lb, forceTracing, false);

        try (span) {
            return span.endWhenComplete(closure.apply(span));
        } catch (Throwable ex) {
            span.recordException(ex);

            throw ex;
        }
    }

    @Override
    public void create(@Nullable TraceSpan parentSpan, String lb, boolean forceTracing, Consumer<TraceSpan> closure) {
        TraceSpan span = create(parentSpan, lb, forceTracing, true);

        try (span) {
            closure.accept(span);
        } catch (Throwable ex) {
            span.recordException(ex);

            throw ex;
        }
    }

    @Override
    public Executor taskWrapping(Executor executor) {
        return Context.taskWrapping(executor);
    }

    @Override
    public ExecutorService taskWrapping(ExecutorService executorService) {
        return Context.taskWrapping(executorService);
    }

    @Override
    public @Nullable Map<String, String> serializeSpanContext() {
        if (Span.current().getSpanContext().isValid()) {
            var propagator = getPropagators().getTextMapPropagator();
            Map<String, String> headers = new HashMap<>(capacity(propagator.fields().size()));

            propagator.inject(Context.current(), headers, (carrier, key, val) -> carrier.put(key, val));

            return headers;
        }

        return null;
    }

    @Override
    public TraceSpan restoreSpanContext(Map<String, String> headers) {
        Context ctx = getPropagators().getTextMapPropagator().extract(Context.current(), headers, GETTER);

        var span = Span.fromContext(ctx);
        var scope = ctx.makeCurrent();

        return new OtelTraceSpan(ctx, scope, span, true);
    }

    @Override
    public Runnable wrap(Runnable runnable) {
        return Context.current().wrap(runnable);
    }

    @Override
    public <T> Callable<T> wrap(Callable<T> callable) {
        return Context.current().wrap(callable);
    }

    @Override
    public <R> CompletableFuture<R> wrap(CompletableFuture<R> fut) {
        if (!Span.current().getSpanContext().isValid()) {
            return fut;
        }

        return new TracingFuture<>(fut);
    }

    private static class MapGetter implements TextMapGetter<Map<String, String>> {
        @Override
        public Iterable<String> keys(Map<String, String> carrier) {
            return carrier.keySet();
        }

        @Override
        public String get(Map<String, String> carrier, String key) {
            return carrier.get(key);
        }
    }

    @Override
    public TraceSpan current() {
        var span = Span.current();

        if (!Span.current().getSpanContext().isValid()) {
            return NoopSpan.INSTANCE;
        }

        var scope = span.makeCurrent();

        return new OtelTraceSpan(Context.current(), scope, span, false);
    }
}
