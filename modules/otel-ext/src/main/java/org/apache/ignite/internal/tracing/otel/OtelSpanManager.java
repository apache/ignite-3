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
import static io.opentelemetry.api.GlobalOpenTelemetry.getTracer;
import static org.apache.ignite.internal.util.IgniteUtils.capacity;

import com.google.auto.service.AutoService;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import io.opentelemetry.context.propagation.TextMapGetter;
import io.opentelemetry.sdk.autoconfigure.AutoConfiguredOpenTelemetrySdk;
import java.util.HashMap;
import java.util.Map;
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

    public OtelSpanManager() {
        AutoConfiguredOpenTelemetrySdk.initialize();
    }

    @Override
    public TraceSpan createSpan(String spanName, @Nullable TraceSpan parent, boolean rootSpan, boolean endRequired) {
        boolean isBeginOfTrace = !Span.current().getSpanContext().isValid();

        if (isBeginOfTrace && !rootSpan && parent == null) {
            return NoopSpan.INSTANCE;
        }

        var spanBuilder = getTracer(null).spanBuilder(spanName);

        if (parent != null && parent.getContext() != null) {
            spanBuilder.setParent(parent.getContext());
        }

        var span = spanBuilder.startSpan();
        var scope = span.makeCurrent();
        var ctx = Context.current();

        return new OtelTraceSpan(ctx, scope, span, endRequired);
    }

    @Override
    public <R> R createSpan(String spanName, @Nullable TraceSpan parent, boolean rootSpan, Function<TraceSpan, R> closure) {
        TraceSpan span = createSpan(spanName, parent, rootSpan, false);

        try (span) {
            return span.wrap(closure.apply(span));
        } catch (Throwable ex) {
            span.recordException(ex);

            throw ex;
        }
    }

    @Override
    public void createSpan(String spanName, @Nullable TraceSpan parent, boolean rootSpan, Consumer<TraceSpan> closure) {
        TraceSpan span = createSpan(spanName, parent, rootSpan, true);

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
    public @Nullable Map<String, String> serializeSpan() {
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
}
