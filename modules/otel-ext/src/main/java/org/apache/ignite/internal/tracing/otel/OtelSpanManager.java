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

import static io.opentelemetry.api.GlobalOpenTelemetry.getTracer;

import com.google.auto.service.AutoService;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import io.opentelemetry.sdk.autoconfigure.AutoConfiguredOpenTelemetrySdk;
import java.util.concurrent.CompletableFuture;
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

        if (parent != null) {
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
