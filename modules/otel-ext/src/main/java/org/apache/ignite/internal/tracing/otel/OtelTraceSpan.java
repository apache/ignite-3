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

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import java.util.function.Supplier;
import org.apache.ignite.internal.tracing.TraceSpan;
import org.jetbrains.annotations.Nullable;

/**
 * Implementation of a {@link TraceSpan}.
 */
public class OtelTraceSpan implements TraceSpan {
    /** Trace context. */
    protected final Context ctx;

    /** Scope delegate. */
    protected final Scope scope;

    /** Span delegate. */
    protected final Span span;

    /** {@code true} if current span should be ended at close moment. */
    protected final boolean endRequired;

    /**
     * Constructor.
     *
     * @param ctx Trace context.
     * @param scope Scope delegate.
     * @param span Span delegate.
     * @param endRequired {@code true} if current span should be ended at close moment.
     */
    public OtelTraceSpan(Context ctx, Scope scope, Span span, boolean endRequired) {
        this.ctx = ctx;
        this.scope = scope;
        this.span = span;
        this.endRequired = endRequired;
    }

    @Override
    public TraceSpan addEvent(Supplier<String> evtSupplier) {
        span.addEvent(evtSupplier.get());

        return this;
    }

    @Override
    public void addAttribute(String attrName, Supplier<String> attrValSupplier) {
        span.setAttribute(attrName, attrValSupplier.get());
    }

    @Override
    public boolean isValid() {
        return span.getSpanContext().isValid();
    }

    @Override
    public <T> @Nullable T getContext() {
        return (T) ctx;
    }

    @Override
    public <T, R extends Throwable> void whenComplete(T val, R throwable) {
        if (throwable != null) {
            span.recordException(throwable);
        } else {
            span.end();
        }
    }

    @Override
    public void recordException(Throwable exception) {
        span.recordException(exception);
    }

    @Override
    public void end() {
        span.end();
    }

    @Override
    public void close() {
        if (endRequired) {
            end();
        }

        scope.close();
    }
}
