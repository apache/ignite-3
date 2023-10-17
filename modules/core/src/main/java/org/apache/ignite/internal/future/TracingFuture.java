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

package org.apache.ignite.internal.future;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import java.util.concurrent.CompletableFuture;

/**
 * A analogue of {@link CompletableFuture} that preserved tracing context.
 *
 * @param <T> The result type returned by this future's join and get methods.
 * @see CompletableFuture
 */
public class TracingFuture<T> extends CompletableFuture<T> {
    private final Context ctx;

    private TracingFuture() {
        ctx = Context.current();
    }

    /** {@inheritDoc} */
    @Override
    public boolean complete(T value) {
        try (Scope ignored = ctx.makeCurrent()) {
            return super.complete(value);
        }
    }

    /** {@inheritDoc} */
    @Override
    public boolean completeExceptionally(Throwable ex) {
        try (Scope ignored = ctx.makeCurrent()) {
            return super.completeExceptionally(ex);
        }
    }

    /**
     * Create future that preserved tracing context.
     *
     * @param <R> The result type returned by this future.
     * @return Future that preserved tracing context.
     */
    public static <R> CompletableFuture<R> create() {
        if (!Span.current().getSpanContext().isValid()) {
            return new CompletableFuture<>();
        }

        return new TracingFuture<>();
    }

    /**
     * Wrap future to preserved tracing context.
     *
     * @param fut0 A future for which context needs to be preserved.
     * @param <R> The result type returned by this future.
     * @return Future that preserved tracing context.
     */
    public static <R> CompletableFuture<R> wrap(CompletableFuture<R> fut0) {
        if (fut0.getClass().equals(TracingFuture.class) || !Span.current().getSpanContext().isValid()) {
            return fut0;
        }

        var fut = new TracingFuture<R>();

        return fut0.whenComplete((r, t) -> {
            if (t != null) {
                fut.completeExceptionally(t);
            } else {
                fut.complete(r);
            }
        });
    }
}
