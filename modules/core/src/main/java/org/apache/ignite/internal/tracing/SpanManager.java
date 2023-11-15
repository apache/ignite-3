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

import io.opentelemetry.context.Context;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;
import java.util.function.Function;
import org.jetbrains.annotations.Nullable;

/**
 * Manager for {@link TraceSpan} instances.
 */
public interface SpanManager {
    /**
     * Creates Span with given name.
     *
     * @param spanName Span name to create.
     * @param parent Parent context.
     * @param rootSpan Root span.
     * @return Created span.
     */
    TraceSpan createSpan(String spanName, @Nullable TraceSpan parent, boolean rootSpan, boolean endRequired);

    /**
     * Creates Span with given name.
     *
     * @param spanName Span name to create.
     * @param parent Parent context.
     * @param rootSpan Root span.
     * @param closure Closure.
     * @return Closure result.
     */
    <R> R createSpan(String spanName, @Nullable TraceSpan parent, boolean rootSpan, Function<TraceSpan, R> closure);

    /**
     * Call closure in span with given name.
     *
     * @param spanName Span name to create.
     * @param parent Parent context.
     * @param rootSpan Root span.
     * @param closure Closure.
     */
    void createSpan(String spanName, @Nullable TraceSpan parent, boolean rootSpan, Consumer<TraceSpan> closure);

    /**
     * Returns a {@link Runnable} that restore trace context and then invokes the input {@link Runnable}.
     */
    Executor taskWrapping(Executor executor);

    /**
     * Returns a {@link Runnable} that restore trace context and then invokes the input {@link Runnable}.
     */
    ExecutorService taskWrapping(ExecutorService executorService);

    /**
     * Returns a {@link Callable} that makes this the {@linkplain Context#current() current context}
     * and then invokes the input {@link Callable}.
     */
    <T> Callable<T> wrap(Callable<T> callable);

    /**
     * Returns a {@link Runnable} that makes this the {@linkplain Context#current() current context}
     * and then invokes the input {@link Runnable}.
     */
    Runnable wrap(Runnable runnable);

    /**
     * Returns a {@link CompletableFuture} that makes this the {@linkplain Context#current() current context}
     * and then invokes the input {@link CompletableFuture}.
     */
    <R> CompletableFuture<R> wrap(CompletableFuture<R> fut);

    @Nullable Map<String, String> serializeSpan();

    TraceSpan restoreSpanContext(Map<String, String> headers);
}
