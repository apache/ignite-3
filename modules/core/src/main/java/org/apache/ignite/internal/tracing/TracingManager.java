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

import java.util.Map;
import java.util.ServiceLoader;
import java.util.ServiceLoader.Provider;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;
import java.util.function.Function;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

/**
 * Tracing Manager.
 */
public class TracingManager {
    /** Instance. */
    private static SpanManager SPAN_MANAGER = NoopSpanManager.INSTANCE;

    /**
     * Gets current span manager.
     *
     * @return Span manager.
     */
    @TestOnly
    public static SpanManager getSpanManager() {
        return SPAN_MANAGER;
    }

    /**
     * Load tracing manager by {@link ServiceLoader} mechanism.
     *
     * @return list of loaded exporters.
     */
    private static SpanManager loadManager() {
        return ServiceLoader
                .load(SpanManager.class)
                .stream()
                .map(Provider::get)
                .findFirst()
                .orElse(NoopSpanManager.INSTANCE);
    }

    public static void initialize(SpanManager spanManager) {
        SPAN_MANAGER = spanManager;
    }

    /**
     * Call closure in span with given label.
     *
     * @param lb Label.
     * @return Created span.
     */
    public static TraceSpan asyncSpan(String lb) {
        return SPAN_MANAGER.create(null, lb, false, false);
    }

    /**
     * Creates span given name.
     *
     * @param lb Label.
     * @return Created span.
     */
    public static TraceSpan rootSpan(String lb) {
        return SPAN_MANAGER.create(null, lb, true, true);
    }

    /**
     * Call closure in span with given label.
     *
     * @param lb Label.
     * @param closure Closure.
     * @return Created span.
     */
    public static <R> R rootSpan(String lb, Function<TraceSpan, R> closure) {
        return SPAN_MANAGER.create(null, lb, true, closure);
    }

    /**
     * Creates span given label.
     *
     * @param lb Label.
     * @return Created span.
     */
    public static TraceSpan span(String lb) {
        return SPAN_MANAGER.create(null, lb, false, true);
    }

    /**
     * Call closure in span with given label.
     *
     * @param spanName Name of span to create.
     * @param closure Closure.
     */
    public static void span(String spanName, Consumer<TraceSpan> closure) {
        SPAN_MANAGER.create(null, spanName, false, closure);
    }

    /**
     * Call closure in span with given label.
     *
     * @param parentSpan Parent span.
     * @param lb Label.
     * @param closure Closure.
     */
    public static void span(@Nullable TraceSpan parentSpan, String lb, Consumer<TraceSpan> closure) {
        SPAN_MANAGER.create(parentSpan, lb, false, closure);
    }

    /**
     * Call closure in span with given label.
     *
     * @param lb Label.
     * @param closure Closure.
     * @return Closure result.
     */
    public static <R> R span(String lb, Function<TraceSpan, R> closure) {
        return SPAN_MANAGER.create(null, lb, false, closure);
    }

    /**
     * Call closure in span with given label.
     *
     * @param parentSpan Parent span.
     * @param lb Label.
     * @param closure Closure.
     * @return Closure result.
     */
    public static <R> R span(@Nullable TraceSpan parentSpan, String lb, Function<TraceSpan, R> closure) {
        return SPAN_MANAGER.create(parentSpan, lb, false, closure);
    }

    /**
     * Returns a {@link Runnable} that restore trace context and then invokes the input {@link Runnable}.
     */
    public static Executor taskWrapping(Executor executor) {
        return SPAN_MANAGER.taskWrapping(executor);
    }

    /**
     * Returns a {@link Callable} that makes this the current context and then invokes the input {@link Callable}.
     */
    public static ExecutorService taskWrapping(ExecutorService executorService) {
        return SPAN_MANAGER.taskWrapping(executorService);
    }

    /**
     * Returns a {@link Callable} that preserve current trace context and then invokes the input {@link Callable}.
     */
    public static <T> Callable<T> wrap(Callable<T> callable) {
        return SPAN_MANAGER.wrap(callable);
    }

    /**
     * Returns a {@link Runnable} that preserve current trace context and then invokes the input {@link Runnable}.
     */
    public static Runnable wrap(Runnable runnable) {
        return SPAN_MANAGER.wrap(runnable);
    }

    public static <R> CompletableFuture<R> wrap(CompletableFuture<R> fut) {
        return SPAN_MANAGER.wrap(fut);
    }

    public static @Nullable Map<String, String> serializeSpanContext() {
        return SPAN_MANAGER.serializeSpanContext();
    }

    public static TraceSpan restoreSpanContext(Map<String, String> headers) {
        return SPAN_MANAGER.restoreSpanContext(headers);
    }

    public static TraceSpan current() {
        return SPAN_MANAGER.current();
    }
}
