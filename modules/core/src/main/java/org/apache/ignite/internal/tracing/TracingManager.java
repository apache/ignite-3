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
import java.util.concurrent.Executor;
import java.util.function.Consumer;
import java.util.function.Function;
import org.jetbrains.annotations.Nullable;

/**
 * Tracing Manager.
 */
public class TracingManager {
    /** Instance. */
    private static final SpanManager SPAN_MANAGER = loadManager();

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

    /**
     * Call closure in span with given name.
     *
     * @param spanName Name of span to create.
     * @return Created span.
     */
    public static TraceSpan asyncSpan(String spanName) {
        return SPAN_MANAGER.createSpan(spanName, null, false, false);
    }

    /**
     * Call closure in span with given name.
     *
     * @param spanName Name of span to create.
     * @param parent Parent context.
     * @return Created span.
     */
    public static TraceSpan asyncSpan(String spanName, TraceSpan parent) {
        return SPAN_MANAGER.createSpan(spanName, parent, false, false);
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
        return SPAN_MANAGER.createSpan(spanName, parent, true, closure);
    }

    /**
     * Creates span given name.
     *
     * @param spanName Name of span to create.
     * @return Created span.
     */
    public static TraceSpan rootSpan(String spanName) {
        return SPAN_MANAGER.createSpan(spanName, null, true, true);
    }

    /**
     * Creates span given name.
     *
     * @param spanName Name of span to create.
     * @param closure Closure.
     * @return Created span.
     */
    public static <R> R rootSpan(String spanName, Function<TraceSpan, R> closure) {
        return SPAN_MANAGER.createSpan(spanName, null, true, closure);
    }


    /**
     * Creates span given name.
     *
     * @param spanName Name of span to create.
     * @return Created span.
     */
    public static TraceSpan span(String spanName) {
        return SPAN_MANAGER.createSpan(spanName, null, false, true);
    }

    /**
     * Call closure in span with given name.
     *
     * @param spanName Name of span to create.
     * @param closure Closure.
     */
    public static void span(String spanName, Consumer<TraceSpan> closure) {
        TraceSpan span = SPAN_MANAGER.createSpan(spanName, null, false, true);

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
        return SPAN_MANAGER.createSpan(spanName, null, false, closure);
    }

    public static Executor taskWrapping(Executor executor) {
        return SPAN_MANAGER.taskWrapping(executor);
    }

    public static @Nullable Map<String, String> serializeSpan() {
        return SPAN_MANAGER.serializeSpan();
    }

    public static TraceSpan restoreSpanContext(Map<String, String> headers) {
        return SPAN_MANAGER.restoreSpanContext(headers);
    }
}
