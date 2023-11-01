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
import java.util.concurrent.Executor;
import java.util.function.Consumer;
import java.util.function.Function;
import org.jetbrains.annotations.Nullable;

/**
 * Noop implementation of {@link SpanManager}.
 */
public class NoopSpanManager implements SpanManager {
    /** Instance. */
    public static final SpanManager INSTANCE = new NoopSpanManager();

    @Override
    public TraceSpan createSpan(String spanName, TraceSpan parent, boolean rootSpan, boolean endRequired) {
        return NoopSpan.INSTANCE;
    }

    @Override
    public <R> R createSpan(String spanName, @Nullable TraceSpan parent, boolean rootSpan, Function<TraceSpan, R> closure) {
        return closure.apply(NoopSpan.INSTANCE);
    }

    @Override
    public void createSpan(String spanName, @Nullable TraceSpan parent, boolean rootSpan, Consumer<TraceSpan> closure) {
        closure.accept(NoopSpan.INSTANCE);
    }

    @Override
    public Executor taskWrapping(Executor executor) {
        return executor;
    }

    @Override
    public @Nullable Map<String, String> serializeSpan() {
        return null;
    }

    @Override
    public TraceSpan restoreSpanContext(Map<String, String> headers) {
        return NoopSpan.INSTANCE;
    }
}
