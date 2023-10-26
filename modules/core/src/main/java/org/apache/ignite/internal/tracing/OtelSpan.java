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

import io.opentelemetry.context.Scope;
import java.util.function.Supplier;

public class OtelSpan implements Span {
    protected final Scope scope;
    protected final io.opentelemetry.api.trace.Span span;
    protected final boolean sync;

    public OtelSpan(Scope scope, io.opentelemetry.api.trace.Span span, boolean sync) {
        this.scope = scope;
        this.span = span;
        this.sync = sync;
    }

    @Override
    public Span addEvent(Supplier<String> evtSupplier) {
        span.addEvent(evtSupplier.get());

        return this;
    }

    @Override
    public void addAttribute(String attrName, Supplier<String> attrValSupplier) {
        span.setAttribute(attrName, attrValSupplier.get());
    }

    @Override
    public void recordException(Throwable exception) {
        span.recordException(exception);
    }

    @Override
    public boolean isValid() {
        return span.getSpanContext().isValid();
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
    public Runnable wrap(Runnable runnable) {
        return () -> {
            try (Scope ignored = span.makeCurrent()) {
                runnable.run();
            }
        };
    }

    @Override
    public void end() {
        span.end();
    }

    @Override
    public void close() {
        if (sync) {
            end();
        }

        scope.close();
    }
}
