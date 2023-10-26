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

import java.util.function.Supplier;

/**
 * Logical piece of a trace that represents a single operation.
 */
public interface Span extends AutoCloseable {
    /**
     * Adds an event to the Span. The timestamp of the event will be the current time
     *
     * @param evtSupplier Event supplier.
     * @return {@code this} for chaining.
     */
    Span addEvent(Supplier<String> evtSupplier);

    /**
     * Adds attribute to span with {@code String} value.
     *
     * @param attrName Attribute name.
     * @param attrValSupplier Attribute value supplier. Supplier is used instead of strict tag value cause of it's lazy nature.
     */
    void addAttribute(String attrName, Supplier<String> attrValSupplier);

    /**
     * Records information about the {@link Throwable} to the {@link Span}.

     * @param exception the {@link Throwable} to record.
     */
    void recordException(Throwable exception);

    /**
     * Returns {@code true} if this {@code SpanContext} is valid.
     *
     * @return {@code true} if this {@code SpanContext} is valid.
     */
    boolean isValid();

    <T, R extends Throwable> void whenComplete(T val, R throwable);

    Runnable wrap(Runnable runnable);

    void end();

    @Override
    void close();
}
