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
import org.jetbrains.annotations.Nullable;

public enum NoopTraceSpan implements TraceSpan {
    /** Instance. */
    INSTANCE;

    @Override
    public TraceSpan addEvent(Supplier<String> evtSupplier) {
        return this;
    }

    @Override
    public void addAttribute(String attrName, Supplier<String> attrValSupplier) {
        // No-op.
    }

    @Override
    public boolean isValid() {
        return false;
    }

    @Override
    public <T> @Nullable T getContext() {
        return null;
    }

    @Override
    public <T, R extends Throwable> void whenComplete(T val, R throwable) {
        // No-op.
    }

    @Override
    public void recordException(Throwable exception) {
        // No-op.
    }

    @Override
    public void end() {
        // No-op.
    }

    @Override
    public void close() {
        // No-op.
    }
}
