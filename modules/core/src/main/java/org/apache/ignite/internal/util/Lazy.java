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

package org.apache.ignite.internal.util;

import java.util.function.Supplier;
import org.jetbrains.annotations.Nullable;

/**
 * Value which will be initialized at the moment of very first access.
 *
 * @param <T> Type of the value.
 */
public class Lazy<T> {
    private static final Supplier<?> EMPTY = () -> {
        throw new IllegalStateException("Should not be called");
    };

    private volatile Supplier<T> supplier;

    // This is a safe race, because we follow two simple rules: single read and safe initialization
    @SuppressWarnings("FieldAccessedSynchronizedAndUnsynchronized")
    private @Nullable T val;

    /**
     * Creates the lazy value with the given value supplier.
     *
     * @param supplier A supplier of the value.
     */
    public Lazy(Supplier<@Nullable T> supplier) {
        this.supplier = supplier;
    }

    /** Returns the value. */
    public @Nullable T get() {
        T v = val;

        if (v == null) {
            if (supplier != EMPTY) {
                synchronized (this) {
                    if (supplier != EMPTY) {
                        v = supplier.get();
                        val = v;
                        supplier = (Supplier<T>) EMPTY; // help GC collects objects acquired by supplier's closure
                    }
                }
            }

            v = val;
        }

        return v;
    }
}
