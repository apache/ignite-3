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

package org.apache.ignite.internal.lang;

import java.util.Objects;

/**
 * Represents a closure that accepts three input parameters and returns no result.
 *
 * @param <T> The type of the first parameter to the closure.
 * @param <U> The type of the second parameter to the closure.
 * @param <V> The type of the third parameter to the closure.
 */
@FunctionalInterface
public interface IgniteTriConsumer<T, U, V> {
    /**
     * Performs the operation on the given arguments.
     *
     * @param t The first argument.
     * @param u The second argument.
     * @param v The third argument.
     */
    void accept(T t, U u, V v);

    /**
     * Returns a composed {@code IgniteTriConsumer} that performs, in sequence, this operation followed by the {@code after} closure.
     * If {@code after} throws an exception, it should be handled by the caller of the closure. If performing this closure throws
     * an exception, the {@code after} closure will not be performed.
     *
     * @param after Closure to execute after this {@code IgniteTriConsumer} completes.
     * @return Composed {@code IgniteTriConsumer}.
     */
    default IgniteTriConsumer<T, U, V> andThen(IgniteTriConsumer<? super T, ? super U, ? super V> after) {
        Objects.requireNonNull(after);

        return (t, u, v) -> {
            accept(t, u, v);

            after.accept(t, u, v);
        };
    }
}
