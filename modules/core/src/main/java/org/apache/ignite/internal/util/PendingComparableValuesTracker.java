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

import static java.util.concurrent.CompletableFuture.completedFuture;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * Tracker that stores comparable value internally, this value can grow when {@link #update(Comparable)} method is called. The tracker gives
 * ability to wait for certain value, see {@link #waitFor(Comparable)}.
 */
public class PendingComparableValuesTracker<T extends Comparable<T>> {
    private static final VarHandle CURRENT;

    static {
        try {
            CURRENT = MethodHandles.lookup().findVarHandle(PendingComparableValuesTracker.class, "current", Comparable.class);
        } catch (ReflectiveOperationException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    /** Map of comparable values to corresponding futures. */
    private final ConcurrentSkipListMap<T, CompletableFuture<Void>> valueFutures = new ConcurrentSkipListMap<>();

    /** Current value. */
    private volatile T current;

    /**
     * Constructor with initial value.
     *
     * @param initialValue Initial value.
     */
    public PendingComparableValuesTracker(T initialValue) {
        current = initialValue;
    }

    /**
     * Updates the internal state, if it is lower than {@code newValue} and completes all futures waiting for {@code newValue}
     * that had been created for corresponding values that are lower than the given one.
     *
     * @param newValue New value.
     */
    public void update(T newValue) {
        while (true) {
            T current = this.current;

            if (newValue.compareTo(current) <= 0) {
                break;
            }

            if (CURRENT.compareAndSet(this, current, newValue)) {
                ConcurrentNavigableMap<T, CompletableFuture<Void>> smallerFutures = valueFutures.headMap(newValue, true);

                smallerFutures.forEach((k, f) -> f.complete(null));

                smallerFutures.clear();

                break;
            }
        }
    }

    /**
     * Provides the future that is completed when this tracker's internal value reaches given one. If the internal value is greater or equal
     * then the given one, returns completed future.
     *
     * @param valueToWait Value to wait.
     * @return Future.
     */
    public CompletableFuture<Void> waitFor(T valueToWait) {
        if (current.compareTo(valueToWait) >= 0) {
            return completedFuture(null);
        }

        CompletableFuture<Void> future = valueFutures.computeIfAbsent(valueToWait, k -> new CompletableFuture<>());

        if (current.compareTo(valueToWait) >= 0) {
            future.complete(null);

            valueFutures.remove(valueToWait);
        }

        return future;
    }

    /**
     * Returns current internal value.
     *
     * @return Current value.
     */
    public T current() {
        return current;
    }
}
