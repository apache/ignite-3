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

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Tracker that stores comparable value internally, this value can grow when {@link #update(Comparable)} method is called. The tracker gives
 * ability to wait for certain value, see {@link #waitFor(Comparable)}.
 */
public class PendingComparableValuesTracker<T extends Comparable<T>> {
    /** Map of comparable values to corresponding futures. */
    private final ConcurrentSkipListMap<T, Collection<CompletableFuture<Void>>> valueFutures = new ConcurrentSkipListMap<>();

    /** Current value. */
    public final AtomicReference<T> current;

    /**
     * Constructor with initial value.
     *
     * @param initialValue Initial value.
     */
    public PendingComparableValuesTracker(T initialValue) {
        current = new AtomicReference<>(initialValue);
    }

    /**
     * Updates the internal state, if it is lower than {@code newValue} and completes all futures waiting for {@code newValue}
     * that had been created for corresponding values that are lower than the given one.
     *
     * @param newValue New value.
     */
    public void update(T newValue) {
        for (Map.Entry<T, Collection<CompletableFuture<Void>>> e : valueFutures.entrySet()) {
            if (newValue.compareTo(e.getKey()) >= 0) {
                valueFutures.compute(e.getKey(), (k, v) -> {
                    if (v != null) {
                        v.forEach(f -> f.complete(null));
                    }

                    return null;
                });
            } else {
                break;
            }
        }

        while (true) {
            T current = this.current.get();

            if (newValue.compareTo(current) > 0) {
                if (this.current.compareAndSet(current, newValue)) {
                    return;
                }
            } else {
                return;
            }
        }
    }

    /**
     * Provides the future that is completed when this tracker's internal value reaches given one. If the internal value is greater or
     * equal then the given one, returns completed future.
     *
     * @param valueToWait Value to wait.
     * @return Future.
     */
    public CompletableFuture<Void> waitFor(T valueToWait) {
        if (current.get().compareTo(valueToWait) >= 0) {
            return completedFuture(null);
        }

        CompletableFuture<Void> future = new CompletableFuture<>();

        valueFutures.compute(valueToWait, (k, v) -> {
            if (v == null) {
                v = new ConcurrentLinkedDeque<>();
            }

            v.add(future);

            return v;
        });

        if (current.get().compareTo(valueToWait) >= 0) {
            future.complete(null);

            valueFutures.compute(valueToWait, (k, v) -> {
                if (v == null) {
                    return null;
                } else {
                    v.remove(future);
                }

                return v;
            });
        }

        return future;
    }

    /**
     * Returns current internal value.
     *
     * @return Current value.
     */
    public T current() {
        return current.get();
    }
}
