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

import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import org.jetbrains.annotations.Nullable;

/**
 * Tracker that stores comparable value internally, this value can grow when {@link #update} method is called. The tracker gives
 * ability to wait for certain value, see {@link #waitFor(Comparable)}. In addition to {@link PendingComparableValuesTracker}
 * given tracker supports multiple independent waiters for same value.
 */
public class PendingIndependentComparableValuesTracker<T extends Comparable<T>, R> extends PendingComparableValuesTracker<T, R> {
    /** Map of comparable values to corresponding futures. */
    private final ConcurrentSkipListMap<T, Set<CompletableFuture<R>>> valueFutures = new ConcurrentSkipListMap<>();

    /**
     * Constructor with initial value.
     *
     * @param initialValue Initial value.
     */
    public PendingIndependentComparableValuesTracker(T initialValue) {
        super(initialValue);
    }

    @Override
    protected void completeWaitersOnUpdate(T newValue, @Nullable R futureResult) {
        ConcurrentNavigableMap<T, Set<CompletableFuture<R>>> smallerFutures = valueFutures.headMap(newValue, true);

        smallerFutures.forEach((k, futureSet) -> futureSet.forEach(f -> f.complete(futureResult)));

        smallerFutures.clear();
    }

    @Override
    protected CompletableFuture<R> addNewWaiter(T valueToWait) {
        CompletableFuture<R> future = new CompletableFuture<>();

        valueFutures.compute(valueToWait, (k, v) -> {
            if (v == null) {
                v = ConcurrentHashMap.newKeySet();
            }
            v.add(future);

            return v;
        });

        Map.Entry<T, R> currentEntry = currentEntry();

        if (currentEntry.getKey().compareTo(valueToWait) >= 0) {
            future.complete(currentEntry.getValue());

            // The future isn't removed from the collection in order not to catch a ConcurrentModificationException
            // It's safe to remove it within smallerFutures.clear() on next completeWaitersOnUpdate iteration.
        }

        return future;
    }

    @Override
    protected void cleanupWaitersOnClose(TrackerClosedException trackerClosedException) {
        valueFutures.forEach((k, futureSet) -> futureSet.forEach(f -> f.completeExceptionally(trackerClosedException)));

        valueFutures.clear();
    }

    /** Returns true if this tracker contains no waiters. */
    @Override
    public boolean isEmpty() {
        return valueFutures.isEmpty();
    }
}
