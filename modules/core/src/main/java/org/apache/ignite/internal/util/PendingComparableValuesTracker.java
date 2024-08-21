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
import static java.util.concurrent.CompletableFuture.failedFuture;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.Comparator;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import org.apache.ignite.internal.close.ManuallyCloseable;
import org.apache.ignite.internal.lang.IgniteBiTuple;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.jetbrains.annotations.Nullable;

/**
 * Tracker that stores comparable value internally, this value can grow when {@link #update} method is called. The tracker gives
 * ability to wait for certain value, see {@link #waitFor(Comparable)}.
 */
public class PendingComparableValuesTracker<T extends Comparable<T>, R> implements ManuallyCloseable {
    private static final VarHandle CURRENT;

    private static final VarHandle CLOSE_GUARD;

    static {
        try {
            CURRENT = MethodHandles.lookup().findVarHandle(PendingComparableValuesTracker.class, "current", Map.Entry.class);
            CLOSE_GUARD = MethodHandles.lookup().findVarHandle(PendingComparableValuesTracker.class, "closeGuard", boolean.class);
        } catch (ReflectiveOperationException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    /** Map of comparable values to corresponding futures. */
    private final ConcurrentSkipListMap<T, CompletableFuture<R>> valueFutures = new ConcurrentSkipListMap<>();

    /** Current value along with associated result. */
    @SuppressWarnings("FieldMayBeFinal") // Changed through CURRENT VarHandle.
    private volatile Map.Entry<T, @Nullable R> current;

    /** Prevents double closing. */
    @SuppressWarnings("unused")
    private volatile boolean closeGuard;

    /** Busy lock to close synchronously. */
    private final IgniteSpinBusyLock busyLock = new IgniteSpinBusyLock();

    private final Comparator<Map.Entry<T, @Nullable R>> comparator;

    /**
     * Constructor with initial value.
     *
     * @param initialValue Initial value.
     */
    public PendingComparableValuesTracker(T initialValue) {
        current = new IgniteBiTuple<>(initialValue, null);
        comparator = Map.Entry.comparingByKey(Comparator.nullsFirst(Comparator.naturalOrder()));
    }

    /**
     * Updates the internal state, if it is lower than {@code newValue} and completes all futures waiting for {@code newValue}
     * that had been created for corresponding values that are lower than the given one.
     *
     * @param newValue New value.
     * @param futureResult A result that will be used to complete a future returned by the
     *        {@link PendingComparableValuesTracker#waitFor(Comparable)}.
     * @throws TrackerClosedException if the tracker is closed.
     */
    public void update(T newValue, @Nullable R futureResult) {
        while (true) {
            if (!busyLock.enterBusy()) {
                throw new TrackerClosedException();
            }

            try {
                Map.Entry<T, @Nullable R> current = this.current;

                IgniteBiTuple<T, @Nullable R> newEntry = new IgniteBiTuple<>(newValue, futureResult);

                if (comparator.compare(newEntry, current) <= 0) {
                    break;
                }

                if (CURRENT.compareAndSet(this, current, newEntry)) {
                    completeWaitersOnUpdate(newValue, futureResult);
                    break;
                }
            } finally {
                busyLock.leaveBusy();
            }
        }
    }

    private static final IgniteLogger LOG = Loggers.forClass(PendingComparableValuesTracker.class);

    /**
     * Provides the future that is completed when this tracker's internal value reaches given one. If the internal value is greater or equal
     * then the given one, returns completed future.
     *
     * <p>When the tracker is closed, the future will complete with an {@link TrackerClosedException}.
     *
     * @param valueToWait Value to wait.
     */
    public CompletableFuture<R> waitFor(T valueToWait) {
        if (!busyLock.enterBusy()) {
            return failedFuture(new TrackerClosedException());
        }

        try {
            if (current.getKey().compareTo(valueToWait) >= 0) {
                return completedFuture(current.getValue());
            }

            LOG.warn("'waitFor' returned incomplete future.");

            return addNewWaiter(valueToWait);
        } finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * Returns current internal value.
     *
     * @throws TrackerClosedException if the tracker is closed.
     */
    public T current() {
        if (!busyLock.enterBusy()) {
            throw new TrackerClosedException();
        }

        try {
            return current.getKey();
        } finally {
            busyLock.leaveBusy();
        }
    }

    @Override
    public void close() {
        if (!CLOSE_GUARD.compareAndSet(this, false, true)) {
            return;
        }

        busyLock.block();

        TrackerClosedException trackerClosedException = new TrackerClosedException();

        cleanupWaitersOnClose(trackerClosedException);
    }

    protected void completeWaitersOnUpdate(T newValue, @Nullable R futureResult) {
        ConcurrentNavigableMap<T, CompletableFuture<R>> smallerFutures = valueFutures.headMap(newValue, true);

        smallerFutures.forEach((k, f) -> f.complete(futureResult));

        smallerFutures.clear();
    }

    protected CompletableFuture<R> addNewWaiter(T valueToWait) {
        CompletableFuture<R> future = valueFutures.computeIfAbsent(valueToWait, k -> new CompletableFuture<>());

        Map.Entry<T, R> currentEntry = current;

        if (currentEntry.getKey().compareTo(valueToWait) >= 0) {
            future.complete(currentEntry.getValue());

            valueFutures.remove(valueToWait);
        }

        return future;
    }

    protected void cleanupWaitersOnClose(TrackerClosedException trackerClosedException) {
        valueFutures.values().forEach(future -> future.completeExceptionally(trackerClosedException));

        valueFutures.clear();
    }

    Map.Entry<T, R> currentEntry() {
        return current;
    }

    /** Returns true if this tracker contains no waiters. */
    public boolean isEmpty() {
        return valueFutures.isEmpty();
    }
}
