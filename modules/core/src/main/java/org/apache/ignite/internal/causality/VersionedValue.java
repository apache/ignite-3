/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.causality;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.CompletableFuture.failedFuture;

import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.ignite.lang.IgniteInternalException;
import org.apache.ignite.lang.IgniteStringFormatter;
import org.apache.ignite.lang.IgniteTriConsumer;

/**
 * Parametrized type to store several versions of the value.
 * A value can be available through the causality token, which is represented by long.
 *
 * @param <T> Type of real value.
 */
public class VersionedValue<T> {
    /** Token until the value is initialized. */
    private static final long NOT_INITIALIZED = -1L;

    /** Default history size. */
    private static final int DEFAULT_HISTORY_SIZE = 2;

    /** Size of stored history. */
    private final int historySize;

    /** List of completion listeners, see {@link #whenComplete(IgniteTriConsumer)}. */
    private final List<IgniteTriConsumer<Long, T, Throwable>> completionListeners = new CopyOnWriteArrayList<>();

    /** Versioned value storage. */
    private final ConcurrentNavigableMap<Long, CompletableFuture<T>> history = new ConcurrentSkipListMap<>();

    /**
     * This lock guarantees that the history is not trimming {@link #trimToSize(long)} during getting a value from versioned storage {@link
     * #get(long)}.
     */
    private final ReadWriteLock trimHistoryLock = new ReentrantReadWriteLock();

    /** Initial future. The future will be completed when {@link VersionedValue} sets a first value. */
    private final CompletableFuture<T> initFut = new CompletableFuture<>();

    /** The supplier may provide a value which will used as a default. */
    private final Supplier<T> defaultValSupplier;

    /** Update mutex. */
    private final Object updateMutex = new Object();

    /** Value that can be used as default. */
    private final AtomicReference<T> defaultValRef;

    /** Last applied causality token. */
    private volatile long actualToken = NOT_INITIALIZED;

    /**
     * Future that will be completed after all updates over the value in context of current causality token will be performed.
     * This {@code updaterFuture} is {@code null} if no updates in context of current causality token have been initiated.
     * See {@link #update(long, BiFunction)}.
     */
    private volatile CompletableFuture<T> updaterFuture = null;

    /**
     * Constructor.
     *
     * @param observableRevisionUpdater     A closure intended to connect this VersionedValue with a revision updater, that this
     *                                      VersionedValue should be able to listen to, for receiving storage revision updates.
     *                                      This closure is called once on a construction of this VersionedValue and accepts a
     *                                      {@code Function<Long, CompletableFuture<?>>} that should be called on every update of
     *                                      storage revision as a listener. IMPORTANT: Revision update shouldn't happen
     *                                      concurrently with {@link #complete(long, T)} operations.
     * @param historySize                   Size of the history of changes to store, including last applied token.
     * @param defaultVal                    Supplier of the default value, that is used on {@link #update(long, BiFunction)} to
     *                                      evaluate the default value if the value is not initialized yet. It is not guaranteed to
     *                                      execute only once.
     */
    public VersionedValue(
            Consumer<Function<Long, CompletableFuture<?>>> observableRevisionUpdater,
            int historySize,
            Supplier<T> defaultVal
    ) {
        this.historySize = historySize;

        this.defaultValSupplier = defaultVal;

        this.defaultValRef = defaultValSupplier == null ? null : new AtomicReference<>();

        if (observableRevisionUpdater != null) {
            observableRevisionUpdater.accept(this::completeOnRevision);
        }
    }

    /**
     * Constructor.
     *
     * @param observableRevisionUpdater     A closure intended to connect this VersionedValue with a revision updater, that this
     *                                      VersionedValue should be able to listen to, for receiving storage revision updates.
     *                                      This closure is called once on a construction of this VersionedValue and accepts a
     *                                      {@code Function<Long, CompletableFuture<?>>} that should be called on every update of
     *                                      storage revision as a listener. IMPORTANT: Revision update shouldn't happen
     *                                      concurrently with {@link #complete(long, T)} operations.
     * @param defaultVal                    Supplier of the default value, that is used on {@link #update(long, BiFunction)} to
     *                                      evaluate the default value if the value is not initialized yet. It is not guaranteed to
     *                                      execute only once.
     */
    public VersionedValue(
            Consumer<Function<Long, CompletableFuture<?>>> observableRevisionUpdater,
            Supplier<T> defaultVal
    ) {
        this(observableRevisionUpdater, DEFAULT_HISTORY_SIZE, defaultVal);
    }

    /**
     * Constructor with default history size that equals 2. See {@link #VersionedValue(Consumer, int, Supplier)}.
     *
     * @param observableRevisionUpdater     A closure intended to connect this VersionedValue with a revision updater, that this
     *                                      VersionedValue should be able to listen to, for receiving storage revision updates.
     *                                      This closure is called once on a construction of this VersionedValue and accepts a
     *                                      {@code Function<Long, CompletableFuture<?>>} that should be called on every update of
     *                                      storage revision as a listener. IMPORTANT: Revision update shouldn't happen
     *                                      concurrently with {@link #complete(long, T)} operations.
     */
    public VersionedValue(Consumer<Function<Long, CompletableFuture<?>>> observableRevisionUpdater) {
        this(observableRevisionUpdater, DEFAULT_HISTORY_SIZE, null);
    }

    /**
     * Creates a future for this value and causality token, or returns it if it already exists.
     *
     * <p>The returned future is associated with an update having the given causality token and completes when this update is finished
     * applying.
     *
     * @param causalityToken Causality token. Let's assume that the update associated with token N is already applied to this value. Then,
     *                       if token N is given as an argument, a completed future will be returned. If token N - 1 is given, this method
     *                       returns the result in the state that is actual for the given token. If the token is strongly outdated, {@link
     *                       OutdatedTokenException} is thrown. If token N + 1 is given, this method will return a future that will be
     *                       completed when the update associated with token N + 1 will have been applied. Tokens that greater than N by
     *                       more than 1 should never be passed.
     * @return The future.
     * @throws OutdatedTokenException If outdated token is passed as an argument.
     */
    public CompletableFuture<T> get(long causalityToken) {
        if (initFut.isDone()) {
            return getInternal(causalityToken);
        }

        return initFut.thenCompose(o -> getInternal(causalityToken));
    }

    /**
     * Gets a future corresponding the token when the {@link VersionedValue} is already initiated.
     *
     * @param causalityToken Causality token.
     * @return The future.
     */
    private CompletableFuture<T> getInternal(long causalityToken) {
        long actualToken0 = this.actualToken;

        if (history.floorEntry(causalityToken) == null) {
            throw new OutdatedTokenException(causalityToken, actualToken0, historySize);
        }

        if (causalityToken <= actualToken0) {
            return getValueForPreviousToken(causalityToken);
        }

        trimHistoryLock.readLock().lock();

        try {
            if (causalityToken <= actualToken0) {
                return getValueForPreviousToken(causalityToken);
            }

            var fut = new CompletableFuture<T>();

            CompletableFuture<T> previousFut = history.putIfAbsent(causalityToken, fut);

            return previousFut == null ? fut : previousFut;
        } finally {
            trimHistoryLock.readLock().unlock();
        }
    }

    /**
     * Gets the latest value of completed future.
     */
    public T latest() {
        for (CompletableFuture<T> fut : history.descendingMap().values()) {
            if (fut.isDone()) {
                return fut.join();
            }
        }

        return getDefault();
    }

    /**
     * Creates (if needed) and returns a default value.
     *
     * @return The value.
     */
    private T getDefault() {
        if (defaultValSupplier != null && defaultValRef.get() == null) {
            T defaultVal = defaultValSupplier.get();

            assert defaultVal != null : "Default value can't be null.";

            defaultValRef.compareAndSet(null, defaultVal);
        }

        return defaultValRef == null ? null : defaultValRef.get();
    }

    /**
     * Gets a value for less or equal token than the actual {@link #actualToken}.
     *
     * @param causalityToken Causality token.
     * @return A completed future that contains a value.
     * @throws OutdatedTokenException If outdated token is passed as an argument.
     */
    private CompletableFuture<T> getValueForPreviousToken(long causalityToken) {
        Entry<Long, CompletableFuture<T>> histEntry = history.floorEntry(causalityToken);

        if (histEntry == null) {
            throw new OutdatedTokenException(causalityToken, actualToken, historySize);
        }

        return histEntry.getValue();
    }

    /**
     * Save the version of the value associated with the given causality token. If someone has got a future to await the value associated
     * with the given causality token (see {@link #get(long)}, then the future will be completed.
     *
     * @param causalityToken Causality token.
     */
    public void complete(long causalityToken) {
        completeOnRevision(causalityToken);
    }

    /**
     * Save the version of the value associated with the given causality token. If someone has got a future to await the value associated
     * with the given causality token (see {@link #get(long)}, then the future will be completed.
     *
     * @param causalityToken Causality token.
     * @param value          Current value.
     */
    public void complete(long causalityToken, T value) {
        long actualToken0 = actualToken;

        if (actualToken0 == NOT_INITIALIZED) {
            history.put(causalityToken, initFut);
        }

        checkToken(actualToken0, causalityToken);

        completeInternal(causalityToken, value, null);
    }

    /**
     * Save the exception associated with the given causality token. If someone has got a future to await the value associated
     * with the given causality token (see {@link #get(long)}, then the future will be completed.
     *
     * @param causalityToken Causality token.
     * @param throwable An exception.
     */
    public void completeExceptionally(long causalityToken, Throwable throwable) {
        long actualToken0 = actualToken;

        if (actualToken0 == NOT_INITIALIZED) {
            history.put(causalityToken, initFut);
        }

        checkToken(actualToken0, causalityToken);

        completeInternal(causalityToken, null, throwable);
    }

    /**
     * This internal method assigns either value or exception according to specific token.
     *
     * @param causalityToken Causality token.
     * @param value          Value to set.
     * @param throwable      An exception.
     */
    private void completeInternal(long causalityToken, T value, Throwable throwable) {
        CompletableFuture<T> res = history.putIfAbsent(
                causalityToken,
                throwable == null ? completedFuture(value) : failedFuture(throwable)
        );

        if (res == null) {
            notifyCompletionListeners(causalityToken, value, throwable);

            return;
        }

        assert !res.isDone() : completeInternalConflictErrorMessage(res, causalityToken, value, throwable);

        if (throwable == null) {
            res.complete(value);
        } else {
            res.completeExceptionally(throwable);
        }

        notifyCompletionListeners(causalityToken, value, throwable);
    }

    /**
     * Builds an error message for the case when there is a conflict between history and a value or exception that is going to be
     * saved.
     *
     * @param future Future.
     * @param token Token.
     * @param value Value.
     * @param throwable Throwable.
     * @return Error message.
     */
    private String completeInternalConflictErrorMessage(CompletableFuture<T> future, long token, T value, Throwable throwable) {
        return future.handle(
            (prevValue, prevThrowable) ->
                IgniteStringFormatter.format(
                    "Different values associated with the token [token={}, value={}, exception={}, prevValue={}, prevException={}]",
                    token,
                    value,
                    throwable,
                    prevValue,
                    prevThrowable
                )
        )
        .join();
    }

    /**
     * Updates the value using the given updater. The updater receives the value on previous token, or default value
     * (see constructor) if the value isn't initialized, or current intermediate value, if this method has been already
     * called for the same token; and returns a new value.<br>
     * The updater will be called after updaters that had been passed to previous calls of this method complete.
     * If an exception ({@link CancellationException} or {@link CompletionException}) was thrown when calculating the value for previous
     * token, then updater is used to process the exception and calculate a new value.<br>
     * This method can be called multiple times for the same token, and doesn't complete the future created for this token.
     * The future is supposed to be completed by storage revision update or a call of {@link #complete(long)} in this case.
     * If this method has been called at least once on the given token, the updater will receive a value that was evaluated
     * by updater on previous call, as intermediate result.<br>
     * As the order of multiple calls of this method on the same token is unknown, operations done by the updater must be
     * commutative. For example:
     * <ul>
     *     <li>this method was called for token N-1 and updater evaluated the value V1;</li>
     *     <li>a storage revision update happened;</li>
     *     <li>this method is called for token N, updater receives V1 and evaluates V2;</li>
     *     <li>this method is called once again for token N, then the updater receives V2 as intermediate result and evaluates V3;</li>
     *     <li>storage revision update happens and the future for token N completes with value V3.</li>
     * </ul>
     * Regardless of order in which this method's calls are made, V3 should be the final result.
     * <br>
     * The method should return a future that will be completed when {@code updater} completes.
     *
     * @param causalityToken Causality token.
     * @param updater        The binary function that accepts previous value and exception, if present, and update it to compute
     *                       the new value.
     * @return               Future for updated value.
     */
    public CompletableFuture<T> update(
            long causalityToken,
            BiFunction<T, Throwable, CompletableFuture<T>> updater
    ) {
        long actualToken0 = this.actualToken;

        checkToken(actualToken0, causalityToken);

        synchronized (updateMutex) {
            CompletableFuture<T> updaterFuture = this.updaterFuture;

            CompletableFuture<T> future = updaterFuture == null ? previousOrDefaultValueFuture(actualToken0) : updaterFuture;

            CompletableFuture<CompletableFuture<T>> f0 = future
                    .handle(updater::apply)
                    .handle((fut, e) -> e == null ? fut : failedFuture(e));

            updaterFuture = f0.thenCompose(Function.identity());

            this.updaterFuture = updaterFuture;

            return updaterFuture;
        }
    }

    /**
     * Add listener for completions of this versioned value on every token. It will be called on every {@link #complete(long)},
     * {@link #complete(long, Object)}, {@link #completeExceptionally(long, Throwable)} and also, if none of mentioned methods was
     * called explicitly, on storage revision update,
     *
     * @param action Action to perform.
     */
    public void whenComplete(IgniteTriConsumer<Long, T, Throwable> action) {
        completionListeners.add(action);
    }

    /**
     * Removes a completion listener, see {@link #whenComplete(IgniteTriConsumer)}.
     *
     * @param action Action to remove.
     */
    public void removeWhenComplete(IgniteTriConsumer<Long, T, Throwable> action) {
        completionListeners.remove(action);
    }

    /**
     * Notify completion listeners.
     *
     * @param causalityToken Token.
     * @param value Value.
     * @param throwable Throwable.
     */
    private void notifyCompletionListeners(long causalityToken, T value, Throwable throwable) {
        Throwable unpackedThrowable = throwable instanceof CompletionException ? throwable.getCause() : throwable;

        List<Exception> exceptions = new ArrayList<>();

        for (IgniteTriConsumer<Long, T, Throwable> listener : completionListeners) {
            try {
                listener.accept(causalityToken, value, unpackedThrowable);
            } catch (Exception e) {
                exceptions.add(e);
            }
        }

        if (!exceptions.isEmpty()) {
            IgniteInternalException ex = new IgniteInternalException();

            exceptions.forEach(ex::addSuppressed);

            throw ex;
        }
    }

    /**
     * Complete because of explicit token update. This also triggers completion of a future created for the given causality token.
     * This future completes after all updaters are complete (see {@link #update(long, BiFunction)}).
     *
     * @param causalityToken Token.
     * @return Future.
     */
    private CompletableFuture<?> completeOnRevision(long causalityToken) {
        long actualToken0 = actualToken;

        assert causalityToken > actualToken0 : IgniteStringFormatter.format(
                "New token should be greater than current [current={}, new={}]", actualToken0, causalityToken);

        if (actualToken0 == NOT_INITIALIZED) {
            history.put(causalityToken, initFut);
        }

        synchronized (updateMutex) {
            CompletableFuture<T> updaterFuture0 = updaterFuture;

            CompletableFuture<?> completeUpdatesFuture = updaterFuture0 == null
                    ? completedFuture(null)
                    : updaterFuture0.whenComplete((v, t) -> completeInternal(causalityToken, v, t));

            updaterFuture = null;

            actualToken = causalityToken;

            return completeUpdatesFuture.thenRun(() -> {
                completeRelatedFuture(causalityToken);

                if (history.size() > 1 && causalityToken - history.firstKey() >= historySize) {
                    trimToSize(causalityToken);
                }

                Entry<Long, CompletableFuture<T>> entry = history.floorEntry(causalityToken);

                assert entry != null && entry.getValue().isDone() : IgniteStringFormatter.format(
                    "Future for the token is not completed [token={}]", causalityToken);
            });
        }
    }

    /**
     * Completes a future related with a specific causality token. It is called only on storage revision update.
     *
     * @param causalityToken The token which is becoming an actual.
     */
    private void completeRelatedFuture(long causalityToken) {
        Entry<Long, CompletableFuture<T>> entry = history.floorEntry(causalityToken);

        CompletableFuture<T> future = entry.getValue();

        if (!future.isDone()) {
            Entry<Long, CompletableFuture<T>> entryBefore = history.headMap(causalityToken).lastEntry();

            CompletableFuture<T> previousFuture = entryBefore == null ? completedFuture(getDefault()) : entryBefore.getValue();

            assert previousFuture.isDone() : IgniteStringFormatter.format("No future for token [token={}]", causalityToken);

            previousFuture.whenComplete((t, throwable) -> {
                if (throwable != null) {
                    future.completeExceptionally(throwable);

                    notifyCompletionListeners(causalityToken, null, throwable);
                } else {
                    future.complete(t);

                    notifyCompletionListeners(causalityToken, t, null);
                }
            });
        } else if (entry.getKey() < causalityToken) {
            // Notifying listeners when there were neither updates nor explicit completions.
            // This future is previous, it is always done.
            future.whenComplete((v, e) -> notifyCompletionListeners(causalityToken, v, e));
        }
    }

    /**
     * Return a future for previous or default value.
     *
     * @param actualToken Token.
     * @return Future.
     */
    private CompletableFuture<T> previousOrDefaultValueFuture(long actualToken) {
        Entry<Long, CompletableFuture<T>> histEntry = history.floorEntry(actualToken);

        if (histEntry == null) {
            return completedFuture(getDefault());
        } else {
            CompletableFuture<T> prevFuture = histEntry.getValue();

            assert prevFuture.isDone() : "Previous value should be ready.";

            return prevFuture;
        }
    }

    /**
     * Trims the storage to history size.
     *
     * @param causalityToken Last token which is being applied.
     */
    private void trimToSize(long causalityToken) {
        Long lastToken = history.lastKey();

        trimHistoryLock.writeLock().lock();

        try {
            for (Long token : history.keySet()) {
                if (token != lastToken && causalityToken - token >= historySize) {
                    history.remove(token);
                }
            }
        } finally {
            trimHistoryLock.writeLock().unlock();
        }
    }

    /**
     * Check that the given causality token os correct according to the actual token.
     *
     * @param actualToken Actual token.
     * @param causalityToken Causality token.
     */
    private static void checkToken(long actualToken, long causalityToken) {
        assert actualToken == NOT_INITIALIZED || actualToken + 1 == causalityToken : IgniteStringFormatter.format(
            "Token must be greater than actual by exactly 1 [token={}, actual={}]", causalityToken, actualToken);
    }
}
