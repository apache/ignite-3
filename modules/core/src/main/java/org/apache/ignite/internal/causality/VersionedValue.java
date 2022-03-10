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

import java.util.Map.Entry;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.ignite.lang.IgniteStringFormatter;
import org.jetbrains.annotations.Nullable;

/**
 * Parametrized type to store several versions of the value.
 * A value can be available through the causality token, which is represented by long.
 *
 * @param <T> Type of real value.
 */
public class VersionedValue<T> {
    /** Last applied causality token. */
    private volatile long actualToken = -1L;

    /** Size of stored history. */
    private final int historySize;

    /** Closure applied on storage revision update. */
    private final BiConsumer<VersionedValue<T>, Long> storageRevisionUpdating;

    /** Versioned value storage. */
    private final ConcurrentNavigableMap<Long, CompletableFuture<T>> history = new ConcurrentSkipListMap<>();

    /**
     * This lock guarantees that the history is not trimming {@link #trimToSize(long)} during getting a value from versioned storage {@link
     * #get(long)}.
     */
    private final ReadWriteLock trimHistoryLock = new ReentrantReadWriteLock();

    /** Temporary value for {@link #update(long, Function, Function)}. */
    private volatile T tempValue = null;

    /** Whether {@link #update(long, Function, Function)} was called since the last revision update. */
    private volatile boolean isUpdating = false;

    /** Update mutex. */
    private final Object updateMutex = new Object();

    /**
     * Constructor.
     *
     * @param storageRevisionUpdating    Closure applied on storage revision update (see {@link #onStorageRevisionUpdate(long)}).
     * @param observableRevisionUpdater  A closure intended to connect this VersionedValue with a revision updater, that this VersionedValue
     *                                   should be able to listen to, for receiving storage revision updates. This closure is called once on
     *                                   a construction of this VersionedValue and accepts a {@code Consumer<Long>} that should be called
     *                                   on every update of storage revision as a listener. IMPORTANT: Revision update shouldn't happen
     *                                   concurrently with {@link #set(long, T)} operations.
     * @param historySize                Size of the history of changes to store, including last applied token.
     * @param defaultVal                 Supplier of the default value, that is used on {@link #update(long, Function, Function)} to
     *                                   evaluate the default value if the value is not initialized yet.
     */
    public VersionedValue(
            @Nullable BiConsumer<VersionedValue<T>, Long> storageRevisionUpdating,
            Consumer<Consumer<Long>> observableRevisionUpdater,
            int historySize,
            Supplier<T> defaultVal
    ) {
        this.storageRevisionUpdating = storageRevisionUpdating;

        this.historySize = historySize;

        //TODO: IGNITE-16553 Added a possibility to set any start value (not only null).
        history.put(actualToken, completedFuture(defaultVal == null ? null : defaultVal.get()));

        observableRevisionUpdater.accept(this::onStorageRevisionUpdate);
    }

    /**
     * Constructor with default history size that equals 2. See {@link #VersionedValue(BiConsumer, Consumer, int, Supplier)}.
     *
     * @param storageRevisionUpdating   Closure applied on storage revision update (see {@link #onStorageRevisionUpdate(long)}.
     * @param observableRevisionUpdater A closure intended to connect this VersionedValue with a revision updater, that this VersionedValue
     *                                  should be able to listen to, for receiving storage revision updates. This closure is called once on
     *                                  a construction of this VersionedValue and accepts a {@code Consumer<Long>} that should be called
     *                                  on every update of storage revision as a listener. IMPORTANT: Revision update shouldn't happen
     *                                  concurrently with {@link #set(long, T)} operations.
     */
    public VersionedValue(
            @Nullable BiConsumer<VersionedValue<T>, Long> storageRevisionUpdating,
            Consumer<Consumer<Long>> observableRevisionUpdater
    ) {
        this(storageRevisionUpdating, observableRevisionUpdater, 2, null);
    }

    /**
     * Constructor with default history size that equals 2 and no closure. See {@link #VersionedValue(BiConsumer, Consumer, int, Supplier)}.
     *
     * @param observableRevisionUpdater A closure intended to connect this VersionedValue with a revision updater, that this VersionedValue
     *                                  should be able to listen to, for receiving storage revision updates. This closure is called once on
     *                                  a construction of this VersionedValue and accepts a {@code Consumer<Long>} that should be called
     *                                  on every update of storage revision as a listener. IMPORTANT: Revision update shouldn't happen
     *                                  concurrently with {@link #set(long, T)} operations.
     */
    public VersionedValue(Consumer<Consumer<Long>> observableRevisionUpdater) {
        this(null, observableRevisionUpdater);
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
        if (causalityToken <= actualToken) {
            return getValueForPreviousToken(causalityToken);
        }

        trimHistoryLock.readLock().lock();

        try {
            if (causalityToken <= actualToken) {
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
        synchronized (updateMutex) {
            for (CompletableFuture<T> fut : history.descendingMap().values()) {
                if (fut.isDone()) {
                    return fut.join();
                }
            }

            throw new AssertionError("History should never be empty.");
        }
    }

    /**
     * Gets a value for less or equal token than the actual {@link #actualToken}.
     *
     * @param causalityToken Causality token.
     * @return A completed future that contained a value.
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
     * @param value          Current value.
     */
    public void set(long causalityToken, T value) {
        long actualToken0 = actualToken;

        assert actualToken0 + 1 == causalityToken : IgniteStringFormatter.format("Token must be greater than actual by exactly 1 "
                + "[token={}, actual={}]", causalityToken, actualToken0);

        setValueInternal(causalityToken, value);
    }

    /**
     * Comparisons an exception to the causality token.
     *
     * @param causalityToken Causality token.
     * @param throwable An exception.
     */
    public void fail(long causalityToken, Throwable throwable) {
        long actualToken0 = actualToken;

        assert actualToken0 + 1 == causalityToken : IgniteStringFormatter.format("Token must be greater than actual by exactly 1 "
                + "[token={}, actual={}]", causalityToken, actualToken0);

        failInternal(causalityToken, throwable);
    }

    /**
     * Updates the value using the given updater. The updater receives the value on previous token, or default value
     * (see constructor) if the value isn't initialized, or current intermediate value, if this method has been already
     * called for the same token; and returns a new value.<br>
     * If an exception ({@link CancellationException} or {@link CompletionException}) was thrown when calculating the value for previous
     * token, then {@code fail} updater is used to process the exception and calculate a new value.<br>
     * This method can be called multiple times for the same token, and doesn't complete the future created for this token.
     * The future is supposed to be completed by storage revision update in this case. If this method has been called at least
     * once on the given token, the updater will receive a value that was evaluated by updater on previous call, as intermediate
     * result.<br>
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
     * The method should return previous value (previous intermediate value, or a value for previous token, if this method
     * is called for first time for given token).
     *
     * @param causalityToken Causality token.
     * @param complete       The function is invoked if the previous future completed successfully.
     * @param fail           The function is invoked if the previous future completed with an exception.
     * @return               Updated value.
     */
    public T update(long causalityToken, Function<T, T> complete, Function<Throwable, T> fail) {
        long actualToken0 = actualToken;

        assert actualToken0 + 1 == causalityToken : IgniteStringFormatter.format("Token must be greater than actual by exactly 1 "
                + "[token={}, actual={}]", causalityToken, actualToken0);

        Entry<Long, CompletableFuture<T>> histEntry = history.floorEntry(actualToken0);

        CompletableFuture<T> previousFuture = histEntry.getValue();

        assert previousFuture.isDone() : "Previous value should be ready.";

        try {
            synchronized (updateMutex) {
                T previousValue = isUpdating ? tempValue : previousFuture.join();

                isUpdating = true;

                T res = complete.apply(previousValue);

                tempValue = res;

                return res;
            }
        } catch (CancellationException | CompletionException e) {
            synchronized (updateMutex) {
                isUpdating = true;

                T res = fail.apply(e);

                tempValue = res;

                return res;
            }
        }
    }

    /**
     * This internal method assigns value according to specific token without additional checks.
     *
     * @param causalityToken Causality token.
     * @param value          Value to set.
     */
    private void setValueInternal(long causalityToken, T value) {
        CompletableFuture<T> res = history.putIfAbsent(causalityToken, CompletableFuture.completedFuture(value));

        if (res == null || res.isCompletedExceptionally()) {
            return;
        }

        assert !res.isDone() : IgniteStringFormatter.format("Different values associated with the token "
            + "[token={}, value={}, prevValue={}]", causalityToken, value, res.join());

        res.complete(value);
    }

    /**
     * Fails a future associated with this causality token.
     *
     * @param causalityToken Causality token.
     * @param throwable      An exception.
     */
    private void failInternal(long causalityToken, Throwable throwable) {
        CompletableFuture<T> res = history.putIfAbsent(causalityToken, CompletableFuture.failedFuture(throwable));

        if (res == null || res.isCompletedExceptionally()) {
            return;
        }

        assert !res.isDone() : IgniteStringFormatter.format("A value already has associated with the token "
                + "[token={}, ex={}, value={}]", causalityToken, throwable, res.join());

        res.completeExceptionally(throwable);
    }

    /**
     * Should be called on a storage revision update. This also triggers completion of a future created for the given causality token. It
     * implies that all possible updates associated with this token have been already applied to the component.
     * <br>
     * This method should not be called concurrently with {@link #update(long, Function, Function)} and {@link #set(long, Object)}
     * methods, as the storage revision update listener is supposed to be called after all other configuration listeners.
     *
     * @param causalityToken Causality token.
     */
    private void onStorageRevisionUpdate(long causalityToken) {
        long actualToken0 = actualToken;

        assert causalityToken > actualToken0 : IgniteStringFormatter.format(
                "New token should be greater than current [current={}, new={}]", actualToken0, causalityToken);

        if (isUpdating) {
            setValueInternal(causalityToken, tempValue);

            isUpdating = false;
        }

        if (storageRevisionUpdating != null) {
            storageRevisionUpdating.accept(this, causalityToken);
        }

        completeRelatedFuture(causalityToken);

        if (history.size() > 1 && causalityToken - history.firstKey() >= historySize) {
            trimToSize(causalityToken);
        }

        Entry<Long, CompletableFuture<T>> entry = history.floorEntry(causalityToken);

        assert entry != null && entry.getValue().isDone() : IgniteStringFormatter.format(
                "Future for the token is not completed [token={}]", causalityToken);

        actualToken = causalityToken;
    }

    /**
     * Completes a future related with a specific causality token.
     *
     * @param causalityToken The token which is becoming an actual.
     */
    private void completeRelatedFuture(long causalityToken) {
        Entry<Long, CompletableFuture<T>> entry = history.floorEntry(causalityToken);

        CompletableFuture<T> future = entry.getValue();

        if (!future.isDone()) {
            Entry<Long, CompletableFuture<T>> entryBefore = history.headMap(causalityToken).lastEntry();

            assert entryBefore != null && entryBefore.getValue().isDone() : IgniteStringFormatter.format(
                "No future for token [token={}]", causalityToken);

            CompletableFuture<T> f =  entryBefore.getValue();

            f.whenComplete((t, throwable) -> {
                if (throwable != null) {
                    future.completeExceptionally(throwable);
                } else {
                    future.complete(t);
                }
            });
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
}
