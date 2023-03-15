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

package org.apache.ignite.internal.causality;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.apache.ignite.lang.IgniteStringFormatter.format;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Supplier;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.util.Lazy;
import org.jetbrains.annotations.Nullable;

/**
 * Parametrized type to store several versions of a value.
 *
 * <p>The value can be available through the causality token, which is represented by a {@code long}.
 *
 * @param <T> Type of real value.
 */
public class AbstractVersionedValue<T> {
    private final IgniteLogger log = Loggers.forClass(AbstractVersionedValue.class);

    /** Token until the value is initialized. */
    private static final long NOT_INITIALIZED = -1L;

    /** Default history size. */
    public static final int DEFAULT_MAX_HISTORY_SIZE = 10;

    /** Size of the history of changes to store, including last applied token. */
    private final int maxHistorySize;

    /** List of completion listeners, see {@link #whenComplete}. */
    private final List<CompletionListener<T>> completionListeners = new CopyOnWriteArrayList<>();

    /** Versioned value storage. */
    private final ConcurrentNavigableMap<Long, CompletableFuture<T>> history = new ConcurrentSkipListMap<>();

    /** Default value that is used when a Versioned Value is completed without an explicit value. */
    @Nullable
    private final Lazy<T> defaultValue;

    /**
     * Last applied causality token.
     *
     * <p>Multi-threaded access is guarded by the {@link #readWriteLock}.
     */
    private long actualToken = NOT_INITIALIZED;

    private final ReadWriteLock readWriteLock = new ReentrantReadWriteLock();

    protected AbstractVersionedValue(@Nullable Supplier<T> defaultValueSupplier) {
        this(DEFAULT_MAX_HISTORY_SIZE, defaultValueSupplier);
    }

    protected AbstractVersionedValue(int maxHistorySize, @Nullable Supplier<T> defaultValueSupplier) {
        this.maxHistorySize = maxHistorySize;
        this.defaultValue = defaultValueSupplier == null ? null : new Lazy<>(defaultValueSupplier);
    }

    /**
     * Creates a future for this value and causality token, or returns it if it already exists.
     *
     * <p>The returned future is associated with an update having the given causality token and completes when this update is finished.
     *
     * @param causalityToken Causality token. Let's assume that the update associated with token N is already applied to this value.
     *         Then, if token N is given as an argument, a completed future will be returned. If token N - 1 is given, this method returns
     *         the result in the state that is actual for the given token. If the token is strongly outdated, {@link OutdatedTokenException}
     *         is thrown. If token N + 1 is given, this method will return a future that will be completed when the update associated with
     *         token N + 1 will have been applied. Tokens that greater than N by more than 1 should never be passed.
     * @return The future.
     * @throws OutdatedTokenException If outdated token is passed as an argument.
     */
    public CompletableFuture<T> get(long causalityToken) {
        assert causalityToken > NOT_INITIALIZED;

        readWriteLock.readLock().lock();

        try {
            if (causalityToken > actualToken) {
                return history.computeIfAbsent(causalityToken, t -> new CompletableFuture<>());
            }

            Entry<Long, CompletableFuture<T>> histEntry = history.floorEntry(causalityToken);

            if (histEntry == null) {
                throw new OutdatedTokenException(causalityToken, actualToken, maxHistorySize);
            }

            return histEntry.getValue();
        } finally {
            readWriteLock.readLock().unlock();
        }
    }

    /**
     * Gets the latest value of completed future.
     */
    public @Nullable T latest() {
        for (CompletableFuture<T> fut : history.descendingMap().values()) {
            if (fut.isDone()) {
                return fut.join();
            }
        }

        return getDefault();
    }

    /**
     * Returns the default value.
     */
    protected final @Nullable T getDefault() {
        return defaultValue == null ? null : defaultValue.get();
    }

    /**
     * Completes the Versioned Value for the given token. This method will look for the previous complete token and complete all registered
     * futures in the {@code (prevToken, causalityToken]} range. If no {@code complete} methods have been called before, all these futures
     * will be complete with the configured default value.
     *
     * <p>Calling this method will trigger the {@link #whenComplete} listeners for the given token.
     *
     * @param causalityToken Causality token.
     */
    public void complete(long causalityToken) {
        CompletableFuture<T> futureForToken;

        readWriteLock.writeLock().lock();

        try {
            setActualToken(causalityToken);

            CompletableFuture<T> previousCompleteFuture = completePreviousFutures();

            futureForToken = history.compute(causalityToken, (token, future) -> {
                if (future == null) {
                    // No registered future for this token exists, we can reuse the previous complete future.
                    return previousCompleteFuture == null ? completedFuture(getDefault()) : previousCompleteFuture;
                } else {
                    if (previousCompleteFuture == null) {
                        future.complete(getDefault());
                    } else {
                        copyState(previousCompleteFuture, future);
                    }

                    return future;
                }
            });

            // Delete older tokens if their amount exceeds the configured history size.
            trimHistory();
        } finally {
            readWriteLock.writeLock().unlock();
        }

        notifyCompletionListeners(causalityToken, futureForToken);
    }

    /**
     * Completes the Versioned Value for the given token with the given {@code future}. This method will look for the previous complete
     * token and complete all registered futures in the {@code (prevToken, causalityToken)} range. If no {@code complete} methods have
     * been called before, all these futures will be complete with the configured default value.
     *
     * <p>Calling this method will trigger the {@link #whenComplete} listeners for the given token.
     */
    protected void complete(long causalityToken, CompletableFuture<T> future) {
        assert future.isDone();

        readWriteLock.writeLock().lock();

        try {
            setActualToken(causalityToken);

            completePreviousFutures();

            CompletableFuture<T> existingFuture = history.putIfAbsent(causalityToken, future);

            if (existingFuture != null) {
                copyState(future, existingFuture);
            }

            // Delete older tokens if their amount exceeds the configured history size.
            trimHistory();
        } finally {
            readWriteLock.writeLock().unlock();
        }

        notifyCompletionListeners(causalityToken, future);
    }

    /**
     * Completes a future related with a specific causality token. It is called only on storage revision update.
     */
    @Nullable
    private CompletableFuture<T> completePreviousFutures() {
        NavigableMap<Long, CompletableFuture<T>> headMap = history.headMap(actualToken);

        if (headMap.isEmpty()) {
            return null;
        }

        List<CompletableFuture<T>> futuresToComplete = List.of();

        CompletableFuture<T> previousCompleteFuture = null;

        for (CompletableFuture<T> future : headMap.descendingMap().values()) {
            if (future.isDone()) {
                previousCompleteFuture = future;

                break;
            }

            // Lazy initialization.
            if (futuresToComplete.isEmpty()) {
                futuresToComplete = new ArrayList<>();
            }

            futuresToComplete.add(future);
        }

        // We found the first complete future, but there are no incomplete futures up to the actual token, so there's nothing to do.
        if (futuresToComplete.isEmpty()) {
            return previousCompleteFuture;
        }

        if (previousCompleteFuture == null) {
            // "complete" method has never been called before, use the default value as the original.
            T defaultValue = getDefault();

            futuresToComplete.forEach(f -> f.complete(defaultValue));
        } else {
            assert previousCompleteFuture.isDone();

            // Create an effectively final variable.
            List<CompletableFuture<T>> futuresToCompleteCopy = futuresToComplete;

            previousCompleteFuture.whenComplete((v, t) -> {
                if (t != null) {
                    futuresToCompleteCopy.forEach(f -> f.completeExceptionally(t));
                } else {
                    futuresToCompleteCopy.forEach(f -> f.complete(v));
                }
            });
        }

        return previousCompleteFuture;
    }

    /**
     * Updates the current token. Must be called under the {@link #readWriteLock writeLock}.
     */
    private void setActualToken(long causalityToken) {
        assert actualToken < causalityToken
                : format("Token must be greater than actual [token={}, actual={}]", causalityToken, actualToken);

        actualToken = causalityToken;
    }

    /**
     * Trims the storage to history size. Must be called under the {@link #readWriteLock writeLock}.
     */
    private void trimHistory() {
        NavigableMap<Long, CompletableFuture<T>> oldTokensMap = history.headMap(actualToken, true);

        if (oldTokensMap.size() <= maxHistorySize) {
            return;
        }

        Iterator<Map.Entry<Long, CompletableFuture<T>>> it = oldTokensMap.entrySet().iterator();

        for (int i = oldTokensMap.size(); i > maxHistorySize; i--) {
            Map.Entry<Long, CompletableFuture<T>> next = it.next();

            // All futures must be explicitly completed before history trimming occurs.
            assert next.getValue().isDone();

            it.remove();
        }
    }

    /**
     * Copies the state of the {@code from} future to the incomplete {@code to} future.
     */
    private void copyState(CompletableFuture<T> from, CompletableFuture<T> to) {
        assert from.isDone();
        assert !to.isDone();

        from.whenComplete((v, t) -> {
            if (t != null) {
                to.completeExceptionally(t);
            } else {
                to.complete(v);
            }
        });
    }

    /**
     * Add listener for completions of this versioned value on every token.
     *
     * @param action Action to perform.
     */
    public void whenComplete(CompletionListener<T> action) {
        completionListeners.add(action);
    }

    /**
     * Removes a completion listener, see {@link #whenComplete}.
     *
     * @param action Action to remove.
     */
    public void removeWhenComplete(CompletionListener<T> action) {
        completionListeners.remove(action);
    }

    /**
     * Notifies completion listeners.
     */
    private void notifyCompletionListeners(long causalityToken, CompletableFuture<T> future) {
        future.whenComplete((v, t) -> {
            Throwable unpackedThrowable = t instanceof CompletionException ? t.getCause() : t;

            for (CompletionListener<T> listener : completionListeners) {
                try {
                    listener.whenComplete(causalityToken, v, unpackedThrowable);
                } catch (Exception e) {
                    log.error("Exception when notifying a completion listener", e);
                }
            }
        });
    }
}
