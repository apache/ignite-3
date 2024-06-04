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
import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;
import static org.apache.ignite.internal.tracing.TracingManager.wrap;
import static org.apache.ignite.internal.util.CompletableFutures.copyStateTo;

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
 * Base implementation of a {@code VersionedValue} intended to be used by other implementations.
 */
class BaseVersionedValue<T> implements VersionedValue<T> {
    private final IgniteLogger log = Loggers.forClass(BaseVersionedValue.class);

    /** Token until the value is initialized. */
    private static final long NOT_INITIALIZED = -1L;

    /** Default history size. */
    private static final int DEFAULT_MAX_HISTORY_SIZE = 10;

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

    BaseVersionedValue(@Nullable Supplier<T> defaultValueSupplier) {
        this(DEFAULT_MAX_HISTORY_SIZE, defaultValueSupplier);
    }

    BaseVersionedValue(int maxHistorySize, @Nullable Supplier<T> defaultValueSupplier) {
        this.maxHistorySize = maxHistorySize;
        this.defaultValue = defaultValueSupplier == null ? null : new Lazy<>(defaultValueSupplier);
    }

    @Override
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

            return wrap(histEntry.getValue());
        } finally {
            readWriteLock.readLock().unlock();
        }
    }

    @Override
    public @Nullable T latest() {
        for (CompletableFuture<T> fut : history.descendingMap().values()) {
            if (fut.isDone()) {
                return fut.join();
            }
        }

        return getDefault();
    }

    @Override
    public long latestCausalityToken() {
        for (Entry<Long, CompletableFuture<T>> entry : history.descendingMap().entrySet()) {
            if (entry.getValue().isDone()) {
                return entry.getKey();
            }
        }

        return NOT_INITIALIZED;
    }

    /**
     * Returns the default value.
     */
    @Nullable T getDefault() {
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
    void complete(long causalityToken) {
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
    void complete(long causalityToken, CompletableFuture<T> future) {
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
     * Looks for the first complete future that comes before the {@link #actualToken} and completes all futures in the range
     * {@code (previousCompleteToken, actualToken)} with the value of this this future.
     *
     * <p>Must be called under the {@link #readWriteLock writeLock}.
     *
     * @return First complete future that comes before {@code actualToken} or {@code null} if no such future exists.
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

        from.whenComplete(copyStateTo(to));
    }

    @Override
    public void whenComplete(CompletionListener<T> action) {
        completionListeners.add(action);
    }

    @Override
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
