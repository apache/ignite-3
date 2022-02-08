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

import java.util.Map.Entry;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import org.apache.ignite.lang.IgniteStringFormatter;
import org.jetbrains.annotations.Nullable;

/**
 * Parametrized type to store several versions of the value.
 * A value can be available through the causality token, which is represented by long.
 *
 * @param <T> Type of real value.
 */
public class VersionedValue<T> {
    /** Last applied casualty token. */
    private volatile long actualToken;

    /** Size of stored history. */
    private final int historySize;

    /** Closure applied on storage revision update. */
    private final BiConsumer<VersionedValue<T>, Long> onStorageRevisionUpdate;

    /** Bersoned value storage. */
    private final ConcurrentNavigableMap<Long, CompletableFuture<T>> history = new ConcurrentSkipListMap<>();

    /**
     * Constructor.
     *
     * @param onStorageRevisionUpdate   Closure applied on storage revision update (see {@link #onStorageRevisionUpdate(long)}.
     * @param observableRevisionUpdater A closure intended to connect this VersionedValue with a revision updater, that this VersionedValue
     *                                  should be able to listen to, for receiving storage revision updates. This closure is called once on
     *                                  a construction of this VersionedValue and accepts a {@link Consumer<Long>} that should be called on
     *                                  every update of storage revision as a listener.
     * @param historySize               Size of the history of changes to store, including last applied token.
     */
    public VersionedValue(
            @Nullable BiConsumer<VersionedValue<T>, Long> onStorageRevisionUpdate,
            Consumer<Consumer<Long>> observableRevisionUpdater,
            int historySize
    ) {
        this.onStorageRevisionUpdate = onStorageRevisionUpdate == null ? (versionedValue, token) -> {
            Entry<Long, CompletableFuture<T>> entry = history.floorEntry(token);

            assert entry != null : IgniteStringFormatter.format("No future by token [token={}]", token);

            if (!entry.getValue().isDone()) {
                Entry<Long, CompletableFuture<T>> entryBefore = history.headMap(token).lastEntry();

                assert entryBefore != null && entryBefore.getValue().isDone() : IgniteStringFormatter.format(
                        "No future by token [token={}]", token);

                entryBefore.getValue().whenComplete((t, throwable) -> {
                    if (throwable != null) {
                        entry.getValue().completeExceptionally(throwable);
                    } else {
                        entry.getValue().complete(t);
                    }
                });
            }
        } : onStorageRevisionUpdate;

        observableRevisionUpdater.accept(this::onStorageRevisionUpdate);

        this.historySize = historySize;
    }

    /**
     * Constructor with default history size that equals 2. See {@link #VersionedValue(BiConsumer, int)}.
     *
     * @param onStorageRevisionUpdate   Closure applied on storage revision update (see {@link #onStorageRevisionUpdate(long)}.
     * @param observableRevisionUpdater A closure intended to connect this VersionedValue with a revision updater, that this VersionedValue
     *                                  should be able to listen to, for receiving storage revision updates. This closure is called once on
     *                                  a construction of this VersionedValue and accepts a {@link Consumer<Long>} that should be called on
     *                                  every update of storage revision as a listener.
     */
    public VersionedValue(
            @Nullable BiConsumer<VersionedValue<T>, Long> onStorageRevisionUpdate,
            Consumer<Consumer<Long>> observableRevisionUpdater
    ) {
        this(null, observableRevisionUpdater, 2);
    }

    /**
     * Constructor with default history size that equals 2 and no closure.
     *
     * @param observableRevisionUpdater A closure intended to connect this VersionedValue with a revision updater, that this VersionedValue
     *                                  should be able to listen to, for receiving storage revision updates. This closure is called once on
     *                                  a construction of this VersionedValue and accepts a {@link Consumer<Long>} that should be called on
     *                                  every update of storage revision as a listener.
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
    public CompletableFuture<T> get(long causalityToken) throws OutdatedTokenException {
        long actualToken0 = actualToken;

        if (causalityToken <= actualToken0) {
            Entry<Long, CompletableFuture<T>> histEntry = history.floorEntry(causalityToken);

            if (histEntry == null) {
                throw new OutdatedTokenException(causalityToken, actualToken0, historySize);
            }

            return histEntry.getValue();
        } else {
            var fut = new CompletableFuture<T>();

            CompletableFuture<T> previousFut = history.putIfAbsent(causalityToken, fut);

            return previousFut == null ? fut : previousFut;
        }
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

        assert causalityToken > actualToken0 : IgniteStringFormatter.format("Token earlier than actual [token={}, actual={}]",
                causalityToken, actualToken0);

        assert actualToken0 + 1 == causalityToken : IgniteStringFormatter.format(
                "Previous token did not complete [token={}, previous={}]", causalityToken, causalityToken - 1);

        CompletableFuture<T> res = history.putIfAbsent(causalityToken, CompletableFuture.completedFuture(value));

        if (res == null || res.isCompletedExceptionally()) {
            return;
        }

        assert !res.isDone() : IgniteStringFormatter.format("Different values asosiated with the token "
                + "[token={}, value={}, prevValeu={}]", causalityToken, value, res.join());

        res.complete(value);
    }

    /**
     * Should be called on a storage revision update. This also triggers completion of a future created for the given causality token. It
     * implies that all possible updates associated with this token have been already applied to the component.
     *
     * @param causalityToken Causality token.
     */
    private void onStorageRevisionUpdate(long causalityToken) {
        long actualToken0 = actualToken;

        assert causalityToken > actualToken0 : IgniteStringFormatter.format(
                "New token shoul be more than current [current={}, new={}]", actualToken0, causalityToken);

        onStorageRevisionUpdate.accept(this, causalityToken);

        if (history.size() > 1 && causalityToken - history.firstKey() >= historySize) {
            trimToSize(causalityToken);
        }

        Entry<Long, CompletableFuture<T>> entry = history.floorEntry(causalityToken);

        assert entry != null && entry.getValue().isDone() : IgniteStringFormatter.format(
                "Future for the token is not completed [token={}]", causalityToken);

        actualToken = causalityToken;
    }

    /**
     * Trims the storage to history size.
     *
     * @param causalityToken Last token which was happend.
     */
    private void trimToSize(long causalityToken) {
        Long lastToken = history.lastKey();

        for (Long token : history.keySet()) {
            if (token != lastToken && causalityToken - token >= historySize) {
                history.remove(token);
            }
        }
    }
}
