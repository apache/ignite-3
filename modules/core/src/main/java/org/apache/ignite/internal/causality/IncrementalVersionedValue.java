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
import static java.util.concurrent.CompletableFuture.failedFuture;
import static org.apache.ignite.internal.causality.BaseVersionedValue.DEFAULT_MAX_HISTORY_SIZE;

import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.ignite.lang.ErrorGroups.Common;
import org.apache.ignite.lang.IgniteException;
import org.jetbrains.annotations.Nullable;

/**
 * Versioned Value flavor that accumulates updates posted by the {@link #update} method and "commits" them when {@link #complete} or
 * {@link #completeExceptionally} are called or a storage revision is updated (see constructors for details).
 */
public class IncrementalVersionedValue<T> implements VersionedValue<T> {
    private final BaseVersionedValue<T> versionedValue;

    /** Update mutex. */
    private final Object updateMutex = new Object();

    /**
     * Token that was used with the most recent {@link #update} call.
     *
     * <p>Multi-threaded access is guarded by {@link #updateMutex}.
     */
    private long expectedToken = -1;

    /**
     * Token that was used with the most recent {@link #completeInternal} call.
     *
     * <p>Multi-threaded access is guarded by {@link #updateMutex}.
     */
    private long lastCompleteToken = -1;

    /**
     * Token that was used with the most recent {@link #deleteInternal} call.
     *
     * <p>Multi-threaded access is guarded by {@link #updateMutex}.</p>
     */
    private long lastDeletedToken = -1;

    /**
     * Future that will be completed after all updates over the value in context of current causality token will be performed.
     *
     * <p>Multi-threaded access is guarded by {@link #updateMutex}.
     */
    private CompletableFuture<T> updaterFuture;

    /**
     * This registry chains two versioned values. The value, that uses this registry in the constructor, will be completed strictly after
     * the value, passed into this method, meaning that {@code resultVv.get(token).isDone();} will always imply
     * {@code vv.get(token).isDone();} for the same token value.
     *
     * <p>While affecting the state of resulting futures, this dependency doesn't affect the order of {@link #update(long, BiFunction)}
     * closures execution. These closures will still be called independently once the required parameter value is available.
     *
     * <p>In the case of "fresh" VV with no updates, first closure is always being executed synchronously inside of the
     * {@link #update(long, BiFunction)} call.
     */
    public static RevisionListenerRegistry dependingOn(IncrementalVersionedValue<?> vv) {
        return listener -> {
            vv.whenComplete((token, value, ex) -> listener.onUpdate(token));

            vv.whenDelete(listener::onDelete);
        };
    }

    /**
     * Constructor.
     *
     * @param registry Registry intended to connect this VersionedValue with a revision updater, that this
     *         VersionedValue should be able to listen to, for receiving storage revision updates. This registry is called once on a
     *         construction of this VersionedValue.
     * @param maxHistorySize Size of the history of changes to store, including last applied token.
     * @param defaultValueSupplier Supplier of the default value, that is used on {@link #update(long, BiFunction)} to evaluate the
     *         default value if the value is not initialized yet. It is not guaranteed to execute only once.
     */
    public IncrementalVersionedValue(
            String name,
            @Nullable RevisionListenerRegistry registry,
            int maxHistorySize,
            @Nullable Supplier<T> defaultValueSupplier
    ) {
        this.versionedValue = new BaseVersionedValue<>(name, maxHistorySize, defaultValueSupplier);

        this.updaterFuture = completedFuture(versionedValue.getDefault());

        if (registry != null) {
            registry.listen(new RevisionListener() {
                @Override
                public CompletableFuture<?> onUpdate(long revision) {
                    return completeInternal(revision);
                }

                @Override
                public void onDelete(long revisionUpperBoundInclusive) {
                    deleteInternal(revisionUpperBoundInclusive);
                }
            });
        }
    }

    /**
     * Constructor.
     *
     * @param registry Registry intended to connect this VersionedValue with a revision updater, that this
     *         VersionedValue should be able to listen to, for receiving storage revision updates. This registry is called once on a
     *         construction of this VersionedValue.
     * @param defaultValueSupplier Supplier of the default value, that is used on {@link #update(long, BiFunction)} to evaluate the
     *         default value if the value is not initialized yet. It is not guaranteed to execute only once.
     */
    public IncrementalVersionedValue(
            String name,
            @Nullable RevisionListenerRegistry registry,
            @Nullable Supplier<T> defaultValueSupplier
    ) {
        this(name, registry, DEFAULT_MAX_HISTORY_SIZE, defaultValueSupplier);
    }

    /**
     * Constructor with default history size.
     *
     * @param registry Registry intended to connect this VersionedValue with a revision updater, that this
     *         VersionedValue should be able to listen to, for receiving storage revision updates. This registry is called once on a
     *         construction of this VersionedValue.
     */
    public IncrementalVersionedValue(String name, RevisionListenerRegistry registry) {
        this(name, registry, DEFAULT_MAX_HISTORY_SIZE, null);
    }

    @Override
    public String name() {
        return versionedValue.name();
    }

    @Override
    public CompletableFuture<T> get(long causalityToken) {
        return versionedValue.get(causalityToken);
    }

    @Override
    public @Nullable T latest() {
        return versionedValue.latest();
    }

    @Override
    public long latestCausalityToken() {
        return versionedValue.latestCausalityToken();
    }

    @Override
    public void whenComplete(CompletionListener<T> action) {
        versionedValue.whenComplete(action);
    }

    @Override
    public void removeWhenComplete(CompletionListener<T> action) {
        versionedValue.removeWhenComplete(action);
    }

    @Override
    public void whenDelete(DeletionListener<T> action) {
        versionedValue.whenDelete(action);
    }

    @Override
    public void removeWhenDelete(DeletionListener<T> action) {
        versionedValue.removeWhenDelete(action);
    }

    /**
     * Updates the value using the given updater. The updater receives the value associated with a previous token, or default value (see
     * constructor) if the value isn't initialized, or current intermediate value, if this method has been already called for the same
     * token; and returns a new value.<br> The updater will be called after updaters that had been passed to previous calls of this method
     * complete. If an exception ({@link CancellationException} or {@link CompletionException}) was thrown when calculating the value for
     * previous token, then updater is used to process the exception and calculate a new value.<br> This method can be called multiple times
     * for the same token, and doesn't complete the future created for this token. The future is supposed to be completed by storage
     * revision update or a call of {@link #complete(long)} in this case. If this method has been called at least once on the given token,
     * the updater will receive a value that was evaluated by updater on previous call, as intermediate result. If no update were done on
     * the given token, the updated will immediately receive the value from the previous token, if it's completed. <br> As the order of
     * multiple calls of this method on the same token is unknown, operations done by the updater must be commutative. For example:
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
     * @param causalityToken Causality token. Used mainly for sanity checks.
     * @param updater The binary function that accepts previous value and exception, if present, and update it to compute the new
     *         value.
     * @return Future for updated value.
     */
    public CompletableFuture<T> update(long causalityToken, BiFunction<T, Throwable, CompletableFuture<T>> updater) {
        synchronized (updateMutex) {
            if (expectedToken == -1) {
                if (causalityToken <= lastCompleteToken) {
                    throw new IgniteException(
                            Common.INTERNAL_ERR,
                            String.format("Causality token is outdated, previous token %d, got %d", lastCompleteToken, causalityToken)
                    );
                }

                expectedToken = causalityToken;
            } else {
                if (expectedToken != causalityToken) {
                    throw new IgniteException(
                            Common.INTERNAL_ERR,
                            String.format("Causality token mismatch, expected %d, got %d", expectedToken, causalityToken)
                    );
                }
            }

            updaterFuture = updaterFuture
                    .handle(updater)
                    .thenCompose(Function.identity());

            return updaterFuture;
        }
    }

    /**
     * Completes the Versioned Value by publishing the result of applied updates under the given token. This method will look for the
     * previous complete token and complete all registered futures in the {@code (prevToken, causalityToken)} range. If no {@code complete}
     * methods have been called before, all these futures will be complete with the configured default value.
     *
     * <p>Calling this method will trigger the {@link #whenComplete} listeners for the given token.
     *
     * @param causalityToken Causality token.
     */
    public void complete(long causalityToken) {
        completeInternal(causalityToken);
    }

    /**
     * Interrupts the current chain of updates and completes it with the given exception. This method will look for the previous complete
     * token and complete all registered futures in the {@code (prevToken, causalityToken)} range. If no {@code complete} methods have been
     * called before, all these futures will be complete with the configured default value.
     *
     * <p>Calling this method will trigger the {@link #whenComplete} listeners for the given token.
     *
     * @param causalityToken Causality token.
     * @param throwable An exception.
     */
    public void completeExceptionally(long causalityToken, Throwable throwable) {
        synchronized (updateMutex) {
            updaterFuture = failedFuture(throwable);

            completeInternal(causalityToken);
        }
    }

    private CompletableFuture<?> completeInternal(long causalityToken) {
        synchronized (updateMutex) {
            assert expectedToken == -1 || expectedToken == causalityToken
                    : String.format("Causality token mismatch, expected %d, got %d", expectedToken, causalityToken);
            assert causalityToken > lastCompleteToken : String.format(
                    "Causality token must be greater than the last completed: [token=%s, lastCompleted=%s]", causalityToken,
                    lastCompleteToken);
            assert causalityToken > lastDeletedToken : String.format(
                    "Causality token must be greater than the last deleted: [token=%s, lastDeleted=%s]", causalityToken,
                    lastDeletedToken);

            lastCompleteToken = causalityToken;
            expectedToken = -1;

            // Create a local copy while we are under the lock to pass it to the lambda below.
            CompletableFuture<T> localUpdaterFuture = updaterFuture;

            if (updaterFuture.isDone()) {
                // Since the future has already been completed, there's no need to store a new future object in the history map and we can
                // save a little bit of memory. This is useful when no "update" calls have been made between two "complete" calls.
                updaterFuture.whenComplete((v, t) -> versionedValue.complete(causalityToken, localUpdaterFuture));
            } else {
                updaterFuture = updaterFuture.whenComplete((v, t) -> versionedValue.complete(causalityToken, localUpdaterFuture));
            }

            return updaterFuture;
        }
    }

    private void deleteInternal(long causalityToken) {
        synchronized (updateMutex) {
            assert causalityToken < lastCompleteToken : String.format(
                    "Causality token must be less than the last completed: [name=%s, token=%s, lastCompleted=%s]",
                    name(),
                    causalityToken,
                    lastCompleteToken
            );
            assert causalityToken > lastDeletedToken : String.format(
                    "Causality token must be greater than the last deleted: [name=%s, token=%s, lastDeleted=%s]",
                    name(),
                    causalityToken,
                    lastDeletedToken
            );

            lastDeletedToken = causalityToken;

            versionedValue.deleteUpTo(causalityToken);
        }
    }
}
