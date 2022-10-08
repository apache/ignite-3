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

package org.apache.ignite.internal.future;

import java.util.ArrayDeque;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.BiConsumer;
import java.util.function.Function;
import org.jetbrains.annotations.Nullable;

/**
 * A little analogue of {@link CompletableFuture} that has the following property: callbacks (like {@link #whenComplete(BiConsumer)}
 * and {@link #thenComposeToCompletable(Function)}) are invoked in the same order in which they were registered.
 *
 * @param <T> Type of payload.
 * @see CompletableFuture
 */
public class OrderingFuture<T> {
    private boolean resolved;
    private T result;
    private Throwable exception;

    private Queue<DependentAction<T>> dependents = new ArrayDeque<>();
    private final CountDownLatch resolveLatch = new CountDownLatch(1);

    private final Lock readLock;
    private final Lock writeLock;

    /**
     * Creates an incomplete future.
     */
    public OrderingFuture() {
        ReentrantReadWriteLock readWriteLock = new ReentrantReadWriteLock();

        readLock = readWriteLock.readLock();
        writeLock = readWriteLock.writeLock();
    }

    /**
     * Creates a future that is alredy completed with the given value.
     *
     * @param result Value with which the future is completed.
     * @param <T> Payload type.
     * @return Completed future.
     */
    public static <T> OrderingFuture<T> completedFuture(@Nullable T result) {
        var future = new OrderingFuture<T>();
        future.complete(result);
        return future;
    }

    /**
     * Creates a future that is alredy completed exceptionally (i.e. failed) with the given exception.
     *
     * @param ex Exception with which the future is failed.
     * @param <T> Payload type.
     * @return Failed future.
     */
    public static <T> OrderingFuture<T> failedFuture(Throwable ex) {
        var future = new OrderingFuture<T>();
        future.completeExceptionally(ex);
        return future;
    }

    /**
     * Adapts a {@link CompletableFuture}. That is, creates an {@link OrderingFuture} that gets completed when the
     * original future is completed (and in the same way in which it gets completed).
     *
     * @param adaptee Future to adapt.
     * @param <T> Payload type.
     * @return Adapting future.
     */
    public static <T> OrderingFuture<T> adapt(CompletableFuture<T> adaptee) {
        var future = new OrderingFuture<T>();

        adaptee.whenComplete((res, ex) -> {
            if (ex != null) {
                future.completeExceptionally(ex);
            } else {
                future.complete(res);
            }
        });

        return future;
    }

    /**
     * Completes this future with the given result if it's not completed yet; otherwise has no effect.
     *
     * @param result Completion value (may be {@code null}).
     */
    public void complete(@Nullable T result) {
        completeInternal(result, null);
    }

    /**
     * Completes this future exceptionally with the given exception if it's not completed yet; otherwise has no effect.
     *
     * @param ex Exception.
     */
    public void completeExceptionally(Throwable ex) {
        completeInternal(null, ex);
    }

    private void completeInternal(T result, Throwable ex) {
        writeLock.lock();

        try {
            if (resolved) {
                return;
            }

            resolved = true;
            this.result = result;
            this.exception = (ex instanceof CompletionException) && ex.getCause() != null ? ex.getCause() : ex;

            resolveDependents();
        } finally {
            writeLock.unlock();
        }
    }

    private void resolveDependents() {
        for (DependentAction<T> action : dependents) {
            try {
                action.onResolved(result, exception);
            } catch (Exception e) {
                // ignore
            }
        }

        dependents = null;

        resolveLatch.countDown();
    }

    /**
     * Returns {@code true} if this future is completed exceptionally, {@code false} if completed normally or not completed.
     *
     * @return {@code true} if this future is completed exceptionally, {@code false} if completed normally or not completed
     */
    public boolean isCompletedExceptionally() {
        readLock.lock();

        try {
            return exception != null;
        } finally {
            readLock.unlock();
        }
    }

    /**
     * Adds a callback that gets executed as soon as this future gets completed for any reason. The action will get both result
     * and exception; if the completion is normal, exception will be {@code null}, otherwise result will be {@code null}.
     * If it's already complete, the action is executed immediately.
     * Any exception produced by the action is swallowed.
     *
     * @param action Action to execute.
     */
    public void whenComplete(BiConsumer<? super T, ? super Throwable> action) {
        readLock.lock();

        try {
            if (resolved) {
                acceptQuietly(action, result, exception);
            } else {
                dependents.add(new WhenComplete<>(action));
            }
        } finally {
            readLock.unlock();
        }
    }

    private static <T> void acceptQuietly(BiConsumer<? super T, ? super Throwable> action, T result, Throwable ex) {
        try {
            action.accept(result, ex);
        } catch (Exception e) {
            // ignore
        }
    }

    /**
     * Creates a composition of this future with a function producing a {@link CompletableFuture}.
     *
     * @param mapper Mapper used to produce a {@link CompletableFuture} from this future result.
     * @param <U> Result future payload type.
     * @return Composition.
     * @see CompletableFuture#thenCompose(Function)
     */
    public <U> CompletableFuture<U> thenComposeToCompletable(Function<? super T, ? extends CompletableFuture<U>> mapper) {
        readLock.lock();

        try {
            if (resolved) {
                if (exception != null) {
                    return CompletableFuture.failedFuture(new CompletionException(exception));
                }

                return applyMapper(mapper, result);
            } else {
                CompletableFuture<U> resultFuture = new CompletableFuture<>();
                dependents.add(new ThenCompose<>(resultFuture, mapper));
                return resultFuture;
            }
        } finally {
            readLock.unlock();
        }
    }

    private static <T, U> CompletableFuture<U> applyMapper(Function<? super T, ? extends CompletableFuture<U>> mapper, T result) {
        try {
            return mapper.apply(result);
        } catch (Throwable e) {
            return CompletableFuture.failedFuture(e);
        }
    }

    /**
     * Returns the completion value, (if the future is completed normally), throws completion cause wrapped in
     * {@link CompletionException} (if the future is completed exceptionally), or returns the provided default value
     * if the future is not completed yet.
     *
     * @param valueIfAbsent Value to return if the future is not completed yet.
     * @return Completion value or default value.
     * @see CompletableFuture#getNow(Object)
     */
    public T getNow(T valueIfAbsent) {
        readLock.lock();

        try {
            if (resolved) {
                if (exception != null) {
                    throw new CompletionException(exception);
                } else {
                    return result;
                }
            } else {
                return valueIfAbsent;
            }
        } finally {
            readLock.unlock();
        }
    }

    /**
     * Returns completion value or throws completion exception (wrapped in {@link ExecutionException}), waiting for
     * completion up to the specified amount of time, if not completed yet. If the time runs out while waiting,
     * throws {@link TimeoutException}.
     *
     * @param timeout Maximum amount of time to wait.
     * @param unit    Unit of time in which the timeout is given.
     * @return Completion value.
     * @throws InterruptedException Thrown if the current thread gets interrupted while waiting for completion.
     * @throws TimeoutException Thrown if the wait for completion times out.
     * @throws ExecutionException Thrown (with the original exception as a cause) if the future completes exceptionally.
     * @see CompletableFuture#get(long, TimeUnit)
     */
    public T get(long timeout, TimeUnit unit) throws InterruptedException, TimeoutException, ExecutionException {
        boolean resolvedInTime = resolveLatch.await(timeout, unit);
        if (!resolvedInTime) {
            throw new TimeoutException();
        }

        readLock.lock();

        try {
            if (exception != null) {
                throw new ExecutionException(exception);
            } else {
                return result;
            }
        } finally {
            readLock.unlock();
        }
    }

    /**
     * Returns a {@link CompletableFuture} that gets completed when this future gets completed (and in the same way).
     * The returned future does not provide any ordering guarantees that this future provides.
     *
     * @return An equivalent {@link CompletableFuture}.
     */
    public CompletableFuture<T> toCompletableFuture() {
        CompletableFuture<T> completableFuture = new CompletableFuture<>();

        this.whenComplete((res, ex) -> completeCompletableFuture(completableFuture, res, ex));

        return completableFuture;
    }

    private static <T> void completeCompletableFuture(CompletableFuture<T> future, T result, Throwable ex) {
        if (ex != null) {
            future.completeExceptionally(ex);
        } else {
            future.complete(result);
        }
    }

    /**
     * Dependent action that gets resolved when this future is completed.
     *
     * @param <T> Payload type.
     */
    private interface DependentAction<T> {
        /**
         * Informs that dependent that the host future is resolved.
         *
         * @param result Normal completion result ({@code null} if completed exceptionally, but might be {@code null} for normal completion.
         * @param ex     Exceptional completion cause ({@code null} if completed normally).
         */
        void onResolved(T result, Throwable ex);
    }

    private static class WhenComplete<T> implements DependentAction<T> {
        private final BiConsumer<? super T, ? super Throwable> action;

        private WhenComplete(BiConsumer<? super T, ? super Throwable> action) {
            this.action = action;
        }

        @Override
        public void onResolved(T result, Throwable ex) {
            acceptQuietly(action, result, ex);
        }
    }

    private static class ThenCompose<T, U> implements DependentAction<T> {
        private final CompletableFuture<U> resultFuture;
        private final Function<? super T, ? extends CompletableFuture<U>> mapper;

        private ThenCompose(CompletableFuture<U> resultFuture, Function<? super T, ? extends CompletableFuture<U>> mapper) {
            this.resultFuture = resultFuture;
            this.mapper = mapper;
        }

        @Override
        public void onResolved(T result, Throwable ex) {
            if (ex != null) {
                resultFuture.completeExceptionally(ex);
                return;
            }

            try {
                CompletableFuture<U> mapResult = mapper.apply(result);

                mapResult.whenComplete((mapRes, mapEx) -> completeCompletableFuture(resultFuture, mapRes, mapEx));
            } catch (Throwable e) {
                resultFuture.completeExceptionally(e);
            }
        }
    }
}
