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

public class OrderingFuture<T> {
    private boolean resolved;
    private T result;
    private Throwable exception;

    private Queue<DependentAction<T>> dependents = new ArrayDeque<>();
    private final CountDownLatch resolveLatch = new CountDownLatch(1);

    private final Lock readLock;
    private final Lock writeLock;

    public OrderingFuture() {
        ReentrantReadWriteLock readWriteLock = new ReentrantReadWriteLock();

        readLock = readWriteLock.readLock();
        writeLock = readWriteLock.writeLock();
    }

    public static <T> OrderingFuture<T> completedFuture(@Nullable T result) {
        var future = new OrderingFuture<T>();
        future.complete(result);
        return future;
    }

    public static <T> OrderingFuture<T> failedFuture(Throwable ex) {
        var future = new OrderingFuture<T>();
        future.completeExceptionally(ex);
        return future;
    }

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

    public void complete(T result) {
        completeInternal(result, null);
    }

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
            this.exception = ex;

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

    public boolean isCompletedExceptionally() {
        readLock.lock();

        try {
            return exception != null;
        } finally {
            readLock.unlock();
        }
    }

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

    private interface DependentAction<T> {
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
