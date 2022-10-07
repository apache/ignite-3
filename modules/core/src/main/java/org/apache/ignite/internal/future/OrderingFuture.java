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
import java.util.function.BiConsumer;
import java.util.function.Function;
import org.jetbrains.annotations.Nullable;

public class OrderingFuture<T> {
    private T result;
    private Throwable exception;
    private boolean resolved;

    private final Queue<ResolveAction<T>> resolveQueue = new ArrayDeque<>();
    private final CountDownLatch resolveLatch = new CountDownLatch(1);

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
        synchronized (this) {
            if (resolved) {
                return;
            }

            this.result = result;
            resolved = true;

            resolveAfterCompletion();
        }
    }

    public void completeExceptionally(Throwable ex) {
        synchronized (this) {
            if (resolved) {
                return;
            }

            this.exception = ex;
            resolved = true;

            resolveAfterCompletion();
        }
    }

    private void resolveAfterCompletion() {
        for (ResolveAction<T> action : resolveQueue) {
            action.onResolved(result, exception);
        }

        resolveLatch.countDown();
    }

    public boolean isCompletedExceptionally() {
        synchronized (this) {
            return exception != null;
        }
    }

    public void whenComplete(BiConsumer<? super T, ? super Throwable> action) {
        synchronized (this) {
            if (resolved) {
                try {
                    action.accept(result, exception);
                } catch (Exception e) {
                    // ignore
                }
            } else {
                resolveQueue.add(new WhenComplete<>(action));
            }
        }
    }

    public <U> CompletableFuture<U> thenComposeToCompletable(Function<? super T, ? extends CompletableFuture<U>> mapper) {
        synchronized (this) {
            if (resolved) {
                if (exception != null) {
                    return CompletableFuture.failedFuture(new CompletionException(exception));
                }

                try {
                    return mapper.apply(result);
                } catch (Throwable e) {
                    return CompletableFuture.failedFuture(e);
                }
            } else {
                CompletableFuture<U> resultFuture = new CompletableFuture<>();
                resolveQueue.add(new ThenCompose<>(resultFuture, mapper));
                return resultFuture;
            }
        }
    }

    public T getNow(T valueIfAbsent) {
        synchronized (this) {
            if (resolved) {
                if (exception != null) {
                    throw new CompletionException(exception);
                } else {
                    return result;
                }
            }
            return valueIfAbsent;
        }
    }

    public T get(long timeout, TimeUnit unit) throws InterruptedException, TimeoutException, ExecutionException {
        boolean resolvedInTime = resolveLatch.await(timeout, unit);
        if (!resolvedInTime) {
            throw new TimeoutException();
        }

        synchronized (this) {
            if (exception != null) {
                throw new ExecutionException(exception);
            }

            return result;
        }
    }

    public CompletableFuture<T> toCompletableFuture() {
        CompletableFuture<T> completableFuture = new CompletableFuture<>();

        this.whenComplete((res, ex) -> {
            if (ex != null) {
                completableFuture.completeExceptionally(ex);
            } else {
                completableFuture.complete(res);
            }
        });

        return completableFuture;
    }

    private interface ResolveAction<T> {
        void onResolved(T result, Throwable ex);
    }

    private static class WhenComplete<T> implements ResolveAction<T> {
        private final BiConsumer<? super T, ? super Throwable> action;

        private WhenComplete(BiConsumer<? super T, ? super Throwable> action) {
            this.action = action;
        }

        @Override
        public void onResolved(T result, Throwable ex) {
            action.accept(result, ex);
        }
    }

    private static class ThenCompose<T, U> implements ResolveAction<T> {
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

                mapResult.whenComplete((mapRes, mapEx) -> {
                    if (mapEx != null) {
                        resultFuture.completeExceptionally(mapEx);
                    } else {
                        resultFuture.complete(mapRes);
                    }
                });
            } catch (Throwable e) {
                resultFuture.completeExceptionally(e);
            }
        }
    }
}
