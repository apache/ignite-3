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
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * A {@link CompletableFuture} that ensures callbacks are called in FIFO order.
 *
 * <p>The default {@link CompletableFuture} does not guarantee the ordering of callbacks, and indeed in some situations
 * executes them in LIFO order.
 */
public class OrderedCompletableFuture<T> extends CompletableFuture<T> {
    private final Queue<CompletableFuture<T>> orderedDependents = new ArrayDeque<>();

    private boolean complete;
    private T result;
    private Throwable error;

    /** Set to true when we are inside public constructor. See comments to the constructor itself. */
    private static final ThreadLocal<Boolean> insidePublicConstructor = ThreadLocal.withInitial(() -> Boolean.FALSE);

    /**
     * Creates a new OrderedCompletableFuture.
     */
    public OrderedCompletableFuture() {
        // super.whenComplete() invokes #newIncompleteFuture(), which in turn creates a new OrderedCompletableFuture.
        // If we do whenComplete() on it as well, we'll get an infinite loop. We use the thread local to break the loop
        // by not issuing whenComplete() on that internally-created future instance. This seems fine as that instance never
        // gets seen by anyone, so the ordering of callbacks on it is not important.

        insidePublicConstructor.set(Boolean.TRUE);

        try {
            super.whenComplete(this::complete);
        } finally {
            insidePublicConstructor.set(Boolean.FALSE);
        }
    }

    private OrderedCompletableFuture(@SuppressWarnings("unused") Void unusedTag) {
        // See comment on the public constructor for hints on why we need this bogus constructor.
    }

    /**
     * Adds a new dependent. If this future is already completed, the dependent will be completed in the same way
     * right off the bat. Otherwise, it will be added to the dependent queue.
     */
    private CompletableFuture<T> orderedDependent() {
        synchronized (orderedDependents) {
            if (complete) {
                if (error == null) {
                    return CompletableFuture.completedFuture(result);
                } else {
                    return CompletableFuture.failedFuture(error);
                }
            } else {
                CompletableFuture<T> future = new CompletableFuture<>();
                orderedDependents.add(future);
                return future;
            }
        }
    }

    /**
     * Completes dependents in FIFO order.
     */
    private void complete(T result, Throwable error) {
        synchronized (orderedDependents) {
            this.complete = true;
            this.result = result;
            this.error = error;

            if (error == null) {
                for (CompletableFuture<T> dependent : orderedDependents) {
                    dependent.complete(result);
                }
            } else {
                for (CompletableFuture<T> dependent : orderedDependents) {
                    dependent.completeExceptionally(error);
                }
            }
        }
    }

    /**
     * Creates an ordered proxy that completes when the given future completes in the same way as it does.
     *
     * @param future Future for which to create an ordered proxy.
     * @param <U> Element type.
     * @return Ordered proxy.
     */
    private <U> OrderedCompletableFuture<U> orderedProxy(CompletableFuture<U> future) {
        OrderedCompletableFuture<U> result = new OrderedCompletableFuture<>();

        future.whenComplete((res, ex) -> {
            if (ex != null) {
                result.completeExceptionally(ex);
            } else {
                result.complete(res);
            }
        });

        return result;
    }

    @Override
    public <U> CompletableFuture<U> thenApply(Function<? super T, ? extends U> fn) {
        return orderedProxy(orderedDependent().thenApply(fn));
    }

    @Override
    public <U> CompletableFuture<U> thenApplyAsync(Function<? super T, ? extends U> fn) {
        return orderedProxy(orderedDependent().thenApplyAsync(fn));
    }

    @Override
    public <U> CompletableFuture<U> thenApplyAsync(Function<? super T, ? extends U> fn, Executor executor) {
        return orderedProxy(orderedDependent().thenApplyAsync(fn, executor));
    }

    @Override
    public CompletableFuture<Void> thenAccept(Consumer<? super T> action) {
        return orderedProxy(orderedDependent().thenAccept(action));
    }

    @Override
    public CompletableFuture<Void> thenAcceptAsync(Consumer<? super T> action) {
        return orderedProxy(orderedDependent().thenAcceptAsync(action));
    }

    @Override
    public CompletableFuture<Void> thenAcceptAsync(Consumer<? super T> action, Executor executor) {
        return orderedProxy(orderedDependent().thenAcceptAsync(action, executor));
    }

    @Override
    public CompletableFuture<Void> thenRun(Runnable action) {
        return orderedProxy(orderedDependent().thenRun(action));
    }

    @Override
    public CompletableFuture<Void> thenRunAsync(Runnable action) {
        return orderedProxy(orderedDependent().thenRunAsync(action));
    }

    @Override
    public CompletableFuture<Void> thenRunAsync(Runnable action, Executor executor) {
        return orderedProxy(orderedDependent().thenRunAsync(action, executor));
    }

    @Override
    public <U, V> CompletableFuture<V> thenCombine(CompletionStage<? extends U> other, BiFunction<? super T, ? super U, ? extends V> fn) {
        return orderedProxy(orderedDependent().thenCombine(other, fn));
    }

    @Override
    public <U, V> CompletableFuture<V> thenCombineAsync(CompletionStage<? extends U> other,
            BiFunction<? super T, ? super U, ? extends V> fn) {
        return orderedProxy(orderedDependent().thenCombineAsync(other, fn));
    }

    @Override
    public <U, V> CompletableFuture<V> thenCombineAsync(CompletionStage<? extends U> other,
            BiFunction<? super T, ? super U, ? extends V> fn, Executor executor) {
        return orderedProxy(orderedDependent().thenCombineAsync(other, fn, executor));
    }

    @Override
    public <U> CompletableFuture<Void> thenAcceptBoth(CompletionStage<? extends U> other, BiConsumer<? super T, ? super U> action) {
        return orderedProxy(orderedDependent().thenAcceptBoth(other, action));
    }

    @Override
    public <U> CompletableFuture<Void> thenAcceptBothAsync(CompletionStage<? extends U> other, BiConsumer<? super T, ? super U> action) {
        return orderedProxy(orderedDependent().thenAcceptBothAsync(other, action));
    }

    @Override
    public <U> CompletableFuture<Void> thenAcceptBothAsync(CompletionStage<? extends U> other, BiConsumer<? super T, ? super U> action,
            Executor executor) {
        return orderedProxy(orderedDependent().thenAcceptBothAsync(other, action, executor));
    }

    @Override
    public CompletableFuture<Void> runAfterBoth(CompletionStage<?> other, Runnable action) {
        return orderedProxy(orderedDependent().runAfterBoth(other, action));
    }

    @Override
    public CompletableFuture<Void> runAfterBothAsync(CompletionStage<?> other, Runnable action) {
        return orderedProxy(orderedDependent().runAfterBothAsync(other, action));
    }

    @Override
    public CompletableFuture<Void> runAfterBothAsync(CompletionStage<?> other, Runnable action, Executor executor) {
        return orderedProxy(orderedDependent().runAfterBothAsync(other, action, executor));
    }

    @Override
    public <U> CompletableFuture<U> applyToEither(CompletionStage<? extends T> other, Function<? super T, U> fn) {
        return orderedProxy(orderedDependent().applyToEither(other, fn));
    }

    @Override
    public <U> CompletableFuture<U> applyToEitherAsync(CompletionStage<? extends T> other, Function<? super T, U> fn) {
        return orderedProxy(orderedDependent().applyToEitherAsync(other, fn));
    }

    @Override
    public <U> CompletableFuture<U> applyToEitherAsync(CompletionStage<? extends T> other, Function<? super T, U> fn, Executor executor) {
        return orderedProxy(orderedDependent().applyToEitherAsync(other, fn, executor));
    }

    @Override
    public CompletableFuture<Void> acceptEither(CompletionStage<? extends T> other, Consumer<? super T> action) {
        return orderedProxy(orderedDependent().acceptEither(other, action));
    }

    @Override
    public CompletableFuture<Void> acceptEitherAsync(CompletionStage<? extends T> other, Consumer<? super T> action) {
        return orderedProxy(orderedDependent().acceptEitherAsync(other, action));
    }

    @Override
    public CompletableFuture<Void> acceptEitherAsync(CompletionStage<? extends T> other, Consumer<? super T> action, Executor executor) {
        return orderedProxy(orderedDependent().acceptEitherAsync(other, action, executor));
    }

    @Override
    public CompletableFuture<Void> runAfterEither(CompletionStage<?> other, Runnable action) {
        return orderedProxy(orderedDependent().runAfterEither(other, action));
    }

    @Override
    public CompletableFuture<Void> runAfterEitherAsync(CompletionStage<?> other, Runnable action) {
        return orderedProxy(orderedDependent().runAfterEitherAsync(other, action));
    }

    @Override
    public CompletableFuture<Void> runAfterEitherAsync(CompletionStage<?> other, Runnable action, Executor executor) {
        return orderedProxy(orderedDependent().runAfterEitherAsync(other, action, executor));
    }

    @Override
    public <U> CompletableFuture<U> thenCompose(Function<? super T, ? extends CompletionStage<U>> fn) {
        return orderedProxy(orderedDependent().thenCompose(fn));
    }

    @Override
    public <U> CompletableFuture<U> thenComposeAsync(Function<? super T, ? extends CompletionStage<U>> fn) {
        return orderedProxy(orderedDependent().thenComposeAsync(fn));
    }

    @Override
    public <U> CompletableFuture<U> thenComposeAsync(Function<? super T, ? extends CompletionStage<U>> fn, Executor executor) {
        return orderedProxy(orderedDependent().thenComposeAsync(fn, executor));
    }

    @Override
    public CompletableFuture<T> whenComplete(BiConsumer<? super T, ? super Throwable> action) {
        return orderedProxy(orderedDependent().whenComplete(action));
    }

    @Override
    public CompletableFuture<T> whenCompleteAsync(BiConsumer<? super T, ? super Throwable> action) {
        return orderedProxy(orderedDependent().whenCompleteAsync(action));
    }

    @Override
    public CompletableFuture<T> whenCompleteAsync(BiConsumer<? super T, ? super Throwable> action, Executor executor) {
        return orderedProxy(orderedDependent().whenCompleteAsync(action, executor));
    }

    @Override
    public <U> CompletableFuture<U> handle(BiFunction<? super T, Throwable, ? extends U> fn) {
        return orderedProxy(orderedDependent().handle(fn));
    }

    @Override
    public <U> CompletableFuture<U> handleAsync(BiFunction<? super T, Throwable, ? extends U> fn) {
        return orderedProxy(orderedDependent().handleAsync(fn));
    }

    @Override
    public <U> CompletableFuture<U> handleAsync(BiFunction<? super T, Throwable, ? extends U> fn, Executor executor) {
        return orderedProxy(orderedDependent().handleAsync(fn, executor));
    }

    @Override
    public CompletableFuture<T> exceptionally(Function<Throwable, ? extends T> fn) {
        return orderedProxy(orderedDependent().exceptionally(fn));
    }

    @Override
    public CompletableFuture<T> toCompletableFuture() {
        return this;
    }

    @Override
    public int getNumberOfDependents() {
        synchronized (orderedDependents) {
            return orderedDependents.size();
        }
    }

    @Override
    public <U> CompletableFuture<U> newIncompleteFuture() {
        // See comment on the public constructor for hints on why we need this thread-local dance.

        if (insidePublicConstructor.get()) {
            return new OrderedCompletableFuture<>(null);
        } else {
            return new OrderedCompletableFuture<>();
        }
    }

    @Override
    public CompletableFuture<T> copy() {
        return thenApply(x -> x);
    }

    @Override
    public CompletionStage<T> minimalCompletionStage() {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public CompletableFuture<T> completeAsync(Supplier<? extends T> supplier, Executor executor) {
        return orderedProxy(orderedDependent().completeAsync(supplier, executor));
    }

    @Override
    public CompletableFuture<T> completeAsync(Supplier<? extends T> supplier) {
        return orderedProxy(orderedDependent().completeAsync(supplier));
    }

    @Override
    public CompletableFuture<T> orTimeout(long timeout, TimeUnit unit) {
        return orderedProxy(orderedDependent().orTimeout(timeout, unit));
    }

    @Override
    public CompletableFuture<T> completeOnTimeout(T value, long timeout, TimeUnit unit) {
        return orderedProxy(orderedDependent().completeOnTimeout(value, timeout, unit));
    }
}
