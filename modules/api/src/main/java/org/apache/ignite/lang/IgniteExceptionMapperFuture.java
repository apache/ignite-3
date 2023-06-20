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

package org.apache.ignite.lang;

import static org.apache.ignite.internal.util.ExceptionUtils.unwrapCause;
import static org.apache.ignite.lang.IgniteExceptionMapperUtil.mapToPublicException;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * This class represents a {@link CompletableFuture}, the main purpose of this future is to automatically map
 * internal exceptions to public ones on completion.
 *
 * @param <T> The result type.
 */
public class IgniteExceptionMapperFuture<T> extends CompletableFuture<T> {
    /** Internal future, whose result will be used to map to a public exception if this delegate completed exceptionally. */
    private final CompletableFuture<T> delegate;

    /**
     * Creates a new instance {@link IgniteExceptionMapperFuture} with the given {@code delegate}
     * as an underlying future that serves all requests.
     *
     * @param delegate Future to be wrapped.
     * @param <U> The result Type.
     * @return New instance {@link IgniteExceptionMapperFuture}.
     */
    public static <U> IgniteExceptionMapperFuture<U> of(CompletableFuture<U> delegate) {
        return new IgniteExceptionMapperFuture<>(delegate);
    }

    /**
     * Creates a new instance {@link IgniteExceptionMapperFuture}.
     *
     * @param delegate The future that serves all requests.
     */
    public IgniteExceptionMapperFuture(CompletableFuture<T> delegate) {
        this.delegate = delegate
                .handle((res, err) -> {
                    if (err != null) {
                        throw new CompletionException(mapToPublicException(unwrapCause(err)));
                    }

                    return res;
                });
    }

    /** {@inheritDoc} */
    @Override
    public boolean isDone() {
        return delegate.isDone();
    }

    /** {@inheritDoc} */
    @Override
    public T get() throws InterruptedException, ExecutionException {
        return delegate.get();
    }

    /** {@inheritDoc} */
    @Override
    public T get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        return delegate.get(timeout, unit);
    }

    /** {@inheritDoc} */
    @Override
    public T join() {
        return delegate.join();
    }

    /** {@inheritDoc} */
    @Override
    public T getNow(T valueIfAbsent) {
        return delegate.getNow(valueIfAbsent);
    }

    /** {@inheritDoc} */
    @Override
    public boolean complete(T value) {
        return delegate.complete(value);
    }

    /** {@inheritDoc} */
    @Override
    public boolean completeExceptionally(Throwable ex) {
        return delegate.completeExceptionally(ex);
    }

    /** {@inheritDoc} */
    @Override
    public <U> CompletableFuture<U> thenApply(Function<? super T, ? extends U> fn) {
        return delegate.thenApply(fn);
    }

    /** {@inheritDoc} */
    @Override
    public <U> CompletableFuture<U> thenApplyAsync(Function<? super T, ? extends U> fn) {
        return delegate.thenApplyAsync(fn);
    }

    /** {@inheritDoc} */
    @Override
    public <U> CompletableFuture<U> thenApplyAsync(Function<? super T, ? extends U> fn, Executor executor) {
        return delegate.thenApplyAsync(fn, executor);
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Void> thenAccept(Consumer<? super T> action) {
        return delegate.thenAccept(action);
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Void> thenAcceptAsync(Consumer<? super T> action) {
        return delegate.thenAcceptAsync(action);
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Void> thenAcceptAsync(Consumer<? super T> action, Executor executor) {
        return delegate.thenAcceptAsync(action, executor);
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Void> thenRun(Runnable action) {
        return delegate.thenRun(action);
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Void> thenRunAsync(Runnable action) {
        return delegate.thenRunAsync(action);
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Void> thenRunAsync(Runnable action, Executor executor) {
        return delegate.thenRunAsync(action, executor);
    }

    /** {@inheritDoc} */
    @Override
    public <U, V> CompletableFuture<V> thenCombine(CompletionStage<? extends U> other, BiFunction<? super T, ? super U, ? extends V> fn) {
        return delegate.thenCombine(other, fn);
    }

    /** {@inheritDoc} */
    @Override
    public <U, V> CompletableFuture<V> thenCombineAsync(CompletionStage<? extends U> other,
            BiFunction<? super T, ? super U, ? extends V> fn) {
        return delegate.thenCombineAsync(other, fn);
    }

    /** {@inheritDoc} */
    @Override
    public <U, V> CompletableFuture<V> thenCombineAsync(CompletionStage<? extends U> other,
            BiFunction<? super T, ? super U, ? extends V> fn, Executor executor) {
        return delegate.thenCombineAsync(other, fn, executor);
    }

    /** {@inheritDoc} */
    @Override
    public <U> CompletableFuture<Void> thenAcceptBoth(CompletionStage<? extends U> other, BiConsumer<? super T, ? super U> action) {
        return delegate.thenAcceptBoth(other, action);
    }

    /** {@inheritDoc} */
    @Override
    public <U> CompletableFuture<Void> thenAcceptBothAsync(CompletionStage<? extends U> other, BiConsumer<? super T, ? super U> action) {
        return delegate.thenAcceptBothAsync(other, action);
    }

    /** {@inheritDoc} */
    @Override
    public <U> CompletableFuture<Void> thenAcceptBothAsync(CompletionStage<? extends U> other, BiConsumer<? super T, ? super U> action,
            Executor executor) {
        return delegate.thenAcceptBothAsync(other, action, executor);
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Void> runAfterBoth(CompletionStage<?> other, Runnable action) {
        return delegate.runAfterBoth(other, action);
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Void> runAfterBothAsync(CompletionStage<?> other, Runnable action) {
        return delegate.runAfterBothAsync(other, action);
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Void> runAfterBothAsync(CompletionStage<?> other, Runnable action, Executor executor) {
        return delegate.runAfterBothAsync(other, action, executor);
    }

    /** {@inheritDoc} */
    @Override
    public <U> CompletableFuture<U> applyToEither(CompletionStage<? extends T> other, Function<? super T, U> fn) {
        return delegate.applyToEither(other, fn);
    }

    /** {@inheritDoc} */
    @Override
    public <U> CompletableFuture<U> applyToEitherAsync(CompletionStage<? extends T> other, Function<? super T, U> fn) {
        return delegate.applyToEitherAsync(other, fn);
    }

    /** {@inheritDoc} */
    @Override
    public <U> CompletableFuture<U> applyToEitherAsync(CompletionStage<? extends T> other, Function<? super T, U> fn, Executor executor) {
        return delegate.applyToEitherAsync(other, fn, executor);
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Void> acceptEither(CompletionStage<? extends T> other, Consumer<? super T> action) {
        return delegate.acceptEither(other, action);
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Void> acceptEitherAsync(CompletionStage<? extends T> other, Consumer<? super T> action) {
        return delegate.acceptEitherAsync(other, action);
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Void> acceptEitherAsync(CompletionStage<? extends T> other, Consumer<? super T> action, Executor executor) {
        return delegate.acceptEitherAsync(other, action, executor);
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Void> runAfterEither(CompletionStage<?> other, Runnable action) {
        return delegate.runAfterEither(other, action);
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Void> runAfterEitherAsync(CompletionStage<?> other, Runnable action) {
        return delegate.runAfterEitherAsync(other, action);
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Void> runAfterEitherAsync(CompletionStage<?> other, Runnable action, Executor executor) {
        return delegate.runAfterEitherAsync(other, action, executor);
    }

    /** {@inheritDoc} */
    @Override
    public <U> CompletableFuture<U> thenCompose(Function<? super T, ? extends CompletionStage<U>> fn) {
        return delegate.thenCompose(fn);
    }

    /** {@inheritDoc} */
    @Override
    public <U> CompletableFuture<U> thenComposeAsync(Function<? super T, ? extends CompletionStage<U>> fn) {
        return delegate.thenComposeAsync(fn);
    }

    /** {@inheritDoc} */
    @Override
    public <U> CompletableFuture<U> thenComposeAsync(Function<? super T, ? extends CompletionStage<U>> fn, Executor executor) {
        return delegate.thenComposeAsync(fn, executor);
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<T> whenComplete(BiConsumer<? super T, ? super Throwable> action) {
        return delegate.whenComplete(action);
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<T> whenCompleteAsync(BiConsumer<? super T, ? super Throwable> action) {
        return delegate.whenCompleteAsync(action);
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<T> whenCompleteAsync(BiConsumer<? super T, ? super Throwable> action, Executor executor) {
        return delegate.whenCompleteAsync(action, executor);
    }

    /** {@inheritDoc} */
    @Override
    public <U> CompletableFuture<U> handle(BiFunction<? super T, Throwable, ? extends U> fn) {
        return delegate.handle(fn);
    }

    /** {@inheritDoc} */
    @Override
    public <U> CompletableFuture<U> handleAsync(BiFunction<? super T, Throwable, ? extends U> fn) {
        return delegate.handleAsync(fn);
    }

    /** {@inheritDoc} */
    @Override
    public <U> CompletableFuture<U> handleAsync(BiFunction<? super T, Throwable, ? extends U> fn, Executor executor) {
        return delegate.handleAsync(fn, executor);
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<T> toCompletableFuture() {
        return delegate.toCompletableFuture();
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<T> exceptionally(Function<Throwable, ? extends T> fn) {
        return delegate.exceptionally(fn);
    }

    /** {@inheritDoc} */
    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        return delegate.cancel(mayInterruptIfRunning);
    }

    /** {@inheritDoc} */
    @Override
    public boolean isCancelled() {
        return delegate.isCancelled();
    }

    /** {@inheritDoc} */
    @Override
    public boolean isCompletedExceptionally() {
        return delegate.isCompletedExceptionally();
    }

    /** {@inheritDoc} */
    @Override
    public void obtrudeValue(T value) {
        delegate.obtrudeValue(value);
    }

    /** {@inheritDoc} */
    @Override
    public void obtrudeException(Throwable ex) {
        delegate.obtrudeException(ex);
    }

    /** {@inheritDoc} */
    @Override
    public int getNumberOfDependents() {
        return delegate.getNumberOfDependents();
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return delegate.toString();
    }

    /** {@inheritDoc} */
    @Override
    public <U> CompletableFuture<U> newIncompleteFuture() {
        return delegate.newIncompleteFuture();
    }

    /** {@inheritDoc} */
    @Override
    public Executor defaultExecutor() {
        return delegate.defaultExecutor();
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<T> copy() {
        return delegate.copy();
    }

    /** {@inheritDoc} */
    @Override
    public CompletionStage<T> minimalCompletionStage() {
        return delegate.minimalCompletionStage();
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<T> completeAsync(Supplier<? extends T> supplier, Executor executor) {
        return delegate.completeAsync(supplier, executor);
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<T> completeAsync(Supplier<? extends T> supplier) {
        return delegate.completeAsync(supplier);
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<T> orTimeout(long timeout, TimeUnit unit) {
        return delegate.orTimeout(timeout, unit);
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<T> completeOnTimeout(T value, long timeout, TimeUnit unit) {
        return delegate.completeOnTimeout(value, timeout, unit);
    }
}
