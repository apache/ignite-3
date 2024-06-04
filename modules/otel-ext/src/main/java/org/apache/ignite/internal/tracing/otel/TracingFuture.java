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

package org.apache.ignite.internal.tracing.otel;

import io.opentelemetry.context.Context;
import java.util.concurrent.CompletableFuture;
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
 * A analogue of {@link CompletableFuture} that preserved tracing context.
 *
 * @param <T> The result type returned by this future's join and get methods.
 * @see CompletableFuture
 */
public class TracingFuture<T> extends CompletableFuture<T> {
    private final CompletableFuture<T> fut;

    public TracingFuture(CompletableFuture<T> fut) {
        this.fut = fut;
    }

    @Override
    public boolean isDone() {
        return fut.isDone();
    }

    @Override
    public T get() throws InterruptedException, ExecutionException {
        return fut.get();
    }

    @Override
    public T get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        return fut.get(timeout, unit);
    }

    @Override
    public T join() {
        return fut.join();
    }

    @Override
    public T getNow(T valueIfAbsent) {
        return fut.getNow(valueIfAbsent);
    }

    @Override
    public boolean complete(T value) {
        return fut.complete(value);
    }

    @Override
    public boolean completeExceptionally(Throwable ex) {
        return fut.completeExceptionally(ex);
    }

    @Override
    public <U> CompletableFuture<U> thenApply(Function<? super T, ? extends U> fn) {
        return fut.thenApply(Context.current().wrapFunction(fn));
    }

    @Override
    public <U> CompletableFuture<U> thenApplyAsync(Function<? super T, ? extends U> fn) {
        return fut.thenApplyAsync(Context.current().wrapFunction(fn));
    }

    @Override
    public <U> CompletableFuture<U> thenApplyAsync(Function<? super T, ? extends U> fn, Executor executor) {
        return fut.thenApplyAsync(Context.current().wrapFunction(fn), executor);
    }

    @Override
    public CompletableFuture<Void> thenAccept(Consumer<? super T> action) {
        return fut.thenAccept(Context.current().wrapConsumer(action));
    }

    @Override
    public CompletableFuture<Void> thenAcceptAsync(Consumer<? super T> action) {
        return fut.thenAcceptAsync(Context.current().wrapConsumer(action));
    }

    @Override
    public CompletableFuture<Void> thenAcceptAsync(Consumer<? super T> action, Executor executor) {
        return fut.thenAcceptAsync(Context.current().wrapConsumer(action), executor);
    }

    @Override
    public CompletableFuture<Void> thenRun(Runnable action) {
        return fut.thenRun(Context.current().wrap(action));
    }

    @Override
    public CompletableFuture<Void> thenRunAsync(Runnable action) {
        return fut.thenRunAsync(Context.current().wrap(action));
    }

    @Override
    public CompletableFuture<Void> thenRunAsync(Runnable action, Executor executor) {
        return fut.thenRunAsync(Context.current().wrap(action), executor);
    }

    @Override
    public <U, V> CompletableFuture<V> thenCombine(CompletionStage<? extends U> other, BiFunction<? super T, ? super U, ? extends V> fn) {
        return fut.thenCombine(other, Context.current().wrapFunction(fn));
    }

    @Override
    public <U, V> CompletableFuture<V> thenCombineAsync(CompletionStage<? extends U> other,
            BiFunction<? super T, ? super U, ? extends V> fn) {
        return fut.thenCombineAsync(other, Context.current().wrapFunction(fn));
    }

    @Override
    public <U, V> CompletableFuture<V> thenCombineAsync(CompletionStage<? extends U> other,
            BiFunction<? super T, ? super U, ? extends V> fn, Executor executor) {
        return fut.thenCombineAsync(other, Context.current().wrapFunction(fn), executor);
    }

    @Override
    public <U> CompletableFuture<Void> thenAcceptBoth(CompletionStage<? extends U> other, BiConsumer<? super T, ? super U> action) {
        return fut.thenAcceptBoth(other, Context.current().wrapConsumer(action));
    }

    @Override
    public <U> CompletableFuture<Void> thenAcceptBothAsync(CompletionStage<? extends U> other, BiConsumer<? super T, ? super U> action) {
        return fut.thenAcceptBothAsync(other, Context.current().wrapConsumer(action));
    }

    @Override
    public <U> CompletableFuture<Void> thenAcceptBothAsync(CompletionStage<? extends U> other, BiConsumer<? super T, ? super U> action,
            Executor executor) {
        return fut.thenAcceptBothAsync(other, Context.current().wrapConsumer(action), executor);
    }

    @Override
    public CompletableFuture<Void> runAfterBoth(CompletionStage<?> other, Runnable action) {
        return fut.runAfterBoth(other, Context.current().wrap(action));
    }

    @Override
    public CompletableFuture<Void> runAfterBothAsync(CompletionStage<?> other, Runnable action) {
        return fut.runAfterBothAsync(other, Context.current().wrap(action));
    }

    @Override
    public CompletableFuture<Void> runAfterBothAsync(CompletionStage<?> other, Runnable action, Executor executor) {
        return fut.runAfterBothAsync(other, Context.current().wrap(action), executor);
    }

    @Override
    public <U> CompletableFuture<U> applyToEither(CompletionStage<? extends T> other, Function<? super T, U> fn) {
        return fut.applyToEither(other, Context.current().wrapFunction(fn));
    }

    @Override
    public <U> CompletableFuture<U> applyToEitherAsync(CompletionStage<? extends T> other, Function<? super T, U> fn) {
        return fut.applyToEitherAsync(other, Context.current().wrapFunction(fn));
    }

    @Override
    public <U> CompletableFuture<U> applyToEitherAsync(CompletionStage<? extends T> other, Function<? super T, U> fn, Executor executor) {
        return fut.applyToEitherAsync(other, Context.current().wrapFunction(fn), executor);
    }

    @Override
    public CompletableFuture<Void> acceptEither(CompletionStage<? extends T> other, Consumer<? super T> action) {
        return fut.acceptEither(other, Context.current().wrapConsumer(action));
    }

    @Override
    public CompletableFuture<Void> acceptEitherAsync(CompletionStage<? extends T> other, Consumer<? super T> action) {
        return fut.acceptEitherAsync(other, Context.current().wrapConsumer(action));
    }

    @Override
    public CompletableFuture<Void> acceptEitherAsync(CompletionStage<? extends T> other, Consumer<? super T> action, Executor executor) {
        return fut.acceptEitherAsync(other, Context.current().wrapConsumer(action), executor);
    }

    @Override
    public CompletableFuture<Void> runAfterEither(CompletionStage<?> other, Runnable action) {
        return fut.runAfterEither(other, Context.current().wrap(action));
    }

    @Override
    public CompletableFuture<Void> runAfterEitherAsync(CompletionStage<?> other, Runnable action) {
        return fut.runAfterEitherAsync(other, Context.current().wrap(action));
    }

    @Override
    public CompletableFuture<Void> runAfterEitherAsync(CompletionStage<?> other, Runnable action, Executor executor) {
        return fut.runAfterEitherAsync(other, Context.current().wrap(action), executor);
    }

    @Override
    public <U> CompletableFuture<U> thenCompose(Function<? super T, ? extends CompletionStage<U>> fn) {
        return fut.thenCompose(Context.current().wrapFunction(fn));
    }

    @Override
    public <U> CompletableFuture<U> thenComposeAsync(Function<? super T, ? extends CompletionStage<U>> fn) {
        return fut.thenComposeAsync(Context.current().wrapFunction(fn));
    }

    @Override
    public <U> CompletableFuture<U> thenComposeAsync(Function<? super T, ? extends CompletionStage<U>> fn, Executor executor) {
        return fut.thenComposeAsync(Context.current().wrapFunction(fn), executor);
    }

    @Override
    public CompletableFuture<T> whenComplete(BiConsumer<? super T, ? super Throwable> action) {
        return fut.whenComplete(Context.current().wrapConsumer(action));
    }

    @Override
    public CompletableFuture<T> whenCompleteAsync(BiConsumer<? super T, ? super Throwable> action) {
        return fut.whenCompleteAsync(Context.current().wrapConsumer(action));
    }

    @Override
    public CompletableFuture<T> whenCompleteAsync(BiConsumer<? super T, ? super Throwable> action, Executor executor) {
        return fut.whenCompleteAsync(Context.current().wrapConsumer(action), executor);
    }

    @Override
    public <U> CompletableFuture<U> handle(BiFunction<? super T, Throwable, ? extends U> fn) {
        return fut.handle(Context.current().wrapFunction(fn));
    }

    @Override
    public <U> CompletableFuture<U> handleAsync(BiFunction<? super T, Throwable, ? extends U> fn) {
        return fut.handleAsync(Context.current().wrapFunction(fn));
    }

    @Override
    public <U> CompletableFuture<U> handleAsync(BiFunction<? super T, Throwable, ? extends U> fn, Executor executor) {
        return fut.handleAsync(Context.current().wrapFunction(fn), executor);
    }

    @Override
    public CompletableFuture<T> toCompletableFuture() {
        return fut.toCompletableFuture();
    }

    @Override
    public CompletableFuture<T> exceptionally(Function<Throwable, ? extends T> fn) {
        return fut.exceptionally(fn);
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        return fut.cancel(mayInterruptIfRunning);
    }

    @Override
    public boolean isCancelled() {
        return fut.isCancelled();
    }

    @Override
    public boolean isCompletedExceptionally() {
        return fut.isCompletedExceptionally();
    }

    @Override
    public void obtrudeValue(T value) {
        fut.obtrudeValue(value);
    }

    @Override
    public void obtrudeException(Throwable ex) {
        fut.obtrudeException(ex);
    }

    @Override
    public int getNumberOfDependents() {
        return fut.getNumberOfDependents();
    }

    @Override
    public String toString() {
        return fut.toString();
    }

    @Override
    public <U> CompletableFuture<U> newIncompleteFuture() {
        return fut.newIncompleteFuture();
    }

    @Override
    public Executor defaultExecutor() {
        return fut.defaultExecutor();
    }

    @Override
    public CompletableFuture<T> copy() {
        return fut.copy();
    }

    @Override
    public CompletionStage<T> minimalCompletionStage() {
        return fut.minimalCompletionStage();
    }

    @Override
    public CompletableFuture<T> completeAsync(Supplier<? extends T> supplier, Executor executor) {
        return fut.completeAsync(supplier, executor);
    }

    @Override
    public CompletableFuture<T> completeAsync(Supplier<? extends T> supplier) {
        return fut.completeAsync(supplier);
    }

    @Override
    public CompletableFuture<T> orTimeout(long timeout, TimeUnit unit) {
        return fut.orTimeout(timeout, unit);
    }

    @Override
    public CompletableFuture<T> completeOnTimeout(T value, long timeout, TimeUnit unit) {
        return fut.completeOnTimeout(value, timeout, unit);
    }
}
