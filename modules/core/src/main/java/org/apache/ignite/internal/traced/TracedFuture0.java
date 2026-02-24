package org.apache.ignite.internal.traced;

import static org.apache.ignite.internal.util.ExceptionUtils.sneakyThrow;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.UnknownNullability;

public class TracedFuture0<T> extends CompletableFuture<T> {
    private final TraceContext traceContext;
    private final CompletableFuture<T> delegate;

    public TracedFuture0(CompletableFuture<T> delegate) {
        this(delegate, new TraceContext());
    }

    private TracedFuture0(CompletableFuture<T> delegate, TraceContext traceContext) {
        this.delegate = delegate;
        this.traceContext = traceContext;

        delegate.whenComplete((v, e) -> {
            var traceEx = e == null ? null : new TracedException(traceContext.traceEntries(), e);

            if (traceEx != null) {
                this.completeExceptionally(traceEx);
            } else {
                this.complete(v);
            }
        });
    }

    public static <T> TracedFuture0<T> wrapFuture(CompletableFuture<T> delegate) {
        return new TracedFuture0<T>(delegate);
    }

    public static <T> TracedFuture0<T> continueWithContext(CompletableFuture<T> delegate, TraceContext context) {
        return new TracedFuture0<T>(delegate, context);
    }

    public TracedFuture0<T> section(String name) {
        return new TracedFuture0<T>(delegate.handle((v, t) -> {
            traceContext.startTrace(name, t);

            if (t != null) {
                sneakyThrow(t);
            }

            return v;
        }), traceContext);
    }

    public <U> TracedFuture0<U> withContext(Function<TraceContextWithValue<T>, U> fn) {
        return new TracedFuture0<>(delegate.thenApply(v -> {
            var traceContextWithValue = new TraceContextWithValue<>(traceContext, v, null);
            return fn.apply(traceContextWithValue);
        }), traceContext);
    }

    public TracedFuture0<T> completeWithValueAndContext(T value, TraceContext traceContext) {
        this.traceContext.addChildToLastEntry(traceContext);
        delegate.complete(value);
        return this;
    }

    @Override
    public @NotNull <U> TracedFuture0<U> thenApply(@NotNull Function<? super T, ? extends U> fn) {
        return new TracedFuture0<>(delegate.thenApply(fn), traceContext);
    }

    @Override
    public @NotNull <U> TracedFuture0<U> thenApplyAsync(@NotNull Function<? super T, ? extends U> fn) {
        return new TracedFuture0<>(delegate.thenApplyAsync(fn), traceContext);
    }

    @Override
    public @NotNull <U> TracedFuture0<U> thenApplyAsync(@NotNull Function<? super T, ? extends U> fn, Executor executor) {
        return new TracedFuture0<>(delegate.thenApplyAsync(fn, executor), traceContext);
    }

    @Override
    public @NotNull TracedFuture0<Void> thenAccept(@NotNull Consumer<? super T> action) {
        return new TracedFuture0<>(delegate.thenAccept(action), traceContext);
    }

    @Override
    public @NotNull TracedFuture0<Void> thenAcceptAsync(@NotNull Consumer<? super T> action) {
        return new TracedFuture0<>(delegate.thenAcceptAsync(action), traceContext);
    }

    @Override
    public @NotNull TracedFuture0<Void> thenAcceptAsync(@NotNull Consumer<? super T> action, Executor executor) {
        return new TracedFuture0<>(delegate.thenAcceptAsync(action, executor), traceContext);
    }

    @Override
    public @NotNull TracedFuture0<Void> thenRun(@NotNull Runnable action) {
        return new TracedFuture0<>(delegate.thenRun(action), traceContext);
    }

    @Override
    public @NotNull TracedFuture0<Void> thenRunAsync(@NotNull Runnable action) {
        return new TracedFuture0<>(delegate.thenRunAsync(action), traceContext);
    }

    @Override
    public @NotNull TracedFuture0<Void> thenRunAsync(@NotNull Runnable action, Executor executor) {
        return new TracedFuture0<>(delegate.thenRunAsync(action, executor), traceContext);
    }

    @Override
    public @NotNull <U, V> TracedFuture0<V> thenCombine(@NotNull CompletionStage<? extends U> other,
            @NotNull BiFunction<? super T, ? super U, ? extends V> fn) {
        return new TracedFuture0<>(delegate.thenCombine(other, fn), traceContext);
    }

    @Override
    public @NotNull <U, V> TracedFuture0<V> thenCombineAsync(@NotNull CompletionStage<? extends U> other,
            @NotNull BiFunction<? super T, ? super U, ? extends V> fn) {
        return new TracedFuture0<>(delegate.thenCombineAsync(other, fn), traceContext);
    }

    @Override
    public @NotNull <U, V> TracedFuture0<V> thenCombineAsync(@NotNull CompletionStage<? extends U> other,
            @NotNull BiFunction<? super T, ? super U, ? extends V> fn, Executor executor) {
        return new TracedFuture0<>(delegate.thenCombineAsync(other, fn, executor), traceContext);
    }

    @Override
    public @NotNull <U> TracedFuture0<Void> thenAcceptBoth(@NotNull CompletionStage<? extends U> other,
            @NotNull BiConsumer<? super T, ? super U> action) {
        return new TracedFuture0<>(delegate.thenAcceptBoth(other, action), traceContext);
    }

    @Override
    public @NotNull <U> TracedFuture0<Void> thenAcceptBothAsync(@NotNull CompletionStage<? extends U> other,
            @NotNull BiConsumer<? super T, ? super U> action) {
        return new TracedFuture0<>(delegate.thenAcceptBothAsync(other, action), traceContext);
    }

    @Override
    public @NotNull <U> TracedFuture0<Void> thenAcceptBothAsync(@NotNull CompletionStage<? extends U> other,
            @NotNull BiConsumer<? super T, ? super U> action, Executor executor) {
        return new TracedFuture0<>(delegate.thenAcceptBothAsync(other, action, executor), traceContext);
    }

    @Override
    public @NotNull TracedFuture0<Void> runAfterBoth(@NotNull CompletionStage<?> other, @NotNull Runnable action) {
        return new TracedFuture0<>(delegate.runAfterBoth(other, action), traceContext);
    }

    @Override
    public @NotNull TracedFuture0<Void> runAfterBothAsync(@NotNull CompletionStage<?> other, @NotNull Runnable action) {
        return new TracedFuture0<>(delegate.runAfterBothAsync(other, action), traceContext);
    }

    @Override
    public @NotNull TracedFuture0<Void> runAfterBothAsync(@NotNull CompletionStage<?> other, @NotNull Runnable action,
            Executor executor) {
        return new TracedFuture0<>(delegate.runAfterBothAsync(other, action, executor), traceContext);
    }

    @Override
    public @NotNull <U> TracedFuture0<U> applyToEither(@NotNull CompletionStage<? extends T> other, @NotNull Function<? super T, U> fn) {
        return new TracedFuture0<>(delegate.applyToEither(other, fn), traceContext);
    }

    @Override
    public @NotNull <U> TracedFuture0<U> applyToEitherAsync(@NotNull CompletionStage<? extends T> other,
            @NotNull Function<? super T, U> fn) {
        return new TracedFuture0<>(delegate.applyToEitherAsync(other, fn), traceContext);
    }

    @Override
    public @NotNull <U> TracedFuture0<U> applyToEitherAsync(@NotNull CompletionStage<? extends T> other,
            @NotNull Function<? super T, U> fn, Executor executor) {
        return new TracedFuture0<>(delegate.applyToEitherAsync(other, fn, executor), traceContext);
    }

    @Override
    public @NotNull TracedFuture0<Void> acceptEither(@NotNull CompletionStage<? extends T> other, @NotNull Consumer<? super T> action) {
        return new TracedFuture0<>(delegate.acceptEither(other, action), traceContext);
    }

    @Override
    public @NotNull TracedFuture0<Void> acceptEitherAsync(@NotNull CompletionStage<? extends T> other,
            @NotNull Consumer<? super T> action) {
        return new TracedFuture0<>(delegate.acceptEitherAsync(other, action), traceContext);
    }

    @Override
    public @NotNull TracedFuture0<Void> acceptEitherAsync(@NotNull CompletionStage<? extends T> other,
            @NotNull Consumer<? super T> action, Executor executor) {
        return new TracedFuture0<>(delegate.acceptEitherAsync(other, action, executor), traceContext);
    }

    @Override
    public @NotNull TracedFuture0<Void> runAfterEither(@NotNull CompletionStage<?> other, @NotNull Runnable action) {
        return new TracedFuture0<>(delegate.runAfterEither(other, action), traceContext);
    }

    @Override
    public @NotNull TracedFuture0<Void> runAfterEitherAsync(@NotNull CompletionStage<?> other, @NotNull Runnable action) {
        return new TracedFuture0<>(delegate.runAfterEitherAsync(other, action), traceContext);
    }

    @Override
    public @NotNull TracedFuture0<Void> runAfterEitherAsync(@NotNull CompletionStage<?> other, @NotNull Runnable action,
            Executor executor) {
        return new TracedFuture0<>(delegate.runAfterEitherAsync(other, action, executor), traceContext);
    }

    @Override
    public @NotNull <U> TracedFuture0<U> thenCompose(@NotNull Function<? super T, ? extends CompletionStage<U>> fn) {
        Function<? super T, ? extends CompletionStage<U>> fn0 = v -> {
            var future = fn.apply(v);
            return checkFutureTraced(future);
        };

        return new TracedFuture0<>(delegate.thenCompose(fn0), traceContext);
    }

    @Override
    public @NotNull <U> TracedFuture0<U> thenComposeAsync(@NotNull Function<? super T, ? extends CompletionStage<U>> fn) {
        Function<? super T, ? extends CompletionStage<U>> fn0 = v -> {
            var future = fn.apply(v);
            return checkFutureTraced(future);
        };

        return new TracedFuture0<>(delegate.thenComposeAsync(fn0), traceContext);
    }

    @Override
    public @NotNull <U> TracedFuture0<U> thenComposeAsync(@NotNull Function<? super T, ? extends CompletionStage<U>> fn,
            Executor executor) {
        Function<? super T, ? extends CompletionStage<U>> fn0 = v -> {
            var future = fn.apply(v);
            return checkFutureTraced(future);
        };

        return new TracedFuture0<>(delegate.thenComposeAsync(fn0, executor), traceContext);
    }

    private <U> CompletionStage<U> checkFutureTraced(CompletionStage<U> future) {
        if (future instanceof TracedFuture0) {
            future.whenComplete((v, e) -> {
                var c = ((TracedFuture0<?>) future).traceContext;

                if (c != traceContext) {
                    traceContext.addChildToLastEntry(c);
                }
            });
        }

        return future;
    }

    @Override
    public @NotNull <U> TracedFuture0<U> handle(@NotNull BiFunction<? super T, Throwable, ? extends U> fn) {
        CompletableFuture<U> fut = delegate.handle((v, e) -> {
            var c = e instanceof CompletionException
                    ? e.getCause()
                    : e;

            var traceEx = e == null ? null : new TracedException(traceContext.traceEntries(), c);

            return fn.apply(v, traceEx);
        });

        return new TracedFuture0<>(fut, traceContext);
    }

    @Override
    public @NotNull <U> TracedFuture0<U> handleAsync(@NotNull BiFunction<? super T, Throwable, ? extends U> fn) {
        CompletableFuture<U> fut = delegate.handleAsync((v, e) -> {
            var c = e instanceof CompletionException
                    ? e.getCause()
                    : e;

            var traceEx = e == null ? null : new TracedException(traceContext.traceEntries(), c);

            return fn.apply(v, traceEx);
        });

        return new TracedFuture0<>(fut, traceContext);
    }

    @Override
    public @NotNull <U> TracedFuture0<U> handleAsync(@NotNull BiFunction<? super T, Throwable, ? extends U> fn, Executor executor) {
        CompletableFuture<U> fut = delegate.handleAsync((v, e) -> {
            var c = e instanceof CompletionException
                    ? e.getCause()
                    : e;

            var traceEx = e == null ? null : new TracedException(traceContext.traceEntries(), c);

            return fn.apply(v, traceEx);
        }, executor);

        return new TracedFuture0<>(fut, traceContext);
    }

    @Override
    public @NotNull TracedFuture0<T> whenComplete(
            @NotNull BiConsumer<? super @UnknownNullability T, ? super @UnknownNullability Throwable> action) {
        var fut = delegate.whenComplete((v, e) -> {
            var c = e instanceof CompletionException
                    ? e.getCause()
                    : e;

            var traceEx = e == null ? null : new TracedException(traceContext.traceEntries(), c);

            action.accept(v, traceEx);
        });

        return new TracedFuture0<>(fut, traceContext);
    }

    @Override
    public @NotNull TracedFuture0<T> whenCompleteAsync(
            @NotNull BiConsumer<? super @UnknownNullability T, ? super @UnknownNullability Throwable> action) {
        var fut = delegate.whenCompleteAsync((v, e) -> {
            var c = e instanceof CompletionException
                    ? e.getCause()
                    : e;

            var traceEx = e == null ? null : new TracedException(traceContext.traceEntries(), c);

            action.accept(v, traceEx);
        });

        return new TracedFuture0<>(fut, traceContext);
    }

    @Override
    public @NotNull TracedFuture0<T> whenCompleteAsync(
            @NotNull BiConsumer<? super @UnknownNullability T, ? super @UnknownNullability Throwable> action, Executor executor) {
        var fut = delegate.whenCompleteAsync((v, e) -> {
            var c = e instanceof CompletionException
                    ? e.getCause()
                    : e;

            var traceEx = e == null ? null : new TracedException(traceContext.traceEntries(), c);

            action.accept(v, traceEx);
        }, executor);

        return new TracedFuture0<>(fut, traceContext);
    }

    @Override
    public @NotNull TracedFuture0<T> exceptionally(@NotNull Function<Throwable, ? extends T> fn) {
        return new TracedFuture0<>(delegate.exceptionally(fn), traceContext);
    }

    @Override
    public @NotNull CompletableFuture<T> toCompletableFuture() {
        CompletableFuture<T> future = new CompletableFuture<>();

        this.whenComplete((v, e) -> {
            if (e == null) {
                future.complete(v);
            } else {
                future.completeExceptionally(e);
            }
        });

        return future;
    }

    @Override
    public boolean isDone() {
        return delegate.isDone();
    }

    @Override
    public boolean isCancelled() {
        return delegate.isCancelled();
    }

    @Override
    public boolean isCompletedExceptionally() {
        return delegate.isCompletedExceptionally();
    }
}
