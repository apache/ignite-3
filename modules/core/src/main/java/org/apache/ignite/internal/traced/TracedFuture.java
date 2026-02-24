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

public class TracedFuture<T> implements CompletionStage<T> {
    private final TraceContext traceContext;
    private final CompletableFuture<T> delegate;

    public TracedFuture(CompletableFuture<T> delegate) {
        this(delegate, new TraceContext());
    }

    private TracedFuture(CompletableFuture<T> delegate, TraceContext traceContext) {
        this.delegate = delegate;
        this.traceContext = traceContext;
    }

    public static <T> TracedFuture<T> wrapFuture(CompletableFuture<T> delegate) {
        return new TracedFuture<T>(delegate);
    }

    public static <T> TracedFuture<T> continueWithContext(CompletableFuture<T> delegate, TraceContext context) {
        return new TracedFuture<T>(delegate, context);
    }

    public TracedFuture<T> section(String name) {
        return new TracedFuture<T>(delegate.handle((v, t) -> {
            traceContext.startTrace(name, t);

            if (t != null) {
                sneakyThrow(t);
            }

            return v;
        }), traceContext);
    }

    public <U> TracedFuture<U> withContext(Function<TraceContextWithValue<T>, U> fn) {
        return new TracedFuture<>(delegate.thenApply(v -> {
            var traceContextWithValue = new TraceContextWithValue<>(traceContext, v, null);
            return fn.apply(traceContextWithValue);
        }), traceContext);
    }

    @Override
    public @NotNull <U> TracedFuture<U> thenApply(@NotNull Function<? super T, ? extends U> fn) {
        return new TracedFuture<>(delegate.thenApply(fn), traceContext);
    }

    @Override
    public @NotNull <U> TracedFuture<U> thenApplyAsync(@NotNull Function<? super T, ? extends U> fn) {
        return new TracedFuture<>(delegate.thenApplyAsync(fn), traceContext);
    }

    @Override
    public @NotNull <U> TracedFuture<U> thenApplyAsync(@NotNull Function<? super T, ? extends U> fn, Executor executor) {
        return new TracedFuture<>(delegate.thenApplyAsync(fn, executor), traceContext);
    }

    @Override
    public @NotNull TracedFuture<Void> thenAccept(@NotNull Consumer<? super T> action) {
        return new TracedFuture<>(delegate.thenAccept(action), traceContext);
    }

    @Override
    public @NotNull TracedFuture<Void> thenAcceptAsync(@NotNull Consumer<? super T> action) {
        return new TracedFuture<>(delegate.thenAcceptAsync(action), traceContext);
    }

    @Override
    public @NotNull TracedFuture<Void> thenAcceptAsync(@NotNull Consumer<? super T> action, Executor executor) {
        return new TracedFuture<>(delegate.thenAcceptAsync(action, executor), traceContext);
    }

    @Override
    public @NotNull TracedFuture<Void> thenRun(@NotNull Runnable action) {
        return new TracedFuture<>(delegate.thenRun(action), traceContext);
    }

    @Override
    public @NotNull TracedFuture<Void> thenRunAsync(@NotNull Runnable action) {
        return new TracedFuture<>(delegate.thenRunAsync(action), traceContext);
    }

    @Override
    public @NotNull TracedFuture<Void> thenRunAsync(@NotNull Runnable action, Executor executor) {
        return new TracedFuture<>(delegate.thenRunAsync(action, executor), traceContext);
    }

    @Override
    public @NotNull <U, V> TracedFuture<V> thenCombine(@NotNull CompletionStage<? extends U> other,
            @NotNull BiFunction<? super T, ? super U, ? extends V> fn) {
        return new TracedFuture<>(delegate.thenCombine(other, fn), traceContext);
    }

    @Override
    public @NotNull <U, V> TracedFuture<V> thenCombineAsync(@NotNull CompletionStage<? extends U> other,
            @NotNull BiFunction<? super T, ? super U, ? extends V> fn) {
        return new TracedFuture<>(delegate.thenCombineAsync(other, fn), traceContext);
    }

    @Override
    public @NotNull <U, V> TracedFuture<V> thenCombineAsync(@NotNull CompletionStage<? extends U> other,
            @NotNull BiFunction<? super T, ? super U, ? extends V> fn, Executor executor) {
        return new TracedFuture<>(delegate.thenCombineAsync(other, fn, executor), traceContext);
    }

    @Override
    public @NotNull <U> TracedFuture<Void> thenAcceptBoth(@NotNull CompletionStage<? extends U> other,
            @NotNull BiConsumer<? super T, ? super U> action) {
        return new TracedFuture<>(delegate.thenAcceptBoth(other, action), traceContext);
    }

    @Override
    public @NotNull <U> TracedFuture<Void> thenAcceptBothAsync(@NotNull CompletionStage<? extends U> other,
            @NotNull BiConsumer<? super T, ? super U> action) {
        return new TracedFuture<>(delegate.thenAcceptBothAsync(other, action), traceContext);
    }

    @Override
    public @NotNull <U> TracedFuture<Void> thenAcceptBothAsync(@NotNull CompletionStage<? extends U> other,
            @NotNull BiConsumer<? super T, ? super U> action, Executor executor) {
        return new TracedFuture<>(delegate.thenAcceptBothAsync(other, action, executor), traceContext);
    }

    @Override
    public @NotNull TracedFuture<Void> runAfterBoth(@NotNull CompletionStage<?> other, @NotNull Runnable action) {
        return new TracedFuture<>(delegate.runAfterBoth(other, action), traceContext);
    }

    @Override
    public @NotNull TracedFuture<Void> runAfterBothAsync(@NotNull CompletionStage<?> other, @NotNull Runnable action) {
        return new TracedFuture<>(delegate.runAfterBothAsync(other, action), traceContext);
    }

    @Override
    public @NotNull TracedFuture<Void> runAfterBothAsync(@NotNull CompletionStage<?> other, @NotNull Runnable action,
            Executor executor) {
        return new TracedFuture<>(delegate.runAfterBothAsync(other, action, executor), traceContext);
    }

    @Override
    public @NotNull <U> TracedFuture<U> applyToEither(@NotNull CompletionStage<? extends T> other, @NotNull Function<? super T, U> fn) {
        return new TracedFuture<>(delegate.applyToEither(other, fn), traceContext);
    }

    @Override
    public @NotNull <U> TracedFuture<U> applyToEitherAsync(@NotNull CompletionStage<? extends T> other,
            @NotNull Function<? super T, U> fn) {
        return new TracedFuture<>(delegate.applyToEitherAsync(other, fn), traceContext);
    }

    @Override
    public @NotNull <U> TracedFuture<U> applyToEitherAsync(@NotNull CompletionStage<? extends T> other,
            @NotNull Function<? super T, U> fn, Executor executor) {
        return new TracedFuture<>(delegate.applyToEitherAsync(other, fn, executor), traceContext);
    }

    @Override
    public @NotNull TracedFuture<Void> acceptEither(@NotNull CompletionStage<? extends T> other, @NotNull Consumer<? super T> action) {
        return new TracedFuture<>(delegate.acceptEither(other, action), traceContext);
    }

    @Override
    public @NotNull TracedFuture<Void> acceptEitherAsync(@NotNull CompletionStage<? extends T> other,
            @NotNull Consumer<? super T> action) {
        return new TracedFuture<>(delegate.acceptEitherAsync(other, action), traceContext);
    }

    @Override
    public @NotNull TracedFuture<Void> acceptEitherAsync(@NotNull CompletionStage<? extends T> other,
            @NotNull Consumer<? super T> action, Executor executor) {
        return new TracedFuture<>(delegate.acceptEitherAsync(other, action, executor), traceContext);
    }

    @Override
    public @NotNull TracedFuture<Void> runAfterEither(@NotNull CompletionStage<?> other, @NotNull Runnable action) {
        return new TracedFuture<>(delegate.runAfterEither(other, action), traceContext);
    }

    @Override
    public @NotNull TracedFuture<Void> runAfterEitherAsync(@NotNull CompletionStage<?> other, @NotNull Runnable action) {
        return new TracedFuture<>(delegate.runAfterEitherAsync(other, action), traceContext);
    }

    @Override
    public @NotNull TracedFuture<Void> runAfterEitherAsync(@NotNull CompletionStage<?> other, @NotNull Runnable action,
            Executor executor) {
        return new TracedFuture<>(delegate.runAfterEitherAsync(other, action, executor), traceContext);
    }

    @Override
    public @NotNull <U> TracedFuture<U> thenCompose(@NotNull Function<? super T, ? extends CompletionStage<U>> fn) {
        Function<? super T, ? extends CompletionStage<U>> fn0 = v -> {
            var future = fn.apply(v);
            return checkFutureTraced(future);
        };

        return new TracedFuture<>(delegate.thenCompose(fn0), traceContext);
    }

    @Override
    public @NotNull <U> TracedFuture<U> thenComposeAsync(@NotNull Function<? super T, ? extends CompletionStage<U>> fn) {
        Function<? super T, ? extends CompletionStage<U>> fn0 = v -> {
            var future = fn.apply(v);
            return checkFutureTraced(future);
        };

        return new TracedFuture<>(delegate.thenComposeAsync(fn0), traceContext);
    }

    @Override
    public @NotNull <U> TracedFuture<U> thenComposeAsync(@NotNull Function<? super T, ? extends CompletionStage<U>> fn,
            Executor executor) {
        Function<? super T, ? extends CompletionStage<U>> fn0 = v -> {
            var future = fn.apply(v);
            return checkFutureTraced(future);
        };

        return new TracedFuture<>(delegate.thenComposeAsync(fn0, executor), traceContext);
    }

    private <U> CompletionStage<U> checkFutureTraced(CompletionStage<U> future) {
        if (future instanceof TracedFuture) {
            future.whenComplete((v, e) -> {
                var c = ((TracedFuture<?>) future).traceContext;

                if (c != traceContext) {
                    traceContext.addChildToLastEntry(c);
                }
            });
        }

        return future;
    }

    @Override
    public @NotNull <U> TracedFuture<U> handle(@NotNull BiFunction<? super T, Throwable, ? extends U> fn) {
        CompletableFuture<U> fut = delegate.handle((v, e) -> {
            var c = e instanceof CompletionException
                    ? e.getCause()
                    : e;

            var traceEx = e == null ? null : new TracedException(traceContext.traceEntries(), c);

            return fn.apply(v, traceEx);
        });

        return new TracedFuture<>(fut, traceContext);
    }

    @Override
    public @NotNull <U> TracedFuture<U> handleAsync(@NotNull BiFunction<? super T, Throwable, ? extends U> fn) {
        CompletableFuture<U> fut = delegate.handleAsync((v, e) -> {
            var c = e instanceof CompletionException
                    ? e.getCause()
                    : e;

            var traceEx = e == null ? null : new TracedException(traceContext.traceEntries(), c);

            return fn.apply(v, traceEx);
        });

        return new TracedFuture<>(fut, traceContext);
    }

    @Override
    public @NotNull <U> TracedFuture<U> handleAsync(@NotNull BiFunction<? super T, Throwable, ? extends U> fn, Executor executor) {
        CompletableFuture<U> fut = delegate.handleAsync((v, e) -> {
            var c = e instanceof CompletionException
                    ? e.getCause()
                    : e;

            var traceEx = e == null ? null : new TracedException(traceContext.traceEntries(), c);

            return fn.apply(v, traceEx);
        }, executor);

        return new TracedFuture<>(fut, traceContext);
    }

    @Override
    public @NotNull TracedFuture<T> whenComplete(
            @NotNull BiConsumer<? super @UnknownNullability T, ? super @UnknownNullability Throwable> action) {
        var fut = delegate.whenComplete((v, e) -> {
            var c = e instanceof CompletionException
                    ? e.getCause()
                    : e;

            var traceEx = e == null ? null : new TracedException(traceContext.traceEntries(), c);

            action.accept(v, traceEx);
        });

        return new TracedFuture<>(fut, traceContext);
    }

    @Override
    public @NotNull TracedFuture<T> whenCompleteAsync(
            @NotNull BiConsumer<? super @UnknownNullability T, ? super @UnknownNullability Throwable> action) {
        var fut = delegate.whenCompleteAsync((v, e) -> {
            var c = e instanceof CompletionException
                    ? e.getCause()
                    : e;

            var traceEx = e == null ? null : new TracedException(traceContext.traceEntries(), c);

            action.accept(v, traceEx);
        });

        return new TracedFuture<>(fut, traceContext);
    }

    @Override
    public @NotNull TracedFuture<T> whenCompleteAsync(
            @NotNull BiConsumer<? super @UnknownNullability T, ? super @UnknownNullability Throwable> action, Executor executor) {
        var fut = delegate.whenCompleteAsync((v, e) -> {
            var c = e instanceof CompletionException
                    ? e.getCause()
                    : e;

            var traceEx = e == null ? null : new TracedException(traceContext.traceEntries(), c);

            action.accept(v, traceEx);
        }, executor);

        return new TracedFuture<>(fut, traceContext);
    }

    @Override
    public @NotNull TracedFuture<T> exceptionally(@NotNull Function<Throwable, ? extends T> fn) {
        return new TracedFuture<>(delegate.exceptionally(fn), traceContext);
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

    public boolean isCompletedSuccessfully() {
        return delegate.isDone() && !delegate.isCompletedExceptionally() && !delegate.isCancelled();
    }
}
