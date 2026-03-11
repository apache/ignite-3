package org.apache.ignite.internal.util.retry;

import static java.util.Optional.empty;

import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Utility class for scheduling asynchronous retry operations.
 *
 * <p>Provides overloaded {@code scheduleRetry} methods that schedule an async operation
 * to run after a delay, optionally invoking callbacks on success or failure. The returned
 * {@link CompletableFuture} is completed when the scheduled operation completes.
 *
 * <p>This class is not intended to manage the retry loop itself — it schedules a single
 * delayed execution. The caller is responsible for chaining successive calls to build a
 * full retry loop, typically driven by a retry context and a timeout strategy.
 */
public class RetryUtil {
    /**
     * Schedules the provided operation to run once after the specified delay.
     *
     * <p>No callbacks are invoked on completion. Equivalent to calling
     * {@link #scheduleRetry(Callable, long, TimeUnit, ScheduledExecutorService, Optional, Optional)}
     * with both optional callbacks empty.
     *
     * @param <T>       result type of the operation.
     * @param operation the async operation to schedule. Must return a non-null {@link CompletableFuture}.
     * @param delay     delay before execution.
     * @param unit      time unit of {@code delay}.
     * @param executor  executor used to schedule the operation.
     * @return a {@link CompletableFuture} completed with the operation's result on success,
     *         or completed exceptionally if the operation fails.
     */
    public static <T> CompletableFuture<T> scheduleRetry(
            Callable<CompletableFuture<T>> operation,
            long delay,
            TimeUnit unit,
            ScheduledExecutorService executor
    ) {
        return scheduleRetry(operation, delay, unit, executor, empty(), empty());
    }

    /**
     * Schedules the provided operation to run once after the specified delay,
     * invoking a callback on successful completion.
     *
     * <p>The {@code onSuccessfulComplete} callback is invoked only if the operation
     * completes without an exception. Equivalent to calling the full overload with
     * an empty {@code onFailure}.
     *
     * @param <T>                  result type of the operation.
     * @param operation            the async operation to schedule.
     * @param delay                delay before execution.
     * @param unit                 time unit of {@code delay}.
     * @param executor             executor used to schedule the operation.
     * @param onSuccessfulComplete optional callback invoked when the operation succeeds,
     *                             for example to reset a retry context.
     * @return a {@link CompletableFuture} completed with the operation's result on success,
     *         or completed exceptionally if the operation fails.
     */
    public static <T> CompletableFuture<T> scheduleRetry(
            Callable<CompletableFuture<T>> operation,
            long delay,
            TimeUnit unit,
            ScheduledExecutorService executor,
            Optional<Runnable> onSuccessfulComplete
    ) {
        return scheduleRetry(operation, delay, unit, executor, onSuccessfulComplete, empty());
    }

    /**
     * Schedules the provided operation to run once after the specified delay,
     * invoking separate callbacks on success and failure.
     *
     * <p>{@code onSuccessfulComplete} is called only if the operation's future completes
     * without an exception. {@code onFailure} is called only if it completes exceptionally.
     * Neither callback is invoked if the other fires.
     *
     * @param <T>                  result type of the operation.
     * @param operation            the async operation to schedule. Must return a non-null
     *                             {@link CompletableFuture}.
     * @param delay                delay before execution.
     * @param unit                 time unit of {@code delay}.
     * @param executor             executor used to schedule the operation.
     * @param onSuccessfulComplete optional callback invoked on success, for example to
     *                             reset a retry context after a successful attempt.
     * @param onFailure            optional callback invoked on failure, for example to
     *                             record a failed attempt or trigger alerting.
     * @return a {@link CompletableFuture} completed with the operation's result on success,
     *         or completed exceptionally if the operation fails.
     */
    public static <T> CompletableFuture<T> scheduleRetry(
            Callable<CompletableFuture<T>> operation,
            long delay,
            TimeUnit unit,
            ScheduledExecutorService executor,
            Optional<Runnable> onSuccessfulComplete,
            Optional<Runnable> onFailure
    ) {
        CompletableFuture<T> future = new CompletableFuture<>();

        executor.schedule(() -> operation.call()
                .whenComplete((res, e) -> {
                    if (e == null) {
                        future.complete(res);

                        onSuccessfulComplete.ifPresent(Runnable::run);
                    } else {
                        future.completeExceptionally(e);

                        onFailure.ifPresent(Runnable::run);
                    }
                }), delay, unit);

        return future;
    }
}
