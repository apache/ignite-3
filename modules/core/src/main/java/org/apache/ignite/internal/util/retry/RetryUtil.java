package org.apache.ignite.internal.util.retry;

import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class RetryUtil {

    /**
     * Schedules the provided operation to be retried after the specified delay.
     *
     * @param operation Operation.
     * @param delay Delay.
     * @param unit Time unit of the delay.
     * @param executor Executor to schedule the retry in.
     * @return Future that is completed when the operation is successful or failed with an exception.
     */
    public static <T> CompletableFuture<T> scheduleRetry(
            Callable<CompletableFuture<T>> operation,
            long delay,
            TimeUnit unit,
            ScheduledExecutorService executor,
            Optional<Runnable> resetRetryContext
    ) {
        CompletableFuture<T> future = new CompletableFuture<>();

        executor.schedule(() -> operation.call()
                .whenComplete((res, e) -> {
                    if (e == null) {
                        future.complete(res);

                        resetRetryContext.ifPresent(Runnable::run);
                    } else {
                        future.completeExceptionally(e);
                    }
                }), delay, unit);

        return future;
    }
}
