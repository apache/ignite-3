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

package org.apache.ignite.internal.util.retry;

import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.jetbrains.annotations.Nullable;

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
     * @param <T>       result type of the operation.
     * @param operation the async operation to schedule. Must return a non-null {@link CompletableFuture}.
     * @param delay     delay before execution.
     * @param unit      time unit of {@code delay}.
     * @param executor  executor used to schedule the operation.
     * @return a {@link CompletableFuture} completed with the operation's result on success,
     *     or completed exceptionally if the operation fails.
     */
    public static <T> CompletableFuture<T> scheduleRetry(
            Callable<CompletableFuture<T>> operation,
            long delay,
            TimeUnit unit,
            ScheduledExecutorService executor
    ) {
        return scheduleRetry(operation, delay, unit, executor, null);
    }

    /**
     * Schedules the provided operation to run once after the specified delay,
     * invoking separate callbacks on success and failure.
     *
     * <p>The provided completion callback is invoked regardless of whether the
     * operation succeeds or fails.
     *
     * @param <T>        result type of the operation.
     * @param operation  the async operation to schedule. Must return a non-null
     *                   {@link CompletableFuture}.
     * @param delay      delay before execution.
     * @param unit       time unit of {@code delay}.
     * @param executor   executor used to schedule the operation.
     * @param onComplete optional callback invoked after the operation completes, whether
     *                   successfully or exceptionally — for example, to reset a retry context.
     * @return a {@link CompletableFuture} completed with the operation's result on success,
     *     or completed exceptionally if the operation fails.
     */
    public static <T> CompletableFuture<T> scheduleRetry(
            Callable<CompletableFuture<T>> operation,
            long delay,
            TimeUnit unit,
            ScheduledExecutorService executor,
            @Nullable Runnable onComplete
    ) {
        CompletableFuture<T> future = new CompletableFuture<>();

        executor.schedule(() -> {
            try {
                operation.call()
                        .whenComplete((res, e) -> {
                            if (e == null) {
                                future.complete(res);
                            } else {
                                future.completeExceptionally(e);
                            }

                            if (onComplete != null) {
                                onComplete.run();
                            }
                        });
            } catch (Exception e) {
                future.completeExceptionally(e);

                if (onComplete != null) {
                    onComplete.run();
                }
            }
        }, delay, unit);

        return future;
    }
}
