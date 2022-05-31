/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.pagememory.persistence.store;

import static org.apache.ignite.internal.testframework.IgniteTestUtils.runAsync;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.ignite.lang.IgniteInternalException;
import org.apache.ignite.lang.IgniteLogger;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.Test;

/**
 * For {@link LongOperationAsyncExecutor} testing.
 */
public class LongOperationAsyncExecutorTest {
    private final IgniteLogger log = IgniteLogger.forClass(LongOperationAsyncExecutorTest.class);

    @Test
    void testAsync() throws Exception {
        LongOperationAsyncExecutor executor = createExecutor();

        Thread testMethodThread = Thread.currentThread();

        // Checks that tasks are executed in a separate thread.

        CompletableFuture<?> task0Future = new CompletableFuture<>();

        Runnable task0 = () -> {
            assertNotSame(testMethodThread, Thread.currentThread());

            assertThat(Thread.currentThread().getName(), startsWith("%test%async-op0-task-"));
        };

        executor.async(createTask(task0, null, task0Future), "op0");

        task0Future.get(100, TimeUnit.MILLISECONDS);

        // Checks that the task will not be executed until the read lock is released.

        CompletableFuture<?> startSupplierFuture = new CompletableFuture<>();

        CompletableFuture<Object> waitSupplierFuture = new CompletableFuture<>();

        try {
            CompletableFuture<?> asyncExecuteSupplierFuture = runAsync(() -> {
                executor.afterAsyncCompletion(() -> {
                    startSupplierFuture.complete(null);

                    try {
                        waitSupplierFuture.get(1, TimeUnit.SECONDS);
                    } catch (Throwable e) {
                        throw new IgniteInternalException("from_run_async", e);
                    }

                    return null;
                });
            });

            startSupplierFuture.get(100, TimeUnit.MILLISECONDS);

            CompletableFuture<?> task1Future = new CompletableFuture<>();

            Runnable task1 = () -> {
                assertNotSame(testMethodThread, Thread.currentThread());

                assertThat(Thread.currentThread().getName(), startsWith("%test%async-op1-task-"));
            };

            executor.async(createTask(task1, null, task1Future), "op1");

            assertThrows(TimeoutException.class, () -> task1Future.get(100, TimeUnit.MILLISECONDS));

            waitSupplierFuture.complete(null);

            task1Future.get(100, TimeUnit.MILLISECONDS);

            asyncExecuteSupplierFuture.get(100, TimeUnit.MILLISECONDS);
        } finally {
            waitSupplierFuture.complete(null);
        }
    }

    @Test
    void testAfterAsyncCompletion() throws Exception {
        LongOperationAsyncExecutor executor = createExecutor();

        Thread testMethodThread = Thread.currentThread();

        // Checks that the supplier does not change the thread and returns a value.

        assertEquals(0, executor.afterAsyncCompletion(() -> {
            assertSame(testMethodThread, Thread.currentThread());

            return 0;
        }));

        // Checks that the supplier cannot be executed until write lock is released.

        CompletableFuture<?> startTaskFuture = new CompletableFuture<>();

        CompletableFuture<?> waitTaskFuture = new CompletableFuture<>();

        CompletableFuture<?> finishTaskFuture = new CompletableFuture<>();

        try {
            Runnable task = () -> {
                try {
                    waitTaskFuture.get(1, TimeUnit.SECONDS);
                } catch (Throwable e) {
                    throw new IgniteInternalException("from_task", e);
                }
            };

            executor.async(createTask(task, startTaskFuture, finishTaskFuture), "op0");

            startTaskFuture.get(100, TimeUnit.MILLISECONDS);

            CompletableFuture<?> asyncExecuteSupplierFuture = runAsync(() -> assertEquals(1, executor.afterAsyncCompletion(() -> 1)));

            assertThrows(TimeoutException.class, () -> asyncExecuteSupplierFuture.get(100, TimeUnit.MILLISECONDS));

            waitTaskFuture.complete(null);

            asyncExecuteSupplierFuture.get(100, TimeUnit.MILLISECONDS);

            finishTaskFuture.get(100, TimeUnit.MILLISECONDS);
        } finally {
            waitTaskFuture.complete(null);
        }
    }

    @Test
    void testAwaitAsyncTaskCompletion() throws Exception {
        LongOperationAsyncExecutor executor = createExecutor();

        // Checks without cancellation.

        CompletableFuture<?> startTask0Future = new CompletableFuture<>();

        CompletableFuture<?> waitTask0Future = new CompletableFuture<>();

        CompletableFuture<?> finishTask0Future = new CompletableFuture<>();

        Runnable task0 = () -> {
            try {
                waitTask0Future.get(1, TimeUnit.SECONDS);
            } catch (Throwable e) {
                throw new IgniteInternalException("from_task_0", e);
            }
        };

        try {
            executor.async(createTask(task0, startTask0Future, finishTask0Future), "op0");

            startTask0Future.get(100, TimeUnit.MILLISECONDS);

            CompletableFuture<?> awaitAsyncTaskCompletionFuture0 = runAsync(() -> executor.awaitAsyncTaskCompletion(false));

            assertThrows(TimeoutException.class, () -> awaitAsyncTaskCompletionFuture0.get(100, TimeUnit.MILLISECONDS));

            waitTask0Future.complete(null);

            awaitAsyncTaskCompletionFuture0.get(100, TimeUnit.MILLISECONDS);

            finishTask0Future.get(100, TimeUnit.MILLISECONDS);
        } finally {
            waitTask0Future.complete(null);
        }

        // Checks with cancellation.

        CompletableFuture<?> startTask1Future = new CompletableFuture<>();

        CompletableFuture<?> waitTask1Future = new CompletableFuture<>();

        CompletableFuture<?> finishTask1Future = new CompletableFuture<>();

        Runnable task1 = () -> {
            try {
                waitTask1Future.get(1, TimeUnit.SECONDS);
            } catch (Throwable e) {
                throw new IgniteInternalException("from_task_1", e);
            }
        };

        try {
            executor.async(createTask(task1, startTask1Future, finishTask1Future), "op0");

            startTask1Future.get(100, TimeUnit.MILLISECONDS);

            CompletableFuture<?> awaitAsyncTaskCompletionFuture1 = runAsync(() -> executor.awaitAsyncTaskCompletion(true));

            awaitAsyncTaskCompletionFuture1.get(100, TimeUnit.MILLISECONDS);

            ExecutionException exception = assertThrows(
                    ExecutionException.class,
                    () -> finishTask1Future.get(100, TimeUnit.MILLISECONDS)
            );

            // Checks that the exception will be from task1.
            assertThat(exception.getCause(), instanceOf(IgniteInternalException.class));
            assertThat(exception.getCause().getMessage(), equalTo("from_task_1"));

            assertThat(exception.getCause().getCause(), instanceOf(InterruptedException.class));
        } finally {
            waitTask1Future.complete(null);
        }
    }

    private Runnable createTask(
            Runnable task,
            @Nullable CompletableFuture<?> startTaskFuture,
            CompletableFuture<?> finishTaskFuture
    ) {
        return new Runnable() {
            /** {@inheritDoc} */
            @Override
            public void run() {
                if (startTaskFuture != null) {
                    startTaskFuture.complete(null);
                }

                try {
                    task.run();

                    finishTaskFuture.complete(null);
                } catch (Throwable t) {
                    finishTaskFuture.completeExceptionally(t);
                }
            }
        };
    }

    private LongOperationAsyncExecutor createExecutor() {
        return new LongOperationAsyncExecutor("test", log);
    }
}
