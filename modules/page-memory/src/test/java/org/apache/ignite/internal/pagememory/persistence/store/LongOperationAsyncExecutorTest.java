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

package org.apache.ignite.internal.pagememory.persistence.store;

import static org.apache.ignite.internal.testframework.IgniteTestUtils.runAsync;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.internal.lang.RunnableX;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.junit.jupiter.api.Test;

/**
 * For {@link LongOperationAsyncExecutor} testing.
 */
public class LongOperationAsyncExecutorTest {
    private final IgniteLogger log = Loggers.forClass(LongOperationAsyncExecutorTest.class);

    @Test
    void testAsync() throws Exception {
        LongOperationAsyncExecutor executor = createExecutor();

        Thread testMethodThread = Thread.currentThread();

        // Checks that tasks are executed in a separate thread.

        Runnable task0 = () -> {
            assertNotSame(Thread.currentThread(), testMethodThread);

            assertThat(Thread.currentThread().getName(), containsString("%test%async-op0-task-"));
        };

        executor.async(task0, "op0").get(1, TimeUnit.SECONDS);

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

            RunnableX task1 = () -> {
                assertNotSame(Thread.currentThread(), testMethodThread);

                assertThat(Thread.currentThread().getName(), containsString("%test%async-op1-task-"));
            };

            CompletableFuture<Void> task1Future = executor.async(task1, "op1");

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
            assertSame(Thread.currentThread(), testMethodThread);

            return 0;
        }));

        // Checks that the supplier cannot be executed until write lock is released.

        CompletableFuture<?> startTaskFuture = new CompletableFuture<>();

        CompletableFuture<?> waitTaskFuture = new CompletableFuture<>();

        try {
            RunnableX task = () -> waitTaskFuture.get(1, TimeUnit.SECONDS);

            CompletableFuture<Void> taskFuture = executor.async(createTask(task, startTaskFuture), "op0");

            startTaskFuture.get(100, TimeUnit.MILLISECONDS);

            CompletableFuture<?> asyncExecuteSupplierFuture = runAsync(() -> assertEquals(1, executor.afterAsyncCompletion(() -> 1)));

            assertThrows(TimeoutException.class, () -> asyncExecuteSupplierFuture.get(100, TimeUnit.MILLISECONDS));

            waitTaskFuture.complete(null);

            asyncExecuteSupplierFuture.get(100, TimeUnit.MILLISECONDS);

            taskFuture.get(100, TimeUnit.MILLISECONDS);
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

        RunnableX task0 = () -> waitTask0Future.get(1, TimeUnit.SECONDS);

        try {
            CompletableFuture<Void> taskFuture = executor.async(createTask(task0, startTask0Future), "op0");

            startTask0Future.get(100, TimeUnit.MILLISECONDS);

            CompletableFuture<?> awaitAsyncTaskCompletionFuture0 = runAsync(() -> executor.awaitAsyncTaskCompletion(false));

            assertThrows(TimeoutException.class, () -> awaitAsyncTaskCompletionFuture0.get(100, TimeUnit.MILLISECONDS));

            waitTask0Future.complete(null);

            awaitAsyncTaskCompletionFuture0.get(100, TimeUnit.MILLISECONDS);

            taskFuture.get(1, TimeUnit.SECONDS);
        } finally {
            waitTask0Future.complete(null);
        }

        // Checks with cancellation.

        CompletableFuture<?> startTask1Future = new CompletableFuture<>();

        CompletableFuture<?> waitTask1Future = new CompletableFuture<>();

        RunnableX task1 = () -> waitTask1Future.get(1, TimeUnit.SECONDS);

        try {
            CompletableFuture<Void> task1Future = executor.async(createTask(task1, startTask1Future), "op0");

            startTask1Future.get(100, TimeUnit.MILLISECONDS);

            CompletableFuture<?> awaitAsyncTaskCompletionFuture1 = runAsync(() -> executor.awaitAsyncTaskCompletion(true));

            awaitAsyncTaskCompletionFuture1.get(100, TimeUnit.MILLISECONDS);

            ExecutionException exception = assertThrows(
                    ExecutionException.class,
                    () -> task1Future.get(100, TimeUnit.MILLISECONDS)
            );

            // Checks that the exception will be from task1.
            assertThat(exception.getCause(), instanceOf(InterruptedException.class));
        } finally {
            waitTask1Future.complete(null);
        }
    }

    private static RunnableX createTask(RunnableX task, CompletableFuture<?> startTaskFuture) {
        return () -> {
            if (startTaskFuture != null) {
                startTaskFuture.complete(null);
            }

            task.run();
        };
    }

    private LongOperationAsyncExecutor createExecutor() {
        return new LongOperationAsyncExecutor("test", log);
    }
}
