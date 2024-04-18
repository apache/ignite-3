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

package org.apache.ignite.internal.pagememory.util;

import static org.apache.ignite.internal.testframework.IgniteTestUtils.hasCause;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.util.ExceptionUtils;
import org.apache.ignite.internal.util.IgniteUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class GradualTaskExecutorTest extends BaseIgniteAbstractTest {
    private final ExecutorService executorService = Executors.newSingleThreadExecutor();

    private final GradualTaskExecutor executor = new GradualTaskExecutor(executorService);

    @AfterEach
    void stopExecutor() throws Exception {
        IgniteUtils.closeAllManually(
                executor,
                () -> IgniteUtils.shutdownAndAwaitTermination(executorService, 10, TimeUnit.SECONDS)
        );
    }

    @Test
    void executesStepsTillTaskCompletes() {
        TwoStageTask twoStageTask = new TwoStageTask();

        CompletableFuture<Void> future = executor.execute(twoStageTask);

        assertThat(future, willCompleteSuccessfully());

        assertTrue(twoStageTask.isCompleted());
    }

    @Test
    void doesNotExecuteStepsWhenTaskIsCompleted(@Mock GradualTask task) throws Exception {
        when(task.isCompleted()).thenReturn(true);

        CompletableFuture<Void> future = executor.execute(task);

        assertThat(future, willCompleteSuccessfully());

        verify(task, never()).runStep();
    }

    @Test
    void veryLongTaskAllowsOtherTasksToRun(@Mock GradualTask infiniteTask) {
        when(infiniteTask.isCompleted()).thenReturn(false);

        executor.execute(infiniteTask);

        TwoStageTask twoStageTask = new TwoStageTask();

        CompletableFuture<Void> future = executor.execute(twoStageTask);

        assertThat(future, willCompleteSuccessfully());

        assertTrue(twoStageTask.isCompleted());
    }

    @Test
    void nonFinishedTasksAreCancelledWhenExecutorIsClosed(@Mock GradualTask infiniteTask) throws Exception {
        CountDownLatch infiniteTaskStartedExecution = new CountDownLatch(1);

        when(infiniteTask.isCompleted()).thenReturn(false);
        doAnswer(invocation -> {
            infiniteTaskStartedExecution.countDown();

            return null;
        }).when(infiniteTask).runStep();

        CompletableFuture<Void> future = executor.execute(infiniteTask);

        assertTrue(infiniteTaskStartedExecution.await(1, TimeUnit.SECONDS), "Infinite task was not started in time");

        assertFalse(future.isDone());

        stopExecutor();

        Exception ex = assertThrows(Exception.class, () -> future.getNow(null));

        assertTrue(
                hasCause(ex, CancellationException.class, null) || hasCause(ex, RejectedExecutionException.class, null),
                "Unexpected exception thrown: " + ExceptionUtils.getFullStackTrace(ex)
        );
    }

    private static class TwoStageTask implements GradualTask {
        private final AtomicInteger stepsRun = new AtomicInteger(0);

        @Override
        public void runStep() {
            stepsRun.incrementAndGet();
        }

        @Override
        public boolean isCompleted() {
            return stepsRun.get() >= 2;
        }
    }
}
