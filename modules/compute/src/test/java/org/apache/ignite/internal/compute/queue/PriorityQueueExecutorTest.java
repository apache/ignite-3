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

package org.apache.ignite.internal.compute.queue;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.apache.ignite.compute.JobStatus.CANCELED;
import static org.apache.ignite.compute.JobStatus.COMPLETED;
import static org.apache.ignite.compute.JobStatus.EXECUTING;
import static org.apache.ignite.compute.JobStatus.FAILED;
import static org.apache.ignite.compute.JobStatus.QUEUED;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureExceptionMatcher.willThrow;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureExceptionMatcher.willTimeoutIn;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willBe;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.testframework.matchers.JobStateMatcher.jobStateWithStatus;
import static org.apache.ignite.internal.testframework.matchers.JobStateMatcher.jobStateWithStatusAndCreateTimeStartTime;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.compute.JobState;
import org.apache.ignite.internal.compute.configuration.ComputeConfiguration;
import org.apache.ignite.internal.compute.state.InMemoryComputeStateMachine;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.eventlog.api.EventLog;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.thread.IgniteThreadFactory;
import org.apache.ignite.lang.IgniteException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Test suite for {@link PriorityQueueExecutor}.
 */
@ExtendWith(ConfigurationExtension.class)
public class PriorityQueueExecutorTest extends BaseIgniteAbstractTest {
    private static final IgniteLogger LOG = Loggers.forClass(PriorityQueueExecutorTest.class);

    @InjectConfiguration
    private ComputeConfiguration configuration;

    private PriorityQueueExecutor priorityQueueExecutor;

    @AfterEach
    void tearDown() {
        if (priorityQueueExecutor != null) {
            priorityQueueExecutor.shutdown();
        }
    }

    @Test
    public void testQueueIsWorking() {
        initExecutor(1);

        CountDownLatch latch1 = new CountDownLatch(1);
        CountDownLatch latch2 = new CountDownLatch(1);

        // Start two tasks
        CompletableFuture<Integer> task1 = submit(() -> {
            latch1.await();
            return 0;
        });

        CompletableFuture<Integer> task2 = submit(() -> {
            latch2.await();
            return 1;
        });

        assertThat(task1.isDone(), is(false));
        assertThat(task2.isDone(), is(false));

        // Current executing task is 1 and task 2 still on queue
        latch2.countDown();
        assertThat(task1, willTimeoutIn(100, TimeUnit.MILLISECONDS));
        assertThat(task2, willTimeoutIn(100, TimeUnit.MILLISECONDS));

        // Task 1 should complete and task 2 also, because count down already.
        latch1.countDown();
        assertThat(task1, willCompleteSuccessfully());
        assertThat(task2, willCompleteSuccessfully());
    }

    @Test
    public void testSubmitWithPriority() {
        initExecutor(1);

        CountDownLatch latch1 = new CountDownLatch(1);
        CountDownLatch latch2 = new CountDownLatch(1);
        CountDownLatch latch3 = new CountDownLatch(1);

        // Start three tasks
        CompletableFuture<Integer> task1 = submit(() -> {
            latch1.await();
            return 0;
        });

        CompletableFuture<Integer> task2 = submit(() -> {
            latch2.await();
            return 1;
        }, 1);

        CompletableFuture<Integer> task3 = submit(() -> {
            latch3.await();
            return 1;
        }, 2);

        assertThat(task1.isDone(), is(false));
        assertThat(task2.isDone(), is(false));
        assertThat(task3.isDone(), is(false));

        // Task 1 should complete, task 2 and 3 still await latch.
        latch1.countDown();
        assertThat(task1, willCompleteSuccessfully());
        assertThat(task2.isDone(), is(false));
        assertThat(task3.isDone(), is(false));

        // Current executing task is 3 because of priority.
        latch2.countDown();
        assertThat(task2, willTimeoutIn(100, TimeUnit.MILLISECONDS));
        assertThat(task3, willTimeoutIn(100, TimeUnit.MILLISECONDS));

        latch3.countDown();
        assertThat(task2, willCompleteSuccessfully());
        assertThat(task3, willCompleteSuccessfully());
    }

    @Test
    public void testSameOrder() {
        initExecutor(1);

        CountDownLatch latch1 = new CountDownLatch(1);
        CountDownLatch latch2 = new CountDownLatch(1);
        CountDownLatch latch3 = new CountDownLatch(1);

        // Start three tasks
        CompletableFuture<Integer> task1 = submit(() -> {
            latch1.await();
            return 0;
        }, 1);

        CompletableFuture<Integer> task2 = submit(() -> {
            latch2.await();
            return 1;
        }, 1);

        CompletableFuture<Integer> task3 = submit(() -> {
            latch3.await();
            return 1;
        }, 1);

        assertThat(task1.isDone(), is(false));
        assertThat(task2.isDone(), is(false));
        assertThat(task3.isDone(), is(false));

        // Task 1 should complete, task 2 and 3 still await latch.
        latch1.countDown();
        assertThat(task1, willCompleteSuccessfully());
        assertThat(task2.isDone(), is(false));
        assertThat(task3.isDone(), is(false));

        // Current executing task is 2 because it was added first.
        latch3.countDown();
        assertThat(task2, willTimeoutIn(100, TimeUnit.MILLISECONDS));
        assertThat(task3, willTimeoutIn(100, TimeUnit.MILLISECONDS));

        latch2.countDown();
        assertThat(task2, willCompleteSuccessfully());
        assertThat(task3, willCompleteSuccessfully());
    }

    @Test
    public void testQueueOverflow() {
        initExecutor(1, 1);

        CountDownLatch latch1 = new CountDownLatch(1);
        CountDownLatch latch2 = new CountDownLatch(1);

        // Submit task for executing
        submit(() -> {
            latch1.await();
            return 0;
        }, 1);

        // Submit task for executing, should be in queue
        submit(() -> {
            latch2.await();
            return 1;
        }, 1);

        // Submit task for execution should throw exception because queue is full.
        assertThat(
                submit(() -> null),
                willThrow(IgniteException.class, "Compute queue overflow")
        );

        // Force the tasks to exit to minimize executor shutdown time
        latch1.countDown();
        latch2.countDown();
    }

    @Test
    void taskCatchesInterruption() {
        initExecutor(1);

        CountDownLatch latch = new CountDownLatch(1);
        QueueExecution<Object> execution = priorityQueueExecutor.submit(() -> {
            while (true) {
                try {
                    latch.await();
                } catch (InterruptedException e) {
                    return completedFuture(0);
                }
            }
        });

        JobState executingState = await().until(execution::state, jobStateWithStatus(EXECUTING));

        assertThat(execution.cancel(), is(true));

        await().until(
                execution::state,
                jobStateWithStatusAndCreateTimeStartTime(CANCELED, executingState.createTime(), executingState.startTime())
        );
        assertThat(execution.resultAsync(), willThrow(CancellationException.class));
    }

    @RepeatedTest(value = 10, failureThreshold = 1)
    void cancelExecutionRace() {
        initExecutor(1);

        QueueExecution<Object> execution = priorityQueueExecutor.submit(() -> {
            try {
                new CountDownLatch(1).await();
            } catch (InterruptedException ignored) {
                // ignored
            }
            return null;
        });

        assertThat(execution.cancel(), is(true));

        assertThat(execution.resultAsync(), willThrow(CancellationException.class));

        assertThat(execution.state(), is(jobStateWithStatus(CANCELED)));
    }

    @Test
    void taskDoesntCatchInterruption() {
        initExecutor(1);

        CountDownLatch latch = new CountDownLatch(1);
        QueueExecution<Object> execution = priorityQueueExecutor.submit(() -> {
            latch.await();
            return completedFuture(0);
        });

        JobState executingState = await().until(execution::state, jobStateWithStatus(EXECUTING));

        assertThat(execution.cancel(), is(true));

        await().until(
                execution::state,
                jobStateWithStatusAndCreateTimeStartTime(CANCELED, executingState.createTime(), executingState.startTime())
        );
        assertThat(execution.resultAsync(), willThrow(InterruptedException.class));
    }

    @Test
    void completedTaskCancel() {
        initExecutor(1);

        QueueExecution<Object> execution = priorityQueueExecutor.submit(() -> completedFuture(0));

        await().until(execution::state, jobStateWithStatus(COMPLETED));

        assertThat(execution.cancel(), is(false));

        assertThat(execution.state(), is(jobStateWithStatus(COMPLETED)));
        assertThat(execution.resultAsync(), willBe(0));
    }

    @Test
    void queuedTaskCancel() {
        initExecutor(1);

        // Occupy the executor with the running task
        CountDownLatch latch = new CountDownLatch(1);
        QueueExecution<Object> runningExecution = priorityQueueExecutor.submit(() -> {
            latch.await();
            return completedFuture(0);
        });

        await().until(runningExecution::state, jobStateWithStatus(EXECUTING));

        // Put the task in the queue
        QueueExecution<Object> execution = priorityQueueExecutor.submit(() -> completedFuture(0));
        await().until(execution::state, jobStateWithStatus(QUEUED));

        // Cancel the task
        assertThat(execution.cancel(), is(true));
        assertThat(execution.state(), jobStateWithStatus(CANCELED));

        // Finish the running task
        latch.countDown();

        // And check that the canceled task was removed from the queue and never executed
        assertThat(execution.resultAsync(), willThrow(CancellationException.class));
    }

    @Test
    void retryTaskFail() {
        initExecutor(1);

        AtomicInteger runTimes = new AtomicInteger();

        int maxRetries = 5;

        QueueExecution<Object> execution = priorityQueueExecutor.submit(() -> {
            runTimes.incrementAndGet();
            throw new RuntimeException();
        }, 0, maxRetries);

        await().until(execution::state, jobStateWithStatus(FAILED));

        assertThat(runTimes.get(), is(maxRetries + 1));
    }

    @Test
    void retryTaskCancel() {
        initExecutor(1);

        AtomicInteger runTimes = new AtomicInteger();

        int maxRetries = 5;

        QueueExecution<Object> execution = priorityQueueExecutor.submit(() -> {
            runTimes.incrementAndGet();
            new CountDownLatch(1).await();
            return null;
        }, 0, maxRetries);

        await().until(execution::state, jobStateWithStatus(EXECUTING));
        execution.cancel();

        await().until(execution::state, jobStateWithStatus(CANCELED));
        assertThat(runTimes.get(), is(1));
    }

    @Test
    void retryTaskSuccess() {
        initExecutor(1);

        AtomicInteger runTimes = new AtomicInteger();

        int maxRetries = 5;

        QueueExecution<Object> execution = priorityQueueExecutor.submit(() -> {
            if (runTimes.incrementAndGet() <= maxRetries) {
                throw new RuntimeException();
            }
            return completedFuture(0);
        }, 0, maxRetries);

        await().until(execution::state, jobStateWithStatus(COMPLETED));

        assertThat(runTimes.get(), is(maxRetries + 1));
    }

    @Test
    void defaultTaskIsNotRetried() {
        initExecutor(1);

        AtomicInteger runTimes = new AtomicInteger();

        QueueExecution<Object> execution = priorityQueueExecutor.submit(() -> {
            runTimes.incrementAndGet();
            throw new RuntimeException();
        });

        await().until(execution::state, jobStateWithStatus(FAILED));

        assertThat(runTimes.get(), is(1));
    }

    @Test
    public void testChangePriorityBeforeExecution() {
        initExecutor(1);

        CountDownLatch latch1 = new CountDownLatch(1);
        CountDownLatch latch2 = new CountDownLatch(1);
        CountDownLatch latch3 = new CountDownLatch(1);

        // Start three tasks
        CompletableFuture<Integer> task1 = submit(() -> {
            latch1.await();
            return 0;
        }, 10);

        CompletableFuture<Integer> task2 = submit(() -> {
            latch2.await();
            return 1;
        }, 5);

        QueueExecution<Integer> runningExecution3 =  priorityQueueExecutor.submit(() -> {
            latch3.await();
            return completedFuture(2);
        }, 1, 0);

        CompletableFuture<Integer> task3 = runningExecution3.resultAsync();

        assertThat(task1.isDone(), is(false));
        assertThat(task2.isDone(), is(false));
        assertThat(task3.isDone(), is(false));

        // Change priority on task3, it should be executed before task2
        assertThat(runningExecution3.changePriority(20), is(true));

        // Task 1 should be completed
        latch1.countDown();
        assertThat(task1, willCompleteSuccessfully());
        assertThat(task2.isDone(), is(false));
        assertThat(task3.isDone(), is(false));

        // Current executing task is 3 because we changed priority
        latch2.countDown();
        assertThat(task2.isDone(), is(false));
        assertThat(task3.isDone(), is(false));

        latch3.countDown();
        assertThat(task1, willCompleteSuccessfully());
        assertThat(task2, willCompleteSuccessfully());
        assertThat(task3, willCompleteSuccessfully());
    }

    @Test
    public void testChangePriorityInTheMiddleExecution() {
        initExecutor(1);

        CountDownLatch latch1 = new CountDownLatch(1);
        CountDownLatch latch2 = new CountDownLatch(1);
        CountDownLatch latch3 = new CountDownLatch(1);
        CountDownLatch latch4 = new CountDownLatch(1);

        // Start four tasks
        CompletableFuture<Integer> task1 = submit(() -> {
            latch1.await();
            return 0;
        }, 10);

        CompletableFuture<Integer> task2 = submit(() -> {
            latch2.await();
            return 1;
        }, 5);

        QueueExecution<Integer> runningExecution =  priorityQueueExecutor.submit(() -> {
            latch3.await();
            return completedFuture(2);
        }, 1, 0);

        CompletableFuture<Integer> task3 = runningExecution.resultAsync();

        CompletableFuture<Integer> task4 = submit(() -> {
            latch4.await();
            return 4;
        }, 5);

        assertThat(task1.isDone(), is(false));
        assertThat(task2.isDone(), is(false));
        assertThat(task3.isDone(), is(false));
        assertThat(task4.isDone(), is(false));

        // Change priority on task3, it should be executed before task2 and task4
        assertThat(runningExecution.changePriority(20), is(true));

        // Task 1 should be completed
        latch1.countDown();
        assertThat(task1, willCompleteSuccessfully());
        assertThat(task2.isDone(), is(false));
        assertThat(task3.isDone(), is(false));
        assertThat(task4.isDone(), is(false));

        // Current executing task is 3 because we changed priority
        latch2.countDown();
        assertThat(task2.isDone(), is(false));
        assertThat(task3.isDone(), is(false));
        assertThat(task4.isDone(), is(false));

        // Current executing task is 3 because we changed priority
        latch4.countDown();
        assertThat(task2.isDone(), is(false));
        assertThat(task3.isDone(), is(false));
        assertThat(task4.isDone(), is(false));

        // Complete task3, task2 and task4 will be executed as well
        latch3.countDown();
        assertThat(task1, willCompleteSuccessfully());
        assertThat(task2, willCompleteSuccessfully());
        assertThat(task3, willCompleteSuccessfully());
        assertThat(task4, willCompleteSuccessfully());
    }

    @Test
    public void testChangePriorityAlreadyExecuting() {
        initExecutor(1);

        CountDownLatch latch1 = new CountDownLatch(1);
        CountDownLatch latch2 = new CountDownLatch(1);

        // Start two tasks
        QueueExecution<Integer> runningExecution1 =  priorityQueueExecutor.submit(() -> {
            latch1.await();
            return completedFuture(2);
        }, 1, 0);

        CompletableFuture<Integer> task1 = runningExecution1.resultAsync();

        CompletableFuture<Integer> task2 = submit(() -> {
            latch2.await();
            return 1;
        }, 5);

        assertThat(task1.isDone(), is(false));
        assertThat(task2.isDone(), is(false));

        // Change priority on task1, it is already in executing stage, should return false
        assertThat(runningExecution1.changePriority(20), is(false));

        // Task 1 should not be completed because change priority failed and task2 has higher priority
        latch1.countDown();
        assertThat(task1, willCompleteSuccessfully());
        assertThat(task2.isDone(), is(false));

        // Complete task2 and task1 will be executed after task2
        latch2.countDown();
        assertThat(task1, willCompleteSuccessfully());
        assertThat(task2, willCompleteSuccessfully());
    }

    @Test
    public void testChangePriorityAllExecuting() {
        initExecutor(3);

        CountDownLatch latch1 = new CountDownLatch(1);
        CountDownLatch latch2 = new CountDownLatch(1);

        // Start three tasks
        QueueExecution<Integer> runningExecution =  priorityQueueExecutor.submit(() -> {
            latch1.await();
            return completedFuture(2);
        }, 1, 0);

        CompletableFuture<Integer> task1 = runningExecution.resultAsync();

        CompletableFuture<Integer> task2 = submit(() -> {
            latch2.await();
            return 1;
        }, 1);

        assertThat(task1.isDone(), is(false));
        assertThat(task2.isDone(), is(false));

        // Change priority on task3, it should be executed before task2
        assertThat(runningExecution.changePriority(2), is(false));

        // Task 1 should not be completed because of changed priority of task3
        latch1.countDown();
        assertThat(task1, willCompleteSuccessfully());
        assertThat(task2.isDone(), is(false));

        // Current executing task is 3 because we changed priority
        latch2.countDown();
        assertThat(task1, willCompleteSuccessfully());
        assertThat(task2, willCompleteSuccessfully());
    }

    private void initExecutor(int threads) {
        initExecutor(threads, Integer.MAX_VALUE);
    }

    private void initExecutor(int threads, int maxQueueSize) {
        assertThat(
                configuration.change(computeChange -> computeChange.changeThreadPoolSize(threads).changeQueueMaxSize(maxQueueSize)),
                willCompleteSuccessfully()
        );

        String nodeName = "testNode";
        priorityQueueExecutor = new PriorityQueueExecutor(
                configuration,
                IgniteThreadFactory.create(nodeName, "compute", LOG),
                new InMemoryComputeStateMachine(configuration, nodeName),
                EventLog.NOOP
        );
    }

    private <R> CompletableFuture<R> submit(Callable<R> job) {
        return submit(job, 0);
    }

    private <R> CompletableFuture<R> submit(Callable<R> job, int priority) {
        return submit(job, priority, 0);
    }

    private <R> CompletableFuture<R> submit(Callable<R> job, int priority, int maxRetries) {
        return priorityQueueExecutor.submit(() -> completedFuture(job.call()), priority, maxRetries).resultAsync();
    }
}
