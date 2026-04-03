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

package org.apache.ignite.internal.compute.executor;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.apache.ignite.compute.JobStatus.CANCELED;
import static org.apache.ignite.compute.JobStatus.COMPLETED;
import static org.apache.ignite.compute.JobStatus.EXECUTING;
import static org.apache.ignite.compute.JobStatus.FAILED;
import static org.apache.ignite.internal.compute.ComputeUtils.getJobExecuteArgumentType;
import static org.apache.ignite.internal.compute.ComputeUtils.getTaskSplitArgumentType;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureExceptionMatcher.willThrow;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willBe;
import static org.apache.ignite.internal.testframework.matchers.JobStateMatcher.jobStateWithStatus;
import static org.apache.ignite.internal.testframework.matchers.JobStateMatcher.jobStateWithStatusAndCreateTimeStartTime;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.Ignite;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.JobExecutionContext;
import org.apache.ignite.compute.JobState;
import org.apache.ignite.compute.task.MapReduceJob;
import org.apache.ignite.compute.task.MapReduceTask;
import org.apache.ignite.compute.task.TaskExecutionContext;
import org.apache.ignite.internal.compute.ComputeJobDataHolder;
import org.apache.ignite.internal.compute.ExecutionOptions;
import org.apache.ignite.internal.compute.SharedComputeUtils;
import org.apache.ignite.internal.compute.configuration.ComputeConfiguration;
import org.apache.ignite.internal.compute.events.ComputeEventMetadata;
import org.apache.ignite.internal.compute.state.InMemoryComputeStateMachine;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.deployunit.loader.UnitsClassLoader;
import org.apache.ignite.internal.eventlog.api.EventLog;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.hlc.TestClockService;
import org.apache.ignite.internal.network.ClusterNodeImpl;
import org.apache.ignite.internal.network.InternalClusterNode;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.lang.CancelHandleHelper;
import org.apache.ignite.network.NetworkAddress;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith({MockitoExtension.class, ConfigurationExtension.class})
class ComputeExecutorTest extends BaseIgniteAbstractTest {
    @Mock
    private Ignite ignite;

    @InjectConfiguration
    private ComputeConfiguration computeConfiguration;

    private final InternalClusterNode localNode = new ClusterNodeImpl(UUID.randomUUID(), "testNode", new NetworkAddress("host", 1));

    private ComputeExecutor computeExecutor;

    private final UnitsClassLoader jobClassLoader = new UnitsClassLoader(List.of(), getClass().getClassLoader());

    @BeforeEach
    void setUp() {
        InMemoryComputeStateMachine stateMachine = new InMemoryComputeStateMachine(computeConfiguration, "testNode");
        computeExecutor = new ComputeExecutorImpl(
                ignite,
                tracker -> ignite,
                stateMachine,
                computeConfiguration,
                localNode,
                new TestClockService(new HybridClockImpl()),
                EventLog.NOOP
        );

        computeExecutor.start();
    }

    @AfterEach
    void tearDown() {
        computeExecutor.stop();
    }

    @Test
    void threadInterruption() {
        JobExecutionInternal<?> execution = executeJob(InterruptingJob.class);
        JobState executingState = await().until(execution::state, jobStateWithStatus(EXECUTING));
        assertThat(execution.cancel(), is(true));
        // InterruptingJob catches interruption and completes normally — cooperative cancellation honors the result.
        await().until(
                execution::state,
                jobStateWithStatusAndCreateTimeStartTime(COMPLETED, executingState.createTime(), executingState.startTime())
        );
    }

    private static class InterruptingJob implements ComputeJob<Object[], Integer> {
        @Override
        public CompletableFuture<Integer> executeAsync(JobExecutionContext context, Object... args) {
            while (true) {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return completedFuture(0);
                }
            }
        }
    }

    @Test
    void cooperativeCancellation() {
        JobExecutionInternal<?> execution = executeJob(CancellingJob.class);
        JobState executingState = await().until(execution::state, jobStateWithStatus(EXECUTING));
        assertThat(execution.cancel(), is(true));
        // CancellingJob checks isCancelled() and completes normally — cooperative cancellation honors the result.
        await().until(
                execution::state,
                jobStateWithStatusAndCreateTimeStartTime(COMPLETED, executingState.createTime(), executingState.startTime())
        );
    }

    private static class CancellingJob implements ComputeJob<Object[], Integer> {
        @Override
        public CompletableFuture<Integer> executeAsync(JobExecutionContext context, Object... args) {
            while (true) {
                try {
                    if (context.isCancelled()) {
                        return completedFuture(0);
                    }
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }
    }

    @Test
    void cancelAwareCancellation() {
        JobExecutionInternal<?> execution = executeJob(CancelAwareJob.class);
        JobState executingState = await().until(execution::state, jobStateWithStatus(EXECUTING));
        assertThat(execution.cancel(), is(true));
        // CancelAwareJob catches interruption and throws CancellationException — job is canceled.
        await().until(
                execution::state,
                jobStateWithStatusAndCreateTimeStartTime(CANCELED, executingState.createTime(), executingState.startTime())
        );
    }

    private static class CancelAwareJob implements ComputeJob<Object[], Integer> {
        @Override
        public CompletableFuture<Integer> executeAsync(JobExecutionContext context, Object... args) {
            while (true) {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    throw new CancellationException();
                }
            }
        }
    }

    @Test
    void retryJobFail() {
        int maxRetries = 5;
        RetryJobFail.runTimes.set(0);

        JobExecutionInternal<?> execution = executeJob(
                ExecutionOptions.builder().maxRetries(maxRetries).build(),
                RetryJobFail.class,
                null
        );

        await().until(execution::state, jobStateWithStatus(FAILED));

        assertThat(RetryJobFail.runTimes.get(), is(maxRetries + 1));
    }

    private static class RetryJobFail implements ComputeJob<Object, Integer> {
        static final AtomicInteger runTimes = new AtomicInteger();

        @Override
        public CompletableFuture<Integer> executeAsync(JobExecutionContext context, Object args) {
            runTimes.incrementAndGet();

            throw new RuntimeException();
        }
    }

    @Test
    void retryJobSuccess() {
        int maxRetries = 5;
        RetryJobSuccess.runTimes.set(0);

        JobExecutionInternal<?> execution = executeJob(
                ExecutionOptions.builder().maxRetries(maxRetries).build(),
                RetryJobSuccess.class,
                SharedComputeUtils.marshalArgOrResult(maxRetries, null)
        );

        await().until(execution::state, jobStateWithStatus(COMPLETED));

        assertThat(RetryJobSuccess.runTimes.get(), is(maxRetries + 1));
    }

    private static class RetryJobSuccess implements ComputeJob<Integer, Integer> {
        static final AtomicInteger runTimes = new AtomicInteger();

        @Override
        public CompletableFuture<Integer> executeAsync(JobExecutionContext context, Integer args) {
            int maxRetries = args;

            if (runTimes.incrementAndGet() <= maxRetries) {
                throw new RuntimeException();
            }

            return completedFuture(0);
        }

    }

    @Test
    void runJobOnce() {
        int maxRetries = 5;
        JobSuccess.runTimes.set(0);

        JobExecutionInternal<?> execution = executeJob(
                ExecutionOptions.builder().maxRetries(maxRetries).build(),
                JobSuccess.class,
                null
        );

        await().until(execution::state, jobStateWithStatus(COMPLETED));

        assertThat(execution.resultAsync().thenApply(h -> SharedComputeUtils.unmarshalResult(h, null, null)), willBe(1));
        assertThat(JobSuccess.runTimes.get(), is(1));
    }

    private static class JobSuccess implements ComputeJob<Object, Integer> {
        static final AtomicInteger runTimes = new AtomicInteger();

        @Override
        public CompletableFuture<Integer> executeAsync(JobExecutionContext context, Object arg) {
            return completedFuture(runTimes.incrementAndGet());
        }
    }

    @Test
    void findJobArgumentType() {
        assertThat(getJobExecuteArgumentType(RetryJobSuccess.class), is(Integer.class));
    }

    private static class Task implements MapReduceTask<String, String, String, String> {

        @Override
        public CompletableFuture<List<MapReduceJob<String, String>>> splitAsync(TaskExecutionContext taskContext, @Nullable String input) {
            return null;
        }

        @Override
        public CompletableFuture<String> reduceAsync(TaskExecutionContext taskContext, Map<UUID, String> results) {
            return null;
        }
    }

    @Test
    void findTaskArgumentType() {
        assertThat(getTaskSplitArgumentType(Task.class), is(String.class));
    }

    @Test
    void cancelCompletedJob() {
        JobExecutionInternal<?> execution = executeJob(SimpleJob.class);

        await().until(execution::state, jobStateWithStatus(COMPLETED));

        assertThat(execution.cancel(), is(false));
    }

    private static class SimpleJob implements ComputeJob<Object[], Integer> {
        @Override
        public CompletableFuture<Integer> executeAsync(JobExecutionContext context, Object... args) {
            return completedFuture(0);
        }
    }

    /**
     * Async job returns a future immediately. A background thread polls isCancelled(),
     * then completes the future after a brief delay. On cancel, the job should end in
     * COMPLETED state with a successful result.
     *
     * <p>This test catches the bug where {@code jobFuture.cancel(true)} in
     * {@code QueueEntry.interrupt()} overrides the job's result with CancellationException,
     * preventing the job from completing normally after cooperative cancellation.
     */
    @Test
    void cancelAsyncJobThatCompletesAfterCancellation() {
        JobExecutionInternal<?> execution = executeJob(AsyncDelayedCompleteJob.class);

        JobState executingState = await().until(execution::state, jobStateWithStatus(EXECUTING));
        assertThat(execution.cancel(), is(true));

        // The job detects cancellation via isCancelled(), does brief cleanup, then completes with a result.
        // Cooperative cancellation should honor the result — status must be COMPLETED, not CANCELED.
        await().until(
                execution::state,
                jobStateWithStatusAndCreateTimeStartTime(COMPLETED, executingState.createTime(), executingState.startTime())
        );
        assertThat(execution.resultAsync().thenApply(h -> SharedComputeUtils.unmarshalResult(h, null, null)), willBe(42));
    }

    /** Async job that returns a future immediately. A background thread polls isCancelled(), then completes. */
    private static class AsyncDelayedCompleteJob implements ComputeJob<Object[], Integer> {
        @Override
        public CompletableFuture<Integer> executeAsync(JobExecutionContext context, Object... args) {
            CompletableFuture<Integer> result = new CompletableFuture<>();

            Thread thread = new Thread(() -> {
                while (!context.isCancelled()) {
                    try {
                        Thread.sleep(50);
                    } catch (InterruptedException e) {
                        break;
                    }
                }

                // Simulate cleanup after cancellation
                try {
                    Thread.sleep(500);
                } catch (InterruptedException ignored) {
                    // ignored
                }

                result.complete(42);
            });
            thread.setDaemon(true);
            thread.start();

            return result;
        }
    }

    @Test
    void cancelAsyncJobThatThrowsOnCancellation() {
        JobExecutionInternal<?> execution = executeJob(AsyncThrowOnCancelJob.class);

        JobState executingState = await().until(execution::state, jobStateWithStatus(EXECUTING));
        assertThat(execution.cancel(), is(true));

        // The job detects cancellation and throws RuntimeException (not CancellationException).
        // Since it's not a CancellationException, the job transitions to FAILED.
        await().until(
                execution::state,
                jobStateWithStatusAndCreateTimeStartTime(FAILED, executingState.createTime(), executingState.startTime())
        );
        assertThat(execution.resultAsync(), willThrow(RuntimeException.class, "Job cancelled"));
    }

    @Test
    void cancelJobWithRetriesDoesNotRetry() {
        int maxRetries = 5;
        AsyncThrowOnCancelJob.runTimes.set(0);

        JobExecutionInternal<?> execution = executeJob(
                ExecutionOptions.builder().maxRetries(maxRetries).build(),
                AsyncThrowOnCancelJob.class,
                null
        );

        await().until(execution::state, jobStateWithStatus(EXECUTING));
        assertThat(execution.cancel(), is(true));

        // The job throws RuntimeException (not CancellationException) after cancel.
        // Even though retries are configured, the job must not be retried because cancel was requested.
        await().until(execution::state, jobStateWithStatus(FAILED));
        assertThat(AsyncThrowOnCancelJob.runTimes.get(), is(1));
    }

    /** Async job that throws RuntimeException when cancellation is detected. */
    private static class AsyncThrowOnCancelJob implements ComputeJob<Object[], Integer> {
        static final AtomicInteger runTimes = new AtomicInteger();

        @Override
        public CompletableFuture<Integer> executeAsync(JobExecutionContext context, Object... args) {
            runTimes.incrementAndGet();
            CompletableFuture<Integer> result = new CompletableFuture<>();

            Thread thread = new Thread(() -> {
                while (!context.isCancelled()) {
                    try {
                        Thread.sleep(50);
                    } catch (InterruptedException e) {
                        break;
                    }
                }

                result.completeExceptionally(new RuntimeException("Job cancelled"));
            });
            thread.setDaemon(true);
            thread.start();

            return result;
        }
    }

    /**
     * Job throws CancellationException without external cancel() call.
     * The exception type alone should move the job to CANCELED.
     */
    @Test
    void jobThrowingCancellationExceptionTransitionsToCanceled() {
        JobExecutionInternal<?> execution = executeJob(SelfCancellingJob.class);

        await().until(
                execution::state,
                jobStateWithStatus(CANCELED)
        );
        assertThat(execution.resultAsync(), willThrow(CancellationException.class));
    }

    /** Job that immediately throws CancellationException without external cancel(). */
    private static class SelfCancellingJob implements ComputeJob<Object[], Void> {
        @Override
        public CompletableFuture<Void> executeAsync(JobExecutionContext context, Object... args) {
            return CompletableFuture.failedFuture(new CancellationException("self-cancelled"));
        }
    }

    @Test
    void cancelAsyncJobViaCancellationToken() {
        JobExecutionInternal<?> execution = executeJob(CancellationTokenJob.class);

        JobState executingState = await().until(execution::state, jobStateWithStatus(EXECUTING));

        // The job registers a cancel action on the cancellation token that completes the future.
        // When cancel() is called, the token fires the action synchronously, completing the job with a result.
        execution.cancel();

        await().until(
                execution::state,
                jobStateWithStatusAndCreateTimeStartTime(COMPLETED, executingState.createTime(), executingState.startTime())
        );

        assertThat(execution.resultAsync().thenApply(h -> SharedComputeUtils.unmarshalResult(h, null, null)), willBe(42));
    }

    /** Async job that uses cancellationToken() to react to cancellation and complete the future. */
    private static class CancellationTokenJob implements ComputeJob<Object[], Integer> {
        @Override
        public CompletableFuture<Integer> executeAsync(JobExecutionContext context, Object... args) {
            CompletableFuture<Integer> result = new CompletableFuture<>();

            CancelHandleHelper.addCancelAction(
                    context.cancellationToken(),
                    () -> result.complete(42),
                    result
            );

            return result;
        }
    }

    private JobExecutionInternal<?> executeJob(Class<?> jobClass) {
        return executeJob(ExecutionOptions.DEFAULT, jobClass, null);
    }

    private JobExecutionInternal<?> executeJob(ExecutionOptions options, Class<?> jobClass, @Nullable ComputeJobDataHolder arg) {
        return computeExecutor.executeJob(
                options,
                jobClass.getName(),
                jobClassLoader,
                ComputeEventMetadata.builder(),
                arg
        );
    }
}
