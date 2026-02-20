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
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willBe;
import static org.apache.ignite.internal.testframework.matchers.JobStateMatcher.jobStateWithStatus;
import static org.apache.ignite.internal.testframework.matchers.JobStateMatcher.jobStateWithStatusAndCreateTimeStartTime;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.Answers.RETURNS_DEEP_STUBS;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.Ignite;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.JobExecutionContext;
import org.apache.ignite.compute.JobState;
import org.apache.ignite.compute.task.MapReduceJob;
import org.apache.ignite.compute.task.MapReduceTask;
import org.apache.ignite.compute.task.TaskExecutionContext;
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
import org.apache.ignite.internal.network.TopologyService;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
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

    @Mock(answer = RETURNS_DEEP_STUBS)
    private TopologyService topologyService;

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
                topologyService,
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
        JobExecutionInternal<?> execution = computeExecutor.executeJob(
                ExecutionOptions.DEFAULT,
                InterruptingJob.class.getName(),
                jobClassLoader,
                ComputeEventMetadata.builder(),
                null
        );
        JobState executingState = await().until(execution::state, jobStateWithStatus(EXECUTING));
        assertThat(execution.cancel(), is(true));
        await().until(
                execution::state,
                jobStateWithStatusAndCreateTimeStartTime(CANCELED, executingState.createTime(), executingState.startTime())
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
        JobExecutionInternal<?> execution = computeExecutor.executeJob(
                ExecutionOptions.DEFAULT,
                CancellingJob.class.getName(),
                jobClassLoader,
                ComputeEventMetadata.builder(),
                null
        );
        JobState executingState = await().until(execution::state, jobStateWithStatus(EXECUTING));
        assertThat(execution.cancel(), is(true));
        await().until(
                execution::state,
                jobStateWithStatusAndCreateTimeStartTime(CANCELED, executingState.createTime(), executingState.startTime())
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
    void retryJobFail() {
        int maxRetries = 5;
        RetryJobFail.runTimes.set(0);

        JobExecutionInternal<?> execution = computeExecutor.executeJob(
                ExecutionOptions.builder().maxRetries(maxRetries).build(),
                RetryJobFail.class.getName(),
                jobClassLoader,
                ComputeEventMetadata.builder(),
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

        JobExecutionInternal<?> execution = computeExecutor.executeJob(
                ExecutionOptions.builder().maxRetries(maxRetries).build(),
                RetryJobSuccess.class.getName(),
                jobClassLoader,
                ComputeEventMetadata.builder(),
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

        JobExecutionInternal<?> execution = computeExecutor.executeJob(
                ExecutionOptions.builder().maxRetries(maxRetries).build(),
                JobSuccess.class.getName(),
                jobClassLoader,
                ComputeEventMetadata.builder(),
                null
        );

        await().until(execution::state, jobStateWithStatus(COMPLETED));

        assertThat(execution.resultAsync().thenApply(h -> SharedComputeUtils.unmarshalArgOrResult(h, null, null)), willBe(1));
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
        JobExecutionInternal<?> execution = computeExecutor.executeJob(
                ExecutionOptions.DEFAULT,
                SimpleJob.class.getName(),
                jobClassLoader,
                ComputeEventMetadata.builder(),
                null
        );

        await().until(execution::state, jobStateWithStatus(COMPLETED));

        assertThat(execution.cancel(), is(false));
    }

    private static class SimpleJob implements ComputeJob<Object[], Integer> {
        @Override
        public CompletableFuture<Integer> executeAsync(JobExecutionContext context, Object... args) {
            return completedFuture(0);
        }
    }
}
