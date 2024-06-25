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
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willBe;
import static org.apache.ignite.internal.testframework.matchers.JobStateMatcher.jobStateWithStatus;
import static org.apache.ignite.internal.testframework.matchers.JobStateMatcher.jobStateWithStatusAndCreateTimeStartTime;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.Ignite;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.JobExecutionContext;
import org.apache.ignite.compute.JobState;
import org.apache.ignite.internal.compute.ExecutionOptions;
import org.apache.ignite.internal.compute.configuration.ComputeConfiguration;
import org.apache.ignite.internal.compute.state.InMemoryComputeStateMachine;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
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

    private ComputeExecutor computeExecutor;

    @BeforeEach
    void setUp() {
        InMemoryComputeStateMachine stateMachine = new InMemoryComputeStateMachine(computeConfiguration, "testNode");
        computeExecutor = new ComputeExecutorImpl(ignite, stateMachine, computeConfiguration);
        computeExecutor.start();
    }

    @AfterEach
    void tearDown() {
        computeExecutor.stop();
    }

    @Test
    void threadInterruption() {
        JobExecutionInternal<Integer> execution = computeExecutor.executeJob(
                ExecutionOptions.DEFAULT,
                InterruptingJob.class,
                null
        );
        JobState executingState = await().until(execution::state, jobStateWithStatus(EXECUTING));
        assertThat(execution.cancel(), is(true));
        await().until(
                execution::state,
                jobStateWithStatusAndCreateTimeStartTime(CANCELED, executingState.createTime(), executingState.startTime())
        );
    }

    private static class InterruptingJob implements ComputeJob<Integer> {
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
        JobExecutionInternal<Integer> execution = computeExecutor.executeJob(
                ExecutionOptions.DEFAULT,
                CancellingJob.class,
                null
        );
        JobState executingState = await().until(execution::state, jobStateWithStatus(EXECUTING));
        assertThat(execution.cancel(), is(true));
        await().until(
                execution::state,
                jobStateWithStatusAndCreateTimeStartTime(CANCELED, executingState.createTime(), executingState.startTime())
        );
    }

    private static class CancellingJob implements ComputeJob<Integer> {
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
        AtomicInteger runTimes = new AtomicInteger();

        int maxRetries = 5;

        JobExecutionInternal<Integer> execution = computeExecutor.executeJob(
                ExecutionOptions.builder().maxRetries(maxRetries).build(),
                RetryJobFail.class,
                null,
                runTimes
        );

        await().until(execution::state, jobStateWithStatus(FAILED));

        assertThat(runTimes.get(), is(maxRetries + 1));
    }

    private static class RetryJobFail implements ComputeJob<Integer> {

        @Override
        public CompletableFuture<Integer> executeAsync(JobExecutionContext context, Object... args) {
            AtomicInteger runTimes = (AtomicInteger) args[0];
            runTimes.incrementAndGet();
            throw new RuntimeException();
        }
    }

    @Test
    void retryJobSuccess() {
        AtomicInteger runTimes = new AtomicInteger();

        int maxRetries = 5;

        JobExecutionInternal<Integer> execution = computeExecutor.executeJob(
                ExecutionOptions.builder().maxRetries(maxRetries).build(),
                RetryJobSuccess.class,
                null,
                runTimes,
                maxRetries
        );

        await().until(execution::state, jobStateWithStatus(COMPLETED));

        assertThat(runTimes.get(), is(maxRetries + 1));
    }

    private static class RetryJobSuccess implements ComputeJob<Integer> {

        @Override
        public CompletableFuture<Integer> executeAsync(JobExecutionContext context, Object... args) {
            AtomicInteger runTimes = (AtomicInteger) args[0];
            int maxRetries = (int) args[1];
            if (runTimes.incrementAndGet() <= maxRetries) {
                throw new RuntimeException();
            }
            return completedFuture(0);
        }

    }

    @Test
    void runJobOnce() {
        AtomicInteger runTimes = new AtomicInteger();

        int maxRetries = 5;

        JobExecutionInternal<Integer> execution = computeExecutor.executeJob(
                ExecutionOptions.builder().maxRetries(maxRetries).build(),
                JobSuccess.class,
                null,
                runTimes
        );

        await().until(execution::state, jobStateWithStatus(COMPLETED));

        assertThat(execution.resultAsync(), willBe(1));
        assertThat(runTimes.get(), is(1));
    }

    private static class JobSuccess implements ComputeJob<Integer> {

        @Override
        public CompletableFuture<Integer> executeAsync(JobExecutionContext context, Object... args) {
            AtomicInteger runTimes = (AtomicInteger) args[0];
            return completedFuture(runTimes.incrementAndGet());
        }

    }

    @Test
    void cancelCompletedJob() {
        JobExecutionInternal<Integer> execution = computeExecutor.executeJob(
                ExecutionOptions.DEFAULT,
                SimpleJob.class,
                null
        );

        await().until(execution::state, jobStateWithStatus(COMPLETED));

        assertThat(execution.cancel(), is(false));
    }

    private static class SimpleJob implements ComputeJob<Integer> {
        @Override
        public CompletableFuture<Integer> executeAsync(JobExecutionContext context, Object... args) {
            return completedFuture(0);
        }
    }
}
