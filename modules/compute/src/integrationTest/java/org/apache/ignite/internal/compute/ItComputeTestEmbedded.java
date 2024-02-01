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

package org.apache.ignite.internal.compute;

import static java.util.stream.Collectors.joining;
import static org.apache.ignite.internal.compute.utils.ComputeTestUtils.assertPublicException;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willBe;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.testframework.matchers.JobStatusMatcher.jobStatusWithState;
import static org.apache.ignite.lang.ErrorGroups.Compute.CLASS_INITIALIZATION_ERR;
import static org.apache.ignite.lang.ErrorGroups.Compute.COMPUTE_ERR_GROUP;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.Ignite;
import org.apache.ignite.compute.ComputeException;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.DeploymentUnit;
import org.apache.ignite.compute.JobExecution;
import org.apache.ignite.compute.JobExecutionContext;
import org.apache.ignite.compute.JobExecutionOptions;
import org.apache.ignite.compute.JobState;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.lang.ErrorGroup;
import org.apache.ignite.lang.IgniteException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Integration tests for Compute functionality in embedded Ignite mode.
 */
@SuppressWarnings("resource")
class ItComputeTestEmbedded extends ItComputeBaseTest {

    @Override
    protected List<DeploymentUnit> units() {
        return List.of();
    }

    @Override
    protected String concatJobClassName() {
        return ConcatJob.class.getName();
    }

    @Override
    protected String getNodeNameJobClassName() {
        return GetNodeNameJob.class.getName();
    }

    @Override
    protected String failingJobClassName() {
        return FailingJob.class.getName();
    }

    @ParameterizedTest
    @MethodSource("wrongJobClassArguments")
    void executesWrongJobClassLocally(String jobClassName, ErrorGroup errorGroup, int errorCode, String msg) {
        IgniteImpl entryNode = node(0);

        IgniteException ex = assertThrows(IgniteException.class, () -> entryNode.compute()
                .execute(Set.of(entryNode.node()), units(), jobClassName));

        assertPublicException(ex, ComputeException.class, errorGroup, errorCode, msg);
    }

    @ParameterizedTest
    @MethodSource("wrongJobClassArguments")
    void executesWrongJobClassLocallyAsync(String jobClassName, ErrorGroup errorGroup, int errorCode, String msg) {
        IgniteImpl entryNode = node(0);

        ExecutionException ex = assertThrows(ExecutionException.class, () -> entryNode.compute()
                .executeAsync(Set.of(entryNode.node()), units(), jobClassName)
                .resultAsync()
                .get(1, TimeUnit.SECONDS));

        assertPublicException(ex, ComputeException.class, errorGroup, errorCode, msg);
    }

    @ParameterizedTest
    @MethodSource("wrongJobClassArguments")
    void executesWrongJobClassOnRemoteNodes(String jobClassName, ErrorGroup errorGroup, int errorCode, String msg) {
        Ignite entryNode = node(0);

        IgniteException ex = assertThrows(IgniteException.class, () -> entryNode.compute()
                .execute(Set.of(node(1).node(), node(2).node()), units(), jobClassName));

        assertPublicException(ex, ComputeException.class, errorGroup, errorCode, msg);
    }

    @ParameterizedTest
    @MethodSource("wrongJobClassArguments")
    void executesWrongJobClassOnRemoteNodesAsync(String jobClassName, ErrorGroup errorGroup, int errorCode, String msg) {
        Ignite entryNode = node(0);

        ExecutionException ex = assertThrows(ExecutionException.class, () -> entryNode.compute()
                .executeAsync(Set.of(node(1).node(), node(2).node()), units(), jobClassName)
                .resultAsync()
                .get(1, TimeUnit.SECONDS));

        assertPublicException(ex, ComputeException.class, errorGroup, errorCode, msg);
    }

    @Test
    void cancelsJobLocally() {
        IgniteImpl entryNode = node(0);

        JobExecution<String> execution = entryNode.compute()
                .executeAsync(Set.of(entryNode.node()), units(), WaitLatchJob.class.getName(), new CountDownLatch(1));

        await().until(execution::statusAsync, willBe(jobStatusWithState(JobState.EXECUTING)));

        assertThat(execution.cancelAsync(), willBe(true));

        await().until(execution::statusAsync, willBe(jobStatusWithState(JobState.CANCELED)));
    }

    @Test
    void cancelsJobRemotely() {
        IgniteImpl entryNode = node(0);

        JobExecution<String> execution = entryNode.compute()
                .executeAsync(Set.of(node(1).node()), units(), WaitLatchJob.class.getName(), new CountDownLatch(1));

        await().until(execution::statusAsync, willBe(jobStatusWithState(JobState.EXECUTING)));

        assertThat(execution.cancelAsync(), willBe(true));

        await().until(execution::statusAsync, willBe(jobStatusWithState(JobState.CANCELED)));
    }

    @Test
    void changeExecutingJobPriorityLocally() {
        IgniteImpl entryNode = node(0);

        JobExecution<String> execution = entryNode.compute()
                .executeAsync(Set.of(entryNode.node()), units(), WaitLatchJob.class.getName(), new CountDownLatch(1));
        await().until(execution::statusAsync, willBe(jobStatusWithState(JobState.EXECUTING)));

        assertThat(execution.changePriorityAsync(2), willBe(false));
    }

    @Test
    void changeExecutingJobPriorityRemotely() {
        IgniteImpl entryNode = node(0);

        JobExecution<String> execution = entryNode.compute()
                .executeAsync(Set.of(node(1).node()), units(), WaitLatchJob.class.getName(), new CountDownLatch(1));
        await().until(execution::statusAsync, willBe(jobStatusWithState(JobState.EXECUTING)));

        assertThat(execution.changePriorityAsync(2), willBe(false));
    }

    @Test
    void changeJobPriorityLocally() {
        IgniteImpl entryNode = node(0);

        CountDownLatch countDownLatch = new CountDownLatch(1);

        // Start 1 task in executor with 1 thread
        JobExecution<String> execution1 = entryNode.compute()
                .executeAsync(Set.of(entryNode.node()), units(), WaitLatchJob.class.getName(), countDownLatch);
        await().until(execution1::statusAsync, willBe(jobStatusWithState(JobState.EXECUTING)));

        // Start one more task
        JobExecution<String> execution2 = entryNode.compute()
                .executeAsync(Set.of(entryNode.node()), units(), WaitLatchJob.class.getName(), new CountDownLatch(1));
        await().until(execution2::statusAsync, willBe(jobStatusWithState(JobState.QUEUED)));

        // Start third task
        JobExecution<String> execution3 = entryNode.compute()
                .executeAsync(Set.of(entryNode.node()), units(), WaitLatchJob.class.getName(), countDownLatch);
        await().until(execution3::statusAsync, willBe(jobStatusWithState(JobState.QUEUED)));

        // Task 1 and 2 are not competed, in queue state
        assertThat(execution2.resultAsync().isDone(), is(false));
        assertThat(execution3.resultAsync().isDone(), is(false));

        // Change priority of task 3, so it should be executed before task 2
        assertThat(execution3.changePriorityAsync(2), willBe(true));

        // Run 1 and 3 task
        countDownLatch.countDown();

        // Tasks 1 and 3 completed successfully
        assertThat(execution1.resultAsync(), willCompleteSuccessfully());
        assertThat(execution3.resultAsync(), willCompleteSuccessfully());
        assertThat(execution1.resultAsync().isDone(), is(true));
        assertThat(execution3.resultAsync().isDone(), is(true));

        // Task 2 is not completed
        assertThat(execution2.resultAsync().isDone(), is(false));
    }

    @Test
    void executesJobLocallyWithOptions() {
        IgniteImpl entryNode = node(0);

        CountDownLatch countDownLatch = new CountDownLatch(1);

        // Start 1 task in executor with 1 thread
        JobExecution<String> execution1 = entryNode.compute()
                .executeAsync(Set.of(entryNode.node()), units(), WaitLatchJob.class.getName(), countDownLatch);
        await().until(execution1::statusAsync, willBe(jobStatusWithState(JobState.EXECUTING)));

        // Start one more task
        JobExecution<String> execution2 = entryNode.compute()
                .executeAsync(Set.of(entryNode.node()), units(), WaitLatchJob.class.getName(), new CountDownLatch(1));
        await().until(execution2::statusAsync, willBe(jobStatusWithState(JobState.QUEUED)));

        // Start third task it should be before task2 in the queue due to higher priority in options
        JobExecutionOptions options = JobExecutionOptions.builder().priority(1).maxRetries(2).build();
        JobExecution<String> execution3 = entryNode.compute()
                .executeAsync(Set.of(entryNode.node()), units(), WaitLatchThrowExceptionOnFirstExecutionJob.class.getName(),
                        options, countDownLatch);
        await().until(execution3::statusAsync, willBe(jobStatusWithState(JobState.QUEUED)));

        // Task 1 and 2 are not competed, in queue state
        assertThat(execution2.resultAsync().isDone(), is(false));
        assertThat(execution3.resultAsync().isDone(), is(false));

        // Reset counter
        WaitLatchThrowExceptionOnFirstExecutionJob.counter.set(0);

        // Run 1 and 3 task
        countDownLatch.countDown();

        // Tasks 1 and 3 completed successfully
        assertThat(execution1.resultAsync(), willCompleteSuccessfully());
        assertThat(execution3.resultAsync(), willCompleteSuccessfully());
        assertThat(execution1.resultAsync().isDone(), is(true));
        assertThat(execution3.resultAsync().isDone(), is(true));

        // Task 3 should be executed 2 times
        assertEquals(2, WaitLatchThrowExceptionOnFirstExecutionJob.counter.get());

        // Task 2 is not completed
        assertThat(execution2.resultAsync().isDone(), is(false));

        // Cancel task2
        execution2.cancelAsync().join();
    }

    private static class ConcatJob implements ComputeJob<String> {
        /** {@inheritDoc} */
        @Override
        public String execute(JobExecutionContext context, Object... args) {
            return Arrays.stream(args)
                    .map(Object::toString)
                    .collect(joining());
        }
    }

    private static class GetNodeNameJob implements ComputeJob<String> {
        /** {@inheritDoc} */
        @Override
        public String execute(JobExecutionContext context, Object... args) {
            return context.ignite().name();
        }
    }

    private static class FailingJob implements ComputeJob<String> {
        /** {@inheritDoc} */
        @Override
        public String execute(JobExecutionContext context, Object... args) {
            throw new JobException("Oops", new Exception());
        }
    }

    private static class JobException extends RuntimeException {
        private JobException(String message, Throwable cause) {
            super(message, cause);
        }
    }

    private static List<Arguments> wrongJobClassArguments() {
        return List.of(
                Arguments.of("org.example.NonExistentJob", COMPUTE_ERR_GROUP, CLASS_INITIALIZATION_ERR, "Cannot load job class by name"),
                Arguments.of(NonComputeJob.class.getName(), COMPUTE_ERR_GROUP, CLASS_INITIALIZATION_ERR,
                        "does not implement ComputeJob interface"),
                Arguments.of(NonEmptyConstructorJob.class.getName(), COMPUTE_ERR_GROUP, CLASS_INITIALIZATION_ERR,
                        "Cannot instantiate job")
        );
    }

    private static class NonComputeJob {
        public String execute(JobExecutionContext context, Object... args) {
            return "";
        }
    }

    private static class NonEmptyConstructorJob implements ComputeJob<String> {
        private NonEmptyConstructorJob(String s) {
        }

        /** {@inheritDoc} */
        @Override
        public String execute(JobExecutionContext context, Object... args) {
            return "";
        }
    }

    private static class WaitLatchJob implements ComputeJob<String> {

        /** {@inheritDoc} */
        @Override
        public String execute(JobExecutionContext context, Object... args) {
            try {
                ((CountDownLatch) args[0]).await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            return null;
        }
    }

    private static class WaitLatchThrowExceptionOnFirstExecutionJob implements ComputeJob<String> {

        static final AtomicInteger counter = new AtomicInteger(0);

        /** {@inheritDoc} */
        @Override
        public String execute(JobExecutionContext context, Object... args) {
            try {
                ((CountDownLatch) args[0]).await();
                if (counter.incrementAndGet() == 1) {
                    throw new RuntimeException();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            return null;
        }
    }
}
