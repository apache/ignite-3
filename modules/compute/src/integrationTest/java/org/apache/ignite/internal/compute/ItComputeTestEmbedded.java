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

import static java.util.concurrent.CompletableFuture.allOf;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;
import static org.apache.ignite.internal.IgniteExceptionTestUtils.assertPublicCheckedException;
import static org.apache.ignite.internal.IgniteExceptionTestUtils.assertPublicException;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willBe;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.testframework.matchers.JobStatusMatcher.jobStatusWithState;
import static org.apache.ignite.lang.ErrorGroups.Common.INTERNAL_ERR;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.DeploymentUnit;
import org.apache.ignite.compute.JobExecution;
import org.apache.ignite.compute.JobExecutionContext;
import org.apache.ignite.compute.JobExecutionOptions;
import org.apache.ignite.compute.JobState;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.lang.IgniteInternalCheckedException;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.internal.util.ExceptionUtils;
import org.apache.ignite.lang.IgniteCheckedException;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.table.KeyValueView;
import org.apache.ignite.table.Table;
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

    @Test
    void cancelsJobLocally() {
        IgniteImpl entryNode = node(0);

        JobExecution<String> execution = entryNode.compute()
                .submit(Set.of(entryNode.node()), units(), WaitLatchJob.class.getName(), new CountDownLatch(1));

        await().until(execution::statusAsync, willBe(jobStatusWithState(JobState.EXECUTING)));

        assertThat(execution.cancelAsync(), willBe(true));

        await().until(execution::statusAsync, willBe(jobStatusWithState(JobState.CANCELED)));
    }

    @Test
    void cancelsJobRemotely() {
        IgniteImpl entryNode = node(0);

        JobExecution<String> execution = entryNode.compute()
                .submit(Set.of(node(1).node()), units(), WaitLatchJob.class.getName(), new CountDownLatch(1));

        await().until(execution::statusAsync, willBe(jobStatusWithState(JobState.EXECUTING)));

        assertThat(execution.cancelAsync(), willBe(true));

        await().until(execution::statusAsync, willBe(jobStatusWithState(JobState.CANCELED)));
    }

    @Test
    void changeExecutingJobPriorityLocally() {
        IgniteImpl entryNode = node(0);

        JobExecution<String> execution = entryNode.compute()
                .submit(Set.of(entryNode.node()), units(), WaitLatchJob.class.getName(), new CountDownLatch(1));
        await().until(execution::statusAsync, willBe(jobStatusWithState(JobState.EXECUTING)));

        assertThat(execution.changePriorityAsync(2), willBe(false));
        assertThat(execution.cancelAsync(), willBe(true));
    }

    @Test
    void changeExecutingJobPriorityRemotely() {
        IgniteImpl entryNode = node(0);

        JobExecution<String> execution = entryNode.compute()
                .submit(Set.of(node(1).node()), units(), WaitLatchJob.class.getName(), new CountDownLatch(1));
        await().until(execution::statusAsync, willBe(jobStatusWithState(JobState.EXECUTING)));

        assertThat(execution.changePriorityAsync(2), willBe(false));
        assertThat(execution.cancelAsync(), willBe(true));
    }

    @Test
    void changeJobPriorityLocally() {
        IgniteImpl entryNode = node(0);

        CountDownLatch countDownLatch = new CountDownLatch(1);

        // Start 1 task in executor with 1 thread
        JobExecution<String> execution1 = entryNode.compute()
                .submit(Set.of(entryNode.node()), units(), WaitLatchJob.class.getName(), countDownLatch);
        await().until(execution1::statusAsync, willBe(jobStatusWithState(JobState.EXECUTING)));

        // Start one more task
        JobExecution<String> execution2 = entryNode.compute()
                .submit(Set.of(entryNode.node()), units(), WaitLatchJob.class.getName(), new CountDownLatch(1));
        await().until(execution2::statusAsync, willBe(jobStatusWithState(JobState.QUEUED)));

        // Start third task
        JobExecution<String> execution3 = entryNode.compute()
                .submit(Set.of(entryNode.node()), units(), WaitLatchJob.class.getName(), countDownLatch);
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

        // Task 2 is not completed
        assertThat(execution2.resultAsync().isDone(), is(false));

        // Finish task 2
        assertThat(execution2.cancelAsync(), willBe(true));
    }

    @Test
    void executesJobLocallyWithOptions() {
        IgniteImpl entryNode = node(0);

        CountDownLatch countDownLatch = new CountDownLatch(1);

        // Start 1 task in executor with 1 thread
        JobExecution<String> execution1 = entryNode.compute()
                .submit(Set.of(entryNode.node()), units(), WaitLatchJob.class.getName(), countDownLatch);
        await().until(execution1::statusAsync, willBe(jobStatusWithState(JobState.EXECUTING)));

        // Start one more task
        JobExecution<String> execution2 = entryNode.compute()
                .submit(Set.of(entryNode.node()), units(), WaitLatchJob.class.getName(), new CountDownLatch(1));
        await().until(execution2::statusAsync, willBe(jobStatusWithState(JobState.QUEUED)));

        // Start third task it should be before task2 in the queue due to higher priority in options
        JobExecutionOptions options = JobExecutionOptions.builder().priority(1).maxRetries(2).build();
        JobExecution<String> execution3 = entryNode.compute()
                .submit(Set.of(entryNode.node()), units(), WaitLatchThrowExceptionOnFirstExecutionJob.class.getName(),
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

        // Task 3 should be executed 2 times
        assertEquals(2, WaitLatchThrowExceptionOnFirstExecutionJob.counter.get());

        // Task 2 is not completed
        assertThat(execution2.resultAsync().isDone(), is(false));

        // Cancel task2
        assertThat(execution2.cancelAsync(), willBe(true));
    }

    @Test
    void shouldNotConvertIgniteException() {
        IgniteImpl entryNode = node(0);

        IgniteException exception = new IgniteException(INTERNAL_ERR, "Test exception");

        IgniteException ex = assertThrows(IgniteException.class, () -> entryNode.compute()
                .execute(Set.of(entryNode.node()), units(), CustomFailingJob.class.getName(), exception));

        assertPublicException(ex, exception.code(), exception.getMessage());
    }

    @Test
    void shouldNotConvertIgniteCheckedException() {
        IgniteImpl entryNode = node(0);

        IgniteCheckedException exception = new IgniteCheckedException(INTERNAL_ERR, "Test exception");

        IgniteCheckedException ex = assertThrows(IgniteCheckedException.class, () -> entryNode.compute()
                .execute(Set.of(entryNode.node()), units(), CustomFailingJob.class.getName(), exception));

        assertPublicCheckedException(ex, exception.code(), exception.getMessage());
    }

    private static Stream<Arguments> privateExceptions() {
        return Stream.of(
                Arguments.of(new IgniteInternalException(INTERNAL_ERR, "Test exception")),
                Arguments.of(new IgniteInternalCheckedException(INTERNAL_ERR, "Test exception")),
                Arguments.of(new RuntimeException("Test exception")),
                Arguments.of(new Exception("Test exception"))
        );
    }

    @ParameterizedTest
    @MethodSource("privateExceptions")
    void shouldConvertToComputeException(Throwable throwable) {
        IgniteImpl entryNode = node(0);

        IgniteException ex = assertThrows(IgniteException.class, () -> entryNode.compute()
                .execute(Set.of(entryNode.node()), units(), CustomFailingJob.class.getName(), throwable));

        assertComputeException(ex, throwable);
    }

    @ParameterizedTest
    @MethodSource("targetNodeIndexes")
    void executesSyncKvGetPutFromJob(int targetNodeIndex) {
        createTestTableWithOneRow();

        int entryNodeIndex = 0;

        IgniteImpl entryNode = node(entryNodeIndex);
        IgniteImpl targetNode = node(targetNodeIndex);

        assertDoesNotThrow(() -> entryNode.compute().execute(Set.of(targetNode.node()), List.of(), PerformSyncKvGetPutJob.class.getName()));
    }

    @Test
    void executesNullReturningJobViaSyncBroadcast() {
        int entryNodeIndex = 0;

        IgniteImpl entryNode = node(entryNodeIndex);

        Map<ClusterNode, Object> results = entryNode.compute()
                .executeBroadcast(new HashSet<>(entryNode.clusterNodes()), List.of(), NullReturningJob.class.getName());

        assertThat(results.keySet(), equalTo(new HashSet<>(entryNode.clusterNodes())));
        assertThat(new HashSet<>(results.values()), contains(nullValue()));
    }

    @Test
    void executesNullReturningJobViaAsyncBroadcast() {
        int entryNodeIndex = 0;

        IgniteImpl entryNode = node(entryNodeIndex);

        CompletableFuture<Map<ClusterNode, Object>> resultsFuture = entryNode.compute()
                .executeBroadcastAsync(new HashSet<>(entryNode.clusterNodes()), List.of(), NullReturningJob.class.getName());
        assertThat(resultsFuture, willCompleteSuccessfully());
        Map<ClusterNode, Object> results = resultsFuture.join();

        assertThat(results.keySet(), equalTo(new HashSet<>(entryNode.clusterNodes())));
        assertThat(new HashSet<>(results.values()), contains(nullValue()));
    }

    @Test
    void executesNullReturningJobViaSubmitBroadcast() {
        int entryNodeIndex = 0;

        IgniteImpl entryNode = node(entryNodeIndex);

        Map<ClusterNode, JobExecution<Object>> executionsMap = entryNode.compute()
                .submitBroadcast(new HashSet<>(entryNode.clusterNodes()), List.of(), NullReturningJob.class.getName());
        assertThat(executionsMap.keySet(), equalTo(new HashSet<>(entryNode.clusterNodes())));

        List<JobExecution<Object>> executions = new ArrayList<>(executionsMap.values());
        List<CompletableFuture<Object>> futures = executions.stream()
                .map(JobExecution::resultAsync)
                .collect(toList());
        assertThat(allOf(futures.toArray(CompletableFuture[]::new)), willCompleteSuccessfully());

        assertThat(futures.stream().map(CompletableFuture::join).collect(toSet()), contains(nullValue()));
    }

    private Stream<Arguments> targetNodeIndexes() {
        return IntStream.range(0, initialNodes()).mapToObj(Arguments::of);
    }

    private static class CustomFailingJob implements ComputeJob<String> {
        @Override
        public String execute(JobExecutionContext context, Object... args) {
            throw ExceptionUtils.sneakyThrow((Throwable) args[0]);
        }
    }

    private static class WaitLatchJob implements ComputeJob<String> {
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

    private static class PerformSyncKvGetPutJob implements ComputeJob<Void> {
        @Override
        public Void execute(JobExecutionContext context, Object... args) {
            Table table = context.ignite().tables().table("test");
            KeyValueView<Integer, Integer> view = table.keyValueView(Integer.class, Integer.class);

            view.get(null, 1);
            view.put(null, 1, 1);

            return null;
        }
    }

    private static class NullReturningJob implements ComputeJob<Void> {
        @Override
        public Void execute(JobExecutionContext context, Object... args) {
            return null;
        }
    }
}
