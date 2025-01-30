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

import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static org.apache.ignite.compute.JobStatus.CANCELED;
import static org.apache.ignite.compute.JobStatus.COMPLETED;
import static org.apache.ignite.compute.JobStatus.EXECUTING;
import static org.apache.ignite.compute.JobStatus.FAILED;
import static org.apache.ignite.compute.JobStatus.QUEUED;
import static org.apache.ignite.internal.IgniteExceptionTestUtils.assertTraceableException;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureExceptionMatcher.willThrow;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.will;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willBe;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.testframework.matchers.JobExecutionMatcher.jobExecutionWithResultStatusAndNode;
import static org.apache.ignite.internal.testframework.matchers.JobStateMatcher.jobStateWithStatus;
import static org.apache.ignite.lang.ErrorGroups.Compute.CLASS_INITIALIZATION_ERR;
import static org.apache.ignite.lang.ErrorGroups.Compute.COMPUTE_JOB_FAILED_ERR;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.in;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.ignite.Ignite;
import org.apache.ignite.compute.BroadcastExecution;
import org.apache.ignite.compute.BroadcastJobTarget;
import org.apache.ignite.compute.ComputeException;
import org.apache.ignite.compute.IgniteCompute;
import org.apache.ignite.compute.JobDescriptor;
import org.apache.ignite.compute.JobExecution;
import org.apache.ignite.compute.JobExecutionOptions;
import org.apache.ignite.compute.JobTarget;
import org.apache.ignite.compute.TaskDescriptor;
import org.apache.ignite.compute.task.TaskExecution;
import org.apache.ignite.deployment.DeploymentUnit;
import org.apache.ignite.internal.ClusterPerClassIntegrationTest;
import org.apache.ignite.internal.table.partition.HashPartition;
import org.apache.ignite.internal.testframework.IgniteTestUtils;
import org.apache.ignite.internal.util.ExceptionUtils;
import org.apache.ignite.lang.CancelHandle;
import org.apache.ignite.lang.CancellationToken;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.lang.TableNotFoundException;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.table.Tuple;
import org.apache.ignite.table.mapper.Mapper;
import org.apache.ignite.table.partition.Partition;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Base integration tests for Compute functionality. To add new compute job for testing both in embedded and standalone mode, add the
 * corresponding job class to the jobs source set. The integration tests depend on this source set so the job class will be visible and it
 * will be automatically compiled and packed into the ignite-integration-test-jobs-1.0-SNAPSHOT.jar.
 */
public abstract class ItComputeBaseTest extends ClusterPerClassIntegrationTest {
    protected abstract List<DeploymentUnit> units();

    protected IgniteCompute compute() {
        return node(0).compute();
    }

    /**
     * Submits the job for execution, verifies that the execution future completes successfully and returns an execution object.
     *
     * @param <T> Job argument (T)ype.
     * @param <R> Job (R)esult type.
     * @param target Execution target.
     * @param descriptor Job descriptor.
     * @param arg Argument of the job.
     * @return Job execution object.
     */
    protected <T, R> JobExecution<R> submit(
            JobTarget target,
            JobDescriptor<T, R> descriptor,
            @Nullable T arg
    ) {
        return submit(target, descriptor, null, arg);
    }

    protected <T, R> JobExecution<R> submit(
            JobTarget target,
            JobDescriptor<T, R> descriptor,
            @Nullable CancellationToken cancellationToken,
            @Nullable T arg
    ) {
        CompletableFuture<JobExecution<R>> executionFut = compute().submitAsync(target, descriptor, arg, cancellationToken);
        assertThat(executionFut, willCompleteSuccessfully());
        return executionFut.join();
    }

    protected <T, R> BroadcastExecution<R> submit(
            Set<ClusterNode> nodes,
            JobDescriptor<T, R> descriptor,
            @Nullable T arg
    ) {
        CompletableFuture<BroadcastExecution<R>> executionFut = compute().submitAsync(BroadcastJobTarget.nodes(nodes), descriptor, arg);
        assertThat(executionFut, willCompleteSuccessfully());
        return executionFut.join();
    }

    private static List<Arguments> wrongJobClassArguments() {
        return List.of(
                Arguments.of("org.example.NonExistentJob", CLASS_INITIALIZATION_ERR, "Cannot load job class by name"),
                Arguments.of(NonComputeJob.class.getName(), CLASS_INITIALIZATION_ERR, "does not implement ComputeJob interface"),
                Arguments.of(NonEmptyConstructorJob.class.getName(), CLASS_INITIALIZATION_ERR, "Cannot instantiate job")
        );
    }

    @ParameterizedTest
    @MethodSource("wrongJobClassArguments")
    void executesWrongJobClassLocally(String jobClassName, int errorCode, String msg) {
        Ignite entryNode = node(0);

        IgniteException ex = assertThrows(
                IgniteException.class, () ->
                        compute().execute(
                                JobTarget.node(clusterNode(entryNode)),
                                JobDescriptor.builder(jobClassName).units(units()).build(),
                                null
                        ));

        assertTraceableException(ex, ComputeException.class, errorCode, msg);
    }

    @ParameterizedTest
    @MethodSource("wrongJobClassArguments")
    void executesWrongJobClassLocallyAsync(String jobClassName, int errorCode, String msg) {
        Ignite entryNode = node(0);

        ExecutionException ex = assertThrows(ExecutionException.class, () -> compute().executeAsync(
                        JobTarget.node(clusterNode(entryNode)), JobDescriptor.builder(jobClassName).units(units()).build(), null)
                .get(1, TimeUnit.SECONDS));

        assertTraceableException(ex, ComputeException.class, errorCode, msg);
    }

    @ParameterizedTest
    @MethodSource("wrongJobClassArguments")
    void executesWrongJobClassOnRemoteNodes(String jobClassName, int errorCode, String msg) {
        IgniteException ex = assertThrows(IgniteException.class, () -> compute().execute(
                JobTarget.anyNode(clusterNode(node(1)), clusterNode(node(2))),
                JobDescriptor.builder(jobClassName).units(units()).build(),
                null));

        assertTraceableException(ex, ComputeException.class, errorCode, msg);
    }

    @ParameterizedTest
    @MethodSource("wrongJobClassArguments")
    void executesWrongJobClassOnRemoteNodesAsync(String jobClassName, int errorCode, String msg) {
        ExecutionException ex = assertThrows(ExecutionException.class, () -> compute().executeAsync(
                JobTarget.anyNode(clusterNode(node(1)), clusterNode(node(2))),
                JobDescriptor.builder(jobClassName).units(units()).build(),
                null
        ).get(1, TimeUnit.SECONDS));

        assertTraceableException(ex, ComputeException.class, errorCode, msg);
    }

    @Test
    void executesJobLocally() {
        Ignite entryNode = node(0);

        String result = compute().execute(
                JobTarget.node(clusterNode(entryNode)),
                JobDescriptor.builder(toStringJobClass()).units(units()).build(),
                42);

        assertThat(result, is("42"));
    }

    @Test
    void executesJobLocallyAsync() {
        Ignite entryNode = node(0);

        JobExecution<String> execution = submit(
                JobTarget.node(clusterNode(entryNode)),
                JobDescriptor.builder(toStringJobClass()).units(units()).build(),
                42
        );

        assertThat(execution.resultAsync(), willBe("42"));
        assertThat(execution.stateAsync(), willBe(jobStateWithStatus(COMPLETED)));
    }

    @Test
    void executesJobOnRemoteNodes() {
        String result = compute().execute(
                JobTarget.anyNode(clusterNode(node(1)), clusterNode(node(2))),
                JobDescriptor.builder(toStringJobClass()).units(units()).build(),
                42);

        assertThat(result, is("42"));
    }

    @Test
    void executesJobOnRemoteNodesAsync() {
        JobExecution<String> execution = submit(
                JobTarget.anyNode(clusterNode(node(1)), clusterNode(node(2))),
                JobDescriptor.builder(toStringJobClass()).units(units()).build(),
                42
        );

        assertThat(execution.resultAsync(), willBe("42"));
        assertThat(execution.stateAsync(), willBe(jobStateWithStatus(COMPLETED)));
    }

    @Test
    void localExecutionActuallyUsesLocalNode() {
        Ignite entryNode = node(0);

        CompletableFuture<String> fut = compute().executeAsync(
                JobTarget.node(clusterNode(entryNode)),
                JobDescriptor.builder(getNodeNameJobClass()).units(units()).build(), null);

        assertThat(fut, willBe(entryNode.name()));
    }

    @Test
    void remoteExecutionActuallyUsesRemoteNode() {
        Ignite remoteNode = node(1);

        CompletableFuture<String> fut = compute().executeAsync(
                JobTarget.node(clusterNode(remoteNode)),
                JobDescriptor.builder(getNodeNameJobClass()).units(units()).build(), null);

        assertThat(fut, willBe(remoteNode.name()));
    }

    @Test
    void executesFailingJobLocally() {
        Ignite entryNode = node(0);

        IgniteException ex = assertThrows(IgniteException.class, () -> compute().execute(
                JobTarget.node(clusterNode(entryNode)),
                JobDescriptor.builder(failingJobClass()).units(units()).build(), null));

        assertComputeException(ex, "JobException", "Oops");
    }

    @Test
    void executesFailingJobLocallyAsync() {
        Ignite entryNode = node(0);

        JobExecution<String> execution = submit(
                JobTarget.node(clusterNode(entryNode)),
                JobDescriptor.builder(failingJobClass()).units(units()).build(),
                null
        );

        ExecutionException ex = assertThrows(ExecutionException.class, () -> execution.resultAsync().get(1, TimeUnit.SECONDS));

        assertComputeException(ex, "JobException", "Oops");

        assertThat(execution.stateAsync(), willBe(jobStateWithStatus(FAILED)));
    }

    @Test
    void executesFailingJobOnRemoteNodes() {
        IgniteException ex = assertThrows(IgniteException.class, () -> compute().execute(
                JobTarget.anyNode(clusterNode(node(1)), clusterNode(node(2))),
                JobDescriptor.builder(failingJobClass()).units(units()).build(), null));

        assertComputeException(ex, "JobException", "Oops");
    }

    @Test
    void executesFailingJobOnRemoteNodesWithOptions() {
        JobExecutionOptions options = JobExecutionOptions.builder().priority(1).maxRetries(2).build();

        String result = compute().execute(
                JobTarget.anyNode(clusterNode(node(1)), clusterNode(node(2))),
                JobDescriptor.builder(FailingJobOnFirstExecution.class).units(units()).options(options).build(),
                null
        );

        assertThat(result, is("done"));
    }

    @Test
    void executesFailingJobOnRemoteNodesAsync() {
        JobExecution<String> execution = submit(
                JobTarget.anyNode(clusterNode(node(1)), clusterNode(node(2))),
                JobDescriptor.builder(failingJobClass()).units(units()).build(),
                null
        );

        ExecutionException ex = assertThrows(ExecutionException.class, () -> execution.resultAsync().get(1, TimeUnit.SECONDS));

        assertComputeException(ex, "JobException", "Oops");

        assertThat(execution.stateAsync(), willBe(jobStateWithStatus(FAILED)));
    }

    @Test
    void broadcastsJobWithArgumentsAsync() {
        BroadcastExecution<String> broadcastExecution = submit(
                Set.of(clusterNode(node(0)), clusterNode(node(1)), clusterNode(node(2))),
                JobDescriptor.builder(toStringJobClass()).units(units()).build(),
                42
        );

        Collection<JobExecution<String>> executions = broadcastExecution.executions();
        assertThat(executions, containsInAnyOrder(
                jobExecutionWithResultStatusAndNode("42", COMPLETED, clusterNode(0)),
                jobExecutionWithResultStatusAndNode("42", COMPLETED, clusterNode(1)),
                jobExecutionWithResultStatusAndNode("42", COMPLETED, clusterNode(2))
        ));

        assertThat(broadcastExecution.resultsAsync(), will(hasSize(3)));
        assertThat(broadcastExecution.resultsAsync(), will(everyItem(is("42"))));
    }

    @Test
    void broadcastExecutesJobOnRespectiveNodes() {
        BroadcastExecution<String> broadcastExecution = submit(
                Set.of(clusterNode(node(0)), clusterNode(node(1)), clusterNode(node(2))),
                JobDescriptor.builder(getNodeNameJobClass()).units(units()).build(),
                null
        );

        Collection<JobExecution<String>> executions = broadcastExecution.executions();
        assertThat(executions, containsInAnyOrder(
                jobExecutionWithResultStatusAndNode(clusterNode(0).name(), COMPLETED, clusterNode(0)),
                jobExecutionWithResultStatusAndNode(clusterNode(1).name(), COMPLETED, clusterNode(1)),
                jobExecutionWithResultStatusAndNode(clusterNode(2).name(), COMPLETED, clusterNode(2))
        ));

        assertThat(broadcastExecution.resultsAsync(), will(hasSize(3)));
        assertThat(broadcastExecution.resultsAsync(), will(containsInAnyOrder(allNodeNames().toArray())));
    }

    @Test
    void broadcastsFailingJob() {
        BroadcastExecution<String> broadcastExecution = submit(
                Set.of(clusterNode(node(0)), clusterNode(node(1)), clusterNode(node(2))),
                JobDescriptor.builder(failingJobClass()).units(units()).build(),
                null
        );

        Collection<JobExecution<String>> executions = broadcastExecution.executions();
        assertThat(executions, hasSize(3));
        for (JobExecution<String> execution : executions) {
            ExecutionException ex = assertThrows(ExecutionException.class, () -> execution.resultAsync().get(1, TimeUnit.SECONDS));
            assertComputeException(ex, "JobException", "Oops");

            assertThat(execution.stateAsync(), willBe(jobStateWithStatus(FAILED)));
        }

        ExecutionException ex = assertThrows(ExecutionException.class, () -> broadcastExecution.resultsAsync().get(1, TimeUnit.SECONDS));
        assertComputeException(ex, "JobException", "Oops");
    }

    @Test
    void executesColocatedWithTupleKey() {
        createTestTableWithOneRow();

        String actualNodeName = compute().execute(
                JobTarget.colocated("test", Tuple.create(Map.of("k", 1))),
                JobDescriptor.builder(getNodeNameJobClass()).units(units()).build(), null);

        assertThat(actualNodeName, in(allNodeNames()));
    }

    @Test
    void executesColocatedWithTupleKeyAsync() {
        createTestTableWithOneRow();

        JobExecution<String> execution = submit(
                JobTarget.colocated("test", Tuple.create(Map.of("k", 1))),
                JobDescriptor.builder(getNodeNameJobClass()).units(units()).build(),
                null
        );

        assertThat(execution.resultAsync(), willBe(in(allNodeNames())));
        assertThat(execution.stateAsync(), willBe(jobStateWithStatus(COMPLETED)));
    }

    @Test
    public void executesColocatedWithNonConsecutiveKeyColumnOrder() {
        sql("DROP TABLE IF EXISTS test");
        sql("CREATE TABLE test (k int, key_int int, v int, key_str VARCHAR, CONSTRAINT PK PRIMARY KEY (key_int, key_str))");
        sql("INSERT INTO test VALUES (1, 2, 3, '4')");

        String actualNodeName = compute().execute(
                JobTarget.colocated("test", Tuple.create(Map.of("key_int", 2, "key_str", "4"))),
                JobDescriptor.builder(getNodeNameJobClass()).units(units()).build(), null);
        assertThat(actualNodeName, in(allNodeNames()));
    }

    @Test
    void executeColocatedThrowsTableNotFoundExceptionWhenTableDoesNotExist() {
        var ex = assertThrows(CompletionException.class,
                () -> compute().submitAsync(
                        JobTarget.colocated("BAD_TABLE", Tuple.create(Map.of("k", 1))),
                        JobDescriptor.builder(getNodeNameJobClass()).units(units()).build(),
                        null
                ).join()
        );

        assertInstanceOf(TableNotFoundException.class, ex.getCause());
        assertThat(ex.getCause().getMessage(), containsString("The table does not exist [name=PUBLIC.BAD_TABLE]"));
    }

    @ParameterizedTest(name = "local: {0}")
    @ValueSource(booleans = {true, false})
    void cancelComputeExecuteAsyncWithCancelHandle(boolean local) {
        Ignite executeNode = local ? node(0) : node(1);

        CancelHandle cancelHandle = CancelHandle.create();

        JobDescriptor<Long, Void> job = JobDescriptor.builder(SilentSleepJob.class).units(units()).build();

        CompletableFuture<Void> execution = compute()
                .executeAsync(JobTarget.node(clusterNode(executeNode)), job, 100L, cancelHandle.token());

        cancelHandle.cancel();

        assertThrows(ExecutionException.class, () -> execution.get(10, TimeUnit.SECONDS));
    }

    @ParameterizedTest(name = "local: {0}")
    @ValueSource(booleans = {true, false})
    void cancelComputeExecuteWithCancelHandle(boolean local) {
        Ignite executeNode = local ? node(0) : node(1);

        CancelHandle cancelHandle = CancelHandle.create();

        JobDescriptor<Long, Void> job = JobDescriptor.builder(SilentSleepJob.class).units(units()).build();

        CompletableFuture<Void> runFut = IgniteTestUtils.runAsync(() -> compute()
                .execute(JobTarget.node(clusterNode(executeNode)), job, 100L, cancelHandle.token()));

        cancelHandle.cancel();

        assertThrows(ExecutionException.class, () -> runFut.get(10, TimeUnit.SECONDS));
    }

    @ParameterizedTest(name = "withLocal: {0}")
    @ValueSource(booleans = {true, false})
    void cancelComputeExecuteBroadcastAsyncWithCancelHandle(boolean local) {
        Ignite entryNode = node(0);
        Set<ClusterNode> executeNodes =
                local ? Set.of(clusterNode(entryNode), clusterNode(node(2))) : Set.of(clusterNode(node(1)), clusterNode(node(2)));

        CancelHandle cancelHandle = CancelHandle.create();

        CompletableFuture<Collection<Void>> resultsFut = compute().executeAsync(
                BroadcastJobTarget.nodes(executeNodes),
                JobDescriptor.builder(SilentSleepJob.class).units(units()).build(), 100L, cancelHandle.token()
        );

        cancelHandle.cancel();

        assertThat(resultsFut, willThrow(ComputeException.class));
    }

    @ParameterizedTest(name = "local: {0}")
    @ValueSource(booleans = {true, false})
    void cancelComputeExecuteBroadcastWithCancelHandle(boolean local) {
        Ignite entryNode = node(0);
        Set<ClusterNode> executeNodes =
                local ? Set.of(clusterNode(entryNode), clusterNode(node(2))) : Set.of(clusterNode(node(1)), clusterNode(node(2)));

        CancelHandle cancelHandle = CancelHandle.create();

        CompletableFuture<Collection<Void>> runFut = IgniteTestUtils.runAsync(() -> compute().execute(
                BroadcastJobTarget.nodes(executeNodes),
                JobDescriptor.builder(SilentSleepJob.class).units(units()).build(), 100L, cancelHandle.token()
        ));

        cancelHandle.cancel();

        assertThat(runFut, willThrow(ComputeException.class));
    }

    @Test
    void cancelComputeExecuteMapReduceAsyncWithCancelHandle() {
        CancelHandle cancelHandle = CancelHandle.create();

        CompletableFuture<Void> execution = compute()
                .executeMapReduceAsync(TaskDescriptor.builder(InfiniteMapReduceTask.class).build(), null, cancelHandle.token());

        cancelHandle.cancel();

        assertThrows(ExecutionException.class, () -> execution.get(10, TimeUnit.SECONDS));
    }

    static void createTestTableWithOneRow() {
        sql("DROP TABLE IF EXISTS test");
        sql("CREATE TABLE test (k int, v int, CONSTRAINT PK PRIMARY KEY (k))");
        sql("INSERT INTO test(k, v) VALUES (1, 101)");
    }

    private List<String> allNodeNames() {
        return IntStream.range(0, initialNodes())
                .mapToObj(ClusterPerClassIntegrationTest::node)
                .map(Ignite::name)
                .collect(toList());
    }

    @Test
    void executesColocatedWithMappedKey() {
        createTestTableWithOneRow();

        String actualNodeName = compute().execute(
                JobTarget.colocated("test", 1, Mapper.of(Integer.class)),
                JobDescriptor.builder(getNodeNameJobClass()).units(units()).build(), null);

        assertThat(actualNodeName, in(allNodeNames()));
    }

    @Test
    void executesColocatedWithMappedKeyAsync() {
        createTestTableWithOneRow();

        JobExecution<String> execution = submit(
                JobTarget.colocated("test", 1, Mapper.of(Integer.class)),
                JobDescriptor.builder(getNodeNameJobClass()).units(units()).build(),
                null
        );

        assertThat(execution.resultAsync(), willBe(in(allNodeNames())));
        assertThat(execution.stateAsync(), willBe(jobStateWithStatus(COMPLETED)));
    }

    @Test
    void submitMapReduce() {
        TaskExecution<Integer> taskExecution = compute().submitMapReduce(
                TaskDescriptor.builder(mapReduceTaskClass()).units(units()).build(),
                units()
        );

        int sumOfNodeNamesLengths = CLUSTER.runningNodes().map(Ignite::name).map(String::length).reduce(Integer::sum).orElseThrow();
        assertThat(taskExecution.resultAsync(), willBe(sumOfNodeNamesLengths));

        // States list contains states for 3 running nodes
        assertThat(taskExecution.statesAsync(), willBe(contains(
                jobStateWithStatus(COMPLETED),
                jobStateWithStatus(COMPLETED),
                jobStateWithStatus(COMPLETED)
        )));
    }

    @Test
    void executeMapReduceAsync() {
        CompletableFuture<Integer> future = compute().executeMapReduceAsync(
                TaskDescriptor.builder(mapReduceTaskClass()).units(units()).build(),
                units()
        );

        int sumOfNodeNamesLengths = CLUSTER.runningNodes().map(Ignite::name).map(String::length).reduce(Integer::sum).orElseThrow();
        assertThat(future, willBe(sumOfNodeNamesLengths));
    }

    @Test
    void executeMapReduce() {
        int result = compute().executeMapReduce(TaskDescriptor.builder(mapReduceTaskClass()).units(units()).build(), units());

        int sumOfNodeNamesLengths = CLUSTER.runningNodes().map(Ignite::name).map(String::length).reduce(Integer::sum).orElseThrow();
        assertThat(result, is(sumOfNodeNamesLengths));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void cancelsJob(boolean local) {
        Ignite executeNode = local ? node(0) : node(1);

        CancelHandle cancelHandle = CancelHandle.create();

        // This job catches the interruption and throws a RuntimeException
        JobDescriptor<Long, Void> job = JobDescriptor.builder(SleepJob.class).units(units()).build();
        JobExecution<Void> execution = submit(JobTarget.node(clusterNode(executeNode)), job, cancelHandle.token(), Long.MAX_VALUE);

        await().until(execution::stateAsync, willBe(jobStateWithStatus(EXECUTING)));

        assertThat(cancelHandle.cancelAsync(), willCompleteSuccessfully());

        CompletionException completionException = assertThrows(CompletionException.class, () -> execution.resultAsync().join());

        // Unwrap CompletionException, ComputeException should be the cause thrown from the API
        assertThat(completionException.getCause(), instanceOf(ComputeException.class));
        ComputeException computeException = (ComputeException) completionException.getCause();

        // ComputeException should be caused by the RuntimeException thrown from the SleepJob
        assertThat(computeException.getCause(), instanceOf(RuntimeException.class));
        RuntimeException runtimeException = (RuntimeException) computeException.getCause();

        // RuntimeException is thrown when SleepJob catches the InterruptedException
        assertThat(runtimeException.toString(), containsString(InterruptedException.class.getName()));

        await().until(execution::stateAsync, willBe(jobStateWithStatus(CANCELED)));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void cancelsNotCancellableJob(boolean local) {
        Ignite executeNode = local ? node(0) : node(1);

        CancelHandle cancelHandle = CancelHandle.create();

        // This job catches the interruption and returns normally
        JobDescriptor<Long, Void> job = JobDescriptor.builder(SilentSleepJob.class).units(units()).build();
        JobExecution<Void> execution = submit(JobTarget.node(clusterNode(executeNode)), job, cancelHandle.token(), Long.MAX_VALUE);

        await().until(execution::stateAsync, willBe(jobStateWithStatus(EXECUTING)));

        assertThat(cancelHandle.cancelAsync(), willCompleteSuccessfully());

        CompletionException completionException = assertThrows(CompletionException.class, () -> execution.resultAsync().join());

        // Unwrap CompletionException, ComputeException should be the cause thrown from the API
        assertThat(completionException.getCause(), instanceOf(ComputeException.class));
        ComputeException computeException = (ComputeException) completionException.getCause();

        // ComputeException should be caused by the CancellationException thrown from the executor which detects that the job completes,
        // but was previously cancelled
        assertThat(computeException.getCause(), instanceOf(CancellationException.class));
        CancellationException cancellationException = (CancellationException) computeException.getCause();
        assertThat(cancellationException.getCause(), is(nullValue()));

        await().until(execution::stateAsync, willBe(jobStateWithStatus(CANCELED)));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void cancelsQueuedJob(boolean local) {
        Ignite executeNode = local ? node(0) : node(1);
        var nodes = JobTarget.node(clusterNode(executeNode));

        JobDescriptor<Long, Void> job = JobDescriptor.builder(SleepJob.class).units(units()).build();

        CancelHandle cancelHandle1 = CancelHandle.create();
        // Start 1 task in executor with 1 thread
        JobExecution<Void> execution1 = submit(nodes, job, cancelHandle1.token(), Long.MAX_VALUE);
        await().until(execution1::stateAsync, willBe(jobStateWithStatus(EXECUTING)));

        CancelHandle cancelHandle2 = CancelHandle.create();
        // Start one more task
        JobExecution<Void> execution2 = submit(nodes, job, cancelHandle2.token(), Long.MAX_VALUE);
        await().until(execution2::stateAsync, willBe(jobStateWithStatus(QUEUED)));

        // Task 2 is not complete, in queued state
        assertThat(execution2.resultAsync().isDone(), is(false));

        // Cancel queued task
        assertThat(cancelHandle2.cancelAsync(), willCompleteSuccessfully());
        await().until(execution2::stateAsync, willBe(jobStateWithStatus(CANCELED)));

        // Cancel running task
        assertThat(cancelHandle1.cancelAsync(), willCompleteSuccessfully());
        await().until(execution1::stateAsync, willBe(jobStateWithStatus(CANCELED)));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void changeExecutingJobPriority(boolean local) {
        Ignite executeNode = local ? node(0) : node(1);

        CancelHandle cancelHandle = CancelHandle.create();
        JobDescriptor<Long, Void> job = JobDescriptor.builder(SleepJob.class).units(units()).build();
        JobExecution<Void> execution = submit(JobTarget.node(clusterNode(executeNode)), job, cancelHandle.token(), Long.MAX_VALUE);
        await().until(execution::stateAsync, willBe(jobStateWithStatus(EXECUTING)));

        assertThat(execution.changePriorityAsync(2), willBe(false));
        assertThat(cancelHandle.cancelAsync(), willCompleteSuccessfully());
    }

    @Test
    void tupleSerialization() {
        ClusterNode executeNode = clusterNode(node(1));

        // Execute the job on remote node to trigger serialization
        Integer result = compute().execute(
                JobTarget.node(executeNode),
                JobDescriptor.builder(TupleJob.class).units(units()).build(),
                Tuple.create().set("COUNT", 1)
        );

        assertThat(result, is(1));
    }

    @MethodSource("tupleCollections")
    @ParameterizedTest
    void tupleCollectionSerialization(Collection<Tuple> arg) {
        List<Tuple> expected = new ArrayList<>(arg);
        expected.add(Tuple.create().set("job-result", "done"));

        for (int nodeIdx = 0; nodeIdx < initialNodes(); nodeIdx++) {
            ClusterNode executeNode = clusterNode(node(nodeIdx));

            Collection<Tuple> result = compute().execute(
                    JobTarget.node(executeNode),
                    JobDescriptor.builder(TupleCollectionJob.class).units(units()).build(),
                    arg
            );

            assertIterableEquals(expected, result);
        }
    }

    private static Stream<Arguments> tupleCollections() {
        return Stream.of(
                List.of(),
                Collections.singletonList(Tuple.create()),
                Collections.singletonList(null),
                List.of(Tuple.create(), Tuple.create().set("key", 1), Tuple.create().set("key", "value1")),
                Set.of(Tuple.create().set("key", 2), Tuple.create().set("key", "value2"))
        ).map(Arguments::of);
    }

    @Test
    void partitionedBroadcast() {
        createTestTableWithOneRow();

        Map<Partition, ClusterNode> replicas = node(0).tables().table("test").partitionManager().primaryReplicasAsync().join();
        Map<Integer, ClusterNode> partitionIdToNode = replicas.entrySet().stream()
                .collect(toMap(entry -> ((HashPartition) entry.getKey()).partitionId(), Entry::getValue));

        // When run job that will return its partition id
        JobDescriptor<Void, Integer> job = JobDescriptor.builder(GetPartitionJob.class).units(units()).build();
        CompletableFuture<BroadcastExecution<Integer>> future = compute()
                .submitAsync(BroadcastJobTarget.table("test"), job, null);

        // Then the jobs are submitted
        assertThat(future, willCompleteSuccessfully());
        BroadcastExecution<Integer> broadcastExecution = future.join();

        // And results contain all partition ids
        assertThat(broadcastExecution.resultsAsync(), will(containsInAnyOrder(partitionIdToNode.keySet().toArray())));

        Collection<JobExecution<Integer>> executions = broadcastExecution.executions();

        // And each execution was submitted to the node that holds the primary replica for a particular partition
        assertThat(executions, hasSize(partitionIdToNode.size()));
        executions.forEach(execution -> {
            Integer partitionId = execution.resultAsync().join(); // safe to join since resultsAsync is already complete
            assertThat(execution.node().name(), is(partitionIdToNode.get(partitionId).name()));
        });
    }

    static Class<ToStringJob> toStringJobClass() {
        return ToStringJob.class;
    }

    private static Class<GetNodeNameJob> getNodeNameJobClass() {
        return GetNodeNameJob.class;
    }

    private static Class<FailingJob> failingJobClass() {
        return FailingJob.class;
    }

    private static Class<MapReduce> mapReduceTaskClass() {
        return MapReduce.class;
    }

    static void assertComputeException(Exception ex, Throwable cause) {
        assertComputeException(ex, cause.getClass().getName(), cause.getMessage());
    }

    static void assertComputeException(Exception ex, Class<?> cause, String causeMsgSubstring) {
        assertComputeException(ex, cause.getName(), causeMsgSubstring);
    }

    private static void assertComputeException(Exception ex, String causeClass, String causeMsgSubstring) {
        assertTraceableException(ex, ComputeException.class, COMPUTE_JOB_FAILED_ERR, "Job execution failed:");
        Throwable cause = ExceptionUtils.unwrapCause(ex);
        assertThat(cause.toString(), containsString(causeClass));
        assertThat(cause.getCause().getMessage(), containsString(causeMsgSubstring));
    }
}
