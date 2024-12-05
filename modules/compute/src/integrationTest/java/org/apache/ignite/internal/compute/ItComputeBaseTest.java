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
import static org.apache.ignite.compute.JobStatus.CANCELED;
import static org.apache.ignite.compute.JobStatus.COMPLETED;
import static org.apache.ignite.compute.JobStatus.EXECUTING;
import static org.apache.ignite.compute.JobStatus.FAILED;
import static org.apache.ignite.compute.JobStatus.QUEUED;
import static org.apache.ignite.internal.IgniteExceptionTestUtils.assertTraceableException;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willBe;
import static org.apache.ignite.internal.testframework.matchers.JobStateMatcher.jobStateWithStatus;
import static org.apache.ignite.lang.ErrorGroups.Compute.CLASS_INITIALIZATION_ERR;
import static org.apache.ignite.lang.ErrorGroups.Compute.COMPUTE_JOB_FAILED_ERR;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.in;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import org.apache.ignite.Ignite;
import org.apache.ignite.compute.ComputeException;
import org.apache.ignite.compute.IgniteCompute;
import org.apache.ignite.compute.JobDescriptor;
import org.apache.ignite.compute.JobExecution;
import org.apache.ignite.compute.JobTarget;
import org.apache.ignite.compute.TaskDescriptor;
import org.apache.ignite.compute.task.TaskExecution;
import org.apache.ignite.deployment.DeploymentUnit;
import org.apache.ignite.internal.ClusterPerClassIntegrationTest;
import org.apache.ignite.internal.testframework.IgniteTestUtils;
import org.apache.ignite.internal.util.ExceptionUtils;
import org.apache.ignite.lang.CancelHandle;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.lang.TableNotFoundException;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.table.Tuple;
import org.apache.ignite.table.mapper.Mapper;
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
                entryNode.compute().execute(
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

        ExecutionException ex = assertThrows(ExecutionException.class, () -> entryNode.compute().executeAsync(
                JobTarget.node(clusterNode(entryNode)), JobDescriptor.builder(jobClassName).units(units()).build(), null)
                .get(1, TimeUnit.SECONDS));

        assertTraceableException(ex, ComputeException.class, errorCode, msg);
    }

    @ParameterizedTest
    @MethodSource("wrongJobClassArguments")
    void executesWrongJobClassOnRemoteNodes(String jobClassName, int errorCode, String msg) {
        Ignite entryNode = node(0);

        IgniteException ex = assertThrows(IgniteException.class, () -> entryNode.compute().execute(
                JobTarget.anyNode(clusterNode(node(1)), clusterNode(node(2))),
                JobDescriptor.builder(jobClassName).units(units()).build(),
                null));

        assertTraceableException(ex, ComputeException.class, errorCode, msg);
    }

    @ParameterizedTest
    @MethodSource("wrongJobClassArguments")
    void executesWrongJobClassOnRemoteNodesAsync(String jobClassName, int errorCode, String msg) {
        Ignite entryNode = node(0);

        ExecutionException ex = assertThrows(ExecutionException.class, () -> entryNode.compute().executeAsync(
                JobTarget.anyNode(clusterNode(node(1)), clusterNode(node(2))),
                JobDescriptor.builder(jobClassName).units(units()).build(),
                null
        ).get(1, TimeUnit.SECONDS));

        assertTraceableException(ex, ComputeException.class, errorCode, msg);
    }

    @Test
    void executesJobLocally() {
        Ignite entryNode = node(0);

        String result = entryNode.compute().execute(
                JobTarget.node(clusterNode(entryNode)),
                JobDescriptor.builder(concatJobClass()).units(units()).build(),
                new Object[]{"a", 42});

        assertThat(result, is("a42"));
    }

    @Test
    void executesJobLocallyAsync() {
        Ignite entryNode = node(0);

        JobExecution<String> execution = entryNode.compute().submit(
                JobTarget.node(clusterNode(entryNode)),
                JobDescriptor.builder(concatJobClass()).units(units()).build(),
                new Object[] {"a", 42});

        assertThat(execution.resultAsync(), willBe("a42"));
        assertThat(execution.stateAsync(), willBe(jobStateWithStatus(COMPLETED)));
        assertThat(execution.cancelAsync(), willBe(false));
    }

    @Test
    void executesJobOnRemoteNodes() {
        Ignite entryNode = node(0);

        String result = entryNode.compute().execute(
                JobTarget.anyNode(clusterNode(node(1)), clusterNode(node(2))),
                JobDescriptor.builder(concatJobClass()).units(units()).build(),
                new Object[]{"a", 42});

        assertThat(result, is("a42"));
    }

    @Test
    void executesJobOnRemoteNodesAsync() {
        Ignite entryNode = node(0);

        JobExecution<String> execution = entryNode.compute().submit(
                JobTarget.anyNode(clusterNode(node(1)), clusterNode(node(2))),
                JobDescriptor.builder(concatJobClass()).units(units()).build(),
                new Object[]{"a", 42});

        assertThat(execution.resultAsync(), willBe("a42"));
        assertThat(execution.stateAsync(), willBe(jobStateWithStatus(COMPLETED)));
        assertThat(execution.cancelAsync(), willBe(false));
    }

    @Test
    void localExecutionActuallyUsesLocalNode() {
        Ignite entryNode = node(0);

        CompletableFuture<String> fut = entryNode.compute().executeAsync(
                JobTarget.node(clusterNode(entryNode)),
                JobDescriptor.builder(getNodeNameJobClass()).units(units()).build(), null);

        assertThat(fut, willBe(entryNode.name()));
    }

    @Test
    void remoteExecutionActuallyUsesRemoteNode() {
        Ignite entryNode = node(0);
        Ignite remoteNode = node(1);

        CompletableFuture<String> fut = entryNode.compute().executeAsync(
                JobTarget.node(clusterNode(remoteNode)),
                JobDescriptor.builder(getNodeNameJobClass()).units(units()).build(), null);

        assertThat(fut, willBe(remoteNode.name()));
    }

    @Test
    void executesFailingJobLocally() {
        Ignite entryNode = node(0);

        IgniteException ex = assertThrows(IgniteException.class, () -> entryNode.compute().execute(
                JobTarget.node(clusterNode(entryNode)),
                JobDescriptor.builder(failingJobClassName()).units(units()).build(), null));

        assertComputeException(ex, "JobException", "Oops");
    }

    @Test
    void executesFailingJobLocallyAsync() {
        Ignite entryNode = node(0);

        JobExecution<String> execution = entryNode.compute().submit(
                JobTarget.node(clusterNode(entryNode)),
                JobDescriptor.<Object, String>builder(failingJobClassName()).units(units()).build(), null);

        ExecutionException ex = assertThrows(ExecutionException.class, () -> execution.resultAsync().get(1, TimeUnit.SECONDS));

        assertComputeException(ex, "JobException", "Oops");

        assertThat(execution.stateAsync(), willBe(jobStateWithStatus(FAILED)));
        assertThat(execution.cancelAsync(), willBe(false));
    }

    @Test
    void executesFailingJobOnRemoteNodes() {
        Ignite entryNode = node(0);

        IgniteException ex = assertThrows(IgniteException.class, () -> entryNode.compute().execute(
                JobTarget.anyNode(clusterNode(node(1)), clusterNode(node(2))),
                JobDescriptor.builder(failingJobClassName()).units(units()).build(), null));

        assertComputeException(ex, "JobException", "Oops");
    }

    @Test
    void executesFailingJobOnRemoteNodesAsync() {
        Ignite entryNode = node(0);

        JobExecution<String> execution = entryNode.compute().submit(
                JobTarget.anyNode(clusterNode(node(1)), clusterNode(node(2))),
                JobDescriptor.<Object, String>builder(failingJobClassName()).units(units()).build(), null);

        ExecutionException ex = assertThrows(ExecutionException.class, () -> execution.resultAsync().get(1, TimeUnit.SECONDS));

        assertComputeException(ex, "JobException", "Oops");

        assertThat(execution.stateAsync(), willBe(jobStateWithStatus(FAILED)));
        assertThat(execution.cancelAsync(), willBe(false));
    }

    @Test
    void broadcastsJobWithArgumentsAsync() {
        Ignite entryNode = node(0);

        Map<ClusterNode, JobExecution<String>> results = entryNode.compute().submitBroadcast(
                Set.of(clusterNode(entryNode), clusterNode(node(1)), clusterNode(node(2))),
                JobDescriptor.builder(concatJobClass()).units(units()).build(),
                new Object[] {"a", 42});

        assertThat(results, is(aMapWithSize(3)));
        for (int i = 0; i < 3; i++) {
            ClusterNode node = clusterNode(node(i));
            JobExecution<String> execution = results.get(node);
            assertThat(execution.resultAsync(), willBe("a42"));
            assertThat(execution.stateAsync(), willBe(jobStateWithStatus(COMPLETED)));
            assertThat(execution.cancelAsync(), willBe(false));
        }
    }

    @Test
    void broadcastExecutesJobOnRespectiveNodes() {
        Ignite entryNode = node(0);

        Map<ClusterNode, JobExecution<String>> results = entryNode.compute().submitBroadcast(
                Set.of(clusterNode(entryNode), clusterNode(node(1)), clusterNode(node(2))),
                JobDescriptor.builder(getNodeNameJobClass()).units(units()).build(), null);

        assertThat(results, is(aMapWithSize(3)));
        for (int i = 0; i < 3; i++) {
            ClusterNode node = clusterNode(node(i));
            JobExecution<String> execution = results.get(node);
            assertThat(execution.resultAsync(), willBe(node.name()));
            assertThat(execution.stateAsync(), willBe(jobStateWithStatus(COMPLETED)));
            assertThat(execution.cancelAsync(), willBe(false));
        }
    }

    @Test
    void broadcastsFailingJob() throws Exception {
        Ignite entryNode = node(0);

        Map<ClusterNode, JobExecution<String>> results = entryNode.compute().submitBroadcast(
                Set.of(clusterNode(entryNode), clusterNode(node(1)), clusterNode(node(2))),
                JobDescriptor.<Object, String>builder(failingJobClassName()).units(units()).build(), null);

        assertThat(results, is(aMapWithSize(3)));
        for (int i = 0; i < 3; i++) {
            JobExecution<String> execution = results.get(clusterNode(node(i)));
            Exception result = (Exception) execution.resultAsync()
                    .handle((res, ex) -> ex != null ? ex : res)
                    .get(1, TimeUnit.SECONDS);

            assertThat(result, is(instanceOf(CompletionException.class)));
            assertComputeException(result, "JobException", "Oops");

            assertThat(execution.stateAsync(), willBe(jobStateWithStatus(FAILED)));
            assertThat(execution.cancelAsync(), willBe(false));
        }
    }

    @Test
    void executesColocatedWithTupleKey() {
        createTestTableWithOneRow();

        Ignite entryNode = node(0);

        String actualNodeName = entryNode.compute().execute(
                JobTarget.colocated("test", Tuple.create(Map.of("k", 1))),
                JobDescriptor.builder(getNodeNameJobClass()).units(units()).build(), null);

        assertThat(actualNodeName, in(allNodeNames()));
    }

    @Test
    void executesColocatedWithTupleKeyAsync() {
        createTestTableWithOneRow();

        Ignite entryNode = node(0);

        JobExecution<String> execution = entryNode.compute().submit(
                JobTarget.colocated("test", Tuple.create(Map.of("k", 1))),
                JobDescriptor.builder(getNodeNameJobClass()).units(units()).build(), null);

        assertThat(execution.resultAsync(), willBe(in(allNodeNames())));
        assertThat(execution.stateAsync(), willBe(jobStateWithStatus(COMPLETED)));
        assertThat(execution.cancelAsync(), willBe(false));
    }

    @Test
    public void executesColocatedWithNonConsecutiveKeyColumnOrder() {
        sql("DROP TABLE IF EXISTS test");
        sql("CREATE TABLE test (k int, key_int int, v int, key_str VARCHAR, CONSTRAINT PK PRIMARY KEY (key_int, key_str))");
        sql("INSERT INTO test VALUES (1, 2, 3, '4')");

        String actualNodeName = node(0).compute().execute(
                JobTarget.colocated("test", Tuple.create(Map.of("key_int", 2, "key_str", "4"))),
                JobDescriptor.builder(getNodeNameJobClass()).units(units()).build(), null);
        assertThat(actualNodeName, in(allNodeNames()));
    }

    @Test
    void executeColocatedThrowsTableNotFoundExceptionWhenTableDoesNotExist() {
        Ignite entryNode = node(0);

        var ex = assertThrows(CompletionException.class,
                () -> entryNode.compute().submit(
                        JobTarget.colocated("\"bad-table\"", Tuple.create(Map.of("k", 1))),
                        JobDescriptor.builder(getNodeNameJobClassName()).units(units()).build(), null).resultAsync().join());

        assertInstanceOf(TableNotFoundException.class, ex.getCause());
        assertThat(ex.getCause().getMessage(), containsString("The table does not exist [name=\"PUBLIC\".\"bad-table\"]"));
    }

    @ParameterizedTest(name = "local: {0}")
    @ValueSource(booleans = {true, false})
    void cancelComputeExecuteAsyncWithCancelHandle(boolean local) {
        Ignite entryNode = node(0);
        Ignite executeNode = local ? node(0) : node(1);

        CancelHandle cancelHandle = CancelHandle.create();

        JobDescriptor<Long, Void> job = JobDescriptor.builder(SilentSleepJob.class).units(units()).build();

        CompletableFuture<Void> execution = entryNode.compute()
                .executeAsync(JobTarget.node(clusterNode(executeNode)), job, cancelHandle.token(), 100L);

        cancelHandle.cancel();

        assertThrows(ExecutionException.class, () -> execution.get(10, TimeUnit.SECONDS));
    }

    @ParameterizedTest(name = "local: {0}")
    @ValueSource(booleans = {true, false})
    void cancelComputeExecuteWithCancelHandle(boolean local) {
        Ignite entryNode = node(0);
        Ignite executeNode = local ? node(0) : node(1);

        CancelHandle cancelHandle = CancelHandle.create();

        JobDescriptor<Long, Void> job = JobDescriptor.builder(SilentSleepJob.class).units(units()).build();

        CompletableFuture<Void> runFut = IgniteTestUtils.runAsync(() -> entryNode.compute()
                .execute(JobTarget.node(clusterNode(executeNode)), job, cancelHandle.token(), 100L));

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

        CompletableFuture<Map<ClusterNode, Void>> executions = entryNode.compute().executeBroadcastAsync(
                executeNodes,
                JobDescriptor.builder(SilentSleepJob.class).units(units()).build(), cancelHandle.token(), 100L
        );

        cancelHandle.cancel();

        assertThrows(ExecutionException.class, () -> executions.get(10, TimeUnit.SECONDS));
    }

    @ParameterizedTest(name = "local: {0}")
    @ValueSource(booleans = {true, false})
    void cancelComputeExecuteBroadcastWithCancelHandle(boolean local) {
        Ignite entryNode = node(0);
        Set<ClusterNode> executeNodes =
                local ? Set.of(clusterNode(entryNode), clusterNode(node(2))) : Set.of(clusterNode(node(1)), clusterNode(node(2)));

        CancelHandle cancelHandle = CancelHandle.create();

        CompletableFuture<Map<ClusterNode, Void>> runFut = IgniteTestUtils.runAsync(() -> entryNode.compute().executeBroadcast(
                executeNodes,
                JobDescriptor.builder(SilentSleepJob.class).units(units()).build(), cancelHandle.token(), 100L
        ));

        cancelHandle.cancel();

        assertThrows(ExecutionException.class, () -> runFut.get(10, TimeUnit.SECONDS));
    }

    @Test
    void cancelComputeExecuteMapReduceAsyncWithCancelHandle() {
        Ignite entryNode = node(0);

        CancelHandle cancelHandle = CancelHandle.create();

        CompletableFuture<Void> execution = entryNode.compute()
                .executeMapReduceAsync(TaskDescriptor.builder(InfiniteMapReduceTask.class).build(), cancelHandle.token(), null);

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

        Ignite entryNode = node(0);

        String actualNodeName = entryNode.compute().execute(
                JobTarget.colocated("test", 1, Mapper.of(Integer.class)),
                JobDescriptor.builder(getNodeNameJobClass()).units(units()).build(), null);

        assertThat(actualNodeName, in(allNodeNames()));
    }

    @Test
    void executesColocatedWithMappedKeyAsync() {
        createTestTableWithOneRow();

        Ignite entryNode = node(0);

        JobExecution<String> execution = entryNode.compute().submit(
                JobTarget.colocated("test", 1, Mapper.of(Integer.class)),
                JobDescriptor.builder(getNodeNameJobClass()).units(units()).build(), null);

        assertThat(execution.resultAsync(), willBe(in(allNodeNames())));
        assertThat(execution.stateAsync(), willBe(jobStateWithStatus(COMPLETED)));
        assertThat(execution.cancelAsync(), willBe(false));
    }

    @Test
    void submitMapReduce() {
        Ignite entryNode = node(0);

        IgniteCompute igniteCompute = entryNode.compute();
        List<DeploymentUnit> units = units();
        @Nullable List<DeploymentUnit> arg = units();
        TaskExecution<Integer> taskExecution = igniteCompute.submitMapReduce(
                TaskDescriptor.<List<DeploymentUnit>, Integer>builder(mapReduceTaskClassName()).units(units).build(), arg);

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
        Ignite entryNode = node(0);

        CompletableFuture<Integer> future = entryNode.compute().executeMapReduceAsync(
                TaskDescriptor.<List<DeploymentUnit>, Integer>builder(mapReduceTaskClassName()).units(units()).build(),
                units());

        int sumOfNodeNamesLengths = CLUSTER.runningNodes().map(Ignite::name).map(String::length).reduce(Integer::sum).orElseThrow();
        assertThat(future, willBe(sumOfNodeNamesLengths));
    }

    @Test
    void executeMapReduce() {
        Ignite entryNode = node(0);

        int result = entryNode.compute().executeMapReduce(
                TaskDescriptor.<List<DeploymentUnit>, Integer>builder(mapReduceTaskClassName()).units(units()).build(),
                units());

        int sumOfNodeNamesLengths = CLUSTER.runningNodes().map(Ignite::name).map(String::length).reduce(Integer::sum).orElseThrow();
        assertThat(result, is(sumOfNodeNamesLengths));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void cancelsJob(boolean local) {
        Ignite entryNode = node(0);
        Ignite executeNode = local ? node(0) : node(1);

        // This job catches the interruption and throws a RuntimeException
        JobDescriptor<Long, Void> job = JobDescriptor.builder(SleepJob.class).units(units()).build();
        JobExecution<Void> execution = entryNode.compute().submit(JobTarget.node(clusterNode(executeNode)), job, Long.MAX_VALUE);

        await().until(execution::stateAsync, willBe(jobStateWithStatus(EXECUTING)));

        assertThat(execution.cancelAsync(), willBe(true));

        CompletionException completionException = assertThrows(CompletionException.class, () -> execution.resultAsync().join());

        // Unwrap CompletionException, ComputeException should be the cause thrown from the API
        assertThat(completionException.getCause(), instanceOf(ComputeException.class));
        ComputeException computeException = (ComputeException) completionException.getCause();

        // ComputeException should be caused by the RuntimeException thrown from the SleepJob
        assertThat(computeException.getCause(), instanceOf(RuntimeException.class));
        RuntimeException runtimeException = (RuntimeException) computeException.getCause();

        // RuntimeException is thrown when SleepJob catches the InterruptedException
        assertThat(runtimeException.getCause(), instanceOf(InterruptedException.class));
        assertThat(runtimeException.getCause().getCause(), is(nullValue()));

        await().until(execution::stateAsync, willBe(jobStateWithStatus(CANCELED)));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void cancelsNotCancellableJob(boolean local) {
        Ignite entryNode = node(0);
        Ignite executeNode = local ? node(0) : node(1);

        // This job catches the interruption and returns normally
        JobDescriptor<Long, Void> job = JobDescriptor.builder(SilentSleepJob.class).units(units()).build();
        JobExecution<Void> execution = entryNode.compute().submit(JobTarget.node(clusterNode(executeNode)), job, Long.MAX_VALUE);

        await().until(execution::stateAsync, willBe(jobStateWithStatus(EXECUTING)));

        assertThat(execution.cancelAsync(), willBe(true));

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
        Ignite entryNode = node(0);
        Ignite executeNode = local ? node(0) : node(1);
        var nodes = JobTarget.node(clusterNode(executeNode));

        JobDescriptor<Long, Void> job = JobDescriptor.builder(SleepJob.class).units(units()).build();

        // Start 1 task in executor with 1 thread
        JobExecution<Void> execution1 = entryNode.compute().submit(nodes, job, Long.MAX_VALUE);
        await().until(execution1::stateAsync, willBe(jobStateWithStatus(EXECUTING)));

        // Start one more task
        JobExecution<Void> execution2 = entryNode.compute().submit(nodes, job, Long.MAX_VALUE);
        await().until(execution2::stateAsync, willBe(jobStateWithStatus(QUEUED)));

        // Task 2 is not complete, in queued state
        assertThat(execution2.resultAsync().isDone(), is(false));

        // Cancel queued task
        assertThat(execution2.cancelAsync(), willBe(true));
        await().until(execution2::stateAsync, willBe(jobStateWithStatus(CANCELED)));

        // Cancel running task
        assertThat(execution1.cancelAsync(), willBe(true));
        await().until(execution1::stateAsync, willBe(jobStateWithStatus(CANCELED)));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void changeExecutingJobPriority(boolean local) {
        Ignite entryNode = node(0);
        Ignite executeNode = local ? node(0) : node(1);

        JobDescriptor<Long, Void> job = JobDescriptor.builder(SleepJob.class).units(units()).build();
        JobExecution<Void> execution = entryNode.compute().submit(JobTarget.node(clusterNode(executeNode)), job, Long.MAX_VALUE);
        await().until(execution::stateAsync, willBe(jobStateWithStatus(EXECUTING)));

        assertThat(execution.changePriorityAsync(2), willBe(false));
        assertThat(execution.cancelAsync(), willBe(true));
    }

    static String concatJobClassName() {
        return ConcatJob.class.getName();
    }

    private static Class<ConcatJob> concatJobClass() {
        return ConcatJob.class;
    }

    private static String getNodeNameJobClassName() {
        return GetNodeNameJob.class.getName();
    }

    private static Class<GetNodeNameJob> getNodeNameJobClass() {
        return GetNodeNameJob.class;
    }

    private static String failingJobClassName() {
        return FailingJob.class.getName();
    }

    private static String mapReduceTaskClassName() {
        return MapReduce.class.getName();
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
        assertThat(cause.getCause().getClass().getName(), containsString(causeClass));
        assertThat(cause.getCause().getMessage(), containsString(causeMsgSubstring));
    }
}
