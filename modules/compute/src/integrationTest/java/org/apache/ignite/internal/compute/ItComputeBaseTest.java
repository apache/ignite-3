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
import static org.apache.ignite.compute.JobExecutionOptions.DEFAULT;
import static org.apache.ignite.compute.JobState.COMPLETED;
import static org.apache.ignite.compute.JobState.FAILED;
import static org.apache.ignite.internal.IgniteExceptionTestUtils.assertTraceableException;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willBe;
import static org.apache.ignite.internal.testframework.matchers.JobStatusMatcher.jobStatusWithState;
import static org.apache.ignite.lang.ErrorGroups.Compute.CLASS_INITIALIZATION_ERR;
import static org.apache.ignite.lang.ErrorGroups.Compute.COMPUTE_JOB_FAILED_ERR;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.in;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import org.apache.ignite.Ignite;
import org.apache.ignite.compute.ComputeException;
import org.apache.ignite.compute.DeploymentUnit;
import org.apache.ignite.compute.IgniteCompute;
import org.apache.ignite.compute.JobDescriptor;
import org.apache.ignite.compute.JobExecution;
import org.apache.ignite.compute.TaskExecution;
import org.apache.ignite.internal.ClusterPerClassIntegrationTest;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.util.ExceptionUtils;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.lang.TableNotFoundException;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.table.Tuple;
import org.apache.ignite.table.mapper.Mapper;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Base integration tests for Compute functionality. To add new compute job for testing both in embedded and standalone mode, add the
 * corresponding job class to the jobs source set. The integration tests depend on this source set so the job class will be visible and it
 * will be automatically compiled and packed into the ignite-integration-test-jobs-1.0-SNAPSHOT.jar.
 */
@SuppressWarnings("resource")
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
        IgniteImpl entryNode = node(0);

        IgniteException ex = assertThrows(IgniteException.class, () -> {
            IgniteCompute igniteCompute = entryNode.compute();
            Set<ClusterNode> nodes = Set.of(entryNode.node());
            List<DeploymentUnit> units = units();
            igniteCompute.execute(nodes, JobDescriptor.builder()
                    .jobClassName(jobClassName)
                    .units(units)
                    .options(DEFAULT)
                    .build());
        });

        assertTraceableException(ex, ComputeException.class, errorCode, msg);
    }

    @ParameterizedTest
    @MethodSource("wrongJobClassArguments")
    void executesWrongJobClassLocallyAsync(String jobClassName, int errorCode, String msg) {
        IgniteImpl entryNode = node(0);

        ExecutionException ex = assertThrows(ExecutionException.class, () -> {
            IgniteCompute igniteCompute = entryNode.compute();
            Set<ClusterNode> nodes = Set.of(entryNode.node());
            List<DeploymentUnit> units = units();
            igniteCompute.executeAsync(nodes, JobDescriptor.builder()
                            .jobClassName(jobClassName)
                            .units(units)
                            .build())
                    .get(1, TimeUnit.SECONDS);
        });

        assertTraceableException(ex, ComputeException.class, errorCode, msg);
    }

    @ParameterizedTest
    @MethodSource("wrongJobClassArguments")
    void executesWrongJobClassOnRemoteNodes(String jobClassName, int errorCode, String msg) {
        Ignite entryNode = node(0);

        IgniteException ex = assertThrows(IgniteException.class, () -> {
            IgniteCompute igniteCompute = entryNode.compute();
            Set<ClusterNode> nodes = Set.of(node(1).node(), node(2).node());
            List<DeploymentUnit> units = units();
            igniteCompute.execute(nodes, JobDescriptor.builder()
                    .jobClassName(jobClassName)
                    .units(units)
                    .options(DEFAULT)
                    .build());
        });

        assertTraceableException(ex, ComputeException.class, errorCode, msg);
    }

    @ParameterizedTest
    @MethodSource("wrongJobClassArguments")
    void executesWrongJobClassOnRemoteNodesAsync(String jobClassName, int errorCode, String msg) {
        Ignite entryNode = node(0);

        ExecutionException ex = assertThrows(ExecutionException.class, () -> {
            IgniteCompute igniteCompute = entryNode.compute();
            Set<ClusterNode> nodes = Set.of(node(1).node(), node(2).node());
            List<DeploymentUnit> units = units();
            igniteCompute.executeAsync(nodes, JobDescriptor.builder()
                            .jobClassName(jobClassName)
                            .units(units)
                            .build())
                    .get(1, TimeUnit.SECONDS);
        });

        assertTraceableException(ex, ComputeException.class, errorCode, msg);
    }

    @Test
    void executesJobLocally() {
        IgniteImpl entryNode = node(0);

        IgniteCompute igniteCompute = entryNode.compute();
        Set<ClusterNode> nodes = Set.of(entryNode.node());
        List<DeploymentUnit> units = units();
        String result = igniteCompute.execute(nodes, JobDescriptor.builder()
                .jobClassName(concatJobClassName())
                .units(units)
                .options(DEFAULT)
                .build(), new Object[]{"a", 42});

        assertThat(result, is("a42"));
    }

    @Test
    void executesJobLocallyAsync() {
        IgniteImpl entryNode = node(0);

        IgniteCompute igniteCompute = entryNode.compute();
        Set<ClusterNode> nodes = Set.of(entryNode.node());
        List<DeploymentUnit> units = units();
        JobExecution<String> execution = igniteCompute.submit(nodes, JobDescriptor.builder()
                .jobClassName(concatJobClassName())
                .units(units)
                .options(DEFAULT)
                .build(), new Object[]{"a", 42});

        assertThat(execution.resultAsync(), willBe("a42"));
        assertThat(execution.statusAsync(), willBe(jobStatusWithState(COMPLETED)));
        assertThat(execution.cancelAsync(), willBe(false));
    }

    @Test
    void executesJobOnRemoteNodes() {
        Ignite entryNode = node(0);

        IgniteCompute igniteCompute = entryNode.compute();
        Set<ClusterNode> nodes = Set.of(node(1).node(), node(2).node());
        List<DeploymentUnit> units = units();
        String result = igniteCompute.execute(nodes, JobDescriptor.builder()
                .jobClassName(concatJobClassName())
                .units(units)
                .options(DEFAULT)
                .build(), new Object[]{"a", 42});

        assertThat(result, is("a42"));
    }

    @Test
    void executesJobOnRemoteNodesAsync() {
        Ignite entryNode = node(0);

        IgniteCompute igniteCompute = entryNode.compute();
        Set<ClusterNode> nodes = Set.of(node(1).node(), node(2).node());
        List<DeploymentUnit> units = units();
        JobExecution<String> execution = igniteCompute.submit(nodes, JobDescriptor.builder()
                .jobClassName(concatJobClassName())
                .units(units)
                .options(DEFAULT)
                .build(), new Object[]{"a", 42});

        assertThat(execution.resultAsync(), willBe("a42"));
        assertThat(execution.statusAsync(), willBe(jobStatusWithState(COMPLETED)));
        assertThat(execution.cancelAsync(), willBe(false));
    }

    @Test
    void localExecutionActuallyUsesLocalNode() {
        IgniteImpl entryNode = node(0);

        IgniteCompute igniteCompute = entryNode.compute();
        Set<ClusterNode> nodes = Set.of(entryNode.node());
        List<DeploymentUnit> units = units();
        CompletableFuture<String> fut = igniteCompute.executeAsync(nodes, JobDescriptor.builder()
                .jobClassName(getNodeNameJobClassName())
                .units(units)
                .build(), new Object[]{});

        assertThat(fut, willBe(entryNode.name()));
    }

    @Test
    void remoteExecutionActuallyUsesRemoteNode() {
        IgniteImpl entryNode = node(0);
        IgniteImpl remoteNode = node(1);

        IgniteCompute igniteCompute = entryNode.compute();
        Set<ClusterNode> nodes = Set.of(remoteNode.node());
        List<DeploymentUnit> units = units();
        CompletableFuture<String> fut = igniteCompute.executeAsync(nodes, JobDescriptor.builder()
                .jobClassName(getNodeNameJobClassName())
                .units(units)
                .build(), new Object[]{});

        assertThat(fut, willBe(remoteNode.name()));
    }

    @Test
    void executesFailingJobLocally() {
        IgniteImpl entryNode = node(0);

        IgniteException ex = assertThrows(IgniteException.class, () -> {
            IgniteCompute igniteCompute = entryNode.compute();
            Set<ClusterNode> nodes = Set.of(entryNode.node());
            List<DeploymentUnit> units = units();
            igniteCompute.execute(nodes, JobDescriptor.builder()
                    .jobClassName(failingJobClassName())
                    .units(units)
                    .options(DEFAULT)
                    .build());
        });

        assertComputeException(ex, "JobException", "Oops");
    }

    @Test
    void executesFailingJobLocallyAsync() {
        IgniteImpl entryNode = node(0);

        IgniteCompute igniteCompute = entryNode.compute();
        Set<ClusterNode> nodes = Set.of(entryNode.node());
        List<DeploymentUnit> units = units();
        JobExecution<String> execution = igniteCompute.submit(nodes, JobDescriptor.builder()
                .jobClassName(failingJobClassName())
                .units(units)
                .options(DEFAULT)
                .build(), new Object[]{});

        ExecutionException ex = assertThrows(ExecutionException.class, () -> execution.resultAsync().get(1, TimeUnit.SECONDS));

        assertComputeException(ex, "JobException", "Oops");

        assertThat(execution.statusAsync(), willBe(jobStatusWithState(FAILED)));
        assertThat(execution.cancelAsync(), willBe(false));
    }

    @Test
    void executesFailingJobOnRemoteNodes() {
        Ignite entryNode = node(0);

        IgniteException ex = assertThrows(IgniteException.class, () -> {
            IgniteCompute igniteCompute = entryNode.compute();
            Set<ClusterNode> nodes = Set.of(node(1).node(), node(2).node());
            List<DeploymentUnit> units = units();
            igniteCompute.execute(nodes, JobDescriptor.builder()
                    .jobClassName(failingJobClassName())
                    .units(units)
                    .options(DEFAULT)
                    .build());
        });

        assertComputeException(ex, "JobException", "Oops");
    }

    @Test
    void executesFailingJobOnRemoteNodesAsync() {
        Ignite entryNode = node(0);

        IgniteCompute igniteCompute = entryNode.compute();
        Set<ClusterNode> nodes = Set.of(node(1).node(), node(2).node());
        List<DeploymentUnit> units = units();
        JobExecution<String> execution = igniteCompute.submit(nodes, JobDescriptor.builder()
                .jobClassName(failingJobClassName())
                .units(units)
                .options(DEFAULT)
                .build(), new Object[]{});

        ExecutionException ex = assertThrows(ExecutionException.class, () -> execution.resultAsync().get(1, TimeUnit.SECONDS));

        assertComputeException(ex, "JobException", "Oops");

        assertThat(execution.statusAsync(), willBe(jobStatusWithState(FAILED)));
        assertThat(execution.cancelAsync(), willBe(false));
    }

    @Test
    void broadcastsJobWithArgumentsAsync() {
        IgniteImpl entryNode = node(0);

        Map<ClusterNode, JobExecution<String>> results = entryNode.compute()
                .submitBroadcast(Set.of(entryNode.node(), node(1).node(), node(2).node()), units(), concatJobClassName(), "a", 42);

        assertThat(results, is(aMapWithSize(3)));
        for (int i = 0; i < 3; i++) {
            ClusterNode node = node(i).node();
            JobExecution<String> execution = results.get(node);
            assertThat(execution.resultAsync(), willBe("a42"));
            assertThat(execution.statusAsync(), willBe(jobStatusWithState(COMPLETED)));
            assertThat(execution.cancelAsync(), willBe(false));
        }
    }

    @Test
    void broadcastExecutesJobOnRespectiveNodes() {
        IgniteImpl entryNode = node(0);

        Map<ClusterNode, JobExecution<String>> results = entryNode.compute()
                .submitBroadcast(Set.of(entryNode.node(), node(1).node(), node(2).node()), units(), getNodeNameJobClassName());

        assertThat(results, is(aMapWithSize(3)));
        for (int i = 0; i < 3; i++) {
            ClusterNode node = node(i).node();
            JobExecution<String> execution = results.get(node);
            assertThat(execution.resultAsync(), willBe(node.name()));
            assertThat(execution.statusAsync(), willBe(jobStatusWithState(COMPLETED)));
            assertThat(execution.cancelAsync(), willBe(false));
        }
    }

    @Test
    void broadcastsFailingJob() throws Exception {
        IgniteImpl entryNode = node(0);

        Map<ClusterNode, JobExecution<String>> results = entryNode.compute()
                .submitBroadcast(Set.of(entryNode.node(), node(1).node(), node(2).node()), units(), failingJobClassName());

        assertThat(results, is(aMapWithSize(3)));
        for (int i = 0; i < 3; i++) {
            JobExecution<String> execution = results.get(node(i).node());
            Exception result = (Exception) execution.resultAsync()
                    .handle((res, ex) -> ex != null ? ex : res)
                    .get(1, TimeUnit.SECONDS);

            assertThat(result, is(instanceOf(CompletionException.class)));
            assertComputeException(result, "JobException", "Oops");

            assertThat(execution.statusAsync(), willBe(jobStatusWithState(FAILED)));
            assertThat(execution.cancelAsync(), willBe(false));
        }
    }

    @Test
    void executesColocatedWithTupleKey() {
        createTestTableWithOneRow();

        IgniteImpl entryNode = node(0);

        String actualNodeName = entryNode.compute()
                .executeColocated("test", Tuple.create(Map.of("k", 1)), units(), getNodeNameJobClassName());

        assertThat(actualNodeName, in(allNodeNames()));
    }

    @Test
    void executesColocatedWithTupleKeyAsync() {
        createTestTableWithOneRow();

        IgniteImpl entryNode = node(0);

        IgniteCompute igniteCompute = entryNode.compute();
        Tuple key = Tuple.create(Map.of("k", 1));
        List<DeploymentUnit> units = units();
        JobExecution<String> execution = igniteCompute.submitColocated("test", key, JobDescriptor.builder()
                .jobClassName(getNodeNameJobClassName())
                .units(units)
                .build(), new Object[]{});

        assertThat(execution.resultAsync(), willBe(in(allNodeNames())));
        assertThat(execution.statusAsync(), willBe(jobStatusWithState(COMPLETED)));
        assertThat(execution.cancelAsync(), willBe(false));
    }

    @Test
    public void executesColocatedWithNonConsecutiveKeyColumnOrder() {
        sql("DROP TABLE IF EXISTS test");
        sql("CREATE TABLE test (k int, key_int int, v int, key_str VARCHAR, CONSTRAINT PK PRIMARY KEY (key_int, key_str))");
        sql("INSERT INTO test VALUES (1, 2, 3, '4')");

        IgniteImpl entryNode = node(0);
        String actualNodeName = entryNode.compute()
                .executeColocated("test", Tuple.create(Map.of("key_int", 2, "key_str", "4")), units(), getNodeNameJobClassName());
        assertThat(actualNodeName, in(allNodeNames()));
    }

    @Test
    void executeColocatedThrowsTableNotFoundExceptionWhenTableDoesNotExist() {
        IgniteImpl entryNode = node(0);

        var ex = assertThrows(CompletionException.class,
                () -> {
                    IgniteCompute igniteCompute = entryNode.compute();
                    Tuple key = Tuple.create(Map.of("k", 1));
                    List<DeploymentUnit> units = units();
                    igniteCompute.submitColocated("\"bad-table\"", key, JobDescriptor.builder()
                            .jobClassName(getNodeNameJobClassName())
                            .units(units)
                            .build()).resultAsync().join();
                });

        assertInstanceOf(TableNotFoundException.class, ex.getCause());
        assertThat(ex.getCause().getMessage(), containsString("The table does not exist [name=\"PUBLIC\".\"bad-table\"]"));
    }

    static void createTestTableWithOneRow() {
        sql("DROP TABLE IF EXISTS test");
        sql("CREATE TABLE test (k int, v int, CONSTRAINT PK PRIMARY KEY (k))");
        sql("INSERT INTO test(k, v) VALUES (1, 101)");
    }

    private List<String> allNodeNames() {
        return IntStream.range(0, initialNodes())
                .mapToObj(ItComputeBaseTest::node)
                .map(Ignite::name)
                .collect(toList());
    }

    @Test
    void executesColocatedWithMappedKey() {
        createTestTableWithOneRow();

        IgniteImpl entryNode = node(0);

        String actualNodeName = entryNode.compute()
                .executeColocated("test", 1, Mapper.of(Integer.class), units(), getNodeNameJobClassName());

        assertThat(actualNodeName, in(allNodeNames()));
    }

    @Test
    void executesColocatedWithMappedKeyAsync() {
        createTestTableWithOneRow();

        IgniteImpl entryNode = node(0);

        JobExecution<String> execution = entryNode.compute()
                .submitColocated("test", 1, Mapper.of(Integer.class), units(), getNodeNameJobClassName());

        assertThat(execution.resultAsync(), willBe(in(allNodeNames())));
        assertThat(execution.statusAsync(), willBe(jobStatusWithState(COMPLETED)));
        assertThat(execution.cancelAsync(), willBe(false));
    }

    @Test
    void submitMapReduce() {
        IgniteImpl entryNode = node(0);

        TaskExecution<Integer> taskExecution = entryNode.compute().submitMapReduce(units(), mapReduceTaskClassName(), units());

        int sumOfNodeNamesLengths = CLUSTER.runningNodes().map(IgniteImpl::name).map(String::length).reduce(Integer::sum).orElseThrow();
        assertThat(taskExecution.resultAsync(), willBe(sumOfNodeNamesLengths));

        // Statuses list contains statuses for 3 running nodes
        assertThat(taskExecution.statusesAsync(), willBe(contains(
                jobStatusWithState(COMPLETED),
                jobStatusWithState(COMPLETED),
                jobStatusWithState(COMPLETED)
        )));
    }

    @Test
    void executeMapReduceAsync() {
        IgniteImpl entryNode = node(0);

        CompletableFuture<Integer> future = entryNode.compute().executeMapReduceAsync(units(), mapReduceTaskClassName(), units());

        int sumOfNodeNamesLengths = CLUSTER.runningNodes().map(IgniteImpl::name).map(String::length).reduce(Integer::sum).orElseThrow();
        assertThat(future, willBe(sumOfNodeNamesLengths));
    }

    @Test
    void executeMapReduce() {
        IgniteImpl entryNode = node(0);

        int result = entryNode.compute().executeMapReduce(units(), mapReduceTaskClassName(), units());

        int sumOfNodeNamesLengths = CLUSTER.runningNodes().map(IgniteImpl::name).map(String::length).reduce(Integer::sum).orElseThrow();
        assertThat(result, is(sumOfNodeNamesLengths));
    }

    static IgniteImpl node(int i) {
        return CLUSTER.node(i);
    }

    static String concatJobClassName() {
        return ConcatJob.class.getName();
    }

    private static String getNodeNameJobClassName() {
        return GetNodeNameJob.class.getName();
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
