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
import static org.apache.ignite.compute.JobStatus.COMPLETED;
import static org.apache.ignite.compute.JobStatus.FAILED;
import static org.apache.ignite.internal.IgniteExceptionTestUtils.assertTraceableException;
import static org.apache.ignite.internal.TestWrappers.unwrapIgniteImpl;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willBe;
import static org.apache.ignite.internal.testframework.matchers.JobStateMatcher.jobStateWithStatus;
import static org.apache.ignite.lang.ErrorGroups.Compute.CLASS_INITIALIZATION_ERR;
import static org.apache.ignite.lang.ErrorGroups.Compute.COMPUTE_JOB_FAILED_ERR;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
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
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.compute.ComputeException;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.IgniteCompute;
import org.apache.ignite.compute.JobDescriptor;
import org.apache.ignite.compute.JobExecution;
import org.apache.ignite.compute.JobExecutionContext;
import org.apache.ignite.compute.JobTarget;
import org.apache.ignite.compute.TaskDescriptor;
import org.apache.ignite.compute.task.TaskExecution;
import org.apache.ignite.deployment.DeploymentUnit;
import org.apache.ignite.internal.ClusterPerClassIntegrationTest;
import org.apache.ignite.internal.util.ExceptionUtils;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.lang.TableNotFoundException;
import org.apache.ignite.marshalling.ByteArrayMarshaller;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.table.Tuple;
import org.apache.ignite.table.mapper.Mapper;
import org.jetbrains.annotations.Nullable;
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

    @Test
    void pojoJobArgumentSerialization() {
        Ignite entryNode = node(0);
        String address = "127.0.0.1:" + unwrapIgniteImpl(entryNode).clientAddress().port();
        try (IgniteClient client = IgniteClient.builder().addresses(address).build()) {
            var argumentPojo = new Pojo("Hey");

            String resultString = client.compute().execute(
                    JobTarget.node(clusterNode(node(1))),
                    JobDescriptor.builder(pojoJobClass())
                            .argumentMarshaller(ByteArrayMarshaller.create())
                            .build(),
                    argumentPojo
            );

            assertThat(resultString, equalTo(argumentPojo.getName()));
        }

    }

    /**
     * Tests that the nested tuples are correctly serialized and deserialized.
     */
    @Test
    void nestedTuplesArgumentSerialization() {
        Ignite entryNode = node(0);
        String address = "127.0.0.1:" + unwrapIgniteImpl(entryNode).clientAddress().port();
        try (IgniteClient client = IgniteClient.builder().addresses(address).build()) {
            var argument = Tuple.create(
                    Map.of("level1_key1", Tuple.create(
                                    Map.of("level2_key1", Tuple.create(
                                            Map.of("level3_key1", "level3_value1"))
                                    )
                            ),
                            "level1_key2", Tuple.create(
                                    Map.of("level2_key1", Tuple.create(
                                            Map.of("level3_key1", "level3_value1"))
                                    )
                            ),
                            "level1_key3", "Non-tuple-string-value",
                            "level1_key4", 42
                    )
            );

            Tuple resultTuple = client.compute().execute(
                    JobTarget.node(clusterNode(node(1))),
                    JobDescriptor.builder(TupleComputeJob.class)
                            .build(),
                    argument
            );

            assertThat(resultTuple, equalTo(argument));
        }
    }

    static Ignite node(int i) {
        return CLUSTER.node(i);
    }

    static ClusterNode clusterNode(Ignite node) {
        return unwrapIgniteImpl(node).node();
    }

    static String concatJobClassName() {
        return ConcatJob.class.getName();
    }

    static Class<ConcatJob> concatJobClass() {
        return ConcatJob.class;
    }

    static String pojoJobClassName() {
        return PojoJob.class.getName();
    }

    private static class TupleComputeJob implements ComputeJob<Tuple, Tuple> {

        @Override
        public @Nullable CompletableFuture<Tuple> executeAsync(JobExecutionContext context, @Nullable Tuple arg) {
            return CompletableFuture.completedFuture(arg);
        }
    }

    static Class<PojoJob> pojoJobClass() {
        return PojoJob.class;
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
