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

package org.apache.ignite.internal.runner.app.client;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.CompletableFuture.failedFuture;
import static org.apache.ignite.compute.JobStatus.CANCELED;
import static org.apache.ignite.compute.JobStatus.COMPLETED;
import static org.apache.ignite.compute.JobStatus.EXECUTING;
import static org.apache.ignite.compute.JobStatus.FAILED;
import static org.apache.ignite.compute.JobStatus.QUEUED;
import static org.apache.ignite.internal.IgniteExceptionTestUtils.assertTraceableException;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureExceptionMatcher.willThrowFast;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.will;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willBe;
import static org.apache.ignite.internal.testframework.matchers.JobStateMatcher.jobStateWithStatus;
import static org.apache.ignite.internal.testframework.matchers.TaskStateMatcher.taskStateWithStatus;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.apache.ignite.lang.ErrorGroups.Common.INTERNAL_ERR;
import static org.apache.ignite.lang.ErrorGroups.Compute.COMPUTE_JOB_FAILED_ERR;
import static org.apache.ignite.lang.ErrorGroups.Table.COLUMN_ALREADY_EXISTS_ERR;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.oneOf;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.client.IgniteClient.Builder;
import org.apache.ignite.client.IgniteClientConnectionException;
import org.apache.ignite.compute.ComputeException;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.IgniteCompute;
import org.apache.ignite.compute.JobDescriptor;
import org.apache.ignite.compute.JobExecution;
import org.apache.ignite.compute.JobExecutionContext;
import org.apache.ignite.compute.JobTarget;
import org.apache.ignite.compute.TaskDescriptor;
import org.apache.ignite.compute.TaskStatus;
import org.apache.ignite.compute.task.MapReduceJob;
import org.apache.ignite.compute.task.MapReduceTask;
import org.apache.ignite.compute.task.TaskExecution;
import org.apache.ignite.compute.task.TaskExecutionContext;
import org.apache.ignite.deployment.DeploymentUnit;
import org.apache.ignite.internal.compute.TaskToJobExecutionWrapper;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.table.Tuple;
import org.apache.ignite.table.mapper.Mapper;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Thin client compute integration test.
 */
@SuppressWarnings("resource")
public class ItThinClientComputeTest extends ItAbstractThinClientTest {
    /** Test trace id. */
    private static final UUID TRACE_ID = UUID.randomUUID();

    @Test
    void testClusterNodes() {
        List<ClusterNode> nodes = sortedNodes();

        assertEquals(2, nodes.size());

        assertEquals("itcct_n_3344", nodes.get(0).name());
        assertEquals(3344, nodes.get(0).address().port());
        assertTrue(nodes.get(0).id().length() > 10);

        assertEquals("itcct_n_3345", nodes.get(1).name());
        assertEquals(3345, nodes.get(1).address().port());
        assertTrue(nodes.get(1).id().length() > 10);
    }

    @Test
    void testExecuteOnSpecificNode() {
        String res1 = client().compute().execute(
                JobTarget.node(node(0)), JobDescriptor.builder(NodeNameJob.class).build(), null
        );
        String res2 = client().compute().execute(
                JobTarget.node(node(1)), JobDescriptor.builder(NodeNameJob.class).build(), null
        );

        assertEquals("itcct_n_3344", res1);
        assertEquals("itcct_n_3345", res2);
    }

    @Test
    void testExecuteOnSpecificNodeAsync() {
        JobExecution<String> execution1 = client().compute().submit(
                JobTarget.node(node(0)), JobDescriptor.builder(NodeNameJob.class).build(), null
        );
        JobExecution<String> execution2 = client().compute().submit(
                JobTarget.node(node(1)), JobDescriptor.builder(NodeNameJob.class).build(), null
        );

        assertThat(execution1.resultAsync(), willBe("itcct_n_3344"));
        assertThat(execution2.resultAsync(), willBe("itcct_n_3345"));

        assertThat(execution1.stateAsync(), willBe(jobStateWithStatus(COMPLETED)));
        assertThat(execution2.stateAsync(), willBe(jobStateWithStatus(COMPLETED)));
    }

    @Test
    void testCancellingCompletedJob() {
        JobExecution<String> execution = client().compute().submit(
                JobTarget.node(node(0)),
                JobDescriptor.builder(NodeNameJob.class).build(), null
        );

        assertThat(execution.resultAsync(), willBe("itcct_n_3344"));

        assertThat(execution.stateAsync(), willBe(jobStateWithStatus(COMPLETED)));

        assertThat(execution.cancelAsync(), willBe(false));
    }

    @Test
    void testChangingPriorityCompletedJob() {
        JobExecution<String> execution = client().compute().submit(
                JobTarget.node(node(0)),
                JobDescriptor.builder(NodeNameJob.class).build(), null
        );

        assertThat(execution.resultAsync(), willBe("itcct_n_3344"));

        assertThat(execution.stateAsync(), willBe(jobStateWithStatus(COMPLETED)));

        assertThat(execution.changePriorityAsync(0), willBe(false));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testCancelOnSpecificNodeAsync(boolean asyncJob) {
        int sleepMs = 1_000_000;
        JobDescriptor sleepJob = JobDescriptor
                .builder(asyncJob ? AsyncSleepJob.class : SleepJob.class)
                .build();

        JobExecution<String> execution1 = client().compute().submit(JobTarget.node(node(0)), sleepJob, sleepMs);
        JobExecution<String> execution2 = client().compute().submit(JobTarget.node(node(1)), sleepJob, sleepMs);

        await().until(execution1::stateAsync, willBe(jobStateWithStatus(EXECUTING)));
        await().until(execution2::stateAsync, willBe(jobStateWithStatus(EXECUTING)));

        assertThat(execution1.cancelAsync(), willBe(true));
        assertThat(execution2.cancelAsync(), willBe(true));

        await().until(execution1::stateAsync, willBe(jobStateWithStatus(CANCELED)));
        await().until(execution2::stateAsync, willBe(jobStateWithStatus(CANCELED)));
    }

    @Test
    void changeJobPriority() {
        int sleepMs = 1_000_000;
        JobDescriptor sleepJob = JobDescriptor.builder(SleepJob.class).build();
        JobTarget target = JobTarget.node(node(0));

        // Start 1 task in executor with 1 thread
        JobExecution<String> execution1 = client().compute().submit(target, sleepJob, sleepMs);
        await().until(execution1::stateAsync, willBe(jobStateWithStatus(EXECUTING)));

        // Start one more long lasting task
        JobExecution<String> execution2 = client().compute().submit(target, sleepJob, sleepMs);
        await().until(execution2::stateAsync, willBe(jobStateWithStatus(QUEUED)));

        // Start third task
        JobExecution<String> execution3 = client().compute().submit(target, sleepJob, sleepMs);
        await().until(execution3::stateAsync, willBe(jobStateWithStatus(QUEUED)));

        // Task 2 and 3 are not completed, in queue state
        assertThat(execution2.resultAsync().isDone(), is(false));
        assertThat(execution3.resultAsync().isDone(), is(false));

        // Change priority of task 3, so it should be executed before task 2
        assertThat(execution3.changePriorityAsync(2), willBe(true));

        // Cancel task 1, task 3 should start executing
        assertThat(execution1.cancelAsync(), willBe(true));
        await().until(execution1::stateAsync, willBe(jobStateWithStatus(CANCELED)));
        await().until(execution3::stateAsync, willBe(jobStateWithStatus(EXECUTING)));

        // Task 2 is still queued
        assertThat(execution2.stateAsync(), willBe(jobStateWithStatus(QUEUED)));

        // Cleanup
        assertThat(execution2.cancelAsync(), willBe(true));
        assertThat(execution3.cancelAsync(), willBe(true));
    }

    @Test
    void testExecuteOnRandomNode() {
        String res = client().compute().execute(JobTarget.anyNode(sortedNodes()), JobDescriptor.builder(NodeNameJob.class).build(), null);

        assertTrue(Set.of("itcct_n_3344", "itcct_n_3345").contains(res));
    }

    @Test
    void testExecuteOnRandomNodeAsync() {
        JobExecution<String> execution = client().compute().submit(
                JobTarget.anyNode(sortedNodes()), JobDescriptor.builder(NodeNameJob.class).build(), null);

        assertThat(
                execution.resultAsync(),
                will(oneOf("itcct_n_3344", "itcct_n_3345"))
        );
        assertThat(execution.stateAsync(), willBe(jobStateWithStatus(COMPLETED)));
    }

    @Test
    void testBroadcastOneNode() {
        Map<ClusterNode, JobExecution<String>> executionsPerNode = client().compute().submitBroadcast(
                Set.of(node(1)),
                JobDescriptor.builder(NodeNameJob.class).build(),
                null);

        assertEquals(1, executionsPerNode.size());

        JobExecution<String> execution = executionsPerNode.get(node(1));

        assertThat(execution.resultAsync(), willBe("itcct_n_3345"));
        assertThat(execution.stateAsync(), willBe(jobStateWithStatus(COMPLETED)));
    }

    @Test
    void testBroadcastAllNodes() {
        Map<ClusterNode, JobExecution<String>> executionsPerNode = client().compute().submitBroadcast(
                new HashSet<>(sortedNodes()),
                JobDescriptor.builder(NodeNameJob.class).build(),
                null
        );

        assertEquals(2, executionsPerNode.size());

        JobExecution<String> execution1 = executionsPerNode.get(node(0));
        JobExecution<String> execution2 = executionsPerNode.get(node(1));

        assertThat(execution1.resultAsync(), willBe("itcct_n_3344"));
        assertThat(execution2.resultAsync(), willBe("itcct_n_3345"));

        assertThat(execution1.stateAsync(), willBe(jobStateWithStatus(COMPLETED)));
        assertThat(execution2.stateAsync(), willBe(jobStateWithStatus(COMPLETED)));
    }

    @Test
    void testCancelBroadcastAllNodes() {
        int sleepMs = 1_000_000;
        Map<ClusterNode, JobExecution<Void>> executionsPerNode = client().compute().submitBroadcast(
                new HashSet<>(sortedNodes()),
                JobDescriptor.builder(SleepJob.class).build(),
                sleepMs);

        assertEquals(2, executionsPerNode.size());

        JobExecution<Void> execution1 = executionsPerNode.get(node(0));
        JobExecution<Void> execution2 = executionsPerNode.get(node(1));

        await().until(execution1::stateAsync, willBe(jobStateWithStatus(EXECUTING)));
        await().until(execution2::stateAsync, willBe(jobStateWithStatus(EXECUTING)));

        assertThat(execution1.cancelAsync(), willBe(true));
        assertThat(execution2.cancelAsync(), willBe(true));

        await().until(execution1::stateAsync, willBe(jobStateWithStatus(CANCELED)));
        await().until(execution2::stateAsync, willBe(jobStateWithStatus(CANCELED)));
    }

    @Test
    void testExecuteWithArgs() {
        JobExecution<String> execution = client().compute().submit(
                JobTarget.anyNode(client().clusterNodes()), JobDescriptor.builder(ConcatJob.class).build(),
                "1:2:3.3"
        );

        assertThat(execution.resultAsync(), willBe("1_2_3.3"));
        assertThat(execution.stateAsync(), willBe(jobStateWithStatus(COMPLETED)));
    }

    @Test
    void testIgniteExceptionInJobPropagatesToClientWithMessageAndCodeAndTraceIdAsync() {
        IgniteException cause = getExceptionInJobExecutionAsync(
                client().compute().submit(JobTarget.node(node(0)), JobDescriptor.builder(IgniteExceptionJob.class).build(), null)
        );

        assertThat(cause.getMessage(), containsString("Custom job error"));
        assertEquals(TRACE_ID, cause.traceId());
        assertEquals(COLUMN_ALREADY_EXISTS_ERR, cause.code());
        assertInstanceOf(CustomException.class, cause);
        assertNull(cause.getCause()); // No stack trace by default.
    }

    @Test
    void testIgniteExceptionInJobPropagatesToClientWithMessageAndCodeAndTraceIdSync() {
        IgniteException cause = getExceptionInJobExecutionSync(
                () -> client().compute().execute(JobTarget.node(node(0)), JobDescriptor.builder(IgniteExceptionJob.class).build(), null)
        );

        assertThat(cause.getMessage(), containsString("Custom job error"));
        assertEquals(TRACE_ID, cause.traceId());
        assertEquals(COLUMN_ALREADY_EXISTS_ERR, cause.code());
        assertInstanceOf(CustomException.class, cause);
        assertNull(cause.getCause()); // No stack trace by default.
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testExceptionInJobPropagatesToClientWithClassAndMessageAsync(boolean asyncJob) {
        IgniteException cause = getExceptionInJobExecutionAsync(
                client().compute().submit(JobTarget.node(node(0)), JobDescriptor.builder(ExceptionJob.class).build(), asyncJob)
        );

        assertComputeExceptionWithClassAndMessage(cause);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testExceptionInJobPropagatesToClientWithClassAndMessageSync(boolean asyncJob) {
        IgniteException cause = getExceptionInJobExecutionSync(
                () -> client().compute().execute(JobTarget.node(node(0)), JobDescriptor.builder(ExceptionJob.class).build(), asyncJob)
        );

        assertComputeExceptionWithClassAndMessage(cause);
    }

    @Test
    void testExceptionInJobWithSendServerExceptionStackTraceToClientPropagatesToClientWithStackTraceAsync() {
        // Second node has sendServerExceptionStackTraceToClient enabled.
        IgniteException cause = getExceptionInJobExecutionAsync(
                client().compute().submit(
                        JobTarget.node(node(1)), JobDescriptor.builder(ExceptionJob.class).build(), null
                )
        );

        assertComputeExceptionWithStackTrace(cause);
    }

    @Test
    void testExceptionInJobWithSendServerExceptionStackTraceToClientPropagatesToClientWithStackTraceSync() {
        // Second node has sendServerExceptionStackTraceToClient enabled.
        IgniteException cause = getExceptionInJobExecutionSync(
                () -> client().compute().execute(JobTarget.node(node(1)), JobDescriptor.builder(ExceptionJob.class).build(), null)
        );

        assertComputeExceptionWithStackTrace(cause);
    }

    @Test
    void testExceptionInBroadcastJobPropagatesToClient() {
        Map<ClusterNode, JobExecution<String>> executions = client().compute().submitBroadcast(
                Set.of(node(0), node(1)),
                JobDescriptor.builder(ExceptionJob.class).build(), null);

        assertComputeExceptionWithClassAndMessage(getExceptionInJobExecutionAsync(executions.get(node(0))));

        // Second node has sendServerExceptionStackTraceToClient enabled.
        assertComputeExceptionWithStackTrace(getExceptionInJobExecutionAsync(executions.get(node(1))));
    }

    @Test
    void testExceptionInColocatedTupleJobPropagatesToClientWithClassAndMessageAsync() {
        var key = Tuple.create().set(COLUMN_KEY, 1);

        IgniteException cause = getExceptionInJobExecutionAsync(
                client().compute().submit(JobTarget.colocated(TABLE_NAME, key), JobDescriptor.builder(ExceptionJob.class).build(), null));

        assertComputeExceptionWithClassAndMessage(cause);
    }

    @Test
    void testExceptionInColocatedTupleJobPropagatesToClientWithClassAndMessageSync() {
        var key = Tuple.create().set(COLUMN_KEY, 1);

        IgniteException cause = getExceptionInJobExecutionSync(
                () -> client().compute().execute(
                        JobTarget.colocated(TABLE_NAME, key),
                        JobDescriptor.builder(ExceptionJob.class).build(),
                        null
                )
        );

        assertComputeExceptionWithClassAndMessage(cause);
    }

    @Test
    void testExceptionInColocatedTupleJobWithSendServerExceptionStackTraceToClientPropagatesToClientWithStackTraceAsync() {
        // Second node has sendServerExceptionStackTraceToClient enabled.
        var key = Tuple.create().set(COLUMN_KEY, 2);

        IgniteCompute igniteCompute = client().compute();
        IgniteException cause = getExceptionInJobExecutionAsync(
                igniteCompute.submit(JobTarget.colocated(TABLE_NAME, key), JobDescriptor.builder(ExceptionJob.class).build(), null)
        );

        assertComputeExceptionWithStackTrace(cause);
    }

    @Test
    void testExceptionInColocatedTupleJobWithSendServerExceptionStackTraceToClientPropagatesToClientWithStackTraceSync() {
        // Second node has sendServerExceptionStackTraceToClient enabled.
        var key = Tuple.create().set(COLUMN_KEY, 2);

        IgniteException cause = getExceptionInJobExecutionSync(
                () -> client().compute().execute(
                        JobTarget.colocated(TABLE_NAME, key),
                        JobDescriptor.builder(ExceptionJob.class).build(),
                        null
                )
        );

        assertComputeExceptionWithStackTrace(cause);
    }

    @Test
    void testExceptionInColocatedPojoJobPropagatesToClientWithClassAndMessageAsync() {
        var key = new TestPojo(1);
        Mapper<TestPojo> mapper = Mapper.of(TestPojo.class);

        IgniteCompute igniteCompute = client().compute();
        IgniteException cause = getExceptionInJobExecutionAsync(
                igniteCompute.submit(JobTarget.colocated(
                        TABLE_NAME, key, mapper), JobDescriptor.builder(ExceptionJob.class).build(), null
                )
        );

        assertComputeExceptionWithClassAndMessage(cause);
    }

    @Test
    void testExceptionInColocatedPojoJobPropagatesToClientWithClassAndMessageSync() {
        var key = new TestPojo(1);
        Mapper<TestPojo> mapper = Mapper.of(TestPojo.class);

        IgniteException cause = getExceptionInJobExecutionSync(() -> client().compute().execute(
                        JobTarget.colocated(TABLE_NAME, key, mapper),
                        JobDescriptor.builder(ExceptionJob.class).build(), null)
        );

        assertComputeExceptionWithClassAndMessage(cause);
    }

    @Test
    void testExceptionInColocatedPojoJobWithSendServerExceptionStackTraceToClientPropagatesToClientWithStackTraceAsync() {
        // Second node has sendServerExceptionStackTraceToClient enabled.
        var key = new TestPojo(2);
        Mapper<TestPojo> mapper = Mapper.of(TestPojo.class);

        IgniteCompute igniteCompute = client().compute();
        IgniteException cause = getExceptionInJobExecutionAsync(
                igniteCompute.submit(JobTarget.colocated(TABLE_NAME, key, mapper), JobDescriptor.builder(ExceptionJob.class).build(), null)
        );

        assertComputeExceptionWithStackTrace(cause);
    }

    @Test
    void testExceptionInColocatedPojoJobWithSendServerExceptionStackTraceToClientPropagatesToClientWithStackTraceSync() {
        // Second node has sendServerExceptionStackTraceToClient enabled.
        var key = new TestPojo(2);
        Mapper<TestPojo> mapper = Mapper.of(TestPojo.class);

        IgniteException cause = getExceptionInJobExecutionSync(
                () -> client().compute().execute(
                        JobTarget.colocated(TABLE_NAME, key, mapper), JobDescriptor.builder(ExceptionJob.class).build(), null)
        );

        assertComputeExceptionWithStackTrace(cause);
    }

    private static IgniteException getExceptionInJobExecutionAsync(JobExecution<String> execution) {
        CompletionException ex = assertThrows(
                CompletionException.class,
                () -> execution.resultAsync().join()
        );

        assertThat(execution.stateAsync(), willBe(jobStateWithStatus(FAILED)));

        return (IgniteException) ex.getCause();
    }

    private static IgniteException getExceptionInJobExecutionSync(Supplier<String> execution) {
        IgniteException ex = assertThrows(IgniteException.class, execution::get);

        return (IgniteException) ex.getCause();
    }

    private static void assertComputeExceptionWithClassAndMessage(IgniteException cause) {
        String expectedMessage = "Job execution failed: java.lang.ArithmeticException: math err";
        assertTraceableException(cause, ComputeException.class, COMPUTE_JOB_FAILED_ERR, expectedMessage);

        assertNull(cause.getCause()); // No stack trace by default.
    }

    private static void assertComputeExceptionWithStackTrace(IgniteException cause) {
        String expectedMessage = "Job execution failed: java.lang.ArithmeticException: math err";
        assertTraceableException(cause, ComputeException.class, COMPUTE_JOB_FAILED_ERR, expectedMessage);

        assertNotNull(cause.getCause());

        assertThat(cause.getCause().getMessage(), containsString(
                "Caused by: java.lang.ArithmeticException: math err" + System.lineSeparator()
                        + "\tat org.apache.ignite.internal.runner.app.client.ItThinClientComputeTest$"
                        + "ExceptionJob.executeAsync(ItThinClientComputeTest.java:")
        );
    }

    @ParameterizedTest
    @CsvSource({"1,3344", "2,3345", "3,3345", "10,3344"})
    void testExecuteColocatedTupleRunsComputeJobOnKeyNode(int key, int port) {
        var keyTuple = Tuple.create().set(COLUMN_KEY, key);

        IgniteCompute igniteCompute = client().compute();
        JobExecution<String> tupleExecution = igniteCompute.submit(
                JobTarget.colocated(TABLE_NAME, keyTuple), JobDescriptor.builder(NodeNameJob.class).build(), null
        );

        String expectedNode = "itcct_n_" + port;
        assertThat(tupleExecution.resultAsync(), willBe(expectedNode));

        assertThat(tupleExecution.stateAsync(), willBe(jobStateWithStatus(COMPLETED)));
    }

    @ParameterizedTest
    @CsvSource({"1,3344", "2,3345", "3,3345", "10,3344"})
    void testExecuteColocatedPojoRunsComputeJobOnKeyNode(int key, int port) {
        var keyPojo = new TestPojo(key);

        IgniteCompute igniteCompute = client().compute();
        Mapper<TestPojo> keyMapper = Mapper.of(TestPojo.class);
        JobExecution<String> pojoExecution = igniteCompute.submit(
                JobTarget.colocated(TABLE_NAME, keyPojo, keyMapper), JobDescriptor.builder(NodeNameJob.class).build(), null);

        String expectedNode = "itcct_n_" + port;
        assertThat(pojoExecution.resultAsync(), willBe(expectedNode));

        assertThat(pojoExecution.stateAsync(), willBe(jobStateWithStatus(COMPLETED)));
    }

    @ParameterizedTest
    @CsvSource({"1,3344", "2,3345", "3,3345", "10,3344"})
    void testCancelColocatedTuple(int key, int port) {
        var keyTuple = Tuple.create().set(COLUMN_KEY, key);
        int sleepMs = 1_000_000;

        IgniteCompute igniteCompute = client().compute();
        JobExecution<Void> tupleExecution = igniteCompute.submit(
                JobTarget.colocated(TABLE_NAME, keyTuple), JobDescriptor.builder(SleepJob.class).build(), sleepMs);

        await().until(tupleExecution::stateAsync, willBe(jobStateWithStatus(EXECUTING)));

        assertThat(tupleExecution.cancelAsync(), willBe(true));

        await().until(tupleExecution::stateAsync, willBe(jobStateWithStatus(CANCELED)));
    }

    @ParameterizedTest
    @CsvSource({"1,3344", "2,3345", "3,3345", "10,3344"})
    void testCancelColocatedPojo(int key, int port) {
        var keyPojo = new TestPojo(key);
        int sleepMs = 1_000_000;

        IgniteCompute igniteCompute = client().compute();
        Mapper<TestPojo> keyMapper = Mapper.of(TestPojo.class);
        JobExecution<Void> pojoExecution = igniteCompute.submit(
                JobTarget.colocated(TABLE_NAME, keyPojo, keyMapper), JobDescriptor.builder(SleepJob.class).build(), sleepMs);

        await().until(pojoExecution::stateAsync, willBe(jobStateWithStatus(EXECUTING)));

        assertThat(pojoExecution.cancelAsync(), willBe(true));

        await().until(pojoExecution::stateAsync, willBe(jobStateWithStatus(CANCELED)));
    }

    @Test
    void testExecuteOnUnknownUnitWithLatestVersionThrows() {
        CompletionException ex = assertThrows(
                CompletionException.class,
                () -> {
                    IgniteCompute igniteCompute = client().compute();
                    JobTarget target = JobTarget.node(node(0));
                    List<DeploymentUnit> units = List.of(new DeploymentUnit("u", "latest"));
                    igniteCompute.executeAsync(target, JobDescriptor.builder(NodeNameJob.class).units(units).build(), null).join();
                });

        var cause = (IgniteException) ex.getCause();
        assertThat(cause.getMessage(), containsString("Deployment unit u:latest doesn't exist"));

        // TODO IGNITE-19823 DeploymentUnitNotFoundException is internal, does not propagate to client.
        assertEquals(INTERNAL_ERR, cause.code());
    }

    @Test
    void testExecuteColocatedOnUnknownUnitWithLatestVersionThrows() {
        CompletionException ex = assertThrows(
                CompletionException.class,
                () -> client().compute().executeAsync(
                        JobTarget.colocated(TABLE_NAME, Tuple.create().set(COLUMN_KEY, 1)),
                        JobDescriptor.builder(NodeNameJob.class)
                                .units(new DeploymentUnit("u", "latest"))
                                .build(),
                        null
                ).join());

        var cause = (IgniteException) ex.getCause();
        assertThat(cause.getMessage(), containsString("Deployment unit u:latest doesn't exist"));

        // TODO IGNITE-19823 DeploymentUnitNotFoundException is internal, does not propagate to client.
        assertEquals(INTERNAL_ERR, cause.code());
    }

    @Test
    void testDelayedJobExecutionThrowsWhenConnectionFails() throws Exception {
        Builder builder = IgniteClient.builder().addresses(getClientAddresses().toArray(new String[0]));
        try (IgniteClient client = builder.build()) {
            int delayMs = 3000;
            CompletableFuture<Void> jobFut = client.compute().executeAsync(
                    JobTarget.node(node(0)), JobDescriptor.builder(SleepJob.class).build(), delayMs);

            // Wait a bit and close the connection.
            Thread.sleep(10);
            client.close();

            assertThat(jobFut, willThrowFast(IgniteClientConnectionException.class, "Channel is closed"));
        }
    }

    @Test
    void testAllSupportedArgTypes() {
        testEchoArg(Byte.MAX_VALUE);
        testEchoArg(Short.MAX_VALUE);
        testEchoArg(Integer.MAX_VALUE);
        testEchoArg(Long.MAX_VALUE);
        testEchoArg(Float.MAX_VALUE);
        testEchoArg(Double.MAX_VALUE);
        testEchoArg(BigDecimal.TEN);
        testEchoArg(UUID.randomUUID());
        testEchoArg("string");
        testEchoArg(LocalDate.now());
        testEchoArg(LocalTime.now());
        testEchoArg(LocalDateTime.now());
        testEchoArg(Instant.now());
    }

    @Test
    void testExecuteColocatedEscapedTableName() {
        var tableName = "\"TBL ABC\"";
        client().sql().execute(null, "CREATE TABLE " + tableName + " (key INT PRIMARY KEY, val INT)");

        Mapper<TestPojo> mapper = Mapper.of(TestPojo.class);
        TestPojo pojoKey = new TestPojo(1);
        Tuple tupleKey = Tuple.create().set("key", pojoKey.key);
        JobDescriptor<Object, String> job = JobDescriptor.builder(NodeNameJob.class).build();

        var tupleRes = client().compute().execute(JobTarget.colocated(tableName, tupleKey), job, null);
        var pojoRes = client().compute().execute(JobTarget.colocated(tableName, pojoKey, mapper), job, null);

        assertEquals(tupleRes, pojoRes);
    }

    @ParameterizedTest
    @CsvSource({"1E3,-3", "1.12E5,-5", "1.12E5,0", "1.123456789,10", "1.123456789,5"})
    void testBigDecimalPropagation(String number, int scale) {
        BigDecimal res = client().compute().execute(
                JobTarget.node(node(0)),
                JobDescriptor.builder(DecimalJob.class).build(),
                number + "," + scale);

        var expected = new BigDecimal(number).setScale(scale, RoundingMode.HALF_UP);
        assertEquals(expected, res);
    }

    @Test
    void testExecuteMapReduce() throws Exception {
        IgniteCompute igniteCompute = client().compute();
        TaskDescriptor<String, String> taskDescriptor = TaskDescriptor.builder(MapReduceNodeNameTask.class).build();
        TaskExecution<String> execution = igniteCompute.submitMapReduce(taskDescriptor, null);

        List<Matcher<? super String>> nodeNames = sortedNodes().stream()
                .map(ClusterNode::name)
                .map(Matchers::containsString)
                .collect(Collectors.toList());
        assertThat(execution.resultAsync(), willBe(allOf(nodeNames)));

        assertThat(execution.stateAsync(), willBe(taskStateWithStatus(TaskStatus.COMPLETED)));
        assertThat(execution.statesAsync(), willBe(everyItem(jobStateWithStatus(COMPLETED))));

        assertThat("compute task and sub tasks ids must be different",
                execution.idsAsync(), willBe(not(hasItem(execution.idAsync().get()))));
    }

    @Test
    void testExecuteMapReduceWithArgs() {
        IgniteCompute igniteCompute = client().compute();
        TaskDescriptor<String, String> taskDescriptor = TaskDescriptor.builder(MapReduceArgsTask.class).build();
        TaskExecution<String> execution = igniteCompute.submitMapReduce(taskDescriptor, "1:2:3.3");

        assertThat(execution.resultAsync(), willBe(containsString("1_2_3.3")));
        assertThat(execution.stateAsync(), willBe(taskStateWithStatus(TaskStatus.COMPLETED)));
    }

    @ParameterizedTest
    @ValueSource(classes = {MapReduceExceptionOnSplitTask.class, MapReduceExceptionOnReduceTask.class})
    <I, M, T> void testExecuteMapReduceExceptionPropagation(Class<? extends MapReduceTask<I, M, T, String>> taskClass) {
        IgniteCompute igniteCompute = client().compute();
        TaskDescriptor<I, String> taskDescriptor = TaskDescriptor.builder(taskClass).build();
        IgniteException cause = getExceptionInJobExecutionAsync(new TaskToJobExecutionWrapper<>(
                igniteCompute.submitMapReduce(taskDescriptor, null))
        );

        assertThat(cause.getMessage(), containsString("Custom job error"));
        assertEquals(TRACE_ID, cause.traceId());
        assertEquals(COLUMN_ALREADY_EXISTS_ERR, cause.code());
        assertInstanceOf(CustomException.class, cause);
        assertNull(cause.getCause()); // No stack trace by default.
    }

    private void testEchoArg(Object arg) {
        Object res = client().compute().execute(JobTarget.node(node(0)), JobDescriptor.builder(EchoJob.class).build(), arg);

        if (arg instanceof byte[]) {
            assertArrayEquals((byte[]) arg, (byte[]) res);
        } else {
            assertEquals(arg, res);
        }

        String str = client().compute().execute(JobTarget.node(node(0)), JobDescriptor.builder(ToStringJob.class).build(), arg);
        assertEquals(arg.toString(), str);
    }

    private ClusterNode node(int idx) {
        return sortedNodes().get(idx);
    }

    private List<ClusterNode> sortedNodes() {
        return client().clusterNodes().stream()
                .sorted(Comparator.comparing(ClusterNode::name))
                .collect(Collectors.toList());
    }

    private static class NodeNameJob implements ComputeJob<Object, String> {
        @Override
        public CompletableFuture<String> executeAsync(JobExecutionContext context, Object arg) {
            return completedFuture(context.ignite().name() + (arg == null ? "" : arg.toString()));
        }
    }

    private static class ConcatJob implements ComputeJob<String, String> {
        @Override
        public CompletableFuture<String> executeAsync(JobExecutionContext context, String args) {
            if (args == null) {
                return nullCompletedFuture();
            }

            return completedFuture(
                    Arrays.stream(args.split(":"))
                            .map(o -> o == null ? "null" : o.toString())
                            .collect(Collectors.joining("_")));
        }
    }

    private static class IgniteExceptionJob implements ComputeJob<Object, String> {
        @Override
        public CompletableFuture<String> executeAsync(JobExecutionContext context, Object args) {
            throw new CustomException(TRACE_ID, COLUMN_ALREADY_EXISTS_ERR, "Custom job error", null);
        }
    }

    private static class ExceptionJob implements ComputeJob<Boolean, String> {
        @Override
        public CompletableFuture<String> executeAsync(JobExecutionContext context, Boolean arg) {
            boolean asyncJob = arg != null && arg;

            if (asyncJob) {
                return failedFuture(new ArithmeticException("math err"));
            } else {
                throw new ArithmeticException("math err");
            }
        }
    }

    private static class EchoJob implements ComputeJob<Object, Object> {
        @Override
        public CompletableFuture<Object> executeAsync(JobExecutionContext context, Object arg) {
            return completedFuture(arg);
        }
    }

    private static class ToStringJob implements ComputeJob<Object, String> {
        @Override
        public CompletableFuture<String> executeAsync(JobExecutionContext context, Object arg) {
            if (arg instanceof byte[]) {
                return completedFuture(Arrays.toString((byte[]) arg));
            }

            return completedFuture(arg.toString());
        }
    }

    private static class SleepJob implements ComputeJob<Integer, Void> {
        @Override
        public @Nullable CompletableFuture<Void> executeAsync(JobExecutionContext context, Integer args) {
            try {
                Thread.sleep(args);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }

            return null;
        }
    }

    private static class AsyncSleepJob implements ComputeJob<Integer, Void> {
        @Override
        public @Nullable CompletableFuture<Void> executeAsync(JobExecutionContext context, Integer args) {
            return CompletableFuture.runAsync(() -> {
                try {
                    Thread.sleep(args);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            });
        }
    }

    private static class DecimalJob implements ComputeJob<String, BigDecimal> {
        @Override
        public CompletableFuture<BigDecimal> executeAsync(JobExecutionContext context, String arg) {
            @SuppressWarnings("DataFlowIssue")
            var args = arg.split(",", 2);

            return completedFuture(new BigDecimal(args[0]).setScale(Integer.parseInt(args[1]), RoundingMode.HALF_UP));
        }
    }

    private static class MapReduceNodeNameTask implements MapReduceTask<String, Object, String, String> {
        @Override
        public CompletableFuture<List<MapReduceJob<Object, String>>> splitAsync(TaskExecutionContext context, String args) {
            return completedFuture(context.ignite().clusterNodes().stream()
                    .map(node -> MapReduceJob.<Object, String>builder()
                            .jobDescriptor(JobDescriptor.builder(NodeNameJob.class).build())
                            .nodes(Set.of(node))
                            .args(args)
                            .build())
                    .collect(Collectors.toList()));
        }

        @Override
        public CompletableFuture<String> reduceAsync(TaskExecutionContext context, Map<UUID, String> results) {
            return completedFuture(results.values().stream()
                    .map(String.class::cast)
                    .collect(Collectors.joining(",")));
        }
    }

    private static class MapReduceArgsTask implements MapReduceTask<String, String, String, String> {
        @Override
        public CompletableFuture<List<MapReduceJob<String, String>>> splitAsync(TaskExecutionContext context, String args) {
            return completedFuture(context.ignite().clusterNodes().stream()
                    .map(node -> MapReduceJob.<String, String>builder()
                            .jobDescriptor(JobDescriptor.builder(ConcatJob.class).build())
                            .nodes(Set.of(node))
                            .args(args)
                            .build())
                    .collect(Collectors.toList()));
        }

        @Override
        public CompletableFuture<String> reduceAsync(TaskExecutionContext context, Map<UUID, String> results) {
            return completedFuture(results.values().stream()
                    .map(String.class::cast)
                    .collect(Collectors.joining(",")));
        }
    }

    private static class MapReduceExceptionOnSplitTask implements MapReduceTask<Object, Object, Object, String> {
        @Override
        public CompletableFuture<List<MapReduceJob<Object, Object>>> splitAsync(TaskExecutionContext context, Object args) {
            throw new CustomException(TRACE_ID, COLUMN_ALREADY_EXISTS_ERR, "Custom job error", null);
        }

        @Override
        public CompletableFuture<String> reduceAsync(TaskExecutionContext context, Map<UUID, Object> results) {
            return completedFuture("expected split exception");
        }
    }

    private static class MapReduceExceptionOnReduceTask implements MapReduceTask<Object, Object, String, String> {

        @Override
        public CompletableFuture<List<MapReduceJob<Object, String>>> splitAsync(TaskExecutionContext context, Object args) {
            return completedFuture(context.ignite().clusterNodes().stream()
                    .map(node -> MapReduceJob.<Object, String>builder()
                            .jobDescriptor(JobDescriptor.builder(NodeNameJob.class).build())
                            .nodes(Set.of(node))
                            .args(args)
                            .build())
                    .collect(Collectors.toList()));
        }

        @Override
        public CompletableFuture<String> reduceAsync(TaskExecutionContext context, Map<UUID, String> results) {
            throw new CustomException(TRACE_ID, COLUMN_ALREADY_EXISTS_ERR, "Custom job error", null);
        }
    }

    /**
     * Custom public exception class.
     */
    public static class CustomException extends IgniteException {
        public CustomException(UUID traceId, int code, String message, Throwable cause) {
            super(traceId, code, message, cause);
        }
    }
}
