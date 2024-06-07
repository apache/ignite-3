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

import static org.apache.ignite.compute.JobState.CANCELED;
import static org.apache.ignite.compute.JobState.COMPLETED;
import static org.apache.ignite.compute.JobState.EXECUTING;
import static org.apache.ignite.compute.JobState.FAILED;
import static org.apache.ignite.compute.JobState.QUEUED;
import static org.apache.ignite.internal.IgniteExceptionTestUtils.assertTraceableException;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureExceptionMatcher.willThrowFast;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.will;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willBe;
import static org.apache.ignite.internal.testframework.matchers.JobStatusMatcher.jobStatusWithState;
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
import java.math.BigInteger;
import java.math.RoundingMode;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Arrays;
import java.util.BitSet;
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
import org.apache.ignite.compute.DeploymentUnit;
import org.apache.ignite.compute.ExecutionTarget;
import org.apache.ignite.compute.JobDescriptor;
import org.apache.ignite.compute.JobExecution;
import org.apache.ignite.compute.JobExecutionContext;
import org.apache.ignite.compute.TaskExecution;
import org.apache.ignite.compute.task.ComputeJobRunner;
import org.apache.ignite.compute.task.MapReduceTask;
import org.apache.ignite.compute.task.TaskExecutionContext;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.table.Tuple;
import org.apache.ignite.table.mapper.Mapper;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;
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
        JobDescriptor job = JobDescriptor.builder().jobClass(NodeNameJob.class).build();
        String res1 = client().compute().execute(ExecutionTarget.fromNode(node(0)), job);
        String res2 = client().compute().execute(ExecutionTarget.fromNode(node(1)), job);

        assertEquals("itcct_n_3344", res1);
        assertEquals("itcct_n_3345", res2);
    }

    @Test
    void testExecuteOnSpecificNodeAsync() {
        JobDescriptor job = JobDescriptor.builder().jobClass(NodeNameJob.class).build();
        JobExecution<String> execution1 = client().compute().submit(ExecutionTarget.fromNode(node(0)), job);
        JobExecution<String> execution2 = client().compute().submit(ExecutionTarget.fromNode(node(1)), job);

        assertThat(execution1.resultAsync(), willBe("itcct_n_3344"));
        assertThat(execution2.resultAsync(), willBe("itcct_n_3345"));

        assertThat(execution1.statusAsync(), willBe(jobStatusWithState(COMPLETED)));
        assertThat(execution2.statusAsync(), willBe(jobStatusWithState(COMPLETED)));
    }

    @Test
    void testCancellingCompletedJob() {
        JobExecution<String> execution = client().compute().submit(Set.of(node(0)), List.of(), NodeNameJob.class.getName());

        assertThat(execution.resultAsync(), willBe("itcct_n_3344"));

        assertThat(execution.statusAsync(), willBe(jobStatusWithState(COMPLETED)));

        assertThat(execution.cancelAsync(), willBe(false));
    }

    @Test
    void testChangingPriorityCompletedJob() {
        JobExecution<String> execution = client().compute().submit(Set.of(node(0)), List.of(), NodeNameJob.class.getName());

        assertThat(execution.resultAsync(), willBe("itcct_n_3344"));

        assertThat(execution.statusAsync(), willBe(jobStatusWithState(COMPLETED)));

        assertThat(execution.changePriorityAsync(0), willBe(false));
    }

    @Test
    void testCancelOnSpecificNodeAsync() {
        int sleepMs = 1_000_000;
        JobExecution<String> execution1 = client().compute().submit(Set.of(node(0)), List.of(), SleepJob.class.getName(), sleepMs);
        JobExecution<String> execution2 = client().compute().submit(Set.of(node(1)), List.of(), SleepJob.class.getName(), sleepMs);

        await().until(execution1::statusAsync, willBe(jobStatusWithState(EXECUTING)));
        await().until(execution2::statusAsync, willBe(jobStatusWithState(EXECUTING)));

        assertThat(execution1.cancelAsync(), willBe(true));
        assertThat(execution2.cancelAsync(), willBe(true));

        await().until(execution1::statusAsync, willBe(jobStatusWithState(CANCELED)));
        await().until(execution2::statusAsync, willBe(jobStatusWithState(CANCELED)));
    }

    @Test
    void changeJobPriority() {
        int sleepMs = 1_000_000;
        // Start 1 task in executor with 1 thread
        JobExecution<String> execution1 = client().compute().submit(Set.of(node(0)), List.of(), SleepJob.class.getName(), sleepMs);
        await().until(execution1::statusAsync, willBe(jobStatusWithState(EXECUTING)));

        // Start one more long lasting task
        JobExecution<String> execution2 = client().compute().submit(Set.of(node(0)), List.of(), SleepJob.class.getName(), sleepMs);
        await().until(execution2::statusAsync, willBe(jobStatusWithState(QUEUED)));

        // Start third task
        JobExecution<String> execution3 = client().compute().submit(Set.of(node(0)), List.of(), SleepJob.class.getName(), sleepMs);
        await().until(execution3::statusAsync, willBe(jobStatusWithState(QUEUED)));

        // Task 2 and 3 are not completed, in queue state
        assertThat(execution2.resultAsync().isDone(), is(false));
        assertThat(execution3.resultAsync().isDone(), is(false));

        // Change priority of task 3, so it should be executed before task 2
        assertThat(execution3.changePriorityAsync(2), willBe(true));

        // Cancel task 1, task 3 should start executing
        assertThat(execution1.cancelAsync(), willBe(true));
        await().until(execution1::statusAsync, willBe(jobStatusWithState(CANCELED)));
        await().until(execution3::statusAsync, willBe(jobStatusWithState(EXECUTING)));

        // Task 2 is still queued
        assertThat(execution2.statusAsync(), willBe(jobStatusWithState(QUEUED)));

        // Cleanup
        assertThat(execution2.cancelAsync(), willBe(true));
        assertThat(execution3.cancelAsync(), willBe(true));
    }

    @Test
    void testExecuteOnRandomNode() {
        String res = client().compute().execute(new HashSet<>(sortedNodes()), List.of(), NodeNameJob.class.getName());

        assertTrue(Set.of("itcct_n_3344", "itcct_n_3345").contains(res));
    }

    @Test
    void testExecuteOnRandomNodeAsync() {
        JobExecution<String> execution = client().compute()
                .submit(new HashSet<>(sortedNodes()), List.of(), NodeNameJob.class.getName());

        assertThat(
                execution.resultAsync(),
                will(oneOf("itcct_n_3344", "itcct_n_3345"))
        );
        assertThat(execution.statusAsync(), willBe(jobStatusWithState(COMPLETED)));
    }

    @Test
    void testBroadcastOneNode() {
        Map<ClusterNode, JobExecution<String>> executionsPerNode = client().compute().submitBroadcast(
                Set.of(node(1)),
                List.of(),
                NodeNameJob.class.getName(),
                "_",
                123);

        assertEquals(1, executionsPerNode.size());

        JobExecution<String> execution = executionsPerNode.get(node(1));

        assertThat(execution.resultAsync(), willBe("itcct_n_3345__123"));
        assertThat(execution.statusAsync(), willBe(jobStatusWithState(COMPLETED)));
    }

    @Test
    void testBroadcastAllNodes() {
        Map<ClusterNode, JobExecution<String>> executionsPerNode = client().compute().submitBroadcast(
                new HashSet<>(sortedNodes()),
                List.of(),
                NodeNameJob.class.getName(),
                "_",
                123);

        assertEquals(2, executionsPerNode.size());

        JobExecution<String> execution1 = executionsPerNode.get(node(0));
        JobExecution<String> execution2 = executionsPerNode.get(node(1));

        assertThat(execution1.resultAsync(), willBe("itcct_n_3344__123"));
        assertThat(execution2.resultAsync(), willBe("itcct_n_3345__123"));

        assertThat(execution1.statusAsync(), willBe(jobStatusWithState(COMPLETED)));
        assertThat(execution2.statusAsync(), willBe(jobStatusWithState(COMPLETED)));
    }

    @Test
    void testCancelBroadcastAllNodes() {
        int sleepMs = 1_000_000;
        Map<ClusterNode, JobExecution<String>> executionsPerNode = client().compute().submitBroadcast(
                new HashSet<>(sortedNodes()),
                List.of(),
                SleepJob.class.getName(),
                sleepMs
        );

        assertEquals(2, executionsPerNode.size());

        JobExecution<String> execution1 = executionsPerNode.get(node(0));
        JobExecution<String> execution2 = executionsPerNode.get(node(1));

        await().until(execution1::statusAsync, willBe(jobStatusWithState(EXECUTING)));
        await().until(execution2::statusAsync, willBe(jobStatusWithState(EXECUTING)));

        assertThat(execution1.cancelAsync(), willBe(true));
        assertThat(execution2.cancelAsync(), willBe(true));

        await().until(execution1::statusAsync, willBe(jobStatusWithState(CANCELED)));
        await().until(execution2::statusAsync, willBe(jobStatusWithState(CANCELED)));
    }

    @Test
    void testExecuteWithArgs() {
        var nodes = new HashSet<>(client().clusterNodes());
        JobExecution<String> execution = client().compute().submit(nodes, List.of(), ConcatJob.class.getName(), 1, "2", 3.3);

        assertThat(execution.resultAsync(), willBe("1_2_3.3"));
        assertThat(execution.statusAsync(), willBe(jobStatusWithState(COMPLETED)));
    }

    @Test
    void testIgniteExceptionInJobPropagatesToClientWithMessageAndCodeAndTraceIdAsync() {
        IgniteException cause = getExceptionInJobExecutionAsync(
                client().compute().submit(Set.of(node(0)), List.of(), IgniteExceptionJob.class.getName())
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
                () -> client().compute().execute(Set.of(node(0)), List.of(), IgniteExceptionJob.class.getName())
        );

        assertThat(cause.getMessage(), containsString("Custom job error"));
        assertEquals(TRACE_ID, cause.traceId());
        assertEquals(COLUMN_ALREADY_EXISTS_ERR, cause.code());
        assertInstanceOf(CustomException.class, cause);
        assertNull(cause.getCause()); // No stack trace by default.
    }

    @Test
    void testExceptionInJobPropagatesToClientWithClassAndMessageAsync() {
        IgniteException cause = getExceptionInJobExecutionAsync(
                client().compute().submit(Set.of(node(0)), List.of(), ExceptionJob.class.getName())
        );

        assertComputeExceptionWithClassAndMessage(cause);
    }

    @Test
    void testExceptionInJobPropagatesToClientWithClassAndMessageSync() {
        IgniteException cause = getExceptionInJobExecutionSync(
                () -> client().compute().execute(Set.of(node(0)), List.of(), ExceptionJob.class.getName())
        );

        assertComputeExceptionWithClassAndMessage(cause);
    }

    @Test
    void testExceptionInJobWithSendServerExceptionStackTraceToClientPropagatesToClientWithStackTraceAsync() {
        // Second node has sendServerExceptionStackTraceToClient enabled.
        IgniteException cause = getExceptionInJobExecutionAsync(
                client().compute().submit(Set.of(node(1)), List.of(), ExceptionJob.class.getName())
        );

        assertComputeExceptionWithStackTrace(cause);
    }

    @Test
    void testExceptionInJobWithSendServerExceptionStackTraceToClientPropagatesToClientWithStackTraceSync() {
        // Second node has sendServerExceptionStackTraceToClient enabled.
        IgniteException cause = getExceptionInJobExecutionSync(
                () -> client().compute().execute(Set.of(node(1)), List.of(), ExceptionJob.class.getName())
        );

        assertComputeExceptionWithStackTrace(cause);
    }

    @Test
    void testExceptionInBroadcastJobPropagatesToClient() {
        Map<ClusterNode, JobExecution<String>> executions = client().compute().submitBroadcast(
                Set.of(node(0), node(1)), List.of(), ExceptionJob.class.getName()
        );

        assertComputeExceptionWithClassAndMessage(getExceptionInJobExecutionAsync(executions.get(node(0))));

        // Second node has sendServerExceptionStackTraceToClient enabled.
        assertComputeExceptionWithStackTrace(getExceptionInJobExecutionAsync(executions.get(node(1))));
    }

    @Test
    void testExceptionInColocatedTupleJobPropagatesToClientWithClassAndMessageAsync() {
        var key = Tuple.create().set(COLUMN_KEY, 1);

        IgniteException cause = getExceptionInJobExecutionAsync(
                client().compute().submitColocated(TABLE_NAME, key, List.of(), ExceptionJob.class.getName()
        ));

        assertComputeExceptionWithClassAndMessage(cause);
    }

    @Test
    void testExceptionInColocatedTupleJobPropagatesToClientWithClassAndMessageSync() {
        var key = Tuple.create().set(COLUMN_KEY, 1);

        IgniteException cause = getExceptionInJobExecutionSync(
                () -> client().compute().executeColocated(TABLE_NAME, key, List.of(), ExceptionJob.class.getName())
        );

        assertComputeExceptionWithClassAndMessage(cause);
    }

    @Test
    void testExceptionInColocatedTupleJobWithSendServerExceptionStackTraceToClientPropagatesToClientWithStackTraceAsync() {
        // Second node has sendServerExceptionStackTraceToClient enabled.
        var key = Tuple.create().set(COLUMN_KEY, 2);

        IgniteException cause = getExceptionInJobExecutionAsync(
                client().compute().submitColocated(TABLE_NAME, key, List.of(), ExceptionJob.class.getName())
        );

        assertComputeExceptionWithStackTrace(cause);
    }

    @Test
    void testExceptionInColocatedTupleJobWithSendServerExceptionStackTraceToClientPropagatesToClientWithStackTraceSync() {
        // Second node has sendServerExceptionStackTraceToClient enabled.
        var key = Tuple.create().set(COLUMN_KEY, 2);

        IgniteException cause = getExceptionInJobExecutionSync(
                () -> client().compute().executeColocated(TABLE_NAME, key, List.of(), ExceptionJob.class.getName())
        );

        assertComputeExceptionWithStackTrace(cause);
    }

    @Test
    void testExceptionInColocatedPojoJobPropagatesToClientWithClassAndMessageAsync() {
        var key = new TestPojo(1);
        Mapper<TestPojo> mapper = Mapper.of(TestPojo.class);

        IgniteException cause = getExceptionInJobExecutionAsync(
                client().compute().submitColocated(TABLE_NAME, key, mapper, List.of(), ExceptionJob.class.getName())
        );

        assertComputeExceptionWithClassAndMessage(cause);
    }

    @Test
    void testExceptionInColocatedPojoJobPropagatesToClientWithClassAndMessageSync() {
        var key = new TestPojo(1);
        Mapper<TestPojo> mapper = Mapper.of(TestPojo.class);

        IgniteException cause = getExceptionInJobExecutionSync(
                () -> client().compute().executeColocated(TABLE_NAME, key, mapper, List.of(), ExceptionJob.class.getName())
        );

        assertComputeExceptionWithClassAndMessage(cause);
    }

    @Test
    void testExceptionInColocatedPojoJobWithSendServerExceptionStackTraceToClientPropagatesToClientWithStackTraceAsync() {
        // Second node has sendServerExceptionStackTraceToClient enabled.
        var key = new TestPojo(2);
        Mapper<TestPojo> mapper = Mapper.of(TestPojo.class);

        IgniteException cause = getExceptionInJobExecutionAsync(
                client().compute().submitColocated(TABLE_NAME, key, mapper, List.of(), ExceptionJob.class.getName())
        );

        assertComputeExceptionWithStackTrace(cause);
    }

    @Test
    void testExceptionInColocatedPojoJobWithSendServerExceptionStackTraceToClientPropagatesToClientWithStackTraceSync() {
        // Second node has sendServerExceptionStackTraceToClient enabled.
        var key = new TestPojo(2);
        Mapper<TestPojo> mapper = Mapper.of(TestPojo.class);

        IgniteException cause = getExceptionInJobExecutionSync(
                () -> client().compute().executeColocated(TABLE_NAME, key, mapper, List.of(), ExceptionJob.class.getName())
        );

        assertComputeExceptionWithStackTrace(cause);
    }

    private static IgniteException getExceptionInJobExecutionAsync(JobExecution<String> execution) {
        CompletionException ex = assertThrows(
                CompletionException.class,
                () -> execution.resultAsync().join()
        );

        assertThat(execution.statusAsync(), willBe(jobStatusWithState(FAILED)));

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
                        + "ExceptionJob.execute(ItThinClientComputeTest.java:")
        );
    }

    @ParameterizedTest
    @CsvSource({"1,3344", "2,3345", "3,3345", "10,3344"})
    void testExecuteColocatedTupleRunsComputeJobOnKeyNode(int key, int port) {
        Tuple keyTuple = Tuple.create().set(COLUMN_KEY, key);
        JobDescriptor job = JobDescriptor.builder().jobClass(NodeNameJob.class).build();
        ExecutionTarget target = ExecutionTarget.fromColocationKey(TABLE_NAME, keyTuple);

        JobExecution<String> tupleExecution = client().compute().submit(target, job);

        String expectedNode = "itcct_n_" + port;
        assertThat(tupleExecution.resultAsync(), willBe(expectedNode));

        assertThat(tupleExecution.statusAsync(), willBe(jobStatusWithState(COMPLETED)));
    }

    @ParameterizedTest
    @CsvSource({"1,3344", "2,3345", "3,3345", "10,3344"})
    void testExecuteColocatedPojoRunsComputeJobOnKeyNode(int key, int port) {
        var keyPojo = new TestPojo(key);

        JobExecution<String> pojoExecution = client().compute().submitColocated(
                TABLE_NAME,
                keyPojo,
                Mapper.of(TestPojo.class),
                List.of(),
                NodeNameJob.class.getName()
        );

        String expectedNode = "itcct_n_" + port;
        assertThat(pojoExecution.resultAsync(), willBe(expectedNode));

        assertThat(pojoExecution.statusAsync(), willBe(jobStatusWithState(COMPLETED)));
    }

    @ParameterizedTest
    @CsvSource({"1,3344", "2,3345", "3,3345", "10,3344"})
    void testCancelColocatedTuple(int key, int port) {
        var keyTuple = Tuple.create().set(COLUMN_KEY, key);
        int sleepMs = 1_000_000;

        JobExecution<String> tupleExecution = client().compute().submitColocated(
                TABLE_NAME,
                keyTuple,
                List.of(),
                SleepJob.class.getName(),
                sleepMs
        );

        await().until(tupleExecution::statusAsync, willBe(jobStatusWithState(EXECUTING)));

        assertThat(tupleExecution.cancelAsync(), willBe(true));

        await().until(tupleExecution::statusAsync, willBe(jobStatusWithState(CANCELED)));
    }

    @ParameterizedTest
    @CsvSource({"1,3344", "2,3345", "3,3345", "10,3344"})
    void testCancelColocatedPojo(int key, int port) {
        var keyPojo = new TestPojo(key);
        int sleepMs = 1_000_000;

        JobExecution<String> pojoExecution = client().compute().submitColocated(
                TABLE_NAME,
                keyPojo,
                Mapper.of(TestPojo.class),
                List.of(),
                SleepJob.class.getName(),
                sleepMs
        );

        await().until(pojoExecution::statusAsync, willBe(jobStatusWithState(EXECUTING)));

        assertThat(pojoExecution.cancelAsync(), willBe(true));

        await().until(pojoExecution::statusAsync, willBe(jobStatusWithState(CANCELED)));
    }

    @Test
    void testExecuteOnUnknownUnitWithLatestVersionThrows() {
        CompletionException ex = assertThrows(
                CompletionException.class,
                () -> client().compute().executeAsync(
                        Set.of(node(0)),
                        List.of(new DeploymentUnit("u", "latest")),
                        NodeNameJob.class.getName()).join());

        var cause = (IgniteException) ex.getCause();
        assertThat(cause.getMessage(), containsString("Deployment unit u:latest doesn't exist"));

        // TODO IGNITE-19823 DeploymentUnitNotFoundException is internal, does not propagate to client.
        assertEquals(INTERNAL_ERR, cause.code());
    }

    @Test
    void testExecuteColocatedOnUnknownUnitWithLatestVersionThrows() {
        CompletionException ex = assertThrows(
                CompletionException.class,
                () -> client().compute().executeColocatedAsync(
                        TABLE_NAME,
                        Tuple.create().set(COLUMN_KEY, 1),
                        List.of(new DeploymentUnit("u", "latest")),
                        NodeNameJob.class.getName()).join());

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
            CompletableFuture<String> jobFut = client.compute().executeAsync(
                    Set.of(node(0)), List.of(), SleepJob.class.getName(), delayMs);

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
        testEchoArg(new byte[] {1, 2, 3});
        testEchoArg(new BitSet(10));
        testEchoArg(LocalDate.now());
        testEchoArg(LocalTime.now());
        testEchoArg(LocalDateTime.now());
        testEchoArg(Instant.now());
        testEchoArg(BigInteger.TEN);
    }

    @Test
    void testExecuteColocatedEscapedTableName() {
        var tableName = "\"TBL ABC\"";
        client().sql().execute(null, "CREATE TABLE " + tableName + " (key INT PRIMARY KEY, val INT)");

        Mapper<TestPojo> mapper = Mapper.of(TestPojo.class);
        TestPojo pojoKey = new TestPojo(1);
        Tuple tupleKey = Tuple.create().set("key", pojoKey.key);

        var tupleRes = client().compute().executeColocated(tableName, tupleKey, List.of(), NodeNameJob.class.getName());
        var pojoRes = client().compute().executeColocated(tableName, pojoKey, mapper, List.of(), NodeNameJob.class.getName());

        assertEquals(tupleRes, pojoRes);
    }

    @ParameterizedTest
    @CsvSource({"1E3,-3", "1.12E5,-5", "1.12E5,0", "1.123456789,10", "1.123456789,5"})
    void testBigDecimalPropagation(String number, int scale) {
        BigDecimal res = client().compute().execute(Set.of(node(0)), List.of(), DecimalJob.class.getName(), number, scale);

        var expected = new BigDecimal(number).setScale(scale, RoundingMode.HALF_UP);
        assertEquals(expected, res);
    }

    @Test
    void testExecuteMapReduce() throws Exception {
        TaskExecution<String> execution = client().compute().submitMapReduce(List.of(), MapReduceNodeNameTask.class.getName());

        List<Matcher<? super String>> nodeNames = sortedNodes().stream()
                .map(ClusterNode::name)
                .map(Matchers::containsString)
                .collect(Collectors.toList());
        assertThat(execution.resultAsync(), willBe(allOf(nodeNames)));

        assertThat(execution.statusAsync(), willBe(jobStatusWithState(COMPLETED)));
        assertThat(execution.statusesAsync(), willBe(everyItem(jobStatusWithState(COMPLETED))));

        assertThat("compute task and sub tasks ids must be different",
                execution.idsAsync(), willBe(not(hasItem(execution.idAsync().get()))));
    }

    @Test
    void testExecuteMapReduceWithArgs() {
        TaskExecution<String> execution = client().compute()
                .submitMapReduce(List.of(), MapReduceArgsTask.class.getName(), 1, "2", 3.3);

        assertThat(execution.resultAsync(), willBe(containsString("1_2_3.3")));
        assertThat(execution.statusAsync(), willBe(jobStatusWithState(COMPLETED)));
    }

    @ParameterizedTest
    @ValueSource(classes = {MapReduceExceptionOnSplitTask.class, MapReduceExceptionOnReduceTask.class})
    void testExecuteMapReduceExceptionPropagation(Class<?> taskClass) {
        IgniteException cause = getExceptionInJobExecutionAsync(
                client().compute().submitMapReduce(List.of(), taskClass.getName())
        );

        assertThat(cause.getMessage(), containsString("Custom job error"));
        assertEquals(TRACE_ID, cause.traceId());
        assertEquals(COLUMN_ALREADY_EXISTS_ERR, cause.code());
        assertInstanceOf(CustomException.class, cause);
        assertNull(cause.getCause()); // No stack trace by default.
    }

    private void testEchoArg(Object arg) {
        Object res = client().compute().execute(Set.of(node(0)), List.of(), EchoJob.class.getName(), arg, arg.toString());

        if (arg instanceof byte[]) {
            assertArrayEquals((byte[]) arg, (byte[]) res);
        } else {
            assertEquals(arg, res);
        }
    }

    private ClusterNode node(int idx) {
        return sortedNodes().get(idx);
    }

    private List<ClusterNode> sortedNodes() {
        return client().clusterNodes().stream()
                .sorted(Comparator.comparing(ClusterNode::name))
                .collect(Collectors.toList());
    }

    private static class NodeNameJob implements ComputeJob<String> {
        @Override
        public String execute(JobExecutionContext context, Object... args) {
            return context.ignite().name() + Arrays.stream(args).map(Object::toString).collect(Collectors.joining("_"));
        }
    }

    private static class ConcatJob implements ComputeJob<String> {
        @Override
        public String execute(JobExecutionContext context, Object... args) {
            if (args == null) {
                return null;
            }

            return Arrays.stream(args).map(o -> o == null ? "null" : o.toString()).collect(Collectors.joining("_"));
        }
    }

    private static class IgniteExceptionJob implements ComputeJob<String> {
        @Override
        public String execute(JobExecutionContext context, Object... args) {
            throw new CustomException(TRACE_ID, COLUMN_ALREADY_EXISTS_ERR, "Custom job error", null);
        }
    }

    private static class ExceptionJob implements ComputeJob<String> {
        @Override
        public String execute(JobExecutionContext context, Object... args) {
            throw new ArithmeticException("math err");
        }
    }

    private static class EchoJob implements ComputeJob<Object> {
        @Override
        public Object execute(JobExecutionContext context, Object... args) {
            var value = args[0];

            if (!(value instanceof byte[])) {
                var expectedString = (String) args[1];
                var valueString = value == null ? "null" : value.toString();
                assertEquals(expectedString, valueString, "Unexpected string representation of value");
            }

            return args[0];
        }
    }

    private static class SleepJob implements ComputeJob<Void> {
        @Override
        public Void execute(JobExecutionContext context, Object... args) {
            try {
                Thread.sleep((Integer) args[0]);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }

            return null;
        }
    }

    private static class DecimalJob implements ComputeJob<BigDecimal> {
        @Override
        public BigDecimal execute(JobExecutionContext context, Object... args) {
            return new BigDecimal((String) args[0]).setScale((Integer) args[1], RoundingMode.HALF_UP);
        }
    }

    private static class MapReduceNodeNameTask implements MapReduceTask<String> {
        @Override
        public List<ComputeJobRunner> split(TaskExecutionContext context, Object... args) {
            return context.ignite().clusterNodes().stream()
                    .map(node -> ComputeJobRunner.builder()
                            .jobClassName(NodeNameJob.class.getName())
                            .nodes(Set.of(node))
                            .args(args)
                            .build())
                    .collect(Collectors.toList());
        }

        @Override
        public String reduce(Map<UUID, ?> results) {
            return results.values().stream()
                    .map(String.class::cast)
                    .collect(Collectors.joining(","));
        }
    }

    private static class MapReduceArgsTask implements MapReduceTask<String> {
        @Override
        public List<ComputeJobRunner> split(TaskExecutionContext context, Object... args) {
            return context.ignite().clusterNodes().stream()
                    .map(node -> ComputeJobRunner.builder()
                            .jobClassName(ConcatJob.class.getName())
                            .nodes(Set.of(node))
                            .args(args)
                            .build())
                    .collect(Collectors.toList());
        }

        @Override
        public String reduce(Map<UUID, ?> results) {
            return results.values().stream()
                    .map(String.class::cast)
                    .collect(Collectors.joining(","));
        }
    }

    private static class MapReduceExceptionOnSplitTask implements MapReduceTask<String> {
        @Override
        public List<ComputeJobRunner> split(TaskExecutionContext context, Object... args) {
            throw new CustomException(TRACE_ID, COLUMN_ALREADY_EXISTS_ERR, "Custom job error", null);
        }

        @Override
        public String reduce(Map<UUID, ?> results) {
            return "expected split exception";
        }
    }

    private static class MapReduceExceptionOnReduceTask implements MapReduceTask<String> {

        @Override
        public List<ComputeJobRunner> split(TaskExecutionContext context, Object... args) {
            return context.ignite().clusterNodes().stream()
                    .map(node -> ComputeJobRunner.builder()
                            .jobClassName(NodeNameJob.class.getName())
                            .nodes(Set.of(node))
                            .args(args)
                            .build())
                    .collect(Collectors.toList());
        }

        @Override
        public String reduce(Map<UUID, ?> results) {
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
