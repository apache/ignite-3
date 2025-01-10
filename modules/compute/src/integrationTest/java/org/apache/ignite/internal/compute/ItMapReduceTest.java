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
import static org.apache.ignite.compute.TaskStatus.CANCELED;
import static org.apache.ignite.compute.TaskStatus.COMPLETED;
import static org.apache.ignite.compute.TaskStatus.EXECUTING;
import static org.apache.ignite.compute.TaskStatus.FAILED;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureExceptionMatcher.willThrow;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willBe;
import static org.apache.ignite.internal.testframework.matchers.JobStateMatcher.jobStateWithStatus;
import static org.apache.ignite.internal.testframework.matchers.TaskStateMatcher.taskStateWithStatusAndCreateTimeStartTimeFinishTime;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

import java.time.Instant;
import java.util.List;
import org.apache.ignite.Ignite;
import org.apache.ignite.compute.IgniteCompute;
import org.apache.ignite.compute.JobStatus;
import org.apache.ignite.compute.TaskDescriptor;
import org.apache.ignite.compute.TaskState;
import org.apache.ignite.compute.TaskStatus;
import org.apache.ignite.compute.task.TaskExecution;
import org.apache.ignite.internal.ClusterPerClassIntegrationTest;
import org.apache.ignite.internal.compute.utils.InteractiveJobs;
import org.apache.ignite.internal.compute.utils.InteractiveTasks;
import org.apache.ignite.internal.compute.utils.TestingJobExecution;
import org.apache.ignite.lang.IgniteException;
import org.hamcrest.Matcher;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

@SuppressWarnings("resource")
class ItMapReduceTest extends ClusterPerClassIntegrationTest {
    @BeforeEach
    void initChannels() {
        InteractiveJobs.clearState();
        InteractiveTasks.clearState();

        List<String> allNodeNames = CLUSTER.runningNodes().map(Ignite::name).collect(toList());
        InteractiveJobs.initChannels(allNodeNames);
    }

    @Test
    void taskMaintainsState() throws Exception {
        Ignite entryNode = CLUSTER.node(0);

        // Given running task.
        IgniteCompute igniteCompute = entryNode.compute();
        TaskExecution<List<String>> taskExecution = igniteCompute.submitMapReduce(
                TaskDescriptor.<Object, List<String>>builder(InteractiveTasks.GlobalApi.name()).build(), null);
        TestingJobExecution<List<String>> testExecution = new TestingJobExecution<>(new TaskToJobExecutionWrapper<>(taskExecution));
        testExecution.assertExecuting();
        InteractiveTasks.GlobalApi.assertAlive();

        // Save state before split.
        TaskState stateBeforeSplit = taskExecution.stateAsync().join();

        // And states list future is not complete yet.
        assertThat(taskExecution.statesAsync().isDone(), is(false));

        // When finish the split job.
        InteractiveTasks.GlobalApi.finishSplit();

        // Then the task is still executing while waiting for the jobs to finish.
        testExecution.assertExecuting();
        assertTaskStateIs(taskExecution, EXECUTING, stateBeforeSplit, nullValue(Instant.class));

        // And states list contains states for 3 running nodes.
        assertJobStates(taskExecution, JobStatus.EXECUTING);

        // When finish the jobs.
        InteractiveJobs.all().finishReturnWorkerNames();

        // Then the task is still executing while waiting for the reduce to finish.
        testExecution.assertExecuting();
        assertTaskStateIs(taskExecution, EXECUTING, stateBeforeSplit, nullValue(Instant.class));

        // When finish the reduce job.
        InteractiveTasks.GlobalApi.finishReduce();

        // Then the task is complete and the result is the list of all node names.
        String[] allNodeNames = CLUSTER.runningNodes().map(Ignite::name).toArray(String[]::new);
        assertThat(taskExecution.resultAsync(), willBe(containsInAnyOrder(allNodeNames)));

        // And task state is completed.
        assertTaskStateIs(taskExecution, COMPLETED, stateBeforeSplit, notNullValue(Instant.class));

        // And states list contains states for 3 completed jobs.
        assertJobStates(taskExecution, JobStatus.COMPLETED);
    }

    @Test
    void splitThrowsException() throws Exception {
        Ignite entryNode = CLUSTER.node(0);

        // Given running task.
        TaskExecution<List<String>> taskExecution = startTask(entryNode, null);

        // Save state before split.
        TaskState stateBeforeSplit = taskExecution.stateAsync().join();

        // When the split job throws an exception.
        InteractiveTasks.GlobalApi.throwException();

        // Then the task fails.
        assertTaskFailed(taskExecution, FAILED, stateBeforeSplit);

        // And states list fails.
        assertThat(taskExecution.statesAsync(), willThrow(IgniteException.class));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void cancelSplit(boolean cooperativeCancel) throws Exception {
        Ignite entryNode = CLUSTER.node(0);

        // Given running task.
        TaskExecution<List<String>> taskExecution = startTask(entryNode, cooperativeCancel ? "NO_INTERRUPT" : "");

        // Save state before split.
        TaskState stateBeforeSplit = taskExecution.stateAsync().join();

        // When cancel the task.
        assertThat(taskExecution.cancelAsync(), willBe(true));

        // Then the task is cancelled.
        assertTaskFailed(taskExecution, CANCELED, stateBeforeSplit);

        // And states list will fail.
        assertThat(taskExecution.statesAsync(), willThrow(RuntimeException.class));

        // And second cancel will fail.
        assertThat(taskExecution.cancelAsync(), willBe(false));
    }

    @Test
    void jobThrowsException() throws Exception {
        Ignite entryNode = CLUSTER.node(0);

        // Given running task.
        TaskExecution<List<String>> taskExecution = startTask(entryNode, null);

        // Save state before split.
        TaskState stateBeforeSplit = taskExecution.stateAsync().join();

        // And finish the split job.
        finishSplit(taskExecution);

        // When jobs throw an exception.
        InteractiveJobs.all().throwException();

        // Then the task fails.
        assertTaskFailed(taskExecution, FAILED, stateBeforeSplit);
    }

    @Test
    void cancelJobs() throws Exception {
        Ignite entryNode = CLUSTER.node(0);

        // Given running task.
        IgniteCompute igniteCompute = entryNode.compute();
        TaskExecution<List<String>> taskExecution = igniteCompute.submitMapReduce(
                TaskDescriptor.<Object, List<String>>builder(InteractiveTasks.GlobalApi.name()).build(), null);
        TestingJobExecution<List<String>> testExecution = new TestingJobExecution<>(new TaskToJobExecutionWrapper<>(taskExecution));
        testExecution.assertExecuting();
        InteractiveTasks.GlobalApi.assertAlive();

        // Save state before split.
        TaskState stateBeforeSplit = taskExecution.stateAsync().join();

        // And finish the split job.
        finishSplit(taskExecution);

        // When cancel the task.
        assertThat(taskExecution.cancelAsync(), willBe(true));

        // Then the task is cancelled.
        assertTaskFailed(taskExecution, FAILED, stateBeforeSplit);

        // And states list contains canceled states.
        assertJobStates(taskExecution, JobStatus.CANCELED);

        // And second cancel will fail.
        assertThat(taskExecution.cancelAsync(), willBe(false));
    }

    @Test
    void reduceThrowsException() throws Exception {
        Ignite entryNode = CLUSTER.node(0);

        // Given running task.
        TaskExecution<List<String>> taskExecution = startTask(entryNode, null);

        // Save state before split.
        TaskState stateBeforeSplit = taskExecution.stateAsync().join();

        // And finish the split job.
        finishSplit(taskExecution);

        // And finish jobs.
        InteractiveJobs.all().finishReturnWorkerNames();

        // And reduce throws an exception.
        InteractiveTasks.GlobalApi.throwException();

        // Then the task fails.
        assertTaskFailed(taskExecution, FAILED, stateBeforeSplit);

        // And states list contains completed states.
        assertJobStates(taskExecution, JobStatus.COMPLETED);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void cancelReduce(boolean cooperativeCancel) throws Exception {
        Ignite entryNode = CLUSTER.node(0);

        // Given running task.
        String arg = cooperativeCancel ? "NO_INTERRUPT" : null;
        IgniteCompute igniteCompute = entryNode.compute();
        TaskExecution<List<String>> taskExecution = igniteCompute.submitMapReduce(
                TaskDescriptor.<String, List<String>>builder(InteractiveTasks.GlobalApi.name()).build(), arg);
        TestingJobExecution<List<String>> testExecution = new TestingJobExecution<>(new TaskToJobExecutionWrapper<>(taskExecution));
        testExecution.assertExecuting();
        InteractiveTasks.GlobalApi.assertAlive();

        // Save state before split.
        TaskState stateBeforeSplit = taskExecution.stateAsync().join();

        // And finish the split job.
        finishSplit(taskExecution);

        // And finish jobs.
        InteractiveJobs.all().finishReturnWorkerNames();

        // Wait for the reduce job to start.
        InteractiveTasks.GlobalApi.assertAlive();

        // When cancel the task.
        assertThat(taskExecution.cancelAsync(), willBe(true));

        // Then the task is cancelled.
        assertTaskFailed(taskExecution, CANCELED, stateBeforeSplit);

        // And states list contains completed states.
        assertJobStates(taskExecution, JobStatus.COMPLETED);

        // And second cancel will fail.
        assertThat(taskExecution.cancelAsync(), willBe(false));
    }

    private static TaskExecution<List<String>> startTask(Ignite entryNode, String args) throws InterruptedException {
        IgniteCompute igniteCompute = entryNode.compute();
        TaskExecution<List<String>> taskExecution = igniteCompute.submitMapReduce(
                TaskDescriptor.<String, List<String>>builder(InteractiveTasks.GlobalApi.name()).build(), args);
        new TestingJobExecution<>(new TaskToJobExecutionWrapper<>(taskExecution)).assertExecuting();
        InteractiveTasks.GlobalApi.assertAlive();
        return taskExecution;
    }

    private static void finishSplit(TaskExecution<List<String>> taskExecution) {
        // Finish the split job.
        InteractiveTasks.GlobalApi.finishSplit();

        // And wait for states list contains states for 3 running nodes.
        assertJobStates(taskExecution, JobStatus.EXECUTING);
    }

    private static void assertTaskFailed(TaskExecution<List<String>> taskExecution, TaskStatus status, TaskState stateBeforeSplit) {
        assertThat(taskExecution.resultAsync(), willThrow(IgniteException.class));
        assertTaskStateIs(taskExecution, status, stateBeforeSplit, notNullValue(Instant.class));
    }

    private static void assertTaskStateIs(
            TaskExecution<List<String>> taskExecution,
            TaskStatus status,
            TaskState stateBeforeSplit,
            Matcher<Instant> finishTimeMatcher
    ) {
        assertThat(taskExecution.stateAsync(), willBe(taskStateWithStatusAndCreateTimeStartTimeFinishTime(
                is(status),
                is(stateBeforeSplit.createTime()),
                is(stateBeforeSplit.startTime()),
                is(finishTimeMatcher)
        )));
        assertThat(taskExecution.idAsync(), willBe(stateBeforeSplit.id()));
    }

    private static void assertJobStates(TaskExecution<List<String>> taskExecution, JobStatus status) {
        await().until(taskExecution::statesAsync, willBe(contains(
                jobStateWithStatus(status),
                jobStateWithStatus(status),
                jobStateWithStatus(status)
        )));
    }
}
