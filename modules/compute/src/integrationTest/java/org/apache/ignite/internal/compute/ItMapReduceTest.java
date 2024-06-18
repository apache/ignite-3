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
import static org.apache.ignite.compute.JobState.CANCELED;
import static org.apache.ignite.compute.JobState.COMPLETED;
import static org.apache.ignite.compute.JobState.EXECUTING;
import static org.apache.ignite.compute.JobState.FAILED;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureExceptionMatcher.willThrow;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willBe;
import static org.apache.ignite.internal.testframework.matchers.JobStatusMatcher.jobStatusWithState;
import static org.apache.ignite.internal.testframework.matchers.JobStatusMatcher.jobStatusWithStateAndCreateTimeStartTimeFinishTime;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

import java.time.Instant;
import java.util.List;
import org.apache.ignite.compute.JobState;
import org.apache.ignite.compute.JobStatus;
import org.apache.ignite.compute.task.TaskExecution;
import org.apache.ignite.internal.ClusterPerClassIntegrationTest;
import org.apache.ignite.internal.app.IgniteImpl;
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

        List<String> allNodeNames = CLUSTER.runningNodes().map(IgniteImpl::name).collect(toList());
        InteractiveJobs.initChannels(allNodeNames);
    }

    @Test
    void taskMaintainsStatus() throws Exception {
        IgniteImpl entryNode = CLUSTER.node(0);

        // Given running task.
        TaskExecution<List<String>> taskExecution = entryNode.compute().submitMapReduce(List.of(), InteractiveTasks.GlobalApi.name(), new Object[]{});
        TestingJobExecution<List<String>> testExecution = new TestingJobExecution<>(taskExecution);
        testExecution.assertExecuting();
        InteractiveTasks.GlobalApi.assertAlive();

        // Save status before split.
        JobStatus statusBeforeSplit = taskExecution.statusAsync().join();

        // And statuses list future is not complete yet.
        assertThat(taskExecution.statusesAsync().isDone(), is(false));

        // When finish the split job.
        InteractiveTasks.GlobalApi.finishSplit();

        // Then the task is still executing while waiting for the jobs to finish.
        testExecution.assertExecuting();
        assertTaskStatusIs(taskExecution, EXECUTING, statusBeforeSplit, nullValue(Instant.class));

        // And statuses list contains statuses for 3 running nodes.
        assertJobStates(taskExecution, EXECUTING);

        // When finish the jobs.
        InteractiveJobs.all().finishReturnWorkerNames();

        // Then the task is still executing while waiting for the reduce to finish.
        testExecution.assertExecuting();
        assertTaskStatusIs(taskExecution, EXECUTING, statusBeforeSplit, nullValue(Instant.class));

        // When finish the reduce job.
        InteractiveTasks.GlobalApi.finishReduce();

        // Then the task is complete and the result is the list of all node names.
        String[] allNodeNames = CLUSTER.runningNodes().map(IgniteImpl::name).toArray(String[]::new);
        assertThat(taskExecution.resultAsync(), willBe(containsInAnyOrder(allNodeNames)));

        // And task status is completed.
        assertTaskStatusIs(taskExecution, COMPLETED, statusBeforeSplit, notNullValue(Instant.class));

        // And statuses list contains statuses for 3 completed jobs.
        assertJobStates(taskExecution, COMPLETED);
    }

    @Test
    void splitThrowsException() throws Exception {
        IgniteImpl entryNode = CLUSTER.node(0);

        // Given running task.
        TaskExecution<List<String>> taskExecution = startTask(entryNode);

        // Save status before split.
        JobStatus statusBeforeSplit = taskExecution.statusAsync().join();

        // When the split job throws an exception.
        InteractiveTasks.GlobalApi.throwException();

        // Then the task fails.
        assertTaskFailed(taskExecution, FAILED, statusBeforeSplit);

        // And statuses list fails.
        assertThat(taskExecution.statusesAsync(), willThrow(IgniteException.class));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void cancelSplit(boolean cooperativeCancel) throws Exception {
        IgniteImpl entryNode = CLUSTER.node(0);

        // Given running task.
        TaskExecution<List<String>> taskExecution = startTask(entryNode, cooperativeCancel ? "NO_INTERRUPT" : "");

        // Save status before split.
        JobStatus statusBeforeSplit = taskExecution.statusAsync().join();

        // When cancel the task.
        assertThat(taskExecution.cancelAsync(), willBe(true));

        // Then the task is cancelled.
        assertTaskFailed(taskExecution, CANCELED, statusBeforeSplit);

        // And statuses list will fail.
        assertThat(taskExecution.statusesAsync(), willThrow(RuntimeException.class));

        // And second cancel will fail.
        assertThat(taskExecution.cancelAsync(), willBe(false));
    }

    @Test
    void jobThrowsException() throws Exception {
        IgniteImpl entryNode = CLUSTER.node(0);

        // Given running task.
        TaskExecution<List<String>> taskExecution = startTask(entryNode);

        // Save status before split.
        JobStatus statusBeforeSplit = taskExecution.statusAsync().join();

        // And finish the split job.
        finishSplit(taskExecution);

        // When jobs throw an exception.
        InteractiveJobs.all().throwException();

        // Then the task fails.
        assertTaskFailed(taskExecution, FAILED, statusBeforeSplit);
    }

    @Test
    void cancelJobs() throws Exception {
        IgniteImpl entryNode = CLUSTER.node(0);

        // Given running task.
        TaskExecution<List<String>> taskExecution = entryNode.compute().submitMapReduce(List.of(), InteractiveTasks.GlobalApi.name(), new Object[]{});
        TestingJobExecution<List<String>> testExecution = new TestingJobExecution<>(taskExecution);
        testExecution.assertExecuting();
        InteractiveTasks.GlobalApi.assertAlive();

        // Save status before split.
        JobStatus statusBeforeSplit = taskExecution.statusAsync().join();

        // And finish the split job.
        finishSplit(taskExecution);

        // When cancel the task.
        assertThat(taskExecution.cancelAsync(), willBe(true));

        // Then the task is cancelled.
        assertTaskFailed(taskExecution, FAILED, statusBeforeSplit);

        // And statuses list contains canceled statuses.
        assertJobStates(taskExecution, CANCELED);

        // And second cancel will fail.
        assertThat(taskExecution.cancelAsync(), willBe(false));
    }

    @Test
    void reduceThrowsException() throws Exception {
        IgniteImpl entryNode = CLUSTER.node(0);

        // Given running task.
        TaskExecution<List<String>> taskExecution = startTask(entryNode);

        // Save status before split.
        JobStatus statusBeforeSplit = taskExecution.statusAsync().join();

        // And finish the split job.
        finishSplit(taskExecution);

        // And finish jobs.
        InteractiveJobs.all().finishReturnWorkerNames();

        // And reduce throws an exception.
        InteractiveTasks.GlobalApi.throwException();

        // Then the task fails.
        assertTaskFailed(taskExecution, FAILED, statusBeforeSplit);

        // And statuses list contains completed statuses.
        assertJobStates(taskExecution, COMPLETED);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void cancelReduce(boolean cooperativeCancel) throws Exception {
        IgniteImpl entryNode = CLUSTER.node(0);

        // Given running task.
        String arg = cooperativeCancel ? "NO_INTERRUPT" : "";
        TaskExecution<List<String>> taskExecution = entryNode.compute().submitMapReduce(List.of(), InteractiveTasks.GlobalApi.name(), new Object[] {arg});
        TestingJobExecution<List<String>> testExecution = new TestingJobExecution<>(taskExecution);
        testExecution.assertExecuting();
        InteractiveTasks.GlobalApi.assertAlive();

        // Save status before split.
        JobStatus statusBeforeSplit = taskExecution.statusAsync().join();

        // And finish the split job.
        finishSplit(taskExecution);

        // And finish jobs.
        InteractiveJobs.all().finishReturnWorkerNames();

        // Wait for the reduce job to start.
        InteractiveTasks.GlobalApi.assertAlive();

        // When cancel the task.
        assertThat(taskExecution.cancelAsync(), willBe(true));

        // Then the task is cancelled.
        assertTaskFailed(taskExecution, CANCELED, statusBeforeSplit);

        // And statuses list contains completed statuses.
        assertJobStates(taskExecution, COMPLETED);

        // And second cancel will fail.
        assertThat(taskExecution.cancelAsync(), willBe(false));
    }

    private static TaskExecution<List<String>> startTask(IgniteImpl entryNode, Object... args) throws InterruptedException {
        TaskExecution<List<String>> taskExecution = entryNode.compute().submitMapReduce(List.of(), InteractiveTasks.GlobalApi.name(), args);
        new TestingJobExecution<>(taskExecution).assertExecuting();
        InteractiveTasks.GlobalApi.assertAlive();
        return taskExecution;
    }

    private static void finishSplit(TaskExecution<List<String>> taskExecution) {
        // Finish the split job.
        InteractiveTasks.GlobalApi.finishSplit();

        // And wait for statuses list contains statuses for 3 running nodes.
        assertJobStates(taskExecution, EXECUTING);
    }

    private static void assertTaskFailed(TaskExecution<List<String>> taskExecution, JobState jobState, JobStatus statusBeforeSplit) {
        assertThat(taskExecution.resultAsync(), willThrow(IgniteException.class));
        assertTaskStatusIs(taskExecution, jobState, statusBeforeSplit, notNullValue(Instant.class));
    }

    private static void assertTaskStatusIs(
            TaskExecution<List<String>> taskExecution,
            JobState jobState,
            JobStatus statusBeforeSplit,
            Matcher<Instant> finishTimeMatcher
    ) {
        assertThat(taskExecution.statusAsync(), willBe(jobStatusWithStateAndCreateTimeStartTimeFinishTime(
                is(jobState),
                is(statusBeforeSplit.createTime()),
                is(statusBeforeSplit.startTime()),
                is(finishTimeMatcher)
        )));
        assertThat(taskExecution.idAsync(), willBe(statusBeforeSplit.id()));
    }

    private static void assertJobStates(TaskExecution<List<String>> taskExecution, JobState state) {
        await().until(taskExecution::statusesAsync, willBe(contains(
                jobStatusWithState(state),
                jobStatusWithState(state),
                jobStatusWithState(state)
        )));
    }
}
