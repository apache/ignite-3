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

package org.apache.ignite.internal.compute.events;

import static org.apache.ignite.compute.JobStatus.CANCELED;
import static org.apache.ignite.compute.JobStatus.EXECUTING;
import static org.apache.ignite.internal.TestWrappers.unwrapIgniteImpl;
import static org.apache.ignite.internal.compute.events.ComputeEventMetadata.Type.BROADCAST;
import static org.apache.ignite.internal.compute.events.ComputeEventMetadata.Type.MAP_REDUCE;
import static org.apache.ignite.internal.compute.events.ComputeEventMetadata.Type.SINGLE;
import static org.apache.ignite.internal.eventlog.api.IgniteEventType.COMPUTE_JOB_CANCELED;
import static org.apache.ignite.internal.eventlog.api.IgniteEventType.COMPUTE_JOB_CANCELING;
import static org.apache.ignite.internal.eventlog.api.IgniteEventType.COMPUTE_JOB_COMPLETED;
import static org.apache.ignite.internal.eventlog.api.IgniteEventType.COMPUTE_JOB_EXECUTING;
import static org.apache.ignite.internal.eventlog.api.IgniteEventType.COMPUTE_JOB_FAILED;
import static org.apache.ignite.internal.eventlog.api.IgniteEventType.COMPUTE_JOB_QUEUED;
import static org.apache.ignite.internal.eventlog.api.IgniteEventType.COMPUTE_TASK_COMPLETED;
import static org.apache.ignite.internal.eventlog.api.IgniteEventType.COMPUTE_TASK_EXECUTING;
import static org.apache.ignite.internal.eventlog.api.IgniteEventType.COMPUTE_TASK_FAILED;
import static org.apache.ignite.internal.eventlog.api.IgniteEventType.COMPUTE_TASK_QUEUED;
import static org.apache.ignite.internal.eventlog.api.IgniteEventType.values;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureExceptionMatcher.willThrow;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willBe;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.testframework.matchers.JobStateMatcher.jobStateWithStatus;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInRelativeOrder;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.in;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import org.apache.ignite.Ignite;
import org.apache.ignite.InitParametersBuilder;
import org.apache.ignite.compute.BroadcastExecution;
import org.apache.ignite.compute.BroadcastJobTarget;
import org.apache.ignite.compute.ComputeException;
import org.apache.ignite.compute.IgniteCompute;
import org.apache.ignite.compute.JobDescriptor;
import org.apache.ignite.compute.JobExecution;
import org.apache.ignite.compute.JobTarget;
import org.apache.ignite.compute.TaskDescriptor;
import org.apache.ignite.compute.task.MapReduceTask;
import org.apache.ignite.compute.task.TaskExecution;
import org.apache.ignite.internal.ClusterPerClassIntegrationTest;
import org.apache.ignite.internal.ConfigOverride;
import org.apache.ignite.internal.catalog.descriptors.CatalogZoneDescriptor;
import org.apache.ignite.internal.compute.FailingJob;
import org.apache.ignite.internal.compute.FailingJobMapReduceTask;
import org.apache.ignite.internal.compute.FailingReduceMapReduceTask;
import org.apache.ignite.internal.compute.FailingSplitMapReduceTask;
import org.apache.ignite.internal.compute.GetNodeNameJob;
import org.apache.ignite.internal.compute.MapReduce;
import org.apache.ignite.internal.compute.SilentSleepJob;
import org.apache.ignite.internal.compute.events.ComputeEventMetadata.Type;
import org.apache.ignite.internal.compute.events.EventMatcher.Event;
import org.apache.ignite.internal.compute.utils.InteractiveJobs;
import org.apache.ignite.internal.compute.utils.InteractiveTasks;
import org.apache.ignite.internal.eventlog.api.IgniteEventType;
import org.apache.ignite.internal.testframework.log4j2.EventLogInspector;
import org.apache.ignite.lang.CancelHandle;
import org.apache.ignite.lang.CancellationToken;
import org.apache.ignite.table.QualifiedName;
import org.apache.ignite.table.Tuple;
import org.apache.ignite.table.mapper.Mapper;
import org.hamcrest.Matcher;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

@ConfigOverride(name = "ignite.compute.threadPoolSize", value = "1")
abstract class ItComputeEventsTest extends ClusterPerClassIntegrationTest {
    final EventLogInspector logInspector = new EventLogInspector();

    @BeforeEach
    void setUp() {
        InteractiveJobs.clearState();
        InteractiveTasks.clearState();
        InteractiveJobs.initChannels(CLUSTER.runningNodes().map(Ignite::name).collect(Collectors.toList()));

        logInspector.start();
    }

    @AfterEach
    void tearDown() {
        logInspector.stop();
        dropAllTables();
    }

    @Override
    protected void configureInitParameters(InitParametersBuilder builder) {
        String allEvents = Arrays.stream(values())
                .map(IgniteEventType::name)
                .filter(name -> name.startsWith("COMPUTE"))
                .collect(Collectors.joining(", ", "[", "]"));

        builder.clusterConfiguration("ignite.eventlog {"
                + " sinks.logSink.channel: testChannel,"
                + " channels.testChannel.events: " + allEvents
                + "}");
    }

    protected abstract IgniteCompute compute();

    @ParameterizedTest
    @ValueSource(ints = {0, 1})
    void singleJob(int targetNodeIndex) {
        JobDescriptor<Void, String> jobDescriptor = JobDescriptor.builder(GetNodeNameJob.class).build();
        JobExecution<String> execution = submit(JobTarget.node(clusterNode(targetNodeIndex)), jobDescriptor, null);

        assertThat(execution.resultAsync(), willCompleteSuccessfully());

        UUID jobId = execution.idAsync().join(); // Safe to join since execution is complete.
        String jobClassName = jobDescriptor.jobClassName();
        String targetNode = node(targetNodeIndex).name();

        assertEvents(
                jobEvent(COMPUTE_JOB_QUEUED, jobId, jobClassName, targetNode),
                jobEvent(COMPUTE_JOB_EXECUTING, jobId, jobClassName, targetNode),
                jobEvent(COMPUTE_JOB_COMPLETED, jobId, jobClassName, targetNode)
        );
    }

    @Test
    void broadcast() throws JsonProcessingException {
        JobDescriptor<Void, String> jobDescriptor = JobDescriptor.builder(GetNodeNameJob.class).build();
        BroadcastJobTarget target = BroadcastJobTarget.nodes(clusterNode(0), clusterNode(1), clusterNode(2));
        BroadcastExecution<String> broadcastExecution = submit(target, jobDescriptor, null);

        assertThat(broadcastExecution.resultsAsync(), willCompleteSuccessfully());

        await().until(logInspector::events, hasSize(9));

        // Now get the task ID from the first event and assert that all events have the same task ID.
        ObjectMapper mapper = new ObjectMapper();
        Event firstEvent = mapper.readValue(logInspector.events().get(0), Event.class);
        UUID taskId = firstEvent.fields.taskId;
        assertThat(taskId, notNullValue());

        String jobClassName = jobDescriptor.jobClassName();
        broadcastExecution.executions().forEach(execution -> {
            UUID jobId = execution.idAsync().join(); // Safe to join since execution is complete.
            String targetNode = execution.node().name();
            assertThat(logInspector.events(), containsInRelativeOrder(
                    broadcastJobEvent(COMPUTE_JOB_QUEUED, jobId, jobClassName, targetNode, taskId),
                    broadcastJobEvent(COMPUTE_JOB_EXECUTING, jobId, jobClassName, targetNode, taskId),
                    broadcastJobEvent(COMPUTE_JOB_COMPLETED, jobId, jobClassName, targetNode, taskId)
            ));
        });
    }

    @Test
    void partitionedBroadcast() throws JsonProcessingException {
        createTestTableWithOneRow();

        JobDescriptor<Void, String> jobDescriptor = JobDescriptor.builder(GetNodeNameJob.class).build();
        BroadcastExecution<String> broadcastExecution = submit(BroadcastJobTarget.table("test"), jobDescriptor, null);

        assertThat(broadcastExecution.resultsAsync(), willCompleteSuccessfully());

        CatalogZoneDescriptor defaultZoneDesc = unwrapIgniteImpl(CLUSTER.aliveNode()).catalogManager().latestCatalog().defaultZone();

        assertNotNull(defaultZoneDesc);

        int defaultPartitionCount = defaultZoneDesc.partitions();
        assertThat(broadcastExecution.executions(), hasSize(defaultPartitionCount));
        await().until(logInspector::events, hasSize(defaultPartitionCount * 3));

        // Now get the task ID from the first event and assert that all events have the same task ID.
        ObjectMapper mapper = new ObjectMapper();
        Event firstEvent = mapper.readValue(logInspector.events().get(0), Event.class);
        UUID taskId = firstEvent.fields.taskId;
        assertThat(taskId, notNullValue());

        String jobClassName = jobDescriptor.jobClassName();
        String tableName = QualifiedName.parse("test").toCanonicalForm();
        broadcastExecution.executions().forEach(execution -> {
            UUID jobId = execution.idAsync().join(); // Safe to join since execution is complete.
            String targetNode = execution.node().name();
            assertThat(logInspector.events(), containsInRelativeOrder(
                    broadcastJobEvent(COMPUTE_JOB_QUEUED, jobId, jobClassName, targetNode, taskId).withTableName(tableName),
                    broadcastJobEvent(COMPUTE_JOB_EXECUTING, jobId, jobClassName, targetNode, taskId).withTableName(tableName),
                    broadcastJobEvent(COMPUTE_JOB_COMPLETED, jobId, jobClassName, targetNode, taskId).withTableName(tableName)
            ));
        });
    }

    @ParameterizedTest
    @ValueSource(ints = {0, 1})
    void failingJob(int targetNodeIndex) {
        JobDescriptor<Void, String> jobDescriptor = JobDescriptor.builder(FailingJob.class).build();
        JobExecution<String> execution = submit(JobTarget.node(clusterNode(targetNodeIndex)), jobDescriptor, null);

        assertThat(execution.resultAsync(), willThrow(ComputeException.class));

        UUID jobId = execution.idAsync().join(); // Safe to join since execution is complete.
        String jobClassName = jobDescriptor.jobClassName();
        String targetNode = node(targetNodeIndex).name();

        assertEvents(
                jobEvent(COMPUTE_JOB_QUEUED, jobId, jobClassName, targetNode),
                jobEvent(COMPUTE_JOB_EXECUTING, jobId, jobClassName, targetNode),
                jobEvent(COMPUTE_JOB_FAILED, jobId, jobClassName, targetNode)
        );
    }

    @ParameterizedTest
    @ValueSource(ints = {0, 1})
    void cancelJob(int targetNodeIndex) {
        CancelHandle cancelHandle = CancelHandle.create();

        JobDescriptor<Long, Void> jobDescriptor = JobDescriptor.builder(SilentSleepJob.class).build();
        JobTarget target = JobTarget.node(clusterNode(targetNodeIndex));
        JobExecution<Void> execution = submit(target, jobDescriptor, cancelHandle.token(), Long.MAX_VALUE);

        // Wait for start executing
        await().until(execution::stateAsync, willBe(jobStateWithStatus(EXECUTING)));

        cancelHandle.cancel();

        assertThat(execution.resultAsync(), willThrow(ComputeException.class));

        UUID jobId = execution.idAsync().join(); // Safe to join since execution is complete.
        String jobClassName = jobDescriptor.jobClassName();
        String targetNode = node(targetNodeIndex).name();

        assertEvents(
                jobEvent(COMPUTE_JOB_QUEUED, jobId, jobClassName, targetNode),
                jobEvent(COMPUTE_JOB_EXECUTING, jobId, jobClassName, targetNode),
                jobEvent(COMPUTE_JOB_CANCELING, jobId, jobClassName, targetNode),
                jobEvent(COMPUTE_JOB_CANCELED, jobId, jobClassName, targetNode)
        );
    }

    @ParameterizedTest
    @ValueSource(ints = {0, 1})
    void cancelQueuedJob(int targetNodeIndex) {
        // Start first job
        CancelHandle cancelHandle1 = CancelHandle.create();

        JobDescriptor<Long, Void> jobDescriptor = JobDescriptor.builder(SilentSleepJob.class).build();
        JobTarget target = JobTarget.node(clusterNode(targetNodeIndex));
        JobExecution<Void> execution1 = submit(target, jobDescriptor, cancelHandle1.token(), Long.MAX_VALUE);
        // Wait for it to start executing
        await().until(execution1::stateAsync, willBe(jobStateWithStatus(EXECUTING)));

        // Start second job, it will be queued due to the queue size limit
        UUID jobId1 = execution1.idAsync().join();
        String targetNode = node(targetNodeIndex).name();

        CancelHandle cancelHandle2 = CancelHandle.create();

        JobExecution<Void> execution2 = submit(target, jobDescriptor, cancelHandle2.token(), Long.MAX_VALUE);
        UUID jobId2 = execution2.idAsync().join();

        // Cancel queued job
        cancelHandle2.cancel();
        await().until(execution2::stateAsync, willBe(jobStateWithStatus(CANCELED)));

        // Cancel executing job
        cancelHandle1.cancel();

        String jobClassName = jobDescriptor.jobClassName();

        // First job should transition queued->executing->canceling->canceled
        // Second one should transition queued->canceled
        assertEvents(
                jobEvent(COMPUTE_JOB_QUEUED, jobId1, jobClassName, targetNode),
                jobEvent(COMPUTE_JOB_EXECUTING, jobId1, jobClassName, targetNode),
                jobEvent(COMPUTE_JOB_QUEUED, jobId2, jobClassName, targetNode),
                jobEvent(COMPUTE_JOB_CANCELED, jobId2, jobClassName, targetNode),
                jobEvent(COMPUTE_JOB_CANCELING, jobId1, jobClassName, targetNode),
                jobEvent(COMPUTE_JOB_CANCELED, jobId1, jobClassName, targetNode)
        );
    }

    @Test
    void executesColocatedWithMappedKey() {
        createTestTableWithOneRow();

        JobTarget jobTarget = JobTarget.colocated("test", 1, Mapper.of(Integer.class));
        JobDescriptor<Void, String> jobDescriptor = JobDescriptor.builder(GetNodeNameJob.class).build();
        JobExecution<String> execution = submit(jobTarget, jobDescriptor, null);

        assertThat(execution.resultAsync(), willCompleteSuccessfully());

        UUID jobId = execution.idAsync().join(); // Safe to join since execution is complete.
        String jobClassName = jobDescriptor.jobClassName();
        String targetNode = execution.node().name();

        String tableName = QualifiedName.parse("test").toCanonicalForm();
        assertEvents(
                jobEvent(COMPUTE_JOB_QUEUED, jobId, jobClassName, targetNode).withTableName(tableName),
                jobEvent(COMPUTE_JOB_EXECUTING, jobId, jobClassName, targetNode).withTableName(tableName),
                jobEvent(COMPUTE_JOB_COMPLETED, jobId, jobClassName, targetNode).withTableName(tableName)
        );
    }

    @Test
    void executesColocatedWithTupleKey() {
        createTestTableWithOneRow();

        JobTarget jobTarget = JobTarget.colocated("test", Tuple.create(Map.of("k", 1)));
        JobDescriptor<Void, String> jobDescriptor = JobDescriptor.builder(GetNodeNameJob.class).build();
        JobExecution<String> execution = submit(jobTarget, jobDescriptor, null);

        assertThat(execution.resultAsync(), willCompleteSuccessfully());

        UUID jobId = execution.idAsync().join(); // Safe to join since execution is complete.
        String jobClassName = jobDescriptor.jobClassName();
        String targetNode = execution.node().name();

        String tableName = QualifiedName.parse("test").toCanonicalForm();
        assertEvents(
                jobEvent(COMPUTE_JOB_QUEUED, jobId, jobClassName, targetNode).withTableName(tableName),
                jobEvent(COMPUTE_JOB_EXECUTING, jobId, jobClassName, targetNode).withTableName(tableName),
                jobEvent(COMPUTE_JOB_COMPLETED, jobId, jobClassName, targetNode).withTableName(tableName)
        );
    }

    @Test
    void taskCompleted() {
        TaskExecution<Integer> execution = compute().submitMapReduce(TaskDescriptor.builder(MapReduce.class).build(), null);

        assertThat(execution.resultAsync(), willCompleteSuccessfully());

        UUID taskId = execution.idAsync().join(); // Safe to join since execution is complete.
        String taskClassName = MapReduce.class.getName();

        List<UUID> jobIds = execution.idsAsync().join();
        UUID firstJobId = jobIds.get(0);
        List<String> nodeNames = CLUSTER.runningNodes().map(Ignite::name).collect(Collectors.toList());

        await().until(logInspector::events, hasSize(3 + 3 * 3)); // 3 task events and 3 job events per node
        assertThat(logInspector.events(), containsInRelativeOrder(
                taskEvent(COMPUTE_TASK_QUEUED, taskClassName, taskId),
                taskEvent(COMPUTE_TASK_EXECUTING, taskClassName, taskId),
                taskJobEvent(COMPUTE_JOB_QUEUED, firstJobId, in(nodeNames)),
                taskJobEvent(COMPUTE_JOB_EXECUTING, firstJobId, in(nodeNames)),
                taskJobEvent(COMPUTE_JOB_COMPLETED, firstJobId, in(nodeNames)),
                taskEvent(COMPUTE_TASK_COMPLETED, taskClassName, taskId)
        ));

        jobIds.forEach(jobId -> assertThat(logInspector.events(), containsInRelativeOrder(
                taskJobEvent(COMPUTE_JOB_QUEUED, jobId, in(nodeNames)),
                taskJobEvent(COMPUTE_JOB_EXECUTING, jobId, in(nodeNames)),
                taskJobEvent(COMPUTE_JOB_COMPLETED, jobId, in(nodeNames))
        )));
    }

    @ParameterizedTest
    @ValueSource(classes = {FailingSplitMapReduceTask.class, FailingJobMapReduceTask.class, FailingReduceMapReduceTask.class})
    void taskFailed(Class<? extends MapReduceTask<Void, Void, Void, Void>> mapReduceClass) {
        TaskExecution<Void> execution = compute().submitMapReduce(TaskDescriptor.builder(mapReduceClass).build(), null);

        assertThat(execution.resultAsync(), willThrow(ComputeException.class));

        UUID taskId = execution.idAsync().join(); // Safe to join since execution is complete.

        // Skip checking possible job events.
        await().until(logInspector::events, containsInRelativeOrder(
                taskEvent(COMPUTE_TASK_QUEUED, mapReduceClass.getName(), taskId),
                taskEvent(COMPUTE_TASK_EXECUTING, mapReduceClass.getName(), taskId),
                taskEvent(COMPUTE_TASK_FAILED, mapReduceClass.getName(), taskId)
        ));
    }

    private <T, R> JobExecution<R> submit(JobTarget target, JobDescriptor<T, R> descriptor, @Nullable T arg) {
        return submit(target, descriptor, null, arg);
    }

    private <T, R> BroadcastExecution<R> submit(BroadcastJobTarget target, JobDescriptor<T, R> descriptor, @Nullable T arg) {
        return submit(target, descriptor, null, arg);
    }

    private <T, R> JobExecution<R> submit(
            JobTarget target,
            JobDescriptor<T, R> descriptor,
            @Nullable CancellationToken cancellationToken,
            @Nullable T arg
    ) {
        CompletableFuture<JobExecution<R>> executionFut = compute().submitAsync(target, descriptor, arg, cancellationToken);
        assertThat(executionFut, willCompleteSuccessfully());
        return executionFut.join();
    }

    private <T, R> BroadcastExecution<R> submit(
            BroadcastJobTarget target,
            JobDescriptor<T, R> descriptor,
            @Nullable CancellationToken cancellationToken,
            @Nullable T arg
    ) {
        CompletableFuture<BroadcastExecution<R>> executionFut = compute().submitAsync(target, descriptor, arg, cancellationToken);
        assertThat(executionFut, willCompleteSuccessfully());
        return executionFut.join();
    }

    private EventMatcher broadcastJobEvent(
            IgniteEventType eventType,
            @Nullable UUID jobId,
            String jobClassName,
            String targetNode,
            UUID taskId
    ) {
        return jobEvent(eventType, BROADCAST, jobId, jobClassName, targetNode).withTaskId(taskId);
    }

    private EventMatcher jobEvent(IgniteEventType eventType, @Nullable UUID jobId, String jobClassName, String targetNode) {
        return jobEvent(eventType, SINGLE, jobId, jobClassName, targetNode);
    }

    protected abstract EventMatcher jobEvent(
            IgniteEventType eventType,
            Type jobType,
            @Nullable UUID jobId,
            String jobClassName,
            String targetNode
    );

    EventMatcher taskEvent(IgniteEventType eventType, String taskClassName, @Nullable UUID taskId) {
        return jobEvent(eventType, MAP_REDUCE, null, taskClassName, node(0).name())
                .withTaskId(taskId)
                .withInitiatorNode(nullValue());
    }

    private EventMatcher taskJobEvent(IgniteEventType eventType, @Nullable UUID jobId, Matcher<? super String> targetNodeMatcher) {
        return jobEvent(eventType, MAP_REDUCE, jobId, GetNodeNameJob.class.getName(), "")
                .withTargetNode(targetNodeMatcher);
    }

    @SafeVarargs
    final void assertEvents(Matcher<String>... matchers) {
        await().until(logInspector::events, contains(matchers));
    }

    private static void createTestTableWithOneRow() {
        sql("DROP TABLE IF EXISTS test");
        sql("CREATE TABLE test (k int, v int, CONSTRAINT PK PRIMARY KEY (k))");
        sql("INSERT INTO test(k, v) VALUES (1, 101)");
    }
}
