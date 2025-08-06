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
import static org.apache.ignite.internal.compute.events.ComputeEventMetadata.Type.SINGLE;
import static org.apache.ignite.internal.eventlog.api.IgniteEventType.COMPUTE_JOB_CANCELED;
import static org.apache.ignite.internal.eventlog.api.IgniteEventType.COMPUTE_JOB_CANCELING;
import static org.apache.ignite.internal.eventlog.api.IgniteEventType.COMPUTE_JOB_COMPLETED;
import static org.apache.ignite.internal.eventlog.api.IgniteEventType.COMPUTE_JOB_EXECUTING;
import static org.apache.ignite.internal.eventlog.api.IgniteEventType.COMPUTE_JOB_FAILED;
import static org.apache.ignite.internal.eventlog.api.IgniteEventType.COMPUTE_JOB_QUEUED;
import static org.apache.ignite.internal.eventlog.api.IgniteEventType.values;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureExceptionMatcher.willThrow;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willBe;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.testframework.matchers.JobStateMatcher.jobStateWithStatus;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.matchesRegex;
import static org.hamcrest.Matchers.notNullValue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import org.apache.ignite.InitParametersBuilder;
import org.apache.ignite.compute.ComputeException;
import org.apache.ignite.compute.IgniteCompute;
import org.apache.ignite.compute.JobDescriptor;
import org.apache.ignite.compute.JobExecution;
import org.apache.ignite.compute.JobTarget;
import org.apache.ignite.internal.ClusterPerClassIntegrationTest;
import org.apache.ignite.internal.ConfigOverride;
import org.apache.ignite.internal.compute.FailingJob;
import org.apache.ignite.internal.compute.GetNodeNameJob;
import org.apache.ignite.internal.compute.SilentSleepJob;
import org.apache.ignite.internal.compute.events.ComputeEventMetadata.Type;
import org.apache.ignite.internal.eventlog.api.IgniteEventType;
import org.apache.ignite.internal.properties.IgniteProductVersion;
import org.apache.ignite.internal.testframework.log4j2.LogInspector;
import org.apache.ignite.internal.testframework.log4j2.LogInspector.Handler;
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

@ConfigOverride(name = "ignite.compute.threadPoolSize", value = "1")
abstract class ItComputeEventsTest extends ClusterPerClassIntegrationTest {
    private final List<String> events = new ArrayList<>();

    private final LogInspector logInspector = new LogInspector(
            "EventLog", // From LogSinkConfigurationSchema.criteria default value
            new Handler(event -> true, event -> events.add(event.getMessage().getFormattedMessage()))
    );

    @BeforeEach
    void startLogInspector() {
        events.clear();
        logInspector.start();
    }

    @AfterEach
    void afterEach() {
        logInspector.stop();
        dropAllTables();
    }

    @Override
    protected int initialNodes() {
        // TODO IGNITE-26116
        return 1;
    }

    @Override
    protected void configureInitParameters(InitParametersBuilder builder) {
        String allEvents = Arrays.stream(values())
                .map(IgniteEventType::name)
                .filter(name -> name.startsWith("COMPUTE_JOB"))
                .collect(Collectors.joining(", ", "[", "]"));

        builder.clusterConfiguration("ignite.eventlog {"
                + " sinks.logSink.channel: testChannel,"
                + " channels.testChannel.events: " + allEvents
                + "}");
    }

    protected abstract IgniteCompute compute();

    @Test
    void executesSingleJobLocally() {
        JobDescriptor<Void, String> jobDescriptor = JobDescriptor.builder(GetNodeNameJob.class).build();
        JobExecution<String> execution = submit(JobTarget.node(clusterNode(0)), jobDescriptor, null);

        assertThat(execution.resultAsync(), willCompleteSuccessfully());

        UUID jobId = execution.idAsync().join(); // Safe to join since execution is complete.
        String jobClassName = jobDescriptor.jobClassName();
        String targetNode = node(0).name();

        assertEvents(
                jobEvent(COMPUTE_JOB_QUEUED, SINGLE, jobId, jobClassName, targetNode),
                jobEvent(COMPUTE_JOB_EXECUTING, SINGLE, jobId, jobClassName, targetNode),
                jobEvent(COMPUTE_JOB_COMPLETED, SINGLE, jobId, jobClassName, targetNode)
        );
    }

    @Test
    void executesFailingJobLocally() {
        JobDescriptor<Void, String> jobDescriptor = JobDescriptor.builder(FailingJob.class).build();
        JobExecution<String> execution = submit(JobTarget.node(clusterNode(0)), jobDescriptor, null);

        assertThat(execution.resultAsync(), willThrow(ComputeException.class));

        UUID jobId = execution.idAsync().join(); // Safe to join since execution is complete.
        String jobClassName = jobDescriptor.jobClassName();
        String targetNode = execution.node().name();

        assertEvents(
                jobEvent(COMPUTE_JOB_QUEUED, SINGLE, jobId, jobClassName, targetNode),
                jobEvent(COMPUTE_JOB_EXECUTING, SINGLE, jobId, jobClassName, targetNode),
                jobEvent(COMPUTE_JOB_FAILED, SINGLE, jobId, jobClassName, targetNode)
        );
    }

    @Test
    void cancelJobLocally() {
        CancelHandle cancelHandle = CancelHandle.create();

        JobDescriptor<Long, Void> jobDescriptor = JobDescriptor.builder(SilentSleepJob.class).build();
        JobExecution<Void> execution = submit(JobTarget.node(clusterNode(0)), jobDescriptor, cancelHandle.token(), Long.MAX_VALUE);

        // Wait for start executing
        await().until(execution::stateAsync, willBe(jobStateWithStatus(EXECUTING)));

        cancelHandle.cancel();

        assertThat(execution.resultAsync(), willThrow(ComputeException.class));

        UUID jobId = execution.idAsync().join(); // Safe to join since execution is complete.
        String jobClassName = jobDescriptor.jobClassName();
        String targetNode = execution.node().name();

        assertEvents(
                jobEvent(COMPUTE_JOB_QUEUED, SINGLE, jobId, jobClassName, targetNode),
                jobEvent(COMPUTE_JOB_EXECUTING, SINGLE, jobId, jobClassName, targetNode),
                jobEvent(COMPUTE_JOB_CANCELING, SINGLE, jobId, jobClassName, targetNode),
                jobEvent(COMPUTE_JOB_CANCELED, SINGLE, jobId, jobClassName, targetNode)
        );
    }

    @Test
    void cancelQueuedJobLocally() {
        // Start first job
        CancelHandle cancelHandle1 = CancelHandle.create();

        JobDescriptor<Long, Void> jobDescriptor = JobDescriptor.builder(SilentSleepJob.class).build();
        JobExecution<Void> execution1 = submit(JobTarget.node(clusterNode(0)), jobDescriptor, cancelHandle1.token(), Long.MAX_VALUE);
        // Wait for it to start executing
        await().until(execution1::stateAsync, willBe(jobStateWithStatus(EXECUTING)));

        // Start second job, it will be queued due to the queue size limit
        UUID jobId1 = execution1.idAsync().join();
        String targetNode = execution1.node().name();

        CancelHandle cancelHandle2 = CancelHandle.create();

        JobExecution<Void> execution2 = submit(JobTarget.node(clusterNode(0)), jobDescriptor, cancelHandle2.token(), Long.MAX_VALUE);
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
                jobEvent(COMPUTE_JOB_QUEUED, SINGLE, jobId1, jobClassName, targetNode),
                jobEvent(COMPUTE_JOB_EXECUTING, SINGLE, jobId1, jobClassName, targetNode),
                jobEvent(COMPUTE_JOB_QUEUED, SINGLE, jobId2, jobClassName, targetNode),
                jobEvent(COMPUTE_JOB_CANCELED, SINGLE, jobId2, jobClassName, targetNode),
                jobEvent(COMPUTE_JOB_CANCELING, SINGLE, jobId1, jobClassName, targetNode),
                jobEvent(COMPUTE_JOB_CANCELED, SINGLE, jobId1, jobClassName, targetNode)
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
                jobEvent(COMPUTE_JOB_QUEUED, SINGLE, jobId, jobClassName, targetNode).withTableName(tableName),
                jobEvent(COMPUTE_JOB_EXECUTING, SINGLE, jobId, jobClassName, targetNode).withTableName(tableName),
                jobEvent(COMPUTE_JOB_COMPLETED, SINGLE, jobId, jobClassName, targetNode).withTableName(tableName)
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
                jobEvent(COMPUTE_JOB_QUEUED, SINGLE, jobId, jobClassName, targetNode).withTableName(tableName),
                jobEvent(COMPUTE_JOB_EXECUTING, SINGLE, jobId, jobClassName, targetNode).withTableName(tableName),
                jobEvent(COMPUTE_JOB_COMPLETED, SINGLE, jobId, jobClassName, targetNode).withTableName(tableName)
        );
    }

    private <T, R> JobExecution<R> submit(JobTarget target, JobDescriptor<T, R> descriptor, @Nullable T arg) {
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

    protected EventMatcher jobEvent(
            IgniteEventType eventType,
            Type jobType,
            @Nullable UUID jobId,
            String jobClassName,
            String targetNode
    ) {
        return EventMatcher.computeJobEvent(eventType)
                .withTimestamp(notNullValue(Long.class))
                .withProductVersion(matchesRegex(IgniteProductVersion.VERSION_PATTERN))
                .withType(jobType.name())
                .withClassName(jobClassName)
                .withJobId(jobId)
                .withTargetNode(targetNode);
    }

    @SafeVarargs
    private void assertEvents(Matcher<String>... matchers) {
        await().until(() -> events, contains(matchers));
    }

    private static void createTestTableWithOneRow() {
        sql("DROP TABLE IF EXISTS test");
        sql("CREATE TABLE test (k int, v int, CONSTRAINT PK PRIMARY KEY (k))");
        sql("INSERT INTO test(k, v) VALUES (1, 101)");
    }
}
