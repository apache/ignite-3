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

import static org.apache.ignite.internal.compute.events.ComputeEventMetadata.Type.SINGLE;
import static org.apache.ignite.internal.eventlog.api.IgniteEventType.COMPUTE_JOB_EXECUTING;
import static org.apache.ignite.internal.eventlog.api.IgniteEventType.COMPUTE_JOB_FAILED;
import static org.apache.ignite.internal.eventlog.api.IgniteEventType.COMPUTE_JOB_QUEUED;
import static org.apache.ignite.internal.eventlog.api.IgniteEventType.values;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInRelativeOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.not;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.ignite.InitParametersBuilder;
import org.apache.ignite.compute.IgniteCompute;
import org.apache.ignite.compute.JobDescriptor;
import org.apache.ignite.compute.JobTarget;
import org.apache.ignite.internal.ClusterPerTestIntegrationTest;
import org.apache.ignite.internal.compute.events.ComputeEventMetadata.Type;
import org.apache.ignite.internal.compute.events.EventMatcher;
import org.apache.ignite.internal.compute.utils.InteractiveJobs;
import org.apache.ignite.internal.compute.utils.TestingJobExecution;
import org.apache.ignite.internal.eventlog.api.IgniteEventType;
import org.apache.ignite.internal.testframework.log4j2.EventLogInspector;
import org.apache.ignite.network.ClusterNode;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests that the failover fails when candidate node is not in the cluster when it is selected for the failover.
 */
abstract class ItFailoverCandidateNotFoundTest extends ClusterPerTestIntegrationTest {
    private final EventLogInspector logInspector = new EventLogInspector();

    @BeforeEach
    void setUp() {
        InteractiveJobs.clearState();

        logInspector.start();
    }

    @AfterEach
    void afterEach() {
        logInspector.stop();
    }

    @Override
    protected void customizeInitParameters(InitParametersBuilder builder) {
        String allComputeEvents = Arrays.stream(values())
                .map(IgniteEventType::name)
                .filter(name -> name.startsWith("COMPUTE_JOB"))
                .collect(Collectors.joining(", ", "[", "]"));

        builder.clusterConfiguration("ignite.eventlog {"
                + " sinks.logSink.channel: testChannel,"
                + " channels.testChannel.events: " + allComputeEvents
                + "}");
    }

    @Override
    protected int initialNodes() {
        return 5;
    }

    /**
     * Make a CMG from non-worker nodes. We wont lose the leader in tests then.
     */
    @Override
    protected int[] cmgMetastoreNodes() {
        return new int[]{0, 3, 4};
    }

    abstract IgniteCompute compute();

    @Test
    void failoverCandidateLeavesCluster() throws Exception {
        // Given remote candidates to execute a job.
        Set<ClusterNode> remoteWorkerCandidates = Set.of(publicClusterNode(1), publicClusterNode(2));
        Set<String> remoteWorkerCandidateNames = remoteWorkerCandidates.stream()
                .map(ClusterNode::name)
                .collect(Collectors.toCollection(HashSet::new));

        // When execute job.
        TestingJobExecution<String> execution = executeGlobalInteractiveJob(remoteWorkerCandidates);

        // Then one of candidates became a worker and run the job.
        String workerNodeName = InteractiveJobs.globalJob().currentWorkerName();
        // And job is running.
        InteractiveJobs.globalJob().assertAlive();
        // And.
        execution.assertExecuting();
        UUID jobId = execution.idSync();

        // Remove worker node from candidates, leaving other node.
        remoteWorkerCandidateNames.remove(workerNodeName);
        assertThat(remoteWorkerCandidateNames, hasSize(1));

        // Stop non-worker candidate node.
        String failoverCandidateNodeName = remoteWorkerCandidateNames.stream().findFirst().orElseThrow();
        stopNode(failoverCandidateNodeName);

        // When stop worker node.
        stopNode(workerNodeName);

        // Then the job is failed, because there are no more failover workers.
        execution.assertFailed();

        String jobClassName = InteractiveJobs.globalJob().name();

        // When node is shut down gracefully, the job execution is interrupted and event could be logged anyway
        // So there would be 2 events from a worker node, 1 failed events from a worker node and 1 failed event from the coordinator
        await().until(logInspector::events, contains(
                jobEvent(COMPUTE_JOB_QUEUED, jobId, jobClassName, workerNodeName),
                jobEvent(COMPUTE_JOB_EXECUTING, jobId, jobClassName, workerNodeName),
                jobEvent(COMPUTE_JOB_FAILED, jobId, jobClassName, workerNodeName),
                jobEvent(COMPUTE_JOB_FAILED, jobId, jobClassName, workerNodeName)
        ));
    }

    @Test
    void failoverAndThenStopWorker() throws Exception {
        // Given remote candidates to execute a job.
        Set<ClusterNode> remoteWorkerCandidates = Set.of(publicClusterNode(1), publicClusterNode(2));
        Set<String> remoteWorkerCandidateNames = remoteWorkerCandidates.stream()
                .map(ClusterNode::name)
                .collect(Collectors.toCollection(HashSet::new));

        // When execute job.
        TestingJobExecution<String> execution = executeGlobalInteractiveJob(remoteWorkerCandidates);

        // Then one of candidates became a worker and run the job.
        String workerNodeName = InteractiveJobs.globalJob().currentWorkerName();
        UUID jobId = execution.idSync();

        // When stop worker node.
        stopNode(workerNodeName);

        // Remove worker node from candidates, leaving other node.
        remoteWorkerCandidateNames.remove(workerNodeName);
        assertThat(remoteWorkerCandidateNames, hasSize(1));

        // Wait for execution on another node
        String failoverWorker = InteractiveJobs.globalJob().currentWorkerName();
        assertThat(failoverWorker, not(equalTo(workerNodeName)));
        assertThat(remoteWorkerCandidateNames, contains(failoverWorker));

        // Stop failover worker node.
        stopNode(failoverWorker);

        // Then the job is failed, because there are no more failover workers.
        execution.assertFailed();

        // When node is shut down gracefully, the job execution is interrupted and event could be logged anyway
        // So there would be 4 events from worker nodes, 2 failed events from both worker nodes and 1 failed event from the coordinator
        // The order of failed events is not determined
        await().until(logInspector::events, hasSize(7));

        String jobClassName = InteractiveJobs.globalJob().name();

        assertThat(logInspector.events(), containsInRelativeOrder(
                jobEvent(COMPUTE_JOB_QUEUED, jobId, jobClassName, workerNodeName),
                jobEvent(COMPUTE_JOB_EXECUTING, jobId, jobClassName, workerNodeName),
                jobEvent(COMPUTE_JOB_QUEUED, jobId, jobClassName, failoverWorker),
                jobEvent(COMPUTE_JOB_EXECUTING, jobId, jobClassName, failoverWorker),
                jobEvent(COMPUTE_JOB_FAILED, jobId, jobClassName, failoverWorker)
        ));

        // Failed event from second worker node
        assertThat(logInspector.events(), hasItem(
                jobEvent(COMPUTE_JOB_FAILED, jobId, jobClassName, workerNodeName)
        ));
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

    private TestingJobExecution<String> executeGlobalInteractiveJob(Set<ClusterNode> nodes) {
        return new TestingJobExecution<>(compute().submitAsync(
                JobTarget.anyNode(nodes),
                JobDescriptor.builder(InteractiveJobs.globalJob().jobClass()).build(),
                null
        ));
    }
}
