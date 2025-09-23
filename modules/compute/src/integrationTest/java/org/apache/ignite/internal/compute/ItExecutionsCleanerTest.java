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

import static org.apache.ignite.internal.IgniteExceptionTestUtils.traceableException;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureExceptionMatcher.willThrow;
import static org.apache.ignite.internal.wrapper.Wrappers.unwrap;
import static org.apache.ignite.lang.ErrorGroups.Compute.RESULT_NOT_FOUND_ERR;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.both;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.not;

import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.ignite.Ignite;
import org.apache.ignite.compute.ComputeException;
import org.apache.ignite.compute.JobDescriptor;
import org.apache.ignite.compute.JobTarget;
import org.apache.ignite.internal.ClusterPerClassIntegrationTest;
import org.apache.ignite.internal.TraceableExceptionMatcher;
import org.apache.ignite.internal.compute.messaging.RemoteJobExecution;
import org.apache.ignite.internal.compute.utils.InteractiveJobs;
import org.apache.ignite.internal.compute.utils.TestingJobExecution;
import org.apache.ignite.lang.CancelHandle;
import org.apache.ignite.lang.CancellationToken;
import org.apache.ignite.network.ClusterNode;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ItExecutionsCleanerTest extends ClusterPerClassIntegrationTest {
    private ExecutionManager localExecutionManager;

    private ExecutionManager remoteExecutionManager;

    private Set<ClusterNode> localNodes;

    private Set<ClusterNode> remoteNodes;

    @Override
    protected int[] cmgMetastoreNodes() {
        return new int[]{0, 1, 2};
    }

    @Override
    protected String getNodeBootstrapConfigTemplate() {
        return "ignite {\n"
                + "  network: {\n"
                + "    port: {},\n"
                + "    nodeFinder.netClusterNodes: [ {} ]\n"
                + "  },\n"
                + "  clientConnector.port: {},\n"
                + "  rest.port: {},\n"
                + "  compute: {"
                + "    threadPoolSize: 1,\n"
                + "    statesLifetimeMillis: 1000\n"
                + "  },\n"
                + "  failureHandler.dumpThreadsOnFailure: false\n"
                + "}";
    }

    @BeforeEach
    void setUp() {
        // Get new references before each test since node can be restarted.
        Ignite localNode = CLUSTER.node(0);
        Ignite remoteNode = CLUSTER.node(1);

        localExecutionManager = executionManager(localNode);
        remoteExecutionManager = executionManager(remoteNode);

        localNodes = Set.of(clusterNode(localNode));
        remoteNodes = Set.of(clusterNode(remoteNode));
    }

    private static ExecutionManager executionManager(Ignite ignite) {
        IgniteComputeImpl compute = unwrap(ignite.compute(), IgniteComputeImpl.class);
        return ((ComputeComponentImpl) compute.computeComponent()).executionManager();
    }

    @Test
    void localCompleted() throws Exception {
        TestingJobExecution<Object> execution = submit(localNodes);
        UUID jobId = execution.idSync();

        // Complete the task
        InteractiveJobs.globalJob().finish();
        execution.assertCompleted();

        // Execution is retained
        assertThat(localExecutionManager.localExecutions(), hasSingleEntry(equalTo(jobId), instanceOf(DelegatingJobExecution.class)));

        // And eventually cleaned
        await().untilAsserted(() -> {
            assertThat(localExecutionManager.localExecutions(), is(anEmptyMap()));

            assertThat(localExecutionManager.localResultAsync(jobId), willThrow(resultNotFoundErr()));
        });
    }

    @Test
    void localCancelled() throws Exception {
        // Start first task
        CancelHandle runningCancelHandle = CancelHandle.create();
        TestingJobExecution<Object> runningExecution = submit(localNodes, runningCancelHandle.token());
        UUID runningJobId = runningExecution.idSync();

        // Start second task
        CancelHandle queuedCancelHandle = CancelHandle.create();
        TestingJobExecution<Object> queuedExecution = submit(localNodes, queuedCancelHandle.token());
        UUID queuedJobId = queuedExecution.idSync();

        // Second task is queued, cancel it
        queuedExecution.assertQueued();
        queuedCancelHandle.cancel();
        queuedExecution.assertCancelled();

        // First task is executing, cancel it
        runningExecution.assertExecuting();
        runningCancelHandle.cancel();
        runningExecution.assertCancelled();

        // All executions are retained
        assertThat(localExecutionManager.localExecutions(), is(aMapWithSize(2)));
        assertThat(localExecutionManager.localExecutions(), hasEntry(equalTo(runningJobId), instanceOf(DelegatingJobExecution.class)));
        assertThat(localExecutionManager.localExecutions(), hasEntry(equalTo(queuedJobId), instanceOf(DelegatingJobExecution.class)));

        // And eventually cleaned
        await().untilAsserted(() -> {
            assertThat(localExecutionManager.localExecutions(), is(anEmptyMap()));

            assertThat(localExecutionManager.localResultAsync(runningJobId), willThrow(resultNotFoundErr()));
            assertThat(localExecutionManager.localResultAsync(queuedJobId), willThrow(resultNotFoundErr()));
        });
    }

    @Test
    void remoteCompleted() throws Exception {
        // Completed
        TestingJobExecution<Object> execution = submit(remoteNodes);
        UUID jobId = execution.idSync();

        // Complete the task
        InteractiveJobs.globalJob().finish();
        execution.assertCompleted();

        // Execution is retained
        assertThat(localExecutionManager.localExecutions(), hasSingleEntry(equalTo(jobId), instanceOf(FailSafeJobExecution.class)));
        assertThat(localExecutionManager.remoteExecutions(), hasSingleEntry(not(equalTo(jobId)), instanceOf(RemoteJobExecution.class)));
        assertThat(remoteExecutionManager.localExecutions(), hasSingleEntry(not(equalTo(jobId)), instanceOf(DelegatingJobExecution.class)));

        // And eventually cleaned
        await().untilAsserted(() -> {
            assertThat(localExecutionManager.localExecutions(), anEmptyMap());
            assertThat(localExecutionManager.remoteExecutions(), anEmptyMap());
            assertThat(remoteExecutionManager.localExecutions(), anEmptyMap());

            assertThat(localExecutionManager.localResultAsync(jobId), willThrow(resultNotFoundErr()));
            assertThat(remoteExecutionManager.localResultAsync(jobId), willThrow(resultNotFoundErr()));
        });
    }

    @Test
    void remoteCancelled() throws Exception {
        // Start first task
        CancelHandle runningCancelHandle = CancelHandle.create();
        TestingJobExecution<Object> runningExecution = submit(remoteNodes, runningCancelHandle.token());
        UUID runningJobId = runningExecution.idSync();

        // Start second task
        CancelHandle queuedCancelHandle = CancelHandle.create();
        TestingJobExecution<Object> queuedExecution = submit(remoteNodes, queuedCancelHandle.token());
        UUID queuedJobId = queuedExecution.idSync();

        // Second task is queued, cancel it
        queuedExecution.assertQueued();
        queuedCancelHandle.cancel();
        queuedExecution.assertCancelled();

        // First task is executing, cancel it
        runningExecution.assertExecuting();
        runningCancelHandle.cancel();
        runningExecution.assertCancelled();

        // All executions are retained
        assertThat(localExecutionManager.localExecutions(), is(aMapWithSize(2)));
        assertThat(localExecutionManager.localExecutions(), hasEntry(equalTo(runningJobId), instanceOf(FailSafeJobExecution.class)));
        assertThat(localExecutionManager.localExecutions(), hasEntry(equalTo(queuedJobId), instanceOf(FailSafeJobExecution.class)));

        // Actual ids are different, so check only the size
        assertThat(localExecutionManager.remoteExecutions(), is(aMapWithSize(2)));
        assertThat(localExecutionManager.remoteExecutions().values(), everyItem(instanceOf(RemoteJobExecution.class)));

        assertThat(remoteExecutionManager.localExecutions(), is(aMapWithSize(2)));
        assertThat(remoteExecutionManager.localExecutions().values(), everyItem(instanceOf(DelegatingJobExecution.class)));

        // And eventually cleaned
        await().untilAsserted(() -> {
            assertThat(localExecutionManager.localExecutions(), is(anEmptyMap()));
            assertThat(localExecutionManager.remoteExecutions(), is(anEmptyMap()));
            assertThat(remoteExecutionManager.localExecutions(), is(anEmptyMap()));

            assertThat(localExecutionManager.localResultAsync(runningJobId), willThrow(resultNotFoundErr()));
            assertThat(remoteExecutionManager.localResultAsync(runningJobId), willThrow(resultNotFoundErr()));
            assertThat(localExecutionManager.localResultAsync(queuedJobId), willThrow(resultNotFoundErr()));
            assertThat(remoteExecutionManager.localResultAsync(queuedJobId), willThrow(resultNotFoundErr()));
        });
    }

    @Test
    void failover() throws Exception {
        TestingJobExecution<Object> execution = submit(
                Set.of(clusterNode(1), clusterNode(2))
        );
        UUID jobId = execution.idSync();

        execution.assertExecuting();

        // Stop the worker node
        String workerNodeName = InteractiveJobs.globalJob().currentWorkerName();
        int workerNodeIndex = CLUSTER.nodeIndex(workerNodeName);
        CLUSTER.stopNode(workerNodeIndex);

        String failoverWorkerNodeName = InteractiveJobs.globalJob().currentWorkerName();

        Ignite failoverNode = CLUSTER.node(CLUSTER.nodeIndex(failoverWorkerNodeName));
        ExecutionManager failoverExecutionManager = executionManager(failoverNode);

        InteractiveJobs.globalJob().assertAlive();
        execution.assertExecuting();

        // Complete the task
        InteractiveJobs.globalJob().finish();
        execution.assertCompleted();

        // Execution is retained
        assertThat(localExecutionManager.localExecutions(), hasSingleEntry(equalTo(jobId), instanceOf(FailSafeJobExecution.class)));
        // There could be 1 or 2 executions, one from the initial execution, another from failover
        assertThat(localExecutionManager.remoteExecutions(), is(aMapWithSize(lessThanOrEqualTo(2))));
        assertThat(localExecutionManager.remoteExecutions(), hasEntry(not(equalTo(jobId)), instanceOf(RemoteJobExecution.class)));

        // Job id on the failover node is different, so just check that something is stored there
        assertThat(
                failoverExecutionManager.localExecutions(),
                hasSingleEntry(not(equalTo(jobId)), instanceOf(DelegatingJobExecution.class))
        );

        // And eventually cleaned
        await().untilAsserted(() -> {
            assertThat(localExecutionManager.localExecutions(), is(anEmptyMap()));
            assertThat(localExecutionManager.remoteExecutions(), is(anEmptyMap()));
            assertThat(failoverExecutionManager.localExecutions(), is(anEmptyMap()));
        });

        // Start node again for next tests
        CLUSTER.startNode(workerNodeIndex);
    }

    private static TestingJobExecution<Object> submit(Set<ClusterNode> nodes) {
        return submit(nodes, null);
    }

    private static TestingJobExecution<Object> submit(Set<ClusterNode> nodes, @Nullable CancellationToken cancellationToken) {
        return new TestingJobExecution<>(CLUSTER.node(0).compute().submitAsync(
                JobTarget.anyNode(nodes), JobDescriptor.builder(InteractiveJobs.globalJob().name()).build(), null, cancellationToken
        ));
    }

    /**
     * Creates a matcher for a map that matches when the map contains a singular entry whose key satisfies the specified {@code keyMatcher}
     * and whose value satisfies the specified {@code valueMatcher}.
     *
     * @param <K> Key type.
     * @param <V> Value type.
     * @param keyMatcher Key matcher.
     * @param valueMatcher Value matcher.
     * @return The matcher.
     */
    private static <K, V> Matcher<Map<? extends K, ? extends V>> hasSingleEntry(
            Matcher<? super K> keyMatcher,
            Matcher<? super V> valueMatcher
    ) {
        return both(Matchers.<K, V>aMapWithSize(1)).and(hasEntry(keyMatcher, valueMatcher));
    }

    private static TraceableExceptionMatcher resultNotFoundErr() {
        return traceableException(ComputeException.class).withCode(equalTo(RESULT_NOT_FOUND_ERR));
    }
}
