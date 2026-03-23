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

import static java.util.UUID.randomUUID;
import static org.apache.ignite.internal.compute.utils.InteractiveJobs.Signal.RETURN_WORKER_NAME;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.assertThrows;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureExceptionMatcher.willThrow;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import org.apache.ignite.compute.BroadcastExecution;
import org.apache.ignite.compute.BroadcastJobTarget;
import org.apache.ignite.compute.IgniteCompute;
import org.apache.ignite.compute.JobDescriptor;
import org.apache.ignite.compute.JobTarget;
import org.apache.ignite.compute.NodeNotFoundException;
import org.apache.ignite.internal.ClusterPerClassIntegrationTest;
import org.apache.ignite.internal.compute.utils.InteractiveJobs;
import org.apache.ignite.internal.compute.utils.TestingJobExecution;
import org.apache.ignite.internal.network.PublicClusterNodeImpl;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.NetworkAddress;
import org.junit.jupiter.api.Test;

/**
 * Tests compute API errors.
 */
@SuppressWarnings("ThrowableNotThrown")
abstract class ItComputeErrorsBaseTest extends ClusterPerClassIntegrationTest {
    private final ClusterNode nonExistingNode = new PublicClusterNodeImpl(
            randomUUID(), "non-existing-name", new NetworkAddress("non-existing-host", 1)
    );

    @Test
    void executeAsyncSucceedsWhenAtLeastOnNodeIsInTheCluster() throws InterruptedException {
        // When set of nodes contain existing and non-existing nodes
        ClusterNode existingNode = clusterNode(0);
        Set<ClusterNode> nodes = Set.of(existingNode, nonExistingNode);

        // And execute a job
        TestingJobExecution<String> execution = executeGlobalInteractiveJob(nodes);

        // Then existing node became a worker and run the job.
        String workerNodeName = InteractiveJobs.globalJob().currentWorkerName();
        assertThat(workerNodeName, is(existingNode.name()));

        // And job is running.
        InteractiveJobs.globalJob().assertAlive();

        // Cleanup
        InteractiveJobs.globalJob().finish();
        execution.assertCompleted();
    }

    @Test
    void executeAsyncFailsWhenNoNodesAreInTheCluster() {
        // When set of nodes contain only non-existing nodes
        Set<ClusterNode> nodes = Set.of(nonExistingNode);

        // And execute a job
        TestingJobExecution<String> execution = executeGlobalInteractiveJob(nodes);

        // Then job fails.
        String errorMessageFragment = "None of the specified nodes are present in the cluster: [" + nonExistingNode.name() + "]";
        assertThat(execution.resultAsync(), willThrow(NodeNotFoundException.class, errorMessageFragment));
    }

    @Test
    void executeSucceedsWhenAtLeastOnNodeIsInTheCluster() {
        // When set of nodes contain existing and non-existing nodes
        ClusterNode existingNode = clusterNode(0);
        Set<ClusterNode> nodes = Set.of(existingNode, nonExistingNode);

        // And execute a job
        String workerNodeName = compute().execute(
                JobTarget.anyNode(nodes),
                JobDescriptor.<String, String>builder(InteractiveJobs.globalJob().name()).build(),
                RETURN_WORKER_NAME.name());

        // Then existing node was a worker and executed the job.
        assertThat(workerNodeName, is(existingNode.name()));
    }

    @Test
    void executeFailsWhenNoNodesAreInTheCluster() {
        // When set of nodes contain only non-existing nodes
        JobTarget nodes = JobTarget.node(nonExistingNode);

        // Then job fails.
        assertThrows(
                NodeNotFoundException.class,
                () -> compute().execute(nodes, JobDescriptor.builder(InteractiveJobs.globalJob().name()).build(), null),
                "None of the specified nodes are present in the cluster: [" + nonExistingNode.name() + "]"
        );
    }

    @Test
    void broadcastAsync() {
        // When set of nodes contain existing and non-existing nodes
        ClusterNode existingNode = clusterNode(0);
        Set<ClusterNode> nodes = Set.of(existingNode, nonExistingNode);

        // And prepare communication channels.
        InteractiveJobs.initChannels(nodes.stream().map(ClusterNode::name).collect(Collectors.toList()));

        // When broadcast a job
        CompletableFuture<BroadcastExecution<String>> executionFut = compute().submitAsync(
                BroadcastJobTarget.nodes(nodes),
                InteractiveJobs.interactiveJobDescriptor(),
                null
        );

        assertThat(executionFut, willCompleteSuccessfully());
        BroadcastExecution<String> execution = executionFut.join();

        // Finish running job so that the results could be retrieved.
        InteractiveJobs.all().finish();

        String errorMessageFragment = "None of the specified nodes are present in the cluster: [" + nonExistingNode.name() + "]";
        assertThat(execution.resultsAsync(), willThrow(NodeNotFoundException.class, errorMessageFragment));
    }

    protected abstract IgniteCompute compute();

    private TestingJobExecution<String> executeGlobalInteractiveJob(Set<ClusterNode> nodes) {
        return new TestingJobExecution<>(compute().submitAsync(
                JobTarget.anyNode(nodes),
                JobDescriptor.<String, String>builder(InteractiveJobs.globalJob().name()).build(),
                ""
        ));
    }
}
