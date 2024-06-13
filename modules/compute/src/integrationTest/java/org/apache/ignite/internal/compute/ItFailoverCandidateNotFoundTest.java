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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.compute.IgniteCompute;
import org.apache.ignite.compute.JobDescriptor;
import org.apache.ignite.internal.ClusterPerTestIntegrationTest;
import org.apache.ignite.internal.compute.utils.InteractiveJobs;
import org.apache.ignite.internal.compute.utils.TestingJobExecution;
import org.apache.ignite.network.ClusterNode;
import org.junit.jupiter.api.Test;

/**
 * Tests that the failover fails when candidate node is not in the cluster when it is selected for the failover.
 */
public class ItFailoverCandidateNotFoundTest extends ClusterPerTestIntegrationTest {
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

    @Test
    void thinClientFailoverCandidateLeavesCluster() throws Exception {
        failoverCandidateLeavesCluster(node(0).compute());
    }

    @Test
    void embeddedFailoverCandidateLeavesCluster() throws Exception {
        String address = "127.0.0.1:" + node(0).clientAddress().port();
        try (IgniteClient client = IgniteClient.builder().addresses(address).build()) {
            failoverCandidateLeavesCluster(client.compute());
        }
    }

    private void failoverCandidateLeavesCluster(IgniteCompute compute) throws Exception {
        // Given remote candidates to execute a job.
        Set<ClusterNode> remoteWorkerCandidates = Set.of(node(1).node(), node(2).node());
        Set<String> remoteWorkerCandidateNames = remoteWorkerCandidates.stream()
                .map(ClusterNode::name)
                .collect(Collectors.toCollection(HashSet::new));

        // When execute job.
        TestingJobExecution<String> execution = executeGlobalInteractiveJob(compute, remoteWorkerCandidates);

        // Then one of candidates became a worker and run the job.
        String workerNodeName = InteractiveJobs.globalJob().currentWorkerName();
        // And job is running.
        InteractiveJobs.globalJob().assertAlive();
        // And.
        execution.assertExecuting();

        // Remove worker node from candidates, leaving other node.
        remoteWorkerCandidateNames.remove(workerNodeName);
        assertThat(remoteWorkerCandidateNames.size(), is(1));

        // Stop non-worker candidate node.
        String failoverCandidateNodeName = remoteWorkerCandidateNames.stream().findFirst().orElseThrow();
        stopNode(failoverCandidateNodeName);

        // When stop worker node.
        stopNode(workerNodeName);

        // Then the job is failed, because there are no more failover workers.
        execution.assertFailed();
    }

    private static TestingJobExecution<String> executeGlobalInteractiveJob(IgniteCompute compute, Set<ClusterNode> nodes) {
        return new TestingJobExecution<>(compute.submit(nodes, JobDescriptor.builder()
                .jobClassName(InteractiveJobs.globalJob().name())
                .build()));
    }
}
