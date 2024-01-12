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
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.in;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.ignite.Ignite;
import org.apache.ignite.compute.JobExecution;
import org.apache.ignite.internal.ClusterPerTestIntegrationTest;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.compute.utils.InteractiveJobs;
import org.apache.ignite.internal.compute.utils.TestingJobExecution;
import org.apache.ignite.internal.table.TableImpl;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.table.Tuple;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Integration tests for worker node shutdown failover.
 *
 * <p>The logic is that if we run the job on remote node and this node has left the topology then we should restart a job on
 * another node. This is not true for broadcast and local jobs. They should not be restarted.
 */
@SuppressWarnings("resource")
class ItWorkerShutdownTest extends ClusterPerTestIntegrationTest {
    /**
     * Map from node name to node index in {@link super#cluster}.
     */
    private static final Map<String, Integer> NODES_NAMES_TO_INDEXES = new HashMap<>();

    private static Set<String> workerCandidates(IgniteImpl... nodes) {
        return Arrays.stream(nodes)
                .map(IgniteImpl::node)
                .map(ClusterNode::name)
                .collect(Collectors.toSet());
    }

    private Set<ClusterNode> clusterNodesByNames(Set<String> nodes) {
        return nodes.stream()
                .map(NODES_NAMES_TO_INDEXES::get)
                .map(this::node)
                .map(IgniteImpl::node)
                .collect(Collectors.toSet());
    }

    /**
     * Initializes channels. Assumption: there is no any running job on the cluster.
     */
    @BeforeEach
    void setUp() {
        InteractiveJobs.clearState();

        NODES_NAMES_TO_INDEXES.clear();
        for (int i = 0; i < 3; i++) {
            NODES_NAMES_TO_INDEXES.put(node(i).name(), i);
        }

        executeSql("DROP TABLE IF EXISTS PUBLIC.TEST");
    }

    @Test
    void remoteExecutionWorkerShutdown() throws Exception {
        // Given entry node.
        IgniteImpl entryNode = node(0);
        // And remote candidates to execute a job.
        Set<String> remoteWorkerCandidates = workerCandidates(node(1), node(2));

        // When execute job.
        TestingJobExecution<String> execution = executeGlobalInteractiveJob(entryNode, remoteWorkerCandidates);

        // Then one of candidates became a worker and run the job.
        String workerNodeName = InteractiveJobs.globalJob().currentWorkerName();
        // And job is running.
        InteractiveJobs.globalJob().assertAlive();
        // And.
        execution.assertExecuting();

        // And save state BEFORE worker has failed.
        long createTimeBeforeFail = execution.createTimeMillis();
        long startTimeBeforeFail = execution.startTimeMillis();
        UUID jobIdBeforeFail = execution.idSync();

        // When stop worker node.
        stopNode(workerNodeName);
        // And remove it from candidates.
        remoteWorkerCandidates.remove(workerNodeName);

        // Then the job is alive: it has been restarted on another candidate.
        InteractiveJobs.globalJob().assertAlive();
        // And.
        execution.assertExecuting();
        // And remaining candidate was chosen as a failover worker.
        String failoverWorker = InteractiveJobs.globalJob().currentWorkerName();
        assertThat(remoteWorkerCandidates, hasItem(failoverWorker));

        // And check create time was not changed but start time changed.
        assertThat(execution.createTimeMillis(), equalTo(createTimeBeforeFail));
        assertThat(execution.startTimeMillis(), greaterThan(startTimeBeforeFail));
        // And id was not changed
        assertThat(execution.idSync(), equalTo(jobIdBeforeFail));

        // When finish job.
        InteractiveJobs.globalJob().finish();

        // Then it is successfully finished.
        execution.assertCompleted();
        // And finish time is greater then create time and start time.
        assertThat(execution.finishTimeMillis(), greaterThan(execution.createTimeMillis()));
        assertThat(execution.finishTimeMillis(), greaterThan(execution.startTimeMillis()));
        // And job id the same
        assertThat(execution.idSync(), equalTo(jobIdBeforeFail));
    }

    @Test
    void remoteExecutionSingleWorkerShutdown() throws Exception {
        // Given.
        IgniteImpl entryNode = node(0);
        // And only one remote candidate to execute a job.
        Set<String> remoteWorkerCandidates = workerCandidates(node(1));

        // When execute job.
        TestingJobExecution<String> execution = executeGlobalInteractiveJob(entryNode, remoteWorkerCandidates);

        // Then the job is running on worker node.
        String workerNodeName = InteractiveJobs.globalJob().currentWorkerName();
        assertThat(remoteWorkerCandidates, hasItem(workerNodeName));
        // And.
        InteractiveJobs.globalJob().assertAlive();
        execution.assertExecuting();

        // When stop worker node.
        stopNode(workerNodeName);

        // Then the job is failed, because there is no any failover worker.
        execution.assertFailed();
    }

    @Test
    void localExecutionWorkerShutdown() throws Exception {
        // Given entry node.
        IgniteImpl entryNode = node(0);

        // When execute job locally.
        TestingJobExecution<String> execution = executeGlobalInteractiveJob(entryNode, Set.of(entryNode.name()));

        // Then the job is running.
        InteractiveJobs.globalJob().assertAlive();
        execution.assertExecuting();

        // And it is running on entry node.
        assertThat(InteractiveJobs.globalJob().currentWorkerName(), equalTo(entryNode.name()));

        // When stop entry node.
        stopNode(entryNode.name());

        // Then the job is failed, because there is no any failover worker.
        assertThat(execution.resultAsync().isCompletedExceptionally(), equalTo(true));
    }

    @Test
    void broadcastExecutionWorkerShutdown() {
        // Given entry node.
        IgniteImpl entryNode = node(0);
        // And prepare communication channels.
        InteractiveJobs.initChannels(allNodeNames());

        // When start broadcast job.
        Map<ClusterNode, JobExecution<Object>> executions = entryNode.compute().broadcastAsync(
                clusterNodesByNames(workerCandidates(node(0), node(1), node(2))),
                List.of(),
                InteractiveJobs.interactiveJobName()
        );

        // Then all three jobs are alive.
        assertThat(executions.size(), is(3));
        executions.forEach((node, execution) -> {
            InteractiveJobs.byNode(node).assertAlive();
            new TestingJobExecution<>(execution).assertExecuting();
        });

        // When stop one of workers.
        stopNode(node(1).name());

        // Then two jobs are alive.
        executions.forEach((node, execution) -> {
            if (node.name().equals(node(1).name())) {
                new TestingJobExecution<>(execution).assertFailed();
            } else {
                InteractiveJobs.byNode(node).assertAlive();
                new TestingJobExecution<>(execution).assertExecuting();
            }
        });

        // When.
        InteractiveJobs.all().finish();

        // Then every job ran once because broadcast execution does not require failover.
        InteractiveJobs.all().assertEachCalledOnce();
    }

    @Test
    void cancelRemoteExecutionOnRestartedJob() throws Exception {
        // Given entry node.
        IgniteImpl entryNode = node(0);
        // And remote candidates to execute a job.
        Set<String> remoteWorkerCandidates = workerCandidates(node(1), node(2));

        // When execute job.
        TestingJobExecution<String> execution = executeGlobalInteractiveJob(entryNode, remoteWorkerCandidates);

        // Then one of candidates became a worker and run the job.
        String workerNodeName = InteractiveJobs.globalJob().currentWorkerName();
        // And job is running.
        InteractiveJobs.globalJob().assertAlive();
        execution.assertExecuting();

        // When stop worker node.
        stopNode(workerNodeName);
        // And remove it from candidates.
        remoteWorkerCandidates.remove(workerNodeName);

        // Then the job is alive: it has been restarted on another candidate.
        InteractiveJobs.globalJob().assertAlive();
        execution.assertExecuting();
        // And remaining candidate was chosen as a failover worker.
        String failoverWorker = InteractiveJobs.globalJob().currentWorkerName();
        assertThat(remoteWorkerCandidates, hasItem(failoverWorker));

        // When cancel job.
        execution.cancelSync();

        // Then it is cancelled.
        execution.assertCancelled();
    }

    @Test
    void colocatedExecutionWorkerShutdown() throws Exception {
        // Given table.
        InteractiveJobs.initChannels(allNodeNames());
        createTestTableWithOneRow();
        TableImpl table = (TableImpl) node(0).tables().table("test");
        // And partition leader for K=1.
        ClusterNode leader = table.leaderAssignment(table.partition(Tuple.create(1).set("K", 1)));

        // When start colocated job on node that is not leader.
        IgniteImpl entryNode = anyNodeExcept(leader);
        TestingJobExecution<Object> execution = new TestingJobExecution<>(entryNode.compute().executeColocatedAsync(
                "test",
                Tuple.create(1).set("K", 1),
                List.of(),
                InteractiveJobs.globalJob().name()
        ));

        // Then the job is alive.
        InteractiveJobs.globalJob().assertAlive();
        execution.assertExecuting();

        // And it is running on leader node.
        String firstWorkerNodeName = InteractiveJobs.globalJob().currentWorkerName();
        assertThat(firstWorkerNodeName, equalTo(leader.name()));

        // When stop worker node.
        stopNode(leader);

        // Then the job is restarted on another node.
        InteractiveJobs.globalJob().assertAlive();
        execution.assertExecuting();

        // And it is running on another node.
        String failoverNodeName = InteractiveJobs.globalJob().currentWorkerName();
        assertThat(failoverNodeName, in(allNodeNames()));
        // And this node is the partition leader for K=1.
        leader = table.leaderAssignment(table.partition(Tuple.create(1).set("K", 1)));
        assertThat(failoverNodeName, equalTo(leader.name()));
        // But this is not the same node as before.
        assertThat(failoverNodeName, not(equalTo(firstWorkerNodeName)));
    }

    private void stopNode(ClusterNode clusterNode) {
        stopNode(clusterNode.name());
    }

    private void stopNode(IgniteImpl ignite) {
        stopNode(ignite.name());
    }

    private void stopNode(String name) {
        int ind = NODES_NAMES_TO_INDEXES.get(name);
        node(ind).stop();
    }

    private IgniteImpl anyNodeExcept(ClusterNode except) {
        String candidateName = allNodeNames()
                .stream()
                .filter(name -> !name.equals(except.name()))
                .findFirst()
                .orElseThrow();

        return nodeByName(candidateName);
    }

    private IgniteImpl nodeByName(String candidateName) {
        return cluster.runningNodes().filter(node -> node.name().equals(candidateName)).findFirst().orElseThrow();
    }

    private TestingJobExecution<String> executeGlobalInteractiveJob(IgniteImpl entryNode, Set<String> nodes) throws InterruptedException {
        return new TestingJobExecution<>(
                entryNode.compute().executeAsync(clusterNodesByNames(nodes), List.of(), InteractiveJobs.globalJob().name())
        );
    }

    private void createTestTableWithOneRow() {
        executeSql("CREATE TABLE test (k int, v int, CONSTRAINT PK PRIMARY KEY (k))");
        executeSql("INSERT INTO test(k, v) VALUES (1, 101)");
    }

    private List<String> allNodeNames() {
        return IntStream.range(0, initialNodes())
                .mapToObj(this::node)
                .map(Ignite::name)
                .collect(toList());
    }
}
