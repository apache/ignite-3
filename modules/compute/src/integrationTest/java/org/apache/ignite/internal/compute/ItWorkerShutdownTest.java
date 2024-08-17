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

import static java.util.stream.Collectors.toSet;
import static org.apache.ignite.internal.TestWrappers.unwrapIgniteImpl;
import static org.apache.ignite.internal.TestWrappers.unwrapTableImpl;
import static org.apache.ignite.internal.catalog.CatalogService.DEFAULT_STORAGE_PROFILE;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.in;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import org.apache.ignite.Ignite;
import org.apache.ignite.compute.IgniteCompute;
import org.apache.ignite.compute.JobDescriptor;
import org.apache.ignite.compute.JobExecution;
import org.apache.ignite.compute.JobTarget;
import org.apache.ignite.internal.ClusterPerTestIntegrationTest;
import org.apache.ignite.internal.TestWrappers;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.compute.utils.InteractiveJobs;
import org.apache.ignite.internal.compute.utils.InteractiveJobs.AllInteractiveJobsApi;
import org.apache.ignite.internal.compute.utils.TestingJobExecution;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.placementdriver.ReplicaMeta;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.table.TableImpl;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.table.Tuple;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Integration tests for worker node shutdown failover.
 *
 * <p>The logic is that if we run the job on the remote node and this node has left the logical topology then we should restart a job on
 * another node. This is not true for broadcast and local jobs. They should not be restarted.
 */
@SuppressWarnings("resource")
public abstract class ItWorkerShutdownTest extends ClusterPerTestIntegrationTest {
    /**
     * Map from node name to node index in {@link super#cluster}.
     */
    private static final Map<String, Integer> NODES_NAMES_TO_INDEXES = new HashMap<>();

    private static final String TABLE_NAME = "test";

    /**
     * CMG == number of nodes in cluster. We wont lose the leader in tests then.
     */
    @Override
    protected int[] cmgMetastoreNodes() {
        return new int[]{0, 1, 2};
    }

    private static Set<String> workerCandidates(Ignite... nodes) {
        return Arrays.stream(nodes)
                .map(Ignite::name)
                .collect(toSet());
    }

    private Set<ClusterNode> clusterNodesByNames(Set<String> nodes) {
        return nodes.stream()
                .map(NODES_NAMES_TO_INDEXES::get)
                .map(this::node)
                .map(TestWrappers::unwrapIgniteImpl)
                .map(IgniteImpl::node)
                .collect(toSet());
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
        Ignite entryNode = node(0);
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
        // And id was not changed.
        assertThat(execution.idSync(), equalTo(jobIdBeforeFail));

        // When finish job.
        InteractiveJobs.globalJob().finish();

        // Then it is successfully finished.
        execution.assertCompleted();
        // And finish time is greater then create time and start time.
        assertThat(execution.finishTimeMillis(), greaterThan(execution.createTimeMillis()));
        assertThat(execution.finishTimeMillis(), greaterThan(execution.startTimeMillis()));
        // And job id the same.
        assertThat(execution.idSync(), equalTo(jobIdBeforeFail));
    }

    @Test
    void remoteExecutionSingleWorkerShutdown() throws Exception {
        // Given.
        Ignite entryNode = node(0);
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
        Ignite entryNode = node(0);

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
        Ignite entryNode = node(0);
        // And prepare communication channels.
        InteractiveJobs.initChannels(allNodeNames());

        // When start broadcast job.
        Map<ClusterNode, JobExecution<Object>> executions = compute(entryNode).submitBroadcast(
                clusterNodesByNames(workerCandidates(node(0), node(1), node(2))),
                JobDescriptor.builder(InteractiveJobs.interactiveJobName()).build(), null);

        // Then all three jobs are alive.
        assertThat(executions.size(), is(3));
        executions.forEach((node, execution) -> {
            InteractiveJobs.byNode(node).assertAlive();
            new TestingJobExecution<>(execution).assertExecuting();
        });

        // When stop one of workers.
        String stoppedNodeName = node(1).name();
        stopNode(node(1));

        // Then two jobs are alive.
        executions.forEach((node, execution) -> {
            if (node.name().equals(stoppedNodeName)) {
                new TestingJobExecution<>(execution).assertFailed();
            } else {
                InteractiveJobs.byNode(node).assertAlive();
                new TestingJobExecution<>(execution).assertExecuting();
            }
        });

        // When.
        InteractiveJobs.all().finish();

        // Then every job ran once because broadcast execution does not require failover.
        AllInteractiveJobsApi.assertEachCalledOnce();
    }

    @Test
    void cancelRemoteExecutionOnRestartedJob() throws Exception {
        // Given entry node.
        Ignite entryNode = node(0);
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
        // Given table with replicas == 3 and partitions == 1.
        createReplicatedTestTableWithOneRow();
        // And partition leader for K=1.
        ClusterNode primaryReplica = getPrimaryReplica(cluster.node(0));

        // When start colocated job on node that is not primary replica.
        Ignite entryNode = anyNodeExcept(primaryReplica);
        TestingJobExecution<Object> execution = new TestingJobExecution<>(
                compute(entryNode).submit(
                        JobTarget.colocated(TABLE_NAME, Tuple.create(1).set("K", 1)),
                        JobDescriptor.builder(InteractiveJobs.globalJob().name()).build(),
                        null));

        // Then the job is alive.
        InteractiveJobs.globalJob().assertAlive();
        execution.assertExecuting();

        // And it is running on primary replica node.
        String firstWorkerNodeName = InteractiveJobs.globalJob().currentWorkerName();
        assertThat(firstWorkerNodeName, equalTo(primaryReplica.name()));

        // When stop worker node.
        stopNode(primaryReplica);

        // Then the job is restarted on another node.
        InteractiveJobs.globalJob().assertAlive();
        execution.assertExecuting();

        // And it is running on another node.
        String failoverNodeName = InteractiveJobs.globalJob().currentWorkerName();
        assertThat(failoverNodeName, in(allNodeNames()));
        // And this node is the primary replica for K=1.
        primaryReplica = getPrimaryReplica(entryNode);
        assertThat(failoverNodeName, equalTo(primaryReplica.name()));
        // But this is not the same node as before.
        assertThat(failoverNodeName, not(equalTo(firstWorkerNodeName)));
    }

    private ClusterNode getPrimaryReplica(Ignite node) {
        IgniteImpl igniteImpl = unwrapIgniteImpl(node);

        try {
            HybridClock clock = igniteImpl.clock();
            TableImpl table = unwrapTableImpl(node.tables().table(TABLE_NAME));
            TablePartitionId tablePartitionId = new TablePartitionId(table.tableId(), table.partition(Tuple.create(1).set("K", 1)));

            ReplicaMeta replicaMeta = igniteImpl.placementDriver().getPrimaryReplica(tablePartitionId, clock.now()).get();
            if (replicaMeta == null || replicaMeta.getLeaseholder() == null) {
                throw new RuntimeException("Can not find primary replica for partition.");
            }

            return unwrapIgniteImpl(nodeByName(replicaMeta.getLeaseholder())).node();

        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    private void stopNode(ClusterNode clusterNode) {
        stopNode(clusterNode.name());
    }

    private void stopNode(Ignite ignite) {
        stopNode(ignite.name());
    }

    private Ignite anyNodeExcept(ClusterNode except) {
        String candidateName = allNodeNames()
                .stream()
                .filter(name -> !name.equals(except.name()))
                .findFirst()
                .orElseThrow();

        return nodeByName(candidateName);
    }

    private Ignite nodeByName(String candidateName) {
        return cluster.runningNodes().filter(node -> node.name().equals(candidateName)).findFirst().orElseThrow();
    }

    private TestingJobExecution<String> executeGlobalInteractiveJob(Ignite entryNode, Set<String> nodes) {
        return new TestingJobExecution<>(
                compute(entryNode).submit(
                        JobTarget.anyNode(clusterNodesByNames(nodes)),
                        JobDescriptor.builder(InteractiveJobs.globalJob().jobClass()).build(), null)
        );
    }

    abstract IgniteCompute compute(Ignite entryNode);

    private void createReplicatedTestTableWithOneRow() {
        // Number of replicas == number of nodes and number of partitions == 1. This gives us the majority on primary replica stop.
        // After the primary replica is stopped we still be able to select new primary replica selected.
        executeSql("CREATE ZONE TEST_ZONE WITH REPLICAS=3, PARTITIONS=1, STORAGE_PROFILES='" + DEFAULT_STORAGE_PROFILE + "'");
        executeSql("CREATE TABLE test (k int, v int, CONSTRAINT PK PRIMARY KEY (k)) WITH PRIMARY_ZONE='TEST_ZONE'");
        executeSql("INSERT INTO test(k, v) VALUES (1, 101)");
    }

    private static List<String> allNodeNames() {
        return new ArrayList<>(NODES_NAMES_TO_INDEXES.keySet());
    }
}
