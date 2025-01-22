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
import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willBe;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willSucceedFast;
import static org.apache.ignite.internal.testframework.matchers.JobExecutionMatcher.jobExecutionWithResultAndNode;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.apache.ignite.internal.util.CompletableFutures.trueCompletedFuture;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.compute.BroadcastExecution;
import org.apache.ignite.compute.BroadcastJobTarget;
import org.apache.ignite.compute.JobDescriptor;
import org.apache.ignite.compute.JobExecution;
import org.apache.ignite.compute.JobExecutionOptions;
import org.apache.ignite.compute.JobState;
import org.apache.ignite.compute.JobTarget;
import org.apache.ignite.deployment.DeploymentUnit;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologyService;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.network.ClusterNodeImpl;
import org.apache.ignite.internal.network.TopologyService;
import org.apache.ignite.internal.placementdriver.PlacementDriver;
import org.apache.ignite.internal.placementdriver.ReplicaMeta;
import org.apache.ignite.internal.table.IgniteTablesInternal;
import org.apache.ignite.internal.table.TableViewInternal;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.lang.CancelHandle;
import org.apache.ignite.lang.CancellationToken;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.NetworkAddress;
import org.apache.ignite.table.QualifiedName;
import org.apache.ignite.table.Tuple;
import org.apache.ignite.table.mapper.Mapper;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class IgniteComputeImplTest extends BaseIgniteAbstractTest {
    private static final String JOB_CLASS_NAME = "org.example.SimpleJob";

    @Mock
    private TopologyService topologyService;

    @Mock
    private IgniteTablesInternal igniteTables;

    @Mock
    private ComputeComponentImpl computeComponent;

    @Mock
    private LogicalTopologyService logicalTopologyService;

    @Mock
    private PlacementDriver placementDriver;

    @Mock
    private HybridClock clock;

    @InjectMocks
    private IgniteComputeImpl compute;

    @Mock
    private TableViewInternal table;

    private final ClusterNode localNode = new ClusterNodeImpl(randomUUID(), "local", new NetworkAddress("local-host", 1));

    private final ClusterNode remoteNode = new ClusterNodeImpl(randomUUID(), "remote", new NetworkAddress("remote-host", 1));

    private final List<DeploymentUnit> testDeploymentUnits = List.of(new DeploymentUnit("test", "1.0.0"));

    @BeforeEach
    void setupMocks() {
        lenient().when(topologyService.localMember()).thenReturn(localNode);
        lenient().when(topologyService.getByConsistentId(localNode.name())).thenReturn(localNode);
        lenient().when(topologyService.getByConsistentId(remoteNode.name())).thenReturn(remoteNode);
    }

    @Test
    void whenNodeIsLocalThenExecutesLocally() {
        respondWhenExecutingSimpleJobLocally(ExecutionOptions.DEFAULT);

        assertThat(
                compute.executeAsync(
                        JobTarget.node(localNode),
                        JobDescriptor.builder(JOB_CLASS_NAME).units(testDeploymentUnits).build(),
                        null),
                willBe("jobResponse")
        );

        verify(computeComponent).executeLocally(ExecutionOptions.DEFAULT, testDeploymentUnits, JOB_CLASS_NAME, null, null);
    }

    @Test
    void safeCallCancelHandleAfterJobProcessing() {
        CancelHandle cancelHandle = CancelHandle.create();

        respondWhenExecutingSimpleJobLocally(ExecutionOptions.DEFAULT, cancelHandle.token());

        assertThat(
                compute.executeAsync(
                        JobTarget.node(localNode),
                        JobDescriptor.builder(JOB_CLASS_NAME).units(testDeploymentUnits).build(),
                        null,
                        cancelHandle.token()
                ),
                willBe("jobResponse")
        );

        assertFalse(cancelHandle.isCancelled());
        cancelHandle.cancel();
        assertThat(cancelHandle.cancelAsync(), willSucceedFast());
    }

    @Test
    void whenNodeIsLocalAndIdIsChangedThenExecutesLocally() {
        respondWhenExecutingSimpleJobLocally(ExecutionOptions.DEFAULT);

        // Imitate node restart by changing the id.
        ClusterNode newNode = new ClusterNodeImpl(randomUUID(), localNode.name(), localNode.address());

        assertThat(
                compute.executeAsync(
                        JobTarget.node(newNode),
                        JobDescriptor.builder(JOB_CLASS_NAME).units(testDeploymentUnits).build(),
                        null),
                willBe("jobResponse")
        );

        verify(computeComponent).executeLocally(ExecutionOptions.DEFAULT, testDeploymentUnits, JOB_CLASS_NAME, null, null);
    }

    @Test
    void whenNodeIsRemoteThenExecutesRemotely() {
        respondWhenExecutingSimpleJobRemotely(ExecutionOptions.DEFAULT);

        assertThat(
                compute.executeAsync(
                        JobTarget.node(remoteNode),
                        JobDescriptor.builder(JOB_CLASS_NAME).units(testDeploymentUnits).build(),
                        "a"),
                willBe("remoteResponse")
        );

        verifyExecuteRemotelyWithFailover(ExecutionOptions.DEFAULT);
    }

    @Test
    void whenNodeIsLocalThenExecutesLocallyWithOptions() {
        ExecutionOptions expectedOptions = ExecutionOptions.builder().priority(1).maxRetries(2).build();
        respondWhenExecutingSimpleJobLocally(expectedOptions);

        JobExecutionOptions options = JobExecutionOptions.builder().priority(1).maxRetries(2).build();
        assertThat(
                compute.executeAsync(
                        JobTarget.node(localNode),
                        JobDescriptor.builder(JOB_CLASS_NAME).units(testDeploymentUnits).options(options).build(),
                        null),
                willBe("jobResponse")
        );

        verify(computeComponent).executeLocally(expectedOptions, testDeploymentUnits, JOB_CLASS_NAME, null, null);
    }

    @Test
    void whenNodeIsRemoteThenExecutesRemotelyWithOptions() {
        ExecutionOptions expectedOptions = ExecutionOptions.builder().priority(1).maxRetries(2).build();
        respondWhenExecutingSimpleJobRemotely(expectedOptions);

        JobExecutionOptions options = JobExecutionOptions.builder().priority(1).maxRetries(2).build();

        assertThat(
                compute.executeAsync(
                        JobTarget.node(remoteNode),
                        JobDescriptor.builder(JOB_CLASS_NAME).units(testDeploymentUnits).options(options).build(),
                        "a"),
                willBe("remoteResponse")
        );

        verifyExecuteRemotelyWithFailover(expectedOptions);
    }

    @Test
    void executesColocatedOnLeaderNodeOfPartitionCorrespondingToTupleKey() {
        respondWhenExecutingSimpleJobRemotely(ExecutionOptions.DEFAULT);
        respondWhenAskForPrimaryReplica();

        assertThat(
                compute.executeAsync(
                        JobTarget.colocated("test", Tuple.create(Map.of("k", 1))),
                        JobDescriptor.builder(JOB_CLASS_NAME).units(testDeploymentUnits).build(),
                        "a"),
                willBe("remoteResponse")
        );
    }

    @Test
    void executesColocatedOnLeaderNodeOfPartitionCorrespondingToMappedKey() {
        respondWhenExecutingSimpleJobRemotely(ExecutionOptions.DEFAULT);
        respondWhenAskForPrimaryReplica();

        assertThat(
                compute.executeAsync(
                        JobTarget.colocated("test", 1, Mapper.of(Integer.class)),
                        JobDescriptor.builder(JOB_CLASS_NAME).units(testDeploymentUnits).build(),
                        "a"
                ),
                willBe("remoteResponse")
        );
    }

    @Test
    void executeBroadcastAsync() {
        respondWhenExecutingSimpleJobLocally(ExecutionOptions.DEFAULT);
        respondWhenExecutingSimpleJobRemotely(ExecutionOptions.DEFAULT);

        CompletableFuture<BroadcastExecution<String>> future = compute.submitAsync(
                BroadcastJobTarget.nodes(localNode, remoteNode),
                JobDescriptor.<String, String>builder(JOB_CLASS_NAME).units(testDeploymentUnits).build(),
                null
        );
        assertThat(future, willCompleteSuccessfully());
        Collection<JobExecution<String>> executions = future.join().executions();

        assertThat(executions, containsInAnyOrder(
                jobExecutionWithResultAndNode("jobResponse", localNode),
                jobExecutionWithResultAndNode("remoteResponse", remoteNode)
        ));
    }

    private void respondWhenAskForPrimaryReplica() {
        when(igniteTables.tableViewAsync(eq(QualifiedName.fromSimple("TEST")))).thenReturn(completedFuture(table));
        ReplicaMeta replicaMeta = mock(ReplicaMeta.class);
        doReturn(randomUUID()).when(replicaMeta).getLeaseholderId();
        CompletableFuture<ReplicaMeta> toBeReturned = completedFuture(replicaMeta);
        doReturn(toBeReturned).when(placementDriver).awaitPrimaryReplica(any(), any(), anyLong(), any());
        doReturn(remoteNode).when(topologyService).getById(any());
    }

    private void respondWhenExecutingSimpleJobLocally(ExecutionOptions executionOptions) {
        when(computeComponent.executeLocally(executionOptions, testDeploymentUnits, JOB_CLASS_NAME, null, null))
                .thenReturn(completedExecution(SharedComputeUtils.marshalArgOrResult("jobResponse", null), localNode));
    }

    private void respondWhenExecutingSimpleJobLocally(ExecutionOptions executionOptions, CancellationToken token) {
        when(computeComponent.executeLocally(executionOptions, testDeploymentUnits, JOB_CLASS_NAME, null, token))
                .thenReturn(completedExecution(SharedComputeUtils.marshalArgOrResult("jobResponse", null), localNode));
    }

    private void respondWhenExecutingSimpleJobRemotely(ExecutionOptions options) {
        when(computeComponent.executeRemotelyWithFailover(
                eq(remoteNode), any(), eq(testDeploymentUnits), eq(JOB_CLASS_NAME), eq(options), any(), isNull()
        )).thenReturn(completedExecution(SharedComputeUtils.marshalArgOrResult("remoteResponse", null), remoteNode));
    }

    private void verifyExecuteRemotelyWithFailover(ExecutionOptions options) {
        verify(computeComponent).executeRemotelyWithFailover(
                eq(remoteNode), any(), eq(testDeploymentUnits), eq(JOB_CLASS_NAME), eq(options), any(), isNull()
        );
    }

    private static <R> CancellableJobExecution<R> completedExecution(@Nullable R result, ClusterNode node) {
        return new CancellableJobExecution<>() {
            @Override
            public CompletableFuture<R> resultAsync() {
                return completedFuture(result);
            }

            @Override
            public CompletableFuture<@Nullable JobState> stateAsync() {
                return nullCompletedFuture();
            }

            @Override
            public CompletableFuture<@Nullable Boolean> cancelAsync() {
                return trueCompletedFuture();
            }

            @Override
            public CompletableFuture<@Nullable Boolean> changePriorityAsync(int newPriority) {
                return nullCompletedFuture();
            }

            @Override
            public ClusterNode node() {
                return node;
            }
        };
    }
}
