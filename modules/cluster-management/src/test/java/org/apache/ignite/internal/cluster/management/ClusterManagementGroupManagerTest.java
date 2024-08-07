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

package org.apache.ignite.internal.cluster.management;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.cluster.management.configuration.ClusterManagementConfiguration;
import org.apache.ignite.internal.cluster.management.network.messages.CmgInitMessage;
import org.apache.ignite.internal.cluster.management.network.messages.CmgMessagesFactory;
import org.apache.ignite.internal.cluster.management.network.messages.InitErrorMessage;
import org.apache.ignite.internal.cluster.management.raft.ClusterStateStorage;
import org.apache.ignite.internal.cluster.management.raft.commands.InitCmgStateCommand;
import org.apache.ignite.internal.cluster.management.topology.LogicalTopology;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.failure.FailureProcessor;
import org.apache.ignite.internal.lang.NodeStoppingException;
import org.apache.ignite.internal.manager.ComponentContext;
import org.apache.ignite.internal.network.ClusterService;
import org.apache.ignite.internal.network.NetworkMessage;
import org.apache.ignite.internal.network.StaticNodeFinder;
import org.apache.ignite.internal.network.utils.ClusterServiceTestUtils;
import org.apache.ignite.internal.raft.RaftManager;
import org.apache.ignite.internal.raft.service.RaftGroupService;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.vault.VaultManager;
import org.apache.ignite.network.NetworkAddress;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
@ExtendWith(ConfigurationExtension.class)
class ClusterManagementGroupManagerTest extends BaseIgniteAbstractTest {
    private ClusterService clusterService;

    private ClusterManagementGroupManager cmgManager;

    private final ComponentContext componentContext = new ComponentContext();

    private final CmgMessagesFactory cmgMessagesFactory = new CmgMessagesFactory();

    @BeforeEach
    void setUp(
            TestInfo testInfo,
            @Mock VaultManager vaultManager,
            @Mock ClusterInitializer clusterInitializer,
            @Mock RaftManager raftManager,
            @Mock ClusterStateStorage clusterStateStorage,
            @Mock LogicalTopology logicalTopology,
            @InjectConfiguration ClusterManagementConfiguration clusterManagementConfiguration,
            @Mock NodeAttributes nodeAttributes,
            @Mock FailureProcessor failureProcessor,
            @Mock RaftGroupService raftGroupService
    ) throws NodeStoppingException {
        var addr = new NetworkAddress("localhost", 10_000);

        clusterService = ClusterServiceTestUtils.clusterService(testInfo, addr.port(), new StaticNodeFinder(List.of(addr)));

        when(raftManager.startRaftGroupNodeAndWaitNodeReadyFuture(any(), any(), any(), any(), any()))
                .thenReturn(completedFuture(raftGroupService));

        ClusterState clusterState = cmgMessagesFactory.clusterState()
                .clusterTag(cmgMessagesFactory.clusterTag().clusterId(UUID.randomUUID()).clusterName("foo").build())
                .cmgNodes(Set.of(clusterService.nodeName()))
                .metaStorageNodes(Set.of(clusterService.nodeName()))
                .version("foo")
                .build();

        when(raftGroupService.run(any()))
                .thenReturn(nullCompletedFuture());

        when(raftGroupService.run(any(InitCmgStateCommand.class)))
                .thenReturn(completedFuture(clusterState));

        cmgManager = new ClusterManagementGroupManager(
                vaultManager,
                clusterService,
                clusterInitializer,
                raftManager,
                clusterStateStorage,
                logicalTopology,
                clusterManagementConfiguration,
                nodeAttributes,
                failureProcessor,
                new ClusterIdHolder(),
                options -> {}
        );

        assertThat(clusterService.startAsync(componentContext), willCompleteSuccessfully());
    }

    @AfterEach
    void tearDown() throws Exception {
        IgniteUtils.closeAll(
                () -> cmgManager.beforeNodeStop(),
                () -> clusterService.beforeNodeStop()
        );

        assertThat(IgniteUtils.stopAsync(componentContext, cmgManager, clusterService), willCompleteSuccessfully());
    }

    @Test
    void cmgManagerDoesNotMissMessagesReceivedBeforeStart() {
        CmgInitMessage initMessage = cmgMessagesFactory.cmgInitMessage()
                .clusterName("foo")
                .clusterId(UUID.randomUUID())
                .cmgNodes(Set.of(clusterService.nodeName()))
                .metaStorageNodes(Set.of(clusterService.nodeName()))
                .initialClusterConfiguration("")
                .build();

        CompletableFuture<NetworkMessage> invokeFuture = clusterService.messagingService()
                .invoke(clusterService.nodeName(), initMessage, 10_000);

        assertThat(cmgManager.startAsync(componentContext), willCompleteSuccessfully());

        assertThat(invokeFuture, willCompleteSuccessfully());

        NetworkMessage response = invokeFuture.join();

        if (response instanceof InitErrorMessage) {
            fail(((InitErrorMessage) response).cause());
        }
    }
}
