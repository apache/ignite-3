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

package org.apache.ignite.internal.metastorage.impl;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.apache.ignite.internal.metastorage.impl.StandaloneMetaStorageManager.configureCmgManagerToStartMetastorage;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Set;
import java.util.UUID;
import java.util.stream.Stream;
import org.apache.ignite.internal.cluster.management.ClusterManagementGroupManager;
import org.apache.ignite.internal.cluster.management.network.messages.CmgMessagesFactory;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologyService;
import org.apache.ignite.internal.configuration.SystemDistributedConfiguration;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.manager.ComponentContext;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.metastorage.command.GetCurrentRevisionsCommand;
import org.apache.ignite.internal.metastorage.command.response.RevisionsInfo;
import org.apache.ignite.internal.metastorage.server.ReadOperationForCompactionTracker;
import org.apache.ignite.internal.metastorage.server.SimpleInMemoryKeyValueStorage;
import org.apache.ignite.internal.metrics.NoOpMetricManager;
import org.apache.ignite.internal.network.ClusterService;
import org.apache.ignite.internal.network.InternalClusterNodeImpl;
import org.apache.ignite.internal.network.TopologyService;
import org.apache.ignite.internal.raft.RaftGroupOptionsConfigurer;
import org.apache.ignite.internal.raft.RaftManager;
import org.apache.ignite.internal.raft.client.TopologyAwareRaftGroupService;
import org.apache.ignite.internal.raft.client.TopologyAwareRaftGroupServiceFactory;
import org.apache.ignite.internal.testframework.IgniteAbstractTest;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Tests that check correctness of an invocation {@link MetaStorageManager#deployWatches()}.
 */
@ExtendWith(ConfigurationExtension.class)
public class MetaStorageDeployWatchesCorrectnessTest extends IgniteAbstractTest {
    @InjectConfiguration
    private static SystemDistributedConfiguration systemConfiguration;

    /**
     * Returns a stream with test arguments.
     *
     * @return Stream of different types of Meta storages to to check.
     * @throws Exception If failed.
     */
    private static Stream<MetaStorageManager> metaStorageProvider() throws Exception {
        HybridClock clock = new HybridClockImpl();
        String mcNodeName = "mc-node-1";

        ClusterManagementGroupManager cmgManager = mock(ClusterManagementGroupManager.class);
        ClusterService clusterService = mock(ClusterService.class);
        RaftManager raftManager = mock(RaftManager.class);
        TopologyAwareRaftGroupService raftGroupService = mock(TopologyAwareRaftGroupService.class);

        when(cmgManager.metaStorageInfo()).thenReturn(completedFuture(
                new CmgMessagesFactory().metaStorageInfo().metaStorageNodes(Set.of(mcNodeName)).build()
        ));
        configureCmgManagerToStartMetastorage(cmgManager);

        TopologyService topologyService = mock(TopologyService.class);
        when(topologyService.localMember()).thenReturn(new InternalClusterNodeImpl(UUID.randomUUID(), mcNodeName, null));

        when(clusterService.nodeName()).thenReturn(mcNodeName);
        when(clusterService.topologyService()).thenReturn(topologyService);
        when(raftManager.startSystemRaftGroupNodeAndWaitNodeReady(any(), any(), any(), any(), any(), any()))
                .thenReturn(raftGroupService);
        when(raftGroupService.run(any(GetCurrentRevisionsCommand.class), anyLong()))
                .thenAnswer(invocation -> completedFuture(new RevisionsInfo(0, -1)));

        var readOperationForCompactionTracker = new ReadOperationForCompactionTracker();

        return Stream.of(
                new MetaStorageManagerImpl(
                        clusterService,
                        cmgManager,
                        mock(LogicalTopologyService.class),
                        raftManager,
                        new SimpleInMemoryKeyValueStorage(mcNodeName, readOperationForCompactionTracker),
                        clock,
                        mock(TopologyAwareRaftGroupServiceFactory.class),
                        new NoOpMetricManager(),
                        systemConfiguration,
                        RaftGroupOptionsConfigurer.EMPTY,
                        readOperationForCompactionTracker
                ),
                StandaloneMetaStorageManager.create()
        );
    }

    /**
     * Invokes {@link MetaStorageManager#deployWatches()} and checks result.
     *
     * @param metastore Meta storage.
     */
    @ParameterizedTest
    @MethodSource("metaStorageProvider")
    public void testCheckCorrectness(MetaStorageManager metastore) {
        var deployWatchesFut = metastore.deployWatches();

        assertFalse(deployWatchesFut.isDone());

        assertThat(metastore.startAsync(new ComponentContext()), willCompleteSuccessfully());

        assertThat(deployWatchesFut, willCompleteSuccessfully());

        metastore.beforeNodeStop();

        assertThat(metastore.stopAsync(new ComponentContext()), willCompleteSuccessfully());
    }
}
