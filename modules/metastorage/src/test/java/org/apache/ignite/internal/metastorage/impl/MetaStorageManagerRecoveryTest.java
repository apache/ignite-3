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
import static org.apache.ignite.internal.hlc.TestClockService.TEST_MAX_CLOCK_SKEW_MILLIS;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willSucceedFast;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.cluster.management.ClusterManagementGroupManager;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologyService;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.manager.ComponentContext;
import org.apache.ignite.internal.metastorage.command.GetCurrentRevisionCommand;
import org.apache.ignite.internal.metastorage.configuration.MetaStorageConfiguration;
import org.apache.ignite.internal.metastorage.server.KeyValueStorage;
import org.apache.ignite.internal.metastorage.server.SimpleInMemoryKeyValueStorage;
import org.apache.ignite.internal.metrics.NoOpMetricManager;
import org.apache.ignite.internal.network.ClusterService;
import org.apache.ignite.internal.network.MessagingService;
import org.apache.ignite.internal.network.serialization.MessageSerializationRegistry;
import org.apache.ignite.internal.raft.RaftManager;
import org.apache.ignite.internal.raft.client.TopologyAwareRaftGroupServiceFactory;
import org.apache.ignite.internal.raft.service.RaftGroupService;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.network.NodeMetadata;
import org.apache.ignite.network.TopologyService;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/** Tests MetaStorage manager recovery basics. */
@ExtendWith(ConfigurationExtension.class)
public class MetaStorageManagerRecoveryTest extends BaseIgniteAbstractTest {
    private static final String NODE_NAME = "node";

    private static final String LEADER_NAME = "ms-leader";

    @InjectConfiguration
    private static MetaStorageConfiguration metaStorageConfiguration;

    private MetaStorageManagerImpl metaStorageManager;

    private KeyValueStorage kvs;

    private HybridClock clock;

    private void createMetaStorage(long remoteRevision) throws Exception {
        ClusterService clusterService = clusterService();
        ClusterManagementGroupManager cmgManager = clusterManagementManager();
        LogicalTopologyService topologyService = mock(LogicalTopologyService.class);
        RaftManager raftManager = raftManager(remoteRevision);

        clock = new HybridClockImpl();
        kvs = spy(new SimpleInMemoryKeyValueStorage(NODE_NAME));

        metaStorageManager = new MetaStorageManagerImpl(
                clusterService,
                cmgManager,
                topologyService,
                raftManager,
                kvs,
                clock,
                mock(TopologyAwareRaftGroupServiceFactory.class),
                new NoOpMetricManager(),
                metaStorageConfiguration,
                completedFuture(() -> TEST_MAX_CLOCK_SKEW_MILLIS)
        );
    }

    private RaftManager raftManager(long remoteRevision) throws Exception {
        RaftManager raft = mock(RaftManager.class);

        RaftGroupService service = mock(RaftGroupService.class);

        when(service.run(any(GetCurrentRevisionCommand.class)))
                .thenAnswer(invocation -> completedFuture(remoteRevision));

        when(raft.startRaftGroupNodeAndWaitNodeReadyFuture(any(), any(), any(), any(), any()))
                .thenAnswer(invocation -> completedFuture(service));

        return raft;
    }

    private ClusterService clusterService() {
        return new ClusterService() {
            @Override
            public String nodeName() {
                return "node";
            }

            @Override
            public TopologyService topologyService() {
                return null;
            }

            @Override
            public MessagingService messagingService() {
                return null;
            }

            @Override
            public MessageSerializationRegistry serializationRegistry() {
                return null;
            }

            @Override
            public boolean isStopped() {
                return false;
            }

            @Override
            public void updateMetadata(NodeMetadata metadata) {
            }

            @Override
            public CompletableFuture<Void> startAsync(ComponentContext componentContext) {
                return nullCompletedFuture();
            }
        };
    }

    private static ClusterManagementGroupManager clusterManagementManager() {
        ClusterManagementGroupManager mock = mock(ClusterManagementGroupManager.class);

        when(mock.metaStorageNodes())
                .thenAnswer(invocation -> completedFuture(Set.of(LEADER_NAME)));

        return mock;
    }

    @Test
    void testRecoverToRevision() throws Exception {
        long targetRevision = 10;

        createMetaStorage(targetRevision);

        assertThat(metaStorageManager.startAsync(new ComponentContext()), willCompleteSuccessfully());

        CompletableFuture<Void> msDeployFut = metaStorageManager.deployWatches();

        for (int i = 0; i < targetRevision; i++) {
            kvs.put(new byte[0], new byte[0], clock.now());
        }

        assertThat(msDeployFut, willSucceedFast());

        // MetaStorage recovered to targetRevision and started watching targetRevision + 1.
        verify(kvs).startWatches(eq(targetRevision + 1), any());
    }

    @Test
    void testRecoverClean() throws Exception {
        createMetaStorage(0);

        assertThat(metaStorageManager.startAsync(new ComponentContext()), willCompleteSuccessfully());

        CompletableFuture<Void> msDeployFut = metaStorageManager.deployWatches();

        assertThat(msDeployFut, willSucceedFast());

        // MetaStorage is at revision 0 and started watching revision 1.
        verify(kvs).startWatches(eq(1L), any());
    }
}
