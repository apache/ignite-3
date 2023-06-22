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

import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willSucceedFast;
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
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.metastorage.command.GetCurrentRevisionCommand;
import org.apache.ignite.internal.metastorage.server.KeyValueStorage;
import org.apache.ignite.internal.metastorage.server.SimpleInMemoryKeyValueStorage;
import org.apache.ignite.internal.raft.RaftManager;
import org.apache.ignite.internal.raft.service.RaftGroupService;
import org.apache.ignite.internal.vault.VaultManager;
import org.apache.ignite.internal.vault.inmemory.InMemoryVaultService;
import org.apache.ignite.network.ClusterService;
import org.apache.ignite.network.MessagingService;
import org.apache.ignite.network.NodeMetadata;
import org.apache.ignite.network.TopologyService;
import org.apache.ignite.network.serialization.MessageSerializationRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Tests MetaStorage manager recovery basics. */
public class MetaStorageManagerRecoveryTest {
    private static final String NODE_NAME = "node";

    private static final String LEADER_NAME = "ms-leader";

    private static final long TARGET_REVISION = 10;

    private MetaStorageManagerImpl metaStorageManager;

    private KeyValueStorage kvs;

    private HybridClock clock;

    @BeforeEach
    void setUp() throws Exception {
        VaultManager vault = new VaultManager(new InMemoryVaultService());
        ClusterService clusterService = cs();
        ClusterManagementGroupManager cmgManager = cmg();
        LogicalTopologyService topologyService = mock(LogicalTopologyService.class);
        RaftManager raftManager = raftManager();

        clock = new HybridClockImpl();
        kvs = spy(new SimpleInMemoryKeyValueStorage(NODE_NAME));

        metaStorageManager = new MetaStorageManagerImpl(
                vault,
                clusterService,
                cmgManager,
                topologyService,
                raftManager,
                kvs,
                clock
        );
    }

    private RaftManager raftManager() throws Exception {
        RaftManager raft = mock(RaftManager.class);

        RaftGroupService service = mock(RaftGroupService.class);

        when(service.run(any(GetCurrentRevisionCommand.class))).thenAnswer(invocation -> {
            return CompletableFuture.completedFuture(TARGET_REVISION);
        });

        when(raft.startRaftGroupNodeAndWaitNodeReadyFuture(any(), any(), any(), any(), any()))
                .thenAnswer(invocation -> CompletableFuture.completedFuture(service));

        return raft;
    }

    private ClusterService cs() {
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
            public void start() {

            }
        };
    }

    private ClusterManagementGroupManager cmg() {
        ClusterManagementGroupManager mock = mock(ClusterManagementGroupManager.class);

        when(mock.metaStorageNodes())
                .thenAnswer(invocation -> CompletableFuture.completedFuture(Set.of(LEADER_NAME)));

        return mock;
    }

    @Test
    void test() {
        metaStorageManager.start();

        CompletableFuture<Void> msDeployFut = metaStorageManager.deployWatches();

        for (int i = 0; i < TARGET_REVISION; i++) {
            kvs.put(new byte[0], new byte[0], clock.now());
        }

        assertThat(msDeployFut, willSucceedFast());

        verify(kvs).startWatches(eq(TARGET_REVISION + 1), any());
    }
}
