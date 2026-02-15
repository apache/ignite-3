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

package org.apache.ignite.internal.cluster.management.raft;

import static java.util.UUID.randomUUID;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Map;
import org.apache.ignite.internal.cluster.management.NodeAttributes;
import org.apache.ignite.internal.cluster.management.topology.LogicalTopology;
import org.apache.ignite.internal.network.ClusterNodeImpl;
import org.apache.ignite.internal.network.InternalClusterNode;
import org.apache.ignite.internal.network.TopologyService;
import org.apache.ignite.internal.raft.service.RaftCommandRunner;
import org.apache.ignite.internal.raft.service.TimeAwareRaftGroupService;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.network.NetworkAddress;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class CmgRaftServiceTest extends BaseIgniteAbstractTest {
    @Mock
    private TimeAwareRaftGroupService raftGroupService;

    @Mock
    private TopologyService topologyService;

    @SuppressWarnings("unused") // It's automatically injected to CmgRaftService.
    @Mock
    private LogicalTopology logicalTopology;

    @InjectMocks
    private CmgRaftService cmgRaftService;

    private final InternalClusterNode localNode = new ClusterNodeImpl(randomUUID(), "local", new NetworkAddress("host", 3000));

    @Test
    void joinReadyCommandIsExecutedWithoutTimeout() {
        when(topologyService.localMember()).thenReturn(localNode);
        when(raftGroupService.run(any(), anyLong())).thenReturn(nullCompletedFuture());

        assertThat(cmgRaftService.completeJoinCluster(new EmptyNodeAttributes()), willCompleteSuccessfully());

        verify(raftGroupService).run(any(), eq(RaftCommandRunner.NO_TIMEOUT));
    }

    private static class EmptyNodeAttributes implements NodeAttributes {
        @Override
        public Map<String, String> userAttributes() {
            return Map.of();
        }

        @Override
        public Map<String, String> systemAttributes() {
            return Map.of();
        }

        @Override
        public List<String> storageProfiles() {
            return List.of();
        }
    }
}
