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

package org.apache.ignite.internal.raft;

import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

import java.util.Set;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.lang.NodeStoppingException;
import org.apache.ignite.internal.network.ClusterService;
import org.apache.ignite.internal.network.MessagingService;
import org.apache.ignite.internal.raft.configuration.RaftConfiguration;
import org.apache.ignite.internal.replicator.TestReplicationGroupId;
import org.apache.ignite.internal.testframework.IgniteAbstractTest;
import org.apache.ignite.network.TopologyService;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * There are tests for RAFT manager.
 * It is mocking all components except Loza and checks API methods of the component in various conditions.
 */
@ExtendWith(MockitoExtension.class)
@ExtendWith(ConfigurationExtension.class)
public class LozaTest extends IgniteAbstractTest {
    /** Mock for network service. */
    @Mock
    private ClusterService clusterNetSvc;

    @InjectConfiguration
    private RaftConfiguration raftConfiguration;

    /**
     * Checks that the all API methods throw the exception ({@link NodeStoppingException})
     * when Loza is closed.
     *
     * @throws Exception If fail.
     */
    @Test
    public void testLozaStop() throws Exception {
        Mockito.doReturn("test_node").when(clusterNetSvc).nodeName();
        Mockito.doReturn(mock(MessagingService.class)).when(clusterNetSvc).messagingService();
        Mockito.doReturn(mock(TopologyService.class)).when(clusterNetSvc).topologyService();

        Loza loza = TestLozaFactory.create(clusterNetSvc, raftConfiguration, workDir, new HybridClockImpl());

        assertThat(loza.startAsync(), willCompleteSuccessfully());

        loza.beforeNodeStop();
        assertThat(loza.stopAsync(), willCompleteSuccessfully());

        TestReplicationGroupId raftGroupId = new TestReplicationGroupId("test_raft_group");

        PeersAndLearners configuration = PeersAndLearners.fromConsistentIds(Set.of("test1"));

        Peer serverPeer = configuration.peer("test1");

        assertThrows(
                NodeStoppingException.class,
                () -> loza.startRaftGroupNodeAndWaitNodeReadyFuture(new RaftNodeId(raftGroupId, serverPeer), configuration, null, null)
        );
        assertThrows(NodeStoppingException.class, () -> loza.startRaftGroupService(raftGroupId, configuration));
        assertThrows(NodeStoppingException.class, () -> loza.stopRaftNode(new RaftNodeId(raftGroupId, serverPeer)));
        assertThrows(NodeStoppingException.class, () -> loza.stopRaftNodes(raftGroupId));
    }
}
