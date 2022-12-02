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

import static org.apache.ignite.internal.raft.server.RaftGroupOptions.defaults;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.raft.configuration.RaftConfiguration;
import org.apache.ignite.internal.raft.service.RaftGroupListener;
import org.apache.ignite.internal.replicator.ReplicationGroupId;
import org.apache.ignite.internal.testframework.IgniteAbstractTest;
import org.apache.ignite.lang.NodeStoppingException;
import org.apache.ignite.network.ClusterLocalConfiguration;
import org.apache.ignite.network.ClusterService;
import org.apache.ignite.network.MessagingService;
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
public class LozaTest extends IgniteAbstractTest {
    /** Mock for network service. */
    @Mock
    private ClusterService clusterNetSvc;

    /**
     * Checks that the all API methods throw the exception ({@link org.apache.ignite.lang.NodeStoppingException})
     * when Loza is closed.
     *
     * @throws Exception If fail.
     */
    @Test
    public void testLozaStop() throws Exception {
        Mockito.doReturn(new ClusterLocalConfiguration("test_node", null)).when(clusterNetSvc).localConfiguration();
        Mockito.doReturn(mock(MessagingService.class)).when(clusterNetSvc).messagingService();
        Mockito.doReturn(mock(TopologyService.class)).when(clusterNetSvc).topologyService();

        Loza loza = new Loza(clusterNetSvc, mock(RaftConfiguration.class), workDir, new HybridClockImpl());

        loza.start();

        loza.beforeNodeStop();
        loza.stop();

        TestReplicationGroupId raftGroupId = new TestReplicationGroupId("test_raft_group");

        List<String> nodes = List.of("test1");

        List<String> newNodes = List.of("test2", "test3");

        Supplier<RaftGroupListener> lsnrSupplier = () -> null;

        assertThrows(
                NodeStoppingException.class,
                () -> loza.startRaftGroupService(raftGroupId, newNodes, List.of())
        );
        assertThrows(NodeStoppingException.class, () -> loza.stopRaftGroup(raftGroupId));
        assertThrows(
                NodeStoppingException.class,
                () -> loza.prepareRaftGroup(raftGroupId, nodes, lsnrSupplier, defaults())
        );
    }

    /**
     * Test replication group id.
     */
    private static class TestReplicationGroupId implements ReplicationGroupId {
        private final String name;

        public TestReplicationGroupId(String name) {
            this.name = name;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            TestReplicationGroupId that = (TestReplicationGroupId) o;
            return Objects.equals(name, that.name);
        }

        @Override
        public int hashCode() {
            return Objects.hash(name);
        }

        @Override
        public String toString() {
            return name;
        }
    }
}
