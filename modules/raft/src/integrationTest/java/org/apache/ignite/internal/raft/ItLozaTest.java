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
import static org.hamcrest.Matchers.containsString;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.manager.ComponentContext;
import org.apache.ignite.internal.metrics.NoOpMetricManager;
import org.apache.ignite.internal.network.ClusterService;
import org.apache.ignite.internal.network.MessagingService;
import org.apache.ignite.internal.network.NetworkMessage;
import org.apache.ignite.internal.network.StaticNodeFinder;
import org.apache.ignite.internal.network.utils.ClusterServiceTestUtils;
import org.apache.ignite.internal.raft.configuration.RaftConfiguration;
import org.apache.ignite.internal.raft.service.RaftGroupListener;
import org.apache.ignite.internal.raft.service.RaftGroupService;
import org.apache.ignite.internal.replicator.TestReplicationGroupId;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.NetworkAddress;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Tests for {@link Loza} functionality.
 */
@ExtendWith(WorkDirectoryExtension.class)
@ExtendWith(ConfigurationExtension.class)
public class ItLozaTest extends BaseIgniteAbstractTest {
    /** Server port offset. */
    private static final int PORT = 20010;

    @WorkDirectory
    private Path dataPath;

    @InjectConfiguration
    private static RaftConfiguration raftConfiguration;

    /**
     * Starts a raft group service with a provided group id on a provided Loza instance.
     *
     * @return Raft group service.
     */
    private RaftGroupService startClient(TestReplicationGroupId groupId, ClusterNode node, Loza loza) throws Exception {
        RaftGroupListener raftGroupListener = mock(RaftGroupListener.class);

        when(raftGroupListener.onSnapshotLoad(any())).thenReturn(true);

        PeersAndLearners configuration = PeersAndLearners.fromConsistentIds(Set.of(node.name()));

        var nodeId = new RaftNodeId(groupId, configuration.peer(node.name()));

        return loza.startRaftGroupNodeAndWaitNodeReadyFuture(nodeId, configuration, raftGroupListener, RaftGroupEventsListener.noopLsnr)
                .get(10, TimeUnit.SECONDS);
    }

    /**
     * Returns the client cluster view.
     *
     * @param testInfo Test info.
     * @param port     Local port.
     * @param srvs     Server nodes of the cluster.
     * @return The client cluster view.
     */
    private static ClusterService clusterService(TestInfo testInfo, int port, List<NetworkAddress> srvs) {
        var network = ClusterServiceTestUtils.clusterService(testInfo, port, new StaticNodeFinder(srvs));

        assertThat(network.startAsync(new ComponentContext()), willCompleteSuccessfully());

        return network;
    }

    /**
     * Tests that RaftGroupServiceImpl uses shared executor for retrying RaftGroupServiceImpl#sendWithRetry().
     */
    @Test
    public void testRaftServiceUsingSharedExecutor(TestInfo testInfo) throws Exception {
        ClusterService service = null;

        Loza loza = null;

        RaftGroupService[] grpSrvcs = new RaftGroupService[5];

        try {
            service = spy(clusterService(testInfo, PORT, List.of()));

            MessagingService messagingServiceMock = spy(service.messagingService());

            when(service.messagingService()).thenReturn(messagingServiceMock);

            CompletableFuture<NetworkMessage> exception = CompletableFuture.failedFuture(new IOException());

            loza = new Loza(service, new NoOpMetricManager(), raftConfiguration, dataPath, new HybridClockImpl());

            assertThat(loza.startAsync(new ComponentContext()), willCompleteSuccessfully());

            for (int i = 0; i < grpSrvcs.length; i++) {
                // return an error on first invocation
                doReturn(exception)
                        // assert that a retry has been issued on the executor
                        .doAnswer(invocation -> {
                            assertThat(Thread.currentThread().getName(), containsString(Loza.CLIENT_POOL_NAME));

                            return exception;
                        })
                        // finally call the real method
                        .doCallRealMethod()
                        .when(messagingServiceMock).invoke(any(ClusterNode.class), any(), anyLong());

                grpSrvcs[i] = startClient(new TestReplicationGroupId(Integer.toString(i)), service.topologyService().localMember(), loza);

                verify(messagingServiceMock, times(3 * (i + 1)))
                        .invoke(any(ClusterNode.class), any(), anyLong());
            }
        } finally {
            for (RaftGroupService srvc : grpSrvcs) {
                srvc.shutdown();

                loza.stopRaftNodes(srvc.groupId());
            }

            if (loza != null) {
                assertThat(loza.stopAsync(new ComponentContext()), willCompleteSuccessfully());
            }

            if (service != null) {
                assertThat(service.stopAsync(new ComponentContext()), willCompleteSuccessfully());
            }
        }
    }
}
