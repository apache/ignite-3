/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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

import static org.apache.ignite.raft.jraft.test.TestUtils.waitForCondition;
import static org.apache.ignite.raft.jraft.test.TestUtils.waitForTopology;
import static org.apache.ignite.utils.ClusterServiceTestUtils.findLocalAddresses;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.ClusterService;
import org.apache.ignite.network.NetworkAddress;
import org.apache.ignite.network.StaticNodeFinder;
import org.apache.ignite.raft.client.Peer;
import org.apache.ignite.raft.client.service.RaftGroupListener;
import org.apache.ignite.raft.client.service.RaftGroupService;
import org.apache.ignite.utils.ClusterServiceTestUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Integration test methods of raft group service.
 */
@ExtendWith(WorkDirectoryExtension.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class ItRaftGroupServiceTest {
    @WorkDirectory
    private static Path workDir;

    private static final int NODES_CNT = 2;

    private static final int NODE_PORT_BASE = 20_000;

    private static final String RAFT_GROUP_NAME = "part1";

    private static List<ClusterService> clusterServices = new ArrayList<>();

    private static List<Loza> raftSrvs = new ArrayList<>();

    private static Map<ClusterNode, RaftGroupService> raftGroups = new HashMap<>();

    @BeforeAll
    public static void beforeAll(TestInfo testInfo) throws Exception {
        List<NetworkAddress> localAddresses = findLocalAddresses(NODE_PORT_BASE,
                NODE_PORT_BASE + NODES_CNT);

        var nodeFinder = new StaticNodeFinder(localAddresses);

        for (int i = 0; i < NODES_CNT; i++) {
            ClusterService clusterService = ClusterServiceTestUtils.clusterService(testInfo, NODE_PORT_BASE + i, nodeFinder);

            clusterServices.add(clusterService);

            clusterService.start();
        }

        assertTrue(waitForTopology(clusterServices.get(NODES_CNT - 1), NODES_CNT, 1000));

        List<ClusterNode> nodes = clusterServices.stream().map(cs -> cs.topologyService().localMember()).collect(Collectors.toList());

        CompletableFuture<RaftGroupService>[] svcFutures = new CompletableFuture[NODES_CNT];

        for (int i = 0; i < NODES_CNT; i++) {
            Loza raftServer = new Loza(clusterServices.get(i), workDir.resolve("node" + i));

            raftSrvs.add(raftServer);

            raftServer.start();

            CompletableFuture<RaftGroupService> raftGroupServiceFuture = raftSrvs.get(i).prepareRaftGroup(
                    RAFT_GROUP_NAME,
                    nodes,
                    () -> mock(RaftGroupListener.class)
            );

            svcFutures[i] = raftGroupServiceFuture;
        }

        CompletableFuture.allOf(svcFutures).get();

        for (int i = 0; i < NODES_CNT; i++) {
            raftGroups.put(clusterServices.get(i).topologyService().localMember(), svcFutures[i].get());
        }
    }

    @AfterAll
    public static void afterAll() throws Exception {
        raftGroups.values().forEach(RaftGroupService::shutdown);

        for (Loza raftSrv : raftSrvs) {
            raftSrv.stopRaftGroup(RAFT_GROUP_NAME);
            raftSrv.stop();
        }

        clusterServices.stream().forEach(ClusterService::stop);
    }

    @Test
    @Timeout(20)
    public void testTransferLeadership() throws Exception {
        RaftGroupService raftGroupService = raftGroups.get(clusterServices.get(0).topologyService().localMember());

        while (raftGroupService.leader() == null) {
            raftGroupService.refreshLeader().get();
        }

        ClusterNode oldLeaderNode = raftGroups.keySet().stream()
                .filter(clusterNode -> new Peer(clusterNode.address()).equals(raftGroupService.leader())).findFirst().get();

        ClusterNode newLeaderNode = raftGroups.keySet().stream()
                .filter(clusterNode -> !new Peer(clusterNode.address()).equals(raftGroupService.leader())).findFirst().get();

        Peer expectedNewLeaderPeer = new Peer(newLeaderNode.address());

        raftGroups.get(oldLeaderNode).transferLeadership(expectedNewLeaderPeer).get();

        assertTrue(waitForCondition(() -> expectedNewLeaderPeer.equals(raftGroups.get(oldLeaderNode).leader()), 10_000));

        assertTrue(waitForCondition(() -> {
            raftGroups.get(newLeaderNode).refreshLeader().join();
            return expectedNewLeaderPeer.equals(raftGroups.get(newLeaderNode).leader());
        }, 10_000));
    }
}
