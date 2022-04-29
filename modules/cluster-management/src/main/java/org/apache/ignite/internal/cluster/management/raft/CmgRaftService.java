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

package org.apache.ignite.internal.cluster.management.raft;

import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import org.apache.ignite.internal.cluster.management.raft.commands.JoinReadyCommand;
import org.apache.ignite.internal.cluster.management.raft.commands.JoinRequestCommand;
import org.apache.ignite.internal.cluster.management.raft.commands.NodesLeaveCommand;
import org.apache.ignite.internal.cluster.management.raft.commands.ReadLogicalTopologyCommand;
import org.apache.ignite.internal.cluster.management.raft.commands.ReadStateCommand;
import org.apache.ignite.internal.cluster.management.raft.commands.WriteStateCommand;
import org.apache.ignite.internal.cluster.management.raft.responses.JoinDeniedResponse;
import org.apache.ignite.internal.cluster.management.raft.responses.LogicalTopologyResponse;
import org.apache.ignite.lang.IgniteInternalException;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.ClusterService;
import org.apache.ignite.raft.client.Peer;
import org.apache.ignite.raft.client.service.RaftGroupService;

/**
 * A wrapper around a {@link RaftGroupService} providing helpful methods for working with the CMG.
 */
public class CmgRaftService {
    private final RaftGroupService raftService;

    private final ClusterService clusterService;

    public CmgRaftService(RaftGroupService raftService, ClusterService clusterService) {
        this.raftService = raftService;
        this.clusterService = clusterService;
    }

    /**
     * Returns {@code true} if the current node is the CMG leader.
     *
     * @return {@code true} if the current node is the CMG leader.
     */
    // TODO: replace with onLeaderElected callback after https://issues.apache.org/jira/browse/IGNITE-16379 is implemented
    public CompletableFuture<Boolean> isCurrentNodeLeader() {
        ClusterNode thisNode = clusterService.topologyService().localMember();

        return leader().thenApply(thisNode::equals);
    }

    private CompletableFuture<ClusterNode> leader() {
        return raftService.refreshLeader().thenApply(v -> resolvePeer(raftService.leader()));
    }

    /**
     * Retrieves the cluster state.
     *
     * @return Future that resolves into the current cluster state or {@code null} if it does not exist.
     */
    public CompletableFuture<ClusterState> readClusterState() {
        return raftService.run(new ReadStateCommand())
                .thenApply(ClusterState.class::cast);
    }

    /**
     * Saves the given {@link ClusterState}.
     *
     * @param clusterState Cluster state.
     * @return Future that represents the state of the operation.
     */
    public CompletableFuture<Void> writeClusterState(ClusterState clusterState) {
        return raftService.run(new WriteStateCommand(clusterState));
    }

    /**
     * Sends a {@link JoinRequestCommand}, starting the validation procedure.
     *
     * @return Future that represents the state of the operation.
     */
    public CompletableFuture<Void> startJoinCluster() {
        ClusterNode localMember = clusterService.topologyService().localMember();

        return raftService.run(new JoinRequestCommand(localMember))
                .thenAccept(response -> {
                    if (response instanceof JoinDeniedResponse) {
                        throw new IgniteInternalException("Join request denied, reason: " + ((JoinDeniedResponse) response).reason());
                    }
                });
    }

    /**
     * Sends a {@link JoinReadyCommand} thus adding the current node to the local topology.
     *
     * @return Future that represents the state of the operation.
     */
    public CompletableFuture<Void> completeJoinCluster() {
        ClusterNode localMember = clusterService.topologyService().localMember();

        return raftService.run(new JoinReadyCommand(localMember))
                .thenAccept(response -> {
                    if (response instanceof JoinDeniedResponse) {
                        throw new IgniteInternalException("JoinReady request denied, reason: " + ((JoinDeniedResponse) response).reason());
                    }
                });
    }

    /**
     * Removes given nodes from the local topology. Should be called by the CMG leader.
     *
     * @return Future that represents the state of the operation.
     */
    public CompletableFuture<Void> removeFromCluster(Set<ClusterNode> nodes) {
        return raftService.run(new NodesLeaveCommand(nodes));
    }

    /**
     * Retrieves the logical topology.
     *
     * @return Logical topology.
     */
    public CompletableFuture<Collection<ClusterNode>> logicalTopology() {
        return raftService.run(new ReadLogicalTopologyCommand())
                .thenApply(LogicalTopologyResponse.class::cast)
                .thenApply(LogicalTopologyResponse::logicalTopology);
    }

    /**
     * Returns a list of consistent IDs of the voting nodes of the CMG.
     *
     * @return List of consistent IDs of the voting nodes of the CMG.
     */
    public Set<String> nodeNames() {
        List<Peer> peers = raftService.peers();

        assert peers != null;

        return peers.stream()
                .map(this::resolvePeer)
                .map(ClusterNode::name)
                .collect(Collectors.toSet());
    }

    private ClusterNode resolvePeer(Peer peer) {
        ClusterNode node = clusterService.topologyService().getByAddress(peer.address());

        assert node != null;

        return node;
    }
}
