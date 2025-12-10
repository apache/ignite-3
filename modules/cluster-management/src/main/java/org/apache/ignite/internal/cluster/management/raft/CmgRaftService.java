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

import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.stream.Collectors.toCollection;
import static java.util.stream.Collectors.toSet;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.close.ManuallyCloseable;
import org.apache.ignite.internal.cluster.management.ClusterManagementGroupManager;
import org.apache.ignite.internal.cluster.management.ClusterState;
import org.apache.ignite.internal.cluster.management.ClusterTag;
import org.apache.ignite.internal.cluster.management.InvalidNodeConfigurationException;
import org.apache.ignite.internal.cluster.management.MetaStorageInfo;
import org.apache.ignite.internal.cluster.management.NodeAttributes;
import org.apache.ignite.internal.cluster.management.network.messages.CmgMessagesFactory;
import org.apache.ignite.internal.cluster.management.raft.commands.ChangeMetaStorageInfoCommand;
import org.apache.ignite.internal.cluster.management.raft.commands.ClusterNodeMessage;
import org.apache.ignite.internal.cluster.management.raft.commands.JoinReadyCommand;
import org.apache.ignite.internal.cluster.management.raft.commands.JoinRequestCommand;
import org.apache.ignite.internal.cluster.management.raft.commands.NodesLeaveCommand;
import org.apache.ignite.internal.cluster.management.raft.responses.LogicalTopologyResponse;
import org.apache.ignite.internal.cluster.management.raft.responses.ValidationErrorResponse;
import org.apache.ignite.internal.cluster.management.topology.LogicalTopology;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologySnapshot;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.network.InternalClusterNode;
import org.apache.ignite.internal.network.TopologyService;
import org.apache.ignite.internal.properties.IgniteProductVersion;
import org.apache.ignite.internal.raft.Peer;
import org.apache.ignite.internal.raft.PeersAndLearners;
import org.apache.ignite.internal.raft.service.RaftCommandRunner;
import org.apache.ignite.internal.raft.service.RaftGroupService;
import org.jetbrains.annotations.Nullable;

/**
 * A wrapper around a {@link RaftGroupService} providing helpful methods for working with the CMG.
 */
public class CmgRaftService implements ManuallyCloseable {
    private static final IgniteLogger LOG = Loggers.forClass(ClusterManagementGroupManager.class);

    private final CmgMessagesFactory msgFactory = new CmgMessagesFactory();

    private final RaftGroupService raftService;

    private final TopologyService topologyService;

    private final LogicalTopology logicalTopology;

    /**
     * Creates a new instance.
     */
    public CmgRaftService(RaftGroupService raftService, TopologyService topologyService, LogicalTopology logicalTopology) {
        this.raftService = raftService;
        this.topologyService = topologyService;
        this.logicalTopology = logicalTopology;
    }

    /**
     * Returns {@code true} if the current node is the CMG leader.
     *
     * @return {@code true} if the current node is the CMG leader.
     */
    public CompletableFuture<Boolean> isCurrentNodeLeader() {
        Peer leader = raftService.leader();

        if (leader == null) {
            return raftService.refreshLeader().thenCompose(v -> isCurrentNodeLeader());
        } else {
            String nodeName = topologyService.localMember().name();

            return completedFuture(leader.consistentId().equals(nodeName));
        }
    }

    /**
     * Retrieves the cluster state.
     *
     * @return Future that resolves into the current cluster state or {@code null} if it does not exist.
     */
    public CompletableFuture<ClusterState> readClusterState() {
        return raftService.run(msgFactory.readStateCommand().build())
                .thenApply(ClusterState.class::cast);
    }

    /**
     * Saves the given {@link ClusterState}.
     *
     * @param clusterState Cluster state.
     * @return Future that resolves to the current CMG state.
     */
    public CompletableFuture<ClusterState> initClusterState(ClusterState clusterState) {
        ClusterNodeMessage localNodeMessage = nodeMessage(topologyService.localMember());

        return raftService.run(msgFactory.initCmgStateCommand().node(localNodeMessage).clusterState(clusterState).build())
                .thenApply(response -> {
                    if (response instanceof ValidationErrorResponse) {
                        throw new IllegalInitArgumentException("Init CMG request denied, reason: "
                                + ((ValidationErrorResponse) response).reason());
                    } else if (response instanceof ClusterState) {
                        return (ClusterState) response;
                    } else {
                        throw new IgniteInternalException("Unexpected response: " + response);
                    }
                });
    }

    /**
     * Sends a {@link JoinRequestCommand}, starting the validation procedure.
     *
     * @param clusterTag Cluster tag of the joining node.
     * @return Future that either resolves into a join token in case of successful validation or into an {@link IgniteInternalException}
     *         otherwise.
     * @see ValidationManager
     */
    public CompletableFuture<Void> startJoinCluster(ClusterTag clusterTag, NodeAttributes nodeAttributes) {
        ClusterNodeMessage localNodeMessage = nodeMessage(topologyService.localMember(), nodeAttributes);

        JoinRequestCommand command = msgFactory.joinRequestCommand()
                .node(localNodeMessage)
                .version(IgniteProductVersion.CURRENT_VERSION.toString())
                .clusterTag(clusterTag)
                .build();

        // Using NO_TIMEOUT because we want a node that doesn't see CMG majority at start to hang out until someone else starts; otherwise,
        // if we employ a timeout here, node-by-node starts might cause inability to form a cluster.
        return raftService.run(command, RaftCommandRunner.NO_TIMEOUT)
                .thenAccept(response -> {
                    if (response instanceof ValidationErrorResponse) {
                        var validationErrorResponse = (ValidationErrorResponse) response;

                        if (validationErrorResponse.isInvalidNodeConfig()) {
                            var invalidNodeConfigurationException = new InvalidNodeConfigurationException(validationErrorResponse.reason());

                            throw new JoinDeniedException("JoinRequest command failed", invalidNodeConfigurationException);
                        } else {
                            throw new JoinDeniedException(validationErrorResponse.reason());
                        }
                    } else if (response != null) {
                        throw new IgniteInternalException("Unexpected response: " + response);
                    } else {
                        LOG.info("JoinRequest command executed successfully");
                    }
                });
    }

    /**
     * Sends a {@link JoinReadyCommand} thus adding the current node to the local topology.
     *
     * @return Future that represents the state of the operation.
     */
    public CompletableFuture<Void> completeJoinCluster(NodeAttributes attributes) {
        LOG.info("Node is ready to join the logical topology");

        ClusterNodeMessage localNodeMessage = nodeMessage(topologyService.localMember(), attributes);

        JoinReadyCommand joinReadyCommand = msgFactory.joinReadyCommand().node(localNodeMessage).build();
        return raftService.run(joinReadyCommand, RaftCommandRunner.NO_TIMEOUT)
                .thenAccept(response -> {
                    if (response instanceof ValidationErrorResponse) {
                        throw new JoinDeniedException("JoinReady request denied, reason: "
                                + ((ValidationErrorResponse) response).reason());
                    } else if (response != null) {
                        throw new IgniteInternalException("Unexpected response: " + response);
                    } else {
                        LOG.info("JoinReady command executed successfully");
                    }
                });
    }

    /**
     * Removes given nodes from the local topology. Should be called by the CMG leader.
     *
     * @return Future that represents the state of the operation.
     */
    public CompletableFuture<Void> removeFromCluster(Set<InternalClusterNode> nodes) {
        NodesLeaveCommand command = msgFactory.nodesLeaveCommand()
                .nodes(nodes.stream().map(this::nodeMessage).collect(toSet()))
                .build();

        return raftService.run(command);
    }

    /**
     * Retrieves the logical topology snapshot.
     *
     * @return Logical topology snapshot.
     */
    public CompletableFuture<LogicalTopologySnapshot> logicalTopology() {
        return raftService.run(msgFactory.readLogicalTopologyCommand().build())
                .thenApply(LogicalTopologyResponse.class::cast)
                .thenApply(LogicalTopologyResponse::logicalTopology);
    }

    /**
     * Returns a future that, when complete, resolves into a list of validated nodes. This list includes all nodes currently present in the
     * Logical Topology as well as nodes that only have passed the validation step.
     */
    public CompletableFuture<Set<InternalClusterNode>> validatedNodes() {
        return raftService.run(msgFactory.readValidatedNodesCommand().build());
    }

    /**
     * Returns a set of consistent IDs of the voting nodes of the CMG.
     *
     * @return Set of consistent IDs of the voting nodes of the CMG.
     */
    public Set<String> nodeNames() {
        List<Peer> peers = raftService.peers();

        assert peers != null;

        return peers.stream()
                .map(Peer::consistentId)
                .collect(toSet());
    }

    /**
     * Returns a set of consistent IDs of the majority of the voting nodes of the CMG, including a leader.
     *
     * @return Set of consistent IDs of the majority of the voting nodes of the CMG, including a leader.
     */
    public CompletableFuture<Set<String>> majority() {
        Peer leader = raftService.leader();

        if (leader == null) {
            return raftService.refreshLeader().thenCompose(v -> majority());
        }

        List<Peer> peers = raftService.peers();

        assert peers != null;

        int peersCount = peers.size();
        String leaderId = leader.consistentId();

        // Take half of the voting peers without the leader.
        Set<String> result = peers.stream()
                .map(Peer::consistentId)
                .filter(consistentId -> !consistentId.equals(leaderId))
                .limit(peersCount / 2)
                .collect(toCollection(HashSet::new));

        // Add the leader back so the resulting set is a majority of the peers and contains a leader.
        result.add(leaderId);

        return completedFuture(result);
    }

    private ClusterNodeMessage nodeMessage(InternalClusterNode node, NodeAttributes attributes) {
        return msgFactory.clusterNodeMessage()
                .id(node.id())
                .name(node.name())
                .host(node.address().host())
                .port(node.address().port())
                .userAttributes(attributes == null ? null : attributes.userAttributes())
                .systemAttributes(attributes == null ? null : attributes.systemAttributes())
                .storageProfiles(attributes == null ? null : attributes.storageProfiles())
                .build();
    }

    private ClusterNodeMessage nodeMessage(InternalClusterNode node) {
        return msgFactory.clusterNodeMessage()
                .id(node.id())
                .name(node.name())
                .host(node.address().host())
                .port(node.address().port())
                .build();
    }

    /**
     * Returns a known set of consistent IDs of the learners nodes of the CMG.
     */
    public CompletableFuture<Set<String>> learners() {
        List<Peer> currentLearners = raftService.learners();

        if (currentLearners == null) {
            return raftService.refreshMembers(true).thenCompose(v -> learners());
        }

        return completedFuture(currentLearners.stream()
                .map(Peer::consistentId)
                .collect(toSet()));
    }

    /**
     * Issues {@code changePeersAndLearnersAsync} request with same peers; learners are recalculated based on the current peers (which is
     * same as CMG nodes) and known logical topology. Any node in the logical topology that is not a CMG node constitutes a learner.
     *
     * @param term RAFT term in which we operate (used to avoid races when changing peers/learners).
     * @return Future that completes when the request is processed.
     */
    public CompletableFuture<Void> updateLearners(long term) {
        List<Peer> currentLearners = raftService.learners();

        if (currentLearners == null) {
            return raftService.refreshMembers(true).thenCompose(v -> updateLearners(term));
        }

        Set<String> currentLearnerNames = currentLearners.stream()
                .map(Peer::consistentId)
                .collect(toSet());

        Set<String> currentPeers = nodeNames();

        Set<String> newLearners = logicalTopology.getLogicalTopology().nodes().stream()
                .map(InternalClusterNode::name)
                .filter(name -> !currentPeers.contains(name))
                .collect(toSet());

        if (currentLearnerNames.equals(newLearners)) {
            return nullCompletedFuture();
        }

        PeersAndLearners newConfiguration = PeersAndLearners.fromConsistentIds(currentPeers, newLearners);

        if (newLearners.isEmpty()) {
            // Methods for working with learners do not support empty peer lists for some reason.
            // TODO: https://issues.apache.org/jira/browse/IGNITE-26855.
            return raftService.changePeersAndLearnersAsync(newConfiguration, term,  0)
                    .thenRun(() -> raftService.updateConfiguration(newConfiguration));
        } else {
            // TODO: https://issues.apache.org/jira/browse/IGNITE-26855.
            return raftService.resetLearners(newConfiguration.learners(), 0);
        }
    }

    /**
     * Changes Metastorage nodes.
     *
     * @return Future that completes when the change is finished.
     */
    public CompletableFuture<Void> changeMetastorageNodes(Set<String> newMetastorageNodes, @Nullable Long metastorageRepairingConfigIndex) {
        ChangeMetaStorageInfoCommand command = msgFactory.changeMetaStorageInfoCommand()
                .metaStorageNodes(Set.copyOf(newMetastorageNodes))
                .metastorageRepairingConfigIndex(metastorageRepairingConfigIndex)
                .build();
        return raftService.run(command);
    }

    /**
     * Retrieves the Metastorage info.
     *
     * @return Future that resolves into the metastorage info or {@code null} if even cluster state does not exist.
     */
    public CompletableFuture<MetaStorageInfo> readMetaStorageInfo() {
        return raftService.run(msgFactory.readMetaStorageInfoCommand().build())
                .thenApply(MetaStorageInfo.class::cast);
    }

    @Override
    public void close() {
        raftService.shutdown();
    }

    // TODO: https://issues.apache.org/jira/browse/IGNITE-26085 Remove, tmp hack
    /**
     * Mark service as stopping.
     */
    public void markAsStopping() {
        raftService.markAsStopping();
    }
}
