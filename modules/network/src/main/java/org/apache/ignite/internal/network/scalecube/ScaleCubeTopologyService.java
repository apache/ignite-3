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

package org.apache.ignite.internal.network.scalecube;

import io.scalecube.cluster.Cluster;
import io.scalecube.cluster.Member;
import io.scalecube.cluster.membership.MembershipEvent;
import io.scalecube.cluster.metadata.MetadataCodec;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.network.AbstractTopologyService;
import org.apache.ignite.internal.network.ClusterNodeImpl;
import org.apache.ignite.internal.network.DuplicateConsistentIdException;
import org.apache.ignite.internal.network.InternalClusterNode;
import org.apache.ignite.internal.network.TopologyEventHandler;
import org.apache.ignite.internal.network.TopologyService;
import org.apache.ignite.network.NetworkAddress;
import org.apache.ignite.network.NodeMetadata;
import org.jetbrains.annotations.Nullable;

/**
 * Implementation of {@link TopologyService} based on ScaleCube.
 */
final class ScaleCubeTopologyService extends AbstractTopologyService {
    /** Logger. */
    private static final IgniteLogger LOG = Loggers.forClass(ScaleCubeTopologyService.class);

    /** Metadata codec. */
    private static final MetadataCodec METADATA_CODEC = MetadataCodec.INSTANCE;

    /**
     * Inner representation of a ScaleCube cluster.
     */
    private volatile Cluster cluster;

    /** Topology members from the network address to the cluster node.. */
    private final ConcurrentMap<NetworkAddress, InternalClusterNode> members = new ConcurrentHashMap<>();

    /** Topology members map from the consistent id to the map from the id to the cluster node. */
    private final ConcurrentMap<String, Map<UUID, InternalClusterNode>> membersByConsistentId = new ConcurrentHashMap<>();

    /** Topology members map from the consistent id to the cluster node. Only contains nodes that joined logical topology. */
    private final ConcurrentMap<String, InternalClusterNode> membersByConsistentIdInLogicalTopology = new ConcurrentHashMap<>();

    /** Topology members map from the id to the cluster node. */
    private final ConcurrentMap<UUID, InternalClusterNode> idToMemberMap = new ConcurrentHashMap<>();

    /**
     * Sets the ScaleCube's {@link Cluster}. Needed for cyclic dependency injection.
     *
     * @param cluster Cluster.
     */
    void setCluster(Cluster cluster) {
        this.cluster = cluster;
    }

    /**
     * Delegates the received topology event to the registered event handlers.
     *
     * @param event Membership event.
     */
    void onMembershipEvent(MembershipEvent event) {
        NodeMetadata metadata = deserializeMetadata(event.newMetadata());
        InternalClusterNode member = fromMember(event.member(), metadata);

        if (event.isAdded()) {
            onAddedEvent(member);
        } else if (event.isUpdated()) {
            onUpdatedEvent(member);
        } else if (event.isRemoved() || event.isLeaving()) {
            onRemovedOrLeftEvent(event, member);
        }

        if (LOG.isInfoEnabled()) {
            LOG.info("Topology snapshot [nodes={}]", members.values().stream().map(InternalClusterNode::name).collect(Collectors.toList()));
        }
    }

    private void onAddedEvent(InternalClusterNode member) {
        @Nullable InternalClusterNode differentNodeWithSameAddress = replaceMemberByAddress(member);

        replaceMemberByConsistentId(member, differentNodeWithSameAddress);

        replaceMemberById(member, differentNodeWithSameAddress);

        LOG.info("Node joined [node={}]", member);

        fireAppearedEvent(member);
    }

    private @Nullable InternalClusterNode replaceMemberByAddress(InternalClusterNode member) {
        InternalClusterNode prevNodeWithSameAddress = members.put(member.address(), member);
        return prevNodeWithSameAddress == null || prevNodeWithSameAddress.id().equals(member.id())
                ? null : prevNodeWithSameAddress;
    }

    private void replaceMemberByConsistentId(InternalClusterNode member, @Nullable InternalClusterNode differentNodeWithSameAddress) {
        membersByConsistentId.compute(member.name(), (name, nodesWithGivenConsistentId) -> {
            if (nodesWithGivenConsistentId == null) {
                nodesWithGivenConsistentId = new ConcurrentHashMap<>();
            }

            if (differentNodeWithSameAddress != null) {
                nodesWithGivenConsistentId.remove(differentNodeWithSameAddress.id());
            }
            nodesWithGivenConsistentId.put(member.id(), member);

            return nodesWithGivenConsistentId;
        });
    }

    private void replaceMemberById(InternalClusterNode member, @Nullable InternalClusterNode differentNodeWithSameAddress) {
        idToMemberMap.put(member.id(), member);
        if (differentNodeWithSameAddress != null) {
            idToMemberMap.remove(differentNodeWithSameAddress.id());
        }
    }

    private void onUpdatedEvent(InternalClusterNode member) {
        @Nullable InternalClusterNode differentNodeWithSameAddress = replaceMemberByAddress(member);

        replaceMemberByConsistentId(member, differentNodeWithSameAddress);

        membersByConsistentIdInLogicalTopology.compute(member.name(), (consId, existingNode) -> {
            if (existingNode != null && existingNode.id().equals(member.id())) {
                return member;
            }
            return existingNode;
        });

        replaceMemberById(member, differentNodeWithSameAddress);
    }

    private void onRemovedOrLeftEvent(MembershipEvent event, InternalClusterNode member) {
        // We treat LEAVING as 'node left' because the node will not be back and we don't want to wait for the suspicion timeout.

        members.compute(member.address(), (addr, node) -> {
            if (node == null || node.id().equals(member.id())) {
                LOG.info("Node left [member={}, eventType={}]", member, event.type());

                return null;
            } else {
                // Ignore stale remove event.
                LOG.info("Node left (noop as it has already reappeared) [member={}, eventType={}]", member, event.type());

                return node;
            }
        });

        membersByConsistentId.compute(member.name(), (consId, nodes) -> {
            if (nodes != null) {
                nodes.remove(member.id());
                if (nodes.isEmpty()) {
                    return null;
                }
            }
            return nodes;
        });
        membersByConsistentIdInLogicalTopology.compute(member.name(), (consId, existingNode) -> {
            if (existingNode != null && existingNode.id().equals(member.id())) {
                return null;
            }
            return existingNode;
        });

        idToMemberMap.remove(member.id());

        fireDisappearedEvent(member);
    }

    /**
     * Updates metadata of the local member.
     *
     * @param metadata metadata of the local member.
     */
    void updateLocalMetadata(@Nullable NodeMetadata metadata) {
        InternalClusterNode node = fromMember(cluster.member(), metadata);
        onUpdatedEvent(node);
    }

    /**
     * Fire a cluster member appearance event.
     *
     * @param member Appeared cluster member.
     */
    private void fireAppearedEvent(InternalClusterNode member) {
        for (TopologyEventHandler handler : getEventHandlers()) {
            handler.onAppeared(member);
        }
    }

    /**
     * Fire a cluster member disappearance event.
     *
     * @param member Disappeared cluster member.
     */
    private void fireDisappearedEvent(InternalClusterNode member) {
        for (TopologyEventHandler handler : getEventHandlers()) {
            handler.onDisappeared(member);
        }
    }

    /** {@inheritDoc} */
    @Override
    public InternalClusterNode localMember() {
        Member localMember = cluster.member();
        NodeMetadata nodeMetadata = cluster.<NodeMetadata>metadata().orElse(null);

        assert localMember != null : "Cluster has not been started";

        return fromMember(localMember, nodeMetadata);
    }

    /** {@inheritDoc} */
    @Override
    public Collection<InternalClusterNode> allMembers() {
        return Collections.unmodifiableCollection(members.values());
    }

    /** {@inheritDoc} */
    @Override
    public Collection<InternalClusterNode> logicalTopologyMembers() {
        List<InternalClusterNode> res = new ArrayList<>(membersByConsistentIdInLogicalTopology.size());

        // membersByConsistentIdInLogicalTopology does not have node metadata,
        // so we get nodes from the physical topology map and filter by logical topology membership.
        for (InternalClusterNode node : members.values()) {
            if (membersByConsistentIdInLogicalTopology.containsKey(node.name())) {
                res.add(node);
            }
        }

        return res;
    }

    /** {@inheritDoc} */
    @Override
    public InternalClusterNode getByAddress(NetworkAddress addr) {
        return members.get(addr);
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable InternalClusterNode getByConsistentId(String consistentId) {
        InternalClusterNode nodeInLogicalTopology = membersByConsistentIdInLogicalTopology.get(consistentId);
        if (nodeInLogicalTopology != null) {
            // Node is in the logical topology, check if it's present in the physical topology. The node could be in the logical topology
            // when it was restored on node start, but the node is not present in the physical topology. In this case, fall through.
            InternalClusterNode node = idToMemberMap.get(nodeInLogicalTopology.id());
            if (node != null) {
                return node;
            }
        }

        // Node is not in the logical topology, check if it's the only node in the physical topology
        Map<UUID, InternalClusterNode> nodes = membersByConsistentId.get(consistentId);
        if (nodes == null) {
            return null;
        }
        if (nodes.size() > 1) {
            LOG.error(
                    "Node \"{}\" has duplicate(s) in the physical topology: {}",
                    consistentId,
                    nodes.values().stream().map(InternalClusterNode::address).collect(Collectors.toList())
            );
            throw new DuplicateConsistentIdException(consistentId);
        }

        try {
            return nodes.values().iterator().next();
        } catch (NoSuchElementException e) {
            // It was concurrently removed.
            return null;
        }
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable InternalClusterNode getById(UUID id) {
        return idToMemberMap.get(id);
    }

    /**
     * Converts the given {@link Member} to a {@link InternalClusterNode}.
     *
     * @param member ScaleCube's cluster member.
     * @return Cluster node.
     */
    private static InternalClusterNode fromMember(Member member, @Nullable NodeMetadata nodeMetadata) {
        var addr = new NetworkAddress(member.address().host(), member.address().port());

        return new ClusterNodeImpl(UUID.fromString(member.id()), member.alias(), addr, nodeMetadata);
    }


    /**
     * Deserializes the given {@link ByteBuffer} to a {@link NodeMetadata}.
     *
     * @param buffer ByteBuffer to deserialize.
     * @return NodeMetadata or null if something goes wrong.
     */
    @Nullable
    private static NodeMetadata deserializeMetadata(@Nullable ByteBuffer buffer) {
        if (buffer == null) {
            return null;
        }
        try {
            return (NodeMetadata) METADATA_CODEC.deserialize(buffer);
        } catch (Exception e) {
            LOG.warn("Couldn't deserialize metadata: {}", e);
            return null;
        }
    }

    @Override
    public void onJoined(InternalClusterNode node, long topologyVersion) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Node joined logical topology [node={}]", node);
        }
        membersByConsistentIdInLogicalTopology.put(node.name(), node);
        this.topologyVersion = topologyVersion;
    }

    @Override
    public void onLeft(InternalClusterNode node, long topologyVersion) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Node left logical topology [node={}]", node);
        }

        membersByConsistentIdInLogicalTopology.remove(node.name());
        this.topologyVersion = topologyVersion;
    }
}
