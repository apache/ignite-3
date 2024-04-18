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
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.network.AbstractTopologyService;
import org.apache.ignite.internal.network.ClusterNodeImpl;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.NetworkAddress;
import org.apache.ignite.network.NodeMetadata;
import org.apache.ignite.network.TopologyEventHandler;
import org.apache.ignite.network.TopologyService;
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
    private final ConcurrentMap<NetworkAddress, ClusterNode> members = new ConcurrentHashMap<>();

    /** Topology members map from the consistent id to the cluster node. */
    private final ConcurrentMap<String, ClusterNode> consistentIdToMemberMap = new ConcurrentHashMap<>();

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
        ClusterNode member = fromMember(event.member(), metadata);

        if (event.isAdded()) {
            members.put(member.address(), member);
            consistentIdToMemberMap.put(member.name(), member);

            LOG.info("Node joined [node={}]", member);

            fireAppearedEvent(member);
        } else if (event.isUpdated()) {
            members.put(member.address(), member);
            consistentIdToMemberMap.put(member.name(), member);
        } else if (event.isRemoved() || event.isLeaving()) {
            // We treat LEAVING as 'node left' because the node will not be back and we don't want to wait for the suspicion timeout.

            members.compute(member.address(), (addr, node) -> {
                // Ignore stale remove event.
                if (node == null || node.id().equals(member.id())) {
                    LOG.info("Node left [member={}, eventType={}]", member, event.type());

                    return null;
                } else {
                    LOG.info("Node left (noop as it has already reappeared) [member={}, eventType={}]", member, event.type());

                    return node;
                }
            });

            consistentIdToMemberMap.compute(member.name(), (consId, node) -> {
                // Ignore stale remove event.
                if (node == null || node.id().equals(member.id())) {
                    return null;
                } else {
                    return node;
                }
            });

            fireDisappearedEvent(member);
        }

        if (LOG.isInfoEnabled()) {
            LOG.info("Topology snapshot [nodes={}]", members.values().stream().map(ClusterNode::name).collect(Collectors.toList()));
        }
    }

    /**
     * Updates metadata of the local member.
     *
     * @param metadata metadata of the local member.
     */
    void updateLocalMetadata(@Nullable NodeMetadata metadata) {
        ClusterNode node = fromMember(cluster.member(), metadata);
        members.put(node.address(), node);
        consistentIdToMemberMap.put(node.name(), node);
    }

    /**
     * Fire a cluster member appearance event.
     *
     * @param member Appeared cluster member.
     */
    private void fireAppearedEvent(ClusterNode member) {
        for (TopologyEventHandler handler : getEventHandlers()) {
            handler.onAppeared(member);
        }
    }

    /**
     * Fire a cluster member disappearance event.
     *
     * @param member Disappeared cluster member.
     */
    private void fireDisappearedEvent(ClusterNode member) {
        for (TopologyEventHandler handler : getEventHandlers()) {
            handler.onDisappeared(member);
        }
    }

    /** {@inheritDoc} */
    @Override
    public ClusterNode localMember() {
        Member localMember = cluster.member();
        NodeMetadata nodeMetadata = cluster.<NodeMetadata>metadata().orElse(null);

        assert localMember != null : "Cluster has not been started";

        return fromMember(localMember, nodeMetadata);
    }

    /** {@inheritDoc} */
    @Override
    public Collection<ClusterNode> allMembers() {
        return Collections.unmodifiableCollection(members.values());
    }

    /** {@inheritDoc} */
    @Override
    public ClusterNode getByAddress(NetworkAddress addr) {
        return members.get(addr);
    }

    /** {@inheritDoc} */
    @Override
    public ClusterNode getByConsistentId(String consistentId) {
        return consistentIdToMemberMap.get(consistentId);
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable ClusterNode getById(String id) {
        return consistentIdToMemberMap.values().stream().filter(member -> member.id().equals(id)).findFirst().orElse(null);
    }

    /**
     * Converts the given {@link Member} to a {@link ClusterNode}.
     *
     * @param member ScaleCube's cluster member.
     * @return Cluster node.
     */
    private static ClusterNode fromMember(Member member, @Nullable NodeMetadata nodeMetadata) {
        var addr = new NetworkAddress(member.address().host(), member.address().port());

        return new ClusterNodeImpl(member.id(), member.alias(), addr, nodeMetadata);
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
}
