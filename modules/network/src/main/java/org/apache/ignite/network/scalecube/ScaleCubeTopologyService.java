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

package org.apache.ignite.network.scalecube;

import io.scalecube.cluster.Cluster;
import io.scalecube.cluster.Member;
import io.scalecube.cluster.membership.MembershipEvent;
import io.scalecube.cluster.metadata.MetadataCodec;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.network.discovery.DiscoveryTopologyEventListener;
import org.apache.ignite.internal.network.discovery.DiscoveryTopologyService;
import org.apache.ignite.network.AbstractTopologyService;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.NetworkAddress;
import org.apache.ignite.network.NodeMetadata;
import org.apache.ignite.network.TopologyEventHandler;
import org.apache.ignite.network.TopologyService;
import org.jetbrains.annotations.Nullable;

/**
 * Implementation of {@link TopologyService} based on ScaleCube.
 */
final class ScaleCubeTopologyService extends AbstractTopologyService implements DiscoveryTopologyService {
    /** Logger. */
    private static final IgniteLogger LOG = Loggers.forClass(ScaleCubeTopologyService.class);

    /** Metadata codec. */
    private static final MetadataCodec METADATA_CODEC = MetadataCodec.INSTANCE;

    /**
     * Inner representation of a ScaleCube cluster.
     */
    private volatile Cluster cluster;

    /** Discovery topology members map from the consistent id to the cluster node. */
    private final ConcurrentMap<String, ClusterNode> consistentIdToDtMemberMap = new ConcurrentHashMap<>();

    /** Physical topology members from the network address to the cluster node.. */
    private final ConcurrentMap<NetworkAddress, ClusterNode> ptMembers = new ConcurrentHashMap<>();

    /** Topology members map from the consistent id to the cluster node. */
    private final ConcurrentMap<String, ClusterNode> consistentIdToPtMemberMap = new ConcurrentHashMap<>();

    private final List<DiscoveryTopologyEventListener> discoveryEventListeners = new CopyOnWriteArrayList<>();

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
            // When a node appears in DT, it immediately appears in PT.
            consistentIdToDtMemberMap.put(member.name(), member);

            ptMembers.put(member.address(), member);
            consistentIdToPtMemberMap.put(member.name(), member);

            LOG.info("Node joined [node={}]", member);

            fireAppearedEvent(member);
        } else if (event.isUpdated()) {
            consistentIdToDtMemberMap.put(member.name(), member);

            ptMembers.put(member.address(), member);
            consistentIdToPtMemberMap.put(member.name(), member);
        } else if (event.isRemoved()) {
            consistentIdToDtMemberMap.compute(member.name(), (consId, node) -> {
                // Ignore stale remove event.
                if (node == null || node.id().equals(member.id())) {
                    return null;
                } else {
                    return node;
                }
            });

            LOG.info("Node left DT [member={}]", member);

            fireDisappearedFromDtEvent(member);
        }

        if (LOG.isInfoEnabled()) {
            LOG.info("Topology snapshot [nodes={}]", ptMembers.values().stream().map(ClusterNode::name).collect(Collectors.toList()));
        }
    }

    /**
     * Updates metadata of the local member.
     *
     * @param metadata metadata of the local member.
     */
    void updateLocalMetadata(@Nullable NodeMetadata metadata) {
        ClusterNode node = fromMember(cluster.member(), metadata);

        consistentIdToDtMemberMap.put(node.name(), node);

        ptMembers.put(node.address(), node);
        consistentIdToPtMemberMap.put(node.name(), node);
    }

    /**
     * Fire a cluster member appearance event.
     *
     * @param member Appeared cluster member.
     */
    private void fireAppearedEvent(ClusterNode member) {
        // Appearing in the Discovery Topology is equivalent to appearing in the Physical topology, so we trigger
        // PT.onAppeared() right away.

        for (TopologyEventHandler handler : getEventHandlers()) {
            handler.onAppeared(member);
        }
    }

    /**
     * Fires a cluster member disappearance event (from Discovery Topology).
     *
     * @param member Disappeared cluster member.
     */
    private void fireDisappearedFromDtEvent(ClusterNode member) {
        for (DiscoveryTopologyEventListener listener : discoveryEventListeners) {
            try {
                listener.onDisappeared(member);
            } catch (Exception | AssertionError e) {
                LOG.error("Error while notifying discovery event listeners about node disappearance: {}", e, member);
            }
        }
    }

    /**
     * Fires a cluster member disappearance event (from Physical Topology).
     *
     * @param member Disappeared cluster member.
     */
    private void fireDisappearedFromPtEvent(ClusterNode member) {
        for (TopologyEventHandler handler : getEventHandlers()) {
            try {
                handler.onDisappeared(member);
            } catch (Exception | AssertionError e) {
                LOG.error("Error while notifying physical topology event listeners about node disappearance: {}", e, member);
            }
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
        return Collections.unmodifiableCollection(ptMembers.values());
    }

    /** {@inheritDoc} */
    @Override
    public ClusterNode getByAddress(NetworkAddress addr) {
        return ptMembers.get(addr);
    }

    /** {@inheritDoc} */
    @Override
    public ClusterNode getByConsistentId(String consistentId) {
        return consistentIdToPtMemberMap.get(consistentId);
    }

    /**
     * Converts the given {@link Member} to a {@link ClusterNode}.
     *
     * @param member ScaleCube's cluster member.
     * @return Cluster node.
     */
    private static ClusterNode fromMember(Member member, @Nullable NodeMetadata nodeMetadata) {
        var addr = new NetworkAddress(member.address().host(), member.address().port());

        return new ClusterNode(member.id(), member.alias(), addr, nodeMetadata);
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
    public void addDiscoveryEventListener(DiscoveryTopologyEventListener listener) {
        discoveryEventListeners.add(listener);
    }

    @Override
    public @Nullable ClusterNode discoveredNodeByConsistentId(String consistentId) {
        return consistentIdToDtMemberMap.get(consistentId);
    }

    @Override
    public void removeFromPhysicalTopology(ClusterNode member) {
        ptMembers.compute(member.address(), (addr, node) -> {
            // Ignore stale remove event.
            if (node == null || node.id().equals(member.id())) {
                return null;
            } else {
                return node;
            }
        });

        consistentIdToPtMemberMap.compute(member.name(), (consId, node) -> {
            // Ignore stale remove event.
            if (node == null || node.id().equals(member.id())) {
                return null;
            } else {
                return node;
            }
        });

        LOG.info("Node left PT [member={}]", member);

        fireDisappearedFromPtEvent(member);
    }
}
