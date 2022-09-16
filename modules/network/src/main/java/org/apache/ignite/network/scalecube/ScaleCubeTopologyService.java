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
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.network.AbstractTopologyService;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.NetworkAddress;
import org.apache.ignite.network.TopologyEventHandler;
import org.apache.ignite.network.TopologyService;

/**
 * Implementation of {@link TopologyService} based on ScaleCube.
 */
final class ScaleCubeTopologyService extends AbstractTopologyService {
    /** Logger. */
    private static final IgniteLogger LOG = Loggers.forClass(ScaleCubeTopologyService.class);

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
        ClusterNode member = fromMember(event.member());

        if (event.isAdded()) {
            members.put(member.address(), member);
            consistentIdToMemberMap.put(member.name(), member);

            LOG.info("Node joined [node={}]", member);

            fireAppearedEvent(member);
        } else if (event.isRemoved()) {
            members.compute(member.address(), (addr, node) -> {
                // Ignore stale remove event.
                if (node == null || node.id().equals(member.id())) {
                    return null;
                } else {
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

            LOG.info("Node left [member={}]", member);

            fireDisappearedEvent(member);
        }

        if (LOG.isInfoEnabled()) {
            LOG.info("Topology snapshot [nodes={}]", members.values().stream().map(ClusterNode::name).collect(Collectors.toList()));
        }
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

        assert localMember != null : "Cluster has not been started";

        return fromMember(localMember);
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

    /**
     * Converts the given {@link Member} to a {@link ClusterNode}.
     *
     * @param member ScaleCube's cluster member.
     * @return Cluster node.
     */
    private static ClusterNode fromMember(Member member) {
        var addr = new NetworkAddress(member.address().host(), member.address().port());

        return new ClusterNode(member.id(), member.alias(), addr);
    }
}
