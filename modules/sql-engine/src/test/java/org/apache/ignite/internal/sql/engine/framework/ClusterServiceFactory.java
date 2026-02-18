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

package org.apache.ignite.internal.sql.engine.framework;

import static java.util.UUID.randomUUID;
import static java.util.concurrent.CompletableFuture.failedFuture;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.ignite.internal.cluster.management.topology.LogicalTopology;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalNode;
import org.apache.ignite.internal.network.AbstractMessagingService;
import org.apache.ignite.internal.network.AbstractTopologyService;
import org.apache.ignite.internal.network.ChannelType;
import org.apache.ignite.internal.network.ClusterNodeImpl;
import org.apache.ignite.internal.network.ClusterService;
import org.apache.ignite.internal.network.InternalClusterNode;
import org.apache.ignite.internal.network.MessagingService;
import org.apache.ignite.internal.network.NetworkMessage;
import org.apache.ignite.internal.network.NetworkMessageHandler;
import org.apache.ignite.internal.network.TopologyEventHandler;
import org.apache.ignite.internal.network.TopologyService;
import org.apache.ignite.internal.network.UnresolvableConsistentIdException;
import org.apache.ignite.internal.sql.engine.util.Commons;
import org.apache.ignite.network.NetworkAddress;
import org.jetbrains.annotations.Nullable;

/**
 * Auxiliary object to create the associated {@link MessagingService}
 * and {@link TopologyService} for each node in the cluster.
 */
public class ClusterServiceFactory {
    private final LogicalTopology logicalTopology;

    private final Map<String, InternalClusterNode> nodeByName = new ConcurrentHashMap<>();
    private final Map<String, LocalMessagingService> messagingServicesByNode = new ConcurrentHashMap<>();
    private final Map<String, LocalTopologyService> topologyServicesByNode = new ConcurrentHashMap<>();

    /**
     * Creates a cluster service object for given collection of nodes.
     *
     * @param logicalTopology A logical topology to create cluster service from.
     */
    ClusterServiceFactory(LogicalTopology logicalTopology) {
        this.logicalTopology = logicalTopology;
    }

    private InternalClusterNode nodeByName(String name) {
        return nodeByName.computeIfAbsent(
                name,
                key -> new ClusterNodeImpl(randomUUID(), name, new NetworkAddress(name + "-host", 1000))
        );
    }

    /**
     * Creates an instance of {@link ClusterService} for a given node.
     *
     * @param nodeName A name of the node to create cluster service for.
     * @return An instance of cluster service.
     */
    public ClusterService forNode(String nodeName) {
        return new TestClusterService(nodeName, this, topologyServicesByNode, messagingServicesByNode);
    }

    /** Stops given node. That is, removes it from physical and logical topologies, and fires necessary events. */
    void stopNode(String nodeName) {
        InternalClusterNode node = nodeByName.remove(nodeName);

        if (node != null) {
            messagingServicesByNode.remove(nodeName);
            topologyServicesByNode.remove(nodeName);

            topologyServicesByNode.values()
                    .forEach(topologyService -> topologyService.evictNode(nodeName));
        }
    }

    /** Disconnects given node. That is, removes it from physical topology only, and fires related events. */
    void disconnectNode(String nodeName) {
        InternalClusterNode node = nodeByName.remove(nodeName);

        if (node != null) {
            messagingServicesByNode.remove(nodeName);
            topologyServicesByNode.remove(nodeName);

            topologyServicesByNode.values()
                    .forEach(topologyService -> topologyService.disconnectNode(nodeName));
        }
    }

    LocalTopologyService createTopologyService(String nodeName) {
        return new LocalTopologyService(nodeName, logicalTopology);
    }

    LocalMessagingService createMessagingService(String nodeName) {
        return new LocalMessagingService(nodeByName(nodeName));
    }

    static class LocalTopologyService extends AbstractTopologyService {
        private final LogicalTopology logicalTopology;
        private final InternalClusterNode localMember;

        private LocalTopologyService(String localMember, LogicalTopology logicalTopology) {
            this.logicalTopology = logicalTopology;
            this.localMember = findNodeByName(logicalTopology, localMember);

            if (this.localMember == null) {
                throw new IllegalArgumentException("Local member is not part of all members");
            }
        }

        private void evictNode(String nodeName) {
            InternalClusterNode nodeToEvict = getByConsistentId(nodeName);

            if (nodeToEvict != null) {
                logicalTopology.removeNodes(Set.of((LogicalNode) nodeToEvict));

                getEventHandlers().forEach(handler -> handler.onDisappeared(nodeToEvict));
            }
        }

        private void disconnectNode(String nodeName) {
            InternalClusterNode nodeToDisconnect = getByConsistentId(nodeName);

            if (nodeToDisconnect != null) {
                getEventHandlers().forEach(handler -> handler.onDisappeared(nodeToDisconnect));
            }
        }

        private static @Nullable InternalClusterNode findNodeByName(LogicalTopology logicalTopology, String name) {
            return logicalTopology.getLogicalTopology().node(name).orElse(null);
        }

        @Override
        public Collection<TopologyEventHandler> getEventHandlers() {
            return super.getEventHandlers();
        }

        /** {@inheritDoc} */
        @Override
        public InternalClusterNode localMember() {
            return localMember;
        }

        /** {@inheritDoc} */
        @Override
        public Collection<InternalClusterNode> allMembers() {
            return Commons.cast(logicalTopology.getLogicalTopology().nodes());
        }

        @Override
        public Collection<InternalClusterNode> logicalTopologyMembers() {
            return Commons.cast(logicalTopology.getLogicalTopology().nodes());
        }

        @Override
        public long logicalTopologyVersion() {
            return logicalTopology.getLogicalTopology().version();
        }

        /** {@inheritDoc} */
        @Override
        public @Nullable InternalClusterNode getByAddress(NetworkAddress addr) {
            throw new AssertionError("Not implemented");
        }

        /** {@inheritDoc} */
        @Override
        public @Nullable InternalClusterNode getByConsistentId(String consistentId) {
            return findNodeByName(logicalTopology, consistentId);
        }

        @Override
        public @Nullable InternalClusterNode getById(UUID id) {
            return logicalTopology.getLogicalTopology().node(id).orElse(null);
        }

        @Override
        public void onJoined(InternalClusterNode node, long topologyVersion) {
        }

        @Override
        public void onLeft(InternalClusterNode node, long topologyVersion) {
        }
    }

    class LocalMessagingService extends AbstractMessagingService {
        private final InternalClusterNode localNode;

        private LocalMessagingService(InternalClusterNode localNode) {
            this.localNode = localNode;
        }

        /** {@inheritDoc} */
        @Override
        public void weakSend(InternalClusterNode recipient, NetworkMessage msg) {
            throw new AssertionError("Not implemented yet");
        }

        /** {@inheritDoc} */
        @Override
        public CompletableFuture<Void> send(InternalClusterNode recipient, ChannelType channelType, NetworkMessage msg) {
            return send(recipient.name(), channelType, msg);
        }

        @Override
        public CompletableFuture<Void> send(String recipientConsistentId, ChannelType channelType, NetworkMessage msg) {
            LocalMessagingService recipient = messagingServicesByNode.get(recipientConsistentId);

            if (recipient == null) {
                return failedFuture(new UnresolvableConsistentIdException(recipientConsistentId));
            }

            for (NetworkMessageHandler handler : recipient.messageHandlers(msg.groupType())) {
                handler.onReceived(msg, localNode, null);
            }

            return nullCompletedFuture();
        }

        @Override
        public CompletableFuture<Void> send(NetworkAddress recipientNetworkAddress, ChannelType channelType, NetworkMessage msg) {
            return failedFuture(new UnsupportedOperationException());
        }

        /** {@inheritDoc} */
        @Override
        public CompletableFuture<Void> respond(InternalClusterNode recipient, ChannelType type, NetworkMessage msg, long correlationId) {
            throw new AssertionError("Not implemented yet");
        }

        /** {@inheritDoc} */
        @Override
        public CompletableFuture<Void> respond(String recipientConsistentId, ChannelType type, NetworkMessage msg, long correlationId) {
            throw new AssertionError("Not implemented yet");
        }

        /** {@inheritDoc} */
        @Override
        public CompletableFuture<NetworkMessage> invoke(InternalClusterNode recipient, ChannelType type, NetworkMessage msg, long timeout) {
            throw new AssertionError("Not implemented yet");
        }

        /** {@inheritDoc} */
        @Override
        public CompletableFuture<NetworkMessage> invoke(String recipientNodeId, ChannelType type, NetworkMessage msg, long timeout) {
            throw new AssertionError("Not implemented yet");
        }

        private Collection<NetworkMessageHandler> messageHandlers(short groupType) {
            return getMessageHandlers(groupType);
        }
    }
}
