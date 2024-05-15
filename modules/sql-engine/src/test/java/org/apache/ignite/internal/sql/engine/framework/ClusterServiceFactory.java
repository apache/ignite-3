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

import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.ignite.internal.network.AbstractMessagingService;
import org.apache.ignite.internal.network.AbstractTopologyService;
import org.apache.ignite.internal.network.ChannelType;
import org.apache.ignite.internal.network.ClusterNodeImpl;
import org.apache.ignite.internal.network.ClusterService;
import org.apache.ignite.internal.network.MessagingService;
import org.apache.ignite.internal.network.NetworkMessage;
import org.apache.ignite.internal.network.NetworkMessageHandler;
import org.apache.ignite.internal.network.serialization.MessageSerializationRegistry;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.NetworkAddress;
import org.apache.ignite.network.NodeMetadata;
import org.apache.ignite.network.TopologyService;
import org.jetbrains.annotations.Nullable;

/**
 * Auxiliary object to create the associated {@link MessagingService}
 * and {@link TopologyService} for each node in the cluster.
 */
public class ClusterServiceFactory {
    private final List<String> allNodes;

    private final Map<String, ClusterNode> nodeByName = new ConcurrentHashMap<>();
    private final Map<String, LocalMessagingService> messagingServicesByNode = new ConcurrentHashMap<>();
    private final Map<String, LocalTopologyService> topologyServicesByNode = new ConcurrentHashMap<>();

    /**
     * Creates a cluster service object for given collection of nodes.
     *
     * @param allNodes A collection of nodes to create cluster service from.
     */
    ClusterServiceFactory(List<String> allNodes) {
        this.allNodes = allNodes;
    }

    private ClusterNode nodeByName(String name) {
        return nodeByName.computeIfAbsent(
                name,
                key -> new ClusterNodeImpl(UUID.randomUUID().toString(), name, new NetworkAddress(name + "-host", 1000))
        );
    }

    /**
     * Creates an instance of {@link ClusterService} for a given node.
     *
     * @param nodeName A name of the node to create cluster service for.
     * @return An instance of cluster service.
     */
    public ClusterService forNode(String nodeName) {
        return new ClusterService() {
            @Override
            public String nodeName() {
                throw new AssertionError("Should not be called");
            }

            /** {@inheritDoc} */
            @Override
            public TopologyService topologyService() {
                return topologyServicesByNode.computeIfAbsent(nodeName, name -> new LocalTopologyService(name, allNodes));
            }

            /** {@inheritDoc} */
            @Override
            public MessagingService messagingService() {
                return messagingServicesByNode.computeIfAbsent(nodeName, key -> new LocalMessagingService(nodeByName(nodeName)));
            }

            /** {@inheritDoc} */
            @Override
            public boolean isStopped() {
                return false;
            }

            /** {@inheritDoc} */
            @Override
            public void updateMetadata(NodeMetadata metadata) {
                throw new AssertionError("Should not be called");
            }

            @Override
            public MessageSerializationRegistry serializationRegistry() {
                throw new AssertionError("Should not be called");
            }

            /** {@inheritDoc} */
            @Override
            public CompletableFuture<Void> startAsync() {
                return nullCompletedFuture();
            }
        };
    }

    private static class LocalTopologyService extends AbstractTopologyService {
        private static final AtomicInteger NODE_COUNTER = new AtomicInteger(1);

        private final ClusterNode localMember;
        private final Map<String, ClusterNode> allMembers;
        private final Map<NetworkAddress, ClusterNode> allMembersByAddress;

        private LocalTopologyService(String localMember, List<String> allMembers) {
            this.allMembers = allMembers.stream()
                    .map(LocalTopologyService::nodeFromName)
                    .collect(Collectors.toMap(ClusterNode::name, Function.identity()));

            this.localMember = this.allMembers.get(localMember);

            if (this.localMember == null) {
                throw new IllegalArgumentException("Local member is not part of all members");
            }

            this.allMembersByAddress = new HashMap<>();

            this.allMembers.forEach((ignored, member) -> allMembersByAddress.put(member.address(), member));
        }

        private static ClusterNode nodeFromName(String name) {
            return new ClusterNodeImpl(name, name, NetworkAddress.from("127.0.0.1:" + NODE_COUNTER.incrementAndGet()));
        }

        /** {@inheritDoc} */
        @Override
        public ClusterNode localMember() {
            return localMember;
        }

        /** {@inheritDoc} */
        @Override
        public Collection<ClusterNode> allMembers() {
            return allMembers.values();
        }

        /** {@inheritDoc} */
        @Override
        public @Nullable ClusterNode getByAddress(NetworkAddress addr) {
            return allMembersByAddress.get(addr);
        }

        /** {@inheritDoc} */
        @Override
        public @Nullable ClusterNode getByConsistentId(String consistentId) {
            return allMembers.get(consistentId);
        }

        @Override
        public @Nullable ClusterNode getById(String id) {
            return allMembers.values().stream().filter(member -> member.id().equals(id)).findFirst().orElse(null);
        }
    }

    private class LocalMessagingService extends AbstractMessagingService {
        private final ClusterNode localNode;

        private LocalMessagingService(ClusterNode localNode) {
            this.localNode = localNode;
        }

        /** {@inheritDoc} */
        @Override
        public void weakSend(ClusterNode recipient, NetworkMessage msg) {
            throw new AssertionError("Not implemented yet");
        }

        /** {@inheritDoc} */
        @Override
        public CompletableFuture<Void> send(ClusterNode recipient, ChannelType channelType, NetworkMessage msg) {
            for (var handler : messagingServicesByNode.get(recipient.name()).messageHandlers(msg.groupType())) {
                handler.onReceived(msg, localNode, null);
            }

            return nullCompletedFuture();
        }

        @Override
        public CompletableFuture<Void> send(String recipientConsistentId, ChannelType channelType, NetworkMessage msg) {
            for (var handler : messagingServicesByNode.get(recipientConsistentId).messageHandlers(msg.groupType())) {
                handler.onReceived(msg, localNode, null);
            }

            return nullCompletedFuture();
        }

        /** {@inheritDoc} */
        @Override
        public CompletableFuture<Void> respond(ClusterNode recipient, ChannelType type, NetworkMessage msg, long correlationId) {
            throw new AssertionError("Not implemented yet");
        }

        /** {@inheritDoc} */
        @Override
        public CompletableFuture<Void> respond(String recipientConsistentId, ChannelType type, NetworkMessage msg, long correlationId) {
            throw new AssertionError("Not implemented yet");
        }

        /** {@inheritDoc} */
        @Override
        public CompletableFuture<NetworkMessage> invoke(ClusterNode recipient, ChannelType type, NetworkMessage msg, long timeout) {
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
