/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
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

package org.apache.ignite.network.scalecube;

import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import io.scalecube.cluster.Cluster;
import io.scalecube.cluster.transport.api.Message;
import io.scalecube.net.Address;
import org.apache.ignite.network.AbstractMessagingService;
import org.apache.ignite.network.MessagingService;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.NetworkMessageHandler;
import org.apache.ignite.network.message.NetworkMessage;

import static org.apache.ignite.network.scalecube.ScaleCubeMessageCodec.HEADER_MESSAGE_TYPE;

/**
 * Implementation of {@link MessagingService} based on ScaleCube.
 */
final class ScaleCubeMessagingService extends AbstractMessagingService {
    /**
     * Inner representation ScaleCube cluster.
     */
    private Cluster cluster;

    /**
     * Resolver for ScaleCube-specific member.
     */
    private final ScaleCubeMemberResolver memberResolver;

    /**
     * Utility map for recognizing member for its address (ScaleCube doesn't provide such information in input
     * message).
     */
    private final Map<Address, ClusterNode> addressMemberMap = new ConcurrentHashMap<>();

    /**
     *
     */
    ScaleCubeMessagingService(ScaleCubeMemberResolver memberResolver) {
        this.memberResolver = memberResolver;
    }

    /**
     * Sets the ScaleCube's {@link Cluster}. Needed for cyclic dependency injection.
     */
    void setCluster(Cluster cluster) {
        this.cluster = cluster;
    }

    /**
     * Delegates the received message to the registered message handlers.
     */
    void fireEvent(Message message) {
        NetworkMessage msg = message.data();
        ClusterNode sender = memberForAddress(message.sender());
        String correlationId = message.correlationId();
        for (NetworkMessageHandler handler : getMessageHandlers())
            handler.onReceived(msg, sender, correlationId);
    }

    /**
     * {@inheritDoc}
     */
    @Override public void weakSend(ClusterNode recipient, NetworkMessage msg) {
        cluster
            .send(memberResolver.resolveMember(recipient), fromNetworkMessage(msg).build())
            .subscribe();
    }

    /**
     * {@inheritDoc}
     */
    @Override public CompletableFuture<Void> send(ClusterNode recipient, NetworkMessage msg) {
        return cluster
            .send(memberResolver.resolveMember(recipient), fromNetworkMessage(msg).build())
            .toFuture();
    }

    /**
     * {@inheritDoc}
     */
    @Override public CompletableFuture<Void> send(ClusterNode recipient, NetworkMessage msg, String correlationId) {
        var message = fromNetworkMessage(msg)
            .correlationId(correlationId)
            .build();
        return cluster
            .send(memberResolver.resolveMember(recipient), message)
            .toFuture();
    }

    /**
     * {@inheritDoc}
     */
    @Override public CompletableFuture<NetworkMessage> invoke(ClusterNode member, NetworkMessage msg, long timeout) {
        var message = fromNetworkMessage(msg)
            .correlationId(UUID.randomUUID().toString())
            .build();
        return cluster
            .requestResponse(memberResolver.resolveMember(member), message)
            .timeout(Duration.ofMillis(timeout))
            .toFuture()
            .thenApply(Message::data);
    }

    /**
     * @param address Inet address.
     * @return Network member corresponded to input address.
     */
    private ClusterNode memberForAddress(Address address) {
        return addressMemberMap.computeIfAbsent(
            address,
            addr -> cluster.members()
                .stream()
                .filter(mem -> mem.address().equals(addr))
                .map(memberResolver::resolveNetworkMember)
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException(String.format(
                    "Network member with address %s could not be found", addr
                )))
        );
    }

    /**
     * Creates a ScaleCube {@link Message} from a {@link NetworkMessage}.
     *
     * @param message Network message.
     * @return ScaleCube {@link Message}.
     */
    private Message.Builder fromNetworkMessage(NetworkMessage message) {
        return Message.builder()
            .data(message)
            .header(HEADER_MESSAGE_TYPE, Short.toString(message.directType()));
    }
}
