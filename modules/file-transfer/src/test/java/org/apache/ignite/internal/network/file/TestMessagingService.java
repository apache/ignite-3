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

package org.apache.ignite.internal.network.file;

import static java.util.concurrent.CompletableFuture.failedFuture;

import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.network.AbstractMessagingService;
import org.apache.ignite.internal.network.ChannelType;
import org.apache.ignite.internal.network.NetworkMessage;
import org.apache.ignite.internal.network.NetworkMessageHandler;
import org.apache.ignite.network.ClusterNode;
import org.jetbrains.annotations.Nullable;

/**
 * Test messaging service. It does not provide any messaging functionality, but allows to trigger events.
 */
public class TestMessagingService extends AbstractMessagingService {

    @Override
    public void weakSend(ClusterNode recipient, NetworkMessage msg) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<Void> send(ClusterNode recipient, ChannelType channelType, NetworkMessage msg) {
        return failedFuture(new UnsupportedOperationException());
    }

    @Override
    public CompletableFuture<Void> send(String recipientConsistentId, ChannelType channelType, NetworkMessage msg) {
        return failedFuture(new UnsupportedOperationException());
    }

    @Override
    public CompletableFuture<Void> respond(ClusterNode recipient, ChannelType channelType, NetworkMessage msg, long correlationId) {
        return failedFuture(new UnsupportedOperationException());
    }

    @Override
    public CompletableFuture<Void> respond(String recipientConsistentId, ChannelType channelType, NetworkMessage msg, long correlationId) {
        return failedFuture(new UnsupportedOperationException());
    }

    @Override
    public CompletableFuture<NetworkMessage> invoke(ClusterNode recipient, ChannelType channelType, NetworkMessage msg, long timeout) {
        return failedFuture(new UnsupportedOperationException());
    }

    @Override
    public CompletableFuture<NetworkMessage> invoke(String recipientConsistentId, ChannelType channelType, NetworkMessage msg,
            long timeout) {
        return failedFuture(new UnsupportedOperationException());
    }

    /**
     * Calls {@link NetworkMessageHandler#onReceived(NetworkMessage, ClusterNode, Long)} on all registered.
     *
     * @param msg Message.
     * @param sender Sender node.
     * @param correlationId Correlation ID.
     */
    public void fireMessage(NetworkMessage msg, ClusterNode sender, @Nullable Long correlationId) {
        getMessageHandlers(msg.groupType()).forEach(h -> h.onReceived(msg, sender, correlationId));
    }
}
