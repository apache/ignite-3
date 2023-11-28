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

package org.apache.ignite.internal.table.impl;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.network.AbstractMessagingService;
import org.apache.ignite.network.ChannelType;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.NetworkMessage;

/**
 * Dummy messaging service for tests purposes.
 * It does not provide any messaging functionality, but allows to trigger events.
 */
public class DummyMessagingService extends AbstractMessagingService {
    private final String localNodeName;

    private final AtomicLong correlationIdGenerator = new AtomicLong();

    public DummyMessagingService(String localNodeName) {
        this.localNodeName = localNodeName;
    }

    /** {@inheritDoc} */
    @Override
    public void weakSend(ClusterNode recipient, NetworkMessage msg) {
        throw new AssertionError("Not implemented yet");
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Void> send(ClusterNode recipient, ChannelType channelType, NetworkMessage msg) {
        throw new AssertionError("Not implemented yet");
    }

    @Override
    public CompletableFuture<Void> send(String recipientConsistentId, ChannelType channelType, NetworkMessage msg) {
        throw new AssertionError("Not implemented yet");
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
        getMessageHandlers(msg.groupType()).forEach(h -> h.onReceived(msg, localNodeName, correlationIdGenerator.getAndIncrement()));

        return CompletableFuture.completedFuture(null);
    }

}
