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

package org.apache.ignite.internal.network.wrapper;

import static java.util.function.Function.identity;

import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.network.ChannelType;
import org.apache.ignite.internal.network.InternalClusterNode;
import org.apache.ignite.internal.network.MessagingService;
import org.apache.ignite.internal.network.NetworkMessage;
import org.apache.ignite.internal.network.NetworkMessageHandler;
import org.apache.ignite.internal.thread.ExecutorChooser;
import org.apache.ignite.internal.util.CompletableFutures;
import org.apache.ignite.network.NetworkAddress;

/**
 * Decorator around {@link MessagingService} that switches the response handling to an executor chosen by
 * the provided executor chooser if the message is actually sent via network (i.e. it is sent not to this
 * same node). Otherwise, proceeds with execution in the same thread which completes the future.
 */
public class JumpToExecutorByConsistentIdAfterSend implements MessagingService {
    private final MessagingService messagingService;

    private final String localConsistentId;

    private final ExecutorChooser<NetworkMessage> executorChooser;

    /** Constructor. */
    public JumpToExecutorByConsistentIdAfterSend(
            MessagingService messagingService,
            String localConsistentId,
            ExecutorChooser<NetworkMessage> executorChooser
    ) {
        this.messagingService = messagingService;
        this.localConsistentId = localConsistentId;
        this.executorChooser = executorChooser;
    }

    @Override
    public void weakSend(InternalClusterNode recipient, NetworkMessage msg) {
        messagingService.weakSend(recipient, msg);
    }

    @Override
    public CompletableFuture<Void> send(InternalClusterNode recipient, ChannelType channelType, NetworkMessage msg) {
        CompletableFuture<Void> future = messagingService.send(recipient, channelType, msg);

        return switchResponseHandlingToAnotherThreadIfNeeded(msg, future, recipient.name());
    }

    @Override
    public CompletableFuture<Void> send(String recipientConsistentId, ChannelType channelType, NetworkMessage msg) {
        CompletableFuture<Void> future = messagingService.send(recipientConsistentId, channelType, msg);

        return switchResponseHandlingToAnotherThreadIfNeeded(msg, future, recipientConsistentId);
    }

    @Override
    public CompletableFuture<Void> send(NetworkAddress recipientNetworkAddress, ChannelType channelType, NetworkMessage msg) {
        throw new UnsupportedOperationException("Sending by network address is not supported by this implementation.");
    }

    @Override
    public CompletableFuture<Void> respond(InternalClusterNode recipient, ChannelType channelType, NetworkMessage msg, long correlationId) {
        CompletableFuture<Void> future = messagingService.respond(recipient, channelType, msg, correlationId);

        return switchResponseHandlingToAnotherThreadIfNeeded(msg, future, recipient.name());
    }

    @Override
    public CompletableFuture<Void> respond(String recipientConsistentId, ChannelType channelType, NetworkMessage msg, long correlationId) {
        CompletableFuture<Void> future = messagingService.respond(recipientConsistentId, channelType, msg, correlationId);

        return switchResponseHandlingToAnotherThreadIfNeeded(msg, future, recipientConsistentId);
    }

    @Override
    public CompletableFuture<NetworkMessage> invoke(
            InternalClusterNode recipient,
            ChannelType channelType,
            NetworkMessage msg,
            long timeout
    ) {
        CompletableFuture<NetworkMessage> future = messagingService.invoke(recipient, channelType, msg, timeout);

        return switchResponseHandlingToAnotherThreadIfNeeded(msg, future, recipient.name());
    }

    @Override
    public CompletableFuture<NetworkMessage> invoke(String recipientConsistentId, ChannelType channelType, NetworkMessage msg,
            long timeout) {
        CompletableFuture<NetworkMessage> future = messagingService.invoke(recipientConsistentId, channelType, msg, timeout);

        return switchResponseHandlingToAnotherThreadIfNeeded(msg, future, recipientConsistentId);
    }

    private <T> CompletableFuture<T> switchResponseHandlingToAnotherThreadIfNeeded(
            NetworkMessage msg,
            CompletableFuture<T> future,
            String recipientConsistentId
    ) {
        if (future.isDone()) {
            return future;
        }

        if (isSelf(recipientConsistentId)) {
            return future;
        }

        return future.handleAsync(CompletableFutures::completedOrFailedFuture, executorChooser.choose(msg))
                .thenCompose(identity());
    }

    private boolean isSelf(String recipientConsistentId) {
        return localConsistentId.equals(recipientConsistentId);
    }

    @Override
    public void addMessageHandler(Class<?> messageGroup, NetworkMessageHandler handler) {
        messagingService.addMessageHandler(messageGroup, handler);
    }

    @Override
    public void addMessageHandler(Class<?> messageGroup, ExecutorChooser<NetworkMessage> executorChooser, NetworkMessageHandler handler) {
        messagingService.addMessageHandler(messageGroup, executorChooser, handler);
    }
}
