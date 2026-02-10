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

package org.apache.ignite.internal.network;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import org.apache.ignite.internal.network.annotations.Marshallable;
import org.apache.ignite.internal.network.annotations.MessageGroup;
import org.apache.ignite.internal.thread.ExecutorChooser;
import org.apache.ignite.network.NetworkAddress;

/**
 * Entry point for sending messages between network members in both weak and patient mode.
 */
// TODO: allow removing event handlers, see https://issues.apache.org/jira/browse/IGNITE-14519
public interface MessagingService {
    /**
     * Send the given message asynchronously to the specific member without any delivery guarantees.
     *
     * @param recipient Recipient of the message.
     * @param msg Message which should be delivered.
     */
    void weakSend(InternalClusterNode recipient, NetworkMessage msg);

    /**
     * Tries to send the given message via default channel asynchronously to the specific cluster member.
     *
     * <p>Guarantees:
     * <ul>
     *     <li>Messages send to same receiver will be delivered in the same order as they were sent;</li>
     *     <li>If a message N has been successfully delivered to a member implies that all messages to same receiver
     *     preceding N have also been successfully delivered.</li>
     *     <li>A message is delivered only once.</li>
     * </ul>
     *
     * <p>Please note that the guarantees only work for same (sender, receiver) pairs. That is, if A sends m1 and m2
     * to B, then the guarantees are maintained. If, on the other hand, A sends m1 to B and m2 to C, then no guarantees
     * exist.
     *
     * <p>If the recipient is not in the physical topology anymore, the result future will be completed with
     * a {@link RecipientLeftException}. This also relates to the case when a node with ID different from the one in
     * the {@link InternalClusterNode} object is found on the other side of the channel.
     *
     * @param recipient Recipient of the message.
     * @param msg Message which should be delivered.
     * @return Future of the send operation.
     */
    default CompletableFuture<Void> send(InternalClusterNode recipient, NetworkMessage msg) {
        return send(recipient, ChannelType.DEFAULT, msg);
    }

    /**
     * Tries to send the given message via specified channel asynchronously to the specific cluster member.
     *
     * <p>Guarantees:
     * <ul>
     *     <li>Messages send to same receiver will be delivered in the same order as they were sent;</li>
     *     <li>If a message N has been successfully delivered to a member implies that all messages to same receiver
     *     preceding N have also been successfully delivered.</li>
     *     <li>A message is delivered only once</li>
     * </ul>
     *
     * <p>Please note that the guarantees only work for same (sender, receiver) pairs. That is, if A sends m1 and m2
     * to B, then the guarantees are maintained. If, on the other hand, A sends m1 to B and m2 to C, then no guarantees
     * exist.
     *
     * <p>If the recipient is not in the physical topology anymore, the result future will be completed with
     * a {@link RecipientLeftException}. This also relates to the case when a node with ID different from the one in
     * the {@link InternalClusterNode} object is found on the other side of the channel.
     *
     * @param recipient Recipient of the message.
     * @param msg Message which should be delivered.
     * @return Future of the send operation.
     */
    CompletableFuture<Void> send(InternalClusterNode recipient, ChannelType channelType, NetworkMessage msg);

    /**
     * Tries to send the given message via specified channel asynchronously to the specific cluster member.
     *
     * <p>Guarantees:
     * <ul>
     *     <li>Messages send to same receiver will be delivered in the same order as they were sent;</li>
     *     <li>If a message N has been successfully delivered to a member implies that all messages to same receiver
     *     preceding N have also been successfully delivered.</li>
     * </ul>
     *
     * <p>Please note that the guarantees only work for same (sender, receiver) pairs. That is, if A sends m1 and m2
     * to B, then the guarantees are maintained. If, on the other hand, A sends m1 to B and m2 to C, then no guarantees
     * exist.
     *
     * @param recipientConsistentId Consistent ID of the recipient of the message.
     * @param msg Message which should be delivered.
     * @return Future of the send operation.
     */
    CompletableFuture<Void> send(String recipientConsistentId, ChannelType channelType, NetworkMessage msg);

    /**
     * Tries to send the given message via specified channel asynchronously to the specific cluster member by its network address.
     *
     * <p>Guarantees:
     * <ul>
     *     <li>Messages send to same receiver will be delivered in the same order as they were sent;</li>
     *     <li>If a message N has been successfully delivered to a member implies that all messages to same receiver
     *     preceding N have also been successfully delivered.</li>
     * </ul>
     *
     * <p>Please note that the guarantees only work for same (sender, receiver) pairs. That is, if A sends m1 and m2
     * to B, then the guarantees are maintained. If, on the other hand, A sends m1 to B and m2 to C, then no guarantees
     * exist.
     *
     * @param recipientNetworkAddress Network address of the recipient of the message.
     * @param msg Message which should be delivered.
     * @return Future of the send operation.
     */
    CompletableFuture<Void> send(NetworkAddress recipientNetworkAddress, ChannelType channelType, NetworkMessage msg);

    /**
     * Sends a response to a {@link #invoke} request.
     * Guarantees are the same as for the {@link #send(InternalClusterNode, NetworkMessage)}.
     *
     * <p>If the recipient is not in the physical topology anymore, the result future will be completed with
     * a {@link RecipientLeftException}. This also relates to the case when a node with ID different from the one in
     * the {@link InternalClusterNode} object is found on the other side of the channel.
     *
     * @param recipient     Recipient of the message.
     * @param msg           Message which should be delivered.
     * @param correlationId Correlation id when replying to the request.
     * @return Future of the send operation.
     */
    default CompletableFuture<Void> respond(InternalClusterNode recipient, NetworkMessage msg, long correlationId) {
        return respond(recipient, ChannelType.DEFAULT, msg, correlationId);
    }

    /**
     * Sends a response to a {@link #invoke} request.
     * Guarantees are the same as for the {@link #send(InternalClusterNode, NetworkMessage)}.
     *
     * <p>If the recipient is not in the physical topology anymore, the result future will be completed with
     * a {@link RecipientLeftException}. This also relates to the case when a node with ID different from the one in
     * the {@link InternalClusterNode} object is found on the other side of the channel.
     *
     * @param recipient     Recipient of the message.
     * @param msg           Message which should be delivered.
     * @param correlationId Correlation id when replying to the request.
     * @return Future of the send operation.
     */
    CompletableFuture<Void> respond(InternalClusterNode recipient, ChannelType channelType, NetworkMessage msg, long correlationId);

    /**
     * Sends a response to a {@link #invoke} request via default channel.
     * Guarantees are the same as for the {@link #send(InternalClusterNode, NetworkMessage)}.
     *
     * <p>If the recipient cannot be resolved (because it has already left the physical topology), the returned future is resolved
     * with the corresponding exception ({@link UnresolvableConsistentIdException}).
     *
     * @param recipientConsistentId Consistent ID of the recipient of the message.
     * @param msg Message which should be delivered.
     * @param correlationId Correlation id when replying to the request.
     * @return Future of the send operation.
     */
    default CompletableFuture<Void> respond(String recipientConsistentId, NetworkMessage msg, long correlationId) {
        return respond(recipientConsistentId, ChannelType.DEFAULT, msg, correlationId);
    }

    /**
     * Sends a response to a {@link #invoke} request via specified channel.
     * Guarantees are the same as for the {@link #send(InternalClusterNode, NetworkMessage)}.
     *
     * <p>If the recipient cannot be resolved (because it has already left the physical topology), the returned future is resolved
     * with the corresponding exception ({@link UnresolvableConsistentIdException}).
     *
     * @param recipientConsistentId Consistent ID of the recipient of the message.
     * @param channelType Channel which will be used to message transfer.
     * @param msg Message which should be delivered.
     * @param correlationId Correlation id when replying to the request.
     * @return Future of the send operation.
     */
    CompletableFuture<Void> respond(String recipientConsistentId, ChannelType channelType, NetworkMessage msg, long correlationId);

    /**
     * Sends a message via default channel asynchronously with same guarantees as
     * {@link #send(InternalClusterNode, NetworkMessage)} and returns a future that will be
     * completed successfully upon receiving a response.
     *
     * <p>If the recipient is not in the physical topology anymore, the result future will be completed with
     * a {@link RecipientLeftException}. This also relates to the case when a node with ID different from the one in
     * the {@link InternalClusterNode} object is found on the other side of the channel.
     *
     * @param recipient Recipient of the message.
     * @param msg       The message.
     * @param timeout   Waiting for response timeout in milliseconds.
     * @return A future holding the response or error if the expected response was not received.
     */
    default CompletableFuture<NetworkMessage> invoke(InternalClusterNode recipient, NetworkMessage msg, long timeout) {
        return invoke(recipient, ChannelType.DEFAULT, msg, timeout);
    }

    /**
     * Sends a message asynchronously  via specified channel with same guarantees as
     * {@link #send(InternalClusterNode, NetworkMessage)} and returns a future that will be
     * completed successfully upon receiving a response.
     *
     * <p>If the recipient is not in the physical topology anymore, the result future will be completed with
     * a {@link RecipientLeftException}. This also relates to the case when a node with ID different from the one in
     * the {@link InternalClusterNode} object is found on the other side of the channel.
     *
     * @param recipient Recipient of the message.
     * @param channelType Channel which will be used to message transfer.
     * @param msg Message which should be delivered.
     * @param timeout Waiting for response timeout in milliseconds.
     * @return A future holding the response or error if the expected response was not received.
     */
    CompletableFuture<NetworkMessage> invoke(InternalClusterNode recipient, ChannelType channelType, NetworkMessage msg, long timeout);

    /**
     * Sends a message via default channel asynchronously with same guarantees as
     * {@link #send(InternalClusterNode, NetworkMessage)} and returns a future that will be
     * completed successfully upon receiving a response.
     *
     * @param recipientConsistentId Consistent ID of the recipient of the message.
     * @param msg The message.
     * @param timeout Waiting for response timeout in milliseconds.
     * @return A future holding the response or error if the expected response was not received.
     */
    default CompletableFuture<NetworkMessage> invoke(String recipientConsistentId, NetworkMessage msg, long timeout) {
        return invoke(recipientConsistentId, ChannelType.DEFAULT, msg, timeout);
    }

    /**
     * Sends a message via specified channel asynchronously with same guarantees as
     * {@link #send(InternalClusterNode, NetworkMessage)} and returns a future that will be
     * completed successfully upon receiving a response.
     *
     * @param recipientConsistentId Consistent ID of the recipient of the message.
     * @param channelType Channel which will be used to message transfer.
     * @param msg The message.
     * @param timeout Waiting for response timeout in milliseconds.
     * @return A future holding the response or error if the expected response was not received.
     */
    CompletableFuture<NetworkMessage> invoke(String recipientConsistentId, ChannelType channelType, NetworkMessage msg, long timeout);

    /**
     * Registers a listener for a group of network message events. Inbound thread pool is used to handle messages.
     * Use {@link #addMessageHandler(Class, ExecutorChooser, NetworkMessageHandler)} to supply your own thread pool on which
     * to handle messages.
     *
     * <p>Message group is specified by providing a class annotated with the {@link MessageGroup} annotation.
     *
     * @param messageGroup Message group descriptor.
     * @param handler      Message handler.
     * @throws IllegalArgumentException If some handlers have already been registered for a different message group class that has the same
     *                                  ID as the given {@code messageGroup}.
     */
    void addMessageHandler(Class<?> messageGroup, NetworkMessageHandler handler);

    /**
     * Registers a listener for a group of network message events.
     *
     * <p>Message group is specified by providing a class annotated with the {@link MessageGroup} annotation.
     *
     * <p>The provided executor chooser will choose the {@link Executor} for each message coming from the network; the message will
     * be handled on this executor. It might be chosen before @{@link Marshallable} fields of the message are unmarshalled, so they
     * might be {@code null} from the point of view of the chooser.
     *
     * <p>The chooser will <b>NOT</b> be used for self-requests (that is, if the destination node is the same as current node). In such
     * cases, handlers will be invoked in the same thread that initiated the send.
     *
     * <p>The executor chooser is invoked in a network I/O thread, so it must never block.
     *
     * <p>If a few handlers handling the same message are registered with the same executor chooser, it might be called
     * only once to call all such handlers on the same thread.
     *
     * @param messageGroup Message group descriptor.
     * @param executorChooser Will choose an {@link Executor} on which to handle a message. It will be called before @{@link Marshallable}
     *     fields are unmarshalled, so they will be {@code null} from the point of view of the chooser.
     * @param handler      Message handler.
     * @throws IllegalArgumentException If some handlers have already been registered for a different message group class that has the same
     *                                  ID as the given {@code messageGroup}.
     */
    void addMessageHandler(Class<?> messageGroup, ExecutorChooser<NetworkMessage> executorChooser, NetworkMessageHandler handler);
}
