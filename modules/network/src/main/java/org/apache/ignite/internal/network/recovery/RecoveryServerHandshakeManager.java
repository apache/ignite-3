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

package org.apache.ignite.internal.network.recovery;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.network.NetworkMessagesFactory;
import org.apache.ignite.internal.network.handshake.HandshakeException;
import org.apache.ignite.internal.network.handshake.HandshakeManager;
import org.apache.ignite.internal.network.netty.HandshakeHandler;
import org.apache.ignite.internal.network.netty.MessageHandler;
import org.apache.ignite.internal.network.netty.NettySender;
import org.apache.ignite.internal.network.netty.NettyUtils;
import org.apache.ignite.internal.network.netty.PipelineUtils;
import org.apache.ignite.internal.network.recovery.message.HandshakeFinishMessage;
import org.apache.ignite.internal.network.recovery.message.HandshakeStartMessage;
import org.apache.ignite.internal.network.recovery.message.HandshakeStartResponseMessage;
import org.apache.ignite.network.NetworkMessage;
import org.apache.ignite.network.OutNetworkObject;
import org.jetbrains.annotations.TestOnly;

/**
 * Recovery protocol handshake manager for a server.
 */
public class RecoveryServerHandshakeManager implements HandshakeManager {
    /** Launch id. */
    private final UUID launchId;

    /** Consistent id. */
    private final String consistentId;

    /** Message factory. */
    private final NetworkMessagesFactory messageFactory;

    /** Handshake completion future. */
    private final CompletableFuture<NettySender> handshakeCompleteFuture = new CompletableFuture<>();

    /** Remote node's launch id. */
    private UUID remoteLaunchId;

    /** Remote node's consistent id. */
    private String remoteConsistentId;

    /** Netty pipeline channel handler context. */
    private ChannelHandlerContext ctx;

    /** Channel. */
    private Channel channel;

    /** Netty pipeline handshake handler. */
    private HandshakeHandler handler;

    /** Count of messages received by the remote node. */
    private long receivedCount;

    /** Recovery descriptor provider. */
    private final RecoveryDescriptorProvider recoveryDescriptorProvider;

    /** Recovery descriptor. */
    private RecoveryDescriptor recoveryDescriptor;

    /**
     * Constructor.
     *
     * @param launchId Launch id.
     * @param consistentId Consistent id.
     * @param messageFactory Message factory.
     * @param recoveryDescriptorProvider Recovery descriptor provider.
     */
    public RecoveryServerHandshakeManager(
            UUID launchId, String consistentId, NetworkMessagesFactory messageFactory,
            RecoveryDescriptorProvider recoveryDescriptorProvider) {
        this.launchId = launchId;
        this.consistentId = consistentId;
        this.messageFactory = messageFactory;
        this.recoveryDescriptorProvider = recoveryDescriptorProvider;
    }

    /** {@inheritDoc} */
    @Override
    public void onInit(ChannelHandlerContext handlerContext) {
        this.ctx = handlerContext;
        this.channel = handlerContext.channel();
        this.handler = (HandshakeHandler) ctx.handler();
    }

    /** {@inheritDoc} */
    @Override
    public void onConnectionOpen() {
        HandshakeStartMessage handshakeStartMessage = messageFactory.handshakeStartMessage()
                .launchId(launchId)
                .consistentId(consistentId)
                .build();

        ChannelFuture sendFuture = channel.writeAndFlush(new OutNetworkObject(handshakeStartMessage, Collections.emptyList(), false));

        NettyUtils.toCompletableFuture(sendFuture).whenComplete((unused, throwable) -> {
            if (throwable != null) {
                handshakeCompleteFuture.completeExceptionally(
                        new HandshakeException("Failed to send handshake start message: " + throwable.getMessage(), throwable)
                );
            }
        });
    }

    /** {@inheritDoc} */
    @Override
    public void onMessage(NetworkMessage message) {
        if (message instanceof HandshakeStartResponseMessage) {
            HandshakeStartResponseMessage msg = (HandshakeStartResponseMessage) message;

            this.remoteLaunchId = msg.launchId();
            this.remoteConsistentId = msg.consistentId();
            this.receivedCount = msg.receivedCount();

            this.recoveryDescriptor = recoveryDescriptorProvider.getRecoveryDescriptor(remoteConsistentId, remoteLaunchId,
                    msg.connectionId(), true);

            handshake(recoveryDescriptor);

            return;
        }

        assert recoveryDescriptor != null : "Wrong server handshake flow";

        if (recoveryDescriptor.unacknowledgedCount() == 0) {
            finishHandshake();
        }

        ctx.fireChannelRead(message);
    }

    private void handshake(RecoveryDescriptor descriptor) {
        PipelineUtils.afterHandshake(ctx.pipeline(), descriptor, createMessageHandler(), messageFactory);

        HandshakeFinishMessage response = messageFactory.handshakeFinishMessage()
                .receivedCount(descriptor.receivedCount())
                .build();

        CompletableFuture<Void> sendFuture = NettyUtils.toCompletableFuture(
                channel.write(new OutNetworkObject(response, Collections.emptyList(), false))
        );

        descriptor.acknowledge(receivedCount);

        int unacknowledgedCount = descriptor.unacknowledgedCount();

        if (unacknowledgedCount > 0) {
            var futs = new CompletableFuture[unacknowledgedCount + 1];
            futs[0] = sendFuture;

            List<OutNetworkObject> networkMessages = descriptor.unacknowledgedMessages();

            for (int i = 0; i < networkMessages.size(); i++) {
                OutNetworkObject networkMessage = networkMessages.get(i);
                futs[i + 1] = NettyUtils.toCompletableFuture(channel.write(networkMessage));
            }

            sendFuture = CompletableFuture.allOf(futs);
        }

        channel.flush();

        boolean hasUnacknowledgedMessages = unacknowledgedCount > 0;

        sendFuture.whenComplete((unused, throwable) -> {
            if (throwable != null) {
                handshakeCompleteFuture.completeExceptionally(
                        new HandshakeException("Failed to send handshake response: " + throwable.getMessage(), throwable)
                );
            } else if (!hasUnacknowledgedMessages) {
                finishHandshake();
            }
        });
    }

    /**
     * Creates a message handler using the consistent id of a remote node.
     *
     * @return New message handler.
     */
    private MessageHandler createMessageHandler() {
        return handler.createMessageHandler(remoteConsistentId);
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<NettySender> handshakeFuture() {
        return handshakeCompleteFuture;
    }

    /**
     * Finishes handshaking process by removing handshake handler from the pipeline and creating a {@link NettySender}.
     */
    private void finishHandshake() {
        // Removes handshake handler from the pipeline as the handshake is finished
        this.ctx.pipeline().remove(this.handler);

        handshakeCompleteFuture.complete(new NettySender(channel, remoteLaunchId.toString(), remoteConsistentId));
    }

    @TestOnly
    public RecoveryDescriptor recoveryDescriptor() {
        return recoveryDescriptor;
    }
}
