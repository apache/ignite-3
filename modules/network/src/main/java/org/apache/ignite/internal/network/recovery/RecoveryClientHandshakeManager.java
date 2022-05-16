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
 * Recovery protocol handshake manager for a client.
 */
public class RecoveryClientHandshakeManager implements HandshakeManager {
    /** Launch id. */
    private final UUID launchId;

    /** Consistent id. */
    private final String consistentId;

    /** Message factory. */
    private final NetworkMessagesFactory messageFactory;

    /** Recovery descriptor provider. */
    private final RecoveryDescriptorProvider recoveryDescriptorProvider;

    /** Connection id. */
    private final short connectionId;

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
    public RecoveryClientHandshakeManager(
            UUID launchId, String consistentId, short connectionId, NetworkMessagesFactory messageFactory,
            RecoveryDescriptorProvider recoveryDescriptorProvider) {
        this.launchId = launchId;
        this.consistentId = consistentId;
        this.connectionId = connectionId;
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
    public void onMessage(NetworkMessage message) {
        if (message instanceof HandshakeStartMessage) {
            HandshakeStartMessage msg = (HandshakeStartMessage) message;

            this.remoteLaunchId = msg.launchId();
            this.remoteConsistentId = msg.consistentId();

            this.recoveryDescriptor = recoveryDescriptorProvider.getRecoveryDescriptor(remoteConsistentId, remoteLaunchId, connectionId,
                    false);

            handshake(recoveryDescriptor);

            return;
        }

        assert recoveryDescriptor != null : "Wrong client handshake flow";

        if (message instanceof HandshakeFinishMessage) {
            HandshakeFinishMessage msg = (HandshakeFinishMessage) message;
            long receivedCount = msg.receivedCount();

            recoveryDescriptor.acknowledge(receivedCount);

            int cnt = recoveryDescriptor.unacknowledgedCount();

            if (cnt == 0) {
                finishHandshake();

                return;
            }

            List<OutNetworkObject> networkMessages = recoveryDescriptor.unacknowledgedMessages();

            for (OutNetworkObject networkMessage : networkMessages) {
                channel.write(networkMessage);
            }

            channel.flush();

            return;
        }

        int cnt = recoveryDescriptor.unacknowledgedCount();

        if (cnt == 0) {
            finishHandshake();
        }

        ctx.fireChannelRead(message);
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<NettySender> handshakeFuture() {
        return handshakeCompleteFuture;
    }

    private void handshake(RecoveryDescriptor descriptor) {
        PipelineUtils.afterHandshake(ctx.pipeline(), descriptor, createMessageHandler(), messageFactory);

        HandshakeStartResponseMessage response = messageFactory.handshakeStartResponseMessage()
                .launchId(launchId)
                .consistentId(consistentId)
                .receivedCount(descriptor.receivedCount())
                .connectionId(connectionId)
                .build();

        ChannelFuture sendFuture = ctx.channel().writeAndFlush(new OutNetworkObject(response, Collections.emptyList(), false));

        NettyUtils.toCompletableFuture(sendFuture).whenComplete((unused, throwable) -> {
            if (throwable != null) {
                handshakeCompleteFuture.completeExceptionally(
                        new HandshakeException("Failed to send handshake response: " + throwable.getMessage(), throwable)
                );
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
