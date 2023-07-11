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

package org.apache.ignite.internal.network.recovery;

import static java.util.Collections.emptyList;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.network.NetworkMessagesFactory;
import org.apache.ignite.internal.network.handshake.HandshakeException;
import org.apache.ignite.internal.network.handshake.HandshakeManager;
import org.apache.ignite.internal.network.netty.ChannelCreationListener;
import org.apache.ignite.internal.network.netty.HandshakeHandler;
import org.apache.ignite.internal.network.netty.MessageHandler;
import org.apache.ignite.internal.network.netty.NettySender;
import org.apache.ignite.internal.network.netty.NettyUtils;
import org.apache.ignite.internal.network.netty.PipelineUtils;
import org.apache.ignite.internal.network.recovery.message.HandshakeFinishMessage;
import org.apache.ignite.internal.network.recovery.message.HandshakeRejectedMessage;
import org.apache.ignite.internal.network.recovery.message.HandshakeStartMessage;
import org.apache.ignite.internal.network.recovery.message.HandshakeStartResponseMessage;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.network.NetworkMessage;
import org.apache.ignite.network.OutNetworkObject;
import org.jetbrains.annotations.TestOnly;

/**
 * Recovery protocol handshake manager for a server.
 */
public class RecoveryServerHandshakeManager implements HandshakeManager {
    private static final IgniteLogger LOG = Loggers.forClass(RecoveryServerHandshakeManager.class);

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

    private short remoteChannelId;

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

    /** Used to detect that a peer uses a stale ID. */
    private final StaleIdDetector staleIdDetector;

    /** Recovery descriptor. */
    private RecoveryDescriptor recoveryDescriptor;

    private final FailureHandler failureHandler = new FailureHandler();

    /**
     * Constructor.
     *
     * @param launchId Launch id.
     * @param consistentId Consistent id.
     * @param messageFactory Message factory.
     * @param recoveryDescriptorProvider Recovery descriptor provider.
     */
    public RecoveryServerHandshakeManager(
            UUID launchId,
            String consistentId,
            NetworkMessagesFactory messageFactory,
            RecoveryDescriptorProvider recoveryDescriptorProvider,
            StaleIdDetector staleIdDetector,
            ChannelCreationListener channelCreationListener
    ) {
        this.launchId = launchId;
        this.consistentId = consistentId;
        this.messageFactory = messageFactory;
        this.recoveryDescriptorProvider = recoveryDescriptorProvider;
        this.staleIdDetector = staleIdDetector;

        this.handshakeCompleteFuture.whenComplete((nettySender, throwable) -> {
            if (throwable != null) {
                releaseResources();

                return;
            }

            channelCreationListener.handshakeFinished(nettySender);
        });
    }

    private void releaseResources() {
        assert ctx.executor().inEventLoop() : "Release resources called outside of event loop";

        RecoveryDescriptor desc = recoveryDescriptor;

        if (desc != null) {
            desc.release(ctx);
        }
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

        ChannelFuture sendFuture = channel.writeAndFlush(new OutNetworkObject(handshakeStartMessage, emptyList(), false));

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

            UUID remoteLaunchId = msg.launchId();
            String remoteConsistentId = msg.consistentId();
            long remoteReceivedCount = msg.receivedCount();
            short remoteChannelId = msg.connectionId();

            if (staleIdDetector.isIdStale(remoteLaunchId.toString())) {
                handleStaleClientId(msg);

                return;
            }

            this.remoteLaunchId = remoteLaunchId;
            this.remoteConsistentId = remoteConsistentId;
            this.receivedCount = remoteReceivedCount;
            this.remoteChannelId = remoteChannelId;

            RecoveryDescriptor descriptor = recoveryDescriptorProvider.getRecoveryDescriptor(
                    this.remoteConsistentId,
                    this.remoteLaunchId,
                    this.remoteChannelId
            );

            while (!descriptor.acquire(ctx)) {
                if (launchId.compareTo(remoteLaunchId) <= 0) {
                    Channel holderChannel = descriptor.holderChannel();

                    if (holderChannel == null) {
                        continue;
                    }

                    holderChannel.close().awaitUninterruptibly();
                } else {
                    String err = "Failed to acquire recovery descriptor during handshake, it is held by: " + descriptor.holder();

                    LOG.info(err);

                    handshakeCompleteFuture.completeExceptionally(new HandshakeException(err));

                    return;
                }
            }

            this.recoveryDescriptor = descriptor;

            handshake(descriptor);

            return;
        }

        if (message instanceof HandshakeRejectedMessage) {
            HandshakeRejectedMessage msg = (HandshakeRejectedMessage) message;

            LOG.warn("Handshake rejected by client: {}", msg.reason());

            handshakeCompleteFuture.completeExceptionally(new HandshakeException(msg.reason()));

            // TODO: IGNITE-16899 Perhaps we need to fail the node by FailureHandler
            failureHandler.handleFailure(new IgniteException("Handshake rejected by client: " + msg.reason()));

            return;
        }

        assert recoveryDescriptor != null : "Wrong server handshake flow";

        if (recoveryDescriptor.unacknowledgedCount() == 0) {
            finishHandshake();
        }

        ctx.fireChannelRead(message);
    }

    private void handleStaleClientId(HandshakeStartResponseMessage msg) {
        String reason = msg.launchId() + " is stale, client should be restarted to be allowed to connect";
        HandshakeRejectedMessage rejectionMessage = messageFactory.handshakeRejectedMessage()
                .reason(reason)
                .build();

        ChannelFuture sendFuture = channel.writeAndFlush(new OutNetworkObject(rejectionMessage, emptyList(), false));

        NettyUtils.toCompletableFuture(sendFuture).whenComplete((unused, throwable) -> {
            if (throwable != null) {
                handshakeCompleteFuture.completeExceptionally(
                        new HandshakeException("Failed to send handshake rejected message: " + throwable.getMessage(), throwable)
                );
            } else {
                handshakeCompleteFuture.completeExceptionally(new HandshakeException(reason));
            }
        });
    }

    private void handshake(RecoveryDescriptor descriptor) {
        PipelineUtils.afterHandshake(ctx.pipeline(), descriptor, createMessageHandler(), messageFactory);

        HandshakeFinishMessage response = messageFactory.handshakeFinishMessage()
                .receivedCount(descriptor.receivedCount())
                .build();

        CompletableFuture<Void> sendFuture = NettyUtils.toCompletableFuture(
                channel.write(new OutNetworkObject(response, emptyList(), false))
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

        handshakeCompleteFuture.complete(new NettySender(channel, remoteLaunchId.toString(), remoteConsistentId, remoteChannelId));
    }

    @TestOnly
    public RecoveryDescriptor recoveryDescriptor() {
        return recoveryDescriptor;
    }
}
