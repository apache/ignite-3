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
import static org.apache.ignite.internal.network.recovery.HandshakeTieBreaker.shouldCloseChannel;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import org.apache.ignite.internal.lang.NodeStoppingException;
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
import org.apache.ignite.internal.network.recovery.message.HandshakeRejectionReason;
import org.apache.ignite.internal.network.recovery.message.HandshakeStartMessage;
import org.apache.ignite.internal.network.recovery.message.HandshakeStartResponseMessage;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.network.NetworkMessage;
import org.apache.ignite.network.OutNetworkObject;

/**
 * Recovery protocol handshake manager for a server.
 */
public class RecoveryServerHandshakeManager implements HandshakeManager {
    private static final IgniteLogger LOG = Loggers.forClass(RecoveryServerHandshakeManager.class);

    private static final int MAX_CLINCH_TERMINATION_AWAIT_ATTEMPTS = 1000;

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

    private final AtomicBoolean stopping;

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
     * @param stopping Defines whether the corresponding connection manager is stopping.
     */
    public RecoveryServerHandshakeManager(
            UUID launchId,
            String consistentId,
            NetworkMessagesFactory messageFactory,
            RecoveryDescriptorProvider recoveryDescriptorProvider,
            StaleIdDetector staleIdDetector,
            ChannelCreationListener channelCreationListener,
            AtomicBoolean stopping
    ) {
        this.launchId = launchId;
        this.consistentId = consistentId;
        this.messageFactory = messageFactory;
        this.recoveryDescriptorProvider = recoveryDescriptorProvider;
        this.staleIdDetector = staleIdDetector;
        this.stopping = stopping;

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
            onHandshakeStartResponseMessage((HandshakeStartResponseMessage) message);

            return;
        }

        if (message instanceof HandshakeRejectedMessage) {
            onHandshakeRejectedMessage((HandshakeRejectedMessage) message);

            return;
        }

        assert recoveryDescriptor != null : "Wrong server handshake flow";

        if (recoveryDescriptor.unacknowledgedCount() == 0) {
            finishHandshake();
        }

        ctx.fireChannelRead(message);
    }

    private void onHandshakeStartResponseMessage(HandshakeStartResponseMessage message) {
        UUID remoteLaunchId = message.launchId();
        String remoteConsistentId = message.consistentId();
        long remoteReceivedCount = message.receivedCount();
        short remoteChannelId = message.connectionId();

        if (staleIdDetector.isIdStale(remoteLaunchId.toString())) {
            handleStaleClientId(message);

            return;
        }

        if (stopping.get()) {
            handleRefusalToEstablishConnectionDueToStopping(message);

            return;
        }

        this.remoteLaunchId = remoteLaunchId;
        this.remoteConsistentId = remoteConsistentId;
        this.receivedCount = remoteReceivedCount;
        this.remoteChannelId = remoteChannelId;

        tryAcquireDescriptorAndFinishHandshake();
    }

    private void handleStaleClientId(HandshakeStartResponseMessage msg) {
        String message = msg.consistentId() + ":" + msg.launchId() + " is stale, client should be restarted to be allowed to connect";

        sendRejectionMessageAndFailHandshake(message, HandshakeRejectionReason.STALE_LAUNCH_ID, HandshakeException::new);
    }

    private void handleRefusalToEstablishConnectionDueToStopping(HandshakeStartResponseMessage msg) {
        String message = msg.consistentId() + ":" + msg.launchId() + " tried to establish a connection with " + consistentId
                + ", but it's stopping";

        sendRejectionMessageAndFailHandshake(message, HandshakeRejectionReason.STOPPING, m -> new NodeStoppingException());
    }

    private void sendRejectionMessageAndFailHandshake(
            String message,
            HandshakeRejectionReason rejectionReason,
            Function<String, Exception> exceptionFactory
    ) {
        HandshakeManagerUtils.sendRejectionMessageAndFailHandshake(
                message,
                rejectionReason,
                channel,
                handshakeCompleteFuture,
                exceptionFactory
        );
    }

    private void tryAcquireDescriptorAndFinishHandshake() {
        tryAcquireDescriptorAndFinishHandshake(0);
    }

    private void tryAcquireDescriptorAndFinishHandshake(int attempt) {
        if (attempt > MAX_CLINCH_TERMINATION_AWAIT_ATTEMPTS) {
            throw new IllegalStateException("Too many attempts during handshake from " + remoteConsistentId + "(" + remoteLaunchId
                    + ":" + remoteChannelId + ") via " + ctx.channel());
        }

        RecoveryDescriptor descriptor = recoveryDescriptorProvider.getRecoveryDescriptor(
                this.remoteConsistentId,
                this.remoteLaunchId,
                this.remoteChannelId
        );

        while (!descriptor.acquire(ctx, handshakeCompleteFuture)) {
            if (shouldCloseChannel(launchId, remoteLaunchId)) {
                // A competitor is holding the descriptor and we win the clinch; so we need to wait on the 'clinch resolved' future till
                // the competitor realises it should terminate (this realization will happen on the other side of the channel), send
                // the corresponding message to this node, terminate its handshake and complete the 'clinch resolved' future.
                DescriptorAcquiry competitorAcquiry = descriptor.holder();

                if (competitorAcquiry == null) {
                    continue;
                }

                competitorAcquiry.clinchResolved().whenComplete((res, ex) -> {
                    // The competitor has finished terminating its handshake, it must've already released the descriptor,
                    // so let's try again.
                    if (ctx.executor().inEventLoop()) {
                        tryAcquireDescriptorAndFinishHandshake(attempt + 1);
                    } else {
                        ctx.executor().execute(() -> tryAcquireDescriptorAndFinishHandshake(attempt + 1));
                    }
                });

                return;
            } else {
                // A competitor is holding the descriptor and we lose the clinch; so we need to send the correspnding message
                // to the other side, where the code handling the message will terminate our handshake and complete the 'clinch resolved'
                // future, making it possible for the competitor to proceed and finish the handshake.
                rejectHandshakeDueToLosingClinch(descriptor);

                return;
            }
        }

        this.recoveryDescriptor = descriptor;

        handshake(descriptor);
    }

    private void onHandshakeRejectedMessage(HandshakeRejectedMessage msg) {
        boolean ignorable = stopping.get() || !msg.reason().critical();

        if (ignorable) {
            LOG.debug("Handshake rejected by client: {}", msg.message());
        } else {
            LOG.warn("Handshake rejected by client: {}", msg.message());
        }

        handshakeCompleteFuture.completeExceptionally(new HandshakeException(msg.message()));

        if (!ignorable) {
            // TODO: IGNITE-16899 Perhaps we need to fail the node by FailureHandler
            failureHandler.handleFailure(new IgniteException("Handshake rejected by client: " + msg.message()));
        }
    }

    private void rejectHandshakeDueToLosingClinch(RecoveryDescriptor descriptor) {
        String localErrorMessage = "Failed to acquire recovery descriptor during handshake, it is held by: "
                + descriptor.holderDescription();

        LOG.debug(localErrorMessage);

        HandshakeManagerUtils.sendRejectionMessageAndFailHandshake(
                localErrorMessage,
                "Handshake clinch detected, this handshake will be terminated, winning channel is " + descriptor.holderDescription(),
                HandshakeRejectionReason.CLINCH,
                channel,
                handshakeCompleteFuture,
                HandshakeException::new
        );
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
    public CompletableFuture<NettySender> localHandshakeFuture() {
        return handshakeCompleteFuture;
    }

    @Override
    public CompletionStage<NettySender> finalHandshakeFuture() {
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
}
