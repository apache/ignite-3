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
import static java.util.function.Function.identity;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.network.NetworkMessagesFactory;
import org.apache.ignite.internal.network.handshake.ChannelAlreadyExistsException;
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
 * Recovery protocol handshake manager for a client.
 */
public class RecoveryClientHandshakeManager implements HandshakeManager {
    private static final IgniteLogger LOG = Loggers.forClass(RecoveryClientHandshakeManager.class);

    /** Message factory. */
    private static final NetworkMessagesFactory MESSAGE_FACTORY = new NetworkMessagesFactory();

    /** Launch id. */
    private final UUID launchId;

    /** Consistent id. */
    private final String consistentId;

    /** Recovery descriptor provider. */
    private final RecoveryDescriptorProvider recoveryDescriptorProvider;

    /** Used to detect that a peer uses a stale ID. */
    private final StaleIdDetector staleIdDetector;

    private final AtomicBoolean stopping;

    /** Connection id. */
    private final short connectionId;

    /** Handshake completion future. */
    private final CompletableFuture<NettySender> localHandshakeCompleteFuture = new CompletableFuture<>();

    /**
     * Master future used to complete the handshake either with the results of this handshake of the competing one
     * (in the opposite direction), if it wins.
     */
    private final CompletableFuture<CompletionStage<NettySender>> masterHandshakeCompleteFuture = new CompletableFuture<>();

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

    private final FailureHandler failureHandler = new FailureHandler();

    /**
     * Constructor.
     *
     * @param launchId Launch id.
     * @param consistentId Consistent id.
     * @param recoveryDescriptorProvider Recovery descriptor provider.
     * @param stopping Defines whether the corresponding connection manager is stopping.
     */
    public RecoveryClientHandshakeManager(
            UUID launchId,
            String consistentId,
            short connectionId,
            RecoveryDescriptorProvider recoveryDescriptorProvider,
            StaleIdDetector staleIdDetector,
            ChannelCreationListener channelCreationListener,
            AtomicBoolean stopping
    ) {
        this.launchId = launchId;
        this.consistentId = consistentId;
        this.connectionId = connectionId;
        this.recoveryDescriptorProvider = recoveryDescriptorProvider;
        this.staleIdDetector = staleIdDetector;
        this.stopping = stopping;

        localHandshakeCompleteFuture.whenComplete((nettySender, throwable) -> {
            if (throwable != null) {
                releaseResources();

                // Complete the master future if it has not yet been completed by the competitor.
                masterHandshakeCompleteFuture.complete(localHandshakeCompleteFuture);

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
    public void onMessage(NetworkMessage message) {
        if (message instanceof HandshakeStartMessage) {
            onHandshakeStartMessage((HandshakeStartMessage) message);

            return;
        }

        if (message instanceof HandshakeRejectedMessage) {
            onHandshakeRejectedMessage((HandshakeRejectedMessage) message);

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

    private void onHandshakeStartMessage(HandshakeStartMessage message) {
        if (staleIdDetector.isIdStale(message.launchId().toString())) {
            handleStaleServerId(message);

            return;
        }

        if (stopping.get()) {
            handleRefusalToEstablishConnectionDueToStopping(message);

            return;
        }

        this.remoteLaunchId = message.launchId();
        this.remoteConsistentId = message.consistentId();

        RecoveryDescriptor descriptor = recoveryDescriptorProvider.getRecoveryDescriptor(
                remoteConsistentId,
                remoteLaunchId,
                connectionId
        );

        while (!descriptor.acquire(ctx, localHandshakeCompleteFuture)) {
            // Don't use the tie-braking logic as this handshake attempt is late: the competitor has already acquired
            // recovery descriptors on both sides, so this handshake attempt must fail regardless of the Tie Breaker's opinion.
            if (LOG.isDebugEnabled()) {
                LOG.debug("Failed to acquire recovery descriptor during handshake, it is held by: {}.", descriptor.holderDescription());
            }

            DescriptorAcquiry competitorAcquiry = descriptor.holder();
            if (competitorAcquiry == null) {
                continue;
            }

            // Complete our master future with the competitor's future. After this our local future has no effect on the final result
            // of this handshake.
            completeMasterFutureWithCompetitorHandshakeFuture(competitorAcquiry);

            return;
        }

        this.recoveryDescriptor = descriptor;

        handshake(this.recoveryDescriptor);
    }

    private void completeMasterFutureWithCompetitorHandshakeFuture(DescriptorAcquiry competitorAcquiry) {
        masterHandshakeCompleteFuture.complete(competitorAcquiry.handshakeCompleteFuture());
        localHandshakeCompleteFuture.completeExceptionally(
                new HandshakeException("Stepping aside to allow an incoming handshake from " + remoteConsistentId + " finish.")
        );
    }

    private void handleStaleServerId(HandshakeStartMessage msg) {
        String message = msg.consistentId() + ":" + msg.launchId() + " is stale, server should be restarted so that clients can connect";
        HandshakeRejectedMessage rejectionMessage = MESSAGE_FACTORY.handshakeRejectedMessage()
                .reasonString(HandshakeRejectionReason.STALE_LAUNCH_ID.name())
                .message(message)
                .build();

        sendHandshakeRejectedMessage(rejectionMessage, message);
    }

    private void handleRefusalToEstablishConnectionDueToStopping(HandshakeStartMessage msg) {
        String message = msg.consistentId() + ":" + msg.launchId() + " tried to establish a connection with " + consistentId
                + ", but it's stopping";
        HandshakeRejectedMessage rejectionMessage = MESSAGE_FACTORY.handshakeRejectedMessage()
                .reasonString(HandshakeRejectionReason.STOPPING.name())
                .message(message)
                .build();

        sendHandshakeRejectedMessage(rejectionMessage, message);
    }

    private void onHandshakeRejectedMessage(HandshakeRejectedMessage msg) {
        boolean ignorable = stopping.get() || !msg.reason().critical();

        if (ignorable) {
            LOG.debug("Handshake rejected by server: {}", msg.message());
        } else {
            LOG.warn("Handshake rejected by server: {}", msg.message());
        }

        if (msg.reason() == HandshakeRejectionReason.CLINCH) {
            giveUpClinch();
        } else {
            localHandshakeCompleteFuture.completeExceptionally(new HandshakeException(msg.message()));
        }

        if (!ignorable) {
            // TODO: IGNITE-16899 Perhaps we need to fail the node by FailureHandler
            failureHandler.handleFailure(new IgniteException("Handshake rejected by server: " + msg.message()));
        }
    }

    private void giveUpClinch() {
        RecoveryDescriptor descriptor = recoveryDescriptorProvider.getRecoveryDescriptor(
                remoteConsistentId,
                remoteLaunchId,
                connectionId
        );

        DescriptorAcquiry myAcquiry = descriptor.holder();
        assert myAcquiry != null;
        assert myAcquiry.channel() == ctx.channel() : "Expected the descriptor to be held by current channel " + ctx.channel()
                + ", but it's held by another channel " + myAcquiry.channel();

        descriptor.release(ctx);

        // Complete the future to allow the competitor that should wait on it acquire the descriptor and finish its handshake.
        myAcquiry.markClinchResolved();

        DescriptorAcquiry competitorAcquiry = descriptor.holder();
        if (competitorAcquiry != null) {
            // The competitor is available, so just complete our master future with the competitor future.
            completeMasterFutureWithCompetitorHandshakeFuture(competitorAcquiry);
        } else {
            // The competitor is not at the lock yet. Maybe it will arrive soon, maybe it will never arrive.
            // The safest thing is to just retry the whole handshake procedure.
            localHandshakeCompleteFuture.completeExceptionally(new ChannelAlreadyExistsException(remoteConsistentId));
        }
    }

    private void sendHandshakeRejectedMessage(HandshakeRejectedMessage rejectionMessage, String reason) {
        ChannelFuture sendFuture = channel.writeAndFlush(new OutNetworkObject(rejectionMessage, emptyList(), false));

        NettyUtils.toCompletableFuture(sendFuture).whenComplete((unused, throwable) -> {
            if (throwable != null) {
                localHandshakeCompleteFuture.completeExceptionally(
                        new HandshakeException("Failed to send handshake rejected message: " + throwable.getMessage(), throwable)
                );
            } else {
                localHandshakeCompleteFuture.completeExceptionally(new HandshakeException(reason));
            }
        });
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<NettySender> localHandshakeFuture() {
        return localHandshakeCompleteFuture;
    }

    /** {@inheritDoc} */
    @Override
    public CompletionStage<NettySender> finalHandshakeFuture() {
        return masterHandshakeCompleteFuture.thenCompose(identity());
    }

    private void handshake(RecoveryDescriptor descriptor) {
        PipelineUtils.afterHandshake(ctx.pipeline(), descriptor, createMessageHandler(), MESSAGE_FACTORY);

        HandshakeStartResponseMessage response = MESSAGE_FACTORY.handshakeStartResponseMessage()
                .launchId(launchId)
                .consistentId(consistentId)
                .receivedCount(descriptor.receivedCount())
                .connectionId(connectionId)
                .build();

        ChannelFuture sendFuture = ctx.channel().writeAndFlush(new OutNetworkObject(response, emptyList(), false));

        NettyUtils.toCompletableFuture(sendFuture).whenComplete((unused, throwable) -> {
            if (throwable != null) {
                localHandshakeCompleteFuture.completeExceptionally(
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
    protected void finishHandshake() {
        // Removes handshake handler from the pipeline as the handshake is finished
        this.ctx.pipeline().remove(this.handler);

        // Complete the master future with the local future of the current handshake as there was no competitor (or we won the competition).
        masterHandshakeCompleteFuture.complete(localHandshakeCompleteFuture);
        localHandshakeCompleteFuture.complete(new NettySender(channel, remoteLaunchId.toString(), remoteConsistentId, connectionId));
    }
}
