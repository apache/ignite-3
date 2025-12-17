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
import static java.util.stream.Collectors.toList;
import static org.apache.ignite.internal.network.recovery.HandshakeTieBreaker.shouldCloseChannel;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.BooleanSupplier;
import java.util.function.Function;
import org.apache.ignite.internal.lang.NodeStoppingException;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.network.ClusterIdSupplier;
import org.apache.ignite.internal.network.InternalClusterNode;
import org.apache.ignite.internal.network.NetworkMessage;
import org.apache.ignite.internal.network.NetworkMessagesFactory;
import org.apache.ignite.internal.network.OutNetworkObject;
import org.apache.ignite.internal.network.TopologyService;
import org.apache.ignite.internal.network.handshake.HandshakeEventLoopSwitcher;
import org.apache.ignite.internal.network.handshake.HandshakeException;
import org.apache.ignite.internal.network.handshake.HandshakeManager;
import org.apache.ignite.internal.network.netty.ChannelCreationListener;
import org.apache.ignite.internal.network.netty.ChannelKey;
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
import org.apache.ignite.internal.network.recovery.message.ProbeMessage;
import org.apache.ignite.internal.version.IgniteProductVersionSource;

/**
 * Recovery protocol handshake manager for an acceptor (here, 'acceptor' means 'the side that accepts the connection').
 */
public class RecoveryAcceptorHandshakeManager implements HandshakeManager {
    private static final IgniteLogger LOG = Loggers.forClass(RecoveryAcceptorHandshakeManager.class);

    private static final int MAX_CLINCH_TERMINATION_AWAIT_ATTEMPTS = 1000;

    private final InternalClusterNode localNode;

    /** Message factory. */
    private final NetworkMessagesFactory messageFactory;

    /** Handshake completion future. */
    private final CompletableFuture<NettySender> handshakeCompleteFuture = new CompletableFuture<>();

    /** Remote node. */
    private InternalClusterNode remoteNode;

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

    private final HandshakeEventLoopSwitcher handshakeEventLoopSwitcher;

    /** Used to detect that a peer uses a stale ID. */
    private final StaleIdDetector staleIdDetector;

    private final ClusterIdSupplier clusterIdSupplier;

    private final BooleanSupplier stopping;

    private final IgniteProductVersionSource productVersionSource;

    /** Recovery descriptor. */
    private RecoveryDescriptor recoveryDescriptor;

    /** Cluster topology service. */
    @SuppressWarnings("FieldCanBeLocal")
    private final TopologyService topologyService;

    /**
     * Constructor.
     *
     * @param localNode {@link InternalClusterNode} representing this node.
     * @param messageFactory Message factory.
     * @param recoveryDescriptorProvider Recovery descriptor provider.
     * @param stopping Defines whether the corresponding connection manager is stopping.
     * @param productVersionSource Source of product version.
     * @param topologyService Cluster topology service.
     */
    public RecoveryAcceptorHandshakeManager(
            InternalClusterNode localNode,
            NetworkMessagesFactory messageFactory,
            RecoveryDescriptorProvider recoveryDescriptorProvider,
            HandshakeEventLoopSwitcher handshakeEventLoopSwitcher,
            StaleIdDetector staleIdDetector,
            ClusterIdSupplier clusterIdSupplier,
            ChannelCreationListener channelCreationListener,
            BooleanSupplier stopping,
            IgniteProductVersionSource productVersionSource,
            TopologyService topologyService
    ) {
        this.localNode = localNode;
        this.messageFactory = messageFactory;
        this.recoveryDescriptorProvider = recoveryDescriptorProvider;
        this.handshakeEventLoopSwitcher = handshakeEventLoopSwitcher;
        this.staleIdDetector = staleIdDetector;
        this.clusterIdSupplier = clusterIdSupplier;
        this.stopping = stopping;
        this.productVersionSource = productVersionSource;
        this.topologyService = topologyService;

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
        sendHandshakeStartMessage();
    }

    private void sendHandshakeStartMessage() {
        HandshakeStartMessage handshakeStartMessage = createHandshakeStartMessage();

        ChannelFuture sendFuture = channel.writeAndFlush(new OutNetworkObject(handshakeStartMessage, emptyList()));

        NettyUtils.toCompletableFuture(sendFuture).whenComplete((unused, throwable) -> {
            if (throwable != null) {
                handshakeCompleteFuture.completeExceptionally(
                        new HandshakeException("Failed to send handshake start message: " + throwable.getMessage(), throwable)
                );
            }
        });
    }

    protected HandshakeStartMessage createHandshakeStartMessage() {
        MyStaleNodeHandlingParams params = new MyStaleNodeHandlingParams(topologyService);

        return messageFactory.handshakeStartMessage()
                .serverNode(HandshakeManagerUtils.clusterNodeToMessage(localNode))
                .serverClusterId(clusterIdSupplier.clusterId())
                .productName(productVersionSource.productName())
                .productVersion(productVersionSource.productVersion().toString())
                .physicalTopologySize(params.physicalTopologySize())
                .minNodeName(params.minNodeName())
                .build();
    }

    /** {@inheritDoc} */
    @Override
    public void onMessage(NetworkMessage message) {
        if (message instanceof ProbeMessage) {
            // No action required, just ignore it.
            return;
        }

        if (message instanceof HandshakeRejectedMessage) {
            onHandshakeRejectedMessage((HandshakeRejectedMessage) message);

            return;
        }

        if (message instanceof HandshakeStartResponseMessage) {
            onHandshakeStartResponseMessage((HandshakeStartResponseMessage) message);

            return;
        }

        // If we are here it means that we acquired the descriptor, we already handled a HandshakeStartresponseMessage and now we are
        // getting unacked messages from another side and acks for our unacked messages that we sent there (if any).

        assert recoveryDescriptor != null : "Wrong acceptor handshake flow, message is " + message;
        assert recoveryDescriptor.holderChannel() == channel : "Expected " + channel + " but was " + recoveryDescriptor.holderChannel()
                + ", message is " + message;

        if (recoveryDescriptor.unacknowledgedCount() == 0) {
            finishHandshake();
        }

        ctx.fireChannelRead(message);
    }

    private void onHandshakeStartResponseMessage(HandshakeStartResponseMessage message) {
        if (possiblyRejectHandshakeStartResponse(message)) {
            return;
        }

        this.remoteNode = message.clientNode().asClusterNode();
        this.receivedCount = message.receivedCount();
        this.remoteChannelId = message.connectionId();

        ChannelKey channelKey = new ChannelKey(remoteNode.name(), remoteNode.id(), remoteChannelId);
        handshakeEventLoopSwitcher.switchEventLoopIfNeeded(channel, channelKey)
                .thenRun(() -> tryAcquireDescriptorAndFinishHandshake(message));
    }

    private boolean possiblyRejectHandshakeStartResponse(HandshakeStartResponseMessage message) {
        if (stopping.getAsBoolean()) {
            handleRefusalToEstablishConnectionDueToStopping(message);

            return true;
        }

        if (staleIdDetector.isIdStale(message.clientNode().id())) {
            handleStaleInitiatorId(message);

            return true;
        }

        return false;
    }

    private void handleStaleInitiatorId(HandshakeStartResponseMessage msg) {


        String message = String.format("%s:%s is stale, it should be restarted to be allowed to connect",
                msg.clientNode().name(), msg.clientNode().id()
        );

        sendRejectionMessageAndFailHandshake(message, HandshakeRejectionReason.STALE_LAUNCH_ID, HandshakeException::new);
    }

    private void handleRefusalToEstablishConnectionDueToStopping(HandshakeStartResponseMessage msg) {
        String message = String.format("%s:%s tried to establish a connection with %s, but it's stopping",
                msg.clientNode().name(), msg.clientNode().id(), localNode.name()
        );

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

    private void tryAcquireDescriptorAndFinishHandshake(HandshakeStartResponseMessage message) {
        tryAcquireDescriptorAndFinishHandshake(message, 0);
    }

    private void tryAcquireDescriptorAndFinishHandshake(HandshakeStartResponseMessage handshakeStartResponse, int attempt) {
        if (attempt > MAX_CLINCH_TERMINATION_AWAIT_ATTEMPTS) {
            throw new IllegalStateException("Too many attempts during handshake from " + remoteNode.name() + "(" + remoteNode.id()
                    + ":" + remoteChannelId + ") via " + ctx.channel());
        }

        RecoveryDescriptor descriptor = recoveryDescriptorProvider.getRecoveryDescriptor(
                remoteNode.name(),
                remoteNode.id(),
                remoteChannelId
        );

        while (!descriptor.tryAcquire(ctx, handshakeCompleteFuture)) {
            if (shouldCloseChannel(localNode.id(), remoteNode.id())) {
                // A competitor is holding the descriptor and we win the clinch; so we need to wait on the 'clinch resolved' future till
                // the competitor realises it should terminate (this realization will happen on the other side of the channel), send
                // the corresponding message to this node, terminate its handshake and complete the 'clinch resolved' future.
                DescriptorAcquiry competitorAcquiry = descriptor.holder();

                if (competitorAcquiry == null) {
                    continue;
                }

                // Ok, there is a competitor. This might be a genuine competitor doing a handshake from the other side, or it might be
                // a core component that is doing maintenance: cleanup after the other side has left the topology or because our node
                // is stopping. If the latter is true, it's no use to wait (they will never release the descriptor), but this only
                // happens if the other side's ID is stale or stopping is true, so let's check it.
                if (possiblyRejectHandshakeStartResponse(handshakeStartResponse)) {
                    return;
                }

                competitorAcquiry.clinchResolved().whenComplete((res, ex) -> {
                    // The competitor has finished terminating its handshake, it must've already released the descriptor,
                    // so let's try again.
                    if (ctx.executor().inEventLoop()) {
                        tryAcquireDescriptorAndFinishHandshake(handshakeStartResponse, attempt + 1);
                    } else {
                        ctx.executor().execute(() -> tryAcquireDescriptorAndFinishHandshake(handshakeStartResponse, attempt + 1));
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

        // Now that we hold the descriptor, let's check again if the other side has left the topology or we are already stopping.
        // This allows to avoid a race between MessagingService/ConnectionManager handling node leave/local node stop and
        // a concurrent handshake. If one of these happened, we are releasing the descriptor to allow the common machinery
        // to acquire it and clean it up.
        if (possiblyRejectHandshakeStartResponse(handshakeStartResponse)) {
            descriptor.release(ctx);
            return;
        }

        this.recoveryDescriptor = descriptor;

        handshake(descriptor);
    }

    private void onHandshakeRejectedMessage(HandshakeRejectedMessage msg) {
        msg.reason().print(stopping.getAsBoolean(), LOG, "Handshake rejected by initiator: {}", msg.message());

        handshakeCompleteFuture.completeExceptionally(HandshakeManagerUtils.createExceptionFromRejectionMessage(msg));
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
                channel.write(new OutNetworkObject(response, emptyList()))
        );

        descriptor.acknowledge(receivedCount);

        int unacknowledgedCount = descriptor.unacknowledgedCount();

        if (unacknowledgedCount > 0) {
            var futs = new CompletableFuture[unacknowledgedCount + 1];
            futs[0] = sendFuture;

            List<OutNetworkObject> networkMessages = descriptor.unacknowledgedMessages();
            if (LOG.isDebugEnabled()) {
                LOG.debug("Resending on handshake: {}", networkMessages.stream().map(OutNetworkObject::networkMessage).collect(toList()));
            }

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
        return handler.createMessageHandler(remoteNode, remoteChannelId);
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

        handshakeCompleteFuture.complete(
                new NettySender(channel, remoteNode.id(), remoteNode.name(), remoteChannelId, recoveryDescriptor)
        );
    }
}
