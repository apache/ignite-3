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
import static java.util.stream.Collectors.toList;
import static org.apache.ignite.internal.network.netty.NettyUtils.toCompletableFuture;
import static org.apache.ignite.internal.network.recovery.HandshakeManagerUtils.clusterNodeToMessage;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import java.io.IOException;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.BooleanSupplier;
import java.util.function.Function;
import org.apache.ignite.internal.failure.FailureProcessor;
import org.apache.ignite.internal.lang.NodeStoppingException;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.network.ClusterIdSupplier;
import org.apache.ignite.internal.network.InternalClusterNode;
import org.apache.ignite.internal.network.NetworkMessage;
import org.apache.ignite.internal.network.NetworkMessagesFactory;
import org.apache.ignite.internal.network.OutNetworkObject;
import org.apache.ignite.internal.network.RecipientLeftException;
import org.apache.ignite.internal.network.TopologyService;
import org.apache.ignite.internal.network.handshake.ChannelAlreadyExistsException;
import org.apache.ignite.internal.network.handshake.HandshakeEventLoopSwitcher;
import org.apache.ignite.internal.network.handshake.HandshakeException;
import org.apache.ignite.internal.network.handshake.HandshakeManager;
import org.apache.ignite.internal.network.netty.ChannelCreationListener;
import org.apache.ignite.internal.network.netty.ChannelKey;
import org.apache.ignite.internal.network.netty.HandshakeHandler;
import org.apache.ignite.internal.network.netty.MessageHandler;
import org.apache.ignite.internal.network.netty.NettySender;
import org.apache.ignite.internal.network.netty.PipelineUtils;
import org.apache.ignite.internal.network.recovery.message.HandshakeFinishMessage;
import org.apache.ignite.internal.network.recovery.message.HandshakeRejectedMessage;
import org.apache.ignite.internal.network.recovery.message.HandshakeRejectionReason;
import org.apache.ignite.internal.network.recovery.message.HandshakeStartMessage;
import org.apache.ignite.internal.network.recovery.message.HandshakeStartResponseMessage;
import org.apache.ignite.internal.network.recovery.message.ProbeMessage;
import org.apache.ignite.internal.version.IgniteProductVersionSource;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

/**
 * Recovery protocol handshake manager for an initiator (here, 'initiator' means 'the side that opens the connection').
 */
public class RecoveryInitiatorHandshakeManager implements HandshakeManager {
    private static final IgniteLogger LOG = Loggers.forClass(RecoveryInitiatorHandshakeManager.class);

    /** Message factory. */
    private static final NetworkMessagesFactory MESSAGE_FACTORY = new NetworkMessagesFactory();

    private final InternalClusterNode localNode;

    /** Recovery descriptor provider. */
    private final RecoveryDescriptorProvider recoveryDescriptorProvider;

    private final HandshakeEventLoopSwitcher handshakeEventLoopSwitcher;

    /** Used to detect that a peer uses a stale ID. */
    private final StaleIdDetector staleIdDetector;

    private final ClusterIdSupplier clusterIdSupplier;

    private final BooleanSupplier stopping;

    private final IgniteProductVersionSource productVersionSource;

    /** Connection id. */
    private final short connectionId;

    /** Handshake completion future. */
    private final CompletableFuture<NettySender> localHandshakeCompleteFuture = new CompletableFuture<>();

    /**
     * Master future used to complete the handshake either with the results of this handshake of the competing one
     * (in the opposite direction), if it wins.
     */
    private final CompletableFuture<CompletionStage<NettySender>> masterHandshakeCompleteFuture = new CompletableFuture<>();

    /** Remote node. */
    private InternalClusterNode remoteNode;

    /** Netty pipeline channel handler context. */
    private ChannelHandlerContext ctx;

    /** Channel. */
    private Channel channel;

    /** Netty pipeline handshake handler. */
    private HandshakeHandler handler;

    /** Recovery descriptor. */
    private RecoveryDescriptor recoveryDescriptor;

    /** Cluster topology service. */
    protected final TopologyService topologyService;

    /** Failure processor. */
    protected final FailureProcessor failureProcessor;

    /**
     * Constructor.
     *
     * @param localNode {@link InternalClusterNode} representing this node.
     * @param recoveryDescriptorProvider Recovery descriptor provider.
     * @param stopping Defines whether the corresponding connection manager is stopping.
     * @param productVersionSource Source of product version.
     */
    public RecoveryInitiatorHandshakeManager(
            InternalClusterNode localNode,
            short connectionId,
            RecoveryDescriptorProvider recoveryDescriptorProvider,
            HandshakeEventLoopSwitcher handshakeEventLoopSwitcher,
            StaleIdDetector staleIdDetector,
            ClusterIdSupplier clusterIdSupplier,
            ChannelCreationListener channelCreationListener,
            BooleanSupplier stopping,
            IgniteProductVersionSource productVersionSource,
            TopologyService topologyService,
            FailureProcessor failureProcessor
    ) {
        this.localNode = localNode;
        this.connectionId = connectionId;
        this.recoveryDescriptorProvider = recoveryDescriptorProvider;
        this.handshakeEventLoopSwitcher = handshakeEventLoopSwitcher;
        this.staleIdDetector = staleIdDetector;
        this.clusterIdSupplier = clusterIdSupplier;
        this.stopping = stopping;
        this.productVersionSource = productVersionSource;
        this.topologyService = topologyService;
        this.failureProcessor = failureProcessor;

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

    @Override
    public void onConnectionOpen() {
        // Sending a probe to make sure we detect a channel that ends up in a strange state upon creation:
        // the initiator sees it as a normally open channel, but the acceptor (at least, Netty) did not even notice that it accepted it.
        // This happens if the initiator tries to connect a acceptor that is stopping its network (and closing its server socket) just
        // the same exact moment, but then starts its network (binding to the port again) still staying in the same OS process.
        sendProbeToAcceptor();
    }

    private void sendProbeToAcceptor() {
        ProbeMessage probe = MESSAGE_FACTORY.probeMessage().build();

        toCompletableFuture(channel.writeAndFlush(new OutNetworkObject(probe, List.of()))).whenComplete((res, ex) -> {
            if (ex != null) {
                if (ex instanceof IOException) {
                    // We don't care: the channel will be reopened.
                    LOG.debug("Could not send a probe message via {}", ex, channel);
                } else {
                    LOG.info("Could not send a probe message via {}", ex, channel);
                }
            }
        });
    }

    /** {@inheritDoc} */
    @Override
    public void onMessage(NetworkMessage message) {
        if (message instanceof HandshakeRejectedMessage) {
            onHandshakeRejectedMessage((HandshakeRejectedMessage) message);

            return;
        }

        if (message instanceof HandshakeStartMessage) {
            onHandshakeStartMessage((HandshakeStartMessage) message);

            return;
        }

        assert recoveryDescriptor != null : "Wrong initiator handshake flow, message is " + message;
        assert recoveryDescriptor.holderChannel() == channel : "Expected " + channel + " but was " + recoveryDescriptor.holderChannel()
                + ", message is " + message;

        if (message instanceof HandshakeFinishMessage) {
            HandshakeFinishMessage msg = (HandshakeFinishMessage) message;
            long receivedCount = msg.receivedCount();

            recoveryDescriptor.acknowledge(receivedCount);

            if (recoveryDescriptor.unacknowledgedCount() == 0) {
                finishHandshake();

                return;
            }

            List<OutNetworkObject> networkMessages = recoveryDescriptor.unacknowledgedMessages();
            if (LOG.isDebugEnabled()) {
                LOG.debug("Resending on handshake: {}", networkMessages.stream().map(OutNetworkObject::networkMessage).collect(toList()));
            }

            for (OutNetworkObject networkMessage : networkMessages) {
                channel.write(networkMessage);
            }

            channel.flush();

            return;
        }

        // If we are here it means that we acquired the descriptor, we already handled a HandshakeFinishMessage and now we are
        // getting unacked messages from another side and acks for our unacked messages that we sent there (if any).

        assert recoveryDescriptor.holderChannel() == channel : "Expected " + channel + " but was " + recoveryDescriptor.holderChannel()
                + ", message is " + message;

        if (recoveryDescriptor.unacknowledgedCount() == 0) {
            finishHandshake();
        }

        ctx.fireChannelRead(message);
    }

    private void onHandshakeStartMessage(HandshakeStartMessage handshakeStartMessage) {
        if (possiblyRejectHandshakeStart(handshakeStartMessage)) {
            return;
        }

        this.remoteNode = handshakeStartMessage.serverNode().asClusterNode();

        ChannelKey channelKey = new ChannelKey(remoteNode.name(), remoteNode.id(), connectionId);
        handshakeEventLoopSwitcher.switchEventLoopIfNeeded(channel, channelKey).thenRun(() -> proceedAfterSavingIds(handshakeStartMessage));
    }

    private void proceedAfterSavingIds(HandshakeStartMessage handshakeStartMessage) {
        RecoveryDescriptor descriptor = recoveryDescriptorProvider.getRecoveryDescriptor(remoteNode.name(), remoteNode.id(), connectionId);

        while (!descriptor.tryAcquire(ctx, localHandshakeCompleteFuture)) {
            // Don't use the tie-breaking logic as this handshake attempt is late: the competitor has already acquired
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

        // Now that we hold the descriptor, let's check again if the other side has left the topology or we are already stopping.
        // This allows to avoid a race between MessagingService/ConnectionManager handling node leave/local node stop and
        // a concurrent handshake. If one of these happened, we are releasing the descriptor to allow the common machinery
        // to acquire it and clean it up.
        if (possiblyRejectHandshakeStart(handshakeStartMessage)) {
            descriptor.release(ctx);
            return;
        }

        this.recoveryDescriptor = descriptor;

        handshake(this.recoveryDescriptor);
    }

    private boolean possiblyRejectHandshakeStart(HandshakeStartMessage message) {
        if (message.serverNode().id().equals(localNode.id())) {
            handleLoopConnection(message);

            return true;
        }

        if (staleIdDetector.isIdStale(message.serverNode().id())) {
            handleStaleAcceptorId(message);

            return true;
        }

        if (clusterIdMismatch(message.serverClusterId(), clusterIdSupplier.clusterId())) {
            handleClusterIdMismatch(message);

            return true;
        }

        if (!productVersionSource.productName().equals(message.productName())) {
            handleProductNameMismatch(message);

            return true;
        }

        if (!productVersionSource.productVersion().toString().equals(message.productVersion())) {
            handleProductVersionMismatch(message);

            return true;
        }

        if (stopping.getAsBoolean()) {
            handleRefusalToEstablishConnectionDueToStopping(message);

            return true;
        }

        return false;
    }

    private void handleLoopConnection(HandshakeStartMessage msg) {
        String message = String.format(
                "Got handshake start from self [localNode=%s, acceptorNode=%s]",
                localNode,
                msg.serverNode()
        );

        // TODO IGNITE-25802 Introduce a specific exception for this case.
        sendRejectionMessageAndFailHandshake(message, HandshakeRejectionReason.LOOP, HandshakeException::new);
    }

    private void completeMasterFutureWithCompetitorHandshakeFuture(DescriptorAcquiry competitorAcquiry) {
        masterHandshakeCompleteFuture.complete(competitorAcquiry.handshakeCompleteFuture());
        localHandshakeCompleteFuture.completeExceptionally(
                new HandshakeException("Stepping aside to allow an incoming handshake from " + remoteNode.name() + " to finish.")
        );
    }

    private static boolean clusterIdMismatch(@Nullable UUID acceptorClusterId, @Nullable UUID initiatorClusterId) {
        return acceptorClusterId != null && initiatorClusterId != null && !acceptorClusterId.equals(initiatorClusterId);
    }

    private void handleStaleAcceptorId(HandshakeStartMessage msg) {
        String message = String.format("%s:%s is stale, node should be restarted so that other nodes can connect",
                msg.serverNode().name(), msg.serverNode().id()
        );

        sendRejectionMessageAndFailHandshake(
                message,
                HandshakeRejectionReason.STALE_LAUNCH_ID,
                unused -> new RecipientLeftException("Recipient is stale: " + msg.serverNode().id())
        );
    }

    private void handleClusterIdMismatch(HandshakeStartMessage msg) {
        String message = String.format(
                "%s:%s belongs to cluster %s which is different from this one %s, connection rejected. "
                        + "Either another cluster is reachable for this one on the network (in this case make sure they can't connect), "
                        + "or CMG/MG repair was made and then some node that did not participate one is started "
                        + "(in this case, migrate the started node to the repaired cluster using CMG/MG repair tools)",
                msg.serverNode().name(), msg.serverNode().id(), msg.serverClusterId(), clusterIdSupplier.clusterId()
        );

        sendRejectionMessageAndFailHandshake(message, HandshakeRejectionReason.CLUSTER_ID_MISMATCH, HandshakeException::new);
    }

    private void handleProductNameMismatch(HandshakeStartMessage msg) {
        String message = String.format("%s:%s runs product '%s' which is different from this one '%s', connection rejected",
                msg.serverNode().name(), msg.serverNode().id(), msg.productName(), productVersionSource.productName()
        );

        sendRejectionMessageAndFailHandshake(message, HandshakeRejectionReason.PRODUCT_MISMATCH, HandshakeException::new);
    }

    private void handleProductVersionMismatch(HandshakeStartMessage msg) {
        String message = String.format("%s:%s runs product version '%s' which is different from this one '%s', connection rejected",
                msg.serverNode().name(), msg.serverNode().id(), msg.productVersion(), productVersionSource.productVersion()
        );

        sendRejectionMessageAndFailHandshake(message, HandshakeRejectionReason.VERSION_MISMATCH, HandshakeException::new);
    }

    private void handleRefusalToEstablishConnectionDueToStopping(HandshakeStartMessage msg) {
        String message = String.format("%s:%s tried to establish a connection with %s, but it's stopping",
                msg.serverNode().name(), msg.serverNode().id(), localNode.name()
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
                localHandshakeCompleteFuture,
                exceptionFactory
        );
    }

    private void onHandshakeRejectedMessage(HandshakeRejectedMessage msg) {
        msg.reason().print(stopping.getAsBoolean(), LOG, "Handshake rejected by acceptor: {}", msg.message());

        if (msg.reason() == HandshakeRejectionReason.CLINCH) {
            giveUpClinch();
        } else {
            localHandshakeCompleteFuture.completeExceptionally(HandshakeManagerUtils.createExceptionFromRejectionMessage(msg));
        }
    }

    private void giveUpClinch() {
        RecoveryDescriptor descriptor = recoveryDescriptorProvider.getRecoveryDescriptor(remoteNode.name(), remoteNode.id(), connectionId);

        DescriptorAcquiry myAcquiry = descriptor.holder();
        assert myAcquiry != null;
        assert myAcquiry.channel() != null;
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
            localHandshakeCompleteFuture.completeExceptionally(new ChannelAlreadyExistsException(remoteNode.id()));
        }
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
                .clientNode(clusterNodeToMessage(localNode))
                .receivedCount(descriptor.receivedCount())
                .connectionId(connectionId)
                .build();

        ChannelFuture sendFuture = ctx.channel().writeAndFlush(new OutNetworkObject(response, emptyList()));

        toCompletableFuture(sendFuture).whenComplete((unused, throwable) -> {
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
        return handler.createMessageHandler(remoteNode, connectionId);
    }

    /**
     * Finishes handshaking process by removing handshake handler from the pipeline and creating a {@link NettySender}.
     */
    protected void finishHandshake() {
        // Removes handshake handler from the pipeline as the handshake is finished
        this.ctx.pipeline().remove(this.handler);

        // Complete the master future with the local future of the current handshake as there was no competitor (or we won the competition).
        masterHandshakeCompleteFuture.complete(localHandshakeCompleteFuture);
        localHandshakeCompleteFuture.complete(
                new NettySender(channel, remoteNode.id(), remoteNode.name(), connectionId, recoveryDescriptor)
        );
    }

    @TestOnly
    void setRemoteNode(InternalClusterNode remoteNode) {
        this.remoteNode = remoteNode;
    }
}
