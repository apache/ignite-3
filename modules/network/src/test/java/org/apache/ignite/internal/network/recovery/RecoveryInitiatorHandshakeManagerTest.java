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

import static java.util.UUID.randomUUID;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.apache.ignite.internal.testframework.asserts.CompletableFutureAssert.assertWillThrowFast;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureExceptionMatcher.willThrow;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyShort;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.channel.DefaultChannelProgressivePromise;
import io.netty.channel.EventLoop;
import io.netty.util.concurrent.EventExecutor;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;
import org.apache.ignite.internal.failure.FailureManager;
import org.apache.ignite.internal.failure.handlers.NoOpFailureHandler;
import org.apache.ignite.internal.lang.NodeStoppingException;
import org.apache.ignite.internal.network.ClusterNodeImpl;
import org.apache.ignite.internal.network.ConstantClusterIdSupplier;
import org.apache.ignite.internal.network.NetworkMessagesFactory;
import org.apache.ignite.internal.network.OutNetworkObject;
import org.apache.ignite.internal.network.TopologyService;
import org.apache.ignite.internal.network.handshake.ChannelAlreadyExistsException;
import org.apache.ignite.internal.network.handshake.HandshakeException;
import org.apache.ignite.internal.network.handshake.NoOpHandshakeEventLoopSwitcher;
import org.apache.ignite.internal.network.netty.ChannelCreationListener;
import org.apache.ignite.internal.network.netty.NettySender;
import org.apache.ignite.internal.network.recovery.message.HandshakeRejectedMessage;
import org.apache.ignite.internal.network.recovery.message.HandshakeRejectionReason;
import org.apache.ignite.internal.network.recovery.message.HandshakeStartMessage;
import org.apache.ignite.internal.properties.IgniteProductVersion;
import org.apache.ignite.internal.version.DefaultIgniteProductVersionSource;
import org.apache.ignite.network.NetworkAddress;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;

@Timeout(10)
class RecoveryInitiatorHandshakeManagerTest extends HandshakeManagerTest {
    private static final UUID LOWER_ID = new UUID(1, 1);
    private static final UUID HIGHER_ID = new UUID(2, 2);

    private static final String INITIATOR_CONSISTENT_ID = "initiator";
    private static final String ACCEPTOR_CONSISTENT_ID = "acceptor";

    private static final short CONNECTION_INDEX = 0;

    private static final String ACCEPTOR_HOST = "acceptor-host";
    private static final String INITIATOR_HOST = "initiator-host";

    private static final int PORT = 1000;

    private static final NetworkMessagesFactory MESSAGE_FACTORY = new NetworkMessagesFactory();

    private static final UUID CORRECT_CLUSTER_ID = new UUID(11, 12);
    private static final UUID WRONG_CLUSTER_ID = new UUID(13, 14);

    @Mock
    private Channel thisChannel;
    @Mock
    private Channel competitorChannel;

    @Mock
    private ChannelHandlerContext thisContext;
    @Mock
    private ChannelHandlerContext competitorContext;

    @Mock
    private ChannelCreationListener channelCreationListener;

    @Mock
    private RecoveryDescriptorProvider recoveryDescriptorProvider;

    @Mock
    private EventExecutor eventExecutor;

    @Mock
    private EventLoop eventLoop;

    @Mock
    private NettySender competitorNettySender;

    @Captor
    private ArgumentCaptor<OutNetworkObject> sentMessageCaptor;

    private final RecoveryDescriptor recoveryDescriptor = new RecoveryDescriptor(100);

    private final AtomicBoolean initiatorHandshakeManagerStopping = new AtomicBoolean(false);

    @Mock
    protected TopologyService topologyService;

    @BeforeEach
    void initMocks() {
        lenient().when(thisContext.channel()).thenReturn(thisChannel);
        lenient().when(competitorContext.channel()).thenReturn(competitorChannel);

        lenient().when(thisContext.executor()).thenReturn(eventExecutor);
        lenient().when(eventExecutor.inEventLoop()).thenReturn(true);

        lenient().when(thisChannel.eventLoop()).thenReturn(eventLoop);

        lenient().when(recoveryDescriptorProvider.getRecoveryDescriptor(any(), any(), anyShort()))
                .thenReturn(recoveryDescriptor);

        lenient().when(thisChannel.writeAndFlush(any())).then(invocation -> {
            DefaultChannelProgressivePromise future = new DefaultChannelProgressivePromise(thisChannel, eventExecutor);
            future.setSuccess();
            return future;
        });
    }

    /**
     * This tests the following scenario: two handshakes in the opposite directions are started,
     * Handshake 1 is faster and it takes both initiator-side and acceptor-side locks (using recovery descriptors
     * as locks), and only then Handshake 2 tries to take the first lock (the one on the initiator side).
     * In such a situation, tie-breaking logic should not be applied (as Handshake 1 could have already
     * established, or almost established, a logical connection); instead, Handshake 2 must stop
     * itself (regardless of what that the Tie Breaker would prescribe).
     */
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void switchesToCompetitorHandshakeWhenCannotAcquireLockAtInitiatorSide(boolean initiatorLaunchIdIsLower) {
        UUID initiatorLaunchId = initiatorLaunchIdIsLower ? LOWER_ID : HIGHER_ID;
        UUID acceptorLaunchId = initiatorLaunchIdIsLower ? HIGHER_ID : LOWER_ID;

        RecoveryInitiatorHandshakeManager manager = initiatorHandshakeManager(initiatorLaunchId);
        CompletableFuture<NettySender> localHandshakeFuture = manager.localHandshakeFuture();
        CompletionStage<NettySender> finalHandshakeFuture = manager.finalHandshakeFuture();

        recoveryDescriptor.tryAcquire(thisContext, completedFuture(competitorNettySender));

        manager.onMessage(handshakeStartMessageFrom(acceptorLaunchId));

        verify(thisChannel, never()).close();
        verify(thisChannel, never()).close(any(ChannelPromise.class));

        HandshakeException ex = assertWillThrowFast(localHandshakeFuture, HandshakeException.class);
        assertThat(ex.getMessage(), is("Stepping aside to allow an incoming handshake from acceptor to finish."));

        assertThat(finalHandshakeFuture.toCompletableFuture(), willCompleteSuccessfully());
        assertThat(finalHandshakeFuture.toCompletableFuture().join(), is(competitorNettySender));
    }

    private RecoveryInitiatorHandshakeManager initiatorHandshakeManager(UUID launchId) {
        return initiatorHandshakeManager(launchId, initiatorHandshakeManagerStopping::get);
    }

    private RecoveryInitiatorHandshakeManager initiatorHandshakeManager(UUID launchId, BooleanSupplier stopping) {
        RecoveryInitiatorHandshakeManager manager = new RecoveryInitiatorHandshakeManager(
                new ClusterNodeImpl(launchId, INITIATOR_CONSISTENT_ID, new NetworkAddress(INITIATOR_HOST, PORT)),
                CONNECTION_INDEX,
                recoveryDescriptorProvider,
                new NoOpHandshakeEventLoopSwitcher(),
                new AllIdsAreFresh(),
                new ConstantClusterIdSupplier(CORRECT_CLUSTER_ID),
                channelCreationListener,
                stopping,
                new DefaultIgniteProductVersionSource(),
                topologyService,
                new FailureManager(new NoOpFailureHandler())
        );

        manager.onInit(thisContext);

        return manager;
    }

    private static HandshakeStartMessage handshakeStartMessageFrom(UUID acceptorLaunchId) {
        return handshakeStartMessageFrom(acceptorLaunchId, CORRECT_CLUSTER_ID);
    }

    private static HandshakeStartMessage handshakeStartMessageFrom(UUID acceptorLaunchId, UUID acceptorClusterId) {
        return MESSAGE_FACTORY.handshakeStartMessage()
                .serverNode(
                        MESSAGE_FACTORY.clusterNodeMessage()
                                .id(acceptorLaunchId)
                                .name(ACCEPTOR_CONSISTENT_ID)
                                .host(ACCEPTOR_HOST)
                                .port(PORT)
                                .build()
                )
                .serverClusterId(acceptorClusterId)
                .productName(IgniteProductVersion.CURRENT_PRODUCT)
                .productVersion(IgniteProductVersion.CURRENT_VERSION.toString())
                .build();
    }

    @Test
    void switchesToCompetitorFutureWhenRejectedDueToClinchAndCompetitorIsHere() {
        RecoveryInitiatorHandshakeManager manager = initiatorHandshakeManager(randomUUID());
        CompletableFuture<NettySender> localHandshakeFuture = manager.localHandshakeFuture();
        CompletionStage<NettySender> finalHandshakeFuture = manager.finalHandshakeFuture();

        manager.setRemoteNode(new ClusterNodeImpl(randomUUID(), ACCEPTOR_CONSISTENT_ID, new NetworkAddress(ACCEPTOR_HOST, PORT)));
        recoveryDescriptor.tryAcquire(thisContext, new CompletableFuture<>());

        DescriptorAcquiry thisAcquiry = recoveryDescriptor.holder();
        assertThat(thisAcquiry, notNullValue());
        thisAcquiry.clinchResolved().whenComplete(((unused, ex) -> {
            assertThat(recoveryDescriptor.tryAcquire(competitorContext, completedFuture(competitorNettySender)), is(true));
        }));

        manager.onMessage(handshakeRejectedMessageDueToClinchFrom());

        HandshakeException ex = assertWillThrowFast(localHandshakeFuture, HandshakeException.class);
        assertThat(ex.getMessage(), startsWith("Stepping aside to allow an incoming handshake from "));

        assertThat(finalHandshakeFuture.toCompletableFuture(), willCompleteSuccessfully());
        assertThat(finalHandshakeFuture.toCompletableFuture().join(), is(competitorNettySender));
    }

    @Test
    void finishesWithChannelAlreadyExistsExceptionWhenRejectedDueToClinchAndCompetitorIsNotHere() {
        RecoveryInitiatorHandshakeManager manager = initiatorHandshakeManager(randomUUID());
        CompletableFuture<NettySender> localHandshakeFuture = manager.localHandshakeFuture();
        CompletionStage<NettySender> finalHandshakeFuture = manager.finalHandshakeFuture();

        manager.setRemoteNode(new ClusterNodeImpl(randomUUID(), ACCEPTOR_CONSISTENT_ID, new NetworkAddress(ACCEPTOR_HOST, PORT)));
        recoveryDescriptor.tryAcquire(thisContext, new CompletableFuture<>());

        manager.onMessage(handshakeRejectedMessageDueToClinchFrom());

        assertWillThrowFast(localHandshakeFuture, ChannelAlreadyExistsException.class);
        assertWillThrowFast(finalHandshakeFuture.toCompletableFuture(), ChannelAlreadyExistsException.class);
    }

    private static HandshakeRejectedMessage handshakeRejectedMessageDueToClinchFrom() {
        return MESSAGE_FACTORY.handshakeRejectedMessage()
                .reasonString(HandshakeRejectionReason.CLINCH.name())
                .message("Rejected")
                .build();
    }

    @Test
    void gettingHandshakeStartMessageWhenStoppingCausesHandshakeToBeFinishedWithNodeStoppingException() {
        RecoveryInitiatorHandshakeManager manager = initiatorHandshakeManager(LOWER_ID);
        initiatorHandshakeManagerStopping.set(true);

        CompletableFuture<NettySender> localHandshakeFuture = manager.localHandshakeFuture();
        CompletionStage<NettySender> finalHandshakeFuture = manager.finalHandshakeFuture();

        manager.onMessage(handshakeStartMessageFrom(HIGHER_ID));

        assertHandshakeRejectedMessageIsSentWithReason(HandshakeRejectionReason.STOPPING);

        assertWillThrowFast(localHandshakeFuture, NodeStoppingException.class);
        assertWillThrowFast(finalHandshakeFuture.toCompletableFuture(), NodeStoppingException.class);
    }

    private void assertHandshakeRejectedMessageIsSentWithReason(HandshakeRejectionReason reason) {
        verify(thisChannel).writeAndFlush(sentMessageCaptor.capture());

        OutNetworkObject outObject = sentMessageCaptor.getValue();
        assertThat(outObject.networkMessage(), instanceOf(HandshakeRejectedMessage.class));

        HandshakeRejectedMessage message = (HandshakeRejectedMessage) outObject.networkMessage();
        assertThat(message.reason(), is(reason));
    }

    @Test
    void failsHandshakeIfNodeLeavesOrOurNodeInitiatesStopConcurrentlyWithAcquiringDescriptor() {
        BooleanSupplier stoppingWhenDescriptorAcquired = () -> recoveryDescriptor.holder() != null;
        assertFalse(stoppingWhenDescriptorAcquired.getAsBoolean());

        RecoveryInitiatorHandshakeManager manager = initiatorHandshakeManager(LOWER_ID, stoppingWhenDescriptorAcquired);
        CompletableFuture<NettySender> localHandshakeFuture = manager.localHandshakeFuture();
        CompletionStage<NettySender> finalHandshakeFuture = manager.finalHandshakeFuture();

        manager.onMessage(handshakeStartMessageFrom(HIGHER_ID));

        assertHandshakeRejectedMessageIsSentWithReason(HandshakeRejectionReason.STOPPING);

        assertThat(localHandshakeFuture, willThrow(NodeStoppingException.class));
        assertThat(finalHandshakeFuture.toCompletableFuture(), willThrow(NodeStoppingException.class));

        assertThat(recoveryDescriptor.holder(), is(nullValue()));
    }

    @Test
    void failsHandshakeIfAcceptorClusterIdDiffersFromOurs() {
        RecoveryInitiatorHandshakeManager manager = initiatorHandshakeManager(LOWER_ID);
        CompletableFuture<NettySender> localHandshakeFuture = manager.localHandshakeFuture();
        CompletionStage<NettySender> finalHandshakeFuture = manager.finalHandshakeFuture();

        manager.onMessage(handshakeStartMessageFrom(HIGHER_ID, WRONG_CLUSTER_ID));

        assertHandshakeRejectedMessageIsSentWithReason(HandshakeRejectionReason.CLUSTER_ID_MISMATCH);

        assertThat(localHandshakeFuture, willThrow(HandshakeException.class));
        assertThat(finalHandshakeFuture.toCompletableFuture(), willThrow(HandshakeException.class));

        assertThat(recoveryDescriptor.holder(), is(nullValue()));
    }

    @Test
    void gettingHandshakeRejectedMessageWithReasonStoppingCausesHandshakeToBeFinishedWithRecipientLeftException() {
        RecoveryInitiatorHandshakeManager manager = initiatorHandshakeManager(LOWER_ID);

        assertThatRejectionWithStoppingCausesRecipientLeftException(manager);
    }
}
