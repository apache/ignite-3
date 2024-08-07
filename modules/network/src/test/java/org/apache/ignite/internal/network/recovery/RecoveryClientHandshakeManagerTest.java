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
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;
import org.apache.ignite.internal.failure.FailureProcessor;
import org.apache.ignite.internal.lang.NodeStoppingException;
import org.apache.ignite.internal.network.ClusterNodeImpl;
import org.apache.ignite.internal.network.ConstantClusterIdSupplier;
import org.apache.ignite.internal.network.NetworkMessagesFactory;
import org.apache.ignite.internal.network.OutNetworkObject;
import org.apache.ignite.internal.network.handshake.ChannelAlreadyExistsException;
import org.apache.ignite.internal.network.handshake.HandshakeException;
import org.apache.ignite.internal.network.netty.ChannelCreationListener;
import org.apache.ignite.internal.network.netty.NettySender;
import org.apache.ignite.internal.network.recovery.message.HandshakeRejectedMessage;
import org.apache.ignite.internal.network.recovery.message.HandshakeRejectionReason;
import org.apache.ignite.internal.network.recovery.message.HandshakeStartMessage;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.network.NetworkAddress;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
@Timeout(10)
class RecoveryClientHandshakeManagerTest extends BaseIgniteAbstractTest {
    private static final UUID LOWER_ID = new UUID(1, 1);
    private static final UUID HIGHER_ID = new UUID(2, 2);

    private static final String CLIENT_CONSISTENT_ID = "client";
    private static final String SERVER_CONSISTENT_ID = "server";

    private static final short CONNECTION_INDEX = 0;

    private static final String SERVER_HOST = "server-host";
    private static final String CLIENT_HOST = "client-host";

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

    private final AtomicBoolean clientHandshakeManagerStopping = new AtomicBoolean(false);

    @Mock
    private FailureProcessor failureProcessor;

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
     * Handshake 1 is faster and it takes both client-side and server-side locks (using recovery descriptors
     * as locks), and only then Handshake 2 tries to take the first lock (the one on the client side).
     * In such a situation, tie-breaking logic should not be applied (as Handshake 1 could have already
     * established, or almost established, a logical connection); instead, Handshake 2 must stop
     * itself (regardless of what that the Tie Breaker would prescribe).
     */
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void switchesToCompetitorHandshakeWhenCannotAcquireLockAtClientSide(boolean clientLaunchIdIsLower) {
        UUID clientLaunchId = clientLaunchIdIsLower ? LOWER_ID : HIGHER_ID;
        UUID serverLaunchId = clientLaunchIdIsLower ? HIGHER_ID : LOWER_ID;

        RecoveryClientHandshakeManager manager = clientHandshakeManager(clientLaunchId);
        CompletableFuture<NettySender> localHandshakeFuture = manager.localHandshakeFuture();
        CompletionStage<NettySender> finalHandshakeFuture = manager.finalHandshakeFuture();

        recoveryDescriptor.tryAcquire(thisContext, completedFuture(competitorNettySender));

        manager.onMessage(handshakeStartMessageFrom(serverLaunchId));

        verify(thisChannel, never()).close();
        verify(thisChannel, never()).close(any(ChannelPromise.class));

        HandshakeException ex = assertWillThrowFast(localHandshakeFuture, HandshakeException.class);
        assertThat(ex.getMessage(), is("Stepping aside to allow an incoming handshake from server to finish."));

        assertThat(finalHandshakeFuture.toCompletableFuture(), willCompleteSuccessfully());
        assertThat(finalHandshakeFuture.toCompletableFuture().join(), is(competitorNettySender));
    }

    private RecoveryClientHandshakeManager clientHandshakeManager(UUID launchId) {
        return clientHandshakeManager(launchId, clientHandshakeManagerStopping::get);
    }

    private RecoveryClientHandshakeManager clientHandshakeManager(UUID launchId, BooleanSupplier stopping) {
        RecoveryClientHandshakeManager manager = new RecoveryClientHandshakeManager(
                new ClusterNodeImpl(launchId.toString(), CLIENT_CONSISTENT_ID, new NetworkAddress(CLIENT_HOST, PORT)),
                CONNECTION_INDEX,
                recoveryDescriptorProvider,
                () -> List.of(thisChannel.eventLoop()),
                new AllIdsAreFresh(),
                new ConstantClusterIdSupplier(CORRECT_CLUSTER_ID),
                channelCreationListener,
                stopping,
                failureProcessor
        );

        manager.onInit(thisContext);

        return manager;
    }

    private static HandshakeStartMessage handshakeStartMessageFrom(UUID serverLaunchId) {
        return handshakeStartMessageFrom(serverLaunchId, CORRECT_CLUSTER_ID);
    }

    private static HandshakeStartMessage handshakeStartMessageFrom(UUID serverLaunchId, UUID serverClusterId) {
        return MESSAGE_FACTORY.handshakeStartMessage()
                .serverNode(
                        MESSAGE_FACTORY.clusterNodeMessage()
                                .id(serverLaunchId.toString())
                                .name(SERVER_CONSISTENT_ID)
                                .host(SERVER_HOST)
                                .port(PORT)
                                .build()
                )
                .serverClusterId(serverClusterId)
                .build();
    }

    @Test
    void switchesToCompetitorFutureWhenRejectedDueToClinchAndCompetitorIsHere() {
        RecoveryClientHandshakeManager manager = clientHandshakeManager(randomUUID());
        CompletableFuture<NettySender> localHandshakeFuture = manager.localHandshakeFuture();
        CompletionStage<NettySender> finalHandshakeFuture = manager.finalHandshakeFuture();

        manager.setRemoteNode(new ClusterNodeImpl(randomUUID().toString(), SERVER_CONSISTENT_ID, new NetworkAddress(SERVER_HOST, PORT)));
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
        RecoveryClientHandshakeManager manager = clientHandshakeManager(randomUUID());
        CompletableFuture<NettySender> localHandshakeFuture = manager.localHandshakeFuture();
        CompletionStage<NettySender> finalHandshakeFuture = manager.finalHandshakeFuture();

        manager.setRemoteNode(new ClusterNodeImpl(randomUUID().toString(), SERVER_CONSISTENT_ID, new NetworkAddress(SERVER_HOST, PORT)));
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
        RecoveryClientHandshakeManager manager = clientHandshakeManager(LOWER_ID);
        clientHandshakeManagerStopping.set(true);

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

        RecoveryClientHandshakeManager manager = clientHandshakeManager(LOWER_ID, stoppingWhenDescriptorAcquired);
        CompletableFuture<NettySender> localHandshakeFuture = manager.localHandshakeFuture();
        CompletionStage<NettySender> finalHandshakeFuture = manager.finalHandshakeFuture();

        manager.onMessage(handshakeStartMessageFrom(HIGHER_ID));

        assertHandshakeRejectedMessageIsSentWithReason(HandshakeRejectionReason.STOPPING);

        assertThat(localHandshakeFuture, willThrow(NodeStoppingException.class));
        assertThat(finalHandshakeFuture.toCompletableFuture(), willThrow(NodeStoppingException.class));

        assertThat(recoveryDescriptor.holder(), is(nullValue()));
    }

    @Test
    void failsHandshakeIfServerClusterIdDiffersFromOurs() {
        RecoveryClientHandshakeManager manager = clientHandshakeManager(LOWER_ID);
        CompletableFuture<NettySender> localHandshakeFuture = manager.localHandshakeFuture();
        CompletionStage<NettySender> finalHandshakeFuture = manager.finalHandshakeFuture();

        manager.onMessage(handshakeStartMessageFrom(HIGHER_ID, WRONG_CLUSTER_ID));

        assertHandshakeRejectedMessageIsSentWithReason(HandshakeRejectionReason.CLUSTER_ID_MISMATCH);

        assertThat(localHandshakeFuture, willThrow(HandshakeException.class));
        assertThat(finalHandshakeFuture.toCompletableFuture(), willThrow(HandshakeException.class));

        assertThat(recoveryDescriptor.holder(), is(nullValue()));
    }
}
