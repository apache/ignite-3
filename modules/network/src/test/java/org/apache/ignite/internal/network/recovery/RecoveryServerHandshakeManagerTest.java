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

import static org.apache.ignite.internal.testframework.asserts.CompletableFutureAssert.assertWillThrowFast;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.startsWith;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyShort;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.channel.DefaultChannelProgressivePromise;
import io.netty.util.concurrent.EventExecutor;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.internal.network.NetworkMessagesFactory;
import org.apache.ignite.internal.network.handshake.HandshakeException;
import org.apache.ignite.internal.network.netty.ChannelCreationListener;
import org.apache.ignite.internal.network.netty.NettySender;
import org.apache.ignite.internal.network.recovery.message.HandshakeRejectedMessage;
import org.apache.ignite.internal.network.recovery.message.HandshakeRejectionReason;
import org.apache.ignite.internal.network.recovery.message.HandshakeStartResponseMessage;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.network.OutNetworkObject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class RecoveryServerHandshakeManagerTest extends BaseIgniteAbstractTest {
    private static final UUID LOWER_ID = new UUID(1, 1);
    private static final UUID HIGHER_ID = new UUID(2, 2);

    private static final String CLIENT_CONSISTENT_ID = "client";
    private static final String SERVER_CONSISTENT_ID = "server";

    private static final short CONNECTION_INDEX = 0;

    private static final NetworkMessagesFactory MESSAGE_FACTORY = new NetworkMessagesFactory();

    @Mock
    private Channel channel;

    @Mock
    private ChannelHandlerContext context;

    @Mock
    private ChannelCreationListener channelCreationListener;

    @Mock
    private RecoveryDescriptorProvider recoveryDescriptorProvider;

    @Mock
    private EventExecutor eventExecutor;

    @Captor
    private ArgumentCaptor<OutNetworkObject> sentMessageCaptor;

    private final RecoveryDescriptor recoveryDescriptor = new RecoveryDescriptor(100);

    @BeforeEach
    void initMocks() {
        lenient().when(context.channel()).thenReturn(channel);
        lenient().when(channel.close()).thenAnswer(invocation -> {
            recoveryDescriptor.release(context);
            return mock(ChannelFuture.class);
        });
        lenient().when(recoveryDescriptorProvider.getRecoveryDescriptor(anyString(), any(), anyShort()))
                .thenReturn(recoveryDescriptor);

        lenient().when(context.executor()).thenReturn(eventExecutor);
        lenient().when(eventExecutor.inEventLoop()).thenReturn(true);

        lenient().when(channel.writeAndFlush(any())).then(invocation -> {
            DefaultChannelProgressivePromise future = new DefaultChannelProgressivePromise(channel, eventExecutor);
            future.setSuccess();
            return future;
        });
    }

    @Test
    @Timeout(10)
    void terminatesCurrentHandshakeInClinchWhenOngoingHandshakeLosesDueToTieBreaking() {
        UUID clientLaunchId = LOWER_ID;
        UUID serverLaunchId = HIGHER_ID;

        RecoveryServerHandshakeManager manager = serverHandshakeManager(serverLaunchId);
        CompletableFuture<NettySender> handshakeFuture = manager.localHandshakeFuture();

        recoveryDescriptor.acquire(context, new CompletableFuture<>());

        manager.onMessage(handshakeStartResponseMessageFrom(clientLaunchId));

        verify(channel, never()).close();
        verify(channel, never()).close(any(ChannelPromise.class));

        HandshakeException ex = assertWillThrowFast(handshakeFuture, HandshakeException.class);
        assertThat(ex.getMessage(), startsWith("Failed to acquire recovery descriptor during handshake, it is held by: "));

        verify(channel).writeAndFlush(sentMessageCaptor.capture());

        OutNetworkObject outObject = sentMessageCaptor.getValue();
        assertThat(outObject.shouldBeSavedForRecovery(), is(false));
        assertThat(outObject.networkMessage(), is(instanceOf(HandshakeRejectedMessage.class)));

        HandshakeRejectedMessage rejectedMessage = (HandshakeRejectedMessage) outObject.networkMessage();
        assertThat(rejectedMessage.reason(), is(HandshakeRejectionReason.CLINCH));
        assertThat(
                rejectedMessage.message(),
                startsWith("Handshake clinch detected, this handshake will be terminated, winning channel is ")
        );
    }

    private RecoveryServerHandshakeManager serverHandshakeManager(UUID launchId) {
        RecoveryServerHandshakeManager manager = new RecoveryServerHandshakeManager(
                launchId,
                SERVER_CONSISTENT_ID,
                MESSAGE_FACTORY,
                recoveryDescriptorProvider,
                new AllIdsAreFresh(),
                channelCreationListener,
                new AtomicBoolean(false)
        );

        manager.onInit(context);

        return manager;
    }

    private static HandshakeStartResponseMessage handshakeStartResponseMessageFrom(UUID clientLaunchId) {
        return MESSAGE_FACTORY.handshakeStartResponseMessage()
                .launchId(clientLaunchId)
                .consistentId(CLIENT_CONSISTENT_ID)
                .connectionId(CONNECTION_INDEX)
                .receivedCount(0)
                .build();
    }
}
