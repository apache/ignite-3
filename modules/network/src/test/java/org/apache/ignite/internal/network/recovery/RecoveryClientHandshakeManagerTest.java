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
import static org.hamcrest.Matchers.is;
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
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.internal.network.NetworkMessagesFactory;
import org.apache.ignite.internal.network.handshake.ChannelAlreadyExistsException;
import org.apache.ignite.internal.network.netty.ChannelCreationListener;
import org.apache.ignite.internal.network.netty.NettySender;
import org.apache.ignite.internal.network.recovery.message.HandshakeStartMessage;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class RecoveryClientHandshakeManagerTest extends BaseIgniteAbstractTest {
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
    @Timeout(10)
    void terminatesCurrentHandshakeWhenCannotAcquireLockAtClientSide(boolean clientLaunchIdIsLower) {
        UUID clientLaunchId = clientLaunchIdIsLower ? LOWER_ID : HIGHER_ID;
        UUID serverLaunchId = clientLaunchIdIsLower ? HIGHER_ID : LOWER_ID;

        RecoveryClientHandshakeManager manager = clientHandshakeManager(clientLaunchId);
        CompletableFuture<NettySender> handshakeFuture = manager.handshakeFuture();

        recoveryDescriptor.acquire(context);

        manager.onMessage(handshakeStartMessageFrom(serverLaunchId));

        verify(channel, never()).close();
        verify(channel, never()).close(any(ChannelPromise.class));

        ChannelAlreadyExistsException ex = assertWillThrowFast(handshakeFuture, ChannelAlreadyExistsException.class);
        assertThat(ex.consistentId(), is(SERVER_CONSISTENT_ID));
    }

    private RecoveryClientHandshakeManager clientHandshakeManager(UUID launchId) {
        RecoveryClientHandshakeManager manager = new RecoveryClientHandshakeManager(
                launchId,
                CLIENT_CONSISTENT_ID,
                CONNECTION_INDEX,
                recoveryDescriptorProvider,
                new AllIdsAreFresh(),
                channelCreationListener,
                new AtomicBoolean()
        );

        manager.onInit(context);

        return manager;
    }

    private static HandshakeStartMessage handshakeStartMessageFrom(UUID serverLaunchId) {
        return MESSAGE_FACTORY.handshakeStartMessage()
                .launchId(serverLaunchId)
                .consistentId(SERVER_CONSISTENT_ID)
                .build();
    }
}
