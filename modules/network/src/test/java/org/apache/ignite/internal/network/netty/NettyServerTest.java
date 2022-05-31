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

package org.apache.ignite.internal.network.netty;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyShort;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPromise;
import io.netty.channel.ServerChannel;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.configuration.schemas.network.NetworkConfiguration;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.network.handshake.HandshakeManager;
import org.apache.ignite.internal.network.serialization.SerializationService;
import org.apache.ignite.internal.network.serialization.UserObjectSerializationContext;
import org.apache.ignite.lang.IgniteInternalException;
import org.apache.ignite.network.NettyBootstrapFactory;
import org.apache.ignite.network.NetworkMessage;
import org.apache.ignite.network.serialization.MessageDeserializer;
import org.apache.ignite.network.serialization.MessageMappingException;
import org.apache.ignite.network.serialization.MessageReader;
import org.apache.ignite.network.serialization.MessageSerializationRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InOrder;
import org.mockito.Mockito;
import org.mockito.verification.VerificationMode;

/**
 * Tests for {@link NettyServer}.
 */
@ExtendWith(ConfigurationExtension.class)
public class NettyServerTest {
    /** Bootstrap factory. */
    private NettyBootstrapFactory bootstrapFactory;

    /** Server. */
    private NettyServer server;

    /** Server configuration. */
    @InjectConfiguration
    private NetworkConfiguration serverCfg;

    /**
     * After each.
     */
    @AfterEach
    final void tearDown() throws Exception {
        server.stop().join();
        bootstrapFactory.stop();
    }

    /**
     * Tests a successful server start scenario.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testSuccessfulServerStart() throws Exception {
        server = getServer(true);

        assertTrue(server.isRunning());
    }

    /**
     * Tests a graceful server shutdown scenario.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testServerGracefulShutdown() throws Exception {
        server = getServer(true);

        server.stop().join();
    }

    /**
     * Tests a scenario where a server is stopped before a server socket is successfully bound.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testServerStoppedBeforeStarted() throws Exception {
        var channel = new EmbeddedServerChannel();

        ChannelPromise future = channel.newPromise();

        server = getServer(false);

        CompletableFuture<Void> stop = server.stop();

        future.setSuccess(null);

        stop.get(3, TimeUnit.SECONDS);

        assertFalse(channel.finish());
    }

    /**
     * Tests that a {@link NettyServer#start} method can be called only once.
     */
    @Test
    public void testStartTwice() {
        server = getServer(true);

        assertThrows(IgniteInternalException.class, server::start);
    }

    /**
     * Tests that handshake manager is invoked upon a client connecting to a server.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testHandshakeManagerInvoked() throws Exception {
        HandshakeManager handshakeManager = mock(HandshakeManager.class);

        when(handshakeManager.handshakeFuture()).thenReturn(CompletableFuture.completedFuture(mock(NettySender.class)));

        MessageSerializationRegistry registry = mock(MessageSerializationRegistry.class);

        when(registry.createDeserializer(anyShort(), anyShort()))
                .thenReturn(new MessageDeserializer<>() {
                    /** {@inheritDoc} */
                    @Override
                    public boolean readMessage(MessageReader reader) throws MessageMappingException {
                        return true;
                    }

                    /** {@inheritDoc} */
                    @Override
                    public Class<NetworkMessage> klass() {
                        return NetworkMessage.class;
                    }

                    /** {@inheritDoc} */
                    @Override
                    public NetworkMessage getMessage() {
                        return mock(NetworkMessage.class);
                    }
                });

        bootstrapFactory = new NettyBootstrapFactory(serverCfg, "");
        bootstrapFactory.start();

        server = new NettyServer(
                serverCfg.value(),
                () -> handshakeManager,
                sender -> {
                },
                (message) -> {
                },
                new SerializationService(registry, mock(UserObjectSerializationContext.class)),
                bootstrapFactory
        );

        server.start().get(3, TimeUnit.SECONDS);

        CompletableFuture<Channel> connectFut = NettyUtils.toChannelCompletableFuture(
                new Bootstrap()
                        .channel(NioSocketChannel.class)
                        .group(new NioEventLoopGroup())
                        .handler(new ChannelInitializer<>() {
                            /** {@inheritDoc} */
                            @Override
                            protected void initChannel(Channel ch) throws Exception {
                                // No-op.
                            }
                        })
                        .connect(server.address())
        );

        Channel channel = connectFut.get(3, TimeUnit.SECONDS);

        ByteBuf buffer = ByteBufAllocator.DEFAULT.buffer();

        // One message only.
        for (int i = 0; i < (NetworkMessage.MSG_TYPE_SIZE_BYTES + 1); i++) {
            buffer.writeByte(1);
        }

        channel.writeAndFlush(buffer).get(3, TimeUnit.SECONDS);

        channel.close().get(3, TimeUnit.SECONDS);

        InOrder order = Mockito.inOrder(handshakeManager);

        order.verify(handshakeManager, timeout()).onInit(any());
        order.verify(handshakeManager, timeout()).handshakeFuture();
        order.verify(handshakeManager, timeout()).onConnectionOpen();
        order.verify(handshakeManager, timeout()).onMessage(any());
    }

    /**
     * Returns verification mode for a one call with a 3-second timeout.
     *
     * @return Verification mode for a one call with a 3-second timeout.
     */
    private static VerificationMode timeout() {
        return Mockito.timeout(TimeUnit.SECONDS.toMillis(3));
    }

    /**
     * Creates a server from a backing {@link ChannelFuture}.
     *
     * @param shouldStart {@code true} if a server should start successfully
     * @return NettyServer.
     */
    private NettyServer getServer(boolean shouldStart) {
        bootstrapFactory = new NettyBootstrapFactory(serverCfg, "");
        bootstrapFactory.start();

        var server = new NettyServer(
                serverCfg.value(),
                () -> mock(HandshakeManager.class),
                null,
                null,
                null,
                bootstrapFactory
        );

        try {
            server.start().get(3, TimeUnit.SECONDS);
        } catch (Exception e) {
            if (shouldStart) {
                fail(e);
            }
        }

        return server;
    }

    /** Server channel on top of the {@link EmbeddedChannel}. */
    private static class EmbeddedServerChannel extends EmbeddedChannel implements ServerChannel {
        // No-op.
    }
}
