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

package org.apache.ignite.internal.network.netty;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.embedded.EmbeddedChannel;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.channels.ClosedChannelException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.internal.future.OrderingFuture;
import org.apache.ignite.internal.network.handshake.HandshakeManager;
import org.apache.ignite.lang.IgniteInternalException;
import org.apache.ignite.network.NetworkMessage;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

/**
 * Tests for {@link NettyClient}.
 */
public class NettyClientTest {
    /** Client. */
    private NettyClient client;

    private final SocketAddress address = InetSocketAddress.createUnresolved("", 0);

    /**
     * After each.
     */
    @AfterEach
    void tearDown() {
        client.stop().join();
    }

    /**
     * Tests a scenario where NettyClient connects successfully.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testSuccessfulConnect() throws Exception {
        var channel = new EmbeddedChannel();

        ClientAndSender tuple = createClientAndSenderFromChannelFuture(channel.newSucceededFuture());

        NettySender sender = tuple.sender.get(3, TimeUnit.SECONDS);
        client = tuple.client;

        assertNotNull(sender);
        assertTrue(sender.isOpen());

        assertFalse(client.failedToConnect());
        assertFalse(client.isDisconnected());

        assertFalse(channel.finish());
    }

    /**
     * Tests a scenario where NettyClient fails to connect.
     */
    @Test
    public void testFailedToConnect() {
        var channel = new EmbeddedChannel();

        ClientAndSender tuple = createClientAndSenderFromChannelFuture(channel.newFailedFuture(new ClosedChannelException()));

        assertThrows(ClosedChannelException.class, () -> {
            try {
                tuple.sender.get(3, TimeUnit.SECONDS);
            } catch (Exception e) {
                throw e.getCause();
            }
        });

        client = tuple.client;

        assertTrue(client.failedToConnect());
        assertFalse(client.isDisconnected());

        assertFalse(channel.finish());
    }

    /**
     * Tests a scenario where a connection is established successfully and is closed afterwards.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testCloseConnection() throws Exception {
        var channel = new EmbeddedChannel();

        ClientAndSender tuple = createClientAndSenderFromChannelFuture(channel.newSucceededFuture());

        NettySender sender = tuple.sender.get(3, TimeUnit.SECONDS);
        client = tuple.client;

        channel.close();

        assertFalse(sender.isOpen());

        assertTrue(tuple.client.isDisconnected());
        assertFalse(tuple.client.failedToConnect());

        assertFalse(channel.finish());
    }

    /**
     * Tests a scenario where a connection is established successfully after a client has been stopped.
     */
    @Test
    public void testStoppedBeforeStarted() {
        var channel = new EmbeddedChannel();

        var future = channel.newPromise();

        ClientAndSender tuple = createClientAndSenderFromChannelFuture(future);

        tuple.client.stop();

        future.setSuccess(null);

        client = tuple.client;

        assertThrows(ExecutionException.class, () -> tuple.sender.get(3, TimeUnit.SECONDS));

        assertTrue(client.isDisconnected());
        assertTrue(client.failedToConnect());

        assertFalse(channel.finish());
    }

    /**
     * Tests that a {@link NettyClient#start} method can be called only once.
     */
    @Test
    public void testStartTwice() {
        var channel = new EmbeddedChannel();

        Bootstrap bootstrap = mockBootstrap();

        Mockito.doReturn(channel.newSucceededFuture()).when(bootstrap).connect(Mockito.any());

        client = new NettyClient(
                address,
                null,
                new MockClientHandshakeManager(channel),
                (message) -> {
                }
        );

        client.start(bootstrap);

        assertThrows(IgniteInternalException.class, () -> {
            client.start(bootstrap);
        });

        assertFalse(channel.finish());
    }

    /**
     * Creates a NettyClient and an associated NettySender future from Netty's ChannelFuture.
     *
     * @param future Channel future.
     * @return Client and a NettySender future.
     */
    private ClientAndSender createClientAndSenderFromChannelFuture(ChannelFuture future) {
        var client = new NettyClient(
                address,
                null,
                new MockClientHandshakeManager(future.channel()),
                (message) -> {
                }
        );

        Bootstrap bootstrap = mockBootstrap();

        Mockito.doReturn(future).when(bootstrap).connect(Mockito.any());

        return new ClientAndSender(client, client.start(bootstrap));
    }

    /**
     * Create mock of a {@link Bootstrap} that implements {@link Bootstrap#clone()}.
     *
     * @return Mocked bootstrap.
     */
    private Bootstrap mockBootstrap() {
        Bootstrap bootstrap = Mockito.mock(Bootstrap.class);

        Mockito.doReturn(bootstrap).when(bootstrap).clone();

        return bootstrap;
    }

    /**
     * Tuple for a NettyClient and a future of a NettySender.
     */
    private static class ClientAndSender {
        private final NettyClient client;

        private final OrderingFuture<NettySender> sender;

        /**
         * Constructor.
         *
         * @param client Netty client.
         * @param sender Netty sender.
         */
        private ClientAndSender(NettyClient client, OrderingFuture<NettySender> sender) {
            this.client = client;
            this.sender = sender;
        }
    }

    /**
     * Client handshake manager that doesn't do any actual handshaking.
     */
    private static class MockClientHandshakeManager implements HandshakeManager {
        /** Sender. */
        private final NettySender sender;

        /** Constructor. */
        private MockClientHandshakeManager(Channel channel) {
            this.sender = new NettySender(channel, "", "");
        }

        /** {@inheritDoc} */
        @Override
        public void onMessage(NetworkMessage message) {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override
        public CompletableFuture<NettySender> handshakeFuture() {
            return CompletableFuture.completedFuture(sender);
        }

        /** {@inheritDoc} */
        @Override
        public void onInit(ChannelHandlerContext ctx) {
            // No-op.
        }
    }
}
