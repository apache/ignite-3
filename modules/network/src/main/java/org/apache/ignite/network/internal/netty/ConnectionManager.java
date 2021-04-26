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

package org.apache.ignite.network.internal.netty;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.BiConsumer;
import org.apache.ignite.lang.IgniteInternalException;
import org.apache.ignite.network.message.MessageSerializationRegistry;
import org.apache.ignite.network.message.NetworkMessage;

/**
 * Class that manages connections both incoming and outgoing.
 */
public class ConnectionManager {
    /** Latest version of the direct marshalling protocol. */
    public static final byte DIRECT_PROTOCOL_VERSION = 1;

    /** Client bootstrap. */
    private final Bootstrap clientBootstrap;

    /** Client socket channel handler event loop group. */
    private final EventLoopGroup clientWorkerGroup = new NioEventLoopGroup();

    /** Server. */
    private final NettyServer server;

    /** Channels. */
    private final Map<InetSocketAddress, NettySender> channels = new ConcurrentHashMap<>();

    /** Clients. */
    private final Map<InetSocketAddress, NettyClient> clients = new ConcurrentHashMap<>();

    /** Serialization registry. */
    private final MessageSerializationRegistry serializationRegistry;

    /** Message listeners. */
    private final List<BiConsumer<InetSocketAddress, NetworkMessage>> listeners = new CopyOnWriteArrayList<>(new ArrayList<>());

    public ConnectionManager(int port, MessageSerializationRegistry provider) {
        this.serializationRegistry = provider;
        this.server = new NettyServer(port, this::onNewIncomingChannel, this::onMessage, serializationRegistry);
        this.clientBootstrap = NettyClient.setupBootstrap(clientWorkerGroup, serializationRegistry, this::onMessage);
    }

    /**
     * Start server.
     *
     * @throws IgniteInternalException If failed to start.
     */
    public void start() throws IgniteInternalException {
        try {
            server.start().join();
        }
        catch (CompletionException e) {
            Throwable cause = e.getCause();
            throw new IgniteInternalException("Failed to start server: " + cause.getMessage(), cause);
        }
    }

    /**
     * @return Server local address.
     */
    public InetSocketAddress getLocalAddress() {
        return server.address();
    }

    /**
     * Get a {@link NettySender}, that sends data from this node to another node with the specified address.
     * @param address Another node's address.
     * @return Sender.
     */
    public CompletableFuture<NettySender> channel(InetSocketAddress address) {
        NettySender channel = channels.get(address);

        if (channel == null) {
            NettyClient client = clients.computeIfAbsent(address, this::connect);
            return client.sender();
        }

        return CompletableFuture.completedFuture(channel);
    }

    /**
     * Callback that is called upon receiving of a new message.
     *
     * @param from Source of the message.
     * @param message New message.
     */
    private void onMessage(InetSocketAddress from, NetworkMessage message) {
        listeners.forEach(consumer -> consumer.accept(from, message));
    }

    /**
     * Callback that is called upon new client connected to the server.
     *
     * @param channel Channel from client to this {@link #server}.
     */
    private void onNewIncomingChannel(SocketChannel channel) {
        InetSocketAddress remoteAddress = channel.remoteAddress();
        // TODO: there might be outgoing connection already
        channels.put(remoteAddress, new NettySender(channel, serializationRegistry));
    }

    /**
     * Create new client from this node to specified address.
     *
     * @param address Target address.
     * @return New netty client.
     */
    private NettyClient connect(InetSocketAddress address) {
        NettyClient client = new NettyClient(
            address.getHostName(),
            address.getPort(),
            serializationRegistry
        );

        client.start(clientBootstrap).whenComplete((sender, throwable) -> {
            if (throwable != null)
                clients.remove(address);
            else
                channels.put(address, sender);
        });

        return client;
    }

    /**
     * Add incoming message listener.
     *
     * @param listener Message listener.
     */
    public void addListener(BiConsumer<InetSocketAddress, NetworkMessage> listener) {
        listeners.add(listener);
    }

    /**
     * Stop server and all clients.
     */
    public void stop() {
        // Stop clients' event loop, there will be no new client channels since this point
        clientWorkerGroup.shutdownGracefully();

        // Stop the server
        this.server.stop();

        // Wait for clients' channels to close
        clients.values().forEach(NettyClient::stop);
    }
}
