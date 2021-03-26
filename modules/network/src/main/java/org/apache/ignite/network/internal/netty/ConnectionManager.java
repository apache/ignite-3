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

import io.netty.channel.socket.SocketChannel;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.function.BiConsumer;
import org.apache.ignite.lang.IgniteInternalException;
import org.apache.ignite.network.message.MessageSerializationRegistry;
import org.apache.ignite.network.message.NetworkMessage;

/**
 * Class that manages connections both incoming and outgoing.
 */
public class ConnectionManager {
    /** Server. */
    private NettyServer server;

    /** Senders that wrap channels both incoming and outgoing. */
    private Map<InetSocketAddress, NettySender> channels = new ConcurrentHashMap<>();

    /** Clients. */
    private Map<InetSocketAddress, NettyClient> clients = new ConcurrentHashMap<>();

    /** Server port. */
    private final int port;

    /** Serialization registry. */
    private final MessageSerializationRegistry serializationRegistry;

    /** Message listeners. */
    private List<BiConsumer<InetSocketAddress, NetworkMessage>> listeners = Collections.synchronizedList(new ArrayList<>());

    public ConnectionManager(int port, MessageSerializationRegistry provider) {
        this.port = port;
        this.serializationRegistry = provider;
        this.server = new NettyServer(port, this::onNewIncomingChannel, this::onMessage, serializationRegistry);
    }

    /**
     * Start server.
     *
     * @throws IgniteInternalException If failed to start.
     */
    public void start() throws IgniteInternalException {
        try {
            server.start().get();
        }
        catch (ExecutionException e) {
            Throwable cause = e.getCause();
            throw new IgniteInternalException("Failed to start server: " + cause.getMessage(), cause);
        }
        catch (InterruptedException e) {
            throw new IgniteInternalException(
                "Got interrupted while waiting for server to start: " + e.getMessage(),
                e
            );
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
        listeners.forEach(consumer -> {
            consumer.accept(from, message);
        });
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
        NettyClient client = new NettyClient(address.getHostName(), address.getPort(), serializationRegistry, (src, message) -> {
            this.onMessage(src, message);
        });

        client.start().whenComplete((sender, throwable) -> {
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
        // TODO: maybe add some flag that prohibts opening new connections from this moment?
        this.server.stop();
        HashMap<InetSocketAddress, NettyClient> map = new HashMap<>(clients);
        map.values().forEach(client -> {
            client.stop();
        });
    }
}
