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
import io.netty.channel.Channel;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.lang.IgniteInternalException;
import org.apache.ignite.network.message.MessageSerializationRegistry;

/**
 * Netty client channel wrapper.
 */
public class NettyClient {
    /** Serialization registry. */
    private final MessageSerializationRegistry serializationRegistry;

    /** Destination address. */
    private final SocketAddress address;

    /** Future that resolves when client channel is opened. */
    private CompletableFuture<NettySender> clientFuture;

    /** Client channel. */
    private volatile Channel channel;

    /** Client close future. */
    private CompletableFuture<Void> clientCloseFuture;

    /**
     * Constructor.
     *
     * @param host Host.
     * @param port Port.
     * @param serializationRegistry Serialization registry.
     */
    public NettyClient(
        String host,
        int port,
        MessageSerializationRegistry serializationRegistry
    ) {
        this(new InetSocketAddress(host, port), serializationRegistry);
    }

    /**
     * Constructor.
     *
     * @param address Destination address.
     * @param serializationRegistry Serialization registry.
     */
    public NettyClient(
        SocketAddress address,
        MessageSerializationRegistry serializationRegistry
    ) {
        this.address = address;
        this.serializationRegistry = serializationRegistry;
    }

    /**
     * Start client.
     *
     * @return Future that resolves when client channel is opened.
     */
    public CompletableFuture<NettySender> start(Bootstrap bootstrap) {
        if (clientFuture != null)
            throw new IgniteInternalException("Attempted to start an already started NettyClient");

        clientFuture = NettyUtils.toChannelCompletableFuture(bootstrap.connect(address))
            .thenApply(ch -> {
                clientCloseFuture = NettyUtils.toCompletableFuture(ch.closeFuture());
                channel = ch;

                return new NettySender(channel, serializationRegistry);
            });

        return clientFuture;
    }

    /**
     * @return Client start future.
     */
    public CompletableFuture<NettySender> sender() {
        return clientFuture;
    }

    /**
     * Stops the client.
     *
     * @return Future that is resolved when the client's channel has closed.
     */
    public CompletableFuture<Void> stop() {
        channel.close();

        return clientCloseFuture;
    }

    /**
     * @return {@code true} if the client has failed to connect to the remote server, {@code false} otherwise.
     */
    public boolean failedToConnect() {
        return clientFuture.isCompletedExceptionally();
    }

    /**
     * @return {@code true} if the client has lost the connection, {@code false} otherwise.
     */
    public boolean isDisconnected() {
        return channel != null && !channel.isOpen();
    }
}
