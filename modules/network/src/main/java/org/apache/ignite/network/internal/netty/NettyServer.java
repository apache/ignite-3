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

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ServerChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.stream.ChunkedWriteHandler;
import java.net.SocketAddress;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import org.apache.ignite.lang.IgniteInternalException;
import org.apache.ignite.network.message.MessageSerializationRegistry;
import org.apache.ignite.network.message.NetworkMessage;
import org.jetbrains.annotations.TestOnly;

/**
 * Netty server channel wrapper.
 */
public class NettyServer {
    /** {@link NioServerSocketChannel} bootstrapper. */
    private final ServerBootstrap bootstrap;

    /** Socket accepter event loop group. */
    private final NioEventLoopGroup bossGroup = new NioEventLoopGroup();

    /** Socket handler event loop group. */
    private final NioEventLoopGroup workerGroup = new NioEventLoopGroup();

    /** Server port. */
    private final int port;

    /** Serialization registry. */
    private final MessageSerializationRegistry serializationRegistry;

    /** Incoming message listener. */
    private final BiConsumer<SocketAddress, NetworkMessage> messageListener;

    /** Server start future. */
    private CompletableFuture<Void> serverStartFuture;

    /** Server socket channel. */
    private volatile ServerChannel channel;

    /** Server close future. */
    private CompletableFuture<Void> serverCloseFuture;

    /** New connections listener. */
    private final Consumer<NettySender> newConnectionListener;

    /**
     * Constructor.
     *
     * @param port Server port.
     * @param newConnectionListener New connections listener.
     * @param messageListener Message listener.
     * @param serializationRegistry Serialization registry.
     */
    public NettyServer(
        int port,
        Consumer<NettySender> newConnectionListener,
        BiConsumer<SocketAddress, NetworkMessage> messageListener,
        MessageSerializationRegistry serializationRegistry
    ) {
        this(new ServerBootstrap(), port, newConnectionListener, messageListener, serializationRegistry);
    }

    /**
     * Constructor.
     *
     * @param bootstrap Server bootstrap.
     * @param port Server port.
     * @param newConnectionListener New connections listener.
     * @param messageListener Message listener.
     * @param serializationRegistry Serialization registry.
     */
    public NettyServer(
        ServerBootstrap bootstrap,
        int port,
        Consumer<NettySender> newConnectionListener,
        BiConsumer<SocketAddress, NetworkMessage> messageListener,
        MessageSerializationRegistry serializationRegistry
    ) {
        this.bootstrap = bootstrap;
        this.port = port;
        this.newConnectionListener = newConnectionListener;
        this.messageListener = messageListener;
        this.serializationRegistry = serializationRegistry;
    }

    /**
     * Starts the server.
     *
     * @return Future that resolves when the server is successfully started.
     */
    public CompletableFuture<Void> start() {
        if (serverStartFuture != null)
            throw new IgniteInternalException("Attempted to start an already started server");

        bootstrap.group(bossGroup, workerGroup)
            .channel(NioServerSocketChannel.class)
            .childHandler(new ChannelInitializer<SocketChannel>() {
                /** {@inheritDoc} */
                @Override public void initChannel(SocketChannel ch) {
                    ch.pipeline().addLast(
                        /*
                         * Decoder that uses org.apache.ignite.network.internal.MessageReader
                         * to read chunked data.
                         */
                        new InboundDecoder(serializationRegistry),
                        // Handles decoded NetworkMessages.
                        new MessageHandler(messageListener),
                        /*
                         * Encoder that uses org.apache.ignite.network.internal.MessageWriter
                         * to write chunked data.
                         */
                        new ChunkedWriteHandler()
                    );

                    newConnectionListener.accept(new NettySender(ch, serializationRegistry));
                }
            })
            /*
             * The maximum queue length for incoming connection indications (a request to connect) is set
             * to the backlog parameter. If a connection indication arrives when the queue is full,
             * the connection is refused.
             */
            .option(ChannelOption.SO_BACKLOG, 128)
            /*
             * When the keepalive option is set for a TCP socket and no data has been exchanged across the socket
             * in either direction for 2 hours (NOTE: the actual value is implementation dependent),
             * TCP automatically sends a keepalive probe to the peer.
             */
            .childOption(ChannelOption.SO_KEEPALIVE, true);

        serverStartFuture = NettyUtils.toCompletableFuture(bootstrap.bind(port), ChannelFuture::channel).thenAccept(ch -> {
            channel = (ServerChannel) ch;

            serverCloseFuture = CompletableFuture.allOf(
                NettyUtils.toCompletableFuture(bossGroup.terminationFuture(), future -> null),
                NettyUtils.toCompletableFuture(workerGroup.terminationFuture(), future -> null),
                NettyUtils.toCompletableFuture(channel.closeFuture(), future -> null)
            );

            // Shutdown event loops on server stop.
            channel.closeFuture().addListener(close -> {
                bossGroup.shutdownGracefully();
                workerGroup.shutdownGracefully();
            });
        });

        return serverStartFuture;
    }

    /**
     * @return Gets the local address of the server.
     */
    public SocketAddress address() {
        return channel.localAddress();
    }

    /**
     * Stops the server.
     *
     * @return Future that is resolved when the server's channel has closed.
     */
    public CompletableFuture<Void> stop() {
        channel.close();

        return serverCloseFuture;
    }

    /**
     * @return {@code true} if the server is running, {@code false} otherwise.
     */
    @TestOnly
    public boolean isRunning() {
        return channel.isOpen() && !bossGroup.isShuttingDown() && !workerGroup.isShuttingDown();
    }

    /**
     * @return Accepter event loop group.
     */
    @TestOnly
    public NioEventLoopGroup getBossGroup() {
        return bossGroup;
    }

    /**
     * @return Worker event loop group.
     */
    @TestOnly
    public NioEventLoopGroup getWorkerGroup() {
        return workerGroup;
    }
}
