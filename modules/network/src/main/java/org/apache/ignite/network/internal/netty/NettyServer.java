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
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.ServerSocketChannel;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.stream.ChunkedWriteHandler;
import java.net.InetSocketAddress;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import org.apache.ignite.network.message.MessageSerializationRegistry;
import org.apache.ignite.network.message.NetworkMessage;

/**
 * Netty server channel wrapper.
 */
public class NettyServer {
    /** {@link ServerSocketChannel} bootstrapper. */
    private final ServerBootstrap bootstrap = new ServerBootstrap();

    /** Socket accepter event loop group. */
    private final NioEventLoopGroup bossGroup = new NioEventLoopGroup();

    /** Socket handler event loop group. */
    private final NioEventLoopGroup workerGroup = new NioEventLoopGroup();

    /** Server port. */
    private final int port;

    /** Serialization registry. */
    private final MessageSerializationRegistry serializationRegistry;

    /** Incoming message listener. */
    private final BiConsumer<InetSocketAddress, NetworkMessage> messageListener;

    /** Server socket channel. */
    private ServerSocketChannel channel;

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
        BiConsumer<InetSocketAddress, NetworkMessage> messageListener,
        MessageSerializationRegistry serializationRegistry
    ) {
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

        CompletableFuture<Void> serverStartFuture = new CompletableFuture<>();

        bootstrap.bind(port).addListener((ChannelFutureListener) bind -> {
            this.channel = (ServerSocketChannel) bind.channel();

            if (bind.isSuccess())
                serverStartFuture.complete(null);
            else
                serverStartFuture.completeExceptionally(bind.cause());

            // Shutdown event loops on server stop.
            channel.closeFuture().addListener(close -> {
                workerGroup.shutdownGracefully();
                bossGroup.shutdownGracefully();
            });
        });

        return serverStartFuture;
    }

    /**
     * @return Gets the local address of the server.
     */
    public InetSocketAddress address() {
        return channel.localAddress();
    }

    /**
     * Stops the server.
     */
    public void stop() {
        channel.close().awaitUninterruptibly();
    }
}
