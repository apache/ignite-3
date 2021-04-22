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
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.stream.ChunkedWriteHandler;
import java.net.InetSocketAddress;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;
import org.apache.ignite.network.message.MessageSerializationRegistry;
import org.apache.ignite.network.message.NetworkMessage;

/**
 * Netty client channel wrapper.
 */
public class NettyClient {
    /** Socket channel bootstrapper. */
    private final Bootstrap bootstrap = new Bootstrap();

    /** Socket channel handler event loop group. */
    private final EventLoopGroup workerGroup = new NioEventLoopGroup();

    /** Serialization registry. */
    private final MessageSerializationRegistry serializationRegistry;

    /** Destination host. */
    private final String host;

    /** Destination port. */
    private final int port;

    /** Incoming message listener. */
    private final BiConsumer<InetSocketAddress, NetworkMessage> messageListener;

    /** Future that resolves when client channel is opened. */
    private final CompletableFuture<NettySender> clientFuture = new CompletableFuture<>();

    /** Client socket channel. */
    private Channel channel;

    public NettyClient(String host, int port, MessageSerializationRegistry serializationRegistry, BiConsumer<InetSocketAddress, NetworkMessage> listener) {
        this.host = host;
        this.port = port;
        this.serializationRegistry = serializationRegistry;
        this.messageListener = listener;
    }

    /**
     * Start client.
     *
     * @return Future that resolves when client channel is opened.
     */
    public CompletableFuture<NettySender> start() {
        bootstrap.group(workerGroup)
            .channel(NioSocketChannel.class)
            /** See {@link NettyServer#start} for netty configuration details. */
            .option(ChannelOption.SO_KEEPALIVE, true)
            .handler(new ChannelInitializer<SocketChannel>() {
                /** {@inheritDoc} */
                @Override public void initChannel(SocketChannel ch)
                    throws Exception {
                    ch.pipeline().addLast(
                        new InboundDecoder(serializationRegistry),
                        new MessageHandler(messageListener),
                        new ChunkedWriteHandler()
                    );
                }
        });

        ChannelFuture connectFuture = bootstrap.connect(host, port);

        connectFuture.addListener(connect -> {
            this.channel = connectFuture.channel();
            if (connect.isSuccess())
                clientFuture.complete(new NettySender(channel, serializationRegistry));
            else {
                Throwable cause = connect.cause();
                clientFuture.completeExceptionally(cause);
            }

            // Shutdown event loop group when channel is closed.
            channel.closeFuture().addListener(close -> {
               workerGroup.shutdownGracefully();
            });
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
     * Stop client.
     */
    public void stop() {
        this.channel.close().awaitUninterruptibly();
    }
}
