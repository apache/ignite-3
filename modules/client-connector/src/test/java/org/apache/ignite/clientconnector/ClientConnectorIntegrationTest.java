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

package org.apache.ignite.clientconnector;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import org.apache.ignite.configuration.annotation.ConfigurationType;
import org.apache.ignite.configuration.schemas.clientconnector.ClientConnectorConfiguration;
import org.apache.ignite.internal.configuration.ConfigurationRegistry;
import org.junit.jupiter.api.Test;
import org.slf4j.helpers.NOPLogger;

import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Client connector integration tests with real sockets.
 */
public class ClientConnectorIntegrationTest {
    @Test
    void testHandshake() throws Exception {
        ChannelFuture channelFuture = startServer();

        var res = clientSendReceive(new byte[]{1, 2, 3, 4, 5});

        channelFuture.cancel(true);
        channelFuture.await();
    }

    private byte[] clientSendReceive(byte[] request) throws Exception {
        CompletableFuture<byte[]> result = new CompletableFuture<>();
        CompletableFuture<ChannelHandlerContext> chCtx = new CompletableFuture<>();

        var workerGroup = new NioEventLoopGroup();

        try {
            Bootstrap b = new Bootstrap();
            b.group(workerGroup);
            b.channel(NioSocketChannel.class);
            b.option(ChannelOption.SO_KEEPALIVE, true);
            b.handler(new ChannelInitializer<SocketChannel>() {
                @Override
                public void initChannel(SocketChannel ch)  {
                    ch.pipeline().addLast(new ChannelInboundHandlerAdapter() {
                        @Override
                        public void channelRead(ChannelHandlerContext ctx, Object msg) {
                            ByteBuf m = (ByteBuf) msg;
                            result.complete(m.array());
                            m.release();
                        }

                        @Override
                        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
                            cause.printStackTrace();
                            ctx.close();
                        }

                        @Override
                        public void channelActive(ChannelHandlerContext ctx) throws Exception {
                            chCtx.complete(ctx);
                            super.channelActive(ctx);
                        }
                    });
                }
            });

            ChannelFuture f = b.connect("127.0.0.1", 10800).sync();

            chCtx.get().channel().writeAndFlush(Unpooled.copiedBuffer(request));

            var res = result.get();

            f.channel().closeFuture().sync();

            return res;
        } finally {
            workerGroup.shutdownGracefully();
        }
    }

    private ChannelFuture startServer() throws InterruptedException {
        var registry = new ConfigurationRegistry(
                Collections.singletonList(ClientConnectorConfiguration.KEY),
                Collections.emptyMap(),
                Collections.singletonList(new TestConfigurationStorage(ConfigurationType.LOCAL))
        );

        var module = new ClientConnectorModule(NOPLogger.NOP_LOGGER);

        module.prepareStart(registry);

        return module.start();
    }
}
