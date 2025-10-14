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

package org.apache.ignite.internal.runner.app.client;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.apache.ignite.lang.IgniteException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;


/**
 * Tests thin client connection behavior when the cluster is not initialized.
 * This test simulates a scenario similar to a docker container proxy that accepts connections
 * and immediately drops them since the node's process starts listening on the port only after initialization.
 */
@ExtendWith(WorkDirectoryExtension.class)
public class ItThinClientUninitializedClusterTest extends BaseIgniteAbstractTest {
    private static final int CLIENT_PORT = 10809;
    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;
    private Channel serverChannel;

    @BeforeEach
    void startDummyNettyServer() throws InterruptedException {
        // Set up a mock tcp socket listener at CLIENT_PORT.
        bossGroup = new NioEventLoopGroup(1);

        workerGroup = new NioEventLoopGroup();

        ServerBootstrap bootstrap = new ServerBootstrap()
                .group(bossGroup, workerGroup)
                .channel(NioServerSocketChannel.class)
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) {
                        // Immediately close every new connection.
                        ch.pipeline().addLast(new ChannelInboundHandlerAdapter() {
                            @Override
                            public void channelActive(ChannelHandlerContext ctx) {
                                ctx.close();
                            }
                        });
                    }
                })
                .childOption(ChannelOption.SO_KEEPALIVE, false);

        ChannelFuture bindFuture = bootstrap.bind("127.0.0.1", CLIENT_PORT).sync();
        serverChannel = bindFuture.channel();
    }

    @Test
    void testThinClientConnectionToNotInitializedClusterThrowsException() {
        IgniteException ex = assertThrows(
                IgniteException.class,
                () -> {
                    try (IgniteClient client = IgniteClient.builder()
                            .addresses("127.0.0.1:" + CLIENT_PORT)
                            .build()) {
                        // Client construction should fail after tcp handshake.
                    }
                }
        );

        // Check that the cause chain contains the helpful error message.
        boolean foundMessage = false;
        Throwable cause = ex;
        while (cause != null) {
            if (cause.getMessage() != null && cause.getMessage().contains("cluster might not have been initialised")) {
                foundMessage = true;
                break;
            }
            cause = cause.getCause();
        }

        assertTrue(foundMessage, "Expected to find 'cluster might not have been initialised' in exception cause chain");
    }

    @AfterEach
    void stopDummyNettyServer() throws InterruptedException {
        // Stop the listener.
        if (serverChannel != null) {
            serverChannel.close().sync();
        }
        if (workerGroup != null) {
            workerGroup.shutdownGracefully().sync();
        }
        if (bossGroup != null) {
            bossGroup.shutdownGracefully().sync();
        }
    }
}
