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

package org.apache.ignite.internal.runner.app.client.proxy;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;

public final class IgniteClientProxy implements AutoCloseable {
    private final EventLoopGroup bossGroup = new NioEventLoopGroup(1);
    private final EventLoopGroup workerGroup = new NioEventLoopGroup();
    private final int targetPort;
    private final int listenPort;

    private IgniteClientProxy(int targetPort, int listenPort) {
        this.targetPort = targetPort;
        this.listenPort = listenPort;
    }

    private void start() throws InterruptedException {
        ServerBootstrap b = new ServerBootstrap();

        ChannelFuture chFut = b.group(bossGroup, workerGroup)
                .channel(NioServerSocketChannel.class)
                .handler(new LoggingHandler(LogLevel.INFO))
                .childHandler(new IgniteClientProxyInitializer(targetPort))
                .childOption(ChannelOption.AUTO_READ, false)
                .bind(listenPort).sync();

        chFut.channel().closeFuture().sync();
    }

    public static AutoCloseable start(int targetPort, int listenPort) throws Exception {
        IgniteClientProxy proxy = new IgniteClientProxy(targetPort, listenPort);
        proxy.start();

        return proxy;
    }

    @Override
    public void close() throws Exception {
        bossGroup.shutdownGracefully();
        workerGroup.shutdownGracefully();
    }
}
