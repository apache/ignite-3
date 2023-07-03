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

/**
 * Proxy for Ignite client with request tracking.
 * Provides a way to test request routing while using real Ignite cluster.
 */
public final class IgniteClientProxy implements AutoCloseable {
    private final EventLoopGroup bossGroup = new NioEventLoopGroup(1);

    private final EventLoopGroup workerGroup = new NioEventLoopGroup();

    private final int listenPort;

    private final ChannelFuture chFut;

    private final IgniteClientProxyInitializer proxyHandler;

    private IgniteClientProxy(int targetPort, int listenPort) throws InterruptedException {
        this.listenPort = listenPort;

        ServerBootstrap b = new ServerBootstrap();

        this.proxyHandler = new IgniteClientProxyInitializer(targetPort);

        this.chFut = b.group(bossGroup, workerGroup)
                .channel(NioServerSocketChannel.class)
                .handler(new LoggingHandler(LogLevel.INFO))
                .childHandler(proxyHandler)
                .childOption(ChannelOption.AUTO_READ, false)
                .bind(listenPort).sync();
    }

    public static IgniteClientProxy start(int targetPort, int listenPort) throws Exception {
        return new IgniteClientProxy(targetPort, listenPort);
    }

    public int listenPort() {
        return listenPort;
    }

    public int requestCount() {
        return proxyHandler.requestCount();
    }

    public int resetRequestCount() {
        return proxyHandler.resetRequestCount();
    }

    @Override
    public void close() throws Exception {
        chFut.channel().close().sync();
        bossGroup.shutdownGracefully();
        workerGroup.shutdownGracefully();
    }
}
