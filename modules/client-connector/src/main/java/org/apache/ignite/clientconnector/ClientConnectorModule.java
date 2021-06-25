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

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.codec.http.HttpServerExpectContinueHandler;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import org.apache.ignite.configuration.schemas.clientconnector.ClientConnectorConfiguration;
import org.apache.ignite.internal.configuration.ConfigurationRegistry;
import org.slf4j.Logger;

import java.net.BindException;

/**
 * Client connector module maintains TCP endpoint for thin client connections.
 *
 */
public class ClientConnectorModule {
    /** */
    private ConfigurationRegistry sysConf;

    /** */
    private final Logger log;

    /**
     * @param log Logger.
     */
    public ClientConnectorModule(Logger log) {
        this.log = log;
    }

    /**
     * @param sysCfg Configuration registry.
     */
    public void prepareStart(ConfigurationRegistry sysCfg) {
        sysConf = sysCfg;
    }

    /**
     * @return channel future.
     * @throws InterruptedException If thread has been interupted during the start.
     */
    public ChannelFuture start() throws InterruptedException {
        return startEndpoint();
    }

    /** */
    private ChannelFuture startEndpoint() throws InterruptedException {
        var configuration = sysConf.getConfiguration(ClientConnectorConfiguration.KEY);

        // TODO: Why defaults are not returned?
        int desiredPort = configuration.port().value() == null ? 10800 : configuration.port().value();
        int portRange = configuration.portRange().value() == null ? 1 : configuration.portRange().value();

        int port = 0;

        Channel ch = null;

        EventLoopGroup parentGrp = new NioEventLoopGroup();
        EventLoopGroup childGrp = new NioEventLoopGroup();

        ServerBootstrap b = new ServerBootstrap();
        b.option(ChannelOption.SO_BACKLOG, 1024);
        b.group(parentGrp, childGrp) // TODO: Why two groups?
            .channel(NioServerSocketChannel.class)
            .handler(new LoggingHandler(LogLevel.INFO))
            .childHandler(new ChannelInitializer<>() {
                @Override
                protected void initChannel(Channel ch) throws Exception {
                    ch.pipeline().addLast(new ClientMessageHandler());
                }
            });

        for (int portCandidate = desiredPort; portCandidate < desiredPort + portRange; portCandidate++) {
            ChannelFuture bindRes = b.bind(portCandidate).await();
            if (bindRes.isSuccess()) {
                ch = bindRes.channel();

                ch.closeFuture().addListener((ChannelFutureListener) fut -> {
                    parentGrp.shutdownGracefully();
                    childGrp.shutdownGracefully();
                });

                port = portCandidate;
                break;
            }
            else if (!(bindRes.cause() instanceof BindException)) {
                parentGrp.shutdownGracefully();
                childGrp.shutdownGracefully();
                throw new RuntimeException(bindRes.cause());
            }
        }

        if (ch == null) {
            String msg = "Cannot start thin client connector endpoint. " +
                "All ports in range [" + desiredPort + ", " + (desiredPort + portRange) + "] are in use.";

            log.error(msg);

            parentGrp.shutdownGracefully();
            childGrp.shutdownGracefully();

            throw new RuntimeException(msg);
        }

        log.info("Thin client connector started successfully on port " + port);

        return ch.closeFuture();
    }
}
