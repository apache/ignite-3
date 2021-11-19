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

package org.apache.ignite.network;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import org.apache.ignite.configuration.schemas.network.NetworkView;
import org.apache.ignite.configuration.schemas.network.OutboundView;
import org.apache.ignite.internal.network.netty.NamedNioEventLoopGroup;

/**
 * Netty bootstrap factory.
 */
public class NettyBootstrapFactory {
    /** Node consistent id. */
    private String consistentId;
    
    /** Network configuration. */
    private NetworkView networkConfiguration;
    
    /** Server boss socket channel handler event loop group. */
    private final EventLoopGroup bossGroup;
    
    /** Server work socket channel handler event loop group. */
    private final EventLoopGroup workerGroup;
    
    /** Client socket channel handler event loop group. */
    private final EventLoopGroup clientWorkerGroup;
    
    /**
     * Constructor.
     *
     * @param networkConfiguration Network configuration.
     * @param consistentId         Consistent id of this node.
     */
    public NettyBootstrapFactory(
            NetworkView networkConfiguration,
            String consistentId
    ) {
        assert consistentId != null;
        assert networkConfiguration != null;
        
        this.consistentId = consistentId;
        this.networkConfiguration = networkConfiguration;
        
        this.bossGroup = NamedNioEventLoopGroup.create(consistentId + "-srv-accept");
        this.workerGroup = NamedNioEventLoopGroup.create(consistentId + "-srv-worker");
        this.clientWorkerGroup = NamedNioEventLoopGroup.create(consistentId + "-client");
    }
    
    /**
     * Creates bootstrap for outbound client connections.
     *
     * @return Bootstrap.
     */
    public Bootstrap createClientBootstrap() {
        OutboundView clientConfiguration = networkConfiguration.outbound();
        Bootstrap clientBootstrap = new Bootstrap();
        
        clientBootstrap.group(clientWorkerGroup)
                .channel(NioSocketChannel.class)
                // See NettyServer#start for netty configuration details.
                .option(ChannelOption.SO_KEEPALIVE, clientConfiguration.soKeepAlive())
                .option(ChannelOption.SO_LINGER, clientConfiguration.soLinger())
                .option(ChannelOption.TCP_NODELAY, clientConfiguration.tcpNoDelay());
        
        return clientBootstrap;
    }
}
