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

package org.apache.ignite.internal.network;

import static java.util.concurrent.CompletableFuture.failedFuture;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;

import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoop;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.concurrent.EventExecutor;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.manager.ComponentContext;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.network.configuration.InboundView;
import org.apache.ignite.internal.network.configuration.NetworkConfiguration;
import org.apache.ignite.internal.network.configuration.NetworkView;
import org.apache.ignite.internal.network.configuration.OutboundView;
import org.apache.ignite.internal.network.handshake.HandshakeEventLoopSwitcher;
import org.apache.ignite.internal.network.netty.NamedNioEventLoopGroup;
import org.apache.ignite.internal.network.netty.NamedNioEventLoopGroup.NetworkThread;
import org.jetbrains.annotations.TestOnly;

/**
 * Netty bootstrap factory. Holds shared {@link EventLoopGroup} instances and encapsulates common Netty {@link Bootstrap} creation logic.
 */
public class NettyBootstrapFactory implements IgniteComponent {
    /** Network configuration. */
    private final NetworkConfiguration networkConfiguration;

    /** Prefix for event loop group names. */
    private final String eventLoopGroupNamePrefix;

    /** Boss socket channel handler event loop group (this group accepts connections). */
    private EventLoopGroup bossGroup;

    /** Work socket channel handler event loop group (this group does network I/O). */
    private EventLoopGroup workerGroup;

    private volatile HandshakeEventLoopSwitcher handshakeEventLoopSwitcher;

    /**
     * Constructor.
     *
     * @param networkConfiguration Network configuration.
     * @param eventLoopGroupNamePrefix Prefix for event loop group names.
     */
    public NettyBootstrapFactory(NetworkConfiguration networkConfiguration, String eventLoopGroupNamePrefix) {
        assert eventLoopGroupNamePrefix != null;
        assert networkConfiguration != null;

        this.networkConfiguration = networkConfiguration;
        this.eventLoopGroupNamePrefix = eventLoopGroupNamePrefix;
    }

    /**
     * Creates bootstrap for outbound client connections.
     *
     * @return Bootstrap.
     */
    public Bootstrap createOutboundBootstrap() {
        OutboundView outboundConfiguration = networkConfiguration.value().outbound();
        Bootstrap outboundBootstrap = new Bootstrap();

        outboundBootstrap.group(workerGroup)
                .channel(NioSocketChannel.class)
                // See createServerBootstrap for netty configuration details.
                .option(ChannelOption.SO_KEEPALIVE, outboundConfiguration.soKeepAlive())
                .option(ChannelOption.SO_LINGER, outboundConfiguration.soLinger())
                .option(ChannelOption.TCP_NODELAY, outboundConfiguration.tcpNoDelay());

        return outboundBootstrap;
    }

    /**
     * Creates bootstrap for inbound server connections.
     *
     * @return Bootstrap.
     */
    public ServerBootstrap createServerBootstrap() {
        InboundView serverConfiguration = networkConfiguration.value().inbound();
        ServerBootstrap serverBootstrap = new ServerBootstrap();

        serverBootstrap.group(bossGroup, workerGroup)
                .channel(NioServerSocketChannel.class)
                /*
                 * The maximum queue length for incoming connection indications (a request to connect) is set
                 * to the backlog parameter. If a connection indication arrives when the queue is full,
                 * the connection is refused.
                 */
                .option(ChannelOption.SO_BACKLOG, serverConfiguration.soBacklog())
                .option(ChannelOption.SO_REUSEADDR, serverConfiguration.soReuseAddr())
                /*
                 * When the keepalive option is set for a TCP socket and no data has been exchanged across the socket
                 * in either direction for 2 hours (NOTE: the actual value is implementation dependent),
                 * TCP automatically sends a keepalive probe to the peer.
                 */
                .childOption(ChannelOption.SO_KEEPALIVE, serverConfiguration.soKeepAlive())
                /*
                 * Specify a linger-on-close timeout. This option disables/enables immediate return from a close()
                 * of a TCP Socket. Enabling this option with a non-zero Integer timeout means that a close() will
                 * block pending the transmission and acknowledgement of all data written to the peer, at which point
                 * the socket is closed gracefully. Upon reaching the linger timeout, the socket is closed forcefully,
                 * with a TCP RST. Enabling the option with a timeout of zero does a forceful close immediately.
                 * If the specified timeout value exceeds 65,535 it will be reduced to 65,535.
                 */
                .childOption(ChannelOption.SO_LINGER, serverConfiguration.soLinger())
                /*
                 * Disable Nagle's algorithm for this connection. Written data to the network is not buffered pending
                 * acknowledgement of previously written data. Valid for TCP only. Setting this option reduces
                 * network latency and and delivery time for small messages.
                 * For more information, see Socket#setTcpNoDelay(boolean)
                 * and https://en.wikipedia.org/wiki/Nagle%27s_algorithm.
                 */
                .childOption(ChannelOption.TCP_NODELAY, serverConfiguration.tcpNoDelay());

        return serverBootstrap;
    }

    /**
     * Returns all event loop groups managed by this factory for which it is necessary to determine blocked threads.
     */
    List<EventLoopGroup> eventLoopGroupsForBlockedThreadsDetection() {
        return List.of(workerGroup);
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Void> startAsync(ComponentContext componentContext) {
        bossGroup = NamedNioEventLoopGroup.create(eventLoopGroupNamePrefix + "-network-accept");
        workerGroup = NamedNioEventLoopGroup.create(eventLoopGroupNamePrefix + "-network-worker");

        this.handshakeEventLoopSwitcher = new HandshakeEventLoopSwitcher(eventLoopsAt(workerGroup));

        return nullCompletedFuture();
    }

    public HandshakeEventLoopSwitcher handshakeEventLoopSwitcher() {
        return handshakeEventLoopSwitcher;
    }

    private static List<EventLoop> eventLoopsAt(EventLoopGroup ... groups) {
        List<EventLoop> channelEventLoops = new ArrayList<>();

        for (EventLoopGroup group : groups) {
            for (EventExecutor child : group) {
                channelEventLoops.add((EventLoop) child);
            }
        }

        return channelEventLoops;
    }

    /**
     * Returns {@code true} if the current thread is a network thread, {@code false} otherwise.
     *
     * @return {@code true} if the current thread is a network thread, {@code false} otherwise.
     */
    public static boolean isInNetworkThread() {
        Thread thread = Thread.currentThread();

        return thread instanceof NetworkThread;
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Void> stopAsync(ComponentContext componentContext) {
        NetworkView configurationView = networkConfiguration.value();
        long quietPeriod = configurationView.shutdownQuietPeriodMillis();
        long shutdownTimeout = configurationView.shutdownTimeoutMillis();

        try {
            workerGroup.shutdownGracefully(quietPeriod, shutdownTimeout, MILLISECONDS).sync();
            bossGroup.shutdownGracefully(quietPeriod, shutdownTimeout, MILLISECONDS).sync();
        } catch (InterruptedException e) {
            return failedFuture(e);
        }

        return nullCompletedFuture();
    }

    /**
     * Returns worker event loop group.
     */
    @TestOnly
    public EventLoopGroup workerEventLoopGroup() {
        return workerGroup;
    }
}
