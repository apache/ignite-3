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

package org.apache.ignite.client;

import static org.mockito.Mockito.mock;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.util.ReferenceCounted;
import java.net.BindException;
import java.net.SocketAddress;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import org.apache.ignite.Ignite;
import org.apache.ignite.client.handler.ClientHandlerMetricSource;
import org.apache.ignite.client.handler.ClientInboundMessageHandler;
import org.apache.ignite.client.handler.configuration.ClientConnectorConfiguration;
import org.apache.ignite.compute.IgniteCompute;
import org.apache.ignite.internal.client.proto.ClientMessageDecoder;
import org.apache.ignite.internal.configuration.AuthenticationConfiguration;
import org.apache.ignite.internal.configuration.ConfigurationRegistry;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.security.authentication.AuthenticationManager;
import org.apache.ignite.internal.security.authentication.AuthenticationManagerImpl;
import org.apache.ignite.internal.sql.engine.QueryProcessor;
import org.apache.ignite.internal.table.IgniteTablesInternal;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.network.ClusterService;
import org.apache.ignite.network.NettyBootstrapFactory;
import org.jetbrains.annotations.Nullable;

/**
 * Client handler module for tests.
 */
public class TestClientHandlerModule implements IgniteComponent {
    /** Configuration registry. */
    private final ConfigurationRegistry registry;

    /** Ignite. */
    private final Ignite ignite;

    /** Connection drop condition. */
    private final Function<Integer, Boolean> shouldDropConnection;

    /** Server response delay function. */
    private final Function<Integer, Integer> responseDelay;

    /** Cluster service. */
    private final ClusterService clusterService;

    /** Compute. */
    private final IgniteCompute compute;

    /** Cluster id. */
    private final UUID clusterId;

    /** Metrics. */
    private final ClientHandlerMetricSource metrics;

    /** Netty channel. */
    private volatile Channel channel;

    /** Netty bootstrap factory. */
    private final NettyBootstrapFactory bootstrapFactory;

    /** Authentication configuration. */
    private final AuthenticationConfiguration authenticationConfiguration;

    /**
     * Constructor.
     *
     * @param ignite Ignite.
     * @param registry Configuration registry.
     * @param bootstrapFactory Bootstrap factory.
     * @param shouldDropConnection Connection drop condition.
     * @param responseDelay Response delay, in milliseconds.
     * @param clusterService Cluster service.
     * @param compute Compute.
     * @param clusterId Cluster id.
     * @param metrics Metrics.
     */
    public TestClientHandlerModule(
            Ignite ignite,
            ConfigurationRegistry registry,
            NettyBootstrapFactory bootstrapFactory,
            Function<Integer, Boolean> shouldDropConnection,
            @Nullable Function<Integer, Integer> responseDelay,
            ClusterService clusterService,
            IgniteCompute compute,
            UUID clusterId,
            ClientHandlerMetricSource metrics,
            AuthenticationConfiguration authenticationConfiguration) {
        assert ignite != null;
        assert registry != null;
        assert bootstrapFactory != null;

        this.ignite = ignite;
        this.registry = registry;
        this.bootstrapFactory = bootstrapFactory;
        this.shouldDropConnection = shouldDropConnection;
        this.responseDelay = responseDelay;
        this.clusterService = clusterService;
        this.compute = compute;
        this.clusterId = clusterId;
        this.metrics = metrics;
        this.authenticationConfiguration = authenticationConfiguration;
    }

    /** {@inheritDoc} */
    @Override
    public void start() {
        if (channel != null) {
            throw new IgniteException("ClientHandlerModule is already started.");
        }

        try {
            channel = startEndpoint().channel();
        } catch (InterruptedException e) {
            throw new IgniteException(e);
        }
    }

    /** {@inheritDoc} */
    @Override
    public void stop() throws Exception {
        if (channel != null) {
            channel.close().await();

            channel = null;
        }
    }

    /**
     * Returns the local address where this handler is bound to.
     *
     * @return the local address of this module, or {@code null} if this module is not started.
     */
    @Nullable
    public SocketAddress localAddress() {
        return channel == null ? null : channel.localAddress();
    }

    /**
     * Starts the endpoint.
     *
     * @return Channel future.
     * @throws InterruptedException If thread has been interrupted during the start.
     * @throws IgniteException      When startup has failed.
     */
    private ChannelFuture startEndpoint() throws InterruptedException {
        var configuration = registry.getConfiguration(ClientConnectorConfiguration.KEY).value();

        var requestCounter = new AtomicInteger();

        ServerBootstrap bootstrap = bootstrapFactory.createServerBootstrap();

        bootstrap.childHandler(new ChannelInitializer<>() {
                    @Override
                    protected void initChannel(Channel ch) {
                        ch.pipeline().addLast(
                                new ClientMessageDecoder(),
                                new ConnectionDropHandler(requestCounter, shouldDropConnection),
                                new ResponseDelayHandler(responseDelay),
                                new ClientInboundMessageHandler(
                                        (IgniteTablesInternal) ignite.tables(),
                                        ignite.transactions(),
                                        mock(QueryProcessor.class),
                                        configuration,
                                        compute,
                                        clusterService,
                                        ignite.sql(),
                                        clusterId,
                                        metrics,
                                        authenticationManager(authenticationConfiguration)));
                    }
                })
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, configuration.connectTimeout());

        int port = configuration.port();
        Channel ch = null;
        ChannelFuture bindRes = bootstrap.bind(port).await();

        if (bindRes.isSuccess()) {
            ch = bindRes.channel();
        } else if (!(bindRes.cause() instanceof BindException)) {
            throw new IgniteException(bindRes.cause());
        }

        if (ch == null) {
            String msg = "Cannot start thin client connector endpoint. Port " + port + " is in use.";

            throw new IgniteException(msg);
        }

        return ch.closeFuture();
    }

    private static class ConnectionDropHandler extends ChannelInboundHandlerAdapter {
        /** Counter. */
        private final AtomicInteger cnt;

        /** Connection drop condition. */
        private final Function<Integer, Boolean> shouldDropConnection;

        /**
         * Constructor.
         *
         * @param cnt Request counter.
         * @param shouldDropConnection Connection drop condition.
         */
        private ConnectionDropHandler(AtomicInteger cnt, Function<Integer, Boolean> shouldDropConnection) {
            this.cnt = cnt;
            this.shouldDropConnection = shouldDropConnection;
        }

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
            if (shouldDropConnection.apply(cnt.incrementAndGet())) {
                ((ReferenceCounted) msg).release();

                ctx.close();
            } else {
                super.channelRead(ctx, msg);
            }
        }
    }

    private static class ResponseDelayHandler extends ChannelInboundHandlerAdapter {
        /** Delay. */
        private final Function<Integer, Integer> delay;

        /** Counter. */
        private final AtomicInteger cnt = new AtomicInteger();

        /**
         * Constructor.
         *
         * @param delay Delay.
         */
        private ResponseDelayHandler(@Nullable Function<Integer, Integer> delay) {
            this.delay = delay;
        }

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
            var delayMs = delay == null ? 0 : delay.apply(cnt.incrementAndGet());

            if (delayMs > 0) {
                Thread.sleep(delayMs);
            }

            super.channelRead(ctx, msg);
        }
    }

    private AuthenticationManager authenticationManager(AuthenticationConfiguration authenticationConfiguration) {
        AuthenticationManagerImpl manager = new AuthenticationManagerImpl();
        authenticationConfiguration.listen(manager);
        return manager;
    }
}
