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

import static java.util.concurrent.CompletableFuture.failedFuture;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.apache.ignite.lang.ErrorGroups.Common.INTERNAL_ERR;

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
import java.util.BitSet;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import org.apache.ignite.Ignite;
import org.apache.ignite.client.fakes.FakeIgnite;
import org.apache.ignite.client.fakes.FakeIgniteQueryProcessor;
import org.apache.ignite.client.fakes.FakeInternalTable;
import org.apache.ignite.client.handler.ClientHandlerMetricSource;
import org.apache.ignite.client.handler.ClientInboundMessageHandler;
import org.apache.ignite.client.handler.ClientPrimaryReplicaTracker;
import org.apache.ignite.client.handler.ClusterInfo;
import org.apache.ignite.client.handler.FakeCatalogService;
import org.apache.ignite.client.handler.configuration.ClientConnectorConfiguration;
import org.apache.ignite.internal.catalog.CatalogService;
import org.apache.ignite.internal.client.proto.ClientMessageDecoder;
import org.apache.ignite.internal.client.proto.HandshakeExtension;
import org.apache.ignite.internal.client.proto.ProtocolBitmaskFeature;
import org.apache.ignite.internal.components.SystemPropertiesNodeProperties;
import org.apache.ignite.internal.compute.IgniteComputeInternal;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.TestClockService;
import org.apache.ignite.internal.lowwatermark.TestLowWatermark;
import org.apache.ignite.internal.manager.ComponentContext;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.network.ClusterService;
import org.apache.ignite.internal.network.NettyBootstrapFactory;
import org.apache.ignite.internal.placementdriver.PlacementDriver;
import org.apache.ignite.internal.schema.AlwaysSyncedSchemaSyncService;
import org.apache.ignite.internal.security.authentication.AuthenticationManager;
import org.apache.ignite.internal.table.IgniteTablesInternal;
import org.apache.ignite.lang.IgniteException;
import org.jetbrains.annotations.Nullable;

/**
 * Client handler module for tests.
 */
public class TestClientHandlerModule implements IgniteComponent {
    /** Ignite. */
    private final Ignite ignite;

    /** Connection drop condition. */
    private final Function<Integer, Boolean> shouldDropConnection;

    /** Server response delay function. */
    private final Function<Integer, Integer> responseDelay;

    /** Cluster service. */
    private final ClusterService clusterService;

    /** Compute. */
    private final IgniteComputeInternal compute;

    /** Cluster id. */
    private final ClusterInfo clusterInfo;

    /** Metrics. */
    private final ClientHandlerMetricSource metrics;

    /** Clock. */
    private final HybridClock clock;

    /** Placement driver. */
    private final PlacementDriver placementDriver;

    /** Netty channel. */
    private volatile @Nullable Channel channel;

    /** Netty bootstrap factory. */
    private final NettyBootstrapFactory bootstrapFactory;

    /** Security configuration. */
    private final AuthenticationManager authenticationManager;

    /** Configuration of the client connector. */
    private final ClientConnectorConfiguration clientConnectorConfiguration;

    /** Features set. */
    private final BitSet features;

    /**
     * Constructor.
     *
     * @param ignite Ignite.
     * @param bootstrapFactory Bootstrap factory.
     * @param shouldDropConnection Connection drop condition.
     * @param responseDelay Response delay, in milliseconds.
     * @param clusterService Cluster service.
     * @param clusterInfo Cluster info.
     * @param metrics Metrics.
     * @param authenticationManager Authentication manager.
     * @param clock Clock.
     * @param placementDriver Placement driver.
     * @param clientConnectorConfiguration Configuration of the client connector.
     * @param features Features.
     */
    public TestClientHandlerModule(
            Ignite ignite,
            NettyBootstrapFactory bootstrapFactory,
            Function<Integer, Boolean> shouldDropConnection,
            @Nullable Function<Integer, Integer> responseDelay,
            ClusterService clusterService,
            ClusterInfo clusterInfo,
            ClientHandlerMetricSource metrics,
            AuthenticationManager authenticationManager,
            HybridClock clock,
            PlacementDriver placementDriver,
            ClientConnectorConfiguration clientConnectorConfiguration,
            @Nullable BitSet features
    ) {
        assert ignite != null;
        assert bootstrapFactory != null;

        this.ignite = ignite;
        this.bootstrapFactory = bootstrapFactory;
        this.shouldDropConnection = shouldDropConnection;
        this.responseDelay = responseDelay;
        this.clusterService = clusterService;
        this.compute = (IgniteComputeInternal) ignite.compute();
        this.clusterInfo = clusterInfo;
        this.metrics = metrics;
        this.authenticationManager = authenticationManager;
        this.clock = clock;
        this.placementDriver = placementDriver;
        this.clientConnectorConfiguration = clientConnectorConfiguration;
        this.features = features;
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Void> startAsync(ComponentContext componentContext) {
        if (channel != null) {
            throw new IgniteException(INTERNAL_ERR, "ClientHandlerModule is already started.");
        }

        try {
            channel = startEndpoint().channel();
        } catch (InterruptedException e) {
            throw new IgniteException(INTERNAL_ERR, e);
        }

        return nullCompletedFuture();
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Void> stopAsync(ComponentContext componentContext) {
        Channel ch = channel;

        if (ch != null) {
            try {
                ch.close().await();
            } catch (InterruptedException e) {
                return failedFuture(e);
            }

            channel = null;
        }

        return nullCompletedFuture();
    }

    /**
     * Returns the local address where this handler is bound to.
     *
     * @return the local address of this module, or {@code null} if this module is not started.
     */
    @Nullable
    SocketAddress localAddress() {
        Channel ch = channel;

        return ch == null ? null : ch.localAddress();
    }

    /**
     * Starts the endpoint.
     *
     * @return Channel future.
     * @throws InterruptedException If thread has been interrupted during the start.
     * @throws IgniteException      When startup has failed.
     */
    private ChannelFuture startEndpoint() throws InterruptedException {
        var configuration = clientConnectorConfiguration.value();

        var requestCounter = new AtomicInteger();
        var connectionIdGen = new AtomicLong();

        ServerBootstrap bootstrap = bootstrapFactory.createServerBootstrap();

        BitSet features;

        if (this.features == null) {
            features = BitSet.valueOf(new long[]{ThreadLocalRandom.current().nextLong()});
            features.set(ProtocolBitmaskFeature.TX_DIRECT_MAPPING.featureId());
            features.set(ProtocolBitmaskFeature.PLATFORM_COMPUTE_JOB.featureId());
            features.set(ProtocolBitmaskFeature.TX_DELAYED_ACKS.featureId());
            features.set(ProtocolBitmaskFeature.TX_PIGGYBACK.featureId());
            features.set(ProtocolBitmaskFeature.TX_ALLOW_NOOP_ENLIST.featureId());
            features.set(ProtocolBitmaskFeature.TABLE_GET_REQS_USE_QUALIFIED_NAME.featureId());
            features.set(ProtocolBitmaskFeature.TX_CLIENT_GETALL_SUPPORTS_TX_OPTIONS.featureId());
        } else {
            features = new BitSet(ProtocolBitmaskFeature.values().length);
            for (int i = this.features.nextSetBit(0); i != -1; i = this.features.nextSetBit(i + 1)) {
                features.set(i);
            }
        }

        bootstrap.childHandler(new ChannelInitializer<>() {
                    @Override
                    protected void initChannel(Channel ch) {
                        CatalogService catalogService = new FakeCatalogService(FakeInternalTable.PARTITIONS);
                        TestClockService clockService = new TestClockService(clock);
                        ch.pipeline().addLast(
                                new ClientMessageDecoder(),
                                new ConnectionDropHandler(requestCounter, shouldDropConnection),
                                new ResponseDelayHandler(responseDelay),
                                new ClientInboundMessageHandler(
                                        (IgniteTablesInternal) ignite.tables(),
                                        ((FakeIgnite) ignite).txManager(),
                                        new FakeIgniteQueryProcessor(ignite.name()),
                                        configuration,
                                        compute,
                                        clusterService,
                                        () -> clusterInfo,
                                        metrics,
                                        authenticationManager,
                                        clockService,
                                        new AlwaysSyncedSchemaSyncService(),
                                        catalogService,
                                        connectionIdGen.incrementAndGet(),
                                        new ClientPrimaryReplicaTracker(
                                                placementDriver,
                                                catalogService,
                                                clockService,
                                                new AlwaysSyncedSchemaSyncService(),
                                                new TestLowWatermark(),
                                                new SystemPropertiesNodeProperties()
                                        ),
                                        Runnable::run,
                                        features,
                                        randomExtensions(),
                                        unused -> null,
                                        bootstrapFactory.handshakeEventLoopSwitcher(),
                                        () -> false
                                )
                        );
                    }
                })
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, configuration.connectTimeoutMillis());

        int port = configuration.port();
        Channel ch = null;
        ChannelFuture bindRes = bootstrap.bind(port).await();

        if (bindRes.isSuccess()) {
            ch = bindRes.channel();
        } else if (!(bindRes.cause() instanceof BindException)) {
            throw new IgniteException(INTERNAL_ERR, bindRes.cause());
        }

        if (ch == null) {
            String address = configuration.listenAddresses().length == 0 ? "" :  configuration.listenAddresses()[0];
            String msg = "Cannot start thin client connector endpoint at address=" + address + ", port=" + port;

            throw new IgniteException(INTERNAL_ERR, msg);
        }

        return ch.closeFuture();
    }

    private static Map<HandshakeExtension, Object> randomExtensions() {
        if (ThreadLocalRandom.current().nextBoolean()) {
            return Map.of();
        }

        return Map.of(HandshakeExtension.AUTHENTICATION_SECRET, String.valueOf(ThreadLocalRandom.current().nextLong()));
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
            var delayMs = delay == null
                    ? 0
                    : delay.apply(cnt.incrementAndGet());

            if (delayMs > 0) {
                try {
                    Thread.sleep(delayMs);
                } catch (InterruptedException e) {
                    if (msg instanceof ReferenceCounted) {
                        ((ReferenceCounted) msg).release();
                    }

                    Thread.currentThread().interrupt();
                    throw e;
                }
            }

            super.channelRead(ctx, msg);
        }
    }
}
