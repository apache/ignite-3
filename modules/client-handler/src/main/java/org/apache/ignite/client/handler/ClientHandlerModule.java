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

package org.apache.ignite.client.handler;

import static java.util.concurrent.CompletableFuture.failedFuture;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.apache.ignite.lang.ErrorGroups.Common.INTERNAL_ERR;
import static org.apache.ignite.lang.ErrorGroups.Network.ADDRESS_UNRESOLVED_ERR;
import static org.apache.ignite.lang.ErrorGroups.Network.PORT_IN_USE_ERR;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.timeout.IdleStateHandler;
import java.net.BindException;
import java.net.InetSocketAddress;
import java.nio.channels.UnresolvedAddressException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import org.apache.ignite.client.handler.configuration.ClientConnectorConfiguration;
import org.apache.ignite.client.handler.configuration.ClientConnectorView;
import org.apache.ignite.internal.catalog.CatalogService;
import org.apache.ignite.internal.client.proto.ClientMessageDecoder;
import org.apache.ignite.internal.cluster.management.ClusterTag;
import org.apache.ignite.internal.compute.IgniteComputeInternal;
import org.apache.ignite.internal.hlc.ClockService;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.lowwatermark.LowWatermark;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.metrics.MetricManager;
import org.apache.ignite.internal.network.ClusterService;
import org.apache.ignite.internal.network.NettyBootstrapFactory;
import org.apache.ignite.internal.network.ssl.SslContextProvider;
import org.apache.ignite.internal.placementdriver.PlacementDriver;
import org.apache.ignite.internal.security.authentication.AuthenticationManager;
import org.apache.ignite.internal.sql.engine.QueryProcessor;
import org.apache.ignite.internal.table.IgniteTablesInternal;
import org.apache.ignite.internal.table.distributed.schema.SchemaSyncService;
import org.apache.ignite.internal.tx.impl.IgniteTransactionsImpl;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.lang.IgniteException;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

/**
 * Client handler module maintains TCP endpoint for thin client connections.
 */
public class ClientHandlerModule implements IgniteComponent {
    /** The logger. */
    private static final IgniteLogger LOG = Loggers.forClass(ClientHandlerModule.class);

    /** Connection id generator.
     * The resulting connection id is local to the current node and is intended for logging, diagnostics, and management purposes. */
    private static final AtomicLong CONNECTION_ID_GEN = new AtomicLong();

    /** Ignite tables API. */
    private final IgniteTablesInternal igniteTables;

    /** Ignite transactions API. */
    private final IgniteTransactionsImpl igniteTransactions;

    /** Cluster ID supplier. */
    private final Supplier<CompletableFuture<ClusterTag>> clusterTagSupplier;

    /** Metrics. */
    private final ClientHandlerMetricSource metrics;

    /** Metric manager. */
    private final MetricManager metricManager;

    /** Netty channel. */
    @Nullable
    private volatile Channel channel;

    /** Processor. */
    private final QueryProcessor queryProcessor;

    /** Compute. */
    private final IgniteComputeInternal igniteCompute;

    /** Cluster. */
    private final ClusterService clusterService;

    /** Netty bootstrap factory. */
    private final NettyBootstrapFactory bootstrapFactory;

    private final AuthenticationManager authenticationManager;

    private final ClockService clockService;

    private final SchemaSyncService schemaSyncService;

    private final CatalogService catalogService;

    private final ClientPrimaryReplicaTracker primaryReplicaTracker;

    private final IgniteSpinBusyLock busyLock = new IgniteSpinBusyLock();

    private final AtomicBoolean stopGuard = new AtomicBoolean();

    private final ClientConnectorConfiguration clientConnectorConfiguration;

    @TestOnly
    @SuppressWarnings("unused")
    private volatile ChannelHandler handler;

    /**
     * Constructor.
     *
     * @param queryProcessor Sql query processor.
     * @param igniteTables Ignite.
     * @param igniteTransactions Transactions.
     * @param igniteCompute Compute.
     * @param clusterService Cluster.
     * @param bootstrapFactory Bootstrap factory.
     * @param clusterTagSupplier ClusterTag supplier.
     * @param metricManager Metric manager.
     * @param authenticationManager Authentication manager.
     * @param clockService Clock service.
     * @param clientConnectorConfiguration Configuration of the connector.
     * @param lowWatermark Low watermark.
     */
    public ClientHandlerModule(
            QueryProcessor queryProcessor,
            IgniteTablesInternal igniteTables,
            IgniteTransactionsImpl igniteTransactions,
            IgniteComputeInternal igniteCompute,
            ClusterService clusterService,
            NettyBootstrapFactory bootstrapFactory,
            Supplier<CompletableFuture<ClusterTag>> clusterTagSupplier,
            MetricManager metricManager,
            ClientHandlerMetricSource metrics,
            AuthenticationManager authenticationManager,
            ClockService clockService,
            SchemaSyncService schemaSyncService,
            CatalogService catalogService,
            PlacementDriver placementDriver,
            ClientConnectorConfiguration clientConnectorConfiguration,
            LowWatermark lowWatermark
    ) {
        assert igniteTables != null;
        assert queryProcessor != null;
        assert igniteCompute != null;
        assert clusterService != null;
        assert bootstrapFactory != null;
        assert clusterTagSupplier != null;
        assert metricManager != null;
        assert metrics != null;
        assert authenticationManager != null;
        assert clockService != null;
        assert schemaSyncService != null;
        assert catalogService != null;
        assert placementDriver != null;
        assert clientConnectorConfiguration != null;
        assert lowWatermark != null;

        this.queryProcessor = queryProcessor;
        this.igniteTables = igniteTables;
        this.igniteTransactions = igniteTransactions;
        this.igniteCompute = igniteCompute;
        this.clusterService = clusterService;
        this.bootstrapFactory = bootstrapFactory;
        this.clusterTagSupplier = clusterTagSupplier;
        this.metricManager = metricManager;
        this.metrics = metrics;
        this.authenticationManager = authenticationManager;
        this.clockService = clockService;
        this.schemaSyncService = schemaSyncService;
        this.catalogService = catalogService;
        this.primaryReplicaTracker = new ClientPrimaryReplicaTracker(placementDriver, catalogService, clockService, schemaSyncService,
                lowWatermark);
        this.clientConnectorConfiguration = clientConnectorConfiguration;
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Void> startAsync() {
        if (channel != null) {
            throw new IgniteInternalException(INTERNAL_ERR, "ClientHandlerModule is already started.");
        }

        var configuration = clientConnectorConfiguration.value();

        metricManager.registerSource(metrics);
        if (configuration.metricsEnabled()) {
            metrics.enable();
        }

        primaryReplicaTracker.start();

        return startEndpoint(configuration).thenAccept(ch -> channel = ch);
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Void> stopAsync() {
        if (!stopGuard.compareAndSet(false, true)) {
            return nullCompletedFuture();
        }

        busyLock.block();

        metricManager.unregisterSource(metrics);
        primaryReplicaTracker.stop();

        var ch = channel;
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
     * @return the local address of this module.
     * @throws IgniteInternalException if the module is not started.
     */
    public InetSocketAddress localAddress() {
        var ch = channel;
        if (ch == null) {
            throw new IgniteInternalException(INTERNAL_ERR, "ClientHandlerModule has not been started");
        }

        return (InetSocketAddress) ch.localAddress();
    }

    /**
     * Starts the endpoint.
     *
     * @param configuration Configuration.
     * @return Channel future.
     */
    private CompletableFuture<Channel> startEndpoint(ClientConnectorView configuration) {
        ServerBootstrap bootstrap = bootstrapFactory.createServerBootstrap();
        CompletableFuture<ClusterTag> clusterTag = clusterTagSupplier.get();
        CompletableFuture<Channel> result = new CompletableFuture<>();

        // Initialize SslContext once on startup to avoid initialization on each connection, and to fail in case of incorrect config.
        SslContext sslContext = configuration.ssl().enabled() ? SslContextProvider.createServerSslContext(configuration.ssl()) : null;

        bootstrap.childHandler(new ChannelInitializer<>() {
                    @Override
                    protected void initChannel(Channel ch) {
                        if (!busyLock.enterBusy()) {
                            ch.close();
                            return;
                        }

                        try {
                            long connectionId = CONNECTION_ID_GEN.incrementAndGet();

                            if (LOG.isDebugEnabled()) {
                                LOG.debug("New client connection [connectionId=" + connectionId
                                        + ", remoteAddress=" + ch.remoteAddress() + ']');
                            }

                            if (configuration.idleTimeout() > 0) {
                                IdleStateHandler idleStateHandler = new IdleStateHandler(
                                        configuration.idleTimeout(), 0, 0, TimeUnit.MILLISECONDS);

                                ch.pipeline().addLast(idleStateHandler);
                                ch.pipeline().addLast(new IdleChannelHandler(configuration.idleTimeout(), metrics, connectionId));
                            }

                            if (sslContext != null) {
                                ch.pipeline().addFirst("ssl", sslContext.newHandler(ch.alloc()));
                            }

                            ClientInboundMessageHandler messageHandler = createInboundMessageHandler(
                                    configuration, clusterTag, connectionId);

                            //noinspection TestOnlyProblems
                            handler = messageHandler;

                            ch.pipeline().addLast(
                                    new ClientMessageDecoder(),
                                    messageHandler
                            );

                            metrics.connectionsInitiatedIncrement();
                        } finally {
                            busyLock.leaveBusy();
                        }
                    }
                })
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, configuration.connectTimeout());

        int port = configuration.port();
        String address = configuration.listenAddress();

        ChannelFuture channelFuture = address.isEmpty() ? bootstrap.bind(port) : bootstrap.bind(address, port);

        channelFuture.addListener((ChannelFutureListener) bindFut -> {
            if (bindFut.isSuccess()) {
                if (LOG.isInfoEnabled()) {
                    LOG.info(
                            "Thin client connector endpoint started successfully [{}]",
                            (address.isEmpty() ? "" : "address=" + address + ",") + "port=" + port);
                }

                result.complete(bindFut.channel());
            } else if (bindFut.cause() instanceof BindException) {
                // TODO IGNITE-21614
                result.completeExceptionally(
                        new IgniteException(
                                PORT_IN_USE_ERR,
                                "Cannot start thin client connector endpoint. Port " + port + " is in use.",
                                bindFut.cause())
                );
            } else if (bindFut.cause() instanceof UnresolvedAddressException) {
                result.completeExceptionally(
                        new IgniteException(
                                ADDRESS_UNRESOLVED_ERR,
                                "Failed to start thin connector endpoint, unresolved socket address \"" + address + "\"",
                                bindFut.cause()
                        )
                );
            } else {
                result.completeExceptionally(
                        new IgniteException(
                                INTERNAL_ERR,
                                "Failed to start thin client connector endpoint: " + bindFut.cause().getMessage(),
                                bindFut.cause()));
            }
        });

        return result;
    }

    private ClientInboundMessageHandler createInboundMessageHandler(
            ClientConnectorView configuration,
            CompletableFuture<ClusterTag> clusterTag,
            long connectionId) {
        return new ClientInboundMessageHandler(
                igniteTables,
                igniteTransactions,
                queryProcessor,
                configuration,
                igniteCompute,
                clusterService,
                clusterTag,
                metrics,
                authenticationManager,
                clockService,
                schemaSyncService,
                catalogService,
                connectionId,
                primaryReplicaTracker
        );
    }
}
