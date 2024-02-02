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

import static org.apache.ignite.lang.ErrorGroups.Common.INTERNAL_ERR;
import static org.apache.ignite.lang.ErrorGroups.Network.PORT_IN_USE_ERR;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.timeout.IdleStateHandler;
import java.net.BindException;
import java.net.InetSocketAddress;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import org.apache.ignite.client.handler.configuration.ClientConnectorConfiguration;
import org.apache.ignite.client.handler.configuration.ClientConnectorView;
import org.apache.ignite.internal.catalog.CatalogService;
import org.apache.ignite.internal.client.proto.ClientMessageDecoder;
import org.apache.ignite.internal.compute.IgniteComputeInternal;
import org.apache.ignite.internal.configuration.ConfigurationRegistry;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
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
import org.apache.ignite.sql.IgniteSql;
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

    /** Configuration registry. */
    private final ConfigurationRegistry registry;

    /** Ignite tables API. */
    private final IgniteTablesInternal igniteTables;

    /** Ignite transactions API. */
    private final IgniteTransactionsImpl igniteTransactions;

    /** Ignite SQL API. */
    private final IgniteSql sql;

    /** Cluster ID supplier. */
    private final Supplier<CompletableFuture<UUID>> clusterIdSupplier;

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

    private final HybridClock clock;

    private final SchemaSyncService schemaSyncService;

    private final CatalogService catalogService;

    private final ClientPrimaryReplicaTracker primaryReplicaTracker;

    private final IgniteSpinBusyLock busyLock = new IgniteSpinBusyLock();

    private final AtomicBoolean stopGuard = new AtomicBoolean();

    @TestOnly
    @SuppressWarnings("unused")
    private volatile ChannelHandler handler;

    /**
     * Constructor.
     *
     * @param queryProcessor Sql query processor.
     * @param igniteTables Ignite.
     * @param igniteTransactions Transactions.
     * @param registry Configuration registry.
     * @param igniteCompute Compute.
     * @param clusterService Cluster.
     * @param bootstrapFactory Bootstrap factory.
     * @param sql SQL.
     * @param clusterIdSupplier ClusterId supplier.
     * @param metricManager Metric manager.
     * @param authenticationManager Authentication manager.
     * @param clock Hybrid clock.
     */
    public ClientHandlerModule(
            QueryProcessor queryProcessor,
            IgniteTablesInternal igniteTables,
            IgniteTransactionsImpl igniteTransactions,
            ConfigurationRegistry registry,
            IgniteComputeInternal igniteCompute,
            ClusterService clusterService,
            NettyBootstrapFactory bootstrapFactory,
            IgniteSql sql,
            Supplier<CompletableFuture<UUID>> clusterIdSupplier,
            MetricManager metricManager,
            ClientHandlerMetricSource metrics,
            AuthenticationManager authenticationManager,
            HybridClock clock,
            SchemaSyncService schemaSyncService,
            CatalogService catalogService,
            PlacementDriver placementDriver
    ) {
        assert igniteTables != null;
        assert registry != null;
        assert queryProcessor != null;
        assert igniteCompute != null;
        assert clusterService != null;
        assert bootstrapFactory != null;
        assert sql != null;
        assert clusterIdSupplier != null;
        assert metricManager != null;
        assert metrics != null;
        assert authenticationManager != null;
        assert clock != null;
        assert schemaSyncService != null;
        assert catalogService != null;
        assert placementDriver != null;

        this.queryProcessor = queryProcessor;
        this.igniteTables = igniteTables;
        this.igniteTransactions = igniteTransactions;
        this.igniteCompute = igniteCompute;
        this.clusterService = clusterService;
        this.registry = registry;
        this.bootstrapFactory = bootstrapFactory;
        this.sql = sql;
        this.clusterIdSupplier = clusterIdSupplier;
        this.metricManager = metricManager;
        this.metrics = metrics;
        this.authenticationManager = authenticationManager;
        this.clock = clock;
        this.schemaSyncService = schemaSyncService;
        this.catalogService = catalogService;
        this.primaryReplicaTracker = new ClientPrimaryReplicaTracker(placementDriver, catalogService, clock, schemaSyncService);
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Void> start() {
        if (channel != null) {
            throw new IgniteInternalException(INTERNAL_ERR, "ClientHandlerModule is already started.");
        }

        var configuration = registry.getConfiguration(ClientConnectorConfiguration.KEY).value();

        metricManager.registerSource(metrics);
        if (configuration.metricsEnabled()) {
            metrics.enable();
        }

        primaryReplicaTracker.start();

        return startEndpoint(configuration).thenAccept(ch -> channel = ch);
    }

    /** {@inheritDoc} */
    @Override
    public void stop() throws Exception {
        if (!stopGuard.compareAndSet(false, true)) {
            return;
        }

        busyLock.block();

        metricManager.unregisterSource(metrics);
        primaryReplicaTracker.stop();

        var ch = channel;
        if (ch != null) {
            ch.close().await();

            channel = null;
        }
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
        CompletableFuture<UUID> clusterId = clusterIdSupplier.get();
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
                                    configuration, clusterId, connectionId);

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

        bootstrap.bind(port).addListener((ChannelFutureListener) bindFut -> {
            if (bindFut.isSuccess()) {
                if (LOG.isInfoEnabled()) {
                    LOG.info("Thin client connector endpoint started successfully [port={}]", port);
                }

                result.complete(bindFut.channel());
                return;
            }

            if (bindFut.cause() instanceof BindException) {
                result.completeExceptionally(
                        new IgniteException(
                                PORT_IN_USE_ERR,
                                "Cannot start thin client connector endpoint. Port " + port + " is in use.",
                                bindFut.cause())
                );
                return;
            }

            result.completeExceptionally(
                    new IgniteException(
                            INTERNAL_ERR,
                            "Failed to start thin client connector endpoint: " + bindFut.cause().getMessage(),
                            bindFut.cause()));
        });

        return result;
    }

    private ClientInboundMessageHandler createInboundMessageHandler(
            ClientConnectorView configuration,
            CompletableFuture<UUID> clusterId,
            long connectionId) {
        return new ClientInboundMessageHandler(
                igniteTables,
                igniteTransactions,
                queryProcessor,
                configuration,
                igniteCompute,
                clusterService,
                sql,
                clusterId,
                metrics,
                authenticationManager,
                clock,
                schemaSyncService,
                catalogService,
                connectionId,
                primaryReplicaTracker
        );
    }
}
