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
import static org.apache.ignite.lang.ErrorGroups.Network.BIND_ERR;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.handler.flush.FlushConsolidationHandler;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.timeout.IdleStateHandler;
import java.net.BindException;
import java.net.InetSocketAddress;
import java.nio.channels.UnresolvedAddressException;
import java.util.BitSet;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import org.apache.ignite.client.handler.configuration.ClientConnectorConfiguration;
import org.apache.ignite.client.handler.configuration.ClientConnectorView;
import org.apache.ignite.internal.catalog.CatalogService;
import org.apache.ignite.internal.client.proto.ClientMessageDecoder;
import org.apache.ignite.internal.client.proto.ProtocolBitmaskFeature;
import org.apache.ignite.internal.compute.IgniteComputeInternal;
import org.apache.ignite.internal.compute.executor.platform.PlatformComputeConnection;
import org.apache.ignite.internal.compute.executor.platform.PlatformComputeTransport;
import org.apache.ignite.internal.hlc.ClockService;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.lowwatermark.LowWatermark;
import org.apache.ignite.internal.manager.ComponentContext;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.metrics.MetricManager;
import org.apache.ignite.internal.network.ClusterService;
import org.apache.ignite.internal.network.NettyBootstrapFactory;
import org.apache.ignite.internal.network.handshake.HandshakeEventLoopSwitcher;
import org.apache.ignite.internal.network.ssl.SslContextProvider;
import org.apache.ignite.internal.placementdriver.PlacementDriver;
import org.apache.ignite.internal.schema.SchemaSyncService;
import org.apache.ignite.internal.security.authentication.AuthenticationManager;
import org.apache.ignite.internal.sql.engine.QueryProcessor;
import org.apache.ignite.internal.table.IgniteTablesInternal;
import org.apache.ignite.internal.tx.TxManager;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.lang.IgniteException;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

/**
 * Client handler module maintains TCP endpoint for thin client connections.
 */
public class ClientHandlerModule implements IgniteComponent, PlatformComputeTransport {
    /** The logger. */
    private static final IgniteLogger LOG = Loggers.forClass(ClientHandlerModule.class);

    /** Supported server features. */
    private static final BitSet SUPPORTED_FEATURES = ProtocolBitmaskFeature.featuresAsBitSet(EnumSet.of(
            ProtocolBitmaskFeature.TABLE_GET_REQS_USE_QUALIFIED_NAME,
            ProtocolBitmaskFeature.TX_DIRECT_MAPPING,
            ProtocolBitmaskFeature.PLATFORM_COMPUTE_JOB,
            ProtocolBitmaskFeature.COMPUTE_TASK_ID,
            ProtocolBitmaskFeature.STREAMER_RECEIVER_EXECUTION_OPTIONS,
            ProtocolBitmaskFeature.TX_DELAYED_ACKS,
            ProtocolBitmaskFeature.TX_PIGGYBACK,
            ProtocolBitmaskFeature.TX_ALLOW_NOOP_ENLIST,
            ProtocolBitmaskFeature.SQL_PARTITION_AWARENESS,
            ProtocolBitmaskFeature.SQL_DIRECT_TX_MAPPING,
            ProtocolBitmaskFeature.TX_CLIENT_GETALL_SUPPORTS_TX_OPTIONS,
            ProtocolBitmaskFeature.SQL_MULTISTATEMENT_SUPPORT,
            ProtocolBitmaskFeature.COMPUTE_OBSERVABLE_TS,
            ProtocolBitmaskFeature.TX_DIRECT_MAPPING_SEND_REMOTE_WRITES,
            ProtocolBitmaskFeature.SQL_PARTITION_AWARENESS_TABLE_NAME,
            ProtocolBitmaskFeature.TX_DIRECT_MAPPING_SEND_DISCARD
    ));

    /** Connection id generator.
     * The resulting connection id is local to the current node and is intended for logging, diagnostics, and management purposes. */
    private static final AtomicLong CONNECTION_ID_GEN = new AtomicLong();

    /** Ignite tables API. */
    private final IgniteTablesInternal igniteTables;

    /** Transaction manager. */
    private final TxManager txManager;

    /** Cluster ID supplier. */
    private final Supplier<ClusterInfo> clusterInfoSupplier;

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

    private final Supplier<Boolean> ddlBatchingSuggestionEnabled;

    private final Executor partitionOperationsExecutor;

    private final ConcurrentHashMap<String, CompletableFuture<PlatformComputeConnection>> computeExecutors = new ConcurrentHashMap<>();

    @TestOnly
    @SuppressWarnings("unused")
    private volatile ClientInboundMessageHandler handler;

    /**
     * Constructor.
     *
     * @param queryProcessor Sql query processor.
     * @param igniteTables Ignite.
     * @param txManager Transaction manager.
     * @param igniteCompute Compute.
     * @param clusterService Cluster.
     * @param bootstrapFactory Bootstrap factory.
     * @param clusterInfoSupplier ClusterInfo supplier.
     * @param metricManager Metric manager.
     * @param authenticationManager Authentication manager.
     * @param clockService Clock service.
     * @param clientConnectorConfiguration Configuration of the connector.
     * @param lowWatermark Low watermark.
     * @param partitionOperationsExecutor Executor for a partition operation.
     * @param ddlBatchingSuggestionEnabled Boolean supplier indicates whether the suggestion related DDL batching is enabled.
     */
    public ClientHandlerModule(
            QueryProcessor queryProcessor,
            IgniteTablesInternal igniteTables,
            TxManager txManager,
            IgniteComputeInternal igniteCompute,
            ClusterService clusterService,
            NettyBootstrapFactory bootstrapFactory,
            Supplier<ClusterInfo> clusterInfoSupplier,
            MetricManager metricManager,
            ClientHandlerMetricSource metrics,
            AuthenticationManager authenticationManager,
            ClockService clockService,
            SchemaSyncService schemaSyncService,
            CatalogService catalogService,
            PlacementDriver placementDriver,
            ClientConnectorConfiguration clientConnectorConfiguration,
            LowWatermark lowWatermark,
            Executor partitionOperationsExecutor,
            Supplier<Boolean> ddlBatchingSuggestionEnabled
    ) {
        assert igniteTables != null;
        assert queryProcessor != null;
        assert txManager != null;
        assert igniteCompute != null;
        assert clusterService != null;
        assert bootstrapFactory != null;
        assert clusterInfoSupplier != null;
        assert metricManager != null;
        assert metrics != null;
        assert authenticationManager != null;
        assert clockService != null;
        assert schemaSyncService != null;
        assert catalogService != null;
        assert placementDriver != null;
        assert clientConnectorConfiguration != null;
        assert ddlBatchingSuggestionEnabled != null;
        assert lowWatermark != null;
        assert partitionOperationsExecutor != null;

        this.queryProcessor = queryProcessor;
        this.igniteTables = igniteTables;
        this.txManager = txManager;
        this.igniteCompute = igniteCompute;
        this.clusterService = clusterService;
        this.bootstrapFactory = bootstrapFactory;
        this.clusterInfoSupplier = clusterInfoSupplier;
        this.metricManager = metricManager;
        this.metrics = metrics;
        this.authenticationManager = authenticationManager;
        this.clockService = clockService;
        this.schemaSyncService = schemaSyncService;
        this.catalogService = catalogService;
        this.primaryReplicaTracker = new ClientPrimaryReplicaTracker(
                placementDriver,
                catalogService,
                clockService,
                schemaSyncService,
                lowWatermark);
        this.clientConnectorConfiguration = clientConnectorConfiguration;
        this.ddlBatchingSuggestionEnabled = ddlBatchingSuggestionEnabled;
        this.partitionOperationsExecutor = partitionOperationsExecutor;
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Void> startAsync(ComponentContext componentContext) {
        if (channel != null) {
            throw new IgniteInternalException(INTERNAL_ERR, "ClientHandlerModule is already started.");
        }

        var configuration = clientConnectorConfiguration.value();

        metricManager.registerSource(metrics);
        if (configuration.metricsEnabled()) {
            metricManager.enable(metrics);
        }

        primaryReplicaTracker.start();

        return startEndpoint(configuration).thenAccept(ch -> channel = ch);
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Void> stopAsync(ComponentContext componentContext) {
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

    /** Enables request handling. */
    public void enable() {
        Channel ch = channel;
        if (ch == null) {
            throw new IgniteInternalException(INTERNAL_ERR, "ClientHandlerModule has not been started");
        }

        ch.config().setAutoRead(true);
    }

    /**
     * Starts the endpoint.
     *
     * @param configuration Configuration.
     * @return Channel future.
     */
    private CompletableFuture<Channel> startEndpoint(ClientConnectorView configuration) {
        ServerBootstrap bootstrap = bootstrapFactory.createServerBootstrap();
        CompletableFuture<Channel> result = new CompletableFuture<>();

        // Initialize SslContext once on startup to avoid initialization on each connection, and to fail in case of incorrect config.
        SslContext sslContext = configuration.ssl().enabled() ? SslContextProvider.createServerSslContext(configuration.ssl()) : null;

        bootstrap.childHandler(new ChannelInitializer<>() {
                    @Override
                    protected void initChannel(Channel ch) {
                        if (!busyLock.enterBusy()) {
                            if (LOG.isDebugEnabled()) {
                                LOG.debug("Client handler stopped, dropping client connection [remoteAddress=" + ch.remoteAddress() + ']');
                            }

                            ch.close();
                            return;
                        }

                        long connectionId = CONNECTION_ID_GEN.incrementAndGet();

                        try {
                            if (LOG.isDebugEnabled()) {
                                LOG.debug("New client connection [connectionId=" + connectionId
                                        + ", remoteAddress=" + ch.remoteAddress() + ']');
                            }

                            if (configuration.idleTimeoutMillis() > 0) {
                                IdleStateHandler idleStateHandler = new IdleStateHandler(
                                        configuration.idleTimeoutMillis(), 0, 0, TimeUnit.MILLISECONDS);

                                ch.pipeline().addLast(idleStateHandler);
                                ch.pipeline().addLast(new IdleChannelHandler(configuration.idleTimeoutMillis(), metrics));
                            }

                            if (sslContext != null) {
                                ch.pipeline().addFirst("ssl", sslContext.newHandler(ch.alloc()));
                            }

                            ClientInboundMessageHandler messageHandler = createInboundMessageHandler(
                                    bootstrapFactory.handshakeEventLoopSwitcher(),
                                    configuration,
                                    connectionId
                            );

                            //noinspection TestOnlyProblems
                            handler = messageHandler;

                            ch.pipeline().addLast(
                                    new FlushConsolidationHandler(FlushConsolidationHandler.DEFAULT_EXPLICIT_FLUSH_AFTER_FLUSHES, true),
                                    new ClientMessageDecoder(),
                                    messageHandler
                            );

                            metrics.connectionsInitiatedIncrement();
                        } catch (Throwable t) {
                            LOG.error("Failed to initialize client connection [connectionId=" + connectionId
                                    + ", remoteAddress=" + ch.remoteAddress() + "]:" + t.getMessage(), t);

                            ch.close();
                        } finally {
                            busyLock.leaveBusy();
                        }
                    }
                })
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, configuration.connectTimeoutMillis())
                .option(ChannelOption.AUTO_READ, false);

        int port = configuration.port();
        String[] addresses = configuration.listenAddresses();

        ChannelFuture channelFuture;
        if (addresses.length == 0) {
            channelFuture = bootstrap.bind(port);
        } else {
            if (addresses.length > 1) {
                // TODO: IGNITE-22369 - support more than one listen address.
                throw new IgniteException(INTERNAL_ERR, "Only one listen address is allowed for now, but got " + List.of(addresses));
            }

            channelFuture = bootstrap.bind(addresses[0], port);
        }

        channelFuture.addListener((ChannelFutureListener) bindFut -> {
            if (bindFut.isSuccess()) {
                if (LOG.isInfoEnabled()) {
                    LOG.info(
                            "Thin client connector endpoint started successfully [{}]",
                            (addresses.length == 0 ? "" : "address=" + addresses[0] + ",") + "port=" + port);
                }

                result.complete(bindFut.channel());
            } else if (bindFut.cause() instanceof BindException) {
                String address = addresses.length == 0 ? "" : addresses[0];
                result.completeExceptionally(
                        new IgniteException(
                                BIND_ERR,
                                "Cannot start thin client connector endpoint at address=" + address + ", port=" + port,
                                bindFut.cause())
                );
            } else if (bindFut.cause() instanceof UnresolvedAddressException) {
                result.completeExceptionally(
                        new IgniteException(
                                ADDRESS_UNRESOLVED_ERR,
                                "Failed to start thin connector endpoint, unresolved socket address \"" + addresses[0] + "\"",
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
            HandshakeEventLoopSwitcher handshakeEventLoopSwitcher,
            ClientConnectorView configuration,
            long connectionId
    ) {
        return new ClientInboundMessageHandler(
                igniteTables,
                txManager,
                queryProcessor,
                configuration,
                igniteCompute,
                clusterService,
                clusterInfoSupplier,
                metrics,
                authenticationManager,
                clockService,
                schemaSyncService,
                catalogService,
                connectionId,
                primaryReplicaTracker,
                partitionOperationsExecutor,
                SUPPORTED_FEATURES,
                Map.of(),
                computeExecutors::remove,
                handshakeEventLoopSwitcher,
                ddlBatchingSuggestionEnabled.get()
                        ? new DdlBatchingSuggester()
                        : ignore -> {}
        );
    }

    @TestOnly
    public ClientInboundMessageHandler handler() {
        return handler;
    }

    @Override
    public String serverAddress() {
        return "127.0.0.1:" + localAddress().getPort();
    }

    @Override
    public boolean sslEnabled() {
        return clientConnectorConfiguration.value().ssl().enabled();
    }

    @Override
    public CompletableFuture<PlatformComputeConnection> registerComputeExecutorId(String computeExecutorId) {
        return computeExecutors.computeIfAbsent(computeExecutorId, k -> new CompletableFuture<>());
    }
}
