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

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.timeout.IdleStateHandler;
import java.net.BindException;
import java.net.InetSocketAddress;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.apache.ignite.client.handler.configuration.ClientConnectorConfiguration;
import org.apache.ignite.client.handler.configuration.ClientConnectorView;
import org.apache.ignite.compute.IgniteCompute;
import org.apache.ignite.internal.client.proto.ClientMessageDecoder;
import org.apache.ignite.internal.configuration.AuthenticationConfiguration;
import org.apache.ignite.internal.configuration.ConfigurationRegistry;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.metrics.MetricManager;
import org.apache.ignite.internal.network.ssl.SslContextProvider;
import org.apache.ignite.internal.security.authentication.AuthenticationManager;
import org.apache.ignite.internal.sql.engine.QueryProcessor;
import org.apache.ignite.internal.table.IgniteTablesInternal;
import org.apache.ignite.lang.ErrorGroups;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.lang.IgniteInternalException;
import org.apache.ignite.network.ClusterService;
import org.apache.ignite.network.NettyBootstrapFactory;
import org.apache.ignite.sql.IgniteSql;
import org.apache.ignite.tx.IgniteTransactions;

/**
 * Client handler module maintains TCP endpoint for thin client connections.
 */
public class ClientHandlerModule implements IgniteComponent {
    /** The logger. */
    private static final IgniteLogger LOG = Loggers.forClass(ClientHandlerModule.class);

    /** Configuration registry. */
    private final ConfigurationRegistry registry;

    /** Ignite tables API. */
    private final IgniteTablesInternal igniteTables;

    /** Ignite transactions API. */
    private final IgniteTransactions igniteTransactions;

    /** Ignite SQL API. */
    private final IgniteSql sql;

    /** Cluster ID supplier. */
    private final Supplier<CompletableFuture<UUID>> clusterIdSupplier;

    /** Metrics. */
    private final ClientHandlerMetricSource metrics;

    /** Metric manager. */
    private final MetricManager metricManager;

    /** Cluster ID. */
    private UUID clusterId;

    /** Netty channel. */
    private volatile Channel channel;

    /** Processor. */
    private final QueryProcessor queryProcessor;

    /** Compute. */
    private final IgniteCompute igniteCompute;

    /** Cluster. */
    private final ClusterService clusterService;

    /** Netty bootstrap factory. */
    private final NettyBootstrapFactory bootstrapFactory;

    private final AuthenticationManager authenticationManager;

    private final AuthenticationConfiguration authenticationConfiguration;

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
     * @param authenticationConfiguration Authentication configuration.
     */
    public ClientHandlerModule(
            QueryProcessor queryProcessor,
            IgniteTablesInternal igniteTables,
            IgniteTransactions igniteTransactions,
            ConfigurationRegistry registry,
            IgniteCompute igniteCompute,
            ClusterService clusterService,
            NettyBootstrapFactory bootstrapFactory,
            IgniteSql sql,
            Supplier<CompletableFuture<UUID>> clusterIdSupplier,
            MetricManager metricManager,
            ClientHandlerMetricSource metrics,
            AuthenticationManager authenticationManager,
            AuthenticationConfiguration authenticationConfiguration) {
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
        assert authenticationConfiguration != null;

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
        this.authenticationConfiguration = authenticationConfiguration;
    }

    /** {@inheritDoc} */
    @Override
    public void start() {
        if (channel != null) {
            throw new IgniteException("ClientHandlerModule is already started.");
        }

        var configuration = registry.getConfiguration(ClientConnectorConfiguration.KEY).value();

        metricManager.registerSource(metrics);

        if (configuration.metricsEnabled()) {
            metrics.enable();
        }

        try {
            channel = startEndpoint(configuration).channel();
            clusterId = clusterIdSupplier.get().join();
        } catch (InterruptedException e) {
            throw new IgniteException(e);
        }
    }

    /** {@inheritDoc} */
    @Override
    public void stop() throws Exception {
        metricManager.unregisterSource(metrics);

        if (channel != null) {
            channel.close().await();

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
        if (channel == null) {
            throw new IgniteInternalException("ClientHandlerModule has not been started");
        }

        return (InetSocketAddress) channel.localAddress();
    }

    /**
     * Starts the endpoint.
     *
     * @param configuration Configuration.
     * @return Channel future.
     * @throws InterruptedException If thread has been interrupted during the start.
     * @throws IgniteException      When startup has failed.
     */
    private ChannelFuture startEndpoint(ClientConnectorView configuration) throws InterruptedException {
        ServerBootstrap bootstrap = bootstrapFactory.createServerBootstrap();

        // Initialize SslContext once on startup to avoid initialization on each connection, and to fail in case of incorrect config.
        SslContext sslContext = configuration.ssl().enabled() ? SslContextProvider.createServerSslContext(configuration.ssl()) : null;

        bootstrap.childHandler(new ChannelInitializer<>() {
                    @Override
                    protected void initChannel(Channel ch) {
                        if (LOG.isDebugEnabled()) {
                            LOG.debug("New client connection [remoteAddress=" + ch.remoteAddress() + ']');
                        }

                        if (configuration.idleTimeout() > 0) {
                            IdleStateHandler idleStateHandler = new IdleStateHandler(
                                    configuration.idleTimeout(), 0, 0, TimeUnit.MILLISECONDS);

                            ch.pipeline().addLast(idleStateHandler);
                            ch.pipeline().addLast(new IdleChannelHandler(configuration.idleTimeout(), metrics));
                        }

                        if (sslContext != null) {
                            ch.pipeline().addFirst("ssl", sslContext.newHandler(ch.alloc()));
                        }

                        ch.pipeline().addLast(
                                new ClientMessageDecoder(),
                                createInboundMessageHandler(configuration));

                        metrics.connectionsInitiatedIncrement();
                    }
                })
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, configuration.connectTimeout());

        int port = configuration.port();
        Channel ch = null;

        ChannelFuture bindRes = bootstrap.bind(port).await();

        if (bindRes.isSuccess()) {
            ch = bindRes.channel();
        } else if (!(bindRes.cause() instanceof BindException)) {
            throw new IgniteException(
                    ErrorGroups.Common.INTERNAL_ERR,
                    "Failed to start thin client connector endpoint: " + bindRes.cause().getMessage(),
                    bindRes.cause());
        }

        if (ch == null) {
            String msg = "Cannot start thin client connector endpoint. Port " + port + " is in use.";

            LOG.debug(msg);

            throw new IgniteException(ErrorGroups.Network.PORT_IN_USE_ERR, msg);
        }

        if (LOG.isInfoEnabled()) {
            LOG.info("Thin client protocol started successfully [port={}]", port);
        }

        return ch.closeFuture();
    }

    private ClientInboundMessageHandler createInboundMessageHandler(ClientConnectorView configuration) {
        ClientInboundMessageHandler clientInboundMessageHandler = new ClientInboundMessageHandler(
                igniteTables,
                igniteTransactions,
                queryProcessor,
                configuration,
                igniteCompute,
                clusterService,
                sql,
                clusterId,
                metrics,
                authenticationManager);
        authenticationConfiguration.listen(clientInboundMessageHandler);
        return clientInboundMessageHandler;
    }

}
