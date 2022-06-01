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

package org.apache.ignite.client.handler;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.handler.timeout.IdleStateHandler;
import java.net.BindException;
import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.compute.IgniteCompute;
import org.apache.ignite.configuration.schemas.clientconnector.ClientConnectorConfiguration;
import org.apache.ignite.internal.client.proto.ClientMessageDecoder;
import org.apache.ignite.internal.configuration.ConfigurationRegistry;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.sql.engine.QueryProcessor;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.lang.IgniteInternalException;
import org.apache.ignite.lang.IgniteLogger;
import org.apache.ignite.network.ClusterService;
import org.apache.ignite.network.NettyBootstrapFactory;
import org.apache.ignite.sql.IgniteSql;
import org.apache.ignite.table.manager.IgniteTables;
import org.apache.ignite.tx.IgniteTransactions;

/**
 * Client handler module maintains TCP endpoint for thin client connections.
 */
public class ClientHandlerModule implements IgniteComponent {
    /** The logger. */
    private static final IgniteLogger LOG = IgniteLogger.forClass(ClientHandlerModule.class);

    /** Configuration registry. */
    private final ConfigurationRegistry registry;

    /** Ignite tables API. */
    private final IgniteTables igniteTables;

    /** Ignite transactions API. */
    private final IgniteTransactions igniteTransactions;

    /** Ignite SQL API. */
    private final IgniteSql sql;

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

    /**
     * Constructor.
     *
     * @param queryProcessor     Sql query processor.
     * @param igniteTables       Ignite.
     * @param igniteTransactions Transactions.
     * @param registry           Configuration registry.
     * @param igniteCompute      Compute.
     * @param clusterService     Cluster.
     * @param bootstrapFactory   Bootstrap factory.
     */
    public ClientHandlerModule(
            QueryProcessor queryProcessor,
            IgniteTables igniteTables,
            IgniteTransactions igniteTransactions,
            ConfigurationRegistry registry,
            IgniteCompute igniteCompute,
            ClusterService clusterService,
            NettyBootstrapFactory bootstrapFactory,
            IgniteSql sql) {
        assert igniteTables != null;
        assert registry != null;
        assert queryProcessor != null;
        assert igniteCompute != null;
        assert clusterService != null;
        assert bootstrapFactory != null;
        assert sql != null;

        this.queryProcessor = queryProcessor;
        this.igniteTables = igniteTables;
        this.igniteTransactions = igniteTransactions;
        this.igniteCompute = igniteCompute;
        this.clusterService = clusterService;
        this.registry = registry;
        this.bootstrapFactory = bootstrapFactory;
        this.sql = sql;
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
     * @return Channel future.
     * @throws InterruptedException If thread has been interrupted during the start.
     * @throws IgniteException      When startup has failed.
     */
    private ChannelFuture startEndpoint() throws InterruptedException {
        var configuration = registry.getConfiguration(ClientConnectorConfiguration.KEY).value();

        int desiredPort = configuration.port();
        int portRange = configuration.portRange();

        int port = 0;
        Channel ch = null;

        ServerBootstrap bootstrap = bootstrapFactory.createServerBootstrap();

        bootstrap.childHandler(new ChannelInitializer<>() {
                    @Override
                    protected void initChannel(Channel ch) {
                        if (configuration.idleTimeout() > 0) {
                            IdleStateHandler idleStateHandler = new IdleStateHandler(
                                    configuration.idleTimeout(), 0, 0, TimeUnit.MILLISECONDS);

                            ch.pipeline().addLast(idleStateHandler);
                            ch.pipeline().addLast(new IdleChannelHandler());
                        }

                        ch.pipeline().addLast(
                                new ClientMessageDecoder(),
                                new ClientInboundMessageHandler(
                                        igniteTables,
                                        igniteTransactions,
                                        queryProcessor,
                                        configuration,
                                        igniteCompute,
                                        clusterService,
                                        sql));
                    }
                })
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, configuration.connectTimeout());

        for (int portCandidate = desiredPort; portCandidate <= desiredPort + portRange; portCandidate++) {
            ChannelFuture bindRes = bootstrap.bind(portCandidate).await();

            if (bindRes.isSuccess()) {
                ch = bindRes.channel();

                port = portCandidate;
                break;
            } else if (!(bindRes.cause() instanceof BindException)) {
                throw new IgniteException(bindRes.cause());
            }
        }

        if (ch == null) {
            String msg = "Cannot start thin client connector endpoint. "
                    + "All ports in range [" + desiredPort + ", " + (desiredPort + portRange) + "] are in use.";

            LOG.error(msg);

            throw new IgniteException(msg);
        }

        LOG.info("Thin client protocol started successfully on port " + port);

        return ch.closeFuture();
    }

    /** Idle channel state handler. */
    private static class IdleChannelHandler extends ChannelDuplexHandler {
        /** {@inheritDoc} */
        @Override
        public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
            if (evt instanceof IdleStateEvent && ((IdleStateEvent) evt).state() == IdleState.READER_IDLE) {
                ctx.close();
            }
        }
    }
}
