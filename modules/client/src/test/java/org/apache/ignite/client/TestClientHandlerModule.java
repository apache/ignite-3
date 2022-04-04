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

package org.apache.ignite.client;

import static org.mockito.Mockito.mock;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import java.net.BindException;
import java.net.SocketAddress;
import org.apache.ignite.client.fakes.FakeIgnite;
import org.apache.ignite.client.handler.ClientInboundMessageHandler;
import org.apache.ignite.compute.IgniteCompute;
import org.apache.ignite.configuration.schemas.clientconnector.ClientConnectorConfiguration;
import org.apache.ignite.internal.client.proto.ClientMessageDecoder;
import org.apache.ignite.internal.configuration.ConfigurationRegistry;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.sql.engine.QueryProcessor;
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
    private final FakeIgnite ignite;

    /** Netty channel. */
    private volatile Channel channel;

    /** Netty bootstrap factory. */
    private final NettyBootstrapFactory bootstrapFactory;

    /**
     * Constructor.
     *
     * @param ignite       Ignite.
     * @param registry           Configuration registry.
     * @param bootstrapFactory   Bootstrap factory.
     */
    public TestClientHandlerModule(
            FakeIgnite ignite,
            ConfigurationRegistry registry,
            NettyBootstrapFactory bootstrapFactory) {
        assert ignite != null;
        assert registry != null;
        assert bootstrapFactory != null;

        this.ignite = ignite;
        this.registry = registry;
        this.bootstrapFactory = bootstrapFactory;
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

        int desiredPort = configuration.port();
        int portRange = configuration.portRange();

        Channel ch = null;

        ServerBootstrap bootstrap = bootstrapFactory.createServerBootstrap();

        bootstrap.childHandler(new ChannelInitializer<>() {
                    @Override
                    protected void initChannel(Channel ch) {
                        ch.pipeline().addLast(
                                new ClientMessageDecoder(),
                                new ClientInboundMessageHandler(
                                        ignite.tables(),
                                        ignite.transactions(),
                                        mock(QueryProcessor.class),
                                        configuration,
                                        mock(IgniteCompute.class),
                                        mock(ClusterService.class)));
                    }
                })
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, configuration.connectTimeout());

        for (int portCandidate = desiredPort; portCandidate <= desiredPort + portRange; portCandidate++) {
            ChannelFuture bindRes = bootstrap.bind(portCandidate).await();

            if (bindRes.isSuccess()) {
                ch = bindRes.channel();

                break;
            } else if (!(bindRes.cause() instanceof BindException)) {
                throw new IgniteException(bindRes.cause());
            }
        }

        if (ch == null) {
            String msg = "Cannot start thin client connector endpoint. "
                    + "All ports in range [" + desiredPort + ", " + (desiredPort + portRange) + "] are in use.";

            throw new IgniteException(msg);
        }

        return ch.closeFuture();
    }
}
