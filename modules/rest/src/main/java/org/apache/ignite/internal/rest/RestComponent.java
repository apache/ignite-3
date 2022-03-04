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

package org.apache.ignite.internal.rest;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import java.net.BindException;
import java.net.InetSocketAddress;
import java.util.function.Consumer;
import org.apache.ignite.configuration.schemas.rest.RestConfiguration;
import org.apache.ignite.configuration.schemas.rest.RestView;
import org.apache.ignite.internal.configuration.ConfigurationManager;
import org.apache.ignite.internal.configuration.ConfigurationRegistry;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.rest.api.RestHandlersRegister;
import org.apache.ignite.internal.rest.api.Routes;
import org.apache.ignite.internal.rest.netty.RestApiInitializer;
import org.apache.ignite.internal.rest.routes.Router;
import org.apache.ignite.internal.rest.routes.SimpleRouter;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.lang.IgniteInternalException;
import org.apache.ignite.lang.IgniteLogger;
import org.apache.ignite.network.NettyBootstrapFactory;

/**
 * Rest component is responsible for starting REST endpoints.
 *
 * <p>It is started on port 10300 by default, but it is possible to change this in configuration itself. Refer to default config file in
 * resources for the example.
 */
public class RestComponent implements RestHandlersRegister, IgniteComponent {
    /** Ignite logger. */
    private final IgniteLogger log = IgniteLogger.forClass(RestComponent.class);

    /** Node configuration register. */
    private final ConfigurationRegistry nodeCfgRegistry;

    /** Netty bootstrap factory. */
    private final NettyBootstrapFactory bootstrapFactory;

    private final SimpleRouter router = new SimpleRouter();

    /** Netty channel. */
    private volatile Channel channel;

    /**
     * Creates a new instance of REST module.
     *
     * @param nodeCfgMgr       Node configuration manager.
     * @param bootstrapFactory Netty bootstrap factory.
     */
    public RestComponent(ConfigurationManager nodeCfgMgr, NettyBootstrapFactory bootstrapFactory) {
        nodeCfgRegistry = nodeCfgMgr.configurationRegistry();

        this.bootstrapFactory = bootstrapFactory;
    }

    /** {@inheritDoc} */
    @Override
    public void registerHandlers(Consumer<Routes> registerAction) {
        registerAction.accept(router);
    }

    /** {@inheritDoc} */
    @Override
    public void start() {
        if (channel != null) {
            throw new IgniteException("RestModule is already started.");
        }

        channel = startRestEndpoint(router).channel();
    }

    /**
     * Start endpoint.
     *
     * @param router Dispatcher of http requests.
     * @return Future which will be notified when this channel is closed.
     * @throws RuntimeException if this module cannot be bound to a port.
     */
    private ChannelFuture startRestEndpoint(Router router) {
        RestView restConfigurationView = nodeCfgRegistry.getConfiguration(RestConfiguration.KEY).value();

        int desiredPort = restConfigurationView.port();
        int portRange = restConfigurationView.portRange();

        int port = 0;
        Channel ch = null;

        ServerBootstrap bootstrap = bootstrapFactory.createServerBootstrap()
                .handler(new LoggingHandler(LogLevel.INFO))
                .childHandler(new RestApiInitializer(router));

        for (int portCandidate = desiredPort; portCandidate <= desiredPort + portRange; portCandidate++) {
            ChannelFuture bindRes = bootstrap.bind(portCandidate).awaitUninterruptibly();

            if (bindRes.isSuccess()) {
                ch = bindRes.channel();
                port = portCandidate;
                break;
            } else if (!(bindRes.cause() instanceof BindException)) {
                throw new RuntimeException(bindRes.cause());
            }
        }

        if (ch == null) {
            String msg = "Cannot start REST endpoint. "
                    + "All ports in range [" + desiredPort + ", " + (desiredPort + portRange) + "] are in use.";

            log.error(msg);

            throw new RuntimeException(msg);
        }

        if (log.isInfoEnabled()) {
            log.info("REST protocol started successfully on port " + port);
        }

        return ch.closeFuture();
    }

    /** {@inheritDoc} */
    @Override
    public void stop() throws Exception {
        // TODO: IGNITE-16636 Use busy-lock approach to guard stopping RestComponent

        if (channel != null) {
            channel.close().await();

            channel = null;
        }
    }

    /**
     * Returns the local address that the REST endpoint is bound to.
     *
     * @return local REST address.
     * @throws IgniteInternalException if the component has not been started yet.
     */
    public InetSocketAddress localAddress() {
        Channel channel = this.channel;

        if (channel == null) {
            throw new IgniteInternalException("RestComponent has not been started");
        }

        return (InetSocketAddress) channel.localAddress();
    }
}
