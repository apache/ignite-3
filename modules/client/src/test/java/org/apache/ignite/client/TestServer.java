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

import static org.apache.ignite.configuration.annotation.ConfigurationType.LOCAL;
import static org.mockito.Mockito.mock;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.ignite.Ignite;
import org.apache.ignite.client.fakes.FakeIgnite;
import org.apache.ignite.client.handler.ClientHandlerModule;
import org.apache.ignite.compute.IgniteCompute;
import org.apache.ignite.configuration.schemas.clientconnector.ClientConnectorConfiguration;
import org.apache.ignite.configuration.schemas.network.NetworkConfiguration;
import org.apache.ignite.internal.configuration.ConfigurationRegistry;
import org.apache.ignite.internal.configuration.storage.TestConfigurationStorage;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.network.ClusterService;
import org.apache.ignite.network.NettyBootstrapFactory;

/**
 * Test server.
 */
public class TestServer implements AutoCloseable {
    private final ConfigurationRegistry cfg;

    private final IgniteComponent module;

    private final NettyBootstrapFactory bootstrapFactory;

    /**
     * Constructor.
     *
     * @param port Port.
     * @param portRange Port range.
     * @param idleTimeout Idle timeout.
     * @param ignite Ignite.
     */
    public TestServer(
            int port,
            int portRange,
            long idleTimeout,
            Ignite ignite
    ) {
        this(port, portRange, idleTimeout, ignite, null);
    }

    /**
     * Constructor.
     *
     * @param port Port.
     * @param portRange Port range.
     * @param idleTimeout Idle timeout.
     * @param ignite Ignite.
     */
    public TestServer(
            int port,
            int portRange,
            long idleTimeout,
            Ignite ignite,
            IgniteComponent module
    ) {
        cfg = new ConfigurationRegistry(
                List.of(ClientConnectorConfiguration.KEY, NetworkConfiguration.KEY),
                Map.of(),
                new TestConfigurationStorage(LOCAL),
                List.of(),
                List.of()
        );

        cfg.start();

        cfg.getConfiguration(ClientConnectorConfiguration.KEY).change(
                local -> local.changePort(port).changePortRange(portRange).changeIdleTimeout(idleTimeout)
        ).join();

        bootstrapFactory = new NettyBootstrapFactory(cfg.getConfiguration(NetworkConfiguration.KEY), "TestServer-");

        bootstrapFactory.start();

        this.module = module != null
                ? module
                : new ClientHandlerModule(
                        ((FakeIgnite) ignite).queryEngine(),
                        ignite.tables(),
                        ignite.transactions(),
                        cfg,
                        mock(IgniteCompute.class),
                        mock(ClusterService.class),
                        bootstrapFactory
                );

        this.module.start();
    }

    public ConfigurationRegistry configurationRegistry() {
        return cfg;
    }

    public int port() {
        SocketAddress addr = ((ClientHandlerModule) module).localAddress();

        return ((InetSocketAddress) Objects.requireNonNull(addr)).getPort();
    }

    public NettyBootstrapFactory bootstrapFactory() {
        return bootstrapFactory;
    }

    @Override
    public void close() throws Exception {
        module.stop();
        bootstrapFactory.stop();
        cfg.stop();
    }
}
