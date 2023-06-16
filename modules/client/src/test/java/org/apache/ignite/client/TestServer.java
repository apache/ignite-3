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

import static org.apache.ignite.configuration.annotation.ConfigurationType.LOCAL;
import static org.mockito.Answers.RETURNS_DEEP_STUBS;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;

import java.io.IOError;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.SocketAddress;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import org.apache.ignite.Ignite;
import org.apache.ignite.client.fakes.FakeCompute;
import org.apache.ignite.client.fakes.FakeIgnite;
import org.apache.ignite.client.handler.ClientHandlerMetricSource;
import org.apache.ignite.client.handler.ClientHandlerModule;
import org.apache.ignite.client.handler.configuration.ClientConnectorConfiguration;
import org.apache.ignite.compute.IgniteCompute;
import org.apache.ignite.internal.configuration.AuthenticationConfiguration;
import org.apache.ignite.internal.configuration.ConfigurationRegistry;
import org.apache.ignite.internal.configuration.ConfigurationTreeGenerator;
import org.apache.ignite.internal.configuration.storage.TestConfigurationStorage;
import org.apache.ignite.internal.configuration.validation.TestConfigurationValidator;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.metrics.MetricManager;
import org.apache.ignite.internal.network.configuration.NetworkConfiguration;
import org.apache.ignite.internal.security.authentication.AuthenticationManager;
import org.apache.ignite.internal.security.authentication.AuthenticationManagerImpl;
import org.apache.ignite.internal.table.IgniteTablesInternal;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.ClusterService;
import org.apache.ignite.network.NettyBootstrapFactory;
import org.apache.ignite.network.NetworkAddress;
import org.jetbrains.annotations.Nullable;
import org.mockito.Mockito;

/**
 * Test server.
 */
public class TestServer implements AutoCloseable {
    private final ConfigurationTreeGenerator generator;

    private final ConfigurationRegistry cfg;

    private final IgniteComponent module;

    private final NettyBootstrapFactory bootstrapFactory;

    private final String nodeName;

    private final ClientHandlerMetricSource metrics;

    /**
     * Constructor.
     *
     * @param idleTimeout Idle timeout.
     * @param ignite Ignite.
     */
    public TestServer(
            long idleTimeout,
            Ignite ignite
    ) {
        this(
                idleTimeout,
                ignite,
                null,
                null,
                null,
                UUID.randomUUID(),
                null,
                null
        );
    }

    /**
     * Constructor.
     *
     * @param idleTimeout Idle timeout.
     * @param ignite Ignite.
     */
    public TestServer(
            long idleTimeout,
            Ignite ignite,
            @Nullable Function<Integer, Boolean> shouldDropConnection,
            @Nullable Function<Integer, Integer> responseDelay,
            @Nullable String nodeName,
            UUID clusterId,
            @Nullable AuthenticationConfiguration authenticationConfiguration,
            @Nullable Integer port
    ) {
        generator = new ConfigurationTreeGenerator(ClientConnectorConfiguration.KEY, NetworkConfiguration.KEY);
        cfg = new ConfigurationRegistry(
                List.of(ClientConnectorConfiguration.KEY, NetworkConfiguration.KEY),
                new TestConfigurationStorage(LOCAL),
                generator,
                new TestConfigurationValidator()
        );

        cfg.start();

        cfg.getConfiguration(ClientConnectorConfiguration.KEY).change(
                local -> local.changePort(port != null ? port : getFreePort()).changeIdleTimeout(idleTimeout)
        ).join();

        bootstrapFactory = new NettyBootstrapFactory(cfg.getConfiguration(NetworkConfiguration.KEY), "TestServer-");

        bootstrapFactory.start();

        if (nodeName == null) {
            nodeName = "server-1";
        }

        this.nodeName = nodeName;

        ClusterService clusterService = mock(ClusterService.class, RETURNS_DEEP_STUBS);
        Mockito.when(clusterService.topologyService().localMember().id()).thenReturn(getNodeId(nodeName));
        Mockito.when(clusterService.topologyService().localMember().name()).thenReturn(nodeName);
        Mockito.when(clusterService.topologyService().localMember()).thenReturn(getClusterNode(nodeName));
        Mockito.when(clusterService.topologyService().getByConsistentId(anyString())).thenAnswer(
                i -> getClusterNode(i.getArgument(0, String.class)));

        IgniteCompute compute = new FakeCompute(nodeName);

        metrics = new ClientHandlerMetricSource();
        metrics.enable();

        AuthenticationConfiguration authenticationConfigToApply = authenticationConfiguration == null
                ? mock(AuthenticationConfiguration.class)
                : authenticationConfiguration;
        module = shouldDropConnection != null
                ? new TestClientHandlerModule(
                        ignite,
                        cfg,
                        bootstrapFactory,
                        shouldDropConnection,
                        responseDelay,
                        clusterService,
                        compute,
                        clusterId,
                        metrics,
                        authenticationConfigToApply)
                : new ClientHandlerModule(
                        ((FakeIgnite) ignite).queryEngine(),
                        (IgniteTablesInternal) ignite.tables(),
                        ignite.transactions(),
                        cfg,
                        compute,
                        clusterService,
                        bootstrapFactory,
                        ignite.sql(),
                        () -> CompletableFuture.completedFuture(clusterId),
                        mock(MetricManager.class),
                        metrics,
                        authenticationManager(authenticationConfigToApply),
                        authenticationConfigToApply
                        );

        module.start();
    }

    /**
     * Gets the port where this instance is listening.
     *
     * @return TCP port.
     */
    public int port() {
        SocketAddress addr = module instanceof ClientHandlerModule
                ? ((ClientHandlerModule) module).localAddress()
                : ((TestClientHandlerModule) module).localAddress();

        return ((InetSocketAddress) Objects.requireNonNull(addr)).getPort();
    }

    /**
     * Gets the node name.
     *
     * @return Node name.
     */
    public String nodeName() {
        return nodeName;
    }

    /**
     * Gets the node name.
     *
     * @return Node name.
     */
    public String nodeId() {
        return getNodeId(nodeName);
    }

    /**
     * Gets metrics.
     *
     * @return Metrics.
     */
    public ClientHandlerMetricSource metrics() {
        return metrics;
    }

    /** {@inheritDoc} */
    @Override
    public void close() throws Exception {
        module.stop();
        bootstrapFactory.stop();
        cfg.stop();
        generator.close();
    }

    private ClusterNode getClusterNode(String name) {
        return new ClusterNode(getNodeId(name), name, new NetworkAddress("127.0.0.1", 8080));
    }

    private static String getNodeId(String name) {
        return name + "-id";
    }

    private static int getFreePort() {
        try (var serverSocket = new ServerSocket(0)) {
            return serverSocket.getLocalPort();
        } catch (IOException e) {
            throw new IOError(e);
        }
    }

    private AuthenticationManager authenticationManager(AuthenticationConfiguration authenticationConfiguration) {
        AuthenticationManagerImpl authenticationManager = new AuthenticationManagerImpl();
        authenticationConfiguration.listen(authenticationManager);
        return authenticationManager;
    }
}
