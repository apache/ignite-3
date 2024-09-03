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
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.util.IgniteUtils.stopAsync;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Answers.RETURNS_DEEP_STUBS;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;

import io.netty.util.ResourceLeakDetector;
import java.io.IOError;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.SocketAddress;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.function.Function;
import org.apache.ignite.Ignite;
import org.apache.ignite.client.fakes.FakeIgnite;
import org.apache.ignite.client.fakes.FakeInternalTable;
import org.apache.ignite.client.handler.ClientHandlerMetricSource;
import org.apache.ignite.client.handler.ClientHandlerModule;
import org.apache.ignite.client.handler.ClusterInfo;
import org.apache.ignite.client.handler.DummyAuthenticationManager;
import org.apache.ignite.client.handler.FakeCatalogService;
import org.apache.ignite.client.handler.FakePlacementDriver;
import org.apache.ignite.client.handler.configuration.ClientConnectorConfiguration;
import org.apache.ignite.client.handler.configuration.ClientConnectorExtensionConfiguration;
import org.apache.ignite.client.handler.configuration.ClientConnectorExtensionConfigurationSchema;
import org.apache.ignite.internal.client.ClientClusterNode;
import org.apache.ignite.internal.cluster.management.ClusterTag;
import org.apache.ignite.internal.cluster.management.network.messages.CmgMessagesFactory;
import org.apache.ignite.internal.compute.IgniteComputeInternal;
import org.apache.ignite.internal.configuration.ConfigurationRegistry;
import org.apache.ignite.internal.configuration.ConfigurationTreeGenerator;
import org.apache.ignite.internal.configuration.NodeConfiguration;
import org.apache.ignite.internal.configuration.storage.TestConfigurationStorage;
import org.apache.ignite.internal.configuration.validation.TestConfigurationValidator;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.hlc.TestClockService;
import org.apache.ignite.internal.lowwatermark.TestLowWatermark;
import org.apache.ignite.internal.manager.ComponentContext;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.metrics.MetricManagerImpl;
import org.apache.ignite.internal.network.ClusterService;
import org.apache.ignite.internal.network.NettyBootstrapFactory;
import org.apache.ignite.internal.network.configuration.NetworkExtensionConfiguration;
import org.apache.ignite.internal.network.configuration.NetworkExtensionConfigurationSchema;
import org.apache.ignite.internal.security.authentication.AuthenticationManager;
import org.apache.ignite.internal.security.authentication.AuthenticationManagerImpl;
import org.apache.ignite.internal.security.configuration.SecurityConfiguration;
import org.apache.ignite.internal.table.IgniteTablesInternal;
import org.apache.ignite.internal.table.distributed.schema.AlwaysSyncedSchemaSyncService;
import org.apache.ignite.internal.tx.impl.IgniteTransactionsImpl;
import org.apache.ignite.network.ClusterNode;
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

    private final AuthenticationManager authenticationManager;

    private final FakeIgnite ignite;

    /**
     * Constructor.
     *
     * @param idleTimeout Idle timeout.
     * @param ignite Ignite.
     */
    public TestServer(
            long idleTimeout,
            FakeIgnite ignite
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
     */
    public TestServer(
            long idleTimeout,
            FakeIgnite ignite,
            @Nullable Function<Integer, Boolean> shouldDropConnection,
            @Nullable Function<Integer, Integer> responseDelay,
            @Nullable String nodeName,
            UUID clusterId,
            @Nullable SecurityConfiguration securityConfiguration,
            @Nullable Integer port
    ) {
        this(
                idleTimeout,
                ignite,
                shouldDropConnection,
                responseDelay,
                nodeName,
                clusterId,
                securityConfiguration,
                port,
                null,
                true
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
            FakeIgnite ignite,
            @Nullable Function<Integer, Boolean> shouldDropConnection,
            @Nullable Function<Integer, Integer> responseDelay,
            @Nullable String nodeName,
            UUID clusterId,
            @Nullable SecurityConfiguration securityConfiguration,
            @Nullable Integer port,
            @Nullable HybridClock clock,
            boolean enableRequestHandling
    ) {
        ResourceLeakDetector.setLevel(ResourceLeakDetector.Level.PARANOID);

        generator = new ConfigurationTreeGenerator(
                List.of(NodeConfiguration.KEY),
                List.of(ClientConnectorExtensionConfigurationSchema.class, NetworkExtensionConfigurationSchema.class),
                List.of()
        );
        cfg = new ConfigurationRegistry(
                List.of(NodeConfiguration.KEY),
                new TestConfigurationStorage(LOCAL),
                generator,
                new TestConfigurationValidator()
        );

        ComponentContext componentContext = new ComponentContext();

        assertThat(cfg.startAsync(componentContext), willCompleteSuccessfully());

        ClientConnectorConfiguration clientConnectorConfiguration = cfg
                .getConfiguration(ClientConnectorExtensionConfiguration.KEY).clientConnector();

        clientConnectorConfiguration.change(
                local -> local
                        .changePort(port != null ? port : getFreePort())
                        .changeIdleTimeout(idleTimeout)
                        .changeSendServerExceptionStackTraceToClient(true)
        ).join();

        bootstrapFactory = new NettyBootstrapFactory(cfg.getConfiguration(NetworkExtensionConfiguration.KEY).network(), "TestServer-");

        assertThat(bootstrapFactory.startAsync(componentContext), willCompleteSuccessfully());

        if (nodeName == null) {
            nodeName = "server-1";
        }

        this.nodeName = nodeName;
        this.ignite = ignite;

        ClusterService clusterService = mock(ClusterService.class, RETURNS_DEEP_STUBS);
        Mockito.when(clusterService.topologyService().localMember().id()).thenReturn(getNodeId(nodeName));
        Mockito.when(clusterService.topologyService().localMember().name()).thenReturn(nodeName);
        Mockito.when(clusterService.topologyService().localMember()).thenReturn(getClusterNode(nodeName));
        Mockito.when(clusterService.topologyService().getByConsistentId(anyString())).thenAnswer(
                i -> getClusterNode(i.getArgument(0, String.class)));

        metrics = new ClientHandlerMetricSource();
        metrics.enable();

        if (clock == null) {
            clock = new HybridClockImpl();
        }

        if (securityConfiguration == null) {
            authenticationManager = new DummyAuthenticationManager();
        } else {
            authenticationManager = new AuthenticationManagerImpl(securityConfiguration, ign -> {});
            assertThat(authenticationManager.startAsync(componentContext), willCompleteSuccessfully());
        }

        ClusterTag tag = new CmgMessagesFactory().clusterTag()
                .clusterName("Test Server")
                .clusterId(clusterId)
                .build();
        ClusterInfo clusterInfo = new ClusterInfo(tag, List.of(tag.clusterId()));

        module = shouldDropConnection != null
                ? new TestClientHandlerModule(
                ignite,
                bootstrapFactory,
                shouldDropConnection,
                responseDelay,
                clusterService,
                clusterInfo,
                metrics,
                authenticationManager,
                clock,
                ignite.placementDriver(),
                clientConnectorConfiguration)
                : new ClientHandlerModule(
                        ignite.queryEngine(),
                        (IgniteTablesInternal) ignite.tables(),
                        (IgniteTransactionsImpl) ignite.transactions(),
                        (IgniteComputeInternal) ignite.compute(),
                        clusterService,
                        bootstrapFactory,
                        () -> clusterInfo,
                        mock(MetricManagerImpl.class),
                        metrics,
                        authenticationManager,
                        new TestClockService(clock),
                        new AlwaysSyncedSchemaSyncService(),
                        new FakeCatalogService(FakeInternalTable.PARTITIONS),
                        ignite.placementDriver(),
                        clientConnectorConfiguration,
                        new TestLowWatermark()
                );

        module.startAsync(componentContext).join();

        if (enableRequestHandling) {
            enableClientRequestHandling();
        }
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
     * Gets the Ignite.
     *
     * @return Ignite
     */
    public Ignite ignite() {
        return ignite;
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

    /**
     * Gets the placement driver.
     *
     * @return Placement driver.
     */
    public FakePlacementDriver placementDriver() {
        return ignite.placementDriver();
    }

    /** {@inheritDoc} */
    @Override
    public void close() {
        assertThat(stopAsync(new ComponentContext(), module, authenticationManager, bootstrapFactory, cfg), willCompleteSuccessfully());

        generator.close();
    }

    /** Enables request handling. */
    void enableClientRequestHandling() {
        if (module instanceof ClientHandlerModule) {
            ((ClientHandlerModule) module).enable();
        }
    }

    private ClusterNode getClusterNode(String name) {
        return new ClientClusterNode(getNodeId(name), name, new NetworkAddress("127.0.0.1", 8080));
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
}
