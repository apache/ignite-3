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
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import org.apache.ignite.Ignite;
import org.apache.ignite.client.fakes.FakeCompute;
import org.apache.ignite.client.fakes.FakeIgnite;
import org.apache.ignite.client.fakes.FakeInternalTable;
import org.apache.ignite.client.handler.ClientHandlerMetricSource;
import org.apache.ignite.client.handler.ClientHandlerModule;
import org.apache.ignite.client.handler.DummyAuthenticationManager;
import org.apache.ignite.client.handler.FakeCatalogService;
import org.apache.ignite.client.handler.FakePlacementDriver;
import org.apache.ignite.client.handler.configuration.ClientConnectorConfiguration;
import org.apache.ignite.internal.client.ClientClusterNode;
import org.apache.ignite.internal.cluster.management.ClusterTag;
import org.apache.ignite.internal.cluster.management.network.messages.CmgMessagesFactory;
import org.apache.ignite.internal.compute.IgniteComputeInternal;
import org.apache.ignite.internal.configuration.ConfigurationRegistry;
import org.apache.ignite.internal.configuration.ConfigurationTreeGenerator;
import org.apache.ignite.internal.configuration.storage.TestConfigurationStorage;
import org.apache.ignite.internal.configuration.validation.TestConfigurationValidator;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.hlc.TestClockService;
import org.apache.ignite.internal.lowwatermark.TestLowWatermark;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.metrics.MetricManagerImpl;
import org.apache.ignite.internal.network.ClusterService;
import org.apache.ignite.internal.network.NettyBootstrapFactory;
import org.apache.ignite.internal.network.configuration.NetworkConfiguration;
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

    private final Ignite ignite;

    private final FakePlacementDriver placementDriver = new FakePlacementDriver(FakeInternalTable.PARTITIONS);

    private final CmgMessagesFactory msgFactory = new CmgMessagesFactory();

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
                null,
                null
        );
    }

    /**
     * Constructor.
     */
    public TestServer(
            long idleTimeout,
            Ignite ignite,
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
            @Nullable SecurityConfiguration securityConfiguration,
            @Nullable Integer port,
            @Nullable HybridClock clock
    ) {
        ResourceLeakDetector.setLevel(ResourceLeakDetector.Level.PARANOID);

        generator = new ConfigurationTreeGenerator(ClientConnectorConfiguration.KEY, NetworkConfiguration.KEY);
        cfg = new ConfigurationRegistry(
                List.of(ClientConnectorConfiguration.KEY, NetworkConfiguration.KEY),
                new TestConfigurationStorage(LOCAL),
                generator,
                new TestConfigurationValidator()
        );

        assertThat(cfg.startAsync(), willCompleteSuccessfully());

        cfg.getConfiguration(ClientConnectorConfiguration.KEY).change(
                local -> local
                        .changePort(port != null ? port : getFreePort())
                        .changeIdleTimeout(idleTimeout)
                        .changeSendServerExceptionStackTraceToClient(true)
        ).join();

        bootstrapFactory = new NettyBootstrapFactory(cfg.getConfiguration(NetworkConfiguration.KEY), "TestServer-");

        assertThat(bootstrapFactory.startAsync(), willCompleteSuccessfully());

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

        IgniteComputeInternal compute = new FakeCompute(nodeName);

        metrics = new ClientHandlerMetricSource();
        metrics.enable();

        if (clock == null) {
            clock = new HybridClockImpl();
        }

        if (securityConfiguration == null) {
            authenticationManager = new DummyAuthenticationManager();
        } else {
            authenticationManager = new AuthenticationManagerImpl(securityConfiguration, ign -> {});
            assertThat(authenticationManager.startAsync(), willCompleteSuccessfully());
        }

        ClusterTag tag = msgFactory.clusterTag()
                .clusterName("Test Server")
                .clusterId(clusterId)
                .build();

        ClientConnectorConfiguration clientConnectorConfiguration = cfg.getConfiguration(ClientConnectorConfiguration.KEY);

        module = shouldDropConnection != null
                ? new TestClientHandlerModule(
                ignite,
                bootstrapFactory,
                shouldDropConnection,
                responseDelay,
                clusterService,
                compute,
                tag,
                metrics,
                authenticationManager,
                clock,
                placementDriver,
                clientConnectorConfiguration)
                : new ClientHandlerModule(
                        ignite,
                        ((FakeIgnite) ignite).queryEngine(),
                        (IgniteTablesInternal) ignite.tables(),
                        (IgniteTransactionsImpl) ignite.transactions(),
                        compute,
                        clusterService,
                        bootstrapFactory,
                        () -> CompletableFuture.completedFuture(tag),
                        mock(MetricManagerImpl.class),
                        metrics,
                        authenticationManager,
                        new TestClockService(clock),
                        new AlwaysSyncedSchemaSyncService(),
                        new FakeCatalogService(FakeInternalTable.PARTITIONS),
                        placementDriver,
                        clientConnectorConfiguration,
                        new TestLowWatermark()
                );

        module.startAsync().join();
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
        return placementDriver;
    }

    /** {@inheritDoc} */
    @Override
    public void close() throws Exception {
        assertThat(stopAsync(module, authenticationManager, bootstrapFactory, cfg), willCompleteSuccessfully());

        generator.close();
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
