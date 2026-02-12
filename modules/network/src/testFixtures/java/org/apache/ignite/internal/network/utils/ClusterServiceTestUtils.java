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

package org.apache.ignite.internal.network.utils;

import static java.util.stream.Collectors.toUnmodifiableList;
import static org.apache.ignite.internal.network.ConstantClusterIdSupplier.withoutClusterId;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.testNodeName;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.IntStream;
import org.apache.ignite.configuration.annotation.ConfigurationType;
import org.apache.ignite.internal.configuration.ConfigurationManager;
import org.apache.ignite.internal.configuration.ConfigurationTreeGenerator;
import org.apache.ignite.internal.configuration.NodeConfiguration;
import org.apache.ignite.internal.configuration.storage.TestConfigurationStorage;
import org.apache.ignite.internal.configuration.validation.TestConfigurationValidator;
import org.apache.ignite.internal.failure.FailureManager;
import org.apache.ignite.internal.failure.handlers.NoOpFailureHandler;
import org.apache.ignite.internal.manager.ComponentContext;
import org.apache.ignite.internal.metrics.NoOpMetricManager;
import org.apache.ignite.internal.network.AbstractClusterService;
import org.apache.ignite.internal.network.ChannelTypeRegistry;
import org.apache.ignite.internal.network.ChannelTypeRegistryProvider;
import org.apache.ignite.internal.network.ClusterIdSupplier;
import org.apache.ignite.internal.network.ClusterService;
import org.apache.ignite.internal.network.MessageSerializationRegistryImpl;
import org.apache.ignite.internal.network.NettyBootstrapFactory;
import org.apache.ignite.internal.network.NodeFinder;
import org.apache.ignite.internal.network.configuration.MulticastNodeFinderConfigurationSchema;
import org.apache.ignite.internal.network.configuration.NetworkConfiguration;
import org.apache.ignite.internal.network.configuration.NetworkExtensionConfiguration;
import org.apache.ignite.internal.network.configuration.NetworkExtensionConfigurationSchema;
import org.apache.ignite.internal.network.configuration.StaticNodeFinderChange;
import org.apache.ignite.internal.network.configuration.StaticNodeFinderConfigurationSchema;
import org.apache.ignite.internal.network.recovery.InMemoryStaleIds;
import org.apache.ignite.internal.network.recovery.StaleIds;
import org.apache.ignite.internal.network.scalecube.TestScaleCubeClusterService;
import org.apache.ignite.internal.network.serialization.MessageSerializationRegistry;
import org.apache.ignite.internal.network.serialization.MessageSerializationRegistryInitializer;
import org.apache.ignite.internal.network.serialization.SerializationRegistryServiceLoader;
import org.apache.ignite.internal.version.DefaultIgniteProductVersionSource;
import org.apache.ignite.internal.version.IgniteProductVersionSource;
import org.apache.ignite.internal.worker.fixtures.NoOpCriticalWorkerRegistry;
import org.apache.ignite.network.NetworkAddress;
import org.junit.jupiter.api.TestInfo;

/**
 * Test utils that provide sort of cluster service mock that manages required node configuration internally.
 */
public class ClusterServiceTestUtils {
    /**
     * Creates a {@link MessageSerializationRegistry} that is pre-populated with all {@link MessageSerializationRegistryInitializer}s,
     * accessible through the classpath.
     */
    public static MessageSerializationRegistry defaultSerializationRegistry() {
        var serviceLoader = new SerializationRegistryServiceLoader(null);

        var serializationRegistry = new MessageSerializationRegistryImpl();

        serviceLoader.registerSerializationFactories(serializationRegistry);

        return serializationRegistry;
    }

    /** Creates a {@link ChannelTypeRegistry} for tests. */
    public static ChannelTypeRegistry defaultChannelTypeRegistry() {
        return ChannelTypeRegistryProvider.loadByServiceLoader(null);
    }

    /**
     * Creates a cluster service and required node configuration manager beneath it. Populates node configuration with specified port.
     * Manages configuration manager lifecycle: on cluster service start starts node configuration manager, on cluster service stop - stops
     * node configuration manager.
     *
     * @param testInfo                 Test info.
     * @param port                     Local port.
     * @param nodeFinder               Node finder.
     */
    public static ClusterService clusterService(TestInfo testInfo, int port, NodeFinder nodeFinder) {
        return clusterService(testInfo, port, nodeFinder, new InMemoryStaleIds());
    }

    /**
     * Creates a cluster service and required node configuration manager beneath it. Populates node configuration with specified port.
     * Manages configuration manager lifecycle: on cluster service start starts node configuration manager, on cluster service stop - stops
     * node configuration manager.
     *
     * @param testInfo                 Test info.
     * @param port                     Local port.
     * @param nodeFinder               Node finder.
     * @param staleIds                 Used to track stale launch IDs.
     */
    public static ClusterService clusterService(TestInfo testInfo, int port, NodeFinder nodeFinder, StaleIds staleIds) {
        return clusterService(testInfo, port, nodeFinder, staleIds, withoutClusterId());
    }

    /**
     * Creates a cluster service and required node configuration manager beneath it. Populates node configuration with specified port.
     * Manages configuration manager lifecycle: on cluster service start starts node configuration manager, on cluster service stop - stops
     * node configuration manager.
     *
     * @param testInfo                 Test info.
     * @param port                     Local port.
     * @param nodeFinder               Node finder.
     * @param staleIds                 Used to track stale launch IDs.
     * @param clusterIdSupplier Supplier of cluster ID.
     */
    public static ClusterService clusterService(
            TestInfo testInfo,
            int port,
            NodeFinder nodeFinder,
            StaleIds staleIds,
            ClusterIdSupplier clusterIdSupplier
    ) {
        return clusterService(testInfo, port, nodeFinder, staleIds, clusterIdSupplier, new DefaultIgniteProductVersionSource());
    }

    /**
     * Creates a cluster service and required node configuration manager beneath it. Populates node configuration with specified port.
     * Manages configuration manager lifecycle: on cluster service start starts node configuration manager, on cluster service stop - stops
     * node configuration manager.
     *
     * @param testInfo                 Test info.
     * @param port                     Local port.
     * @param nodeFinder               Node finder.
     * @param staleIds                 Used to track stale launch IDs.
     * @param clusterIdSupplier Supplier of cluster ID.
     * @param productVersionSource Product version source.
     */
    public static ClusterService clusterService(
            TestInfo testInfo,
            int port,
            NodeFinder nodeFinder,
            StaleIds staleIds,
            ClusterIdSupplier clusterIdSupplier,
            IgniteProductVersionSource productVersionSource
    ) {
        String nodeName = testNodeName(testInfo, port);

        return clusterService(nodeName, port, nodeFinder, staleIds, clusterIdSupplier, productVersionSource);
    }

    /**
     * Creates a cluster service with predefined name.
     *
     * @param nodeName Node name.
     * @param port Local port.
     * @param nodeFinder Node finder.
     * @return Cluster service instance.
     */
    public static ClusterService clusterService(String nodeName, int port, NodeFinder nodeFinder) {
        return clusterService(nodeName, port, nodeFinder, new InMemoryStaleIds(), withoutClusterId());
    }

    /**
     * Creates a cluster service with predefined name.
     *
     * @param nodeName Node name.
     * @param port Local port.
     * @param nodeFinder Node finder.
     * @param staleIds Used to track stale launch IDs.
     * @param clusterIdSupplier Supplier of cluster ID.
     * @return Cluster service instance.
     */
    private static ClusterService clusterService(
            String nodeName,
            int port,
            NodeFinder nodeFinder,
            StaleIds staleIds,
            ClusterIdSupplier clusterIdSupplier
    ) {
        return clusterService(nodeName, port, nodeFinder, staleIds, clusterIdSupplier, new DefaultIgniteProductVersionSource());
    }

    /**
     * Creates a cluster service with predefined name.
     *
     * @param nodeName Node name.
     * @param port Local port.
     * @param nodeFinder Node finder.
     * @param staleIds Used to track stale launch IDs.
     * @param clusterIdSupplier Supplier of cluster ID.
     * @param productVersionSource Product version source.
     * @return Cluster service instance.
     */
    private static ClusterService clusterService(
            String nodeName,
            int port,
            NodeFinder nodeFinder,
            StaleIds staleIds,
            ClusterIdSupplier clusterIdSupplier,
            IgniteProductVersionSource productVersionSource
    ) {
        ConfigurationTreeGenerator generator = new ConfigurationTreeGenerator(
                List.of(NodeConfiguration.KEY),
                List.of(NetworkExtensionConfigurationSchema.class),
                List.of(StaticNodeFinderConfigurationSchema.class, MulticastNodeFinderConfigurationSchema.class)
        );
        ConfigurationManager nodeConfigurationMgr = new ConfigurationManager(
                Collections.singleton(NodeConfiguration.KEY),
                new TestConfigurationStorage(ConfigurationType.LOCAL),
                generator,
                new TestConfigurationValidator()
        );

        NetworkConfiguration networkConfiguration = nodeConfigurationMgr.configurationRegistry()
                .getConfiguration(NetworkExtensionConfiguration.KEY).network();

        var bootstrapFactory = new NettyBootstrapFactory(networkConfiguration, nodeName);

        MessageSerializationRegistry serializationRegistry = defaultSerializationRegistry();

        nodeConfigurationMgr.startAsync(new ComponentContext()).join();

        String[] netClusterNodes = nodeFinder.findNodes().stream().map(NetworkAddress::toString).toArray(String[]::new);

        networkConfiguration.change(netCfg ->
                netCfg
                        .changePort(port)
                        .changeNodeFinder(c -> c
                                .convert(StaticNodeFinderChange.class)
                                .changeNetClusterNodes(netClusterNodes)
                        )
        ).join();

        return new TestScaleCubeClusterService(
                nodeName,
                networkConfiguration,
                bootstrapFactory,
                serializationRegistry,
                staleIds,
                clusterIdSupplier,
                new NoOpCriticalWorkerRegistry(),
                new FailureManager(new NoOpFailureHandler()),
                new NoOpMetricManager(),
                defaultChannelTypeRegistry(),
                productVersionSource
        ) {
            @Override
            public CompletableFuture<Void> startAsync(ComponentContext componentContext) {
                return CompletableFuture.allOf(
                        bootstrapFactory.startAsync(componentContext),
                        super.startAsync(componentContext)
                );
            }

            @Override
            public CompletableFuture<Void> stopAsync(ComponentContext componentContext) {
                return CompletableFuture.allOf(
                        super.stopAsync(componentContext),
                        bootstrapFactory.stopAsync(componentContext),
                        nodeConfigurationMgr.stopAsync(componentContext)
                );
            }
        };
    }

    /**
     * Creates a list of {@link NetworkAddress}es within a given port range.
     *
     * @param startPort Start port (inclusive).
     * @param endPort   End port (exclusive).
     * @return Configuration closure.
     */
    public static List<NetworkAddress> findLocalAddresses(int startPort, int endPort) {
        return IntStream.range(startPort, endPort)
                .mapToObj(port -> new NetworkAddress("localhost", port))
                .collect(toUnmodifiableList());
    }

    /**
     * Waits for the {@code expected} amount of nodes to appear in a topology.
     *
     * @param cluster The cluster.
     * @param expected Expected count.
     * @param timeout The timeout in millis.
     * @return {@code True} if topology size is equal to expected.
     */
    public static boolean waitForTopology(ClusterService cluster, int expected, int timeout) throws InterruptedException {
        return waitForCondition(() -> cluster.topologyService().allMembers().size() >= expected, timeout);
    }
}
