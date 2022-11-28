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

package org.apache.ignite.utils;

import static java.util.stream.Collectors.toUnmodifiableList;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.testNodeName;

import io.github.classgraph.ClassGraph;
import io.github.classgraph.ClassInfo;
import io.github.classgraph.ScanResult;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;
import org.apache.ignite.configuration.annotation.ConfigurationType;
import org.apache.ignite.internal.configuration.ConfigurationManager;
import org.apache.ignite.internal.configuration.storage.TestConfigurationStorage;
import org.apache.ignite.internal.network.NetworkMessagesSerializationRegistryInitializer;
import org.apache.ignite.internal.network.configuration.NetworkConfiguration;
import org.apache.ignite.internal.network.configuration.NodeFinderType;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.network.ClusterLocalConfiguration;
import org.apache.ignite.network.ClusterService;
import org.apache.ignite.network.MessageSerializationRegistryImpl;
import org.apache.ignite.network.MessagingService;
import org.apache.ignite.network.NettyBootstrapFactory;
import org.apache.ignite.network.NetworkAddress;
import org.apache.ignite.network.NodeFinder;
import org.apache.ignite.network.NodeMetadata;
import org.apache.ignite.network.StaticNodeFinder;
import org.apache.ignite.network.TopologyService;
import org.apache.ignite.network.scalecube.TestScaleCubeClusterServiceFactory;
import org.apache.ignite.network.serialization.MessageSerializationRegistry;
import org.apache.ignite.network.serialization.MessageSerializationRegistryInitializer;
import org.junit.jupiter.api.TestInfo;

/**
 * Test utils that provide sort of cluster service mock that manages required node configuration internally.
 */
public class ClusterServiceTestUtils {
    private static final TestScaleCubeClusterServiceFactory SERVICE_FACTORY = new TestScaleCubeClusterServiceFactory();

    /** Registry initializers collected via reflection. */
    private static final List<Method> REGISTRY_INITIALIZERS = collectRegistryInitializers();

    private static List<Method> collectRegistryInitializers() {
        // Automatically find all registry initializers to avoid configuring serialization registry manually in every test.
        String className = MessageSerializationRegistryInitializer.class.getName();

        ClassGraph classGraph = new ClassGraph().acceptPackages("org.apache.ignite").enableClassInfo();

        // ClassGraph library should be in classpath along with ignite-network test-jar
        try (ScanResult scanResult = classGraph.scan()) {
            return scanResult.getClassesImplementing(className).stream()
                    .map(ClassInfo::loadClass)
                    // This class is registered automatically in the MessageSerializationRegistryImpl
                    .filter(clazz -> clazz != NetworkMessagesSerializationRegistryInitializer.class)
                    .map(clazz -> {
                        try {
                            return clazz.getMethod("registerFactories", MessageSerializationRegistry.class);
                        } catch (NoSuchMethodException e) {
                            throw new RuntimeException("Failed to collect MessageSerializationRegistryInitializers", e);
                        }
                    })
                    .collect(toUnmodifiableList());
        }
    }

    /**
     * Creates a {@link MessageSerializationRegistry} that is pre-populated with all {@link MessageSerializationRegistryInitializer}s,
     * accessible through the classpath.
     */
    public static MessageSerializationRegistry defaultSerializationRegistry() {
        var registry = new MessageSerializationRegistryImpl();

        REGISTRY_INITIALIZERS.forEach(method -> {
            try {
                method.invoke(method.getDeclaringClass(), registry);
            } catch (InvocationTargetException | IllegalAccessException e) {
                throw new RuntimeException("Failed to invoke registry initializer", e);
            }
        });

        return registry;
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
        var ctx = new ClusterLocalConfiguration(testNodeName(testInfo, port), defaultSerializationRegistry());

        ConfigurationManager nodeConfigurationMgr = new ConfigurationManager(
                Collections.singleton(NetworkConfiguration.KEY),
                Map.of(),
                new TestConfigurationStorage(ConfigurationType.LOCAL),
                List.of(),
                List.of()
        );

        NetworkConfiguration networkConfiguration = nodeConfigurationMgr.configurationRegistry().getConfiguration(NetworkConfiguration.KEY);

        var bootstrapFactory = new NettyBootstrapFactory(networkConfiguration, ctx.getName());

        ClusterService clusterSvc = SERVICE_FACTORY.createClusterService(ctx, networkConfiguration, bootstrapFactory);

        assert nodeFinder instanceof StaticNodeFinder : "Only StaticNodeFinder is supported at the moment";

        return new ClusterService() {
            @Override
            public TopologyService topologyService() {
                return clusterSvc.topologyService();
            }

            @Override
            public MessagingService messagingService() {
                return clusterSvc.messagingService();
            }

            @Override
            public ClusterLocalConfiguration localConfiguration() {
                return clusterSvc.localConfiguration();
            }

            @Override
            public boolean isStopped() {
                return clusterSvc.isStopped();
            }

            @Override
            public void updateMetadata(NodeMetadata metadata) {
                clusterSvc.updateMetadata(metadata);
            }

            @Override
            public void start() {
                nodeConfigurationMgr.start();

                NetworkConfiguration configuration = nodeConfigurationMgr.configurationRegistry()
                        .getConfiguration(NetworkConfiguration.KEY);

                configuration.change(netCfg ->
                        netCfg
                                .changePort(port)
                                .changeNodeFinder(c -> c
                                        .changeType(NodeFinderType.STATIC.toString())
                                        .changeNetClusterNodes(
                                                nodeFinder.findNodes().stream().map(NetworkAddress::toString).toArray(String[]::new)
                                        )
                                )
                ).join();

                bootstrapFactory.start();

                clusterSvc.start();
            }

            @Override
            public void stop() {
                try {
                    IgniteUtils.closeAll(
                            clusterSvc::stop,
                            bootstrapFactory::stop,
                            nodeConfigurationMgr::stop
                    );
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
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
}
