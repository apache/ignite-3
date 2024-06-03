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

package org.apache.ignite.internal.configuration;

import static java.util.concurrent.CompletableFuture.allOf;
import static java.util.stream.Collectors.toUnmodifiableList;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willBe;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.apache.ignite.internal.util.IgniteUtils.startAsync;
import static org.apache.ignite.internal.util.IgniteUtils.stopAsync;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;
import org.apache.ignite.configuration.ConfigurationValue;
import org.apache.ignite.configuration.annotation.ConfigurationRoot;
import org.apache.ignite.configuration.annotation.ConfigurationType;
import org.apache.ignite.configuration.annotation.Value;
import org.apache.ignite.internal.cluster.management.ClusterInitializer;
import org.apache.ignite.internal.cluster.management.ClusterManagementGroupManager;
import org.apache.ignite.internal.cluster.management.NodeAttributesCollector;
import org.apache.ignite.internal.cluster.management.configuration.ClusterManagementConfiguration;
import org.apache.ignite.internal.cluster.management.configuration.NodeAttributesConfiguration;
import org.apache.ignite.internal.cluster.management.raft.TestClusterStateStorage;
import org.apache.ignite.internal.cluster.management.topology.LogicalTopologyImpl;
import org.apache.ignite.internal.cluster.management.topology.LogicalTopologyServiceImpl;
import org.apache.ignite.internal.configuration.storage.ConfigurationStorageListener;
import org.apache.ignite.internal.configuration.storage.DistributedConfigurationStorage;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.configuration.validation.TestConfigurationValidator;
import org.apache.ignite.internal.failure.FailureProcessor;
import org.apache.ignite.internal.failure.NoOpFailureProcessor;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.manager.ComponentContext;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.metastorage.configuration.MetaStorageConfiguration;
import org.apache.ignite.internal.metastorage.impl.MetaStorageManagerImpl;
import org.apache.ignite.internal.metastorage.server.SimpleInMemoryKeyValueStorage;
import org.apache.ignite.internal.metrics.NoOpMetricManager;
import org.apache.ignite.internal.network.ClusterService;
import org.apache.ignite.internal.network.StaticNodeFinder;
import org.apache.ignite.internal.network.utils.ClusterServiceTestUtils;
import org.apache.ignite.internal.raft.Loza;
import org.apache.ignite.internal.raft.TestLozaFactory;
import org.apache.ignite.internal.raft.client.TopologyAwareRaftGroupServiceFactory;
import org.apache.ignite.internal.raft.configuration.RaftConfiguration;
import org.apache.ignite.internal.storage.configurations.StorageConfiguration;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.vault.VaultManager;
import org.apache.ignite.internal.vault.inmemory.InMemoryVaultService;
import org.apache.ignite.network.NetworkAddress;
import org.apache.ignite.raft.jraft.rpc.impl.RaftGroupEventsClientListener;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Integration test for checking different aspects of distributed configuration.
 */
@ExtendWith(WorkDirectoryExtension.class)
@ExtendWith(ConfigurationExtension.class)
public class ItDistributedConfigurationPropertiesTest extends BaseIgniteAbstractTest {
    /** Test distributed configuration schema. */
    @ConfigurationRoot(rootName = "root", type = ConfigurationType.DISTRIBUTED)
    public static class DistributedConfigurationSchema {
        @Value(hasDefault = true)
        public String str = "foo";
    }

    @InjectConfiguration
    private static RaftConfiguration raftConfiguration;

    @InjectConfiguration
    private static ClusterManagementConfiguration clusterManagementConfiguration;

    @InjectConfiguration
    private static NodeAttributesConfiguration nodeAttributes;

    @InjectConfiguration
    private static StorageConfiguration storageConfiguration;

    @InjectConfiguration
    private static MetaStorageConfiguration metaStorageConfiguration;

    /**
     * An emulation of an Ignite node, that only contains components necessary for tests.
     */
    private static class Node {
        private final VaultManager vaultManager;

        private final ClusterService clusterService;

        private final Loza raftManager;

        private final ClusterManagementGroupManager cmgManager;

        private final MetaStorageManager metaStorageManager;

        private final ConfigurationTreeGenerator generator;

        private final ConfigurationManager distributedCfgManager;

        /** The future have to be complete after the node start and all Meta storage watches are deployd. */
        private final CompletableFuture<Void> deployWatchesFut;

        private final FailureProcessor failureProcessor;

        /** Flag that disables storage updates. */
        private volatile boolean receivesUpdates = true;

        /**
         * Constructor that simply creates a subset of components of this node.
         */
        Node(
                TestInfo testInfo,
                Path workDir,
                NetworkAddress addr,
                List<NetworkAddress> memberAddrs,
                RaftConfiguration raftConfiguration
        ) {
            vaultManager = new VaultManager(new InMemoryVaultService());

            clusterService = ClusterServiceTestUtils.clusterService(
                    testInfo,
                    addr.port(),
                    new StaticNodeFinder(memberAddrs)
            );

            HybridClock clock = new HybridClockImpl();

            var raftGroupEventsClientListener = new RaftGroupEventsClientListener();

            raftManager = TestLozaFactory.create(
                    clusterService,
                    raftConfiguration,
                    workDir, clock,
                    raftGroupEventsClientListener
            );

            var clusterStateStorage = new TestClusterStateStorage();
            var logicalTopology = new LogicalTopologyImpl(clusterStateStorage);

            var clusterInitializer = new ClusterInitializer(
                    clusterService,
                    hocon -> hocon,
                    new TestConfigurationValidator()
            );

            this.failureProcessor = new NoOpFailureProcessor(clusterService.nodeName());

            cmgManager = new ClusterManagementGroupManager(
                    vaultManager,
                    clusterService,
                    clusterInitializer,
                    raftManager,
                    clusterStateStorage,
                    logicalTopology,
                    clusterManagementConfiguration,
                    new NodeAttributesCollector(nodeAttributes, storageConfiguration),
                    failureProcessor
            );

            var logicalTopologyService = new LogicalTopologyServiceImpl(logicalTopology, cmgManager);

            var topologyAwareRaftGroupServiceFactory = new TopologyAwareRaftGroupServiceFactory(
                    clusterService,
                    logicalTopologyService,
                    Loza.FACTORY,
                    raftGroupEventsClientListener
            );

            metaStorageManager = new MetaStorageManagerImpl(
                    clusterService,
                    cmgManager,
                    logicalTopologyService,
                    raftManager,
                    new SimpleInMemoryKeyValueStorage(name()),
                    clock,
                    topologyAwareRaftGroupServiceFactory,
                    new NoOpMetricManager(),
                    metaStorageConfiguration
            );

            deployWatchesFut = metaStorageManager.deployWatches();

            // create a custom storage implementation that is able to "lose" some storage updates
            var distributedCfgStorage = new DistributedConfigurationStorage("test", metaStorageManager) {
                /** {@inheritDoc} */
                @Override
                public synchronized void registerConfigurationListener(ConfigurationStorageListener listener) {
                    super.registerConfigurationListener(changedEntries -> {
                        if (receivesUpdates) {
                            return listener.onEntriesChanged(changedEntries);
                        } else {
                            return nullCompletedFuture();
                        }
                    });
                }
            };

            generator = new ConfigurationTreeGenerator(DistributedConfiguration.KEY);
            distributedCfgManager = new ConfigurationManager(
                    List.of(DistributedConfiguration.KEY),
                    distributedCfgStorage,
                    generator,
                    new TestConfigurationValidator()
            );
        }

        /**
         * Starts the created components.
         */
        CompletableFuture<Void> start() {
            assertThat(
                    startAsync(new ComponentContext(),
                            vaultManager, clusterService, raftManager, failureProcessor, cmgManager, metaStorageManager),
                    willCompleteSuccessfully()
            );

            return CompletableFuture.runAsync(() ->
                    assertThat(distributedCfgManager.startAsync(new ComponentContext()), willCompleteSuccessfully())
            );
        }

        /**
         * Waits for watches deployed.
         */
        void waitWatches() {
            assertThat("Watches were not deployed", deployWatchesFut, willCompleteSuccessfully());
        }

        /**
         * Stops the created components.
         */
        void stop() {
            var components = List.of(
                    distributedCfgManager,
                    cmgManager,
                    failureProcessor,
                    metaStorageManager,
                    raftManager,
                    clusterService,
                    vaultManager
            );

            for (IgniteComponent igniteComponent : components) {
                igniteComponent.beforeNodeStop();
            }

            assertThat(stopAsync(new ComponentContext(), components), willCompleteSuccessfully());

            generator.close();
        }

        /**
         * Disables the propagation of storage events on this node.
         */
        void stopReceivingUpdates() {
            receivesUpdates = false;
        }

        String name() {
            return clusterService.nodeName();
        }
    }

    private Node firstNode;

    private Node secondNode;

    /**
     * Before each.
     */
    @BeforeEach
    void setUp(@WorkDirectory Path workDir, TestInfo testInfo) throws Exception {
        var firstNodeAddr = new NetworkAddress("localhost", 10000);

        var secondNodeAddr = new NetworkAddress("localhost", 10001);

        List<NetworkAddress> allNodes = List.of(firstNodeAddr, secondNodeAddr);

        firstNode = new Node(
                testInfo,
                workDir.resolve("firstNode"),
                firstNodeAddr,
                allNodes,
                raftConfiguration
        );

        secondNode = new Node(
                testInfo,
                workDir.resolve("secondNode"),
                secondNodeAddr,
                allNodes,
                raftConfiguration
        );

        CompletableFuture<?>[] startFutures = Stream.of(firstNode, secondNode).parallel().map(Node::start)
                .toArray(CompletableFuture[]::new);

        firstNode.cmgManager.initCluster(List.of(firstNode.name()), List.of(), "cluster");

        assertThat(allOf(startFutures), willCompleteSuccessfully());

        Stream.of(firstNode, secondNode).parallel().forEach(Node::waitWatches);
    }

    /**
     * After each.
     */
    @AfterEach
    void tearDown() throws Exception {
        List<AutoCloseable> closeables = Stream.of(firstNode, secondNode)
                .filter(Objects::nonNull)
                .map(n -> (AutoCloseable) n::stop)
                .collect(toUnmodifiableList());

        IgniteUtils.closeAll(closeables);
    }

    /**
     * Tests a scenario when a distributed property is lagging behind the latest value (e.g. due to network delays. storage listeners logic,
     * etc.). In this case the "direct" value should always be in the up-to-date state.
     */
    @Test
    public void testFallingBehindDistributedStorageValue() throws Exception {
        ConfigurationValue<String> firstValue = firstNode.distributedCfgManager.configurationRegistry()
                .getConfiguration(DistributedConfiguration.KEY)
                .str();

        ConfigurationValue<String> secondValue = secondNode.distributedCfgManager.configurationRegistry()
                .getConfiguration(DistributedConfiguration.KEY)
                .str();

        // check initial values
        assertThat(firstValue.value(), is("foo"));
        assertThat(secondValue.directProxy().value(), is("foo"));
        assertThat(secondValue.value(), is("foo"));

        // update the property to a new value and check that the change is propagated to the second node
        CompletableFuture<Void> changeFuture = firstValue.update("bar");

        assertThat(changeFuture, willBe(nullValue(Void.class)));

        assertThat(firstValue.value(), is("bar"));
        assertThat(secondValue.directProxy().value(), is("bar"));
        assertTrue(waitForCondition(() -> "bar".equals(secondValue.value()), 1000));

        // disable storage updates on the second node. This way the new values will never be propagated into the
        // configuration storage
        secondNode.stopReceivingUpdates();

        // update the property and check that only the "direct" value of the second property reflects the latest
        // state
        changeFuture = firstValue.update("baz");

        assertThat(changeFuture, willBe(nullValue(Void.class)));

        assertThat(firstValue.value(), is("baz"));
        assertThat(secondValue.directProxy().value(), is("baz"));
        assertFalse(waitForCondition(() -> "baz".equals(secondValue.value()), 100));
    }
}
