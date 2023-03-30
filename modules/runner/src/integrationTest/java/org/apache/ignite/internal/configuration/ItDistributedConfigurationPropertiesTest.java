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

import static java.util.stream.Collectors.toUnmodifiableList;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willBe;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;
import org.apache.ignite.configuration.ConfigurationValue;
import org.apache.ignite.configuration.annotation.ConfigurationRoot;
import org.apache.ignite.configuration.annotation.ConfigurationType;
import org.apache.ignite.configuration.annotation.Value;
import org.apache.ignite.internal.cluster.management.ClusterManagementGroupManager;
import org.apache.ignite.internal.cluster.management.DistributedConfigurationUpdater;
import org.apache.ignite.internal.cluster.management.configuration.ClusterManagementConfiguration;
import org.apache.ignite.internal.cluster.management.configuration.NodeAttributesConfiguration;
import org.apache.ignite.internal.cluster.management.raft.TestClusterStateStorage;
import org.apache.ignite.internal.cluster.management.topology.LogicalTopologyImpl;
import org.apache.ignite.internal.cluster.management.topology.LogicalTopologyServiceImpl;
import org.apache.ignite.internal.configuration.storage.ConfigurationStorageListener;
import org.apache.ignite.internal.configuration.storage.Data;
import org.apache.ignite.internal.configuration.storage.DistributedConfigurationStorage;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.metastorage.impl.MetaStorageManagerImpl;
import org.apache.ignite.internal.metastorage.server.SimpleInMemoryKeyValueStorage;
import org.apache.ignite.internal.raft.Loza;
import org.apache.ignite.internal.raft.configuration.RaftConfiguration;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.vault.VaultManager;
import org.apache.ignite.internal.vault.inmemory.InMemoryVaultService;
import org.apache.ignite.lang.NodeStoppingException;
import org.apache.ignite.network.ClusterService;
import org.apache.ignite.network.NetworkAddress;
import org.apache.ignite.network.StaticNodeFinder;
import org.apache.ignite.utils.ClusterServiceTestUtils;
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
public class ItDistributedConfigurationPropertiesTest {
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
    private static SecurityConfiguration securityConfiguration;

    @InjectConfiguration
    private static NodeAttributesConfiguration nodeAttributes;

    /**
     * An emulation of an Ignite node, that only contains components necessary for tests.
     */
    private static class Node {
        private final VaultManager vaultManager;

        private final ClusterService clusterService;

        private final Loza raftManager;

        private final ClusterManagementGroupManager cmgManager;

        private final MetaStorageManager metaStorageManager;

        private final ConfigurationManager distributedCfgManager;

        private final DistributedConfigurationUpdater distributedConfigurationUpdater;

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

            HybridClockImpl clock = new HybridClockImpl();
            raftManager = new Loza(clusterService, raftConfiguration, workDir, clock);

            var clusterStateStorage = new TestClusterStateStorage();
            var logicalTopology = new LogicalTopologyImpl(clusterStateStorage);

            distributedConfigurationUpdater = new DistributedConfigurationUpdater();
            distributedConfigurationUpdater.setClusterRestConfiguration(securityConfiguration);

            cmgManager = new ClusterManagementGroupManager(
                    vaultManager,
                    clusterService,
                    raftManager,
                    clusterStateStorage,
                    logicalTopology,
                    clusterManagementConfiguration,
                    distributedConfigurationUpdater,
                    nodeAttributes
            );

            metaStorageManager = new MetaStorageManagerImpl(
                    vaultManager,
                    clusterService,
                    cmgManager,
                    new LogicalTopologyServiceImpl(logicalTopology, cmgManager),
                    raftManager,
                    new SimpleInMemoryKeyValueStorage(name()),
                    clock
            );

            // create a custom storage implementation that is able to "lose" some storage updates
            var distributedCfgStorage = new DistributedConfigurationStorage(metaStorageManager, vaultManager) {
                /** {@inheritDoc} */
                @Override
                public synchronized void registerConfigurationListener(ConfigurationStorageListener listener) {
                    super.registerConfigurationListener(new ConfigurationStorageListener() {
                        @Override
                        public CompletableFuture<Void> onEntriesChanged(Data changedEntries) {
                            if (receivesUpdates) {
                                return listener.onEntriesChanged(changedEntries);
                            } else {
                                return CompletableFuture.completedFuture(null);
                            }
                        }

                        @Override
                        public CompletableFuture<Void> onRevisionUpdated(long newRevision) {
                            if (receivesUpdates) {
                                return listener.onRevisionUpdated(newRevision);
                            } else {
                                return CompletableFuture.completedFuture(null);
                            }
                        }
                    });
                }
            };

            distributedCfgManager = new ConfigurationManager(
                    List.of(DistributedConfiguration.KEY),
                    Set.of(),
                    distributedCfgStorage,
                    List.of(),
                    List.of()
            );
        }

        /**
         * Starts the created components.
         */
        void start() {
            vaultManager.start();

            Stream.of(clusterService, raftManager, cmgManager, metaStorageManager, distributedConfigurationUpdater)
                    .forEach(IgniteComponent::start);

            // deploy watches to propagate data from the metastore into the vault
            try {
                metaStorageManager.deployWatches();
            } catch (NodeStoppingException e) {
                throw new RuntimeException(e);
            }

            distributedCfgManager.start();
        }

        /**
         * Stops the created components.
         */
        void stop() throws Exception {
            var components = List.of(
                    distributedCfgManager,
                    cmgManager,
                    metaStorageManager,
                    raftManager,
                    clusterService,
                    vaultManager,
                    distributedConfigurationUpdater
            );

            for (IgniteComponent igniteComponent : components) {
                igniteComponent.beforeNodeStop();
            }

            for (IgniteComponent component : components) {
                component.stop();
            }
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

        Stream.of(firstNode, secondNode).parallel().forEach(Node::start);

        firstNode.cmgManager.initCluster(List.of(firstNode.name()), List.of(), "cluster");
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
        assertThat(((ConfigurationValue<String>) secondValue.directProxy()).value(), is("foo"));
        assertThat(secondValue.value(), is("foo"));

        // update the property to a new value and check that the change is propagated to the second node
        CompletableFuture<Void> changeFuture = firstValue.update("bar");

        assertThat(changeFuture, willBe(nullValue(Void.class)));

        assertThat(firstValue.value(), is("bar"));
        assertThat(((ConfigurationValue<String>) secondValue.directProxy()).value(), is("bar"));
        assertTrue(waitForCondition(() -> "bar".equals(secondValue.value()), 1000));

        // disable storage updates on the second node. This way the new values will never be propagated into the
        // configuration storage
        secondNode.stopReceivingUpdates();

        // update the property and check that only the "direct" value of the second property reflects the latest
        // state
        changeFuture = firstValue.update("baz");

        assertThat(changeFuture, willBe(nullValue(Void.class)));

        assertThat(firstValue.value(), is("baz"));
        assertThat(((ConfigurationValue<String>) secondValue.directProxy()).value(), is("baz"));
        assertFalse(waitForCondition(() -> "baz".equals(secondValue.value()), 100));
    }
}
