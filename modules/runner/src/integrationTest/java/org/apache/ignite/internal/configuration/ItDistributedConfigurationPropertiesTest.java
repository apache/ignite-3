/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
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

package org.apache.ignite.internal.configuration;

import static java.util.stream.Collectors.toUnmodifiableList;
import static org.apache.ignite.internal.configuration.util.ConfigurationUtil.directProxy;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willBe;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;
import org.apache.ignite.configuration.ConfigurationValue;
import org.apache.ignite.configuration.annotation.ConfigurationRoot;
import org.apache.ignite.configuration.annotation.ConfigurationType;
import org.apache.ignite.configuration.annotation.Value;
import org.apache.ignite.internal.cluster.management.ClusterManagementGroupManager;
import org.apache.ignite.internal.cluster.management.raft.ConcurrentMapClusterStateStorage;
import org.apache.ignite.internal.configuration.storage.ConfigurationStorageListener;
import org.apache.ignite.internal.configuration.storage.DistributedConfigurationStorage;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.metastorage.server.SimpleInMemoryKeyValueStorage;
import org.apache.ignite.internal.raft.Loza;
import org.apache.ignite.internal.rest.RestComponent;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.vault.VaultManager;
import org.apache.ignite.internal.vault.inmemory.InMemoryVaultService;
import org.apache.ignite.network.ClusterService;
import org.apache.ignite.network.NetworkAddress;
import org.apache.ignite.network.StaticNodeFinder;
import org.apache.ignite.utils.ClusterServiceTestUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Integration test for checking different aspects of distributed configuration.
 */
@ExtendWith(WorkDirectoryExtension.class)
public class ItDistributedConfigurationPropertiesTest {
    /** Test distributed configuration schema. */
    @ConfigurationRoot(rootName = "root", type = ConfigurationType.DISTRIBUTED)
    public static class DistributedConfigurationSchema {
        @Value(hasDefault = true)
        public String str = "foo";
    }

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

        /** Flag that disables storage updates. */
        private volatile boolean receivesUpdates = true;

        /**
         * Constructor that simply creates a subset of components of this node.
         */
        Node(
                TestInfo testInfo,
                Path workDir,
                NetworkAddress addr,
                List<NetworkAddress> memberAddrs
        ) {
            vaultManager = new VaultManager(new InMemoryVaultService());

            clusterService = ClusterServiceTestUtils.clusterService(
                    testInfo,
                    addr.port(),
                    new StaticNodeFinder(memberAddrs)
            );

            raftManager = new Loza(clusterService, workDir);

            cmgManager = new ClusterManagementGroupManager(
                    vaultManager,
                    clusterService,
                    raftManager,
                    mock(RestComponent.class),
                    new ConcurrentMapClusterStateStorage()
            );

            metaStorageManager = new MetaStorageManager(
                    vaultManager,
                    clusterService,
                    cmgManager,
                    raftManager,
                    new SimpleInMemoryKeyValueStorage()
            );

            // create a custom storage implementation that is able to "lose" some storage updates
            var distributedCfgStorage = new DistributedConfigurationStorage(metaStorageManager, vaultManager) {
                /** {@inheritDoc} */
                @Override
                public synchronized void registerConfigurationListener(@NotNull ConfigurationStorageListener listener) {
                    super.registerConfigurationListener(changedEntries -> {
                        if (receivesUpdates) {
                            return listener.onEntriesChanged(changedEntries);
                        } else {
                            return CompletableFuture.completedFuture(null);
                        }
                    });
                }
            };

            distributedCfgManager = new ConfigurationManager(
                    List.of(DistributedConfiguration.KEY),
                    Map.of(),
                    distributedCfgStorage,
                    List.of(),
                    List.of()
            );
        }

        /**
         * Starts the created components.
         */
        void start() throws Exception {
            vaultManager.start();

            Stream.of(clusterService, raftManager, cmgManager, metaStorageManager)
                    .forEach(IgniteComponent::start);

            // deploy watches to propagate data from the metastore into the vault
            metaStorageManager.deployWatches();

            distributedCfgManager.start();
        }

        /**
         * Stops the created components.
         */
        void stop() throws Exception {
            var components = List.of(
                    distributedCfgManager, cmgManager, metaStorageManager, raftManager, clusterService, vaultManager
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
            return clusterService.topologyService().localMember().name();
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
                allNodes
        );

        secondNode = new Node(
                testInfo,
                workDir.resolve("secondNode"),
                secondNodeAddr,
                allNodes
        );

        firstNode.start();
        secondNode.start();

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
        assertThat(directProxy(secondValue).value(), is("foo"));
        assertThat(secondValue.value(), is("foo"));

        // update the property to a new value and check that the change is propagated to the second node
        CompletableFuture<Void> changeFuture = firstValue.update("bar");

        assertThat(changeFuture, willBe(nullValue(Void.class)));

        assertThat(firstValue.value(), is("bar"));
        assertThat(directProxy(secondValue).value(), is("bar"));
        assertTrue(waitForCondition(() -> "bar".equals(secondValue.value()), 100));

        // disable storage updates on the second node. This way the new values will never be propagated into the
        // configuration storage
        secondNode.stopReceivingUpdates();

        // update the property and check that only the "direct" value of the second property reflects the latest
        // state
        changeFuture = firstValue.update("baz");

        assertThat(changeFuture, willBe(nullValue(Void.class)));

        assertThat(firstValue.value(), is("baz"));
        assertThat(directProxy(secondValue).value(), is("baz"));
        assertFalse(waitForCondition(() -> "baz".equals(secondValue.value()), 100));
    }
}
