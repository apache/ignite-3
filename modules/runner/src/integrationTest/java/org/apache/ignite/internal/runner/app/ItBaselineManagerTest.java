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

package org.apache.ignite.internal.runner.app;

import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toUnmodifiableList;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.testNodeName;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.array;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;
import org.apache.ignite.configuration.notifications.ConfigurationListener;
import org.apache.ignite.configuration.schemas.runner.ClusterConfiguration;
import org.apache.ignite.configuration.schemas.runner.ClusterView;
import org.apache.ignite.configuration.schemas.runner.NodeConfiguration;
import org.apache.ignite.internal.baseline.BaselineManager;
import org.apache.ignite.internal.configuration.ConfigurationManager;
import org.apache.ignite.internal.configuration.storage.DistributedConfigurationStorage;
import org.apache.ignite.internal.configuration.storage.LocalConfigurationStorage;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.metastorage.server.SimpleInMemoryKeyValueStorage;
import org.apache.ignite.internal.raft.Loza;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.vault.VaultManager;
import org.apache.ignite.internal.vault.inmemory.InMemoryVaultService;
import org.apache.ignite.lang.NodeStoppingException;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.ClusterService;
import org.apache.ignite.network.NetworkAddress;
import org.apache.ignite.network.StaticNodeFinder;
import org.apache.ignite.network.scalecube.TestScaleCubeClusterServiceFactory;
import org.apache.ignite.utils.ClusterServiceTestUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Tests basic functionality of the {@link BaselineManager}.
 */
@ExtendWith(WorkDirectoryExtension.class)
public class ItBaselineManagerTest {
    /**
     * An emulation of an Ignite node, that only contains components necessary for tests.
     */
    private static class Node {
        private final List<String> metaStorageNodes;

        private final VaultManager vaultManager;

        private final ClusterService clusterService;

        private final Loza raftManager;

        private final ConfigurationManager cfgManager;

        private final MetaStorageManager metaStorageManager;

        private final ConfigurationManager distributedCfgManager;

        private final BaselineManager baselineManager;

        /**
         * Constructor that simply creates a subset of components of this node.
         */
        Node(
                TestInfo testInfo,
                Path workDir,
                NetworkAddress addr,
                List<NetworkAddress> memberAddrs,
                List<String> metaStorageNodes
        ) {
            this.metaStorageNodes = metaStorageNodes;

            vaultManager = new VaultManager(new InMemoryVaultService());

            clusterService = ClusterServiceTestUtils.clusterService(
                    testInfo,
                    addr.port(),
                    new StaticNodeFinder(memberAddrs),
                    new TestScaleCubeClusterServiceFactory()
            );

            raftManager = new Loza(clusterService, workDir);

            cfgManager = new ConfigurationManager(
                    List.of(NodeConfiguration.KEY),
                    Map.of(),
                    new LocalConfigurationStorage(vaultManager),
                    List.of(),
                    List.of()
            );

            metaStorageManager = new MetaStorageManager(
                    vaultManager,
                    cfgManager,
                    clusterService,
                    raftManager,
                    new SimpleInMemoryKeyValueStorage()
            );

            var distributedCfgStorage = new DistributedConfigurationStorage(metaStorageManager, vaultManager);

            distributedCfgManager = new ConfigurationManager(
                    List.of(ClusterConfiguration.KEY),
                    Map.of(),
                    distributedCfgStorage,
                    List.of(),
                    List.of()
            );

            baselineManager = new BaselineManager(
                    distributedCfgManager.configurationRegistry().getConfiguration(ClusterConfiguration.KEY), clusterService);
        }

        /**
         * Starts the created components.
         */
        void start() throws Exception {
            vaultManager.start();

            cfgManager.start();

            // metastorage configuration
            String metaStorageCfg = metaStorageNodes.stream()
                    .map(Object::toString)
                    .collect(joining("\", \"", "\"", "\""));

            var config = String.format("{ node: { metastorageNodes : [ %s ] } }", metaStorageCfg);

            cfgManager.bootstrap(config);

            Stream.of(clusterService, raftManager, metaStorageManager)
                    .forEach(IgniteComponent::start);

            // deploy watches to propagate data from the metastore into the vault
            metaStorageManager.deployWatches();

            distributedCfgManager.start();

            distributedCfgManager.configurationRegistry().initializeDefaults();

            baselineManager.start();
        }

        /**
         * Stops the created components.
         */
        void stop() throws Exception {
            var components = List.of(
                    baselineManager, distributedCfgManager, metaStorageManager, raftManager, clusterService, cfgManager, vaultManager
            );

            for (IgniteComponent igniteComponent : components) {
                igniteComponent.beforeNodeStop();
            }

            for (IgniteComponent component : components) {
                component.stop();
            }
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

        String firstNodeName = testNodeName(testInfo, firstNodeAddr.port());

        var secondNodeAddr = new NetworkAddress("localhost", 10001);

        firstNode = new ItBaselineManagerTest.Node(
                testInfo,
                workDir.resolve("firstNode"),
                firstNodeAddr,
                List.of(firstNodeAddr, secondNodeAddr),
                List.of(firstNodeName)
        );

        secondNode = new ItBaselineManagerTest.Node(
                testInfo,
                workDir.resolve("secondNode"),
                secondNodeAddr,
                List.of(firstNodeAddr, secondNodeAddr),
                List.of(firstNodeName)
        );

        firstNode.start();
        secondNode.start();
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
     * Checks {@link BaselineManager#setBaseline(java.util.Set)} functionality.
     */
    @Test
    void testSetBaseline() throws Exception {
        assertThat(firstNode.baselineManager.baselineNodes(), is(empty()));

        assertThat(secondNode.baselineManager.baselineNodes(), is(empty()));

        // set firstNode as a set of baseline nodes
        ClusterNode firstClusterNode = firstNode.clusterService.topologyService().localMember();
        firstNode.baselineManager.setBaseline(Set.of(firstClusterNode.name()));

        assertThat(firstNode.baselineManager.baselineNodes(), containsInAnyOrder(firstClusterNode));

        // check that new baseline is available on a new node.
        assertTrue(waitForCondition(() -> {
            try {
                return secondNode.baselineManager.baselineNodes().equals(List.of(firstClusterNode));
            } catch (NodeStoppingException e) {
                fail();
            }

            return false;
        }, 10_000));
    }

    /**
     * Checks {@link BaselineManager#listenBaselineChange(ConfigurationListener)} functionality.
     */
    @Test
    void testBaselineListener() throws Exception {
        assertThat(firstNode.baselineManager.baselineNodes(), is(empty()));

        assertThat(secondNode.baselineManager.baselineNodes(), is(empty()));

        ClusterNode firstClusterNode = firstNode.clusterService.topologyService().localMember();

        AtomicInteger listenerCounter = new AtomicInteger(0);

        ConfigurationListener<ClusterView> baselineListener = ctx -> {
            listenerCounter.incrementAndGet();

            assertThat(ctx.newValue(), is(notNullValue()));

            assertThat(ctx.newValue().baselineNodes(), array(equalTo(firstClusterNode.name())));

            return CompletableFuture.completedFuture(null);
        };

        // set firstNode as a set of baseline nodes, register listners on baseline change.
        firstNode.baselineManager.listenBaselineChange(baselineListener);
        secondNode.baselineManager.listenBaselineChange(baselineListener);

        firstNode.baselineManager.setBaseline(Set.of(firstClusterNode.name()));

        // check that new baseline is available on a second node.
        assertTrue(waitForCondition(() -> listenerCounter.get() == 2, 10_000));
    }
}
