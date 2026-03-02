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

package org.apache.ignite.internal.configuration.storage;

import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willBe;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.apache.ignite.internal.util.IgniteUtils.startAsync;
import static org.apache.ignite.internal.util.IgniteUtils.stopAsync;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.Serializable;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.cluster.management.ClusterIdHolder;
import org.apache.ignite.internal.cluster.management.ClusterInitializer;
import org.apache.ignite.internal.cluster.management.ClusterManagementGroupManager;
import org.apache.ignite.internal.cluster.management.NodeAttributesCollector;
import org.apache.ignite.internal.cluster.management.configuration.NodeAttributesConfiguration;
import org.apache.ignite.internal.cluster.management.raft.TestClusterStateStorage;
import org.apache.ignite.internal.cluster.management.topology.LogicalTopologyImpl;
import org.apache.ignite.internal.cluster.management.topology.LogicalTopologyServiceImpl;
import org.apache.ignite.internal.configuration.ComponentWorkingDir;
import org.apache.ignite.internal.configuration.RaftGroupOptionsConfigHelper;
import org.apache.ignite.internal.configuration.SystemDistributedConfiguration;
import org.apache.ignite.internal.configuration.SystemLocalConfiguration;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.configuration.validation.TestConfigurationValidator;
import org.apache.ignite.internal.disaster.system.SystemDisasterRecoveryStorage;
import org.apache.ignite.internal.failure.FailureManager;
import org.apache.ignite.internal.failure.NoOpFailureManager;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.manager.ComponentContext;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.metastorage.impl.MetaStorageManagerImpl;
import org.apache.ignite.internal.metastorage.server.ReadOperationForCompactionTracker;
import org.apache.ignite.internal.metastorage.server.SimpleInMemoryKeyValueStorage;
import org.apache.ignite.internal.metrics.MetricManager;
import org.apache.ignite.internal.metrics.NoOpMetricManager;
import org.apache.ignite.internal.network.ClusterService;
import org.apache.ignite.internal.network.StaticNodeFinder;
import org.apache.ignite.internal.network.utils.ClusterServiceTestUtils;
import org.apache.ignite.internal.raft.Loza;
import org.apache.ignite.internal.raft.RaftGroupOptionsConfigurer;
import org.apache.ignite.internal.raft.TestLozaFactory;
import org.apache.ignite.internal.raft.client.TopologyAwareRaftGroupServiceFactory;
import org.apache.ignite.internal.raft.configuration.RaftConfiguration;
import org.apache.ignite.internal.raft.storage.LogStorageManager;
import org.apache.ignite.internal.raft.util.SharedLogStorageManagerUtils;
import org.apache.ignite.internal.storage.configurations.StorageConfiguration;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.apache.ignite.internal.vault.VaultManager;
import org.apache.ignite.internal.vault.persistence.PersistentVaultService;
import org.apache.ignite.network.NetworkAddress;
import org.apache.ignite.raft.jraft.rpc.impl.RaftGroupEventsClientListener;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Tests for the {@link DistributedConfigurationStorage}.
 */
@ExtendWith(WorkDirectoryExtension.class)
@ExtendWith(ConfigurationExtension.class)
public class ItDistributedConfigurationStorageTest extends BaseIgniteAbstractTest {
    @InjectConfiguration
    private static RaftConfiguration raftConfiguration;

    @InjectConfiguration
    private static NodeAttributesConfiguration nodeAttributes;

    @InjectConfiguration
    private static StorageConfiguration storageConfiguration;

    @InjectConfiguration
    private static SystemLocalConfiguration systemLocalConfiguration;

    @InjectConfiguration
    private static SystemDistributedConfiguration systemDistributedConfiguration;

    /**
     * An emulation of an Ignite node, that only contains components necessary for tests.
     */
    private static class Node {
        private final VaultManager vaultManager;

        private final ClusterService clusterService;

        private final ClusterManagementGroupManager cmgManager;

        private final Loza raftManager;

        private final LogStorageManager partitionsLogStorageManager;

        private final LogStorageManager cmgLogStorageManager;

        private final LogStorageManager msLogStorageManager;

        private final MetaStorageManager metaStorageManager;

        private final DistributedConfigurationStorage cfgStorage;

        /** The future have to be complete after the node start and all Meta storage watches are deployd. */
        private final CompletableFuture<Void> deployWatchesFut;

        private final FailureManager failureManager;

        /**
         * Constructor that simply creates a subset of components of this node.
         */
        Node(TestInfo testInfo, Path workDir) {
            var addr = new NetworkAddress("localhost", 10000);

            vaultManager = new VaultManager(new PersistentVaultService(workDir.resolve("vault")));

            clusterService = ClusterServiceTestUtils.clusterService(
                    testInfo,
                    addr.port(),
                    new StaticNodeFinder(List.of(addr))
            );

            HybridClock clock = new HybridClockImpl();

            var raftGroupEventsClientListener = new RaftGroupEventsClientListener();

            ComponentWorkingDir workingDir = new ComponentWorkingDir(workDir);

            partitionsLogStorageManager = SharedLogStorageManagerUtils.create(
                    clusterService.nodeName(),
                    workingDir.raftLogPath()
            );

            raftManager = TestLozaFactory.create(
                    clusterService,
                    raftConfiguration,
                    systemLocalConfiguration,
                    clock,
                    raftGroupEventsClientListener
            );

            this.failureManager = new NoOpFailureManager();

            var clusterStateStorage = new TestClusterStateStorage();
            var logicalTopology = new LogicalTopologyImpl(clusterStateStorage, failureManager);

            var clusterInitializer = new ClusterInitializer(
                    clusterService,
                    hocon -> hocon,
                    new TestConfigurationValidator()
            );

            ComponentWorkingDir cmgWorkDir = new ComponentWorkingDir(workDir.resolve("cmg"));

            cmgLogStorageManager =
                    SharedLogStorageManagerUtils.create(clusterService.nodeName(), cmgWorkDir.raftLogPath());

            RaftGroupOptionsConfigurer cmgRaftConfigurer =
                    RaftGroupOptionsConfigHelper.configureProperties(cmgLogStorageManager, cmgWorkDir.metaPath());

            MetricManager metricManager = new NoOpMetricManager();

            cmgManager = new ClusterManagementGroupManager(
                    vaultManager,
                    new SystemDisasterRecoveryStorage(vaultManager),
                    clusterService,
                    clusterInitializer,
                    raftManager,
                    clusterStateStorage,
                    logicalTopology,
                    new NodeAttributesCollector(nodeAttributes, storageConfiguration),
                    failureManager,
                    new ClusterIdHolder(),
                    cmgRaftConfigurer,
                    metricManager
            );

            var logicalTopologyService = new LogicalTopologyServiceImpl(logicalTopology, cmgManager);

            var topologyAwareRaftGroupServiceFactory = new TopologyAwareRaftGroupServiceFactory(
                    clusterService,
                    logicalTopologyService,
                    Loza.FACTORY,
                    raftGroupEventsClientListener
            );

            ComponentWorkingDir metastorageWorkDir = new ComponentWorkingDir(workDir.resolve("metastorage"));

            msLogStorageManager =
                    SharedLogStorageManagerUtils.create(clusterService.nodeName(), metastorageWorkDir.raftLogPath());

            RaftGroupOptionsConfigurer msRaftConfigurer =
                    RaftGroupOptionsConfigHelper.configureProperties(msLogStorageManager, metastorageWorkDir.metaPath());

            var readOperationForCompactionTracker = new ReadOperationForCompactionTracker();

            metaStorageManager = new MetaStorageManagerImpl(
                    clusterService,
                    cmgManager,
                    logicalTopologyService,
                    raftManager,
                    new SimpleInMemoryKeyValueStorage(name(), readOperationForCompactionTracker),
                    clock,
                    topologyAwareRaftGroupServiceFactory,
                    metricManager,
                    systemDistributedConfiguration,
                    msRaftConfigurer,
                    readOperationForCompactionTracker
            );

            deployWatchesFut = metaStorageManager.deployWatches();

            cfgStorage = new DistributedConfigurationStorage(name(), metaStorageManager);
        }

        /**
         * Starts the created components.
         */
        void startUpToCmgManager() {
            assertThat(
                    startAsync(new ComponentContext(),
                            vaultManager,
                            clusterService,
                            partitionsLogStorageManager,
                            cmgLogStorageManager,
                            msLogStorageManager,
                            raftManager,
                            failureManager,
                            cmgManager
                    ),
                    willCompleteSuccessfully()
            );
        }

        /**
         * Starts the created components.
         */
        void startComponentsAfterCmgManager() {
            assertThat(
                    startAsync(new ComponentContext(), metaStorageManager),
                    willCompleteSuccessfully()
            );

            // this is needed to avoid assertion errors
            cfgStorage.registerConfigurationListener(changedEntries -> nullCompletedFuture());
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
            var components =
                    List.of(
                            metaStorageManager,
                            cmgManager,
                            failureManager,
                            raftManager,
                            partitionsLogStorageManager,
                            cmgLogStorageManager,
                            msLogStorageManager,
                            clusterService,
                            vaultManager
                    );

            for (IgniteComponent igniteComponent : components) {
                igniteComponent.beforeNodeStop();
            }

            assertThat(stopAsync(new ComponentContext(), components), willCompleteSuccessfully());
        }

        String name() {
            return clusterService.nodeName();
        }
    }

    /**
     * Tests a scenario when a node is restarted with an existing PDS folder. A node is started and some data is written to the distributed
     * configuration storage. We then expect that the same data can be read by the node after restart.
     *
     * @see <a href="https://issues.apache.org/jira/browse/IGNITE-15213">IGNITE-15213</a>
     */
    @Test
    void testRestartWithPds(@WorkDirectory Path workDir, TestInfo testInfo) throws Exception {
        var node = new Node(testInfo, workDir);

        Map<String, Serializable> data = Map.of("foo", "bar");

        try {
            node.startUpToCmgManager();

            node.cmgManager.initCluster(List.of(node.name()), List.of(), "cluster");

            node.startComponentsAfterCmgManager();

            node.waitWatches();

            assertThat(node.cfgStorage.write(data, 0), willBe(equalTo(true)));

            assertTrue(waitForCondition(
                    () -> node.metaStorageManager.appliedRevision() != 0,
                    3000
            ));
        } finally {
            node.stop();
        }

        var node2 = new Node(testInfo, workDir);

        try {
            node2.startUpToCmgManager();
            node2.startComponentsAfterCmgManager();

            node2.waitWatches();

            CompletableFuture<Data> storageData = node2.cfgStorage.readDataOnRecovery();

            assertThat(storageData.thenApply(Data::values), willBe(equalTo(data)));
        } finally {
            node2.stop();
        }
    }
}
