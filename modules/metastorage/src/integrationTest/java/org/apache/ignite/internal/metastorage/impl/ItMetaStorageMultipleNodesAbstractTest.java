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

package org.apache.ignite.internal.metastorage.impl;

import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toSet;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.util.IgniteUtils.closeAll;
import static org.apache.ignite.internal.util.IgniteUtils.startAsync;
import static org.apache.ignite.internal.util.IgniteUtils.stopAsync;
import static org.hamcrest.MatcherAssert.assertThat;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;
import org.apache.ignite.internal.cluster.management.ClusterIdHolder;
import org.apache.ignite.internal.cluster.management.ClusterInitializer;
import org.apache.ignite.internal.cluster.management.ClusterManagementGroupManager;
import org.apache.ignite.internal.cluster.management.NodeAttributesCollector;
import org.apache.ignite.internal.cluster.management.configuration.NodeAttributesConfiguration;
import org.apache.ignite.internal.cluster.management.raft.ClusterStateStorage;
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
import org.apache.ignite.internal.metastorage.server.KeyValueStorage;
import org.apache.ignite.internal.metastorage.server.ReadOperationForCompactionTracker;
import org.apache.ignite.internal.metastorage.server.SimpleInMemoryKeyValueStorage;
import org.apache.ignite.internal.metrics.NoOpMetricManager;
import org.apache.ignite.internal.network.ClusterService;
import org.apache.ignite.internal.network.StaticNodeFinder;
import org.apache.ignite.internal.network.utils.ClusterServiceTestUtils;
import org.apache.ignite.internal.raft.Loza;
import org.apache.ignite.internal.raft.Peer;
import org.apache.ignite.internal.raft.RaftGroupOptionsConfigurer;
import org.apache.ignite.internal.raft.TestLozaFactory;
import org.apache.ignite.internal.raft.client.TopologyAwareRaftGroupServiceFactory;
import org.apache.ignite.internal.raft.configuration.RaftConfiguration;
import org.apache.ignite.internal.raft.storage.LogStorageFactory;
import org.apache.ignite.internal.raft.util.SharedLogStorageFactoryUtils;
import org.apache.ignite.internal.storage.configurations.StorageConfiguration;
import org.apache.ignite.internal.testframework.IgniteAbstractTest;
import org.apache.ignite.internal.vault.VaultManager;
import org.apache.ignite.internal.vault.inmemory.InMemoryVaultService;
import org.apache.ignite.network.NetworkAddress;
import org.apache.ignite.raft.jraft.rpc.impl.RaftGroupEventsClientListener;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Tests for scenarios in which multiple Meta Storage nodes participate.
 */
@ExtendWith(ConfigurationExtension.class)
abstract class ItMetaStorageMultipleNodesAbstractTest extends IgniteAbstractTest {
    static final long AWAIT_TIMEOUT = SECONDS.toMillis(10);

    @InjectConfiguration
    private RaftConfiguration raftConfiguration;

    @InjectConfiguration
    private NodeAttributesConfiguration nodeAttributes;

    @InjectConfiguration
    private StorageConfiguration storageConfiguration;

    @InjectConfiguration
    private SystemLocalConfiguration systemLocalConfiguration;

    /**
     * Large interval to effectively disable idle safe time propagation.
     */
    @InjectConfiguration("mock.idleSafeTimeSyncIntervalMillis=1000000")
    private SystemDistributedConfiguration systemDistributedConfiguration;

    private TestInfo testInfo;

    KeyValueStorage createStorage(String nodeName, Path path, ReadOperationForCompactionTracker readOperationForCompactionTracker) {
        return new SimpleInMemoryKeyValueStorage(nodeName, readOperationForCompactionTracker);
    }

    void startMetastorageOn(List<Node> nodes) {
        ComponentContext componentContext = new ComponentContext();

        for (Node node : nodes) {
            assertThat(node.metaStorageManager.startAsync(componentContext), willCompleteSuccessfully());
        }
    }

    class Node {
        private final VaultManager vaultManager;

        final ClusterService clusterService;

        final HybridClock clock = new HybridClockImpl();

        private final Loza raftManager;

        private final LogStorageFactory partitionsLogStorageFactory;

        private final LogStorageFactory msLogStorageFactory;

        private final LogStorageFactory cmgLogStorageFactory;

        private final ClusterStateStorage clusterStateStorage = new TestClusterStateStorage();

        final ClusterManagementGroupManager cmgManager;

        final MetaStorageManagerImpl metaStorageManager;

        /** The future have to be complete after the node start and all Meta storage watches are deployd. */
        private final CompletableFuture<Void> deployWatchesFut;

        private final FailureManager failureManager;

        Node(ClusterService clusterService, Path dataPath) {
            this.clusterService = clusterService;

            this.vaultManager = new VaultManager(new InMemoryVaultService());

            Path basePath = dataPath.resolve(name());

            var raftGroupEventsClientListener = new RaftGroupEventsClientListener();

            ComponentWorkingDir workingDir = new ComponentWorkingDir(basePath.resolve("raft"));

            partitionsLogStorageFactory = SharedLogStorageFactoryUtils.create(
                    clusterService.nodeName(),
                    workingDir.raftLogPath()
            );

            this.raftManager = TestLozaFactory.create(
                    clusterService,
                    raftConfiguration,
                    systemLocalConfiguration,
                    clock,
                    raftGroupEventsClientListener
            );

            this.failureManager = new NoOpFailureManager();

            var logicalTopology = new LogicalTopologyImpl(clusterStateStorage, failureManager);

            var clusterInitializer = new ClusterInitializer(
                    clusterService,
                    hocon -> hocon,
                    new TestConfigurationValidator()
            );

            ComponentWorkingDir cmgWorkDir = new ComponentWorkingDir(basePath.resolve("cmg"));

            cmgLogStorageFactory =
                    SharedLogStorageFactoryUtils.create(clusterService.nodeName(), cmgWorkDir.raftLogPath());

            RaftGroupOptionsConfigurer cmgRaftConfigurator =
                    RaftGroupOptionsConfigHelper.configureProperties(cmgLogStorageFactory, cmgWorkDir.metaPath());

            this.cmgManager = new ClusterManagementGroupManager(
                    vaultManager,
                    new SystemDisasterRecoveryStorage(vaultManager),
                    clusterService,
                    clusterInitializer,
                    raftManager,
                    clusterStateStorage,
                    logicalTopology,
                    new NodeAttributesCollector(nodeAttributes, storageConfiguration),
                    failureManager,
                    raftGroupEventsClientListener,
                    new ClusterIdHolder(),
                    cmgRaftConfigurator,
                    new NoOpMetricManager()
            );

            var logicalTopologyService = new LogicalTopologyServiceImpl(logicalTopology, cmgManager);

            var topologyAwareRaftGroupServiceFactory = new TopologyAwareRaftGroupServiceFactory(
                    clusterService,
                    logicalTopologyService,
                    Loza.FACTORY,
                    raftGroupEventsClientListener
            );

            ComponentWorkingDir metastorageWorkDir = new ComponentWorkingDir(basePath.resolve("metastorage"));

            msLogStorageFactory =
                    SharedLogStorageFactoryUtils.create(clusterService.nodeName(), metastorageWorkDir.raftLogPath());

            RaftGroupOptionsConfigurer msRaftConfigurator =
                    RaftGroupOptionsConfigHelper.configureProperties(msLogStorageFactory, metastorageWorkDir.metaPath());

            var readOperationForCompactionTracker = new ReadOperationForCompactionTracker();

            this.metaStorageManager = new MetaStorageManagerImpl(
                    clusterService,
                    cmgManager,
                    logicalTopologyService,
                    raftManager,
                    createStorage(name(), metastorageWorkDir.dbPath(), readOperationForCompactionTracker),
                    clock,
                    topologyAwareRaftGroupServiceFactory,
                    new NoOpMetricManager(),
                    systemDistributedConfiguration,
                    msRaftConfigurator,
                    readOperationForCompactionTracker
            );

            deployWatchesFut = metaStorageManager.deployWatches();
        }

        void start() {
            List<IgniteComponent> components = List.of(
                    vaultManager,
                    clusterService,
                    partitionsLogStorageFactory,
                    msLogStorageFactory,
                    cmgLogStorageFactory,
                    raftManager,
                    clusterStateStorage,
                    failureManager,
                    cmgManager
            );

            assertThat(startAsync(new ComponentContext(), components), willCompleteSuccessfully());
        }

        /**
         * Waits for watches deployed.
         */
        void waitWatches() {
            assertThat("Watches were not deployed", deployWatchesFut, willCompleteSuccessfully());
        }

        String name() {
            return clusterService.nodeName();
        }

        void stop() throws Exception {
            List<IgniteComponent> components = List.of(
                    metaStorageManager,
                    cmgManager,
                    failureManager,
                    raftManager,
                    clusterStateStorage,
                    partitionsLogStorageFactory,
                    msLogStorageFactory,
                    cmgLogStorageFactory,
                    clusterService,
                    vaultManager
            );

            closeAll(Stream.concat(
                    components.stream().map(c -> c::beforeNodeStop),
                    Stream.of(() -> assertThat(stopAsync(new ComponentContext(), components), willCompleteSuccessfully()))
            ));
        }

        CompletableFuture<Set<String>> getMetaStorageLearners() {
            return metaStorageManager
                    .metaStorageService()
                    .thenApply(MetaStorageServiceImpl::raftGroupService)
                    .thenCompose(service -> service.refreshMembers(false).thenApply(v -> service.learners()))
                    .thenApply(learners -> learners.stream().map(Peer::consistentId).collect(toSet()));
        }
    }

    final List<Node> nodes = new ArrayList<>();

    final Node startNode() {
        var nodeFinder = new StaticNodeFinder(List.of(new NetworkAddress("localhost", 10_000)));

        ClusterService clusterService = ClusterServiceTestUtils.clusterService(testInfo, 10_000 + nodes.size(), nodeFinder);

        var node = new Node(clusterService, workDir);

        node.start();

        nodes.add(node);

        return node;
    }

    @BeforeEach
    void saveTestInfo(TestInfo testInfo) {
        this.testInfo = testInfo;
    }

    @AfterEach
    void tearDown() throws Exception {
        closeAll(nodes.parallelStream().map(node -> node::stop));
    }

    final void enableIdleSafeTimeSync() {
        CompletableFuture<Void> updateIdleSafeTimeSyncIntervalFuture = systemDistributedConfiguration
                .idleSafeTimeSyncIntervalMillis()
                .update(100L);

        assertThat(updateIdleSafeTimeSyncIntervalFuture, willCompleteSuccessfully());
    }
}
