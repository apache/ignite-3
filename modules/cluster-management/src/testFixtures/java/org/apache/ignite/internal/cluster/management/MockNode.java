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

package org.apache.ignite.internal.cluster.management;

import static java.util.Collections.reverse;
import static org.apache.ignite.internal.lang.IgniteSystemProperties.COLOCATION_FEATURE_FLAG;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.testNodeName;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.util.IgniteUtils.stopAsync;
import static org.hamcrest.MatcherAssert.assertThat;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import org.apache.ignite.internal.cluster.management.configuration.NodeAttributesConfiguration;
import org.apache.ignite.internal.cluster.management.raft.RocksDbClusterStateStorage;
import org.apache.ignite.internal.cluster.management.topology.LogicalTopologyImpl;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalNode;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologySnapshot;
import org.apache.ignite.internal.configuration.RaftGroupOptionsConfigHelper;
import org.apache.ignite.internal.configuration.SystemLocalConfiguration;
import org.apache.ignite.internal.configuration.validation.TestConfigurationValidator;
import org.apache.ignite.internal.disaster.system.SystemDisasterRecoveryStorage;
import org.apache.ignite.internal.failure.FailureManager;
import org.apache.ignite.internal.failure.NoOpFailureManager;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.manager.ComponentContext;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.metrics.NoOpMetricManager;
import org.apache.ignite.internal.network.ClusterService;
import org.apache.ignite.internal.network.InternalClusterNode;
import org.apache.ignite.internal.network.NodeFinder;
import org.apache.ignite.internal.network.utils.ClusterServiceTestUtils;
import org.apache.ignite.internal.raft.RaftGroupConfiguration;
import org.apache.ignite.internal.raft.RaftGroupOptionsConfigurer;
import org.apache.ignite.internal.raft.TestLozaFactory;
import org.apache.ignite.internal.raft.configuration.RaftConfiguration;
import org.apache.ignite.internal.raft.storage.LogStorageFactory;
import org.apache.ignite.internal.raft.util.SharedLogStorageFactoryUtils;
import org.apache.ignite.internal.storage.configurations.StorageConfiguration;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.util.ReverseIterator;
import org.apache.ignite.internal.vault.VaultManager;
import org.apache.ignite.internal.vault.persistence.PersistentVaultService;
import org.apache.ignite.network.NetworkAddress;
import org.apache.ignite.raft.jraft.rpc.impl.RaftGroupEventsClientListener;
import org.junit.jupiter.api.TestInfo;

/**
 * Fake node for integration tests.
 */
public class MockNode {
    private final ClusterManagementGroupManager clusterManager;

    private final ClusterService clusterService;

    private final Path workDir;

    private final List<IgniteComponent> components;

    private CompletableFuture<Void> startFuture;

    /**
     * Fake node constructor.
     */
    public MockNode(
            TestInfo testInfo,
            NetworkAddress addr,
            NodeFinder nodeFinder,
            Path workDir,
            RaftConfiguration raftConfiguration,
            SystemLocalConfiguration systemLocalConfiguration,
            NodeAttributesConfiguration nodeAttributes,
            StorageConfiguration storageProfilesConfiguration,
            Consumer<RaftGroupConfiguration> onConfigurationCommittedListener
    ) {
        this(
                testInfo,
                addr,
                nodeFinder,
                workDir,
                raftConfiguration,
                systemLocalConfiguration,
                nodeAttributes,
                () -> Map.of(COLOCATION_FEATURE_FLAG, Boolean.TRUE.toString()),
                storageProfilesConfiguration,
                onConfigurationCommittedListener
        );
    }

    /**
     * Fake node constructor.
     */
    public MockNode(
            TestInfo testInfo,
            NetworkAddress addr,
            NodeFinder nodeFinder,
            Path workDir,
            RaftConfiguration raftConfiguration,
            SystemLocalConfiguration systemLocalConfiguration,
            NodeAttributesConfiguration nodeAttributes,
            NodeAttributesProvider attributesProvider,
            StorageConfiguration storageProfilesConfiguration,
            Consumer<RaftGroupConfiguration> onConfigurationCommittedListener
    ) {
        String nodeName = testNodeName(testInfo, addr.port());

        this.workDir = workDir.resolve(nodeName);

        Path vaultDir;
        try {
            vaultDir = Files.createDirectories(this.workDir.resolve("vault"));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        var vaultManager = new VaultManager(new PersistentVaultService(vaultDir));

        var clusterIdHolder = new ClusterIdHolder();

        this.clusterService = ClusterServiceTestUtils.clusterService(nodeName, addr.port(), nodeFinder);

        LogStorageFactory partitionsLogStorageFactory = SharedLogStorageFactoryUtils.create(
                clusterService.nodeName(),
                this.workDir.resolve("partitions/log")
        );

        var eventsClientListener = new RaftGroupEventsClientListener();

        var raftManager = TestLozaFactory.create(
                clusterService,
                raftConfiguration,
                systemLocalConfiguration,
                new HybridClockImpl(),
                eventsClientListener
        );

        var clusterStateStorage =
                new RocksDbClusterStateStorage(this.workDir.resolve("cmg/data"), clusterService.nodeName());

        FailureManager failureManager = new NoOpFailureManager();

        LogStorageFactory cmgLogStorageFactory =
                SharedLogStorageFactoryUtils.create(
                        clusterService.nodeName(),
                        this.workDir.resolve("cmg/log")
                );

        RaftGroupOptionsConfigurer cmgRaftConfigurer =
                RaftGroupOptionsConfigHelper.configureProperties(cmgLogStorageFactory, this.workDir.resolve("cmg/meta"));

        var collector = new NodeAttributesCollector(nodeAttributes, storageProfilesConfiguration);

        collector.register(attributesProvider);

        this.clusterManager = new ClusterManagementGroupManager(
                vaultManager,
                new SystemDisasterRecoveryStorage(vaultManager),
                clusterService,
                new ClusterInitializer(
                        clusterService,
                        hocon -> hocon,
                        new TestConfigurationValidator()
                ),
                raftManager,
                clusterStateStorage,
                new LogicalTopologyImpl(clusterStateStorage, failureManager),
                collector,
                failureManager,
                eventsClientListener,
                clusterIdHolder,
                cmgRaftConfigurer,
                new NoOpMetricManager(),
                onConfigurationCommittedListener
        );

        components = List.of(
                vaultManager,
                clusterService,
                partitionsLogStorageFactory,
                cmgLogStorageFactory,
                raftManager,
                clusterStateStorage,
                failureManager,
                clusterManager
        );
    }

    /**
     * Start fake node.
     */
    public CompletableFuture<Void> startAsync() {
        return IgniteUtils.startAsync(new ComponentContext(), components);
    }

    /**
     * Start fake node.
     */
    public void startAndJoin() {
        assertThat(startAsync(), willCompleteSuccessfully());

        startFuture = clusterManager.onJoinReady();
    }

    /**
     * Method should be called before node stop.
     */
    public void beforeNodeStop() {
        ReverseIterator<IgniteComponent> it = new ReverseIterator<>(components);

        it.forEachRemaining(IgniteComponent::beforeNodeStop);
    }

    /**
     * Stop fake node.
     */
    public void stop() {
        List<IgniteComponent> componentsToStop = new ArrayList<>(components);

        reverse(componentsToStop);

        assertThat(stopAsync(new ComponentContext(), componentsToStop), willCompleteSuccessfully());
    }

    public InternalClusterNode localMember() {
        return clusterService.topologyService().localMember();
    }

    public String name() {
        return localMember().name();
    }

    public ClusterManagementGroupManager clusterManager() {
        return clusterManager;
    }

    public CompletableFuture<Void> startFuture() {
        return startFuture;
    }

    public ClusterService clusterService() {
        return clusterService;
    }

    public Path workDir() {
        return workDir;
    }

    CompletableFuture<Set<LogicalNode>> logicalTopologyNodes() {
        return clusterManager().logicalTopology().thenApply(LogicalTopologySnapshot::nodes);
    }

    CompletableFuture<Set<InternalClusterNode>> validatedNodes() {
        return clusterManager().validatedNodes();
    }
}
