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
import static org.apache.ignite.internal.testframework.IgniteTestUtils.testNodeName;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.util.IgniteUtils.stopAsync;
import static org.hamcrest.MatcherAssert.assertThat;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.cluster.management.configuration.ClusterManagementConfiguration;
import org.apache.ignite.internal.cluster.management.configuration.NodeAttributesConfiguration;
import org.apache.ignite.internal.cluster.management.raft.RocksDbClusterStateStorage;
import org.apache.ignite.internal.cluster.management.topology.LogicalTopologyImpl;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalNode;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologySnapshot;
import org.apache.ignite.internal.configuration.validation.TestConfigurationValidator;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.network.ClusterService;
import org.apache.ignite.internal.network.NodeFinder;
import org.apache.ignite.internal.network.utils.ClusterServiceTestUtils;
import org.apache.ignite.internal.raft.LozaUtils;
import org.apache.ignite.internal.raft.configuration.RaftConfiguration;
import org.apache.ignite.internal.storage.configurations.StorageConfiguration;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.util.ReverseIterator;
import org.apache.ignite.internal.vault.VaultManager;
import org.apache.ignite.internal.vault.persistence.PersistentVaultService;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.NetworkAddress;
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
            ClusterManagementConfiguration cmgConfiguration,
            NodeAttributesConfiguration nodeAttributes,
            StorageConfiguration storageProfilesConfiguration
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

        this.clusterService = ClusterServiceTestUtils.clusterService(nodeName, addr.port(), nodeFinder);

        var raftManager = LozaUtils.create(clusterService, raftConfiguration, this.workDir, new HybridClockImpl());

        var clusterStateStorage = new RocksDbClusterStateStorage(this.workDir.resolve("cmg"), clusterService.nodeName());

        this.clusterManager = new ClusterManagementGroupManager(
                vaultManager,
                clusterService,
                new ClusterInitializer(clusterService, hocon -> hocon, new TestConfigurationValidator()),
                raftManager,
                clusterStateStorage,
                new LogicalTopologyImpl(clusterStateStorage),
                cmgConfiguration,
                new NodeAttributesCollector(nodeAttributes, storageProfilesConfiguration)
        );

        components = List.of(
                vaultManager,
                clusterService,
                raftManager,
                clusterStateStorage,
                clusterManager
        );
    }

    /**
     * Start fake node.
     */
    public CompletableFuture<Void> startAsync() {
        return IgniteUtils.startAsync(components);
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

        assertThat(stopAsync(componentsToStop), willCompleteSuccessfully());
    }

    public ClusterNode localMember() {
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

    CompletableFuture<Set<ClusterNode>> validatedNodes() {
        return clusterManager().validatedNodes();
    }
}
