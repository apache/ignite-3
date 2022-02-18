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

package org.apache.ignite.internal.cluster.management;

import static org.apache.ignite.utils.ClusterServiceTestUtils.clusterService;
import static org.mockito.Mockito.mock;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.ignite.internal.cluster.management.raft.RocksDbRaftStorage;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.raft.Loza;
import org.apache.ignite.internal.rest.RestComponent;
import org.apache.ignite.internal.vault.VaultManager;
import org.apache.ignite.internal.vault.persistence.PersistentVaultService;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.ClusterService;
import org.apache.ignite.network.NetworkAddress;
import org.apache.ignite.network.NodeFinder;
import org.junit.jupiter.api.TestInfo;

/**
 * Fake node for integration tests.
 */
class MockNode {
    private ClusterManagementGroupManager clusterManager;

    private ClusterService clusterService;

    private final TestInfo testInfo;

    private final NodeFinder nodeFinder;

    private final Path workDir;

    private final List<IgniteComponent> components = new ArrayList<>();

    MockNode(TestInfo testInfo, NetworkAddress addr, NodeFinder nodeFinder, Path workDir) throws IOException {
        this.testInfo = testInfo;
        this.nodeFinder = nodeFinder;
        this.workDir = workDir;

        init(addr.port());
    }

    private void init(int port) throws IOException {
        Path vaultDir = workDir.resolve("vault");

        var vaultManager = new VaultManager(new PersistentVaultService(Files.createDirectories(vaultDir)));

        this.clusterService = clusterService(testInfo, port, nodeFinder);

        Loza raftManager = new Loza(clusterService, workDir);

        this.clusterManager = new ClusterManagementGroupManager(
                vaultManager,
                clusterService,
                raftManager,
                mock(RestComponent.class),
                new RocksDbRaftStorage(workDir.resolve("cmg"))
        );

        components.add(vaultManager);
        components.add(clusterService);
        components.add(raftManager);
        components.add(clusterManager);
    }

    void start() {
        components.forEach(IgniteComponent::start);
    }

    void beforeNodeStop() {
        Collections.reverse(components);

        components.forEach(IgniteComponent::beforeNodeStop);
    }

    void stop() throws Exception {
        for (IgniteComponent component : components) {
            component.stop();
        }
    }

    void restart() throws Exception {
        int port = localMember().address().port();

        beforeNodeStop();
        stop();

        components.clear();

        init(port);

        start();
    }

    ClusterNode localMember() {
        return clusterService.topologyService().localMember();
    }

    ClusterManagementGroupManager clusterManager() {
        return clusterManager;
    }
}
