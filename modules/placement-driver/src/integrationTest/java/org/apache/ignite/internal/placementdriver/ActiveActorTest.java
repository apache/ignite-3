/*
 * Copyright 2022 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.placementdriver;

import static org.apache.ignite.internal.raft.Loza.CLIENT_POOL_NAME;
import static org.apache.ignite.internal.raft.Loza.CLIENT_POOL_SIZE;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.testNodeName;
import static org.apache.ignite.utils.ClusterServiceTestUtils.defaultSerializationRegistry;
import static org.mockito.Mockito.mock;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import org.apache.ignite.internal.cluster.management.ClusterManagementGroupManager;
import org.apache.ignite.internal.cluster.management.configuration.ClusterManagementConfiguration;
import org.apache.ignite.internal.cluster.management.raft.RocksDbClusterStateStorage;
import org.apache.ignite.internal.cluster.management.topology.LogicalTopologyImpl;
import org.apache.ignite.internal.cluster.management.topology.LogicalTopologyServiceImpl;
import org.apache.ignite.internal.configuration.ConfigurationManager;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.network.configuration.NetworkConfiguration;
import org.apache.ignite.internal.raft.Loza;
import org.apache.ignite.internal.raft.configuration.RaftConfiguration;
import org.apache.ignite.internal.testframework.IgniteAbstractTest;
import org.apache.ignite.internal.thread.NamedThreadFactory;
import org.apache.ignite.internal.vault.VaultManager;
import org.apache.ignite.network.ClusterLocalConfiguration;
import org.apache.ignite.network.NettyBootstrapFactory;
import org.apache.ignite.network.scalecube.TestScaleCubeClusterServiceFactory;
import org.apache.ignite.utils.ClusterServiceTestUtils;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ConfigurationExtension.class)
public class ActiveActorTest extends IgniteAbstractTest {
    @InjectConfiguration
    private static RaftConfiguration raftConfiguration;

    @InjectConfiguration
    private static ClusterManagementConfiguration clusterManagementConfiguration;

    private void startNode(TestInfo testInfo, int idx) {
        String name = testNodeName(testInfo, idx);

        Path dir = workDir.resolve(name);

        partialNode = new ArrayList<>();

        VaultManager vault = mock(VaultManager.class);

        var clusterSvc = ClusterServiceTestUtils.clusterService(testInfo, addr.port(), nodeFinder);

        HybridClock hybridClock = new HybridClockImpl();

        ScheduledExecutorService raftExecutorService = new ScheduledThreadPoolExecutor(CLIENT_POOL_SIZE,
                new NamedThreadFactory(NamedThreadFactory.threadPrefix(clusterSvc.localConfiguration().getName(),
                        CLIENT_POOL_NAME), logger()
                )
        );

        var raftMgr = new Loza(clusterSvc, raftConfiguration, dir, hybridClock, raftExecutorService);

        var clusterStateStorage = new RocksDbClusterStateStorage(dir.resolve("cmg"));

        var logicalTopologyService = new LogicalTopologyImpl(clusterStateStorage);

        var cmgManager = new ClusterManagementGroupManager(
                vault,
                clusterSvc,
                raftMgr,
                clusterStateStorage,
                logicalTopologyService,
                clusterManagementConfiguration
        );

        PlacementDriverManager placementDriverManager = new PlacementDriverManager(
                clusterSvc,
                raftConfiguration,
                cmgManager,
                new LogicalTopologyServiceImpl(logicalTopologyService, cmgManager),
                raftExecutorService
        );
    }
}
