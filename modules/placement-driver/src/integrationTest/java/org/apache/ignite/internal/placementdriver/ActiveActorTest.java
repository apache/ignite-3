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

package org.apache.ignite.internal.placementdriver;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.stream.Collectors.toList;
import static org.apache.ignite.internal.util.IgniteUtils.closeAll;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.distributionzones.configuration.DistributionZonesConfiguration;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.raft.client.TopologyAwareRaftGroupServiceTest;
import org.apache.ignite.internal.schema.configuration.TablesConfiguration;
import org.apache.ignite.internal.vault.VaultManager;
import org.apache.ignite.internal.vault.inmemory.InMemoryVaultService;
import org.apache.ignite.network.ClusterService;
import org.apache.ignite.raft.jraft.rpc.impl.RaftGroupEventsClientListener;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

/**
 * Placement driver active actor test.
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
public class ActiveActorTest extends TopologyAwareRaftGroupServiceTest {
    private Map<String, PlacementDriverManager> placementDriverManagers = new HashMap<>();

    @Mock
    MetaStorageManager msm;

    @InjectConfiguration()
    private TablesConfiguration tblsCfg;

    @InjectConfiguration
    private DistributionZonesConfiguration dstZnsCfg;

    @AfterEach
    @Override
    protected void afterTest() throws Exception {
        List<AutoCloseable> closeables = placementDriverManagers.values().stream().map(p -> (AutoCloseable) p::stop).collect(toList());

        closeAll(closeables);

        placementDriverManagers.clear();

        super.afterTest();
    }

    /** {@inheritDoc} */
    @Override
    protected void afterNodeStart(
            String nodeName,
            ClusterService clusterService,
            Set<String> placementDriverNodesNames,
            RaftGroupEventsClientListener eventsClientListener
    ) {
        PlacementDriverManager placementDriverManager = new PlacementDriverManager(
                msm,
                new VaultManager(new InMemoryVaultService()),
                TestReplicationGroup.GROUP_ID,
                clusterService,
                raftConfiguration,
                () -> completedFuture(placementDriverNodesNames),
                new LogicalTopologyServiceTestImpl(clusterService),
                executor,
                tblsCfg,
                dstZnsCfg,
                new HybridClockImpl(),
                eventsClientListener
        );

        placementDriverManager.start();

        placementDriverManagers.put(nodeName, placementDriverManager);
    }

    /**
     * The method is called after every node of the cluster starts.
     *
     * @param nodeName Node name.
     */
    @Override
    protected void afterNodeStop(String nodeName) {
        placementDriverManagers.remove(nodeName);
    }

    /** {@inheritDoc} */
    @Override
    protected boolean afterInitCheckCondition(String leaderName) {
        return checkSingleActiveActor(leaderName);
    }

    /** {@inheritDoc} */
    @Override
    protected boolean afterLeaderChangeCheckCondition(String leaderName) {
        return checkSingleActiveActor(leaderName);
    }

    private boolean checkSingleActiveActor(String leaderName) {
        for (Map.Entry<String, PlacementDriverManager> e : placementDriverManagers.entrySet()) {
            if (e.getValue().isActiveActor() != e.getKey().equals(leaderName)) {
                return false;
            }
        }

        return true;
    }
}
