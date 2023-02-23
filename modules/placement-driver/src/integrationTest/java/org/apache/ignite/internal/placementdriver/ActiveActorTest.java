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
import org.apache.ignite.internal.raft.client.TopologyAwareRaftGroupServiceTest;
import org.apache.ignite.network.ClusterService;
import org.junit.jupiter.api.AfterEach;

/**
 * Placement driver active actor test.
 */
public class ActiveActorTest extends TopologyAwareRaftGroupServiceTest {
    private Map<String, PlacementDriverManager> placementDriverManagers = new HashMap<>();

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
    protected void afterNodeStart(String nodeName, ClusterService clusterService, Set<String> placementDriverNodesNames) {
        PlacementDriverManager placementDriverManager = new PlacementDriverManager(
                TestReplicationGroup.GROUP_ID,
                clusterService,
                raftConfiguration,
                () -> completedFuture(placementDriverNodesNames),
                new LogicalTopologyServiceTestImpl(clusterService),
                executor
        );

        placementDriverManager.start();

        placementDriverManagers.put(nodeName, placementDriverManager);
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
