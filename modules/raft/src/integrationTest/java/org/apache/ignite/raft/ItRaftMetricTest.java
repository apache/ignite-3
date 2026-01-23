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

package org.apache.ignite.raft;

import static java.util.List.of;
import static org.apache.ignite.internal.ClusterPerTestIntegrationTest.aggressiveLowWatermarkIncreaseClusterConfig;
import static org.apache.ignite.internal.TestWrappers.unwrapIgniteImpl;
import static org.apache.ignite.internal.metrics.TestMetricUtils.testMetricChangeAfterOperation;
import static org.apache.ignite.internal.metrics.sources.RaftMetricSource.RAFT_GROUP_LEADERS;
import static org.apache.ignite.internal.metrics.sources.RaftMetricSource.SOURCE_NAME;
import static org.awaitility.Awaitility.await;

import org.apache.ignite.InitParametersBuilder;
import org.apache.ignite.internal.ClusterPerClassIntegrationTest;
import org.junit.jupiter.api.Test;

public class ItRaftMetricTest extends ClusterPerClassIntegrationTest {
    @Override
    protected void configureInitParameters(InitParametersBuilder builder) {
        builder.clusterConfiguration(aggressiveLowWatermarkIncreaseClusterConfig());
    }

    @Test
    void testLeaderCount() {
        testMetricChangeAfterOperation(CLUSTER, SOURCE_NAME, of(RAFT_GROUP_LEADERS), of((long) DEFAULT_PARTITION_COUNT), () -> {
            sql("CREATE ZONE TEST_ZONE WITH STORAGE_PROFILES='default'");
            sql("CREATE TABLE TEST (id INT PRIMARY KEY, val VARCHAR) ZONE TEST_ZONE");
        });

        testMetricChangeAfterOperation(CLUSTER, SOURCE_NAME, of(RAFT_GROUP_LEADERS), of((long) -DEFAULT_PARTITION_COUNT), () -> {
            int initialNodes = getRaftNodesCount();

            sql("DROP TABLE TEST");
            sql("DROP ZONE TEST_ZONE");

            // Waiting for zone partitions to be destroyed.
            await().until(() -> initialNodes - getRaftNodesCount() >= DEFAULT_PARTITION_COUNT);
        });
    }

    private static int getRaftNodesCount() {
        return CLUSTER.runningNodes()
                .mapToInt(node -> unwrapIgniteImpl(node).raftManager().localNodes().size())
                .sum();
    }
}
