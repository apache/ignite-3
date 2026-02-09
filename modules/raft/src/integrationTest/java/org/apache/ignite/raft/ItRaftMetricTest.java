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

import static org.apache.ignite.internal.ClusterPerTestIntegrationTest.aggressiveLowWatermarkIncreaseClusterConfig;
import static org.apache.ignite.internal.TestMetricUtils.testMetricChangeAfterOperation;
import static org.apache.ignite.internal.TestWrappers.unwrapIgniteImpl;
import static org.apache.ignite.internal.metrics.sources.RaftMetricSource.RAFT_GROUP_LEADERS;
import static org.apache.ignite.internal.metrics.sources.RaftMetricSource.SOURCE_NAME;
import static org.awaitility.Awaitility.await;

import java.util.List;
import org.apache.ignite.InitParametersBuilder;
import org.apache.ignite.internal.ClusterPerClassIntegrationTest;
import org.apache.ignite.internal.metrics.sources.RaftMetricSource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

/** Tests for {@link RaftMetricSource}. */
public class ItRaftMetricTest extends ClusterPerClassIntegrationTest {
    private static final String ZONE_NAME = "TEST_ZONE";

    @Override
    protected void configureInitParameters(InitParametersBuilder builder) {
        // To trigger zone's raft partitions destruction.
        builder.clusterConfiguration(aggressiveLowWatermarkIncreaseClusterConfig());
    }

    @BeforeEach
    void setUp() {
        dropTableAndZone();
    }

    @Test
    @Disabled("https://issues.apache.org/jira/browse/IGNITE-27728")
    void testLeaderCountIncreases() {
        testMetricChangeAfterOperation(
                CLUSTER,
                SOURCE_NAME,
                List.of(RAFT_GROUP_LEADERS),
                List.of((long) DEFAULT_PARTITION_COUNT),
                ItRaftMetricTest::createZoneAndTable
        );
    }

    @Test
    @Disabled("https://issues.apache.org/jira/browse/IGNITE-27728")
    void testLeaderCountDecreases() {
        createZoneAndTable();

        testMetricChangeAfterOperation(
                CLUSTER,
                SOURCE_NAME,
                List.of(RAFT_GROUP_LEADERS),
                List.of((long) -DEFAULT_PARTITION_COUNT),
                () -> {
                    int initialNodes = getRaftNodesCount();

                    dropTableAndZone();

                    // Waiting for zone partitions to be destroyed.
                    await().until(() -> initialNodes - getRaftNodesCount() >= DEFAULT_PARTITION_COUNT);
                });
    }

    private static void dropTableAndZone() {
        sql("DROP ZONE IF EXISTS " + ZONE_NAME);
    }

    private static void createZoneAndTable() {
        sql("CREATE ZONE " + ZONE_NAME + " WITH STORAGE_PROFILES='default'");
    }

    private static int getRaftNodesCount() {
        return CLUSTER.runningNodes()
                .mapToInt(node -> unwrapIgniteImpl(node).raftManager().localNodes().size())
                .sum();
    }
}
