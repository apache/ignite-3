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
import static org.apache.ignite.internal.metrics.sources.RaftMetricSource.RAFT_GROUP_LEADERS;
import static org.apache.ignite.internal.metrics.sources.RaftMetricSource.SOURCE_NAME;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.Matchers.is;

import org.apache.ignite.InitParametersBuilder;
import org.apache.ignite.internal.ClusterPerClassIntegrationTest;
import org.apache.ignite.internal.TestMetricUtils;
import org.apache.ignite.internal.metrics.sources.RaftMetricSource;
import org.junit.jupiter.api.Test;

/** Tests for {@link RaftMetricSource}. */
public class ItRaftMetricTest extends ClusterPerClassIntegrationTest {
    private static final String ZONE_NAME = "TEST_ZONE";

    // CMG and Metastore leaders.
    private static final int SYSTEM_RAFT_LEADER_COUNT = 2;

    private static final int PARTITION_COUNT = 10;

    @Override
    protected boolean shouldCreateDefaultZone() {
        return false;
    }

    @Override
    protected void configureInitParameters(InitParametersBuilder builder) {
        // To trigger zone's raft partitions destruction.
        builder.clusterConfiguration(aggressiveLowWatermarkIncreaseClusterConfig());
    }

    @Test
    void testLeaderCountIncreases() {
        createZoneIfNotExists();

        awaitExpectedLeaderCount(PARTITION_COUNT + SYSTEM_RAFT_LEADER_COUNT);
    }

    @Test
    void testLeaderCountDecreases() {
        createZoneIfNotExists();

        awaitExpectedLeaderCount(PARTITION_COUNT + SYSTEM_RAFT_LEADER_COUNT);

        dropZone();

        awaitExpectedLeaderCount(SYSTEM_RAFT_LEADER_COUNT);
    }

    private static void awaitExpectedLeaderCount(long expected) {
        await().until(() -> TestMetricUtils.metricValue(CLUSTER, SOURCE_NAME, RAFT_GROUP_LEADERS), is(expected));
    }

    private static void dropZone() {
        sql("DROP ZONE " + ZONE_NAME);
    }

    private static void createZoneIfNotExists() {
        sql("CREATE ZONE IF NOT EXISTS " + ZONE_NAME + " WITH STORAGE_PROFILES='default', PARTITIONS = " + PARTITION_COUNT);
    }
}
