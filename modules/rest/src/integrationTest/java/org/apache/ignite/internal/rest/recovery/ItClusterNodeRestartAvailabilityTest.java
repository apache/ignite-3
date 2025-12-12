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

package org.apache.ignite.internal.rest.recovery;

import static org.apache.ignite.internal.TestDefaultProfilesNames.DEFAULT_AIPERSIST_PROFILE_NAME;

import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.ignite.internal.ClusterPerClassIntegrationTest;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Tests for cluster availability during node restarts. These tests verify that data remains accessible and consistent when nodes are killed
 * and restarted.
 */
@MicronautTest
public class ItClusterNodeRestartAvailabilityTest extends ClusterPerClassIntegrationTest {
    private static final int NODES_COUNT = 3;
    private static final int RESTART_NODE_INDEX = 1;
    private static final int PARTITIONS_COUNT = 32;
    private static final String TEST_ZONE_NAME = "test_zone";
    private static final int TABLE_COUNT = 10;

    private static final int ROW_COUNT = 100;

    @Override
    protected int initialNodes() {
        return NODES_COUNT;
    }

    @BeforeAll
    public void setup() {
        sql(String.format("CREATE ZONE \"%s\" (PARTITIONS %d, REPLICAS %d, AUTO SCALE UP 10,"
                        + " AUTO SCALE DOWN 10, CONSISTENCY MODE 'HIGH_AVAILABILITY') storage profiles ['%s']",
                TEST_ZONE_NAME, PARTITIONS_COUNT, initialNodes(), DEFAULT_AIPERSIST_PROFILE_NAME));
        for (int table = 0; table < TABLE_COUNT; table++) {
            String tableName = "test_table_" + table;
            sql(String.format("CREATE TABLE PUBLIC.\"%s\" (id INT PRIMARY KEY, val INT) ZONE \"%s\"", tableName,
                    TEST_ZONE_NAME));
        }
    }

    @Test
    public void singleKillAddDataAndCheckNodeReturns() throws Exception {
        Set<String> zonesFromTables = Set.of(TEST_ZONE_NAME);

        awaitAllPartitionsHealthyAndAvailable(zonesFromTables);

        CLUSTER.stopNode(RESTART_NODE_INDEX);

        fillTables();

        CLUSTER.startNode(RESTART_NODE_INDEX);

        awaitAllPartitionsHealthyAndAvailable(zonesFromTables);
    }

    private void awaitAllPartitionsHealthyAndAvailable(Set<String> zoneNames) throws InterruptedException {
        Set<Integer> partitionIds = IntStream.range(0, PARTITIONS_COUNT)
                .boxed()
                .collect(Collectors.toSet());

        for (String zoneName : zoneNames) {
            awaitPartitionsToBeHealthy(CLUSTER, zoneName, partitionIds);
        }
    }

    private void fillTables() {
        int val = 0;
        for (int test = 0; test < TABLE_COUNT; test++) {
            String tableName = "test_table_" + test;
            for (int i = 0; i < ROW_COUNT; i++) {
                sql(String.format("INSERT INTO PUBLIC.\"%s\" VALUES (%d, %d)", tableName, val++, val++));
            }
        }
    }
}
