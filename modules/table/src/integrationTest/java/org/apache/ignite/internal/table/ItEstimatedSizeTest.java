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

package org.apache.ignite.internal.table;

import static org.apache.ignite.internal.TestDefaultProfilesNames.DEFAULT_AIMEM_PROFILE_NAME;
import static org.apache.ignite.internal.TestDefaultProfilesNames.DEFAULT_AIPERSIST_PROFILE_NAME;
import static org.apache.ignite.internal.TestDefaultProfilesNames.DEFAULT_ROCKSDB_PROFILE_NAME;
import static org.apache.ignite.internal.TestDefaultProfilesNames.DEFAULT_TEST_PROFILE_NAME;
import static org.apache.ignite.internal.TestWrappers.unwrapIgniteImpl;
import static org.apache.ignite.internal.TestWrappers.unwrapTableViewInternal;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.IMMEDIATE_TIMER_VALUE;
import static org.apache.ignite.internal.distributionzones.rebalance.RebalanceUtil.STABLE_ASSIGNMENTS_PREFIX;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.apache.ignite.internal.testframework.flow.TestFlowUtils.subscribeToList;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willBe;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.ClusterPerTestIntegrationTest;
import org.apache.ignite.internal.affinity.Assignments;
import org.apache.ignite.internal.lang.ByteArray;
import org.apache.ignite.internal.metastorage.Entry;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

/**
 * Integration tests for {@link InternalTable#estimatedSize()} method.
 *
 * <p>This class doesn't use the Parameterized Test approach in order to reduce the number of created clusters.
 */
public class ItEstimatedSizeTest extends ClusterPerTestIntegrationTest {
    private static final String TEST_ZONE_NAME = "TestZone";

    private static final String TEST_TABLE_NAME_PREFIX = "Test_";

    private static final long NUM_ROWS = 100;

    private static final String[] ALL_STORAGE_PROFILES = {
            DEFAULT_TEST_PROFILE_NAME,
            DEFAULT_AIPERSIST_PROFILE_NAME,
            DEFAULT_AIMEM_PROFILE_NAME,
            DEFAULT_ROCKSDB_PROFILE_NAME
    };

    @BeforeEach
    void setUp() {
        executeSql(String.format(
                "CREATE ZONE %s WITH "
                        + "REPLICAS=%d, "
                        + "DATA_NODES_AUTO_ADJUST_SCALE_UP=%d, "
                        + "DATA_NODES_AUTO_ADJUST_SCALE_DOWN=%d, "
                        + "STORAGE_PROFILES='%s'",
                TEST_ZONE_NAME,
                initialNodes(),
                IMMEDIATE_TIMER_VALUE,
                IMMEDIATE_TIMER_VALUE,
                String.join(", ", ALL_STORAGE_PROFILES)
        ));
    }

    @Test
    void testEstimatedSize() {
        for (String profile : ALL_STORAGE_PROFILES) {
            String tableName = createTableWithData(profile);

            assertThat(tableSize(tableName), willBe(NUM_ROWS));

            for (int i = 0; i < NUM_ROWS / 2; i++) {
                executeSql(String.format("DELETE FROM %s WHERE key = %d", tableName, i));
            }

            assertThat(tableSize(tableName), willBe(NUM_ROWS / 2));

            executeSql(String.format("DELETE FROM %s", tableName));

            assertThat(tableSize(tableName), willBe(0L));
        }
    }

    @Test
    @Disabled("https://issues.apache.org/jira/browse/IGNITE-23001")
    void testEstimatedSizeAfterScaleUp() throws InterruptedException {
        for (String profile : ALL_STORAGE_PROFILES) {
            String tableName = createTableWithData(profile);

            assertThat(tableSize(tableName), willBe(NUM_ROWS));
        }

        cluster.startNode(initialNodes());

        for (String profile : ALL_STORAGE_PROFILES) {
            waitForRebalance(initialNodes() + 1);

            assertThat(tableSize(tableName(profile)), willBe(NUM_ROWS));
        }
    }

    @Test
    void testEstimatedAfterScaleDown() throws InterruptedException {
        for (String profile : ALL_STORAGE_PROFILES) {
            String tableName = createTableWithData(profile);

            assertThat(tableSize(tableName), willBe(NUM_ROWS));
        }

        cluster.stopNode(initialNodes() - 1);

        for (String profile : ALL_STORAGE_PROFILES) {
            waitForRebalance(initialNodes() - 1);

            assertThat(tableSize(tableName(profile)), willBe(NUM_ROWS));
        }
    }

    private String createTableWithData(String profile) {
        String tableName = tableName(profile);

        executeSql(String.format(
                "CREATE TABLE %s (key INT PRIMARY KEY) WITH PRIMARY_ZONE=%s, STORAGE_PROFILE='%s'",
                tableName,
                TEST_ZONE_NAME,
                profile
        ));

        for (int i = 0; i < NUM_ROWS; i++) {
            executeSql(String.format("INSERT INTO %s VALUES (%d)", tableName, i));
        }

        return tableName;
    }

    private static String tableName(String profile) {
        return TEST_TABLE_NAME_PREFIX + profile;
    }

    private CompletableFuture<Long> tableSize(String tableName) {
        return tableViewInternal(tableName).internalTable().estimatedSize();
    }

    private void waitForRebalance(int numNodes) throws InterruptedException {
        MetaStorageManager metaStorageManager = unwrapIgniteImpl(cluster.aliveNode()).metaStorageManager();

        var stableAssignmentsPrefix = new ByteArray(STABLE_ASSIGNMENTS_PREFIX);

        assertTrue(waitForCondition(() -> {
            CompletableFuture<List<Entry>> entriesFuture = subscribeToList(metaStorageManager.prefix(stableAssignmentsPrefix));

            assertThat(entriesFuture, willCompleteSuccessfully());

            long assignedNodesNum = entriesFuture.join().stream()
                    .map(entry -> Assignments.fromBytes(entry.value()))
                    .filter(Objects::nonNull)
                    .flatMap(assignments -> assignments.nodes().stream())
                    .distinct()
                    .count();

            return assignedNodesNum == numNodes;
        }, 10_000));
    }

    private TableViewInternal tableViewInternal(String tableName) {
        return unwrapTableViewInternal(cluster.aliveNode().tables().table(tableName));
    }
}
