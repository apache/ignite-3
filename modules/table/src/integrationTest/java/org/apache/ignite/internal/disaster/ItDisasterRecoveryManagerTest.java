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

package org.apache.ignite.internal.disaster;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.ignite.internal.catalog.CatalogService.DEFAULT_STORAGE_PROFILE;
import static org.apache.ignite.internal.table.TableTestUtils.TABLE_NAME;
import static org.apache.ignite.internal.table.TableTestUtils.getTableIdStrict;
import static org.apache.ignite.internal.table.TableTestUtils.getTableStrict;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;

import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.ClusterPerTestIntegrationTest;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.placementdriver.ReplicaMeta;
import org.apache.ignite.internal.replicator.ZonePartitionId;
import org.apache.ignite.internal.sql.SqlCommon;
import org.apache.ignite.internal.table.distributed.disaster.DisasterRecoveryManager;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** For {@link DisasterRecoveryManager} integration testing. */
// TODO https://issues.apache.org/jira/browse/IGNITE-22332 Add test cases.
public class ItDisasterRecoveryManagerTest extends ClusterPerTestIntegrationTest {
    private static final String ZONE_NAME = "ZONE_" + TABLE_NAME;

    @Override
    protected int initialNodes() {
        return 1;
    }

    @BeforeEach
    void setUp() {
        executeSql(String.format(
                "CREATE ZONE %s WITH REPLICAS=%s, PARTITIONS=%s, STORAGE_PROFILES='%s'",
                ZONE_NAME, initialNodes(), initialNodes(), DEFAULT_STORAGE_PROFILE
        ));

        executeSql(String.format(
                "CREATE TABLE %s (id INT PRIMARY KEY, val INT) WITH PRIMARY_ZONE='%s'",
                TABLE_NAME,
                ZONE_NAME
        ));
    }

    @Test
    void testRestartPartitions() {
        IgniteImpl node = cluster.aliveNode();

        insert(0, 0);
        insert(1, 1);

        int partitionId = 0;

        CompletableFuture<Void> restartPartitionsFuture = node.disasterRecoveryManager().restartPartitions(
                Set.of(node.name()),
                ZONE_NAME,
                SqlCommon.DEFAULT_SCHEMA_NAME + "." + TABLE_NAME,
                Set.of(partitionId)
        );

        assertThat(restartPartitionsFuture, willCompleteSuccessfully());
        assertThat(
                awaitPrimaryReplicaForNow(node, new ZonePartitionId(zoneId(node), tableId(node), partitionId)), willCompleteSuccessfully()
        );

        insert(2, 2);
        insert(3, 3);

        assertThat(selectAll(), hasSize(4));
    }

    private void insert(int id, int val) {
        executeSql(String.format(
                "INSERT INTO %s (id, val) VALUES (%s, %s)",
                TABLE_NAME, id, val
        ));
    }

    private List<List<Object>> selectAll() {
        return executeSql(String.format(
                "SELECT * FROM %s",
                TABLE_NAME
        ));
    }

    private static int tableId(IgniteImpl node) {
        return getTableIdStrict(node.catalogManager(), TABLE_NAME, node.clock().nowLong());
    }

    private static int zoneId(IgniteImpl node) {
        return getTableStrict(node.catalogManager(), TABLE_NAME, node.clock().nowLong()).zoneId();
    }

    private static CompletableFuture<ReplicaMeta> awaitPrimaryReplicaForNow(IgniteImpl node, ZonePartitionId zonePartitionId) {
        return node.placementDriver().awaitPrimaryReplicaForTable(zonePartitionId, node.clock().now(), 60, SECONDS);
    }
}
