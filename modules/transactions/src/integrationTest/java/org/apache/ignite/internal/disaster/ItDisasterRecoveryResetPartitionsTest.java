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

import static org.apache.ignite.internal.TestWrappers.unwrapIgniteImpl;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.DEFAULT_ZONE_NAME;
import static org.apache.ignite.internal.distributionzones.rebalance.ZoneRebalanceUtil.STABLE_ASSIGNMENTS_PREFIX;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.assertThrows;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.apache.ignite.internal.testframework.flow.TestFlowUtils.subscribeToList;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Flow.Publisher;
import org.apache.ignite.internal.ClusterPerTestIntegrationTest;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.catalog.CatalogService;
import org.apache.ignite.internal.catalog.descriptors.CatalogZoneDescriptor;
import org.apache.ignite.internal.lang.ByteArray;
import org.apache.ignite.internal.metastorage.Entry;
import org.apache.ignite.internal.partitiondistribution.Assignments;
import org.apache.ignite.internal.table.distributed.disaster.DisasterRecoveryManager;
import org.apache.ignite.sql.SqlException;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.Test;

/** Tests for disaster recovery manager reset partition command. */
public class ItDisasterRecoveryResetPartitionsTest extends ClusterPerTestIntegrationTest {
    private static final String TABLE_NAME = "PERSON";

    @Override
    protected int initialNodes() {
        return 2;
    }

    @Test
    void shouldResetWithLostPartitions() throws InterruptedException {
        String selectSql = "select * from " + TABLE_NAME;
        executeSql("CREATE TABLE " + TABLE_NAME + "(ID INTEGER PRIMARY KEY, NAME VARCHAR(100));");
        executeSql("INSERT INTO " + TABLE_NAME + "(ID, NAME) VALUES (1, 'Ed'), (2, 'Ann'), (3, 'Emma');");

        assertDoesNotThrow(() -> executeSql(selectSql));

        String nodeToStop = cluster.node(1).name();

        stopNode(nodeToStop);

        assertThrows(SqlException.class, () -> executeSql(selectSql), "Mandatory nodes were excluded from mapping:");

        DisasterRecoveryManager disasterRecoveryManager = unwrapIgniteImpl(cluster.aliveNode()).disasterRecoveryManager();
        CompletableFuture<Void> resetFuture = disasterRecoveryManager.resetPartitions(
                DEFAULT_ZONE_NAME,
                Set.of(),
                true,
                -1
        );

        assertThat(resetFuture, willCompleteSuccessfully());

        assertTrue(waitForCondition(() -> !hasAssignmentsForNode(DEFAULT_ZONE_NAME, nodeToStop), 10000));

        // SQL engine reads assignments from AssignmentsTracker that is updated asynchronously
        // via metastorage watch events. The waitForCondition above verifies metastorage state directly, but there is
        // a propagation delay before the watch event updates the cache. Retry the query to tolerate this lag.
        assertTrue(waitForCondition(() -> {
            try {
                executeSql(selectSql);
                return true;
            } catch (SqlException e) {
                return false;
            }
        }, 10_000), "SQL query should succeed after partition reset");
    }

    private boolean hasAssignmentsForNode(String zoneName, String nodeName) {
        IgniteImpl ignite = unwrapIgniteImpl(cluster.aliveNode());

        ByteArray keyPrefix = prefix(zoneName, ignite);

        Publisher<Entry> publisher = ignite.metaStorageManager().prefix(keyPrefix);

        CompletableFuture<List<Entry>> stableAssignmentsFuture = subscribeToList(publisher);

        assertThat(stableAssignmentsFuture, willCompleteSuccessfully());

        return stableAssignmentsFuture.join().stream()
                .map(entry -> Assignments.fromBytes(entry.value()).nodes())
                .flatMap(Collection::stream)
                .anyMatch(assignment -> nodeName.equals(assignment.consistentId()));
    }

    private static ByteArray prefix(String zoneName, IgniteImpl ignite) {
        int zoneId = getZoneId(ignite.catalogManager(), zoneName, ignite.clock().nowLong());

        return new ByteArray(STABLE_ASSIGNMENTS_PREFIX + zoneId);
    }

    private static @Nullable Integer getZoneId(CatalogService catalogService, String zoneName, long timestamp) {
        CatalogZoneDescriptor zone = catalogService.activeCatalog(timestamp).zone(zoneName);

        return zone == null ? null : zone.id();
    }
}
