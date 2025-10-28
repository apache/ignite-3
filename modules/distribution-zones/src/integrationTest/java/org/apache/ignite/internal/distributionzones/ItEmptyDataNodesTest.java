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

package org.apache.ignite.internal.distributionzones;

import static org.apache.ignite.internal.TestWrappers.unwrapIgniteImpl;
import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.assertThrowsWithCause;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.hasCause;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.util.ByteUtils.bytesToIntKeepingOrder;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.nio.charset.StandardCharsets;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Predicate;
import org.apache.ignite.internal.ClusterPerTestIntegrationTest;
import org.apache.ignite.internal.TestWrappers;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.distributionzones.exception.EmptyDataNodesException;
import org.apache.ignite.internal.lang.ByteArray;
import org.apache.ignite.internal.metastorage.server.WatchListenerInhibitor;
import org.apache.ignite.internal.placementdriver.EmptyAssignmentsException;
import org.apache.ignite.sql.SqlException;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.Test;

class ItEmptyDataNodesTest extends ClusterPerTestIntegrationTest {
    private static final String ZONE_NAME = "zone0";
    private static final String TABLE_NAME = "table0";

    @Test
    public void testInitialEmptyDataNodes() throws InterruptedException {
        createZoneAndTableWithEmptyDataNodes();

        IgniteImpl node = unwrapIgniteImpl(cluster.aliveNode());
        int zoneId = node.catalogManager().activeCatalog(node.clock().now().longValue()).zone(ZONE_NAME.toUpperCase()).id();

        assertTrue(currentDataNodes(node, zoneId).isEmpty());

        setAdditionalNodeFilter(null);

        // Trigger scale down and data nodes recalculation.
        stopNode(2);
        int aliveNodesCount = initialNodes() - 1;

        waitForCondition(() -> currentDataNodes(node, zoneId).size() == aliveNodesCount, 5_000);

        sql("INSERT INTO " + TABLE_NAME + " VALUES (1, 1)");
        sql("SELECT * FROM " + TABLE_NAME + " WHERE id = 1");
    }

    @Test
    public void testEmptyDataNodesException() {
        createZoneAndTableWithEmptyDataNodes();

        IgniteImpl node = unwrapIgniteImpl(cluster.aliveNode());
        int zoneId = node.catalogManager().activeCatalog(node.clock().now().longValue()).zone(ZONE_NAME.toUpperCase()).id();

        assertTrue(currentDataNodes(node, zoneId).isEmpty());

        assertThrowsWithCause(() -> sql("INSERT INTO " + TABLE_NAME + " VALUES (1, 1)"), EmptyDataNodesException.class);
    }

    @Test
    public void testInterruptZoneCreation() throws InterruptedException {
        runningNodes().forEach(n -> WatchListenerInhibitor.metastorageEventsInhibitor(n).startInhibit());

        IgniteImpl node = unwrapIgniteImpl(cluster.aliveNode());
        int activeCatalogVersion = node.catalogManager().activeCatalog(node.clock().now().longValue()).version();

        node.sql().executeAsync(null, format("CREATE ZONE {} (PARTITIONS 1, AUTO SCALE DOWN 0) STORAGE PROFILES ['default']", ZONE_NAME));

        ByteArray catalogVersionKey = new ByteArray("catalog.version".getBytes(StandardCharsets.UTF_8));
        waitForCondition(() -> {
            int ver = bytesToIntKeepingOrder(node.metaStorageManager().getLocally(catalogVersionKey).value());
            return ver > activeCatalogVersion;
        }, 5000);

        for (int i = 0; i < runningNodes().count(); i++) {
            stopNode(i);
            startNode(i);
            // No need to stop inhibiting after the node stop.
        }

        sql(format("CREATE TABLE {} (id INT PRIMARY KEY, val INT) ZONE {}", TABLE_NAME, ZONE_NAME));
        sql("INSERT INTO " + TABLE_NAME + " VALUES (1, 1)");
        sql("SELECT * FROM " + TABLE_NAME + " WHERE id = 1");
    }

    @Test
    public void testInitialEmptyAssignmentsWithSuccessfulWaiting() throws InterruptedException {
        createZoneAndTableWithEmptyDataNodes();

        IgniteImpl node = unwrapIgniteImpl(cluster.aliveNode());
        int zoneId = node.catalogManager().activeCatalog(node.clock().now().longValue()).zone(ZONE_NAME.toUpperCase()).id();

        assertTrue(currentDataNodes(node, zoneId).isEmpty());

        setAdditionalNodeFilter(null);

        // Assignments placement driver should wait for non-empty assignments for SQL.
        CompletableFuture<?> sqlFut = sqlAsync("SELECT * FROM " + TABLE_NAME);

        // Trigger scale down and data nodes recalculation.
        stopNode(2);

        assertThat(sqlFut, willCompleteSuccessfully());
    }

    @Test
    public void testEmptyAssignmentsException() {
        createZoneAndTableWithEmptyDataNodes();

        IgniteImpl node = unwrapIgniteImpl(cluster.aliveNode());
        int zoneId = node.catalogManager().activeCatalog(node.clock().now().longValue()).zone(ZONE_NAME.toUpperCase()).id();

        assertTrue(currentDataNodes(node, zoneId).isEmpty());

        try {
            sql("SELECT * FROM " + TABLE_NAME);
            fail();
        } catch (Exception e) {
            assertInstanceOf(SqlException.class, e);
            assertTrue(hasCause(e, EmptyAssignmentsException.class, null));
            assertTrue(hasCause(e, EmptyDataNodesException.class, null));
        }
    }

    private void createZoneAndTableWithEmptyDataNodes() {
        setAdditionalNodeFilter(n -> false);

        sql(format("CREATE ZONE {} (PARTITIONS 1, AUTO SCALE DOWN 0) STORAGE PROFILES ['default']", ZONE_NAME));

        sql(format("CREATE TABLE {} (id INT PRIMARY KEY, val INT) ZONE {}", TABLE_NAME, ZONE_NAME));
    }

    private static Set<String> currentDataNodes(IgniteImpl node, int zoneId) {
        CompletableFuture<Set<String>> nodeFut = node.distributionZoneManager().currentDataNodes(zoneId);
        assertThat(nodeFut, willCompleteSuccessfully());
        return nodeFut.join();
    }

    private void sql(String sql) {
        cluster.aliveNode().sql().execute(null, sql);
    }

    private CompletableFuture<?> sqlAsync(String sql) {
        return cluster.aliveNode().sql().executeAsync(null, sql);
    }

    private void setAdditionalNodeFilter(@Nullable Predicate<NodeWithAttributes> filter) {
        cluster.runningNodes()
                .map(TestWrappers::unwrapIgniteImpl)
                .forEach(node -> node.distributionZoneManager().setAdditionalNodeFilter(filter));
    }
}
