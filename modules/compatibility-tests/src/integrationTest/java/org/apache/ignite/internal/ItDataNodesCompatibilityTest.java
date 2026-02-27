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

package org.apache.ignite.internal;

import static org.apache.ignite.internal.TestWrappers.unwrapIgniteImpl;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.DEFAULT_FILTER;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Predicate;
import org.apache.ignite.Ignite;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.catalog.descriptors.CatalogZoneDescriptor;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedClass;
import org.junit.jupiter.params.provider.MethodSource;

/** Compatibility tests for data nodes and data nodes history. */
@ParameterizedClass(name = "{displayName}({argumentsWithNames})")
@MethodSource("baseVersions")
public class ItDataNodesCompatibilityTest extends CompatibilityTestBase {
    private static final String ZONE_NAME = "TEST_ZONE";

    private static final String TABLE_NAME = "TEST_TABLE";

    @Override
    protected int nodesCount() {
        return 3;
    }

    @Override
    protected boolean restartWithCurrentEmbeddedVersion() {
        return false;
    }

    @Override
    protected void setupBaseVersion(Ignite baseIgnite) {
        String createZoneDdl = String.format(
                "CREATE ZONE %s WITH PARTITIONS=1, REPLICAS=1, STORAGE_PROFILES='default', DATA_NODES_FILTER='$[?(@.nodeIndex == \"0\")]'",
                ZONE_NAME
        );

        sql(baseIgnite, createZoneDdl);

        sql(baseIgnite, String.format("CREATE TABLE %s(ID INT PRIMARY KEY, VAL VARCHAR) ZONE %s", TABLE_NAME, ZONE_NAME));

        String insertDml = String.format("INSERT INTO %s (ID, VAL) VALUES (?, ?) ", TABLE_NAME);

        baseIgnite.transactions().runInTransaction(tx -> {
            for (int i = 0; i < 10; i++) {
                sql(baseIgnite, tx, insertDml, i, "str_" + i);
            }
        });
    }

    /**
     * Tests that data nodes are correctly recovered on the newer version of cluster.
     *
     * <ul>
     *     <li>write data nodes on old cluster, in legacy format, with some data nodes filter</li>
     *     <li>start new version of cluster, check that data nodes are recovered correctly</li>
     *     <li>changes filter to default and waits for data nodes recalculation</li>
     *     <li>changes the filter once again, to another filter, waits for data nodes recalculation</li>
     *     <li>restarts the cluster once again</li>
     *     <li>checks that the latest data nodes version is still active</li>
     * </ul>
     *
     * @throws Exception If failed.
     */
    @Test
    void testDataNodesChange() throws Exception {
        cluster.stop();

        int initialNodesCount = nodesCount();
        cluster.startEmbedded(initialNodesCount);

        IgniteImpl node = unwrapIgniteImpl(cluster.node(0));
        int zoneId = node.catalogManager().activeCatalog(node.clock().currentLong()).zone(ZONE_NAME).id();

        log.info("Test: zoneId=" + zoneId);

        Set<String> dataNodes = dataNodes(node, zoneId);

        assertEquals(Set.of(cluster.node(0).name()), dataNodes);

        log.info("Test: changing filter to default.");

        sql(String.format(
                "ALTER ZONE %s SET DATA_NODES_FILTER='%s'",
                ZONE_NAME,
                DEFAULT_FILTER
        ));

        // Let's wait for replication to complete on other nodes.
        waitForZoneState(ZONE_NAME, zone -> zone.filter().equals(DEFAULT_FILTER));

        waitForCondition(() -> dataNodes(node, zoneId).size() == nodesCount(), 10_000);

        log.info("Test: changing filter.");

        String newFilter = "$[?(@.nodeIndex == \"1\")]";
        sql(String.format(
                "ALTER ZONE %s SET DATA_NODES_FILTER='%s'",
                ZONE_NAME,
                newFilter
        ));

        // Let's wait for replication to complete on other nodes.
        waitForZoneState(ZONE_NAME, zone -> zone.filter().equals(newFilter));

        waitForCondition(() -> dataNodes(node, zoneId).equals(Set.of(cluster.node(1).name())), 10_000);

        // Check that we read the new data nodes after one more restart.
        cluster.stop();

        cluster.startEmbedded(initialNodesCount);

        IgniteImpl restartedNode = unwrapIgniteImpl(cluster.node(0));

        Set<String> dataNodesAfterRestart = dataNodes(restartedNode, zoneId);

        assertEquals(Set.of(cluster.node(1).name()), dataNodesAfterRestart);
    }

    private static Set<String> dataNodes(IgniteImpl node, int zoneId) {
        CompletableFuture<Set<String>> fut = node.distributionZoneManager().currentDataNodes(zoneId);
        assertThat(fut, willCompleteSuccessfully());
        return fut.join();
    }

    private void waitForZoneState(String zoneName, Predicate<CatalogZoneDescriptor> predicate)
            throws InterruptedException {
        assertTrue(waitForCondition(() -> {
            boolean tested = true;

            for (Ignite n : cluster.nodes()) {
                IgniteImpl node = unwrapIgniteImpl(n);
                CatalogZoneDescriptor zone = node.catalogManager().activeCatalog(node.clock().currentLong()).zone(zoneName);
                tested = tested && predicate.test(zone);
            }

            return tested;
        }, 10_0000));
    }
}
