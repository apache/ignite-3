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

import static java.util.stream.Collectors.toSet;
import static org.apache.ignite.internal.TestWrappers.unwrapIgniteImpl;
import static org.apache.ignite.internal.catalog.CatalogService.DEFAULT_STORAGE_PROFILE;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zoneDataNodesHistoryKey;
import static org.apache.ignite.internal.metastorage.impl.MetaStorageCompactionTriggerConfiguration.DATA_AVAILABILITY_TIME_SYSTEM_PROPERTY_NAME;
import static org.apache.ignite.internal.metastorage.impl.MetaStorageCompactionTriggerConfiguration.INTERVAL_SYSTEM_PROPERTY_NAME;
import static org.apache.ignite.internal.sql.engine.util.SqlTestUtils.executeUpdate;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureExceptionMatcher.willThrow;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.InitParametersBuilder;
import org.apache.ignite.internal.ClusterPerClassIntegrationTest;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.catalog.CatalogManager;
import org.apache.ignite.internal.catalog.descriptors.CatalogZoneDescriptor;
import org.apache.ignite.internal.distributionzones.DataNodesHistory.DataNodesHistorySerializer;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.lang.ByteArray;
import org.apache.ignite.internal.metastorage.Entry;
import org.apache.ignite.internal.metastorage.exceptions.CompactedException;
import org.junit.jupiter.api.Test;

/**
 * Test for case of meta storage compaction.
 */
public class ItDistributionZoneMetaStorageCompactionTest extends ClusterPerClassIntegrationTest {
    private static final String ZONE_NAME = "TEST_ZONE";

    @Override
    protected int initialNodes() {
        return 2;
    }

    @Override
    protected void configureInitParameters(InitParametersBuilder builder) {
        builder.clusterConfiguration(createClusterConfigWithCompactionProperties(10, 10));
    }

    private static String createClusterConfigWithCompactionProperties(long interval, long dataAvailabilityTime) {
        return String.format(
                "ignite.system.properties: {"
                        + "%s = \"%s\", "
                        + "%s = \"%s\""
                        + "}",
                INTERVAL_SYSTEM_PROPERTY_NAME, interval, DATA_AVAILABILITY_TIME_SYSTEM_PROPERTY_NAME, dataAvailabilityTime
        );
    }

    /**
     * Tests that data nodes history is available for timestamp that matches a revision that was compacted.
     */
    @Test
    public void testCompaction() throws InterruptedException {
        String zoneSql = "create zone " + ZONE_NAME + " with partitions=1, storage_profiles='" + DEFAULT_STORAGE_PROFILE + "'"
                + ", data_nodes_auto_adjust_scale_down=0";

        CLUSTER.doInSession(0, session -> {
            executeUpdate(zoneSql, session);
        });

        IgniteImpl ignite = unwrapIgniteImpl(CLUSTER.node(0));

        HybridTimestamp beforeNodesStop = ignite.clock().now();

        CatalogManager catalogManager = ignite.catalogManager();
        CatalogZoneDescriptor zone = catalogManager.activeCatalog(beforeNodesStop.longValue())
                .zone(ZONE_NAME);

        int zoneId = zone.id();

        Set<String> dataNodesBeforeNodeStop = dataNodes(ignite, zoneId, beforeNodesStop);

        assertEquals(initialNodes(), dataNodesBeforeNodeStop.size());

        assertTrue(waitForCondition(
                () -> dataNodesInLocalMetaStorage(ignite, zoneId, beforeNodesStop).size() == initialNodes(),
                1000
        ));

        long revisionAfterCreateZone = ignite.metaStorageManager().appliedRevision();

        // Put some data to increment revision once more.
        ignite.metaStorageManager().put(new ByteArray("dummy_key"), "dummy_value".getBytes());

        assertTrue(waitForCondition(() -> ignite.metaStorageManager().appliedRevision() > revisionAfterCreateZone, 1000));

        CLUSTER.stopNode(1);

        // Wait for data nodes adjustment.
        assertTrue(waitForCondition(
                () -> dataNodes(ignite, zoneId, ignite.clock().now()).size() == 1,
                10_000
        ));

        // Wait for meta storage to be compacted.
        assertTrue(waitForCondition(() -> ignite.metaStorageManager().appliedCompactionRevision() > revisionAfterCreateZone, 1000));

        // Check that old revision is not available after compaction.
        assertThat(
                ignite.metaStorageManager().get(zoneDataNodesHistoryKey(zoneId), revisionAfterCreateZone),
                willThrow(CompactedException.class)
        );

        // Check that data nodes for old timestamp are still available.
        assertEquals(dataNodesBeforeNodeStop, dataNodes(ignite, zoneId, beforeNodesStop));
    }

    private static Set<String> dataNodes(IgniteImpl ignite, int zoneId, HybridTimestamp ts) {
        CompletableFuture<Set<String>> dataNodesBeforeStopFut = ignite
                .distributionZoneManager()
                .dataNodesManager()
                .dataNodes(zoneId, ts);

        assertThat(dataNodesBeforeStopFut, willCompleteSuccessfully());

        return dataNodesBeforeStopFut.join();
    }

    private static Set<String> dataNodesInLocalMetaStorage(IgniteImpl ignite, int zoneId, HybridTimestamp ts) {
        Entry e = ignite.metaStorageManager().getLocally(zoneDataNodesHistoryKey(zoneId));

        if (e.empty()) {
            return Set.of();
        }

        DataNodesHistory history = DataNodesHistorySerializer.deserialize(e.value());
        Set<String> nodes = history.dataNodesForTimestamp(ts).dataNodes().stream()
                .map(NodeWithAttributes::nodeName)
                .collect(toSet());

        return nodes;
    }
}
