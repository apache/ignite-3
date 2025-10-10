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

package org.apache.ignite.internal.rebalance;

import static org.apache.ignite.internal.TestWrappers.unwrapIgniteImpl;
import static org.apache.ignite.internal.catalog.CatalogService.DEFAULT_STORAGE_PROFILE;
import static org.apache.ignite.internal.distributionzones.DistributionZonesTestUtil.getZoneIdStrict;
import static org.apache.ignite.internal.distributionzones.ZoneMetricSource.LOCAL_UNREBALANCED_PARTITIONS_COUNT;
import static org.apache.ignite.internal.distributionzones.ZoneMetricSource.TOTAL_UNREBALANCED_PARTITIONS_COUNT;
import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;
import static org.apache.ignite.internal.partitiondistribution.PartitionDistributionUtils.calculateAssignments;
import static org.apache.ignite.internal.sql.engine.util.SqlTestUtils.executeUpdate;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.Ignite;
import org.apache.ignite.internal.ClusterPerTestIntegrationTest;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.distributionzones.ZoneMetricSource;
import org.apache.ignite.internal.distributionzones.rebalance.ZoneRebalanceUtil;
import org.apache.ignite.internal.lang.ByteArray;
import org.apache.ignite.internal.metastorage.Entry;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.metastorage.WatchListener;
import org.apache.ignite.internal.metrics.IntMetric;
import org.apache.ignite.internal.metrics.MetricSet;
import org.apache.ignite.internal.partitiondistribution.Assignment;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;

/**
 * Tests rebalance metrics.
 */
@EnabledIf("org.apache.ignite.internal.lang.IgniteSystemProperties#colocationEnabled")
public class ItRebalanceMetricsTest extends ClusterPerTestIntegrationTest {
    private static final String ZONE_NAME = "TEST_ZONE";

    @Override
    protected int initialNodes() {
        return 1;
    }

    @Test
    void testRebalanceMetrics() throws Exception {
        // Create a zone with 7 partitions, 1 replica, and auto scale up set to Integer.MAX_VALUE.
        createZone(ZONE_NAME, 7, 1, Integer.MAX_VALUE);

        IgniteImpl node0 = unwrapIgniteImpl(cluster.node(0));
        int zoneId = getZoneIdStrict(node0.catalogManager(), ZONE_NAME, node0.clock().nowLong());

        // Calculate target assignments for 7 partitions and 1 replica across 2 nodes.
        List<Set<Assignment>> targetAssignments = calculateAssignments(
                List.of(cluster.nodeName(0), cluster.nodeName(1)),
                7,
                1,
                1);

        // Number of partitions that should be moved to the node 1.
        long partCnt = targetAssignments
                .stream()
                .filter(assignments -> assignments.contains(Assignment.forPeer(cluster.nodeName(1))))
                .count();

        // local and total unrebalanced partitions on node 0
        checkRebalanceMetrics(node0, ZONE_NAME, 0, 0);

        AtomicBoolean expected0 = new AtomicBoolean();
        AtomicBoolean expected1 = new AtomicBoolean();

        WatchListener listener = event -> {
            Entry entry = event.entryEvent().newEntry();

            if (entry.value() == null) {
                return nullCompletedFuture();
            }

            var partitionId = new String(entry.key());

            if (partitionId.startsWith(ZoneRebalanceUtil.PENDING_ASSIGNMENTS_QUEUE_PREFIX)) {
                // let's check metrics:
                MetricSet zoneMetric0 = zoneMetricSet(unwrapIgniteImpl(cluster.node(0)));
                IntMetric local0 = zoneMetric0.get(LOCAL_UNREBALANCED_PARTITIONS_COUNT);
                IntMetric total0 = zoneMetric0.get(TOTAL_UNREBALANCED_PARTITIONS_COUNT);

                MetricSet zoneMetric1 = zoneMetricSet(unwrapIgniteImpl(cluster.node(1)));
                IntMetric local1 = zoneMetric1.get(LOCAL_UNREBALANCED_PARTITIONS_COUNT);
                IntMetric total1 = zoneMetric1.get(TOTAL_UNREBALANCED_PARTITIONS_COUNT);

                // Expected metric values:
                // node0: localUnrebalancedPartitions == 0, totalUnrebalancedPartitions == partCnt
                // node1: localUnrebalancedPartitions == partCnt, totalUnrebalancedPartitions == partCnt
                if (local0.value() == 0 && total0.value() == partCnt) {
                    expected0.set(true);
                }
                if (local1.value() == partCnt && total1.value() == partCnt) {
                    expected1.set(true);
                }
            }

            return nullCompletedFuture();
        };

        // Start a new node and register a watch listener for pending assignments.
        Ignite node1 = startNode(1);
        MetaStorageManager metastorage = unwrapIgniteImpl(node1).metaStorageManager();

        ByteArray key = new ByteArray(ZoneRebalanceUtil.PENDING_ASSIGNMENTS_QUEUE_PREFIX + zoneId + "_part_");
        metastorage.registerPrefixWatch(key, listener);

        // Set auto scale up timer to 0 in order to trigger rebalance immediately.
        cluster.doInSession(0, session -> {
            executeUpdate(format("alter zone {} set auto scale up {}", ZONE_NAME, 0), session);
        });

        boolean res = waitForCondition(() -> expected0.get() && expected1.get(), 30_000, 200);
        if (!res) {
            // Log metric values.
            MetricSet zoneMetric0 = zoneMetricSet(unwrapIgniteImpl(cluster.node(0)));
            zoneMetric0.iterator().forEachRemaining(metric ->
                    log.warn(">>>>> metrics 0 [name=" + metric.name() + ", value=" + metric.getValueAsString()));

            MetricSet zoneMetric1 = zoneMetricSet(unwrapIgniteImpl(cluster.node(1)));
            zoneMetric1.iterator().forEachRemaining(metric ->
                    log.warn(">>>>> metrics 1 [name=" + metric.name() + ", value=" + metric.getValueAsString()));
        }

        assertThat(res, is(true));
    }

    @Test
    void testZoneRenaming() {
        // Create a zone with 7 partitions, 1 replica, and auto scale up set to Integer.MAX_VALUE.
        createZone(ZONE_NAME, 7, 1, Integer.MAX_VALUE);

        MetricSet zoneMetric0 = zoneMetricSet(unwrapIgniteImpl(cluster.node(0)), ZONE_NAME);
        assertThat(zoneMetric0, is(notNullValue()));

        IntMetric local0 = zoneMetric0.get(LOCAL_UNREBALANCED_PARTITIONS_COUNT);
        IntMetric total0 = zoneMetric0.get(TOTAL_UNREBALANCED_PARTITIONS_COUNT);
        assertThat(local0, is(notNullValue()));
        assertThat(total0, is(notNullValue()));

        renameZone(ZONE_NAME, "NEW_" + ZONE_NAME);

        MetricSet zoneMetric1 = zoneMetricSet(unwrapIgniteImpl(cluster.node(0)), "NEW_" + ZONE_NAME);
        assertThat(zoneMetric1, is(notNullValue()));

        IntMetric local1 = zoneMetric0.get(LOCAL_UNREBALANCED_PARTITIONS_COUNT);
        IntMetric total1 = zoneMetric0.get(TOTAL_UNREBALANCED_PARTITIONS_COUNT);
        assertThat(local1, is(notNullValue()));
        assertThat(total1, is(notNullValue()));

        assertThat(local0.value(), equalTo(local1.value()));
        assertThat(total0.value(), equalTo(total1.value()));
    }

    /**
     * Creates a distribution zone with the specified parameters.
     *
     * @param zoneName Name of the zone.
     * @param partitions Number of partitions.
     * @param replicas Number of replicas.
     * @param scaleUp Auto scale up value.
     */
    private void createZone(String zoneName, int partitions, int replicas, int scaleUp) {
        String sql = String.format("create zone %s "
                + "(partitions %d, replicas %d, "
                + "auto scale up %d, "
                + "auto scale down 0) "
                + "storage profiles ['%s']", zoneName, partitions, replicas, scaleUp, DEFAULT_STORAGE_PROFILE);

        cluster.doInSession(0, session -> {
            executeUpdate(sql, session);
        });
    }

    /**
     * Renames a distribution zone.
     *
     * @param oldName Current name of the zone.
     * @param newName New name of the zone.
     */
    private void renameZone(String oldName, String newName) {
        String sql = String.format("alter zone %s rename to %s", oldName, newName);

        cluster.doInSession(0, session -> {
            executeUpdate(sql, session);
        });
    }

    /**
     * Checks the rebalance metrics for the given Ignite instance.
     *
     * @param ignite Ignite instance.
     * @param zoneName Name of the zone.
     * @param localUnrebalanced Expected number of local unrebalanced partitions.
     * @param totalUnrebalanced Expected total number of unrebalanced partitions.
     */
    private static void checkRebalanceMetrics(
            IgniteImpl ignite,
            String zoneName,
            int localUnrebalanced,
            int totalUnrebalanced
    ) {
        MetricSet metrics = zoneMetricSet(ignite, zoneName);

        assertThat(metrics, is(notNullValue()));

        assertThat(((IntMetric) metrics.get(LOCAL_UNREBALANCED_PARTITIONS_COUNT)).value(), is(localUnrebalanced));
        assertThat(((IntMetric) metrics.get(TOTAL_UNREBALANCED_PARTITIONS_COUNT)).value(), is(totalUnrebalanced));
    }

    /**
     * Returns the metric set for the zone {@link #ZONE_NAME}.
     *
     * @param ignite Ignite instance.
     * @return MetricSet for the zone.
     */
    private static MetricSet zoneMetricSet(IgniteImpl ignite) {
        return zoneMetricSet(ignite, ZONE_NAME);
    }

    /**
     * Returns the metric set for the given {@code zone}.
     *
     * @param ignite Ignite instance.
     * @return MetricSet for the zone.
     */
    private static MetricSet zoneMetricSet(IgniteImpl ignite, String zone) {
        return ignite
                .metricManager()
                .metricSnapshot()
                .metrics()
                .get(ZoneMetricSource.sourceName(zone));
    }
}
