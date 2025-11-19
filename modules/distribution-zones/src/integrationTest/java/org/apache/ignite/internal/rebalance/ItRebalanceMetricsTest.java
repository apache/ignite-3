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
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.internal.ClusterPerTestIntegrationTest;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.distributionzones.ZoneMetricSource;
import org.apache.ignite.internal.distributionzones.rebalance.ZoneRebalanceUtil;
import org.apache.ignite.internal.lang.ByteArray;
import org.apache.ignite.internal.metastorage.Entry;
import org.apache.ignite.internal.metastorage.WatchEvent;
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
    private static final String ZONE_NAME_TO_RENAME = "TEST_ZONE_TO_RENAME";

    static class PendingAssignmentsWatchListener implements WatchListener {
        private final AtomicBoolean skipEventProcessing;
        private final CountDownLatch latch;
        private final String pendingKey;

        PendingAssignmentsWatchListener(String pendingKey, CountDownLatch latch, AtomicBoolean skipEventProcessing) {
            this.pendingKey = pendingKey;
            this.latch = latch;
            this.skipEventProcessing = skipEventProcessing;
        }

        @Override
        public CompletableFuture<Void> onUpdate(WatchEvent event) {
            if (skipEventProcessing.get()) {
                return nullCompletedFuture();
            }

            Entry entry = event.entryEvent().newEntry();

            if (entry.value() == null) {
                return nullCompletedFuture();
            }

            var eventKey = new String(entry.key());

            if (eventKey.startsWith(pendingKey)) {
                try {
                    latch.await();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }

            return nullCompletedFuture();
        }
    }

    @Override
    protected int initialNodes() {
        return 1;
    }

    @Test
    void testRebalanceMetrics() throws Exception {
        int partitionCount = 7;
        int replicaCount = 1;
        int scaleUpTimeout = Integer.MAX_VALUE;

        createZone(ZONE_NAME, partitionCount, replicaCount, scaleUpTimeout);

        IgniteImpl node0 = unwrapIgniteImpl(cluster.node(0));
        int zoneId = getZoneIdStrict(node0.catalogManager(), ZONE_NAME, node0.clock().nowLong());

        // Calculate target assignments for the zone.
        List<Set<Assignment>> targetAssignments = calculateAssignments(
                List.of(cluster.nodeName(0), cluster.nodeName(1)),
                partitionCount,
                replicaCount,
                1);

        // Number of partitions that should be moved to the node 1.
        int partitionCountToRebalance = (int) targetAssignments
                .stream()
                .filter(assignments -> assignments.contains(Assignment.forPeer(cluster.nodeName(1))))
                .count();

        assertRebalanceMetrics(node0, ZONE_NAME, 0, 0);

        CountDownLatch latch = new CountDownLatch(1);
        AtomicBoolean skipEventProcessing = new AtomicBoolean(false);
        String pendingAssignmentsKey = ZoneRebalanceUtil.PENDING_ASSIGNMENTS_QUEUE_PREFIX + zoneId + "_part_";
        ByteArray pendingAssignmentsBytes = new ByteArray(pendingAssignmentsKey);

        WatchListener listener = new PendingAssignmentsWatchListener(pendingAssignmentsKey, latch, skipEventProcessing);

        // Start a new node and register a watch listener for pending assignments.
        IgniteImpl node1 = unwrapIgniteImpl(startNode(1));

        node0.metaStorageManager().registerPrefixWatch(pendingAssignmentsBytes, listener);
        node1.metaStorageManager().registerPrefixWatch(pendingAssignmentsBytes, listener);

        // Set auto scale up timer to 0 in order to trigger rebalance immediately.
        cluster.doInSession(0, session -> {
            executeUpdate(format("alter zone {} set auto scale up {}", ZONE_NAME, 0), session);
        });

        boolean res = waitForCondition(() -> checkRebalanceMetrics(node0, ZONE_NAME, 0, partitionCountToRebalance)
                && checkRebalanceMetrics(node1, ZONE_NAME, partitionCountToRebalance, partitionCountToRebalance), 100, 30_000);

        if (!res) {
            log.warn(">>>>> partitions to rebalance = " + partitionCountToRebalance);
            MetricSet zoneMetric0 = zoneMetricSet(unwrapIgniteImpl(cluster.node(0)));
            zoneMetric0.iterator().forEachRemaining(metric ->
                    log.warn(">>>>> metrics 0 [name=" + metric.name() + ", value=" + metric.getValueAsString() + ']'));

            MetricSet zoneMetric1 = zoneMetricSet(unwrapIgniteImpl(cluster.node(1)));
            zoneMetric1.iterator().forEachRemaining(metric ->
                    log.warn(">>>>> metrics 1 [name=" + metric.name() + ", value=" + metric.getValueAsString() + ']'));
        }

        // Unblock rebalance.
        skipEventProcessing.set(true);
        latch.countDown();

        assertThat(res, is(true));

        res = waitForCondition(() -> checkRebalanceMetrics(node0, ZONE_NAME, 0, 0)
                && checkRebalanceMetrics(node1, ZONE_NAME, 0, 0), 100, 30_000);

        assertThat(res, is(true));
    }

    @Test
    void testZoneRenaming() {
        createZone(ZONE_NAME_TO_RENAME, 7, 1, Integer.MAX_VALUE);

        MetricSet zoneMetric0 = zoneMetricSet(unwrapIgniteImpl(cluster.node(0)), ZONE_NAME_TO_RENAME);
        assertThat(zoneMetric0, is(notNullValue()));

        IntMetric localUnrebalancedPartitionsMetric = zoneMetric0.get(LOCAL_UNREBALANCED_PARTITIONS_COUNT);
        IntMetric totalUnrebalancedPartitionsMetric = zoneMetric0.get(TOTAL_UNREBALANCED_PARTITIONS_COUNT);
        assertThat(localUnrebalancedPartitionsMetric, is(notNullValue()));
        assertThat(totalUnrebalancedPartitionsMetric, is(notNullValue()));

        int local = localUnrebalancedPartitionsMetric.value();
        int total  = totalUnrebalancedPartitionsMetric.value();

        renameZone(ZONE_NAME_TO_RENAME, "RENAMED_" + ZONE_NAME_TO_RENAME);

        assertRebalanceMetrics(unwrapIgniteImpl(cluster.node(0)), "RENAMED_" + ZONE_NAME_TO_RENAME, local, total);
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
    private static void assertRebalanceMetrics(
            IgniteImpl ignite,
            String zoneName,
            int localUnrebalanced,
            int totalUnrebalanced
    ) {
        MetricSet metrics = zoneMetricSet(ignite, zoneName);

        assertThat(metrics, is(notNullValue()));

        IntMetric local = metrics.get(LOCAL_UNREBALANCED_PARTITIONS_COUNT);
        IntMetric total = metrics.get(TOTAL_UNREBALANCED_PARTITIONS_COUNT);

        assertThat(local, is(notNullValue()));
        assertThat(total, is(notNullValue()));

        assertThat(local.value(), is(localUnrebalanced));
        assertThat(total.value(), is(totalUnrebalanced));
    }

    /**
     * Returns {@code true} zone metrics are equal to the given parameters.
     *
     * @param ignite Ignite instance.
     * @param zoneName Name of the zone.
     * @param localUnrebalanced Expected number of local unrebalanced partitions.
     * @param totalUnrebalanced Expected total number of unrebalanced partitions.
     * @return {@code true} if metrics are equal to the given parameters.
     */
    private static boolean checkRebalanceMetrics(
            IgniteImpl ignite,
            String zoneName,
            int localUnrebalanced,
            int totalUnrebalanced
    ) {
        MetricSet metrics = zoneMetricSet(ignite, zoneName);

        if (metrics == null) {
            return false;
        }

        IntMetric local = metrics.get(LOCAL_UNREBALANCED_PARTITIONS_COUNT);
        IntMetric total = metrics.get(TOTAL_UNREBALANCED_PARTITIONS_COUNT);

        return local != null && total != null
                && local.value() == localUnrebalanced && total.value() == totalUnrebalanced;
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
