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
import static org.apache.ignite.internal.distributionzones.ZoneMetricSource.LOCAL_UNREBALANCED_PARTITIONS_COUNT;
import static org.apache.ignite.internal.distributionzones.ZoneMetricSource.TOTAL_UNREBALANCED_PARTITIONS_COUNT;
import static org.apache.ignite.internal.distributionzones.rebalance.ZoneRebalanceUtil.pendingPartAssignmentsQueueKey;
import static org.apache.ignite.internal.distributionzones.rebalance.ZoneRebalanceUtil.stablePartAssignmentsKey;
import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;
import static org.apache.ignite.internal.partitiondistribution.PartitionDistributionUtils.calculateAssignments;
import static org.apache.ignite.internal.sql.engine.util.SqlTestUtils.executeUpdate;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import org.apache.ignite.internal.ClusterPerTestIntegrationTest;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.catalog.descriptors.CatalogZoneDescriptor;
import org.apache.ignite.internal.distributionzones.ZoneMetricSource;
import org.apache.ignite.internal.metastorage.Entry;
import org.apache.ignite.internal.metrics.IntMetric;
import org.apache.ignite.internal.metrics.MetricSet;
import org.apache.ignite.internal.partitiondistribution.Assignment;
import org.apache.ignite.internal.partitiondistribution.Assignments;
import org.apache.ignite.internal.partitiondistribution.AssignmentsQueue;
import org.apache.ignite.internal.replicator.ZonePartitionId;
import org.awaitility.Awaitility;
import org.awaitility.core.ConditionEvaluationListener;
import org.awaitility.core.EvaluatedCondition;
import org.awaitility.core.TimeoutEvent;
import org.junit.jupiter.api.Test;

/**
 * Tests rebalance metrics.
 */
public class ItRebalanceMetricsTest extends ClusterPerTestIntegrationTest {
    private static final String ZONE_NAME = "TEST_ZONE";
    private static final String ZONE_NAME_TO_RENAME = "TEST_ZONE_TO_RENAME";

    @Override
    protected int initialNodes() {
        return 1;
    }

    @Test
    void testRebalanceMetrics() {
        int partitionCount = 7;
        int replicaCount = 1;
        int scaleUpTimeout = Integer.MAX_VALUE;

        createZone(ZONE_NAME, partitionCount, replicaCount, scaleUpTimeout);

        IgniteImpl node0 = unwrapIgniteImpl(cluster.node(0));

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

        // Start a new node and register a watch listener for pending assignments.
        IgniteImpl node1 = unwrapIgniteImpl(startNode(1));

        // Task to block a single threaded rebalance pool.
        CountDownLatch rebalanceLatch = new CountDownLatch(1);
        Runnable poisonPill = () -> {
            try {
                rebalanceLatch.await();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        };

        // Block rebalance scheduler to avoid moving pending assignments to stable ones.
        node0.threadPoolsManager().rebalanceScheduler().execute(poisonPill);
        node1.threadPoolsManager().rebalanceScheduler().execute(poisonPill);

        // Set auto scale up timer to 0 in order to trigger rebalance immediately.
        cluster.doInSession(0, session -> {
            executeUpdate(format("alter zone {} set auto scale up {}", ZONE_NAME, 0), session);
        });

        assertRebalanceMetrics(
                List.of(node0, node1),
                ZONE_NAME,
                List.of(0, partitionCountToRebalance),
                List.of(partitionCountToRebalance, partitionCountToRebalance),
                30);

        // Unblock rebalance.
        rebalanceLatch.countDown();

        assertRebalanceMetrics(
                List.of(node0, node1),
                ZONE_NAME,
                List.of(0, 0),
                List.of(0, 0),
                30);
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
     * Checks the rebalance metrics for the given Ignite instance.
     *
     * @param nodes Ignite instances.
     * @param zoneName Name of the zone.
     * @param localUnrebalanced Expected number of local unrebalanced partitions.
     * @param totalUnrebalanced Expected total number of unrebalanced partitions.
     * @param timeout Timeout (in seconds) to wait for the expected values.
     */
    private void assertRebalanceMetrics(
            List<IgniteImpl> nodes,
            String zoneName,
            List<Integer> localUnrebalanced,
            List<Integer> totalUnrebalanced,
            int timeout
    ) {
        Awaitility
                .with()
                .conditionEvaluationListener(new ConditionEvaluationListener<Boolean>() {
                    @Override
                    public void conditionEvaluated(EvaluatedCondition<Boolean> condition) {
                    }

                    @Override
                    public void onTimeout(TimeoutEvent event) {
                        for (int i = 0; i < nodes.size(); ++i) {
                            log.warn(
                                    ">>>>> partitions to rebalance [node= {}, expectedLocal={}, expectedTotal={}]",
                                    nodes.get(i).name(),
                                    localUnrebalanced.get(i),
                                    totalUnrebalanced.get(i));

                            zoneMetricSet(nodes.get(i)).iterator().forEachRemaining(metric ->
                                    log.warn("  ^-- metrics [name={}, value={}]", metric.name(), metric.getValueAsString()));

                            logAssignments(nodes.get(i), zoneName);
                        }
                    }
                })
                .await()
                .atMost(Duration.ofSeconds(timeout))
                .untilAsserted(() -> {
                    boolean res = true;
                    for (int i = 0; i < nodes.size(); ++i) {
                        res = res && checkRebalanceMetrics(nodes.get(i), zoneName, localUnrebalanced.get(i), totalUnrebalanced.get(i));
                    }

                    assertThat(res, is(true));
                });
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

    private void logAssignments(IgniteImpl ignite, String zoneName) {
        CatalogZoneDescriptor desc = ignite.catalogManager().latestCatalog().zone(zoneName);

        for (int i = 0; i < desc.partitions(); ++i) {
            ZonePartitionId zonePartitionId = new ZonePartitionId(desc.id(), i);

            Entry pendingEntry = ignite.metaStorageManager().getLocally(pendingPartAssignmentsQueueKey(zonePartitionId));
            AssignmentsQueue pendingAssignmentsQueue = AssignmentsQueue.fromBytes(pendingEntry.value());
            Assignments pendingAssignments = pendingAssignmentsQueue == null
                    ? null
                    : pendingAssignmentsQueue.peekLast();

            Entry stableEntry = ignite.metaStorageManager().getLocally(stablePartAssignmentsKey(zonePartitionId));
            Assignments stableAssignments = stableEntry.value() == null
                    ? Assignments.EMPTY
                    : Assignments.fromBytes(stableEntry.value());

            log.warn("  ^-- [partId={}, pending={}, stable={}]", i, pendingAssignments, stableAssignments);
        }
    }
}
