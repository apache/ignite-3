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

package org.apache.ignite.internal.tx;

import static java.util.stream.Collectors.toSet;
import static org.apache.ignite.internal.TestWrappers.unwrapIgniteImpl;
import static org.apache.ignite.internal.TestWrappers.unwrapTableImpl;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.assertThrows;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.Ignite;
import org.apache.ignite.internal.ClusterPerClassIntegrationTest;
import org.apache.ignite.internal.TestWrappers;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.metrics.LongMetric;
import org.apache.ignite.internal.metrics.MetricSet;
import org.apache.ignite.internal.placementdriver.ReplicaMeta;
import org.apache.ignite.internal.replicator.ZonePartitionId;
import org.apache.ignite.internal.table.NodeUtils;
import org.apache.ignite.internal.table.TableImpl;
import org.apache.ignite.internal.tx.metrics.TransactionMetricsSource;
import org.apache.ignite.table.KeyValueView;
import org.apache.ignite.table.Tuple;
import org.apache.ignite.tx.Transaction;
import org.apache.ignite.tx.TransactionException;
import org.apache.ignite.tx.TransactionOptions;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Tests transaction metrics.
 */
public class ItTransactionMetricsTest extends ClusterPerClassIntegrationTest {
    public static final String TABLE_NAME = "test_table_name";

    @Override
    protected int initialNodes() {
        return 2;
    }

    @BeforeAll
    void createTable() {
        sql("CREATE TABLE " + TABLE_NAME + " (id INT PRIMARY KEY, val VARCHAR)");
    }

    /**
     * Returns a key value view for the table {@link #TABLE_NAME}.
     *
     * @param nodeIndex Node index to create a key value view.
     * @return Key value view.
     */
    private static KeyValueView<Integer, String> keyValueView(int nodeIndex) {
        return keyValueView(CLUSTER.node(nodeIndex));
    }

    /**
     * Returns a key value view for the table {@link #TABLE_NAME}.
     *
     * @param node Node to create a key value view.
     * @return Key value view.
     */
    private static KeyValueView<Integer, String> keyValueView(Ignite node) {
        return node.tables().table(TABLE_NAME).keyValueView(Integer.class, String.class);
    }

    /**
     * Returns a snapshot of transaction metrics for the given node.
     *
     * @param nodeIndex Node index to capture transaction metrics.
     * @return Snapshot of transaction metrics.
     */
    private static Map<String, Long> metricValues(int nodeIndex) {
        var values = new HashMap<String, Long>();

        MetricSet txMetrics = unwrapIgniteImpl(node(nodeIndex))
                .metricManager()
                .metricSnapshot()
                .metrics()
                .get(TransactionMetricsSource.SOURCE_NAME);

        txMetrics.iterator().forEachRemaining(metric -> {
            if (metric instanceof LongMetric) {
                values.put(metric.name(), ((LongMetric) metric).value());
            }
        });

        return values;
    }

    private static void testMetricValues(Map<String, Long> initial, Map<String, Long> actual, String... ignored) {
        assertThat("Number of metrics should be the same.", initial.size(), is(actual.size()));

        var exclude = Set.of(ignored);

        for (Map.Entry<String, Long> e : initial.entrySet()) {
            if (!exclude.contains(e.getKey())) {
                assertThat("Metric name = " + e.getKey(), actual.get(e.getKey()), is(e.getValue()));
            }
        }
    }

    /**
     * Tests that TotalCommits and RoCommits are incremented when a read only transaction successfully committed.
     *
     * @param implicit {@code true} if a transaction should be implicit and {@code false} otherwise.
     */
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testCommitReadOnlyTransaction(boolean implicit) {
        Map<String, Long> metrics0 = metricValues(0);
        Map<String, Long> metrics1 = metricValues(1);

        Transaction tx = implicit ? null : node(0).transactions().begin(new TransactionOptions().readOnly(true));
        keyValueView(0).get(tx, 12);
        if (!implicit) {
            tx.commit();
        }

        Map<String, Long> actualMetrics0 = metricValues(0);
        Map<String, Long> actualMetrics1 = metricValues(1);

        // Check that there are no updates on the node 1.
        testMetricValues(metrics1, actualMetrics1);

        // Check that all transaction metrics ere not changed except TotalCommits and RoCommits.
        testMetricValues(metrics0, actualMetrics0, "TotalCommits", "RoCommits");

        assertThat(actualMetrics0.get("TotalCommits"), is(metrics0.get("TotalCommits") + (implicit ? 0 : 1)));
        assertThat(actualMetrics0.get("RoCommits"), is(metrics0.get("RoCommits") + (implicit ? 0 : 1)));
    }

    /**
     * Tests that TotalCommits and RwCommits are incremented when a read write transaction successfully committed.
     *
     * @param implicit {@code true} if a transaction should be implicit and {@code false} otherwise.
     */
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testCommitReadWriteTransaction(boolean implicit) {
        Map<String, Long> metrics0 = metricValues(0);
        Map<String, Long> metrics1 = metricValues(1);

        Transaction tx = implicit ? null : node(0).transactions().begin();
        keyValueView(0).put(tx, 12, "value");
        if (!implicit) {
            tx.commit();
        }

        Map<String, Long> actualMetrics0 = metricValues(0);
        Map<String, Long> actualMetrics1 = metricValues(1);

        // Check that there are no updates on the node 1.
        testMetricValues(metrics1, actualMetrics1);

        // Check that all transaction metrics ere not changed except TotalCommits and RwCommits.
        testMetricValues(metrics0, actualMetrics0, "TotalCommits", "RwCommits");

        assertThat(actualMetrics0.get("TotalCommits"), is(metrics0.get("TotalCommits") + 1));
        assertThat(actualMetrics0.get("RwCommits"), is(metrics0.get("RwCommits") + 1));
    }

    /**
     * Tests that TotalCommits and RwCommits/RoCommits are incremented when an "empty" transaction successfully committed.
     * Empty means that there are no entries enlisted into the transaction.
     */
    @Test
    void testCommitEmptyTransaction() {
        Map<String, Long> metrics0 = metricValues(0);
        Map<String, Long> metrics1 = metricValues(1);

        Transaction rwTx = node(0).transactions().begin();
        rwTx.commit();

        Transaction roTx = node(0).transactions().begin(new TransactionOptions().readOnly(true));
        roTx.commit();

        Map<String, Long> actualMetrics0 = metricValues(0);
        Map<String, Long> actualMetrics1 = metricValues(1);

        // Check that there are no updates on the node 1.
        testMetricValues(metrics1, actualMetrics1);

        // Check that all transaction metrics ere not changed except TotalCommits, RwCommits and RoCommits.
        testMetricValues(metrics0, actualMetrics0, "TotalCommits", "RwCommits", "RoCommits");

        assertThat(actualMetrics0.get("TotalCommits"), is(metrics0.get("TotalCommits") + 2));
        assertThat(actualMetrics0.get("RwCommits"), is(metrics0.get("RwCommits") + 1));
        assertThat(actualMetrics0.get("RoCommits"), is(metrics0.get("RoCommits") + 1));
    }

    /**
     * Tests that TotalRollbacks and RwRollbacks/RoRollbacks are incremented when a transaction rolled back.
     */
    @Test
    @Disabled("https://issues.apache.org/jira/browse/IGNITE-27812")
    void testRollbackTransaction() {
        Map<String, Long> metrics0 = metricValues(0);
        Map<String, Long> metrics1 = metricValues(1);

        Transaction rwTx = node(0).transactions().begin();
        keyValueView(0).put(rwTx, 12, "value");
        rwTx.rollback();

        Transaction roTx = node(0).transactions().begin(new TransactionOptions().readOnly(true));
        keyValueView(0).get(roTx, 12);
        roTx.rollback();

        Map<String, Long> actualMetrics0 = metricValues(0);
        Map<String, Long> actualMetrics1 = metricValues(1);

        // Check that there are no updates on the node 1.
        testMetricValues(metrics1, actualMetrics1);

        // Check that all transaction metrics ere not changed except TotalRollbacks, RwRollbacks and RoRollbacks.
        testMetricValues(metrics0, actualMetrics0, "TotalRollbacks", "RwRollbacks", "RoRollbacks");

        assertThat(actualMetrics0.get("TotalRollbacks"), is(metrics0.get("TotalRollbacks") + 2));
        assertThat(actualMetrics0.get("RwRollbacks"), is(metrics0.get("RwRollbacks") + 1));
        assertThat(actualMetrics0.get("RoRollbacks"), is(metrics0.get("RoRollbacks") + 1));
    }

    /**
     * Tests that TotalRollbacks and RwRollbacks/RoRollbacks are incremented when a transaction rolled back due to timeout.
     */
    @Test
    void testTimeoutRollbackTransaction() throws Exception {
        Map<String, Long> metrics0 = metricValues(0);
        Map<String, Long> metrics1 = metricValues(1);

        Transaction rwTx = node(0).transactions().begin(new TransactionOptions().timeoutMillis(1000));
        keyValueView(0).put(rwTx, 12, "value");

        Transaction roTx = node(0).transactions().begin(new TransactionOptions().readOnly(true).timeoutMillis(1000));
        keyValueView(0).get(roTx, 12);

        // wait for completion of the transactions due to timeout.
        assertThat(waitForCondition(() -> {
            Map<String, Long> m = metricValues(0);

            boolean total = m.get("TotalRollbacks") == metrics0.get("TotalRollbacks") + 2;
            boolean rw = m.get("RwRollbacks") == metrics0.get("RwRollbacks") + 1;
            boolean ro = m.get("RoRollbacks") == metrics0.get("RoRollbacks") + 1;

            return total && rw && ro;
        }, 5_000), is(true));

        // Check that there are no updates on the node 1.
        testMetricValues(metrics1, metricValues(1));
    }

    /**
     * Tests that TotalRollbacks and RwRollbacks are incremented when a transaction rolled back due to a deadlock.
     */
    @Test
    void testDeadlockTransaction() throws Exception {
        Map<String, Long> metrics0 = metricValues(0);
        Map<String, Long> metrics1 = metricValues(1);

        KeyValueView<Integer, String> kv = keyValueView(0);

        Transaction rwTx1 = node(0).transactions().begin();
        Transaction rwTx2 = node(0).transactions().begin();

        kv.put(rwTx1, 12, "value");
        kv.put(rwTx2, 24, "value");

        CompletableFuture<?> asyncOp1 = kv.getAsync(rwTx1, 24);
        CompletableFuture<?> asyncOp2 = kv.getAsync(rwTx2, 12);

        assertThat(waitForCondition(() -> asyncOp1.isDone() && asyncOp2.isDone(), 5_000), is(true));

        rwTx1.commit();
        // rwTx2 should be rolled back due to a deadlock

        Map<String, Long> actualMetrics0 = metricValues(0);
        Map<String, Long> actualMetrics1 = metricValues(1);

        // Check that there are no updates on the node 1.
        testMetricValues(metrics1, actualMetrics1);

        // Check that all transaction metrics ere not changed except TotalRollbacks, TotalCommits, RwRollbacks and RwCommits.
        testMetricValues(metrics0, actualMetrics0, "TotalRollbacks", "TotalCommits", "RwRollbacks", "RwCommits");

        assertThat(actualMetrics0.get("TotalRollbacks"), is(metrics0.get("TotalRollbacks") + 1));
        assertThat(actualMetrics0.get("RwRollbacks"), is(metrics0.get("RwRollbacks") + 1));

        assertThat(actualMetrics0.get("TotalCommits"), is(metrics0.get("TotalCommits") + 1));
        assertThat(actualMetrics0.get("RwCommits"), is(metrics0.get("RwCommits") + 1));
    }

    /**
     * Tests that TotalRollbacks and RwRollbacks are incremented when a transaction rolled back due to a lease expiration.
     */
    @Test
    @Disabled("https://issues.apache.org/jira/browse/IGNITE-27812")
    void testRollbackTransactionOnLeaseExpiration() {
        Map<String, Long> metrics0 = metricValues(0);
        Map<String, Long> metrics1 = metricValues(1);

        int key = 12;

        TableImpl table = unwrapTableImpl(node(0).tables().table(TABLE_NAME));

        int partitionId = table.partitionId(Tuple.create().set("id", key));

        Transaction tx = node(0).transactions().begin();

        keyValueView(0).put(tx, key, "value");

        ZonePartitionId replicationGroupId = new ZonePartitionId(table.zoneId(), partitionId);

        ReplicaMeta leaseholder = NodeUtils.leaseholder(unwrapIgniteImpl(node(0)), replicationGroupId);

        IgniteImpl leaseholderNode = CLUSTER
                .runningNodes()
                .filter(n -> n.cluster().localNode().id().equals(leaseholder.getLeaseholderId()))
                .findFirst()
                .map(TestWrappers::unwrapIgniteImpl)
                .orElseThrow();

        NodeUtils.stopLeaseProlongation(
                CLUSTER.runningNodes().map(TestWrappers::unwrapIgniteImpl).collect(toSet()),
                leaseholderNode,
                replicationGroupId,
                null
        );

        // Wait for the lease expiration.
        unwrapIgniteImpl(node(0))
                .clockService()
                .waitFor(leaseholder.getExpirationTime().tick())
                .orTimeout(10, TimeUnit.SECONDS)
                .join();

        assertThrows(TransactionException.class, tx::commit, null);

        Map<String, Long> actualMetrics0 = metricValues(0);
        Map<String, Long> actualMetrics1 = metricValues(1);

        // Check that there are no updates on the node 1.
        testMetricValues(metrics1, actualMetrics1);

        // Check that all transaction metrics ere not changed except TotalCommits and RoCommits.
        testMetricValues(metrics0, actualMetrics0, "TotalRollbacks", "RwRollbacks");

        assertThat(actualMetrics0.get("TotalRollbacks"), is(metrics0.get("TotalRollbacks") + 1));
        assertThat(actualMetrics0.get("RwRollbacks"), is(metrics0.get("RwRollbacks") + 1));
    }

    /**
     * Tests that TotalCommits, RwCommits and RoCommits are incremented when a SQL engine is used.
     */
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testSqlTransaction(boolean implicit) {
        Object[] emptyArgs = new Object[0];

        Map<String, Long> metrics0 = metricValues(0);
        Map<String, Long> metrics1 = metricValues(1);

        // read-only transaction.
        Transaction tx = implicit ? null : node(0).transactions().begin(new TransactionOptions().readOnly(true));
        sql(0, tx, "select * from " + TABLE_NAME, emptyArgs);
        if (!implicit) {
            tx.commit();
        }

        // read-write transaction.
        tx = implicit ? null : node(0).transactions().begin();
        sql(0, tx, "delete from " + TABLE_NAME, emptyArgs);
        if (!implicit) {
            tx.commit();
        }

        Map<String, Long> actualMetrics0 = metricValues(0);
        Map<String, Long> actualMetrics1 = metricValues(1);

        // Check that there are no updates on the node 1.
        testMetricValues(metrics1, actualMetrics1);

        // Check that all transaction metrics ere not changed except TotalCommits and RoCommits.
        testMetricValues(metrics0, actualMetrics0, "TotalCommits", "RwCommits", "RoCommits");

        assertThat(actualMetrics0.get("TotalCommits"), is(metrics0.get("TotalCommits") + 2));
        assertThat(actualMetrics0.get("RoCommits"), is(metrics0.get("RoCommits") + 1));
        assertThat(actualMetrics0.get("RwCommits"), is(metrics0.get("RwCommits") + 1));
    }

    @Test
    void globalPendingWriteIntentsMetric() throws Exception {
        String zoneName = "zone_single_partition_no_replicas_tx_metrics";

        String table1 = "test_table_pending_wi_1";
        String table2 = "test_table_pending_wi_2";

        sql("CREATE ZONE " + zoneName + " (PARTITIONS 1, REPLICAS 1) storage profiles ['default']");

        sql("CREATE TABLE " + table1 + "(id INT PRIMARY KEY, val INT) ZONE " + zoneName);
        sql("CREATE TABLE " + table2 + "(id INT PRIMARY KEY, val INT) ZONE " + zoneName);

        Transaction tx = node(0).transactions().begin();

        int table1Inserts = 3;
        int table2Inserts = 5;

        try {
            for (int i = 0; i < table1Inserts; i++) {
                sql(tx, "INSERT INTO " + table1 + " VALUES(?, ?)", i, i);
            }

            for (int i = 0; i < table2Inserts; i++) {
                sql(tx, "INSERT INTO " + table2 + " VALUES(?, ?)", i, i);
            }

            Awaitility.await()
                    .atMost(Duration.ofSeconds(10))
                    .untilAsserted(() -> assertThat(totalPendingWriteIntents(), is((long) table1Inserts + table2Inserts)));
        } finally {
            tx.commit();
        }

        Awaitility.await()
                .atMost(Duration.ofSeconds(10))
                .untilAsserted(() -> assertThat(totalPendingWriteIntents(), is(0L)));
    }

    private static long totalPendingWriteIntents() {
        long sum = 0;

        for (int i = 0; i < CLUSTER.nodes().size(); i++) {
            sum += pendingWriteIntentsOnNode(i);
        }

        return sum;
    }

    private static long pendingWriteIntentsOnNode(int nodeIdx) {
        MetricSet metrics = unwrapIgniteImpl(node(nodeIdx))
                .metricManager()
                .metricSnapshot()
                .metrics()
                .get(TransactionMetricsSource.SOURCE_NAME);

        assertThat("Transaction metrics must be present on node " + nodeIdx, metrics != null, is(true));

        LongMetric metric = metrics.get(TransactionMetricsSource.METRIC_PENDING_WRITE_INTENTS);

        assertThat("Metric must be present: "
                + TransactionMetricsSource.METRIC_PENDING_WRITE_INTENTS, metric != null, is(true));

        return metric.value();
    }
}
