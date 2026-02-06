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

import static org.apache.ignite.internal.TestWrappers.unwrapIgniteImpl;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.DEFAULT_MIN_STALE_ROWS_COUNT;
import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;
import static org.apache.ignite.internal.table.distributed.PartitionTableStatsMetricSource.METRIC_COUNTER;
import static org.apache.ignite.internal.table.distributed.PartitionTableStatsMetricSource.METRIC_LAST_MILESTONE_TIMESTAMP;
import static org.apache.ignite.internal.table.distributed.PartitionTableStatsMetricSource.METRIC_NEXT_MILESTONE;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.util.Map;
import java.util.Objects;
import org.apache.ignite.internal.catalog.Catalog;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableDescriptor;
import org.apache.ignite.internal.metrics.LongMetric;
import org.apache.ignite.internal.metrics.MetricManager;
import org.apache.ignite.internal.metrics.MetricSet;
import org.apache.ignite.internal.sql.BaseSqlIntegrationTest;
import org.apache.ignite.internal.table.distributed.PartitionModificationCounter;
import org.apache.ignite.internal.table.distributed.PartitionTableStatsMetricSource;
import org.apache.ignite.table.KeyValueView;
import org.apache.ignite.table.QualifiedName;
import org.apache.ignite.tx.Transaction;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Integration tests for table partition statistics metrics exposed via {@link PartitionTableStatsMetricSource}.
 *
 * <p>Includes {@link PartitionModificationCounter partition modification counter} metrics.
 */
public class ItPartitionTableStatsMetricTest extends BaseSqlIntegrationTest {
    private static final String ZONE_1_PART_NO_REPLICAS = "zone_single_partition_no_replicas";
    private static final String ZONE_1_PART_REPLICAS = "zone_single_partition";
    private static final String ZONE_8_PART_NO_REPLICAS = "zone_multi_partition";

    private static final int UNDEFINED_METRIC_VALUE = -1;

    @BeforeAll
    void setupDistributionZones() {
        sqlScript(
                format("CREATE ZONE {} (PARTITIONS 1, REPLICAS {}) storage profiles ['default'];", ZONE_1_PART_REPLICAS, initialNodes()),
                format("CREATE ZONE {} (PARTITIONS 1, REPLICAS 1) storage profiles ['default'];", ZONE_1_PART_NO_REPLICAS),
                format("CREATE ZONE {} (PARTITIONS 8, REPLICAS 1) storage profiles ['default'];", ZONE_8_PART_NO_REPLICAS)
        );
    }

    @BeforeEach
    void dropTables() {
        dropAllTables();
    }

    /**
     * Tests that counters are updated independently for tables in the same zone.
     */
    @Test
    void twoTablesInTheSameZone() {
        String tab1 = "T1";
        String tab2 = "T2";

        sqlScript(
                format("CREATE TABLE {}(id INT PRIMARY KEY, val INT) ZONE {};", tab1, ZONE_1_PART_NO_REPLICAS),
                format("CREATE TABLE {}(id INT PRIMARY KEY, val INT) ZONE {};", tab2, ZONE_1_PART_NO_REPLICAS)
        );

        sql(format("INSERT INTO {} VALUES(0, 0), (1, 1);", tab1));

        expectModsCount(tab1, 2L);
        expectNextMilestone(tab1, DEFAULT_MIN_STALE_ROWS_COUNT);

        sql(format("INSERT INTO {} VALUES(0, 0), (1, 1), (2, 2);", tab2));

        expectModsCount(tab2, 3L);
        expectNextMilestone(tab2, DEFAULT_MIN_STALE_ROWS_COUNT);

        expectModsCount(tab1, 2L);
    }

    /**
     * Tests that dropping and creating a table with the same name resets the counter value.
     */
    @Test
    void recreateTableWithTheSameName() {
        String table = "test_table";

        sqlScript(
                format("CREATE TABLE {}(id INT PRIMARY KEY, val INT) ZONE {};", table, ZONE_1_PART_NO_REPLICAS),
                "INSERT INTO test_table VALUES(0, 0), (1, 1);"
        );
        expectModsCount(table, 2);

        sqlScript(
                format("DROP TABLE {}", table),
                format("CREATE TABLE {}(id INT PRIMARY KEY, val INT) ZONE {};", table, ZONE_1_PART_NO_REPLICAS)
        );
        expectModsCount(table, 0);

        sql("INSERT INTO test_table VALUES(0, 0), (1, 1);");
        expectModsCount(table, 2);
    }

    /**
     * Tests that different types of updates are counted.
     */
    @Test
    void differentUpdateTypes() {
        String tabName = "test_table";
        sql(format("CREATE TABLE {}(id INT PRIMARY KEY, val INT) ZONE {};", tabName, ZONE_8_PART_NO_REPLICAS));
        KeyValueView<Integer, Integer> keyValueView = CLUSTER.aliveNode().tables().table("test_table")
                .keyValueView(Integer.class, Integer.class);

        int expectedMods = 0;

        // Implicit transaction.
        {
            sql("INSERT INTO test_table VALUES(0, 0);");
            expectModsCount(tabName, ++expectedMods);

            sql("UPDATE test_table SET val=1 WHERE id=0");
            expectModsCount(tabName, ++expectedMods);

            keyValueView.put(null, 0, 2);
            expectModsCount(tabName, ++expectedMods);

            sql("INSERT INTO test_table VALUES(1, 1), (2, 2);");
            expectedMods += 2;
            expectModsCount(tabName, expectedMods);

            keyValueView.putAll(null, Map.of(3, 3, 4, 4, 5, 5));
            expectedMods += 3;
            expectModsCount(tabName, expectedMods);

            sql("UPDATE test_table SET val=20 WHERE val = 2");
            expectedMods += 2;
            expectModsCount(tabName, expectedMods);

            sql("DELETE FROM test_table");
            expectedMods += 6;
            expectModsCount(tabName, expectedMods);
        }

        // Explicit transaction.
        {
            {
                Transaction tx = CLUSTER.aliveNode().transactions().begin();
                sql(tx, "INSERT INTO test_table VALUES(0, 0);");
                expectModsCount(tabName, expectedMods);
                tx.commit();
                expectModsCount(tabName, ++expectedMods);
            }

            {
                Transaction tx = CLUSTER.aliveNode().transactions().begin();

                sql(tx, "UPDATE test_table SET val=1 WHERE id=0");
                keyValueView.put(tx, 0, 2);
                sql(tx, "INSERT INTO test_table VALUES(1, 1), (2, 2);");
                keyValueView.putAll(tx, Map.of(3, 3, 4, 4, 5, 5));
                expectModsCount(tabName, expectedMods);

                tx.commit();
                expectedMods += 6;
                expectModsCount(tabName, expectedMods);
            }

            {
                Transaction tx = CLUSTER.aliveNode().transactions().begin();

                sql(tx, "UPDATE test_table SET val=20 WHERE val = 2");
                sql(tx, "DELETE FROM test_table");
                expectModsCount(tabName, expectedMods);

                tx.commit();
                expectedMods += 6;
                expectModsCount(tabName, expectedMods);
            }
        }

        for (int part = 0; part < 8; part++) {
            assertThat(metricFromAnyNode(tabName, part, METRIC_NEXT_MILESTONE), is(DEFAULT_MIN_STALE_ROWS_COUNT));
        }
    }

    /**
     * Tests that the milestone timestamp is updated only when
     * the number of modifications reaches the configured threshold.
     */
    @Test
    void reachMilestoneUpdateTest() {
        String tableWithReplicas = "TEST_TABLE";
        String tableNoReplicas = "TEST_TABLE_NO_REPLICAS";

        int replicas = initialNodes();

        sqlScript(
                format("CREATE TABLE {}(id INT PRIMARY KEY, val INT) ZONE {};", tableWithReplicas, ZONE_1_PART_REPLICAS),
                format("CREATE TABLE {}(id INT PRIMARY KEY, val INT) ZONE {};", tableNoReplicas, ZONE_1_PART_NO_REPLICAS),
                format("INSERT INTO {} VALUES(0, 0);", tableWithReplicas),
                format("INSERT INTO {} VALUES(0, 0);", tableNoReplicas)
        );

        expectModsCount(tableNoReplicas, 1);
        expectModsCount(tableWithReplicas, replicas);

        long initTsNoReplicas = metricFromAnyNode(tableNoReplicas, 0, METRIC_LAST_MILESTONE_TIMESTAMP);
        long initTsWithReplicas = metricFromAnyNode(tableWithReplicas, 0, METRIC_LAST_MILESTONE_TIMESTAMP);

        long modsCount = DEFAULT_MIN_STALE_ROWS_COUNT / 2;

        // Perform a bunch of modifications.
        {
            for (int i = 0; i < modsCount; i++) {
                sql(format("UPDATE  {} SET VAL=?", tableWithReplicas), i);
                sql(format("UPDATE  {} SET VAL=?", tableNoReplicas), i);
            }

            long expectedModsCount = 1 + modsCount;

            expectModsCount(tableNoReplicas, expectedModsCount);
            expectModsCount(tableWithReplicas, expectedModsCount * replicas);

            // Timestamp should not change as we did not reach the threshold.
            assertThat(metricFromAnyNode(tableNoReplicas, 0, METRIC_LAST_MILESTONE_TIMESTAMP), is(initTsNoReplicas));
            assertThat(metricFromAnyNode(tableWithReplicas, 0, METRIC_LAST_MILESTONE_TIMESTAMP), is(initTsWithReplicas));

            assertThat(metricFromAnyNode(tableNoReplicas, 0, METRIC_NEXT_MILESTONE), is(DEFAULT_MIN_STALE_ROWS_COUNT));
            assertThat(metricFromAnyNode(tableWithReplicas, 0, METRIC_NEXT_MILESTONE), is(DEFAULT_MIN_STALE_ROWS_COUNT));
        }

        // Perform another bunch of modifications to reach the milestone.
        {
            for (int i = 0; i < modsCount; i++) {
                sql(format("UPDATE  {} SET VAL=?", tableWithReplicas), i);
                sql(format("UPDATE  {} SET VAL=?", tableNoReplicas), i);
            }

            long expectedModsCount = 1 + modsCount + modsCount;

            expectModsCount(tableNoReplicas, expectedModsCount);
            expectModsCount(tableWithReplicas, expectedModsCount * replicas);

            // Timestamp should change because we reached the threshold.
            Awaitility.await().untilAsserted(() ->
                    assertThat(metricFromAnyNode(tableNoReplicas, 0, METRIC_LAST_MILESTONE_TIMESTAMP), greaterThan(initTsNoReplicas))
            );
            Awaitility.await().untilAsserted(() ->
                    assertThat(metricFromAnyNode(tableWithReplicas, 0, METRIC_LAST_MILESTONE_TIMESTAMP), greaterThan(initTsWithReplicas))
            );

            Awaitility.await().untilAsserted(() ->
                    assertThat(metricFromAnyNode(tableNoReplicas, 0, METRIC_NEXT_MILESTONE), is(DEFAULT_MIN_STALE_ROWS_COUNT * 2))
            );
            Awaitility.await().untilAsserted(() ->
                    assertThat(metricFromAnyNode(tableWithReplicas, 0, METRIC_NEXT_MILESTONE), is(DEFAULT_MIN_STALE_ROWS_COUNT * 2))
            );
        }
    }

    private void expectModsCount(String tableName, long value) {
        expectLongValue(tableName, value, METRIC_COUNTER);
    }

    static void expectNextMilestone(String tableName, long value) {
        expectLongValue(tableName, value, METRIC_NEXT_MILESTONE);
    }

    private static void expectLongValue(String tableName, long value, String metricName) {
        QualifiedName qualifiedName = QualifiedName.parse(tableName);
        Catalog catalog = unwrapIgniteImpl(node(0)).catalogManager().latestCatalog();
        CatalogTableDescriptor tableDesc = catalog.table(qualifiedName.schemaName(), qualifiedName.objectName());
        int partsCount = catalog.zone(tableDesc.zoneId()).partitions();

        Awaitility.await().untilAsserted(() -> {
            long summaryValue = 0;

            for (int part = 0; part < partsCount; part++) {
                int tableId = tableIdByName(QualifiedName.parse(tableName));

                String metricSourceName =
                        PartitionTableStatsMetricSource.formatSourceName(tableId, part);

                boolean metricFound = false;

                for (int i = 0; i < CLUSTER.nodes().size(); i++) {
                    long metricValue = metricFromNode(i, tableName, part, metricName);

                    if (metricValue != UNDEFINED_METRIC_VALUE) {
                        metricFound = true;

                        summaryValue += metricValue;
                    }
                }

                if (!metricFound) {
                    throw new IllegalArgumentException("Metrics not found " + metricSourceName);
                }
            }

            assertThat(summaryValue, is(value));
        });
    }

    private static int tableIdByName(QualifiedName qualifiedName) {
        CatalogTableDescriptor tableDesc = unwrapIgniteImpl(node(0)).catalogManager()
                .latestCatalog()
                .table(qualifiedName.schemaName(), qualifiedName.objectName());

        assertNotNull(tableDesc);

        return tableDesc.id();
    }

    private long metricFromAnyNode(String tableName, int partId, String metricName) {
        for (int i = 0; i < CLUSTER.nodes().size(); i++) {
            long value = metricFromNode(i, tableName, partId, metricName);

            if (value != UNDEFINED_METRIC_VALUE) {
                return value;
            }
        }

        return UNDEFINED_METRIC_VALUE;
    }

    private static long metricFromNode(int nodeIdx, String tableName, int partId, String metricName) {
        int tableId = tableIdByName(QualifiedName.parse(tableName));

        String metricSourceName =
                PartitionTableStatsMetricSource.formatSourceName(tableId, partId);

        MetricManager metricManager = unwrapIgniteImpl(node(nodeIdx)).metricManager();

        MetricSet metrics = metricManager.metricSnapshot().metrics().get(metricSourceName);

        if (metrics != null) {
            LongMetric metric = metrics.get(metricName);
            Objects.requireNonNull(metric, "metric does not exist: " + metricName);

            return metric.value();
        }

        return UNDEFINED_METRIC_VALUE;
    }

    private static void sqlScript(String ... queries) {
        CLUSTER.aliveNode().sql().executeScript(String.join(";", queries));
    }
}
