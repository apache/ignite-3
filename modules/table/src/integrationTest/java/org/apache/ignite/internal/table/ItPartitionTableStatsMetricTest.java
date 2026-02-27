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

import static org.apache.ignite.internal.catalog.commands.CatalogUtils.DEFAULT_MIN_STALE_ROWS_COUNT;
import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;
import static org.apache.ignite.internal.table.distributed.PartitionTableStatsMetricSource.METRIC_COUNTER;
import static org.apache.ignite.internal.table.distributed.PartitionTableStatsMetricSource.METRIC_LAST_MILESTONE_TIMESTAMP;
import static org.apache.ignite.internal.table.distributed.PartitionTableStatsMetricSource.METRIC_NEXT_MILESTONE;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;

import java.util.Map;
import org.apache.ignite.internal.table.distributed.PartitionModificationCounter;
import org.apache.ignite.internal.table.distributed.PartitionTableStatsMetricSource;
import org.apache.ignite.table.KeyValueView;
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
// TODO: Revisit test during https://issues.apache.org/jira/browse/IGNITE-28002
public class ItPartitionTableStatsMetricTest extends BasePartitionTableStatsMetricTest {
    private static final String ZONE_1_PART_NO_REPLICAS = "zone_single_partition_no_replicas";
    private static final String ZONE_1_PART_REPLICAS = "zone_single_partition";
    private static final String ZONE_8_PART_NO_REPLICAS = "zone_multi_partition";

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
        String t1 = "T1";
        String t2 = "T2";

        sqlScript(
                format("CREATE TABLE {}(id INT PRIMARY KEY, val INT) ZONE {};", t1, ZONE_1_PART_NO_REPLICAS),
                format("CREATE TABLE {}(id INT PRIMARY KEY, val INT) ZONE {};", t2, ZONE_1_PART_NO_REPLICAS)
        );
        enableStats(t1);
        enableStats(t2);

        sql(format("INSERT INTO {} VALUES(0, 0), (1, 1);", t1));

        expectModsCount(t1, 2L);
        expectNextMilestone(t1, DEFAULT_MIN_STALE_ROWS_COUNT);

        sql(format("INSERT INTO {} VALUES(0, 0), (1, 1), (2, 2);", t2));

        expectModsCount(t2, 3L);
        expectNextMilestone(t2, DEFAULT_MIN_STALE_ROWS_COUNT);

        expectModsCount(t1, 2L);
    }


    /**
     * Tests that dropping and creating a table with the same name resets the counter value.
     */
    @Test
    void recreateTableWithTheSameName() {
        String table = "test_table";

        sqlScript(
                format("CREATE TABLE {}(id INT PRIMARY KEY, val INT) ZONE {};", table, ZONE_1_PART_NO_REPLICAS),
                format("INSERT INTO {} VALUES(0, 0), (1, 1);", table)
        );
        enableStats(table);
        expectModsCount(table, 2);

        sqlScript(
                format("DROP TABLE {}", table),
                format("CREATE TABLE {}(id INT PRIMARY KEY, val INT) ZONE {};", table, ZONE_1_PART_NO_REPLICAS)
        );
        enableStats(table);
        expectModsCount(table, 0);

        sql(format("INSERT INTO {} VALUES(0, 0), (1, 1);", table));
        expectModsCount(table, 2);

    }

    /**
     * Tests that different types of updates are counted.
     */
    @Test
    void differentUpdateTypes() {
        String table = "test_table";

        sql(format("CREATE TABLE {}(id INT PRIMARY KEY, val INT) ZONE {};", table, ZONE_8_PART_NO_REPLICAS));
        enableStats(table);

        KeyValueView<Integer, Integer> keyValueView = CLUSTER.aliveNode().tables().table(table)
                .keyValueView(Integer.class, Integer.class);

        int expectedMods = 0;

        // Implicit transaction.
        {
            sql(format("INSERT INTO {} VALUES(0, 0);", table));
            expectModsCount(table, ++expectedMods);

            sql(format("UPDATE {} SET val=1 WHERE id=0", table));
            expectModsCount(table, ++expectedMods);

            keyValueView.put(null, 0, 2);
            expectModsCount(table, ++expectedMods);

            sql(format("INSERT INTO {} VALUES(1, 1), (2, 2);", table));
            expectedMods += 2;
            expectModsCount(table, expectedMods);

            keyValueView.putAll(null, Map.of(3, 3, 4, 4, 5, 5));
            expectedMods += 3;
            expectModsCount(table, expectedMods);

            sql(format("UPDATE {} SET val=20 WHERE val = 2", table));
            expectedMods += 2;
            expectModsCount(table, expectedMods);

            sql(format("DELETE FROM {}", table));
            expectedMods += 6;
            expectModsCount(table, expectedMods);
        }

        // Explicit transaction.
        {
            {
                Transaction tx = CLUSTER.aliveNode().transactions().begin();

                sql(tx, format("INSERT INTO {} VALUES(0, 0)", table));
                expectModsCount(table, expectedMods);

                tx.commit();
                expectModsCount(table, ++expectedMods);
            }

            {
                Transaction tx = CLUSTER.aliveNode().transactions().begin();

                sql(tx, format("UPDATE {} SET val=1 WHERE id=0", table));
                keyValueView.put(tx, 0, 2);

                sql(tx, format("INSERT INTO {} VALUES(1, 1), (2, 2)", table));
                keyValueView.putAll(tx, Map.of(3, 3, 4, 4, 5, 5));

                expectModsCount(table, expectedMods);

                tx.commit();
                expectedMods += 6;
                expectModsCount(table, expectedMods);
            }

            {
                Transaction tx = CLUSTER.aliveNode().transactions().begin();

                sql(tx, format("UPDATE {} SET val=20 WHERE val=2", table));
                sql(tx, format("DELETE FROM {}", table));
                expectModsCount(table, expectedMods);

                tx.commit();
                expectedMods += 6;
                expectModsCount(table, expectedMods);
            }
        }

        for (int part = 0; part < 8; part++) {
            assertThat(metricFromAnyNode(table, part, METRIC_NEXT_MILESTONE), is(DEFAULT_MIN_STALE_ROWS_COUNT));
        }

    }

    /**
     * Tests that the milestone timestamp is updated only when the number of modifications reaches the configured threshold.
     */
    @Test
    void reachMilestoneUpdateTest() {
        String tableWithReplicas = "test_table";
        String tableWithoutReplicas = "test_table_no_replicas";

        int replicas = initialNodes();

        sqlScript(
                format("CREATE TABLE {}(id INT PRIMARY KEY, val INT) ZONE {};", tableWithReplicas, ZONE_1_PART_REPLICAS),
                format("CREATE TABLE {}(id INT PRIMARY KEY, val INT) ZONE {};", tableWithoutReplicas, ZONE_1_PART_NO_REPLICAS),
                format("INSERT INTO {} VALUES(0, 0);", tableWithReplicas),
                format("INSERT INTO {} VALUES(0, 0);", tableWithoutReplicas)
        );
        enableStats(tableWithReplicas);
        enableStats(tableWithoutReplicas);

        expectModsCount(tableWithoutReplicas, 1);
        expectModsCount(tableWithReplicas, replicas);

        long initTsNoReplicas = metricFromAnyNode(tableWithoutReplicas, 0, METRIC_LAST_MILESTONE_TIMESTAMP);
        long initTsWithReplicas = metricFromAnyNode(tableWithReplicas, 0, METRIC_LAST_MILESTONE_TIMESTAMP);

        long modsCount = DEFAULT_MIN_STALE_ROWS_COUNT / 2;

        // Perform a bunch of modifications.
        {
            for (int i = 0; i < modsCount; i++) {
                sql(format("UPDATE  {} SET VAL=?", tableWithReplicas), i);
                sql(format("UPDATE  {} SET VAL=?", tableWithoutReplicas), i);
            }

            long expectedModsCount = 1 + modsCount;

            expectModsCount(tableWithoutReplicas, expectedModsCount);
            expectModsCount(tableWithReplicas, expectedModsCount * replicas);

            // Timestamp should not change as we did not reach the threshold.
            assertThat(metricFromAnyNode(tableWithoutReplicas, 0, METRIC_LAST_MILESTONE_TIMESTAMP), is(initTsNoReplicas));
            assertThat(metricFromAnyNode(tableWithReplicas, 0, METRIC_LAST_MILESTONE_TIMESTAMP), is(initTsWithReplicas));

            assertThat(metricFromAnyNode(tableWithoutReplicas, 0, METRIC_NEXT_MILESTONE), is(DEFAULT_MIN_STALE_ROWS_COUNT));
            assertThat(metricFromAnyNode(tableWithReplicas, 0, METRIC_NEXT_MILESTONE), is(DEFAULT_MIN_STALE_ROWS_COUNT));
        }

        // Perform another bunch of modifications to reach the milestone.
        {
            for (int i = 0; i < modsCount; i++) {
                sql(format("UPDATE  {} SET VAL=?", tableWithReplicas), i);
                sql(format("UPDATE  {} SET VAL=?", tableWithoutReplicas), i);
            }

            long expectedModsCount = 1 + modsCount + modsCount;

            expectModsCount(tableWithoutReplicas, expectedModsCount);
            expectModsCount(tableWithReplicas, expectedModsCount * replicas);

            // Timestamp should change because we reached the threshold.
            Awaitility.await().untilAsserted(() ->
                    assertThat(metricFromAnyNode(tableWithoutReplicas, 0, METRIC_LAST_MILESTONE_TIMESTAMP), greaterThan(initTsNoReplicas))
            );
            Awaitility.await().untilAsserted(() ->
                    assertThat(metricFromAnyNode(tableWithReplicas, 0, METRIC_LAST_MILESTONE_TIMESTAMP),
                            greaterThan(initTsWithReplicas))
            );

            Awaitility.await().untilAsserted(() ->
                    assertThat(metricFromAnyNode(tableWithoutReplicas, 0, METRIC_NEXT_MILESTONE), is(DEFAULT_MIN_STALE_ROWS_COUNT * 2))
            );
            Awaitility.await().untilAsserted(() ->
                    assertThat(metricFromAnyNode(tableWithReplicas, 0, METRIC_NEXT_MILESTONE), is(DEFAULT_MIN_STALE_ROWS_COUNT * 2))
            );
        }
    }

    private static void expectAggregatedMetricValue(String tableName, long value, String metricName) {
        TableTuple table = TableTuple.of(tableName);

        Awaitility.await().untilAsserted(() -> {
            long aggregatedValue = 0;

            for (int part = 0; part < table.partsCount; part++) {
                String metricSourceName = PartitionTableStatsMetricSource.formatSourceName(table.id, part);

                boolean metricFound = false;

                for (int i = 0; i < CLUSTER.nodes().size(); i++) {
                    long metricValue = metricFromNode(i, table.id, part, metricName);

                    if (metricValue != UNDEFINED_METRIC_VALUE) {
                        metricFound = true;

                        aggregatedValue += metricValue;
                    }
                }

                if (!metricFound) {
                    throw new IllegalArgumentException("Metrics not found " + metricSourceName);
                }
            }

            assertThat(aggregatedValue, is(value));
        });
    }

    private static void expectModsCount(String tableName, long value) {
        expectAggregatedMetricValue(tableName, value, METRIC_COUNTER);
    }

    static void expectNextMilestone(String tableName, long value) {
        expectAggregatedMetricValue(tableName, value, METRIC_NEXT_MILESTONE);
    }
}
