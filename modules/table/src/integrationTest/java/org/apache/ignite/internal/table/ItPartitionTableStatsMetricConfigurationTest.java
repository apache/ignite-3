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

import static org.apache.ignite.internal.catalog.CatalogService.DEFAULT_STORAGE_PROFILE;
import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;
import static org.apache.ignite.internal.table.ItPartitionTableStatsMetricTest.expectNextMilestone;

import org.apache.ignite.InitParametersBuilder;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Check that partition modification counter settings are depends on configuration settings. */
// TODO: Revisit test during https://issues.apache.org/jira/browse/IGNITE-28002
public class ItPartitionTableStatsMetricConfigurationTest extends BasePartitionTableStatsMetricTest {
    private static final String ZONE_1_PART_NO_REPLICAS = "zone_single_partition_no_replicas";
    private static final int MIN_STALE_ROWS = 2;
    private static final double STALE_ROWS_FRACTION = 1.0d;

    @BeforeAll
    void setupDistributionZones() {
        sql(format("CREATE ZONE {} (PARTITIONS 1, REPLICAS 1) storage profiles ['" + DEFAULT_STORAGE_PROFILE + "'];",
                ZONE_1_PART_NO_REPLICAS));
    }

    @BeforeEach
    void dropTables() {
        dropAllTables();
    }

    @Override
    protected void configureInitParameters(InitParametersBuilder builder) {
        String clusterConfiguration = format(
                "ignite { sql: {createTable: { staleRowsFraction: {}, minStaleRowsCount: {} } } }",
                STALE_ROWS_FRACTION, MIN_STALE_ROWS
        );

        builder.clusterConfiguration(clusterConfiguration);
    }

    @Override
    protected int initialNodes() {
        return 1;
    }

    @Test
    void testStaleConfigurationApplied() {
        String tableName = "T1";

        sql(format("CREATE TABLE {}(id INT PRIMARY KEY, val INT) ZONE {};", tableName, ZONE_1_PART_NO_REPLICAS));

        sql(format("INSERT INTO {} VALUES(0, 0), (1, 1);", tableName));

        enableStats(tableName);

        expectNextMilestone(tableName, 2 * MIN_STALE_ROWS);

        sql(format("INSERT INTO {} VALUES(2, 2), (3, 3);", tableName));

        expectNextMilestone(tableName, 2 * 2 * MIN_STALE_ROWS);
    }
}
