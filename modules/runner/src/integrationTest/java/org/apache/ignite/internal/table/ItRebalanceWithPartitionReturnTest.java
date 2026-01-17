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

import static java.util.stream.Collectors.toSet;
import static org.apache.ignite.internal.TestDefaultProfilesNames.DEFAULT_AIPERSIST_PROFILE_NAME;
import static org.apache.ignite.internal.TestWrappers.unwrapIgniteImpl;
import static org.apache.ignite.internal.TestWrappers.unwrapTableImpl;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.DEFAULT_FILTER;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.stream.IntStream;
import org.apache.ignite.Ignite;
import org.apache.ignite.internal.ClusterPerTestIntegrationTest;
import org.apache.ignite.internal.ConfigOverride;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.catalog.descriptors.ConsistencyMode;
import org.apache.ignite.internal.partitiondistribution.Assignment;
import org.apache.ignite.internal.replicator.ZonePartitionId;
import org.apache.ignite.sql.ResultSet;
import org.apache.ignite.sql.SqlRow;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Checks that the data is not lost when the partition is moved to another node and then moved back.
 */
public class ItRebalanceWithPartitionReturnTest extends ClusterPerTestIntegrationTest {
    /** URL. */
    private static final String URL = "jdbc:ignite:thin://127.0.0.1:10800";

    /** Connection. */
    private static Connection conn;

    /** Default schema. */
    private static final String DEFAULT_SCHEMA = "PUBLIC";

    /** Statement. */
    private Statement stmt;

    private static final String ZONE_NAME = "TEST_ZONE";

    private static final String TABLE_NAME = "table1";

    private static final String STORAGE_PROFILES = String.format("'%s'", DEFAULT_AIPERSIST_PROFILE_NAME);

    private static final String COLUMN_KEY = "key";

    private static final String COLUMN_VAL = "val";

    @Override
    protected int initialNodes() {
        return 3;
    }

    @BeforeEach
    protected void setUpBase() throws Exception {
        conn = DriverManager.getConnection(URL);
        conn.setSchema(DEFAULT_SCHEMA);
        conn.setAutoCommit(true);
        conn.setSchema(DEFAULT_SCHEMA);
        stmt = conn.createStatement();
        assert stmt != null;
        assert !stmt.isClosed();
    }

    @AfterEach
    protected void tearDownBase() throws Exception {
        if (stmt != null) {
            stmt.close();
            assert stmt.isClosed();
        }
        conn.close();
        conn = null;
    }

    private void countThroughJdbc(int rowCount) throws SQLException {
        int actualCount = 0;
        try (Statement stmt = conn.createStatement()) {
            try (java.sql.ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM " + TABLE_NAME)) {
                while (rs.next()) {
                    log.info("JDBC result is " + rs.getInt(1));
                    actualCount = rs.getInt(1);
                }
            }
        }
        assertEquals(rowCount, actualCount);
    }

    @Test
    @ConfigOverride(name = "ignite.nodeAttributes", value = "{ nodeAttributes: {region = US, storage = SSD}}")
    @ConfigOverride(name = "ignite.nodeAttributes", value = "{ nodeAttributes: {region = EU, storage = SSD}}", nodeIndex = 0)
    public void test() throws Exception {
        String filter = DEFAULT_FILTER;
        int partCount = 24;
        int rowCount = 400;

        Ignite node0 = unwrapIgniteImpl(node(0));

        node0.sql().execute(
                createZoneSql(partCount, 1, 10_000, 10_000, filter, STORAGE_PROFILES, ConsistencyMode.STRONG_CONSISTENCY)
        );

        node0.sql().execute(createTableSql());

        // First transaction.
        StringBuilder sqlInsert = new StringBuilder("INSERT INTO " + TABLE_NAME + " (key, val) VALUES \n");

        for (int i = 0; i < rowCount / 2; i++) {
            sqlInsert.append("(").append(i + 1).append(", 'value" + (i + 1) + "')");
            if (i != rowCount / 2 - 1) {
                sqlInsert.append(", \n");
            }
        }

        node0.sql().execute(sqlInsert.toString());

        // Second transaction.
        sqlInsert = new StringBuilder("INSERT INTO " + TABLE_NAME + " (key, val) VALUES \n");

        for (int i = rowCount / 2; i < rowCount; i++) {
            sqlInsert.append("(").append(i + 1).append(", 'value" + (i + 1) + "')");
            if (i != rowCount - 1) {
                sqlInsert.append(", \n");
            }
        }

        node0.sql().execute(sqlInsert.toString());

        String changedFilter = "$[?(@.region == \"US\")]";
        node0.sql().execute(alterZoneSql(changedFilter));

        IgniteImpl igniteImpl = unwrapIgniteImpl(node0);
        TableImpl tableImpl = unwrapTableImpl(node0.tables().table(TABLE_NAME));

        try {
            assertTrue(waitForCondition(() -> {
                Set<String> nodeNames = new HashSet<>();

                for (int j = 0; j < partCount; j++) {
                    ZonePartitionId replicationGroupId = new ZonePartitionId(tableImpl.zoneId(), j);

                    Set<Assignment> nodes = unwrapIgniteImpl(node0)
                            .placementDriver()
                            .getAssignments(replicationGroupId, igniteImpl.clock().current())
                            .join()
                            .nodes();

                    nodeNames.addAll(nodes.stream().map(Assignment::consistentId).collect(toSet()));
                }

                return nodeNames.size() == 2;
            }, 10_000));
        } catch (AssertionError e) {
            Set<String> nodeNames = new HashSet<>();

            for (int j = 0; j < partCount; j++) {
                ZonePartitionId replicationGroupId = new ZonePartitionId(tableImpl.zoneId(), j);

                Set<Assignment> nodes = unwrapIgniteImpl(node0)
                        .placementDriver()
                        .getAssignments(replicationGroupId, igniteImpl.clock().current())
                        .join()
                        .nodes();

                nodeNames.addAll(nodes.stream().map(Assignment::consistentId).collect(toSet()));
            }

            throw new AssertionError("Test: wrong assignments, nodes=" + nodeNames, e);
        }

        // Check count.
        countThroughJdbc(rowCount);
        try (ResultSet<SqlRow> rs = node0.sql().execute("SELECT COUNT(*) FROM " + TABLE_NAME)) {
            SqlRow row = rs.next();
            log.info("SRV result is " + row.longValue(0));
            assertEquals(rowCount, row.longValue(0));
        }

        TableImpl tableOnNode0 = unwrapTableImpl(node0.tables().table(TABLE_NAME));

        // Wait for storages' eviction.
        assertTrue(waitForCondition(() -> {
            long storagesCount = IntStream.range(0, partCount)
                    .mapToObj(p -> tableOnNode0.internalTable().storage().getMvPartition(p))
                    .filter(Objects::nonNull)
                    .count();

            return storagesCount == 0;
        }, 10_000));

        TableImpl tableOnNode2 = unwrapTableImpl(unwrapIgniteImpl(node(2)).tables().table(TABLE_NAME));
        long storagesCountOnNode2 = IntStream.range(0, partCount)
                .mapToObj(p -> tableOnNode2.internalTable().storage().getMvPartition(p))
                .filter(Objects::nonNull)
                .count();

        node0.sql().execute(alterZoneSql(DEFAULT_FILTER));

        assertTrue(waitForCondition(() -> {
            Set<String> nodeNames = new HashSet<>();

            for (int j = 0; j < partCount; j++) {
                ZonePartitionId replicationGroupId = new ZonePartitionId(tableImpl.zoneId(), j);

                Set<Assignment> nodes = unwrapIgniteImpl(node0)
                        .placementDriver()
                        .getAssignments(replicationGroupId, igniteImpl.clock().current())
                        .join()
                        .nodes();

                nodeNames.addAll(nodes.stream().map(Assignment::consistentId).collect(toSet()));
            }

            return nodeNames.size() == 3;
        }, 10_000));

        // Wait for storages' eviction on node2.
        assertTrue(waitForCondition(() -> {
            long storagesCount = IntStream.range(0, partCount)
                    .mapToObj(p -> tableOnNode2.internalTable().storage().getMvPartition(p))
                    .filter(Objects::nonNull)
                    .count();

            return storagesCount < storagesCountOnNode2;
        }, 10_000));

        // Check count.
        countThroughJdbc(rowCount);
        try (ResultSet<SqlRow> rs = node0.sql().execute("SELECT COUNT(*) FROM " + TABLE_NAME)) {
            SqlRow row = rs.next();
            log.info("SRV result is " + row.longValue(0));
            assertEquals(rowCount, row.longValue(0));
        }

        try (ResultSet<SqlRow> rs = node0.sql().execute("SELECT * FROM " + TABLE_NAME + " order by key limit 10;")) {
            while (rs.hasNext()) {
                SqlRow row = rs.next();
                log.info("Rows: " + row.intValue(0) + ", " + row.stringValue(1));
            }
        }
    }

    private static String createZoneSql(
            int partitions,
            int replicas,
            int scaleUp,
            int scaleDown,
            String filter,
            String storageProfiles,
            ConsistencyMode consistencyMode
    ) {
        String sqlFormat = "CREATE ZONE \"%s\" WITH "
                + "\"REPLICAS\" = %s, "
                + "\"PARTITIONS\" = %s, "
                + "\"DATA_NODES_FILTER\" = '%s', "
                + "\"DATA_NODES_AUTO_ADJUST_SCALE_UP\" = %s, "
                + "\"DATA_NODES_AUTO_ADJUST_SCALE_DOWN\" = %s, "
                + "\"STORAGE_PROFILES\" = %s, "
                + "\"CONSISTENCY_MODE\" = '%s'";
        return String.format(sqlFormat, ZONE_NAME, replicas, partitions, filter, scaleUp, scaleDown, storageProfiles, consistencyMode);
    }

    private static String alterZoneSql(String filter) {
        return String.format("ALTER ZONE \"%s\" SET (NODES FILTER '%s')", ZONE_NAME, filter);
    }

    private static String createTableSql() {
        return String.format(
                "CREATE TABLE %s(%s INT PRIMARY KEY, %s VARCHAR) ZONE %s STORAGE PROFILE '%s'",
                TABLE_NAME, COLUMN_KEY, COLUMN_VAL, ZONE_NAME, DEFAULT_AIPERSIST_PROFILE_NAME
        );
    }
}
