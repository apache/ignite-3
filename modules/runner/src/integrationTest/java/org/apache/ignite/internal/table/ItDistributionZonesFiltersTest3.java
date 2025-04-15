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
import org.apache.ignite.InitParametersBuilder;
import org.apache.ignite.internal.ClusterConfiguration;
import org.apache.ignite.internal.ClusterPerTestIntegrationTest;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.catalog.descriptors.ConsistencyMode;
import org.apache.ignite.internal.partitiondistribution.Assignment;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.sql.ResultSet;
import org.apache.ignite.sql.SqlRow;
import org.apache.ignite.table.Table;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Integration test for data nodes' filters functionality.
 */
public class ItDistributionZonesFiltersTest3 extends ClusterPerTestIntegrationTest {
    /** URL. */
    protected static final String URL = "jdbc:ignite:thin://127.0.0.1:10800";
    /** Connection. */
    protected static Connection conn;
    /** Default schema. */
    protected static final String DEFAULT_SCHEMA = "PUBLIC";
    /** Statement. */
    protected Statement stmt;
    private static final String ZONE_NAME = "TEST_ZONE";
    private static final String TABLE_NAME = "table1";
    private static final String STORAGE_PROFILES = String.format("'%s'", DEFAULT_AIPERSIST_PROFILE_NAME);

    @Language("HOCON")
    private static final String NODE_ATTRIBUTES = "{region = US, storage = SSD}";

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

    @Language("HOCON")
    private static final String STORAGE_PROFILES_CONFIGS = String.format(
            "{%s.engine = aipersist}",
            DEFAULT_AIPERSIST_PROFILE_NAME
    );

    private static final String COLUMN_KEY = "key";
    private static final String COLUMN_VAL = "val";
    private int node = 0;

    @Language("HOCON")
    private static String createStartConfig(@Language("HOCON") String nodeAttributes, @Language("HOCON") String storageProfiles) {
        return "ignite {\n"
                + "  network: {\n"
                + "    port: {},\n"
                + "    nodeFinder.netClusterNodes: [ {} ]\n"
                + "  },\n"
                + "  storage.profiles: " + storageProfiles + ",\n"
                + "  clientConnector.port: {},\n"
                + "  rest: {\n"
                + "    port: {},\n"
                + "    ssl.port: {} \n"
                + "  },\n"
                + "  failureHandler.dumpThreadsOnFailure: false,\n"
                + "  nodeAttributes.nodeAttributes: {}\n"
                + "}";
    }

    protected String updateStartupConfiguration() {
        String filter = "{}";
        if (node == 0) {
            filter = "{region = EU}";
        } else if (node == 1) {
            filter = "{region = US}";
        } else if (node == 2) {
            filter = "{region = US}";
        }
        String cfg = createStartConfig(filter, STORAGE_PROFILES_CONFIGS);
        node++;
        return cfg;
    }

    /**
     * Inheritors should override this method to change configuration of the test cluster before its creation.
     */
    @Override
    protected void customizeConfiguration(ClusterConfiguration.Builder clusterConfigurationBuilder) {
        clusterConfigurationBuilder.nodeAttributesProvider(nodeIndex ->
                nodeIndex == 0
                        ? "{region = EU, storage = SSD}"
                        : NODE_ATTRIBUTES
        );
    }

    @Override
    protected void customizeInitParameters(InitParametersBuilder builder) {
        super.customizeInitParameters(builder);
    }

    @Override
    protected String getNodeBootstrapConfigTemplate() {
        return createStartConfig(NODE_ATTRIBUTES, STORAGE_PROFILES_CONFIGS);
    }

    public void countThroughJdbc(int rowCount) throws SQLException {
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
    public void testChangingFilter2() throws Exception {
        String filter = DEFAULT_FILTER;
        int partCount = 24;
        int rowCount = 400;

        Ignite node0 = unwrapIgniteImpl(node(0));

        node0.sql().execute(
                null,
                createZoneSql(partCount, 1, 10_000, 10_000, filter, STORAGE_PROFILES, ConsistencyMode.STRONG_CONSISTENCY)
        );

        node0.sql().execute(null, createTableSql());

        Table table = node0.tables().table(TABLE_NAME);

        StringBuilder sqlInsert = new StringBuilder("INSERT INTO " + TABLE_NAME + " (key, val) VALUES \n");

        for (int i = 0; i < rowCount / 2; i++) {
            /*table.recordView().upsert(null, Tuple.create()
                    .set(COLUMN_KEY, Integer.valueOf(i))
                    .set(COLUMN_VAL, "val"));*/
            sqlInsert.append("(").append(i + 1).append(", 'value" + (i + 1) + "')");
            if (i != rowCount / 2 - 1) {
                sqlInsert.append(", \n");
            }
        }

        node0.sql().execute(null, sqlInsert.toString());

        //---
        sqlInsert = new StringBuilder("INSERT INTO " + TABLE_NAME + " (key, val) VALUES \n");

        for (int i = rowCount / 2; i < rowCount; i++) {
            /*table.recordView().upsert(null, Tuple.create()
                    .set(COLUMN_KEY, Integer.valueOf(i))
                    .set(COLUMN_VAL, "val"));*/
            sqlInsert.append("(").append(i + 1).append(", 'value" + (i + 1) + "')");
            if (i != rowCount - 1) {
                sqlInsert.append(", \n");
            }
        }
        node0.sql().execute(null, sqlInsert.toString());
        //---

        String changedFilter = "$[?(@.region == \"US\")]";
        node0.sql().execute(null, alterZoneSql(changedFilter));

        IgniteImpl igniteImpl = unwrapIgniteImpl(node0);
        TableImpl tableImpl = unwrapTableImpl(node0.tables().table(TABLE_NAME));

        try {
            assertTrue(waitForCondition(() -> {
                Set<String> nodeNames = new HashSet<>();

                for (int j =0; j < partCount; j++) {
                    Set<Assignment> nodes = unwrapIgniteImpl(node0)
                            .placementDriver()
                            .getAssignments(new TablePartitionId(tableImpl.tableId(), j), igniteImpl.clock().current())
                            .join()
                            .nodes();

                    nodeNames.addAll(nodes.stream().map(Assignment::consistentId).collect(toSet()));
                }

                return nodeNames.size() == 2;
            }, 10_000));
        } catch (AssertionError e) {
            Set<String> nodeNames = new HashSet<>();

            for (int j =0; j < partCount; j++) {
                Set<Assignment> nodes = unwrapIgniteImpl(node0)
                        .placementDriver()
                        .getAssignments(new TablePartitionId(tableImpl.tableId(), j), igniteImpl.clock().current())
                        .join()
                        .nodes();

                nodeNames.addAll(nodes.stream().map(Assignment::consistentId).collect(toSet()));
            }

            throw new AssertionError("qqq wrong assignments, nodes=" + nodeNames, e);
        }

        // Check count.
        countThroughJdbc(rowCount);
        try (ResultSet<SqlRow> rs = node0.sql().execute(null, "SELECT COUNT(*) FROM " + TABLE_NAME)) {
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

        node0.sql().execute(null, alterZoneSql(DEFAULT_FILTER));

        assertTrue(waitForCondition(() -> {
            Set<String> nodeNames = new HashSet<>();

            for (int j =0; j < partCount; j++) {
                Set<Assignment> nodes = unwrapIgniteImpl(node0)
                        .placementDriver()
                        .getAssignments(new TablePartitionId(tableImpl.tableId(), j), igniteImpl.clock().current())
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
        try (ResultSet<SqlRow> rs = node0.sql().execute(null, "SELECT COUNT(*) FROM " + TABLE_NAME)) {
            SqlRow row = rs.next();
            log.info("SRV result is " + row.longValue(0));
            assertEquals(rowCount, row.longValue(0));
        }

        try (ResultSet<SqlRow> rs = node0.sql().execute(null, "SELECT * FROM " + TABLE_NAME + " order by key limit 10;")) {
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
        return String.format("ALTER ZONE \"%s\" SET \"DATA_NODES_FILTER\" = '%s'", ZONE_NAME, filter);
    }
    private static String createTableSql() {
        return String.format(
                "CREATE TABLE %s(%s INT PRIMARY KEY, %s VARCHAR) ZONE %s STORAGE PROFILE '%s'",
                TABLE_NAME, COLUMN_KEY, COLUMN_VAL, ZONE_NAME, DEFAULT_AIPERSIST_PROFILE_NAME
        );
    }
}