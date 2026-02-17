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

package org.apache.ignite.internal.sql.engine.systemviews;

import static org.apache.ignite.internal.TestWrappers.unwrapIgniteImpl;
import static org.apache.ignite.internal.catalog.CatalogService.DEFAULT_STORAGE_PROFILE;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.DEFAULT_CONSISTENCY_MODE;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.DEFAULT_FILTER;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.DEFAULT_REPLICA_COUNT;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.DEFAULT_VARLEN_LENGTH;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.DEFAULT_ZONE_QUORUM_SIZE;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.IMMEDIATE_TIMER_VALUE;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.INFINITE_TIMER_VALUE;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.assertThrows;

import java.util.List;
import java.util.Objects;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.catalog.Catalog;
import org.apache.ignite.internal.catalog.CatalogManager;
import org.apache.ignite.internal.catalog.PartitionCountCalculationParameters;
import org.apache.ignite.internal.catalog.PartitionCountCalculator;
import org.apache.ignite.internal.catalog.descriptors.ConsistencyMode;
import org.apache.ignite.internal.sql.engine.util.MetadataMatcher;
import org.apache.ignite.sql.ColumnType;
import org.apache.ignite.sql.SqlException;
import org.junit.jupiter.api.Test;

/**
 * End-to-end tests to verify zones system view.
 */
public class ItZonesSystemViewTest extends AbstractSystemViewTest {
    private static final String ZONE_NAME = "TEST_ZONE";

    private static final String ALTER_ZONE_NAME = "NEW_TEST_ZONE";

    @Test
    public void systemViewDefaultZone() {
        IgniteImpl node = unwrapIgniteImpl(CLUSTER.aliveNode());

        // Check that there is no default zone yet before test table is created.
        assertQuery("SELECT COUNT(*) FROM SYSTEM.ZONES").returns(0L).check();
        // Table for lazy default zone creation.
        createTableOnly("test_table");
        // Check that the default zone was created and is presented on zone view.
        assertQuery("SELECT COUNT(*) FROM SYSTEM.ZONES").returns(1L).check();

        CatalogManager catalogManager = node.catalogManager();
        Catalog catalog = Objects.requireNonNull(
                catalogManager.catalog(catalogManager.activeCatalogVersion(node.clock().nowLong()))
        );

        PartitionCountCalculator partitionCountCalculator = node.partitionCountCalculator();
        PartitionCountCalculationParameters partitionCountCalculationParameters = PartitionCountCalculationParameters.builder()
                .replicaFactor(DEFAULT_REPLICA_COUNT)
                .dataNodesFilter(DEFAULT_FILTER)
                .storageProfiles(List.of(DEFAULT_STORAGE_PROFILE))
                .build();
        int defaultZoneExpectedPartitionCount = partitionCountCalculator.calculate(partitionCountCalculationParameters);

        assertQuery("SELECT ZONE_NAME, ZONE_PARTITIONS, ZONE_REPLICAS, ZONE_QUORUM_SIZE, DATA_NODES_AUTO_ADJUST_SCALE_UP,"
                + " DATA_NODES_AUTO_ADJUST_SCALE_DOWN, DATA_NODES_FILTER, IS_DEFAULT_ZONE, ZONE_CONSISTENCY_MODE FROM SYSTEM.ZONES")
                .returns(
                catalog.defaultZone().name(),
                defaultZoneExpectedPartitionCount,
                DEFAULT_REPLICA_COUNT,
                DEFAULT_ZONE_QUORUM_SIZE,
                IMMEDIATE_TIMER_VALUE,
                INFINITE_TIMER_VALUE,
                DEFAULT_FILTER,
                true,
                DEFAULT_CONSISTENCY_MODE.name()
        ).check();
    }

    @Test
    public void systemViewCustomZone() {
        sql(createZoneSql(ZONE_NAME, 1, 5, 2, 3, 4, DEFAULT_FILTER));

        assertQuery(selectFromZonesSystemView(ZONE_NAME)).returns(
                ZONE_NAME,
                1,
                5,
                2,
                3,
                4,
                DEFAULT_FILTER,
                false,
                DEFAULT_CONSISTENCY_MODE.name()
        ).check();

        sql("DROP ZONE " + ZONE_NAME);
    }

    @Test
    public void systemViewAlterCustomZone() {
        sql(createZoneSql(ZONE_NAME, 1, 5, 2, 3, 4, DEFAULT_FILTER));

        assertQuery(selectFromZonesSystemView(ZONE_NAME)).returns(
                ZONE_NAME,
                1,
                5,
                2,
                3,
                4,
                DEFAULT_FILTER,
                false,
                DEFAULT_CONSISTENCY_MODE.name()
        ).check();

        sql("ALTER ZONE " + ZONE_NAME + " SET (REPLICAS 100)");

        assertQuery(selectFromZonesSystemView(ZONE_NAME)).returns(
                ZONE_NAME,
                1,
                100,
                2,
                3,
                4,
                DEFAULT_FILTER,
                false,
                DEFAULT_CONSISTENCY_MODE.name()
        ).check();

        sql("ALTER ZONE " + ZONE_NAME + " SET (QUORUM SIZE 20)");

        assertQuery(selectFromZonesSystemView(ZONE_NAME)).returns(
                ZONE_NAME,
                1,
                100,
                20,
                3,
                4,
                DEFAULT_FILTER,
                false,
                DEFAULT_CONSISTENCY_MODE.name()
        ).check();

        sql("DROP ZONE " + ZONE_NAME);
    }

    @Test
    public void systemViewRenameCustomZone() {
        sql(createZoneSql(ZONE_NAME, 1, 5, 2, 3, 4, DEFAULT_FILTER));

        assertQuery(selectFromZonesSystemView(ZONE_NAME)).returns(
                ZONE_NAME,
                1,
                5,
                2,
                3,
                4,
                DEFAULT_FILTER,
                false,
                DEFAULT_CONSISTENCY_MODE.name()
        ).check();

        sql("ALTER ZONE " + ZONE_NAME + " RENAME TO " + ALTER_ZONE_NAME);

        assertQuery(selectFromZonesSystemView(ALTER_ZONE_NAME)).returns(
                ALTER_ZONE_NAME,
                1,
                5,
                2,
                3,
                4,
                DEFAULT_FILTER,
                false,
                DEFAULT_CONSISTENCY_MODE.name()
        ).check();

        assertQuery("SELECT COUNT(*) FROM SYSTEM.ZONES").returns(1L).check();

        sql("DROP ZONE " + ALTER_ZONE_NAME);
    }

    @Test
    public void systemViewDropCustomZone() {
        assertQuery("SELECT COUNT(*) FROM SYSTEM.ZONES").returns(0L).check();

        sql(createZoneSql(ZONE_NAME, 1, 5, 2, 3, 4, DEFAULT_FILTER));

        assertQuery("SELECT COUNT(*) FROM SYSTEM.ZONES").returns(1L).check();

        sql("DROP ZONE " + ZONE_NAME);

        assertQuery("SELECT COUNT(*) FROM SYSTEM.ZONES").returns(0L).check();
    }

    @Test
    public void systemViewCustomConsistencyMode() {
        sql(createZoneSql(ZONE_NAME, 1, 2, 3, 4, DEFAULT_FILTER, ConsistencyMode.HIGH_AVAILABILITY));

        assertQuery(selectFromZonesSystemView(ZONE_NAME)).returns(
                ZONE_NAME,
                1,
                2,
                2,
                3,
                4,
                DEFAULT_FILTER,
                false,
                ConsistencyMode.HIGH_AVAILABILITY.name()
        ).check();

        sql("ALTER ZONE " + ZONE_NAME + " SET (REPLICAS 100)");

        assertQuery(selectFromZonesSystemView(ZONE_NAME)).returns(
                ZONE_NAME,
                1,
                100,
                2,
                3,
                4,
                DEFAULT_FILTER,
                false,
                ConsistencyMode.HIGH_AVAILABILITY.name()
        ).check();

        sql("DROP ZONE " + ZONE_NAME);
    }

    @Test
    public void systemViewAlterConsistencyMode() {
        sql(createZoneSql(ZONE_NAME, 1, 5, 2, 3, 4, DEFAULT_FILTER));

        assertQuery(selectFromZonesSystemView(ZONE_NAME)).returns(
                ZONE_NAME,
                1,
                5,
                2,
                3,
                4,
                DEFAULT_FILTER,
                false,
                DEFAULT_CONSISTENCY_MODE.name()
        ).check();

        assertThrows(
                SqlException.class,
                () -> sql("ALTER ZONE " + ZONE_NAME + " SET (CONSISTENCY MODE 'HIGH_AVAILABILITY')"),
                "CONSISTENCY MODE");

        assertQuery(selectFromZonesSystemView(ZONE_NAME)).returns(
                ZONE_NAME,
                1,
                5,
                2,
                3,
                4,
                DEFAULT_FILTER,
                false,
                DEFAULT_CONSISTENCY_MODE.name()
        ).check();

        sql("DROP ZONE " + ZONE_NAME);
    }

    @Test
    public void zonesMetadata() {
        assertQuery("SELECT * FROM SYSTEM.ZONES")
                .columnMetadata(
                        new MetadataMatcher().name("ZONE_NAME").type(ColumnType.STRING).precision(DEFAULT_VARLEN_LENGTH).nullable(true),
                        new MetadataMatcher().name("ZONE_PARTITIONS").type(ColumnType.INT32).nullable(true),
                        new MetadataMatcher().name("ZONE_REPLICAS").type(ColumnType.INT32).nullable(true),
                        new MetadataMatcher().name("DATA_NODES_AUTO_ADJUST_SCALE_UP").type(ColumnType.INT32).nullable(true),
                        new MetadataMatcher().name("DATA_NODES_AUTO_ADJUST_SCALE_DOWN").type(ColumnType.INT32).nullable(true),
                        new MetadataMatcher().name("DATA_NODES_FILTER").type(ColumnType.STRING).precision(DEFAULT_VARLEN_LENGTH),
                        new MetadataMatcher().name("IS_DEFAULT_ZONE").type(ColumnType.BOOLEAN).nullable(true),
                        new MetadataMatcher().name("ZONE_CONSISTENCY_MODE").type(ColumnType.STRING).precision(DEFAULT_VARLEN_LENGTH)
                                .nullable(true),
                        new MetadataMatcher().name("ZONE_ID").type(ColumnType.INT32).nullable(true),

                        // Legacy columns.
                        new MetadataMatcher().name("NAME").type(ColumnType.STRING).precision(DEFAULT_VARLEN_LENGTH).nullable(true),
                        new MetadataMatcher().name("PARTITIONS").type(ColumnType.INT32).nullable(true),
                        new MetadataMatcher().name("REPLICAS").type(ColumnType.INT32).nullable(true),
                        new MetadataMatcher().name("CONSISTENCY_MODE").type(ColumnType.STRING).precision(DEFAULT_VARLEN_LENGTH)
                                .nullable(true),
                        new MetadataMatcher().name("ZONE_QUORUM_SIZE").type(ColumnType.INT32).nullable(true)
                )
                .check();
    }

    @Test
    public void storageProfilesMetadata() {
        assertQuery("SELECT * FROM SYSTEM.ZONE_STORAGE_PROFILES")
                .columnMetadata(
                        new MetadataMatcher().name("ZONE_NAME").type(ColumnType.STRING).precision(DEFAULT_VARLEN_LENGTH).nullable(true),
                        new MetadataMatcher().name("STORAGE_PROFILE").type(ColumnType.STRING).precision(DEFAULT_VARLEN_LENGTH)
                                .nullable(true),
                        new MetadataMatcher().name("IS_DEFAULT_PROFILE").type(ColumnType.BOOLEAN).nullable(true),
                        new MetadataMatcher().name("ZONE_ID").type(ColumnType.INT32).nullable(true)
                )
                .check();
    }

    private static String createZoneSql(
            String zoneName,
            int partitions,
            int replicas,
            int quorumSize,
            int scaleUp,
            int scaleDown,
            String filter
    ) {
        String sqlFormat = "CREATE ZONE %s ("
                + "PARTITIONS %d, "
                + "REPLICAS %d, "
                + "QUORUM SIZE %d, "
                + "AUTO SCALE UP %d, "
                + "AUTO SCALE DOWN %d,"
                + "NODES FILTER '%s') "
                + "STORAGE PROFILES ['%s']";

        return String.format(sqlFormat, zoneName, partitions, replicas, quorumSize, scaleUp, scaleDown, filter, DEFAULT_STORAGE_PROFILE);
    }

    private static String createZoneSql(
            String zoneName,
            int partitions,
            int replicas,
            int scaleUp,
            int scaleDown,
            String filter,
            ConsistencyMode consistencyMode
    ) {
        String sqlFormat = "CREATE ZONE %s ("
                + "PARTITIONS %d, "
                + "REPLICAS %d, "
                + "AUTO SCALE UP %d, "
                + "AUTO SCALE DOWN %d, "
                + "NODES FILTER '%s', "
                + "CONSISTENCY MODE '%s') "
                + "STORAGE PROFILES ['%s']";

        return String.format(
                sqlFormat,
                zoneName,
                partitions,
                replicas,
                scaleUp,
                scaleDown,
                filter,
                consistencyMode,
                DEFAULT_STORAGE_PROFILE
        );
    }

    private static String selectFromZonesSystemView(String zoneName) {
        String sqlFormat = "SELECT ZONE_NAME, ZONE_PARTITIONS, ZONE_REPLICAS, ZONE_QUORUM_SIZE, "
                + "DATA_NODES_AUTO_ADJUST_SCALE_UP, DATA_NODES_AUTO_ADJUST_SCALE_DOWN, DATA_NODES_FILTER, IS_DEFAULT_ZONE, "
                + "CONSISTENCY_MODE FROM SYSTEM.ZONES WHERE ZONE_NAME = '%s'";

        return String.format(sqlFormat, zoneName);
    }
}
