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
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.DEFAULT_PARTITION_COUNT;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.DEFAULT_REPLICA_COUNT;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.IMMEDIATE_TIMER_VALUE;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.INFINITE_TIMER_VALUE;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.assertThrows;

import java.util.Objects;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.catalog.Catalog;
import org.apache.ignite.internal.catalog.CatalogManager;
import org.apache.ignite.internal.catalog.descriptors.ConsistencyMode;
import org.apache.ignite.internal.sql.BaseSqlIntegrationTest;
import org.apache.ignite.internal.testframework.IgniteTestUtils;
import org.apache.ignite.sql.SqlException;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * End-to-end tests to verify zones system view.
 */
public class ItZonesSystemViewTest extends BaseSqlIntegrationTest {
    private static final String ZONE_NAME = "TEST_ZONE";

    private static final String ALTER_ZONE_NAME = "NEW_TEST_ZONE";

    @BeforeAll
    void beforeAll() {
        IgniteTestUtils.await(systemViewManager().completeRegistration());
    }

    @Test
    public void systemViewDefaultZone() {
        IgniteImpl node = unwrapIgniteImpl(CLUSTER.aliveNode());
        CatalogManager catalogManager = node.catalogManager();
        Catalog catalog = Objects.requireNonNull(
                catalogManager.catalog(catalogManager.activeCatalogVersion(node.clock().nowLong()))
        );

        assertQuery("SELECT * FROM SYSTEM.ZONES").returns(
                catalog.defaultZone().name(),
                DEFAULT_PARTITION_COUNT,
                DEFAULT_REPLICA_COUNT,
                IMMEDIATE_TIMER_VALUE,
                INFINITE_TIMER_VALUE,
                DEFAULT_FILTER,
                true,
                DEFAULT_CONSISTENCY_MODE.name()
        ).check();
    }

    @Test
    public void systemViewCustomZone() {
        sql(createZoneSql(ZONE_NAME, 1, 2, 3, 4, DEFAULT_FILTER));

        assertQuery(selectFromZonesSystemView(ZONE_NAME)).returns(
                ZONE_NAME,
                1,
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
        sql(createZoneSql(ZONE_NAME, 1, 2, 3, 4, DEFAULT_FILTER));

        assertQuery(selectFromZonesSystemView(ZONE_NAME)).returns(
                ZONE_NAME,
                1,
                2,
                3,
                4,
                DEFAULT_FILTER,
                false,
                DEFAULT_CONSISTENCY_MODE.name()
        ).check();

        sql("ALTER ZONE " + ZONE_NAME + " SET REPLICAS = 100");

        assertQuery(selectFromZonesSystemView(ZONE_NAME)).returns(
                ZONE_NAME,
                1,
                100,
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
        sql(createZoneSql(ZONE_NAME, 1, 2, 3, 4, DEFAULT_FILTER));

        assertQuery(selectFromZonesSystemView(ZONE_NAME)).returns(
                ZONE_NAME,
                1,
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
                2,
                3,
                4,
                DEFAULT_FILTER,
                false,
                DEFAULT_CONSISTENCY_MODE.name()
        ).check();

        assertQuery("SELECT COUNT(*) FROM SYSTEM.ZONES").returns(2L).check();

        sql("DROP ZONE " + ALTER_ZONE_NAME);
    }

    @Test
    public void systemViewDropCustomZone() {
        assertQuery("SELECT COUNT(*) FROM SYSTEM.ZONES").returns(1L).check();

        sql(createZoneSql(ZONE_NAME, 1, 2, 3, 4, DEFAULT_FILTER));

        assertQuery("SELECT COUNT(*) FROM SYSTEM.ZONES").returns(2L).check();

        sql("DROP ZONE " + ZONE_NAME);

        assertQuery("SELECT COUNT(*) FROM SYSTEM.ZONES").returns(1L).check();
    }

    @Test
    public void systemViewCustomConsistencyMode() {
        sql(createZoneSql(ZONE_NAME, 1, 2, 3, 4, DEFAULT_FILTER, ConsistencyMode.HIGH_AVAILABILITY));

        assertQuery(selectFromZonesSystemView(ZONE_NAME)).returns(
                ZONE_NAME,
                1,
                2,
                3,
                4,
                DEFAULT_FILTER,
                false,
                ConsistencyMode.HIGH_AVAILABILITY.name()
        ).check();

        sql("ALTER ZONE " + ZONE_NAME + " SET REPLICAS = 100");

        assertQuery(selectFromZonesSystemView(ZONE_NAME)).returns(
                ZONE_NAME,
                1,
                100,
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
        sql(createZoneSql(ZONE_NAME, 1, 2, 3, 4, DEFAULT_FILTER));

        assertQuery(selectFromZonesSystemView(ZONE_NAME)).returns(
                ZONE_NAME,
                1,
                2,
                3,
                4,
                DEFAULT_FILTER,
                false,
                DEFAULT_CONSISTENCY_MODE.name()
        ).check();

        assertThrows(
                SqlException.class,
                () -> sql("ALTER ZONE " + ZONE_NAME + " SET CONSISTENCY_MODE = 'HIGH_AVAILABILITY'"),
                "CONSISTENCY_MODE");

        assertQuery(selectFromZonesSystemView(ZONE_NAME)).returns(
                ZONE_NAME,
                1,
                2,
                3,
                4,
                DEFAULT_FILTER,
                false,
                DEFAULT_CONSISTENCY_MODE.name()
        ).check();

        sql("DROP ZONE " + ZONE_NAME);
    }

    private static String createZoneSql(String zoneName, int partitions, int replicas, int scaleUp, int scaleDown, String filter) {
        String sqlFormat = "CREATE ZONE \"%s\" WITH "
                + "\"PARTITIONS\" = %d, "
                + "\"REPLICAS\" = %d, "
                + "\"DATA_NODES_AUTO_ADJUST_SCALE_UP\" = %d, "
                + "\"DATA_NODES_AUTO_ADJUST_SCALE_DOWN\" = %d,"
                + "\"DATA_NODES_FILTER\" = '%s',"
                + "\"STORAGE_PROFILES\" = '%s'";

        return String.format(sqlFormat, zoneName, partitions, replicas, scaleUp, scaleDown, filter, DEFAULT_STORAGE_PROFILE);
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
        String sqlFormat = "CREATE ZONE \"%s\" WITH "
                + "\"PARTITIONS\" = %d, "
                + "\"REPLICAS\" = %d, "
                + "\"DATA_NODES_AUTO_ADJUST_SCALE_UP\" = %d, "
                + "\"DATA_NODES_AUTO_ADJUST_SCALE_DOWN\" = %d,"
                + "\"DATA_NODES_FILTER\" = '%s',"
                + "\"STORAGE_PROFILES\" = '%s',"
                + "\"CONSISTENCY_MODE\" = '%s'";

        return String.format(
                sqlFormat,
                zoneName,
                partitions,
                replicas,
                scaleUp,
                scaleDown,
                filter,
                DEFAULT_STORAGE_PROFILE,
                consistencyMode
        );
    }

    private static String selectFromZonesSystemView(String zoneName) {
        String sqlFormat = "SELECT * FROM SYSTEM.ZONES WHERE NAME = '%s'";

        return String.format(sqlFormat, zoneName);
    }
}
