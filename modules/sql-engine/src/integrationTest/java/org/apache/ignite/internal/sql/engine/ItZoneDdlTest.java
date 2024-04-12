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

package org.apache.ignite.internal.sql.engine;

import static org.apache.ignite.internal.catalog.CatalogService.DEFAULT_STORAGE_PROFILE;
import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.util.Objects;
import org.apache.ignite.internal.ClusterPerClassIntegrationTest;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.catalog.Catalog;
import org.apache.ignite.internal.catalog.CatalogManager;
import org.apache.ignite.internal.catalog.DistributionZoneCantBeDroppedValidationException;
import org.apache.ignite.internal.catalog.DistributionZoneExistsValidationException;
import org.apache.ignite.internal.catalog.DistributionZoneNotFoundValidationException;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogZoneDescriptor;
import org.apache.ignite.internal.testframework.IgniteTestUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * Integration tests for DDL statements that affect distribution zones.
 */
public class ItZoneDdlTest extends ClusterPerClassIntegrationTest {
    private static final String ZONE_NAME = "TEST_ZONE";

    @AfterEach
    void tearDown() {
        tryToDropZone(ZONE_NAME, false);
        dropAllTables();
    }

    @Test
    public void testCreateZone() {
        tryToCreateZone(ZONE_NAME, true);

        IgniteTestUtils.assertThrowsWithCause(
                () -> tryToCreateZone(ZONE_NAME, true),
                DistributionZoneExistsValidationException.class,
                String.format("Distribution zone with name '%s' already exists", ZONE_NAME)
        );

        tryToCreateZone(ZONE_NAME, false);
    }

    @Test
    public void testDropZone() {
        tryToCreateZone(ZONE_NAME, true);

        tryToDropZone(ZONE_NAME, true);

        IgniteTestUtils.assertThrowsWithCause(
                () -> tryToDropZone(ZONE_NAME, true),
                DistributionZoneNotFoundValidationException.class,
                String.format("Distribution zone with name '%s' not found", ZONE_NAME)
        );

        tryToDropZone(ZONE_NAME, false);
    }

    @Test
    public void testRenameZone() {
        tryToCreateZone(ZONE_NAME, true);

        tryToRenameZone(ZONE_NAME, "renamed_" + ZONE_NAME, true);
        tryToRenameZone("renamed_" + ZONE_NAME, ZONE_NAME, true);

        IgniteTestUtils.assertThrowsWithCause(
                () -> tryToRenameZone("not_existing_" + ZONE_NAME, "another_" + ZONE_NAME, true),
                DistributionZoneNotFoundValidationException.class,
                String.format("Distribution zone with name '%s' not found", ("not_existing_" + ZONE_NAME).toUpperCase())
        );

        tryToRenameZone("not_existing_" + ZONE_NAME, "another_" + ZONE_NAME, false);
    }

    @Test
    public void testRenameToExistingZone() {
        tryToCreateZone(ZONE_NAME, true);
        tryToCreateZone(ZONE_NAME + "_2", true);

        IgniteTestUtils.assertThrowsWithCause(
                () -> tryToRenameZone(ZONE_NAME, ZONE_NAME + "_2", true),
                DistributionZoneExistsValidationException.class,
                String.format("Distribution zone with name '%s' already exists", ZONE_NAME + "_2")
        );

        IgniteTestUtils.assertThrowsWithCause(
                () -> tryToRenameZone(ZONE_NAME, ZONE_NAME + "_2", false),
                DistributionZoneExistsValidationException.class,
                String.format("Distribution zone with name '%s' already exists", ZONE_NAME + "_2")
        );
    }

    @Test
    @SuppressWarnings("ThrowableNotThrown")
    public void testRenameDefaultZone() {
        CatalogZoneDescriptor defaultZone = latestActiveCatalog().defaultZone();
        int originalDefaultZoneId = defaultZone.id();

        sql("CREATE TABLE T1(ID INT PRIMARY KEY)");

        String initialDefaultZoneName = defaultZone.name();
        String initialDefaultZoneNameQuoted = '\"' + initialDefaultZoneName + '\"';
        String newDefaultZoneName = ZONE_NAME;
        tryToRenameZone(initialDefaultZoneNameQuoted, newDefaultZoneName, true);

        defaultZone = latestActiveCatalog().defaultZone();
        assertEquals(newDefaultZoneName, defaultZone.name());
        assertEquals(originalDefaultZoneId, defaultZone.id());

        IgniteTestUtils.assertThrowsWithCause(
                () -> tryToDropZone(newDefaultZoneName, false),
                DistributionZoneCantBeDroppedValidationException.class,
                "Default distribution zone can't be dropped"
        );

        IgniteTestUtils.assertThrowsWithCause(
                () -> tryToDropZone(initialDefaultZoneNameQuoted, true),
                DistributionZoneNotFoundValidationException.class,
                format("Distribution zone with name '{}' not found", initialDefaultZoneName)
        );

        // Nothing prevents us from creating another zone with the original name.
        tryToCreateZone(initialDefaultZoneNameQuoted, true);

        sql("CREATE TABLE T2(ID INT PRIMARY KEY)");

        Catalog catalog = latestActiveCatalog();

        assertEquals(2, catalog.tables().stream().filter(tab -> tab.zoneId() == originalDefaultZoneId).count());

        CatalogZoneDescriptor initialDefaultZone = catalog.zone(initialDefaultZoneName);
        assertNotNull(initialDefaultZone);
        assertNotEquals(initialDefaultZone.id(), catalog.defaultZone().id());

        // Revert changes.
        tryToDropZone(initialDefaultZoneNameQuoted, true);
        tryToRenameZone(newDefaultZoneName, initialDefaultZoneNameQuoted, true);
    }

    @Test
    public void testAlterZone() {
        tryToCreateZone(ZONE_NAME, true);

        tryToAlterZone(ZONE_NAME, 100, true);

        IgniteTestUtils.assertThrowsWithCause(
                () -> tryToAlterZone("not_existing_" + ZONE_NAME, 200, true),
                DistributionZoneNotFoundValidationException.class,
                String.format("Distribution zone with name '%s' not found", ("not_existing_" + ZONE_NAME).toUpperCase())
        );

        tryToAlterZone("not_existing_" + ZONE_NAME, 200, false);
    }

    @Test
    @SuppressWarnings("ThrowableNotThrown")
    public void testChangeDefaultZone() {
        String initialDefaultZoneName = latestActiveCatalog().defaultZone().name();
        String newDefaultZoneName = "test_zone";

        // Set default non-existing zone.
        {
            IgniteTestUtils.assertThrowsWithCause(
                    () -> tryToSetDefaultZone(newDefaultZoneName, true),
                    DistributionZoneNotFoundValidationException.class,
                    format("Distribution zone with name '{}' not found", newDefaultZoneName.toUpperCase())
            );

            // No error expected with IF EXISTS condition.
            tryToSetDefaultZone(newDefaultZoneName, false);

            // Nothing has changed.
            assertEquals(initialDefaultZoneName, latestActiveCatalog().defaultZone().name());
        }

        // Set existing zone as default.
        {
            tryToCreateZone(newDefaultZoneName, true);
            tryToSetDefaultZone(newDefaultZoneName, true);

            assertEquals(newDefaultZoneName.toUpperCase(), latestActiveCatalog().defaultZone().name());

            // Set the default zone that is already set by default does not produce any errors.
            tryToSetDefaultZone(newDefaultZoneName, true);

            IgniteTestUtils.assertThrowsWithCause(
                    () -> tryToDropZone(newDefaultZoneName, false),
                    DistributionZoneCantBeDroppedValidationException.class,
                    "Default distribution zone can't be dropped"
            );
        }

        // Create table in a new zone.
        {
            sql("CREATE TABLE t1(id INT PRIMARY KEY)");

            Catalog catalog = latestActiveCatalog();

            CatalogTableDescriptor tab = catalog.tables().stream()
                    .filter(t -> "T1".equals(t.name())).findFirst().orElseThrow();

            assertEquals(catalog.defaultZone().id(), tab.zoneId());
        }

        // Re-create initial default zone.
        {
            String quotedZoneName = "\"" + initialDefaultZoneName + "\"";

            tryToDropZone(quotedZoneName, true);
            tryToCreateZone(quotedZoneName, true);
            tryToSetDefaultZone(quotedZoneName, true);

            sql("CREATE TABLE t2(id INT PRIMARY KEY)");

            Catalog catalog = latestActiveCatalog();
            assertEquals(initialDefaultZoneName, catalog.defaultZone().name());

            CatalogTableDescriptor tab = catalog.tables().stream()
                    .filter(t -> "T2".equals(t.name())).findFirst().orElseThrow();
            assertEquals(catalog.defaultZone().id(), tab.zoneId());
        }

        // Drop non-default zone.
        {
            sql("DROP TABLE T1");

            tryToDropZone(newDefaultZoneName, true);
        }
    }

    private static void tryToCreateZone(String zoneName, boolean failIfExists) {
        sql(String.format(
                "CREATE ZONE %s WITH STORAGE_PROFILES='%s'", failIfExists ? zoneName : "IF NOT EXISTS " + zoneName, DEFAULT_STORAGE_PROFILE
        ));
    }

    private static void tryToDropZone(String zoneName, boolean failIfNotExists) {
        sql(String.format("DROP ZONE %s", failIfNotExists ? zoneName : "IF EXISTS " + zoneName));
    }

    private static void tryToRenameZone(String formZoneName, String toZoneName, boolean failIfNoExists) {
        sql(String.format("ALTER ZONE %s RENAME TO %s", failIfNoExists ? formZoneName : "IF EXISTS " + formZoneName, toZoneName));
    }

    private static void tryToAlterZone(String zoneName, int dataNodesAutoAdjust, boolean failIfNotExists) {
        sql(String.format(
                "ALTER ZONE %s SET DATA_NODES_AUTO_ADJUST=%s",
                failIfNotExists ? zoneName : "IF EXISTS " + zoneName, dataNodesAutoAdjust
        ));
    }

    private static void tryToSetDefaultZone(String zoneName, boolean failIfNotExists) {
        sql(format(
                "ALTER ZONE {} SET DEFAULT ",
                failIfNotExists ? zoneName : "IF EXISTS " + zoneName
        ));
    }

    @SuppressWarnings("resource")
    private static Catalog latestActiveCatalog() {
        IgniteImpl node = CLUSTER.aliveNode();
        CatalogManager catalogManager = node.catalogManager();
        Catalog catalog = catalogManager.catalog(catalogManager.activeCatalogVersion(node.clock().nowLong()));

        return Objects.requireNonNull(catalog);
    }
}
