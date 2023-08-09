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

import org.apache.ignite.internal.distributionzones.DistributionZoneAlreadyExistsException;
import org.apache.ignite.internal.distributionzones.DistributionZoneNotFoundException;
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
    }

    @Test
    public void testCreateZone() {
        tryToCreateZone(ZONE_NAME, true);

        IgniteTestUtils.assertThrowsWithCause(
                () -> tryToCreateZone(ZONE_NAME, true),
                DistributionZoneAlreadyExistsException.class,
                String.format("Distribution zone already exists [zoneName=%s]", ZONE_NAME)
        );

        tryToCreateZone(ZONE_NAME, false);
    }

    @Test
    public void testDropZone() {
        tryToCreateZone(ZONE_NAME, true);

        tryToDropZone(ZONE_NAME, true);

        IgniteTestUtils.assertThrowsWithCause(
                () -> tryToDropZone(ZONE_NAME, true),
                DistributionZoneNotFoundException.class,
                String.format("Distribution zone is not found [zoneName=%s]", ZONE_NAME)
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
                DistributionZoneNotFoundException.class,
                String.format("Distribution zone is not found [zoneName=%s]", ("not_existing_" + ZONE_NAME).toUpperCase())
        );

        tryToRenameZone("not_existing_" + ZONE_NAME, "another_" + ZONE_NAME, false);
    }

    @Test
    public void testRenameToExistingZone() {
        tryToCreateZone(ZONE_NAME, true);
        tryToCreateZone(ZONE_NAME + "_2", true);

        IgniteTestUtils.assertThrowsWithCause(
                () -> tryToRenameZone(ZONE_NAME, ZONE_NAME + "_2", true),
                DistributionZoneAlreadyExistsException.class,
                String.format("Distribution zone already exists [zoneName=%s]", ZONE_NAME + "_2")
        );

        IgniteTestUtils.assertThrowsWithCause(
                () -> tryToRenameZone(ZONE_NAME, ZONE_NAME + "_2", false),
                DistributionZoneAlreadyExistsException.class,
                String.format("Distribution zone already exists [zoneName=%s]", ZONE_NAME + "_2")
        );
    }

    @Test
    public void testAlterZone() {
        tryToCreateZone(ZONE_NAME, true);

        tryToAlterZone(ZONE_NAME, 100, true);

        IgniteTestUtils.assertThrowsWithCause(
                () -> tryToAlterZone("not_existing_" + ZONE_NAME, 200, true),
                DistributionZoneNotFoundException.class,
                String.format("Distribution zone is not found [zoneName=%s]", ("not_existing_" + ZONE_NAME).toUpperCase())
        );

        tryToAlterZone("not_existing_" + ZONE_NAME, 200, false);
    }

    private static void tryToCreateZone(String zoneName, boolean failIfExists) {
        sql(String.format("CREATE ZONE %s", failIfExists ? zoneName : "IF NOT EXISTS " + zoneName));
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
}
