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

package org.apache.ignite.internal.cli.commands.recovery;

import static org.apache.ignite.internal.TestDefaultProfilesNames.DEFAULT_AIPERSIST_PROFILE_NAME;
import static org.apache.ignite.internal.cli.commands.Options.Constants.CLUSTER_URL_OPTION;
import static org.apache.ignite.internal.cli.commands.Options.Constants.RECOVERY_PARTITION_IDS_OPTION;
import static org.apache.ignite.internal.cli.commands.Options.Constants.RECOVERY_TABLE_NAME_OPTION;
import static org.apache.ignite.internal.cli.commands.Options.Constants.RECOVERY_ZONE_NAME_OPTION;

import org.apache.ignite.internal.cli.CliIntegrationTest;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/** Base test class for Cluster Recovery reset partitions commands. */
public abstract class ItResetPartitionsTest extends CliIntegrationTest {
    private static final String ZONE = "first_ZONE";

    private static final String TABLE_NAME = "first_ZONE_table";

    private static final String QUALIFIED_TABLE_NAME = "PUBLIC." + TABLE_NAME;

    private static final int DEFAULT_PARTITION_COUNT = 25;

    @BeforeAll
    public void createTables() {
        sql(String.format("CREATE ZONE \"%s\" WITH storage_profiles='%s'", ZONE, DEFAULT_AIPERSIST_PROFILE_NAME));
        sql(String.format("CREATE TABLE PUBLIC.\"%s\" (id INT PRIMARY KEY, val INT) WITH PRIMARY_ZONE = '%s'", TABLE_NAME, ZONE));
    }

    @Test
    public void testResetAllPartitions() {
        execute(CLUSTER_URL_OPTION, NODE_URL,
                RECOVERY_TABLE_NAME_OPTION, QUALIFIED_TABLE_NAME,
                RECOVERY_ZONE_NAME_OPTION, ZONE);

        assertErrOutputIsEmpty();
        assertOutputContains("Successfully reset partitions.");
    }

    @Test
    public void testResetSpecifiedPartitions() {
        execute(CLUSTER_URL_OPTION, NODE_URL,
                RECOVERY_TABLE_NAME_OPTION, QUALIFIED_TABLE_NAME,
                RECOVERY_ZONE_NAME_OPTION, ZONE,
                RECOVERY_PARTITION_IDS_OPTION, "1,2");

        assertErrOutputIsEmpty();
        assertOutputContains("Successfully reset partitions.");
    }

    @Test
    public void testResetPartitionZoneNotFound() {
        String unknownZone = "unknown_zone";

        execute(CLUSTER_URL_OPTION, NODE_URL,
                RECOVERY_TABLE_NAME_OPTION, QUALIFIED_TABLE_NAME,
                RECOVERY_ZONE_NAME_OPTION, unknownZone,
                RECOVERY_PARTITION_IDS_OPTION, "1,2");

        assertErrOutputContains("Distribution zone was not found [zoneName=" + unknownZone + "]");
        assertOutputIsEmpty();
    }

    @Test
    public void testResetPartitionTableNotFound() {
        String unknownTable = "PUBLIC.unknown_table";

        execute(CLUSTER_URL_OPTION, NODE_URL,
                RECOVERY_TABLE_NAME_OPTION, unknownTable,
                RECOVERY_ZONE_NAME_OPTION, ZONE);

        assertErrOutputContains("The table does not exist [name=" + unknownTable + "]");
        assertOutputIsEmpty();
    }

    @Test
    public void testResetPartitionsIllegalPartitionNegative() {
        execute(CLUSTER_URL_OPTION, NODE_URL,
                RECOVERY_TABLE_NAME_OPTION, QUALIFIED_TABLE_NAME,
                RECOVERY_ZONE_NAME_OPTION, ZONE,
                RECOVERY_PARTITION_IDS_OPTION, "0,5,-10");

        assertErrOutputContains("Partition ID can't be negative, found: -10");
        assertOutputIsEmpty();
    }

    @Test
    public void testResetPartitionsPartitionsOutOfRange() {
        execute(CLUSTER_URL_OPTION, NODE_URL,
                RECOVERY_TABLE_NAME_OPTION, QUALIFIED_TABLE_NAME,
                RECOVERY_ZONE_NAME_OPTION, ZONE,
                RECOVERY_PARTITION_IDS_OPTION, String.valueOf(DEFAULT_PARTITION_COUNT));

        assertErrOutputContains(String.format(
                "Partition IDs should be in range [0, %d] for zone %s, found: %d",
                DEFAULT_PARTITION_COUNT - 1,
                ZONE,
                DEFAULT_PARTITION_COUNT
        ));
        assertOutputIsEmpty();
    }
}
