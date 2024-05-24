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

package org.apache.ignite.internal.cli.commands.recovery.restart;

import static java.util.stream.Collectors.joining;
import static org.apache.ignite.internal.TestDefaultProfilesNames.DEFAULT_AIPERSIST_PROFILE_NAME;
import static org.apache.ignite.internal.cli.commands.Options.Constants.CLUSTER_URL_OPTION;
import static org.apache.ignite.internal.cli.commands.Options.Constants.RECOVERY_NODE_NAMES_OPTION;
import static org.apache.ignite.internal.cli.commands.Options.Constants.RECOVERY_PARTITION_IDS_OPTION;
import static org.apache.ignite.internal.cli.commands.Options.Constants.RECOVERY_PURGE_OPTION;
import static org.apache.ignite.internal.cli.commands.Options.Constants.RECOVERY_TABLE_NAME_OPTION;
import static org.apache.ignite.internal.cli.commands.Options.Constants.RECOVERY_ZONE_NAME_OPTION;

import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.cli.CliIntegrationTest;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/** Base test class for Cluster Recovery restart partitions commands. */
public abstract class ItRestartPartitionsTest extends CliIntegrationTest {
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
    public void testRestartAllPartitions() {
        execute(CLUSTER_URL_OPTION, NODE_URL,
                RECOVERY_TABLE_NAME_OPTION, QUALIFIED_TABLE_NAME,
                RECOVERY_ZONE_NAME_OPTION, ZONE
        );

        assertErrOutputIsEmpty();
        assertOutputContains("Successfully restarted partitions without cleanup.");
    }

    @Test
    public void testRestartAllPartitionsPurge() {
        execute(CLUSTER_URL_OPTION, NODE_URL,
                RECOVERY_TABLE_NAME_OPTION, QUALIFIED_TABLE_NAME,
                RECOVERY_ZONE_NAME_OPTION, ZONE,
                RECOVERY_PURGE_OPTION
        );

        assertErrOutputIsEmpty();
        assertOutputContains("Successfully restarted partitions with cleanup.");
    }

    @Test
    public void testRestartSpecifiedPartitions() {
        execute(CLUSTER_URL_OPTION, NODE_URL,
                RECOVERY_TABLE_NAME_OPTION, QUALIFIED_TABLE_NAME,
                RECOVERY_ZONE_NAME_OPTION, ZONE,
                RECOVERY_PARTITION_IDS_OPTION, "1,2"
        );

        assertErrOutputIsEmpty();
        assertOutputContains("Successfully restarted partitions without cleanup.");
    }

    @Test
    public void testRestartPartitionsByNodes() {
        String nodeNames = CLUSTER.runningNodes()
                .limit(initialNodes() - 1)
                .map(IgniteImpl::name)
                .collect(joining(","));

        execute(CLUSTER_URL_OPTION, NODE_URL,
                RECOVERY_TABLE_NAME_OPTION, QUALIFIED_TABLE_NAME,
                RECOVERY_ZONE_NAME_OPTION, ZONE,
                RECOVERY_PARTITION_IDS_OPTION, "1,2",
                RECOVERY_NODE_NAMES_OPTION, nodeNames
        );

        assertErrOutputIsEmpty();
        assertOutputContains("Successfully restarted partitions without cleanup.");
    }

    @Test
    public void testRestartPartitionZoneNotFound() {
        String unknownZone = "unknown_zone";

        execute(CLUSTER_URL_OPTION, NODE_URL,
                RECOVERY_TABLE_NAME_OPTION, QUALIFIED_TABLE_NAME,
                RECOVERY_ZONE_NAME_OPTION, unknownZone,
                RECOVERY_PARTITION_IDS_OPTION, "1,2"
        );

        assertOutputIsEmpty();
        assertErrOutputContains("Distribution zone was not found [zoneName=" + unknownZone + "]");
    }

    @Test
    public void testRestartPartitionTableNotFound() {
        String unknownTable = "PUBLIC.unknown_table";

        execute(CLUSTER_URL_OPTION, NODE_URL,
                RECOVERY_TABLE_NAME_OPTION, unknownTable,
                RECOVERY_ZONE_NAME_OPTION, ZONE
        );

        assertOutputIsEmpty();
        assertErrOutputContains("The table does not exist [name=" + unknownTable + "]");
    }

    @Test
    public void testRestartPartitionNodeNotFound() {
        String unknownNode = "unknownNode";

        execute(CLUSTER_URL_OPTION, NODE_URL,
                RECOVERY_TABLE_NAME_OPTION, QUALIFIED_TABLE_NAME,
                RECOVERY_NODE_NAMES_OPTION, unknownNode,
                RECOVERY_ZONE_NAME_OPTION, ZONE
        );

        assertOutputIsEmpty();
        assertErrOutputContains("Some nodes are missing: [" + unknownNode + "]");
    }

    @Test
    public void testRestartPartitionsIllegalPartitionNegative() {
        execute(CLUSTER_URL_OPTION, NODE_URL,
                RECOVERY_TABLE_NAME_OPTION, QUALIFIED_TABLE_NAME,
                RECOVERY_ZONE_NAME_OPTION, ZONE,
                RECOVERY_PARTITION_IDS_OPTION, "0,5,-10"
        );

        assertOutputIsEmpty();
        assertErrOutputContains("Partition ID can't be negative, found: -10");
    }

    @Test
    public void testRestartPartitionsPartitionsOutOfRange() {
        execute(CLUSTER_URL_OPTION, NODE_URL,
                RECOVERY_TABLE_NAME_OPTION, QUALIFIED_TABLE_NAME,
                RECOVERY_ZONE_NAME_OPTION, ZONE,
                RECOVERY_PARTITION_IDS_OPTION, String.valueOf(DEFAULT_PARTITION_COUNT)
        );

        assertOutputIsEmpty();
        assertErrOutputContains(String.format(
                "Partition IDs should be in range [0, %d] for zone %s, found: %d",
                DEFAULT_PARTITION_COUNT - 1,
                ZONE,
                DEFAULT_PARTITION_COUNT
        ));
    }
}
