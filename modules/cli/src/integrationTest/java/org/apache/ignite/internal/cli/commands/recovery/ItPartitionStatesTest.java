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

import static java.util.stream.Collectors.toSet;
import static org.apache.ignite.internal.TestDefaultProfilesNames.DEFAULT_AIPERSIST_PROFILE_NAME;
import static org.apache.ignite.internal.cli.commands.Options.Constants.CLUSTER_URL_OPTION;
import static org.apache.ignite.internal.cli.commands.Options.Constants.RECOVERY_LOCAL_OPTION;
import static org.apache.ignite.internal.cli.commands.Options.Constants.RECOVERY_NODE_NAMES_OPTION;
import static org.apache.ignite.internal.cli.commands.Options.Constants.RECOVERY_PARTITION_GLOBAL_OPTION;
import static org.apache.ignite.internal.cli.commands.Options.Constants.RECOVERY_PARTITION_IDS_OPTION;
import static org.apache.ignite.internal.cli.commands.Options.Constants.RECOVERY_ZONE_NAMES_OPTION;

import java.util.HashSet;
import java.util.Set;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.cli.CliIntegrationTest;
import org.apache.ignite.internal.util.CollectionUtils;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class ItPartitionStatesTest extends CliIntegrationTest {
    private static final int DEFAULT_PARTITION_COUNT = 25;

    private static final Set<String> ZONES = Set.of("first_zone", "second_zone", "third_zone");

    private static final Set<String> MIXED_ZONES = Set.of("mixed_case_zone", "MIXED_CASE_ZONE");

    private static final Set<String> ALL_ZONES = new HashSet<>(CollectionUtils.concat(ZONES, MIXED_ZONES));

    private static final Set<String> STATES = Set.of("HEALTHY", "AVAILABLE");

    private static Set<String> nodeNames;

    @BeforeAll
    public static void createTables() {
        ALL_ZONES.forEach(name -> {
            sql(String.format("CREATE ZONE \"%s\" WITH storage_profiles='%s'", name, DEFAULT_AIPERSIST_PROFILE_NAME));
            sql("CREATE TABLE \"" + name + "_table\" (id INT PRIMARY KEY, val INT) WITH PRIMARY_ZONE = '" + name + "'");
        });

        sql("CREATE ZONE \"empty_zone\" WITH storage_profiles='" + DEFAULT_AIPERSIST_PROFILE_NAME + "'");

        nodeNames = CLUSTER.runningNodes().map(IgniteImpl::name).collect(toSet());
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testPartitionStates(boolean global) {
        execute("recovery", "partition-states",
                CLUSTER_URL_OPTION, NODE_URL,
                global ? RECOVERY_PARTITION_GLOBAL_OPTION : RECOVERY_LOCAL_OPTION
        );

        for (int i = 0; i < DEFAULT_PARTITION_COUNT; i++) {
            assertOutputContains(String.valueOf(i));
        }

        checkOutput(global, ALL_ZONES, nodeNames);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testPartitionStatesByZones(boolean global) {
        execute("recovery", "partition-states",
                CLUSTER_URL_OPTION, NODE_URL,
                RECOVERY_ZONE_NAMES_OPTION, String.join(",", ZONES),
                global ? RECOVERY_PARTITION_GLOBAL_OPTION : RECOVERY_LOCAL_OPTION
        );

        assertOutputContains("Table name");
        assertOutputContains("Partition ID");
        assertOutputContains("State");

        for (int i = 0; i < DEFAULT_PARTITION_COUNT; i++) {
            assertOutputContains(String.valueOf(i));
        }

        checkOutput(global, ZONES, nodeNames);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testPartitionStatesZonesMixedCase(boolean global) {
        execute("recovery", "partition-states",
                CLUSTER_URL_OPTION, NODE_URL,
                RECOVERY_ZONE_NAMES_OPTION, String.join(",", MIXED_ZONES),
                global ? RECOVERY_PARTITION_GLOBAL_OPTION : RECOVERY_LOCAL_OPTION
        );

        checkOutput(global, MIXED_ZONES, nodeNames);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testPartitionStatesMissingZone(boolean global) {
        String unknownZone = "UNKNOWN_ZONE";

        execute("recovery", "partition-states",
                CLUSTER_URL_OPTION, NODE_URL,
                RECOVERY_ZONE_NAMES_OPTION, unknownZone,
                global ? RECOVERY_PARTITION_GLOBAL_OPTION : RECOVERY_LOCAL_OPTION
        );

        assertErrOutputContains("Some distribution zones are missing: [UNKNOWN_ZONE]");

        assertOutputIsEmpty();
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testPartitionStatesMissingPartition(boolean global) {
        String unknownPartition = "-1";

        execute("recovery", "partition-states",
                CLUSTER_URL_OPTION, NODE_URL,
                RECOVERY_PARTITION_IDS_OPTION, unknownPartition,
                global ? RECOVERY_PARTITION_GLOBAL_OPTION : RECOVERY_LOCAL_OPTION);

        assertErrOutputContains("Some partitions are missing: [-1]");

        assertOutputIsEmpty();
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testPartitionStatesMissingNode() {
        String unknownNode = "unknown_node";

        execute("recovery", "partition-states",
                CLUSTER_URL_OPTION, NODE_URL,
                RECOVERY_NODE_NAMES_OPTION, unknownNode,
                RECOVERY_LOCAL_OPTION);

        assertErrOutputContains("Some nodes are missing: [unknown_node]");

        assertOutputIsEmpty();
    }


    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testPartitionStatesEmptyResult(boolean global) {
        execute("recovery", "partition-states",
                CLUSTER_URL_OPTION, NODE_URL,
                RECOVERY_ZONE_NAMES_OPTION, "empty_zone",
                global ? RECOVERY_PARTITION_GLOBAL_OPTION : RECOVERY_LOCAL_OPTION
        );

        checkOutput(global, Set.of(), Set.of());
    }

    private void checkOutput(boolean global, Set<String> zoneNames, Set<String> nodes) {
        assertErrOutputIsEmpty();

        if (global) {
            assertOutputDoesNotContain("Node name");
        } else {
            assertOutputContains("Node name");

            if (!nodes.isEmpty()) {
                assertOutputContainsAnyIgnoringCase(nodes);
            }

            Set<String> anotherNodes = CollectionUtils.difference(nodeNames, nodes);

            if (!anotherNodes.isEmpty()) {
                assertOutputDoesNotContainIgnoreCase(anotherNodes);
            }
        }

        assertOutputContains("Zone name");
        assertOutputContains("Table name");
        assertOutputContains("Partition ID");
        assertOutputContains("State");

        if (!zoneNames.isEmpty()) {
            assertOutputContainsAllIgnoringCase(zoneNames);

            Set<String> tableNames = zoneNames.stream().map(it -> it + "_table").collect(toSet());

            assertOutputContainsAllIgnoringCase(tableNames);
        }

        Set<String> anotherZones = CollectionUtils.difference(ZONES, zoneNames);

        if (!anotherZones.isEmpty()) {
            assertOutputDoesNotContainIgnoreCase(anotherZones);
        }

        if (!zoneNames.isEmpty() && nodeNames.isEmpty()) {
            assertOutputContainsAnyIgnoringCase(STATES);
        }
    }
}
