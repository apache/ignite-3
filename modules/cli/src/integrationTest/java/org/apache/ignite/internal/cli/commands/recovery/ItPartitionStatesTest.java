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
import static org.apache.ignite.internal.cli.commands.Options.Constants.PLAIN_OPTION;
import static org.apache.ignite.internal.cli.commands.Options.Constants.RECOVERY_LOCAL_OPTION;
import static org.apache.ignite.internal.cli.commands.Options.Constants.RECOVERY_NODE_NAMES_OPTION;
import static org.apache.ignite.internal.cli.commands.Options.Constants.RECOVERY_PARTITION_GLOBAL_OPTION;
import static org.apache.ignite.internal.cli.commands.Options.Constants.RECOVERY_PARTITION_IDS_OPTION;
import static org.apache.ignite.internal.cli.commands.Options.Constants.RECOVERY_ZONE_NAMES_OPTION;

import java.util.HashSet;
import java.util.Set;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.cli.CliIntegrationTest;
import org.apache.ignite.internal.cli.commands.recovery.partitions.PartitionStatesCommand;
import org.apache.ignite.internal.util.CollectionUtils;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/** Tests for {@link PartitionStatesCommand}. */
public class ItPartitionStatesTest extends CliIntegrationTest {
    private static final int DEFAULT_PARTITION_COUNT = 25;

    private static final Set<String> ZONES = Set.of("first_ZONE", "second_ZONE", "third_ZONE");

    private static final Set<String> MIXED_CASE_ZONES = Set.of("mixed_first_zone", "MIXED_FIRST_ZONE", "mixed_second_zone",
            "MIXED_SECOND_ZONE");

    private static final Set<String> ZONES_CONTAINING_TABLES = new HashSet<>(CollectionUtils.concat(ZONES, MIXED_CASE_ZONES));

    private static final String EMPTY_ZONE = "empty_ZONE";

    private static final Set<String> STATES = Set.of("HEALTHY", "AVAILABLE");

    private static final int DONT_CHECK_PARTITIONS = -1;

    private static Set<String> nodeNames;

    @BeforeAll
    public static void createTables() {
        ZONES_CONTAINING_TABLES.forEach(name -> {
            sql(String.format("CREATE ZONE \"%s\" WITH storage_profiles='%s'", name, DEFAULT_AIPERSIST_PROFILE_NAME));
            sql(String.format("CREATE TABLE \"%s_table\" (id INT PRIMARY KEY, val INT) WITH PRIMARY_ZONE = '%1$s'", name));
        });

        sql(String.format("CREATE ZONE \"%s\" WITH storage_profiles='%s'", EMPTY_ZONE, DEFAULT_AIPERSIST_PROFILE_NAME));

        nodeNames = CLUSTER.runningNodes().map(IgniteImpl::name).collect(toSet());
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testPartitionStates(boolean global) {
        execute("recovery", "partition-states",
                CLUSTER_URL_OPTION, NODE_URL,
                global ? RECOVERY_PARTITION_GLOBAL_OPTION : RECOVERY_LOCAL_OPTION,
                PLAIN_OPTION
        );

        checkOutput(global, ZONES_CONTAINING_TABLES, nodeNames, DEFAULT_PARTITION_COUNT);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testPartitionStatesByZones(boolean global) {
        execute("recovery", "partition-states",
                CLUSTER_URL_OPTION, NODE_URL,
                RECOVERY_ZONE_NAMES_OPTION, String.join(",", ZONES),
                global ? RECOVERY_PARTITION_GLOBAL_OPTION : RECOVERY_LOCAL_OPTION,
                PLAIN_OPTION
        );

        checkOutput(global, ZONES, nodeNames, DEFAULT_PARTITION_COUNT);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testPartitionStatesByPartitions(boolean global) {
        String partitions = "0,1";

        execute("recovery", "partition-states",
                CLUSTER_URL_OPTION, NODE_URL,
                RECOVERY_PARTITION_IDS_OPTION, partitions,
                global ? RECOVERY_PARTITION_GLOBAL_OPTION : RECOVERY_LOCAL_OPTION,
                PLAIN_OPTION
        );

        checkOutput(global, ZONES, nodeNames, 2);
    }

    @Test
    void testLocalPartitionStatesByNodes() {
        Set<String> nodes = nodeNames.stream().limit(nodeNames.size() - 1).collect(toSet());

        execute("recovery", "partition-states",
                CLUSTER_URL_OPTION, NODE_URL,
                RECOVERY_NODE_NAMES_OPTION, String.join(",", nodes),
                RECOVERY_LOCAL_OPTION,
                PLAIN_OPTION
        );

        checkOutput(false, ZONES, nodes, DONT_CHECK_PARTITIONS);
    }

    @Test
    void testLocalPartitionStatesByNodesIsCaseSensitive() {
        Set<String> nodeNames = Set.of(CLUSTER.node(0).node().name(), CLUSTER.node(1).node().name());

        String url = "state/local?nodeNames=" + String.join(",", nodeNames).toUpperCase();

        execute("recovery", "partition-states",
                CLUSTER_URL_OPTION, NODE_URL,
                RECOVERY_NODE_NAMES_OPTION, String.join(",", nodeNames).toUpperCase(),
                RECOVERY_LOCAL_OPTION,
                PLAIN_OPTION
        );

        assertErrOutputContains("Some nodes are missing: ");

        nodeNames.forEach(name -> assertErrOutputContains(name.toUpperCase()));

        assertOutputIsEmpty();
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testPartitionStatesZonesMixedCase(boolean global) {
        execute("recovery", "partition-states",
                CLUSTER_URL_OPTION, NODE_URL,
                RECOVERY_ZONE_NAMES_OPTION, String.join(",", MIXED_CASE_ZONES),
                global ? RECOVERY_PARTITION_GLOBAL_OPTION : RECOVERY_LOCAL_OPTION,
                PLAIN_OPTION
        );

        checkOutput(global, MIXED_CASE_ZONES, nodeNames, DEFAULT_PARTITION_COUNT);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testPartitionStatesMissingZone(boolean global) {
        String unknownZone = "UNKNOWN_ZONE";

        execute("recovery", "partition-states",
                CLUSTER_URL_OPTION, NODE_URL,
                RECOVERY_ZONE_NAMES_OPTION, unknownZone,
                global ? RECOVERY_PARTITION_GLOBAL_OPTION : RECOVERY_LOCAL_OPTION,
                PLAIN_OPTION
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
                global ? RECOVERY_PARTITION_GLOBAL_OPTION : RECOVERY_LOCAL_OPTION,
                PLAIN_OPTION
        );

        assertErrOutputContains("Some partitions are missing: [-1]");

        assertOutputIsEmpty();
    }

    @Test
    void testPartitionStatesMissingNode() {
        String unknownNode = "unknown_node";

        execute("recovery", "partition-states",
                CLUSTER_URL_OPTION, NODE_URL,
                RECOVERY_NODE_NAMES_OPTION, unknownNode,
                RECOVERY_LOCAL_OPTION, PLAIN_OPTION
        );

        assertErrOutputContains("Some nodes are missing: [unknown_node]");

        assertOutputIsEmpty();
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testPartitionStatesEmptyResult(boolean global) {
        execute("recovery", "partition-states",
                CLUSTER_URL_OPTION, NODE_URL,
                RECOVERY_ZONE_NAMES_OPTION, EMPTY_ZONE,
                global ? RECOVERY_PARTITION_GLOBAL_OPTION : RECOVERY_LOCAL_OPTION,
                PLAIN_OPTION
        );

        checkOutput(global, Set.of(), Set.of(), 0);
    }

    private void checkOutput(boolean global, Set<String> zoneNames, Set<String> nodes, int partitions) {
        assertErrOutputIsEmpty();
        assertOutputStartsWith((global ? "" : "Node name\t") + "Zone name\tTable name\tPartition ID\tState");

        if (!global) {
            if (!nodes.isEmpty()) {
                assertOutputContainsAny(nodes);
            }

            Set<String> anotherNodes = CollectionUtils.difference(nodeNames, nodes);

            if (!anotherNodes.isEmpty()) {
                assertOutputDoesNotContainIgnoreCase(anotherNodes);
            }
        }

        if (!zoneNames.isEmpty()) {
            assertOutputContainsAll(zoneNames);

            Set<String> tableNames = zoneNames.stream().map(it -> it + "_table").collect(toSet());

            assertOutputContainsAllIgnoringCase(tableNames);
        }

        Set<String> anotherZones = CollectionUtils.difference(ZONES, zoneNames);

        if (!anotherZones.isEmpty()) {
            assertOutputDoesNotContain(anotherZones);
        }

        if (!zoneNames.isEmpty() && nodeNames.isEmpty()) {
            assertOutputContainsAny(STATES);
        }

        if (partitions != DONT_CHECK_PARTITIONS && partitions != DEFAULT_PARTITION_COUNT) {
            for (int i = 0; i < partitions; i++) {
                assertOutputContains("\t" + i + "\t");
            }

            for (int i = partitions; i < DEFAULT_PARTITION_COUNT; i++) {
                assertOutputDoesNotContain("\t" + i + "\t");
            }
        }
    }
}
