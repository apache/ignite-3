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

package org.apache.ignite.internal.catalog.commands;

import static org.apache.ignite.internal.catalog.commands.CatalogUtils.DEFAULT_FILTER;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.DEFAULT_REPLICA_COUNT;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.DEFAULT_ZONE_QUORUM_SIZE;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.IMMEDIATE_TIMER_VALUE;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.INFINITE_TIMER_VALUE;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.assertThrows;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import java.util.List;
import org.apache.ignite.internal.catalog.Catalog;
import org.apache.ignite.internal.catalog.CatalogCommand;
import org.apache.ignite.internal.catalog.CatalogTestUtils;
import org.apache.ignite.internal.catalog.CatalogValidationException;
import org.apache.ignite.internal.catalog.UpdateContext;
import org.apache.ignite.internal.catalog.descriptors.CatalogZoneDescriptor;
import org.apache.ignite.internal.catalog.storage.AlterZoneEntry;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Tests to verify validation of {@link AlterZoneCommand}.
 */
@SuppressWarnings("ThrowableNotThrown")
public class AlterZoneCommandValidationTest extends AbstractCommandValidationTest {
    @ParameterizedTest(name = "[{index}] ''{argumentsWithNames}''")
    @MethodSource("nullAndBlankStrings")
    void zoneNameMustNotBeNullOrBlank(String zone) {
        assertThrows(
                CatalogValidationException.class,
                () -> AlterZoneCommand.builder().zoneName(zone).build(),
                "Name of the zone can't be null or blank"
        );
    }

    @Test
    void alterNonExistingZone() {
        CatalogCommand cmd = AlterZoneCommand.builder().zoneName("not_existing_zone").build();
        assertThrows(
                CatalogValidationException.class,
                () -> cmd.get(new UpdateContext(catalogWithDefaultZone())),
                "Distribution zone with name 'not_existing_zone' not found"
        );
    }

    @Test
    void zonePartitions() {
        assertThrows(
                CatalogValidationException.class,
                () -> alterZoneBuilder().partitions(42).build(),
                "Partitions number cannot be altered"
        );
    }

    @Test
    void zoneReplicas() {
        assertThrows(
                CatalogValidationException.class,
                () -> alterZoneBuilder().replicas(-1).build(),
                "Invalid number of replicas"
        );

        assertThrows(
                CatalogValidationException.class,
                () -> alterZoneBuilder().replicas(0).build(),
                "Invalid number of replicas"
        );

        // Let's check the success cases.
        alterZoneBuilder().replicas(1).build();
        alterZoneBuilder().replicas(Integer.MAX_VALUE).build();
        alterZoneBuilder().replicas(DEFAULT_REPLICA_COUNT).build();
    }

    private static List<Arguments> replicasChanges() {
        return List.of(
                arguments(1, 1, 2, 2),

                arguments(2, 2, 1, 1),
                arguments(2, 2, 3, 2),
                arguments(2, 2, 4, 2),
                arguments(2, 2, 5, 2),

                arguments(3, 2, 1, 1),

                arguments(5, 3, 4, 2),
                arguments(5, 2, 4, 2),
                arguments(5, 2, 6, 2),
                arguments(5, 2, 7, 2),

                arguments(6, 3, 4, 2)
        );
    }

    @ParameterizedTest
    @MethodSource("replicasChanges")
    void adjustQuorumSize(int initialReplicas, int quorumSize, int targetReplicas, int targetQuorumSize) {
        Catalog catalog = catalogWithDefaultZone(alterZoneBuilder()
                .replicas(initialReplicas)
                .quorumSize(quorumSize)
                .build()
        );

        assertThat(getZoneDescriptor(alterZoneBuilder().replicas(targetReplicas), catalog).quorumSize(), is(targetQuorumSize));
    }

    @Test
    void defaultZoneReplicas() {
        CatalogZoneDescriptor zoneDescriptor = getZoneDescriptor(alterZoneBuilder().replicas(DEFAULT_REPLICA_COUNT));
        assertThat(zoneDescriptor.quorumSize(), is(DEFAULT_ZONE_QUORUM_SIZE));
        assertThat(zoneDescriptor.consensusGroupSize(), is(DEFAULT_ZONE_QUORUM_SIZE * 2 - 1));
    }

    @ParameterizedTest
    @ValueSource(ints = {-1, 0})
    void zoneInvalidQuorumSize(int quorumSize) {
        assertThrows(
                CatalogValidationException.class,
                () -> alterZoneBuilder().quorumSize(quorumSize).build(),
                "Invalid quorum size"
        );
    }

    @ParameterizedTest
    @MethodSource("quorumTable")
    @SuppressWarnings("PMD.UnusedFormalParameter") // shared quorumTable() has extra params used by CreateZoneCommandValidationTest.
    void zoneQuorumSize(
            @Nullable Integer replicas,
            int defaultQuorumSize,
            int defaultConsensusGroupSize,
            int minQuorumSize,
            int maxQuorumSize
    ) {
        String errorMessageFragmentMinQuorum = minQuorumSize > 1
                ? "Quorum size is less than the minimum quorum value"
                : "Invalid quorum size"; // Special case when quorum size of 0 is rejected earlier.

        assertThrows(
                CatalogValidationException.class,
                () -> getZoneDescriptor(alterZoneBuilder().replicas(replicas).quorumSize(minQuorumSize - 1)),
                errorMessageFragmentMinQuorum
        );

        assertThrows(
                CatalogValidationException.class,
                () -> getZoneDescriptor(alterZoneBuilder().replicas(replicas).quorumSize(maxQuorumSize + 1)),
                "Quorum size exceeds the maximum quorum value"
        );

        for (int i = minQuorumSize; i <= maxQuorumSize; i++) {
            assertThat(getZoneDescriptor(alterZoneBuilder().replicas(replicas).quorumSize(i)).quorumSize(), is(i));
        }
    }

    @Test
    void zoneAutoAdjustScaleUp() {
        assertThrows(
                CatalogValidationException.class,
                () -> alterZoneBuilder().dataNodesAutoAdjustScaleUp(-1).build(),
                "Invalid data nodes auto adjust scale up"
        );

        // Let's check the success cases.
        alterZoneBuilder().dataNodesAutoAdjustScaleUp(IMMEDIATE_TIMER_VALUE).build();
        alterZoneBuilder().dataNodesAutoAdjustScaleUp(INFINITE_TIMER_VALUE).build();
        alterZoneBuilder().dataNodesAutoAdjustScaleUp(10).build();
    }

    @Test
    void zoneAutoAdjustScaleDown() {
        assertThrows(
                CatalogValidationException.class,
                () -> alterZoneBuilder().dataNodesAutoAdjustScaleDown(-1).build(),
                "Invalid data nodes auto adjust scale down"
        );

        // Let's check the success cases.
        alterZoneBuilder().dataNodesAutoAdjustScaleDown(IMMEDIATE_TIMER_VALUE).build();
        alterZoneBuilder().dataNodesAutoAdjustScaleDown(INFINITE_TIMER_VALUE).build();
        alterZoneBuilder().dataNodesAutoAdjustScaleDown(10).build();
    }

    @Test
    void zoneFilter() {
        assertThrows(
                CatalogValidationException.class,
                () -> alterZoneBuilder().filter("not a JsonPath").build(),
                "Invalid filter"
        );

        // Missing ']' after 'nodeAttributes'.
        assertThrows(
                CatalogValidationException.class,
                () -> alterZoneBuilder().filter("['nodeAttributes'[?(@.['region'] == 'EU')]").build(),
                "Invalid filter"
        );

        // Let's check the success cases.
        alterZoneBuilder().filter("['nodeAttributes'][?(@.['region'] == 'EU')]").build();
        alterZoneBuilder().filter(DEFAULT_FILTER).build();
    }

    private static AlterZoneCommandBuilder alterZoneBuilder() {
        return CatalogTestUtils.alterZoneBuilder(ZONE_NAME);
    }

    /**
     * Builds the command and gets update entries based on the catalog with the default zone. Needed because quorum size validation requires
     * knowing current (or previous) value of number of replicas.
     *
     * @param builder Alter zone command builder.
     * @return Catalog zone descriptor.
     */
    private static CatalogZoneDescriptor getZoneDescriptor(AlterZoneCommandBuilder builder) {
        return getZoneDescriptor(builder, catalogWithDefaultZone());
    }

    /**
     * Builds the command and gets update entries based on the provided catalog. Needed because quorum size validation requires knowing
     * current (or previous) value of number of replicas.
     *
     * @param builder Alter zone command builder.
     * @param catalog Catalog to alter.
     * @return Catalog zone descriptor.
     */
    private static CatalogZoneDescriptor getZoneDescriptor(AlterZoneCommandBuilder builder, Catalog catalog) {
        return ((AlterZoneEntry) builder.build().get(new UpdateContext(catalog)).get(0)).descriptor();
    }
}
