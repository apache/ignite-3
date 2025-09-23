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

import static java.lang.Math.round;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.DEFAULT_FILTER;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.DEFAULT_PARTITION_COUNT;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.DEFAULT_REPLICA_COUNT;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.IMMEDIATE_TIMER_VALUE;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.INFINITE_TIMER_VALUE;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.MAX_PARTITION_COUNT;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.defaultQuorumSize;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.assertThrows;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import java.util.List;
import org.apache.ignite.internal.catalog.Catalog;
import org.apache.ignite.internal.catalog.CatalogCommand;
import org.apache.ignite.internal.catalog.CatalogTestUtils;
import org.apache.ignite.internal.catalog.CatalogValidationException;
import org.apache.ignite.internal.catalog.UpdateContext;
import org.apache.ignite.internal.catalog.descriptors.CatalogZoneDescriptor;
import org.apache.ignite.internal.catalog.descriptors.ConsistencyMode;
import org.apache.ignite.internal.catalog.storage.NewZoneEntry;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Tests to verify validation of {@link CreateZoneCommand}.
 */
@SuppressWarnings("ThrowableNotThrown")
public class CreateZoneCommandValidationTest extends AbstractCommandValidationTest {
    @ParameterizedTest(name = "[{index}] ''{argumentsWithNames}''")
    @MethodSource("nullAndBlankStrings")
    void zoneNameMustNotBeNullOrBlank(String zone) {
        assertThrows(
                CatalogValidationException.class,
                () -> CreateZoneCommand.builder().zoneName(zone).build(),
                "Name of the zone can't be null or blank"
        );
    }

    @Test
    void zonePartitions() {
        assertThrows(
                CatalogValidationException.class,
                () -> createZoneBuilder().partitions(-1).build(),
                "Invalid number of partitions"
        );

        assertThrows(
                CatalogValidationException.class,
                () -> createZoneBuilder().partitions(0).build(),
                "Invalid number of partitions"
        );

        assertThrows(
                CatalogValidationException.class,
                () -> createZoneBuilder().partitions(MAX_PARTITION_COUNT + 1).build(),
                "Invalid number of partitions"
        );

        // Let's check the success cases.
        createZoneBuilder().partitions(1).build();
        createZoneBuilder().partitions(MAX_PARTITION_COUNT).build();
        createZoneBuilder().partitions(10).build();
        createZoneBuilder().partitions(DEFAULT_PARTITION_COUNT).build();
    }

    @Test
    void zoneReplicas() {
        assertThrows(
                CatalogValidationException.class,
                () -> createZoneBuilder().replicas(-1).build(),
                "Invalid number of replicas"
        );

        assertThrows(
                CatalogValidationException.class,
                () -> createZoneBuilder().replicas(0).build(),
                "Invalid number of replicas"
        );

        // Let's check the success cases.
        createZoneBuilder().replicas(1).build();
        createZoneBuilder().replicas(Integer.MAX_VALUE).build();
        createZoneBuilder().replicas(DEFAULT_REPLICA_COUNT).build();
    }

    @ParameterizedTest
    @ValueSource(ints = {-1, 0})
    void zoneInvalidQuorumSize(int quorumSize) {
        assertThrows(
                CatalogValidationException.class,
                () -> createZoneBuilder().quorumSize(quorumSize).build(),
                "Invalid quorum size"
        );
    }

    @ParameterizedTest
    @MethodSource("quorumTable")
    void zoneQuorumSize(
            @Nullable Integer replicas,
            int defaultQuorumSize,
            int defaultConsensusGroupSize,
            int minQuorumSize,
            int maxQuorumSize
    ) {
        String errorMessageFragmentMinQuorum = minQuorumSize > 1
                ? "Specified quorum size doesn't fit into the specified replicas count"
                : "Invalid quorum size"; // Special case when quorum size of 0 is rejected earlier.

        assertThrows(
                CatalogValidationException.class,
                () -> getZoneDescriptor(createZoneBuilder().replicas(replicas).quorumSize(minQuorumSize - 1)),
                errorMessageFragmentMinQuorum
        );

        String errorMessageFragmentMaxQuorum = replicas != null
                ? "Specified quorum size doesn't fit into the specified replicas count"
                : "Specified quorum size doesn't fit into the default replicas count"; // Special case when replicas count is not specified

        assertThrows(
                CatalogValidationException.class,
                () -> getZoneDescriptor(createZoneBuilder().replicas(replicas).quorumSize(maxQuorumSize + 1)),
                errorMessageFragmentMaxQuorum
        );

        CatalogZoneDescriptor zoneDescriptor = getZoneDescriptor(createZoneBuilder().replicas(replicas));
        assertThat(zoneDescriptor.quorumSize(), is(defaultQuorumSize));
        assertThat(zoneDescriptor.consensusGroupSize(), is(defaultConsensusGroupSize));

        for (int i = minQuorumSize; i <= maxQuorumSize; i++) {
            getZoneDescriptor(createZoneBuilder().replicas(replicas).quorumSize(i));
        }
    }

    @Test
    void extremeQuorumSize() {
        int replicas = Integer.MAX_VALUE; // REPLICAS = ALL
        int maxQuorumSize = (int) (round(replicas / 2.0));
        int defaultQuorumSize = defaultQuorumSize(replicas);
        int defaultConsensusGroupSize = defaultQuorumSize * 2 - 1;

        String errorMessageFragmentMaxQuorum = "Specified quorum size doesn't fit into the specified replicas count";

        assertThrows(
                CatalogValidationException.class,
                () -> getZoneDescriptor(createZoneBuilder().replicas(replicas).quorumSize(maxQuorumSize + 1)),
                errorMessageFragmentMaxQuorum
        );

        CatalogZoneDescriptor zoneDescriptor = getZoneDescriptor(createZoneBuilder().replicas(replicas));
        assertThat(zoneDescriptor.quorumSize(), is(defaultQuorumSize));
        assertThat(zoneDescriptor.consensusGroupSize(), is(defaultConsensusGroupSize));

        zoneDescriptor = getZoneDescriptor(createZoneBuilder().replicas(replicas).quorumSize(maxQuorumSize - 1));
        assertThat(zoneDescriptor.quorumSize(), is(maxQuorumSize - 1));
    }

    @Test
    void zoneAutoAdjustScaleUp() {
        assertThrows(
                CatalogValidationException.class,
                () -> createZoneBuilder().dataNodesAutoAdjustScaleUp(-1).build(),
                "Invalid data nodes auto adjust scale up"
        );

        // Let's check the success cases.
        createZoneBuilder().dataNodesAutoAdjustScaleUp(IMMEDIATE_TIMER_VALUE).build();
        createZoneBuilder().dataNodesAutoAdjustScaleUp(INFINITE_TIMER_VALUE).build();
        createZoneBuilder().dataNodesAutoAdjustScaleUp(10).build();
    }

    @Test
    void zoneAutoAdjustScaleDown() {
        assertThrows(
                CatalogValidationException.class,
                () -> createZoneBuilder().dataNodesAutoAdjustScaleDown(-1).build(),
                "Invalid data nodes auto adjust scale down"
        );

        // Let's check the success cases.
        createZoneBuilder().dataNodesAutoAdjustScaleDown(IMMEDIATE_TIMER_VALUE).build();
        createZoneBuilder().dataNodesAutoAdjustScaleDown(INFINITE_TIMER_VALUE).build();
        createZoneBuilder().dataNodesAutoAdjustScaleDown(10).build();
    }

    @Test
    void zoneFilter() {
        assertThrows(
                CatalogValidationException.class,
                () -> createZoneBuilder().filter("not a JsonPath").build(),
                "Invalid filter"
        );

        // Missing ']' after 'nodeAttributes'.
        assertThrows(
                CatalogValidationException.class,
                () -> createZoneBuilder().filter("['nodeAttributes'[?(@.['region'] == 'EU')]").build(),
                "Invalid filter"
        );

        // Let's check the success cases.
        createZoneBuilder().filter("['nodeAttributes'][?(@.['region'] == 'EU')]").build();
        createZoneBuilder().filter(DEFAULT_FILTER).build();
    }

    @Test
    void zoneStorageProfiles() {
        assertThrows(
                CatalogValidationException.class,
                () -> createZoneBuilder().storageProfilesParams(List.of()).build(),
                "Storage profile cannot be empty"
        );

        assertThrows(
                CatalogValidationException.class,
                () -> createZoneBuilder().storageProfilesParams(null).build(),
                "Storage profile cannot be null"
        );

        // Let's check the success case.
        createZoneBuilder().storageProfilesParams(
                List.of(StorageProfileParams.builder().storageProfile("lru_rocks").build())).build();
    }

    @Test
    void zoneConsistencyMode() {
        // Let's check the success case.
        createZoneBuilder().consistencyModeParams(ConsistencyMode.STRONG_CONSISTENCY).build();
        createZoneBuilder().consistencyModeParams(ConsistencyMode.HIGH_AVAILABILITY).build();
        // STRONG_CONSISTENCY is used in this case.
        createZoneBuilder().consistencyModeParams(null).build();
    }

    @Test
    void exceptionIsThrownIfZoneAlreadyExist() {
        Catalog catalog = catalogWithZone("some_zone");

        CatalogCommand command = createZoneCommand("some_zone");

        assertThrows(
                CatalogValidationException.class,
                () -> command.get(new UpdateContext(catalog)),
                "Distribution zone with name 'some_zone' already exists"
        );
    }

    private static CatalogCommand createZoneParams(@Nullable Integer scaleUp, @Nullable Integer scaleDown) {
        return createZoneBuilder()
                .dataNodesAutoAdjustScaleUp(scaleUp)
                .dataNodesAutoAdjustScaleDown(scaleDown)
                .build();
    }

    private static CreateZoneCommandBuilder createZoneBuilder() {
        return CatalogTestUtils.createZoneBuilder(ZONE_NAME);
    }

    /**
     * Builds the command and gets update entries based on the catalog with the default zone. Needed because quorum size validation requires
     * knowing a number of replicas.
     *
     * @param builder Create zone command builder.
     * @return Catalog zone descriptor.
     */
    private static CatalogZoneDescriptor getZoneDescriptor(CreateZoneCommandBuilder builder) {
        return ((NewZoneEntry) builder.build().get(new UpdateContext(emptyCatalog())).get(0)).descriptor();
    }
}
