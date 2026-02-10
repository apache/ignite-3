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

package org.apache.ignite.internal.catalog.descriptors;

import static java.util.Collections.emptyList;
import static org.apache.ignite.internal.catalog.CatalogService.DEFAULT_STORAGE_PROFILE;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.fromParams;
import static org.apache.ignite.internal.catalog.descriptors.CatalogZoneDescriptor.updateRequiresAssignmentsRecalculation;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import java.util.function.Function;
import org.apache.ignite.internal.catalog.Catalog;
import org.apache.ignite.internal.catalog.UpdateContext;
import org.apache.ignite.internal.catalog.commands.AlterZoneCommand;
import org.apache.ignite.internal.catalog.commands.AlterZoneCommandBuilder;
import org.apache.ignite.internal.catalog.commands.StorageProfileParams;
import org.apache.ignite.internal.catalog.storage.AlterZoneEntry;
import org.junit.jupiter.api.Test;

class CatalogZoneDescriptorTest {
    @Test
    void toStringContainsTypeAndFields() {
        var descriptor = createZoneDescriptor();

        String toString = descriptor.toString();

        assertThat(toString, startsWith("CatalogZoneDescriptor ["));
        assertThat(toString, containsString("id=1"));
        assertThat(toString, containsString("name=zone1"));
        assertThat(toString, containsString("partitions=2"));
        assertThat(toString, containsString("replicas=5"));
        assertThat(toString, containsString("quorumSize=3"));
        assertThat(toString, containsString("dataNodesAutoAdjustScaleUp=6"));
        assertThat(toString, containsString("dataNodesAutoAdjustScaleDown=7"));
        assertThat(toString, containsString("filter=the-filter"));
        assertThat(toString, containsString("storageProfiles=CatalogStorageProfilesDescriptor ["));
        assertThat(toString, containsString("consistencyMode=STRONG_CONSISTENCY"));
    }

    @Test
    void testUpdateRequiresAssignmentsRecalculationAutoAdjustScaleUp() {
        doTestUpdateRequiresAssignmentsRecalculation(builder -> builder.dataNodesAutoAdjustScaleUp(100), false);
    }

    @Test
    void testUpdateRequiresAssignmentsRecalculationAutoAdjustScaleDown() {
        doTestUpdateRequiresAssignmentsRecalculation(builder -> builder.dataNodesAutoAdjustScaleDown(100), false);
    }

    @Test
    void testUpdateRequiresAssignmentsRecalculationFilter() {
        doTestUpdateRequiresAssignmentsRecalculation(builder -> builder.filter("foo"), true);
    }

    @Test
    void testUpdateRequiresAssignmentsRecalculationReplicas() {
        doTestUpdateRequiresAssignmentsRecalculation(builder -> builder.replicas(100500), true);
    }

    @Test
    void testUpdateRequiresAssignmentsRecalculationQuorumSize() {
        doTestUpdateRequiresAssignmentsRecalculation(builder -> builder.quorumSize(2), true);
    }

    @Test
    void testUpdateRequiresAssignmentsRecalculationStorageProfiles() {
        doTestUpdateRequiresAssignmentsRecalculation(builder -> builder.storageProfilesParams(
                List.of(StorageProfileParams.builder().storageProfile(DEFAULT_STORAGE_PROFILE + "2").build())
        ), true);
    }

    private static void doTestUpdateRequiresAssignmentsRecalculation(
            Function<AlterZoneCommandBuilder, AlterZoneCommandBuilder> alter,
            boolean expectedResult
    ) {
        var oldZone = createZoneDescriptor();

        Catalog catalog = createCatalogWithSingleZone(oldZone);

        var alterZoneCommand = (AlterZoneCommand) alter.apply(AlterZoneCommand.builder().zoneName(oldZone.name())).build();

        var alterZoneEntry = (AlterZoneEntry) alterZoneCommand.get(new UpdateContext(catalog)).get(0);
        alterZoneEntry.applyUpdate(catalog, oldZone.updateTimestamp().addPhysicalTime(1));

        CatalogZoneDescriptor newZone = alterZoneEntry.descriptor();

        assertEquals(expectedResult, updateRequiresAssignmentsRecalculation(oldZone, newZone));
    }

    private static CatalogZoneDescriptor createZoneDescriptor() {
        return new CatalogZoneDescriptor(
                1,
                "zone1",
                2,
                5,
                3,
                6,
                7,
                "the-filter",
                fromParams(List.of(StorageProfileParams.builder().storageProfile(DEFAULT_STORAGE_PROFILE).build())),
                ConsistencyMode.STRONG_CONSISTENCY
        );
    }

    private static Catalog createCatalogWithSingleZone(CatalogZoneDescriptor zone) {
        return new Catalog(1, 1, 1, List.of(zone), emptyList(), zone.id());
    }
}
