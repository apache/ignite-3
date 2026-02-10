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

package org.apache.ignite.internal.catalog;

import static org.apache.ignite.internal.catalog.CatalogService.DEFAULT_STORAGE_PROFILE;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.DEFAULT_FILTER;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.DEFAULT_PARTITION_COUNT;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.DEFAULT_REPLICA_COUNT;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.IMMEDIATE_TIMER_VALUE;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.INFINITE_TIMER_VALUE;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureExceptionMatcher.willThrow;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureExceptionMatcher.willThrowFast;
import static org.apache.ignite.internal.util.CompletableFutures.falseCompletedFuture;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Objects;
import org.apache.ignite.internal.catalog.commands.AlterZoneCommand;
import org.apache.ignite.internal.catalog.commands.AlterZoneSetDefaultCommand;
import org.apache.ignite.internal.catalog.commands.CreateZoneCommand;
import org.apache.ignite.internal.catalog.commands.DropZoneCommand;
import org.apache.ignite.internal.catalog.commands.RenameZoneCommand;
import org.apache.ignite.internal.catalog.commands.StorageProfileParams;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogZoneDescriptor;
import org.apache.ignite.internal.catalog.events.CatalogEvent;
import org.apache.ignite.internal.catalog.events.CatalogEventParameters;
import org.apache.ignite.internal.catalog.events.CreateZoneEventParameters;
import org.apache.ignite.internal.catalog.events.DropZoneEventParameters;
import org.apache.ignite.internal.event.EventListener;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Tests for zone related commands. */
public class CatalogZoneTest extends BaseCatalogManagerTest {

    private static final String DEFAULT_ZONE_TABLE = "DEFAULT_ZONE_TABLE";

    private static final String TEST_ZONE_NAME = "TEST_ZONE_NAME";

    @BeforeEach
    public void createTableWithLazyDefaultZone() {
        createSomeTable(DEFAULT_ZONE_TABLE);
    }

    @Test
    public void testCreateZone() {
        String zoneName = TEST_ZONE_NAME;

        CatalogCommand cmd = CreateZoneCommand.builder()
                .zoneName(zoneName)
                .partitions(42)
                .replicas(15)
                .filter("expression")
                .storageProfilesParams(List.of(StorageProfileParams.builder().storageProfile("test_profile").build()))
                .build();

        long beforeZoneCreated = clock.nowLong();

        tryApplyAndExpectApplied(cmd);

        // Validate catalog version from the past.
        assertNull(manager.activeCatalog(beforeZoneCreated).zone(zoneName));

        // Validate actual catalog
        CatalogZoneDescriptor zone = manager.activeCatalog(clock.nowLong()).zone(zoneName);

        assertNotNull(zone);

        // Validate newly created zone
        assertEquals(zoneName, zone.name());
        assertEquals(42, zone.partitions());
        assertEquals(15, zone.replicas());
        assertEquals(IMMEDIATE_TIMER_VALUE, zone.dataNodesAutoAdjustScaleUp());
        assertEquals(INFINITE_TIMER_VALUE, zone.dataNodesAutoAdjustScaleDown());
        assertEquals("expression", zone.filter());
        assertEquals("test_profile", zone.storageProfiles().profiles().get(0).storageProfile());

        // Operation increments catalog version.
        assertEquals(manager.activeCatalog(beforeZoneCreated).version() + 1, latestActiveCatalog().version());
    }

    @Test
    public void testSetDefaultZone() {
        CatalogZoneDescriptor initialDefaultZone = latestActiveCatalog().defaultZone();

        // Create new zone
        {
            StorageProfileParams storageProfile = StorageProfileParams.builder()
                    .storageProfile("test_profile")
                    .build();

            CatalogCommand createZoneCmd = CreateZoneCommand.builder()
                    .zoneName(TEST_ZONE_NAME)
                    .storageProfilesParams(List.of(storageProfile))
                    .build();

            tryApplyAndExpectApplied(createZoneCmd);

            assertNotEquals(TEST_ZONE_NAME, latestActiveCatalog().defaultZone().name());
        }

        // Set new zone as default.
        {
            CatalogCommand setDefaultCmd = AlterZoneSetDefaultCommand.builder()
                    .zoneName(TEST_ZONE_NAME)
                    .build();

            long beforeDefaultZoneChanged = clock.nowLong();

            tryApplyAndExpectApplied(setDefaultCmd);
            assertEquals(TEST_ZONE_NAME, latestActiveCatalog().defaultZone().name());

            // Make sure history has not been affected.
            Catalog prevCatalog = manager.activeCatalog(beforeDefaultZoneChanged);
            assertNotEquals(TEST_ZONE_NAME, prevCatalog.defaultZone().name());
            assertNotEquals(latestActiveCatalog().defaultZone().id(), prevCatalog.defaultZone().id());

            // Operation increments catalog version.
            assertEquals(manager.activeCatalog(beforeDefaultZoneChanged).version() + 1, latestActiveCatalog().version());
        }

        // Create table in the new zone.
        {
            tryApplyAndExpectApplied(simpleTable(TABLE_NAME));

            Catalog catalog = latestActiveCatalog();
            CatalogTableDescriptor tab = Objects.requireNonNull(catalog.table(SCHEMA_NAME, TABLE_NAME));

            assertEquals(catalog.defaultZone().id(), tab.zoneId());
        }

        // Setting default zone that is already the default changes nothing.
        {
            int lastVer =  manager.latestCatalogVersion();

            CatalogCommand setDefaultCmd = AlterZoneSetDefaultCommand.builder()
                    .zoneName(TEST_ZONE_NAME)
                    .build();

            tryApplyAndExpectNotApplied(setDefaultCmd);
            assertEquals(lastVer, manager.latestCatalogVersion());
        }

        // Drop old default zone.
        {
            tryApplyAndExpectApplied(dropTableCommand(DEFAULT_ZONE_TABLE));
            CatalogCommand dropCommand = DropZoneCommand.builder()
                    .zoneName(initialDefaultZone.name())
                    .build();

            tryApplyAndExpectApplied(dropCommand);
        }
    }

    @Test
    public void testDropZone() {
        String zoneName = TEST_ZONE_NAME;

        CatalogCommand cmd = CreateZoneCommand.builder()
                .zoneName(zoneName)
                .storageProfilesParams(List.of(StorageProfileParams.builder().storageProfile(DEFAULT_STORAGE_PROFILE).build()))
                .build();

        tryApplyAndExpectApplied(cmd);

        long beforeDropTimestamp = clock.nowLong();

        CatalogCommand dropCommand = DropZoneCommand.builder()
                .zoneName(zoneName)
                .build();

        tryApplyAndExpectApplied(dropCommand);

        // Validate catalog version from the past.
        Catalog catalog = manager.activeCatalog(beforeDropTimestamp);

        CatalogZoneDescriptor zone = catalog.zone(zoneName);

        assertNotNull(zone);
        assertEquals(zoneName, zone.name());
        assertSame(zone, catalog.zone(zone.id()));

        // Validate actual catalog
        catalog = latestActiveCatalog();
        assertNull(catalog.zone(zoneName));
        assertNull(catalog.zone(zone.id()));

        // Try to drop non-existing zone.
        assertThat(manager.execute(dropCommand),
                willThrow(CatalogValidationException.class, "Distribution zone with name 'TEST_ZONE_NAME' not found."));

        // Operation increments catalog version.
        assertEquals(manager.activeCatalog(beforeDropTimestamp).version() + 1, catalog.version());
    }

    @Test
    public void testDropDefaultZoneIsRejected() {
        // Drop default zone is rejected.
        {
            Catalog catalog = latestActiveCatalog();
            CatalogCommand dropCommand = DropZoneCommand.builder()
                    .zoneName(catalog.defaultZone().name())
                    .build();

            assertThat(manager.execute(dropCommand),
                    willThrow(CatalogValidationException.class, "Default distribution zone can't be dropped: zoneName=Default."));

            assertSame(catalog, manager.activeCatalog(clock.nowLong()));
            assertEquals(catalog.version(), manager.latestCatalogVersion());
        }

        // Renamed zone deletion is also rejected.
        {
            CatalogZoneDescriptor zoneBeforeRename = latestActiveCatalog().defaultZone();

            CatalogCommand renameCommand = RenameZoneCommand.builder()
                    .zoneName(zoneBeforeRename.name())
                    .newZoneName(TEST_ZONE_NAME)
                    .build();

            tryApplyAndExpectApplied(renameCommand);

            Catalog catalog = latestActiveCatalog();

            // Default zone was renamed successfully
            assertNull(catalog.zone(zoneBeforeRename.name()));
            assertNotNull(catalog.defaultZone());
            assertSame(catalog.zone(TEST_ZONE_NAME), catalog.defaultZone());

            CatalogCommand dropCommand = DropZoneCommand.builder()
                    .zoneName(TEST_ZONE_NAME)
                    .build();

            assertThat(manager.execute(dropCommand),
                    willThrow(CatalogValidationException.class, "Default distribution zone can't be dropped: zoneName=TEST_ZONE_NAME."));

            // Renamed default zone is still available.
            catalog = latestActiveCatalog();
            assertNotNull(catalog.defaultZone());
            assertSame(catalog.zone(TEST_ZONE_NAME), catalog.defaultZone());
        }
    }

    @Test
    public void testRenameZone() {
        String zoneName = TEST_ZONE_NAME;

        CatalogCommand cmd = CreateZoneCommand.builder()
                .zoneName(zoneName)
                .partitions(42)
                .replicas(15)
                .storageProfilesParams(List.of(StorageProfileParams.builder().storageProfile(DEFAULT_STORAGE_PROFILE).build()))
                .build();

        tryApplyAndExpectApplied(cmd);

        long beforeDropTimestamp = clock.nowLong();

        String newZoneName = "RenamedZone";

        CatalogCommand renameZoneCmd = RenameZoneCommand.builder()
                .zoneName(zoneName)
                .newZoneName(newZoneName)
                .build();

        tryApplyAndExpectApplied(renameZoneCmd);

        // Validate catalog version from the past.
        Catalog catalog = manager.activeCatalog(beforeDropTimestamp);
        CatalogZoneDescriptor oldZone = catalog.zone(zoneName);

        assertNotNull(oldZone);
        assertEquals(zoneName, oldZone.name());
        assertSame(oldZone, catalog.zone(oldZone.id()));

        // Validate actual catalog
        catalog = latestActiveCatalog();

        CatalogZoneDescriptor zone = catalog.zone(newZoneName);

        assertNotNull(zone);
        assertNull(catalog.zone(zoneName));
        assertEquals(newZoneName, zone.name());
        assertEquals(oldZone.id(), zone.id());

        // Operation increments catalog version.
        assertEquals(manager.activeCatalog(beforeDropTimestamp).version() + 1, catalog.version());
    }

    @Test
    public void testRenameDefaultZone() {
        CatalogZoneDescriptor defaultZone = latestActiveCatalog().defaultZone();

        assertNotEquals(TEST_ZONE_NAME, defaultZone.name());

        CatalogCommand renameZoneCmd = RenameZoneCommand.builder()
                .zoneName(defaultZone.name())
                .newZoneName(TEST_ZONE_NAME)
                .build();

        int ver = manager.latestCatalogVersion();
        tryApplyAndExpectApplied(renameZoneCmd);

        // Operation increments catalog version.
        assertEquals(ver + 1, manager.latestCatalogVersion());
        assertEquals(TEST_ZONE_NAME, latestActiveCatalog().defaultZone().name());
        assertEquals(defaultZone.id(), latestActiveCatalog().defaultZone().id());
    }

    @Test
    public void testDefaultZone() {
        CatalogZoneDescriptor defaultZone = latestActiveCatalog().defaultZone();

        int catalogVersion = manager.latestCatalogVersion();

        // Try to create zone with default zone name.
        CatalogCommand cmd = CreateZoneCommand.builder()
                .zoneName(defaultZone.name())
                .partitions(42)
                .replicas(15)
                .storageProfilesParams(List.of(StorageProfileParams.builder().storageProfile(DEFAULT_STORAGE_PROFILE).build()))
                .build();
        assertThat(manager.execute(cmd),
                willThrow(CatalogValidationException.class, "Distribution zone with name 'Default' already exists."));

        // Validate default zone wasn't changed.
        assertSame(defaultZone, latestActiveCatalog().zone(defaultZone.name()));
        assertEquals(catalogVersion, manager.latestCatalogVersion());
    }

    @Test
    public void testAlterZone() {
        String zoneName = TEST_ZONE_NAME;

        CatalogCommand cmd = CreateZoneCommand.builder()
                .zoneName(zoneName)
                .partitions(42)
                .replicas(15)
                .filter("expression")
                .storageProfilesParams(List.of(StorageProfileParams.builder().storageProfile(DEFAULT_STORAGE_PROFILE).build()))
                .build();

        CatalogCommand alterCmd = AlterZoneCommand.builder()
                .zoneName(zoneName)
                .replicas(2)
                .quorumSize(2) // need to adjust the quorum size as well in accordance with new replicas count
                .dataNodesAutoAdjustScaleUp(3)
                .dataNodesAutoAdjustScaleDown(4)
                .filter("newExpression")
                .storageProfilesParams(List.of(StorageProfileParams.builder().storageProfile("test_profile").build()))
                .build();

        tryApplyAndExpectApplied(cmd);

        long beforeZoneAltered = clock.nowLong();

        tryApplyAndExpectApplied(alterCmd);

        // Validate actual catalog
        CatalogZoneDescriptor zone = latestActiveCatalog().zone(zoneName);

        assertNotNull(zone);
        assertSame(zone.id(), manager.activeCatalog(beforeZoneAltered).zone(zoneName).id());

        assertEquals(zoneName, zone.name());
        assertEquals(42, zone.partitions());
        assertEquals(2, zone.replicas());
        assertEquals(2, zone.quorumSize());
        assertEquals(3, zone.dataNodesAutoAdjustScaleUp());
        assertEquals(4, zone.dataNodesAutoAdjustScaleDown());
        assertEquals("newExpression", zone.filter());
        assertEquals("test_profile", zone.storageProfiles().profiles().get(0).storageProfile());

        // Operation increments catalog version.
        assertEquals(manager.activeCatalog(beforeZoneAltered).version() + 1, latestActiveCatalog().version());
    }

    @Test
    public void testCreateZoneWithSameName() {
        String zoneName = TEST_ZONE_NAME;

        CatalogCommand cmd = CreateZoneCommand.builder()
                .zoneName(zoneName)
                .partitions(42)
                .replicas(15)
                .storageProfilesParams(List.of(StorageProfileParams.builder().storageProfile(DEFAULT_STORAGE_PROFILE).build()))
                .build();

        tryApplyAndExpectApplied(cmd);

        long afterFirstZoneCreated = clock.nowLong();

        // Try to create zone with same name.
        cmd = CreateZoneCommand.builder()
                .zoneName(zoneName)
                .partitions(8)
                .replicas(1)
                .storageProfilesParams(List.of(StorageProfileParams.builder().storageProfile(DEFAULT_STORAGE_PROFILE).build()))
                .build();

        assertThat(manager.execute(cmd), willThrowFast(CatalogValidationException.class, "Distribution zone with name 'TEST_ZONE_NAME'"));

        // Validate zone was NOT changed
        Catalog catalog = latestActiveCatalog();
        CatalogZoneDescriptor zone = catalog.zone(zoneName);

        assertNotNull(zone);
        assertSame(zone, catalog.zone(zone.id()));

        assertEquals(zoneName, zone.name());
        assertEquals(42, zone.partitions());
        assertEquals(15, zone.replicas());

        assertSame(catalog, manager.activeCatalog(afterFirstZoneCreated));
    }

    @Test
    public void testCreateZoneDropZoneReuseName() {
        String zoneName = TEST_ZONE_NAME;

        CatalogCommand cmd = CreateZoneCommand.builder()
                .zoneName(zoneName)
                .partitions(42)
                .replicas(15)
                .storageProfilesParams(List.of(StorageProfileParams.builder().storageProfile(DEFAULT_STORAGE_PROFILE).build()))
                .build();

        tryApplyAndExpectApplied(cmd);

        CatalogZoneDescriptor zone = latestActiveCatalog().zone(zoneName);
        assertNotNull(zone);

        // Try to create zone with same name.
        cmd = DropZoneCommand.builder().zoneName(zoneName).build();
        tryApplyAndExpectApplied(cmd);

        cmd = CreateZoneCommand.builder()
                .zoneName(zoneName)
                .partitions(10)
                .replicas(5)
                .storageProfilesParams(List.of(StorageProfileParams.builder().storageProfile(DEFAULT_STORAGE_PROFILE).build()))
                .build();
        tryApplyAndExpectApplied(cmd);

        CatalogZoneDescriptor newZone = latestActiveCatalog().zone(zoneName);
        assertNotNull(newZone);
        assertNotEquals(zone.id(), newZone.id());
        assertSame(newZone, latestActiveCatalog().zone(newZone.id()));

        assertEquals(zoneName, newZone.name());
        assertEquals(10, newZone.partitions());
        assertEquals(5, newZone.replicas());
    }

    @Test
    public void testCreateZoneEvents() {
        String zoneName = TEST_ZONE_NAME;

        CatalogCommand cmd = CreateZoneCommand.builder()
                .zoneName(zoneName)
                .storageProfilesParams(List.of(StorageProfileParams.builder().storageProfile(DEFAULT_STORAGE_PROFILE).build()))
                .build();

        EventListener<CatalogEventParameters> eventListener = mock(EventListener.class);
        when(eventListener.notify(any())).thenReturn(falseCompletedFuture());

        manager.listen(CatalogEvent.ZONE_CREATE, eventListener);
        manager.listen(CatalogEvent.ZONE_DROP, eventListener);

        tryApplyAndExpectApplied(cmd);

        verify(eventListener).notify(any(CreateZoneEventParameters.class));

        CatalogCommand dropCommand = DropZoneCommand.builder()
                .zoneName(zoneName)
                .build();

        tryApplyAndExpectApplied(dropCommand);

        verify(eventListener).notify(any(DropZoneEventParameters.class));
        verifyNoMoreInteractions(eventListener);
    }

    @Test
    void testCreateZoneWithDefaults() {
        tryApplyAndExpectApplied(
                CreateZoneCommand.builder()
                        .zoneName(TEST_ZONE_NAME)
                        .storageProfilesParams(
                                List.of(StorageProfileParams.builder().storageProfile(DEFAULT_STORAGE_PROFILE).build())
                        ).build()
        );

        CatalogZoneDescriptor zone = latestActiveCatalog().zone(TEST_ZONE_NAME);

        assertEquals(DEFAULT_PARTITION_COUNT, zone.partitions());
        assertEquals(DEFAULT_REPLICA_COUNT, zone.replicas());
        assertEquals(IMMEDIATE_TIMER_VALUE, zone.dataNodesAutoAdjustScaleUp());
        assertEquals(INFINITE_TIMER_VALUE, zone.dataNodesAutoAdjustScaleDown());
        assertEquals(DEFAULT_FILTER, zone.filter());
        assertEquals(DEFAULT_STORAGE_PROFILE, zone.storageProfiles().defaultProfile().storageProfile());
    }
}
