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
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
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
import java.util.concurrent.CompletableFuture;
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
import org.junit.jupiter.api.Test;

/** Tests for zone related commands. */
public class CatalogZoneTest extends BaseCatalogManagerTest {

    private static final String TEST_ZONE_NAME = "TEST_ZONE_NAME";

    @Test
    public void testCreateZone() {
        String zoneName = TEST_ZONE_NAME;

        CatalogCommand cmd = CreateZoneCommand.builder()
                .zoneName(zoneName)
                .partitions(42)
                .replicas(15)
                .dataNodesAutoAdjust(73)
                .filter("expression")
                .storageProfilesParams(List.of(StorageProfileParams.builder().storageProfile("test_profile").build()))
                .build();

        assertThat(manager.execute(cmd), willCompleteSuccessfully());

        // Validate catalog version from the past.
        assertNull(manager.zone(zoneName, 0));
        assertNull(manager.zone(zoneName, 123L));

        // Validate actual catalog
        CatalogZoneDescriptor zone = manager.zone(zoneName, clock.nowLong());

        assertNotNull(zone);
        assertSame(zone, manager.zone(zone.id(), clock.nowLong()));

        // Validate that catalog returns null for previous timestamps.
        assertNull(manager.zone(zone.id(), 0));
        assertNull(manager.zone(zone.id(), 123L));

        // Validate newly created zone
        assertEquals(zoneName, zone.name());
        assertEquals(42, zone.partitions());
        assertEquals(15, zone.replicas());
        assertEquals(73, zone.dataNodesAutoAdjust());
        assertEquals(INFINITE_TIMER_VALUE, zone.dataNodesAutoAdjustScaleUp());
        assertEquals(INFINITE_TIMER_VALUE, zone.dataNodesAutoAdjustScaleDown());
        assertEquals("expression", zone.filter());
        assertEquals("test_profile", zone.storageProfiles().profiles().get(0).storageProfile());
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

            assertThat(manager.execute(createZoneCmd), willCompleteSuccessfully());

            assertNotEquals(TEST_ZONE_NAME, latestActiveCatalog().defaultZone().name());
        }

        // Set new zone as default.
        {
            CatalogCommand setDefaultCmd = AlterZoneSetDefaultCommand.builder()
                    .zoneName(TEST_ZONE_NAME)
                    .build();

            int prevVer = latestActiveCatalog().version();

            assertThat(manager.execute(setDefaultCmd), willCompleteSuccessfully());
            assertEquals(TEST_ZONE_NAME, latestActiveCatalog().defaultZone().name());

            // Make sure history has not been affected.
            Catalog prevCatalog = Objects.requireNonNull(manager.catalog(prevVer));
            assertNotEquals(TEST_ZONE_NAME, prevCatalog.defaultZone().name());
            assertNotEquals(latestActiveCatalog().defaultZone().id(), prevCatalog.defaultZone().id());
        }

        // Create table in the new zone.
        {
            assertThat(manager.execute(simpleTable(TABLE_NAME)), willCompleteSuccessfully());

            Catalog catalog = latestActiveCatalog();
            CatalogTableDescriptor tab = Objects.requireNonNull(manager.table(TABLE_NAME, catalog.time()));

            assertEquals(catalog.defaultZone().id(), tab.zoneId());
        }

        // Setting default zone that is already the default changes nothing.
        {
            int lastVer =  manager.latestCatalogVersion();

            CatalogCommand setDefaultCmd = AlterZoneSetDefaultCommand.builder()
                    .zoneName(TEST_ZONE_NAME)
                    .build();

            assertThat(manager.execute(setDefaultCmd), willCompleteSuccessfully());
            assertEquals(lastVer, manager.latestCatalogVersion());
        }

        // Drop old default zone.
        {
            CatalogCommand dropCommand = DropZoneCommand.builder()
                    .zoneName(initialDefaultZone.name())
                    .build();

            assertThat(manager.execute(dropCommand), willCompleteSuccessfully());
        }
    }

    @Test
    public void testDropZone() {
        String zoneName = TEST_ZONE_NAME;

        CatalogCommand cmd = CreateZoneCommand.builder()
                .zoneName(zoneName)
                .storageProfilesParams(List.of(StorageProfileParams.builder().storageProfile(DEFAULT_STORAGE_PROFILE).build()))
                .build();

        assertThat(manager.execute(cmd), willCompleteSuccessfully());

        long beforeDropTimestamp = clock.nowLong();

        CatalogCommand dropCommand = DropZoneCommand.builder()
                .zoneName(zoneName)
                .build();

        CompletableFuture<?> fut = manager.execute(dropCommand);

        assertThat(fut, willCompleteSuccessfully());

        // Validate catalog version from the past.
        CatalogZoneDescriptor zone = manager.zone(zoneName, beforeDropTimestamp);

        assertNotNull(zone);
        assertEquals(zoneName, zone.name());

        assertSame(zone, manager.zone(zone.id(), beforeDropTimestamp));

        // Validate actual catalog
        assertNull(manager.zone(zoneName, clock.nowLong()));
        assertNull(manager.zone(zone.id(), clock.nowLong()));

        // Try to drop non-existing zone.
        assertThat(manager.execute(dropCommand), willThrow(DistributionZoneNotFoundValidationException.class));
    }

    @Test
    public void testDropDefaultZoneIsRejected() {
        // Drop default zone is rejected.
        {
            Catalog catalog = latestActiveCatalog();
            CatalogCommand dropCommand = DropZoneCommand.builder()
                    .zoneName(catalog.defaultZone().name())
                    .build();

            int ver = catalog.version();

            assertThat(manager.execute(dropCommand), willThrow(DistributionZoneCantBeDroppedValidationException.class));

            assertEquals(ver, manager.latestCatalogVersion());
        }

        // Renamed zone deletion is also rejected.
        {
            CatalogCommand renameCommand = RenameZoneCommand.builder()
                    .zoneName(latestActiveCatalog().defaultZone().name())
                    .newZoneName(TEST_ZONE_NAME)
                    .build();

            int ver = manager.latestCatalogVersion();

            assertThat(manager.execute(renameCommand), willCompleteSuccessfully());

            assertSame(ver + 1, manager.latestCatalogVersion());

            ver = manager.latestCatalogVersion();

            CatalogCommand dropCommand = DropZoneCommand.builder()
                    .zoneName(TEST_ZONE_NAME)
                    .build();

            assertThat(manager.execute(dropCommand), willThrow(DistributionZoneCantBeDroppedValidationException.class));
            assertSame(ver, manager.latestCatalogVersion());
        }
    }

    @Test
    public void testRenameZone() throws InterruptedException {
        String zoneName = TEST_ZONE_NAME;

        CatalogCommand cmd = CreateZoneCommand.builder()
                .zoneName(zoneName)
                .partitions(42)
                .replicas(15)
                .storageProfilesParams(List.of(StorageProfileParams.builder().storageProfile(DEFAULT_STORAGE_PROFILE).build()))
                .build();

        assertThat(manager.execute(cmd), willCompleteSuccessfully());

        long beforeDropTimestamp = clock.nowLong();

        Thread.sleep(5);

        String newZoneName = "RenamedZone";

        CatalogCommand renameZoneCmd = RenameZoneCommand.builder()
                .zoneName(zoneName)
                .newZoneName(newZoneName)
                .build();

        assertThat(manager.execute(renameZoneCmd), willCompleteSuccessfully());

        // Validate catalog version from the past.
        CatalogZoneDescriptor zone = manager.zone(zoneName, beforeDropTimestamp);

        assertNotNull(zone);
        assertEquals(zoneName, zone.name());

        assertSame(zone, manager.zone(zone.id(), beforeDropTimestamp));

        // Validate actual catalog
        zone = manager.zone(newZoneName, clock.nowLong());

        assertNotNull(zone);
        assertNull(manager.zone(zoneName, clock.nowLong()));
        assertEquals(newZoneName, zone.name());

        assertSame(zone, manager.zone(zone.id(), clock.nowLong()));
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
        assertThat(manager.execute(renameZoneCmd), willCompleteSuccessfully());

        assertEquals(ver + 1, manager.latestCatalogVersion());
        assertEquals(TEST_ZONE_NAME, latestActiveCatalog().defaultZone().name());
        assertEquals(defaultZone.id(), latestActiveCatalog().defaultZone().id());
    }

    @Test
    public void testDefaultZone() {
        CatalogZoneDescriptor defaultZone = latestActiveCatalog().defaultZone();

        // Try to create zone with default zone name.
        CatalogCommand cmd = CreateZoneCommand.builder()
                .zoneName(defaultZone.name())
                .partitions(42)
                .replicas(15)
                .storageProfilesParams(List.of(StorageProfileParams.builder().storageProfile(DEFAULT_STORAGE_PROFILE).build()))
                .build();
        assertThat(manager.execute(cmd), willThrow(DistributionZoneExistsValidationException.class));

        // Validate default zone wasn't changed.
        assertSame(defaultZone, manager.zone(defaultZone.name(), clock.nowLong()));
    }

    @Test
    public void testAlterZone() {
        String zoneName = TEST_ZONE_NAME;

        CatalogCommand cmd = CreateZoneCommand.builder()
                .zoneName(zoneName)
                .partitions(42)
                .replicas(15)
                .dataNodesAutoAdjust(73)
                .filter("expression")
                .storageProfilesParams(List.of(StorageProfileParams.builder().storageProfile(DEFAULT_STORAGE_PROFILE).build()))
                .build();

        CatalogCommand alterCmd = AlterZoneCommand.builder()
                .zoneName(zoneName)
                .partitions(10)
                .replicas(2)
                .dataNodesAutoAdjustScaleUp(3)
                .dataNodesAutoAdjustScaleDown(4)
                .filter("newExpression")
                .storageProfilesParams(List.of(StorageProfileParams.builder().storageProfile("test_profile").build()))
                .build();

        assertThat(manager.execute(cmd), willCompleteSuccessfully());
        assertThat(manager.execute(alterCmd), willCompleteSuccessfully());

        // Validate actual catalog
        CatalogZoneDescriptor zone = manager.zone(zoneName, clock.nowLong());
        assertNotNull(zone);
        assertSame(zone, manager.zone(zone.id(), clock.nowLong()));

        assertEquals(zoneName, zone.name());
        assertEquals(10, zone.partitions());
        assertEquals(2, zone.replicas());
        assertEquals(INFINITE_TIMER_VALUE, zone.dataNodesAutoAdjust());
        assertEquals(3, zone.dataNodesAutoAdjustScaleUp());
        assertEquals(4, zone.dataNodesAutoAdjustScaleDown());
        assertEquals("newExpression", zone.filter());
        assertEquals("test_profile", zone.storageProfiles().profiles().get(0).storageProfile());
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

        assertThat(manager.execute(cmd), willCompleteSuccessfully());

        // Try to create zone with same name.
        cmd = CreateZoneCommand.builder()
                .zoneName(zoneName)
                .partitions(8)
                .replicas(1)
                .storageProfilesParams(List.of(StorageProfileParams.builder().storageProfile(DEFAULT_STORAGE_PROFILE).build()))
                .build();

        assertThat(manager.execute(cmd), willThrowFast(DistributionZoneExistsValidationException.class));

        // Validate zone was NOT changed
        CatalogZoneDescriptor zone = manager.zone(zoneName, clock.nowLong());

        assertNotNull(zone);
        assertSame(zone, manager.zone(zoneName, clock.nowLong()));
        assertSame(zone, manager.zone(zone.id(), clock.nowLong()));

        assertEquals(zoneName, zone.name());
        assertEquals(42, zone.partitions());
        assertEquals(15, zone.replicas());
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

        CompletableFuture<?> fut = manager.execute(cmd);

        assertThat(fut, willCompleteSuccessfully());

        verify(eventListener).notify(any(CreateZoneEventParameters.class));

        CatalogCommand dropCommand = DropZoneCommand.builder()
                .zoneName(zoneName)
                .build();

        fut = manager.execute(dropCommand);

        assertThat(fut, willCompleteSuccessfully());

        verify(eventListener).notify(any(DropZoneEventParameters.class));
        verifyNoMoreInteractions(eventListener);
    }

    @Test
    void testCreateZoneWithDefaults() {
        assertThat(
                manager.execute(
                        CreateZoneCommand.builder()
                                .zoneName(TEST_ZONE_NAME)
                                .storageProfilesParams(
                                        List.of(StorageProfileParams.builder().storageProfile(DEFAULT_STORAGE_PROFILE).build())
                                ).build()
                ),
                willCompleteSuccessfully()
        );

        CatalogZoneDescriptor zone = manager.zone(TEST_ZONE_NAME, clock.nowLong());

        assertEquals(DEFAULT_PARTITION_COUNT, zone.partitions());
        assertEquals(DEFAULT_REPLICA_COUNT, zone.replicas());
        assertEquals(INFINITE_TIMER_VALUE, zone.dataNodesAutoAdjust());
        assertEquals(IMMEDIATE_TIMER_VALUE, zone.dataNodesAutoAdjustScaleUp());
        assertEquals(INFINITE_TIMER_VALUE, zone.dataNodesAutoAdjustScaleDown());
        assertEquals(DEFAULT_FILTER, zone.filter());
        assertEquals(DEFAULT_STORAGE_PROFILE, zone.storageProfiles().defaultProfile().storageProfile());
    }
}
