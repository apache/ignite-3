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

package org.apache.ignite.internal.catalog.compaction;

import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;

import it.unimi.dsi.fastutil.ints.Int2IntMap;
import java.util.List;
import org.apache.ignite.internal.catalog.Catalog;
import org.apache.ignite.internal.catalog.commands.CreateZoneCommand;
import org.apache.ignite.internal.catalog.commands.CreateZoneCommandBuilder;
import org.apache.ignite.internal.catalog.commands.DropZoneCommand;
import org.apache.ignite.internal.catalog.commands.DropZoneCommandBuilder;
import org.apache.ignite.internal.catalog.commands.StorageProfileParams;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link CatalogManagerCompactionFacade}.
 */
class CatalogManagerCompactionFacadeTest extends AbstractCatalogCompactionTest {
    private CatalogManagerCompactionFacade catalogManagerFacade;

    @BeforeEach
    void setupHelper() {
        catalogManagerFacade = new CatalogManagerCompactionFacade(catalogManager);
    }

    @Test
    void testCollectZonesWithPartitionsBetween() {
        CreateZoneCommandBuilder zoneCommandBuilder = CreateZoneCommand.builder()
                .storageProfilesParams(List.of(StorageProfileParams.builder().storageProfile("ai-persist").build()))
                .partitions(1);

        DropZoneCommandBuilder dropZoneCommandBuilder = DropZoneCommand.builder();

        long from1 = clockService.nowLong();

        assertThat(catalogManager.execute(zoneCommandBuilder.zoneName("test1").build()), willCompleteSuccessfully());
        assertThat(catalogManager.execute(dropZoneCommandBuilder.zoneName("test1").build()), willCompleteSuccessfully());

        long from2 = clockService.nowLong();

        assertThat(catalogManager.execute(zoneCommandBuilder.zoneName("test2").build()), willCompleteSuccessfully());
        assertThat(catalogManager.execute(dropZoneCommandBuilder.zoneName("test2").build()), willCompleteSuccessfully());

        long from3 = clockService.nowLong();
        assertThat(catalogManager.execute(zoneCommandBuilder.zoneName("test3").build()), willCompleteSuccessfully());
        assertThat(catalogManager.execute(dropZoneCommandBuilder.zoneName("test3").build()), willCompleteSuccessfully());

        // Take into account that there is the default zone.
        {
            Int2IntMap zonesWithParts = catalogManagerFacade.collectZonesWithPartitionsBetween(
                    from1,
                    clockService.nowLong());

            assertThat(zonesWithParts.keySet(), hasSize(3));
        }

        {
            Int2IntMap zonesWithParts = catalogManagerFacade.collectZonesWithPartitionsBetween(
                    from2,
                    clockService.nowLong());

            assertThat(zonesWithParts.keySet(), hasSize(2));
        }

        {
            Int2IntMap zonesWithParts = catalogManagerFacade.collectZonesWithPartitionsBetween(
                    from3,
                    clockService.nowLong());

            assertThat(zonesWithParts.keySet(), hasSize(1));
        }

        {
            Int2IntMap zonesWithParts = catalogManagerFacade.collectZonesWithPartitionsBetween(
                    clockService.nowLong(),
                    clockService.nowLong());

            assertThat(zonesWithParts.keySet(), hasSize(0));
        }
    }

    @Test
    void testCatalogPriorToVersionAtTsNullable() {
        Catalog earliestCatalog = catalogManager.catalog(catalogManager.earliestCatalogVersion());
        assertNotNull(earliestCatalog);

        assertNull(catalogManagerFacade.catalogPriorToVersionAtTsNullable(earliestCatalog.time()));
    }

    @Test
    void testCatalogAtTsNullable() {
        Catalog earliestCatalog = catalogManager.catalog(catalogManager.earliestCatalogVersion());
        assertNotNull(earliestCatalog);

        assertSame(earliestCatalog, catalogManagerFacade.catalogAtTsNullable(earliestCatalog.time()));
    }
}
