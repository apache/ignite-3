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

import static org.apache.ignite.internal.catalog.CatalogTestUtils.columnParams;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.sql.ColumnType.INT32;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;

import it.unimi.dsi.fastutil.ints.Int2IntMap;
import java.util.List;
import org.apache.ignite.internal.catalog.Catalog;
import org.apache.ignite.internal.catalog.commands.CreateTableCommand;
import org.apache.ignite.internal.catalog.commands.CreateTableCommandBuilder;
import org.apache.ignite.internal.catalog.commands.CreateZoneCommand;
import org.apache.ignite.internal.catalog.commands.CreateZoneCommandBuilder;
import org.apache.ignite.internal.catalog.commands.DropTableCommand;
import org.apache.ignite.internal.catalog.commands.DropTableCommandBuilder;
import org.apache.ignite.internal.catalog.commands.DropZoneCommand;
import org.apache.ignite.internal.catalog.commands.DropZoneCommandBuilder;
import org.apache.ignite.internal.catalog.commands.StorageProfileParams;
import org.apache.ignite.internal.catalog.commands.TableHashPrimaryKey;
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

    // TODO https://issues.apache.org/jira/browse/IGNITE-22522 Remove this test.
    @Test
    void testCollectTablesWithPartitionsBetween() {
        CreateTableCommandBuilder tableCmdBuilder = CreateTableCommand.builder()
                .schemaName("PUBLIC")
                .columns(List.of(columnParams("key1", INT32), columnParams("key2", INT32), columnParams("val", INT32, true)))
                .primaryKey(TableHashPrimaryKey.builder().columns(List.of("key1", "key2")).build())
                .colocationColumns(List.of("key2"));

        DropTableCommandBuilder dropTableCommandBuilder = DropTableCommand.builder()
                        .schemaName("PUBLIC");

        long from1 = clockService.nowLong();

        assertThat(catalogManager.execute(tableCmdBuilder.tableName("test1").build()), willCompleteSuccessfully());
        assertThat(catalogManager.execute(dropTableCommandBuilder.tableName("test1").build()), willCompleteSuccessfully());

        long from2 = clockService.nowLong();

        assertThat(catalogManager.execute(tableCmdBuilder.tableName("test2").build()), willCompleteSuccessfully());
        assertThat(catalogManager.execute(dropTableCommandBuilder.tableName("test2").build()), willCompleteSuccessfully());

        long from3 = clockService.nowLong();
        assertThat(catalogManager.execute(tableCmdBuilder.tableName("test3").build()), willCompleteSuccessfully());
        assertThat(catalogManager.execute(dropTableCommandBuilder.tableName("test3").build()), willCompleteSuccessfully());

        {
            Int2IntMap tablesWithParts = catalogManagerFacade.collectTablesWithPartitionsBetween(
                    from1,
                    clockService.nowLong());

            assertThat(tablesWithParts.keySet(), hasSize(3));
        }

        {
            Int2IntMap tablesWithParts = catalogManagerFacade.collectTablesWithPartitionsBetween(
                    from2,
                    clockService.nowLong());

            assertThat(tablesWithParts.keySet(), hasSize(2));
        }

        {
            Int2IntMap tablesWithParts = catalogManagerFacade.collectTablesWithPartitionsBetween(
                    from3,
                    clockService.nowLong());

            assertThat(tablesWithParts.keySet(), hasSize(1));
        }

        {
            Int2IntMap tablesWithParts = catalogManagerFacade.collectTablesWithPartitionsBetween(
                    clockService.nowLong(),
                    clockService.nowLong());

            assertThat(tablesWithParts.keySet(), hasSize(0));
        }
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
            Int2IntMap tablesWithParts = catalogManagerFacade.collectZonesWithPartitionsBetween(
                    from1,
                    clockService.nowLong());

            assertThat(tablesWithParts.keySet(), hasSize(4));
        }

        {
            Int2IntMap tablesWithParts = catalogManagerFacade.collectZonesWithPartitionsBetween(
                    from2,
                    clockService.nowLong());

            assertThat(tablesWithParts.keySet(), hasSize(3));
        }

        {
            Int2IntMap tablesWithParts = catalogManagerFacade.collectZonesWithPartitionsBetween(
                    from3,
                    clockService.nowLong());

            assertThat(tablesWithParts.keySet(), hasSize(2));
        }

        {
            Int2IntMap tablesWithParts = catalogManagerFacade.collectZonesWithPartitionsBetween(
                    clockService.nowLong(),
                    clockService.nowLong());

            assertThat(tablesWithParts.keySet(), hasSize(1));
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
