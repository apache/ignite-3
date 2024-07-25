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

import it.unimi.dsi.fastutil.ints.Int2IntMap;
import java.util.List;
import org.apache.ignite.internal.catalog.Catalog;
import org.apache.ignite.internal.catalog.commands.CreateTableCommand;
import org.apache.ignite.internal.catalog.commands.CreateTableCommandBuilder;
import org.apache.ignite.internal.catalog.commands.DropTableCommand;
import org.apache.ignite.internal.catalog.commands.DropTableCommandBuilder;
import org.apache.ignite.internal.catalog.commands.TableHashPrimaryKey;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link CatalogManagerCompactionHelper}.
 */
class CatalogManagerCompactionHelperTest extends AbstractCatalogCompactionTest {
    private CatalogManagerCompactionHelper catalogManagerHelper;

    @BeforeEach
    void setupHelper() {
        catalogManagerHelper = new CatalogManagerCompactionHelper(catalogManager);;
    }

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
            Int2IntMap tablesWithParts = catalogManagerHelper.collectTablesWithPartitionsBetween(from1,
                    clockService.nowLong());

            assertThat(tablesWithParts.keySet(), hasSize(3));
        }

        {
            Int2IntMap tablesWithParts = catalogManagerHelper.collectTablesWithPartitionsBetween(from2,
                    clockService.nowLong());

            assertThat(tablesWithParts.keySet(), hasSize(2));
        }

        {
            Int2IntMap tablesWithParts = catalogManagerHelper.collectTablesWithPartitionsBetween(from3,
                    clockService.nowLong());

            assertThat(tablesWithParts.keySet(), hasSize(1));
        }

        {
            Int2IntMap tablesWithParts = catalogManagerHelper.collectTablesWithPartitionsBetween(
                    clockService.nowLong(),
                    clockService.nowLong()
            );

            assertThat(tablesWithParts.keySet(), hasSize(0));
        }
    }

    @Test
    void testCatalogByTsNullable() {
        Catalog earliestCatalog = catalogManager.catalog(catalogManager.earliestCatalogVersion());
        assertNotNull(earliestCatalog);

        assertNull(catalogManagerHelper.catalogByTsNullable(earliestCatalog.time() - 1));
    }
}
