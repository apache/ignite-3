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

package org.apache.ignite.internal.table;

import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.util.List;
import org.apache.ignite.internal.catalog.CatalogManager;
import org.apache.ignite.internal.catalog.CatalogService;
import org.apache.ignite.internal.catalog.commands.ColumnParams;
import org.apache.ignite.internal.catalog.commands.CreateTableParams;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableDescriptor;
import org.jetbrains.annotations.Nullable;

/**
 * Utils to manage tables inside tests.
 */
public class TableTestUtils {
    /**
     * Creates table in the catalog.
     *
     * @param catalogManager Catalog manager.
     * @param schemaName Schema name.
     * @param zoneName Zone name.
     * @param tableName Table name.
     * @param columns Table columns.
     * @param pkColumns Primary key columns.
     */
    public static void createTable(
            CatalogManager catalogManager,
            String schemaName,
            String zoneName,
            String tableName,
            List<ColumnParams> columns,
            List<String> pkColumns
    ) {
        CreateTableParams.Builder builder = CreateTableParams.builder()
                .schemaName(schemaName)
                .zone(zoneName)
                .tableName(tableName)
                .columns(columns)
                .primaryKeyColumns(pkColumns);

        assertThat(catalogManager.createTable(builder.build()), willCompleteSuccessfully());
    }

    /**
     * Returns table ID form catalog, {@code null} if table is absent.
     *
     * @param catalogService Catalog service.
     * @param tableName Table name.
     * @param timestamp Timestamp.
     */
    public static @Nullable Integer getTableId(CatalogService catalogService, String tableName, long timestamp) {
        CatalogTableDescriptor table = catalogService.table(tableName, timestamp);

        return table == null ? null : table.id();
    }

    /**
     * Returns table ID from catalog.
     *
     * @param catalogService Catalog service.
     * @param tableName Table name.
     * @param timestamp Timestamp.
     * @throws AssertionError If table is absent.
     */
    public static int getTableIdStrict(CatalogService catalogService, String tableName, long timestamp) {
        Integer tableId = getTableId(catalogService, tableName, timestamp);

        assertNotNull(tableId, "tableName=" + tableName + ", timestamp=" + timestamp);

        return tableId;
    }
}
