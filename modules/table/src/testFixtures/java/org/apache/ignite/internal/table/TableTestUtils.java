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
import org.apache.ignite.internal.catalog.CatalogCommand;
import org.apache.ignite.internal.catalog.CatalogManager;
import org.apache.ignite.internal.catalog.CatalogService;
import org.apache.ignite.internal.catalog.commands.ColumnParams;
import org.apache.ignite.internal.catalog.commands.CreateHashIndexCommand;
import org.apache.ignite.internal.catalog.commands.CreateTableCommand;
import org.apache.ignite.internal.catalog.commands.DropIndexCommand;
import org.apache.ignite.internal.catalog.commands.DropTableCommand;
import org.apache.ignite.internal.catalog.descriptors.CatalogIndexDescriptor;
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
        CatalogCommand command = CreateTableCommand.builder()
                .schemaName(schemaName)
                .zone(zoneName)
                .tableName(tableName)
                .columns(columns)
                .primaryKeyColumns(pkColumns)
                .build();

        assertThat(catalogManager.execute(command), willCompleteSuccessfully());
    }

    /**
     * Drops table in the catalog.
     *
     * @param catalogManager Catalog manager.
     * @param schemaName Schema name.
     * @param tableName Table name.
     */
    public static void dropTable(CatalogManager catalogManager, String schemaName, String tableName) {
        assertThat(
                catalogManager.execute(DropTableCommand.builder().schemaName(schemaName).tableName(tableName).build()),
                willCompleteSuccessfully()
        );
    }

    /**
     * Drops index in the catalog.
     *
     * @param catalogManager Catalog manager.
     * @param schemaName Schema name.
     * @param indexName Index name.
     */
    public static void dropIndex(CatalogManager catalogManager, String schemaName, String indexName) {
        assertThat(
                catalogManager.execute(DropIndexCommand.builder().schemaName(schemaName).indexName(indexName).build()),
                willCompleteSuccessfully()
        );
    }

    /**
     * Creates hash index in the catalog.
     *
     * @param catalogManager Catalog manager.
     * @param schemaName Schema name.
     * @param tableName Table name.
     * @param indexName Index name.
     * @param columns Index columns.
     * @param unique Unique constraint flag.
     */
    public static void createHashIndex(
            CatalogManager catalogManager,
            String schemaName,
            String tableName,
            String indexName,
            List<String> columns,
            boolean unique
    ) {
        CatalogCommand command = CreateHashIndexCommand.builder()
                .schemaName(schemaName)
                .tableName(tableName)
                .indexName(indexName)
                .columns(columns)
                .unique(unique)
                .build();

        assertThat(catalogManager.execute(command), willCompleteSuccessfully());
    }

    /**
     * Returns table descriptor form catalog, {@code null} if table is absent.
     *
     * @param catalogService Catalog service.
     * @param tableName Table name.
     * @param timestamp Timestamp.
     */
    public static @Nullable CatalogTableDescriptor getTable(CatalogService catalogService, String tableName, long timestamp) {
        return catalogService.table(tableName, timestamp);
    }

    /**
     * Returns table descriptor form catalog.
     *
     * @param catalogService Catalog service.
     * @param tableName Table name.
     * @param timestamp Timestamp.
     * @throws AssertionError If table descriptor is absent.
     */
    public static CatalogTableDescriptor getTableStrict(CatalogService catalogService, String tableName, long timestamp) {
        CatalogTableDescriptor table = catalogService.table(tableName, timestamp);

        assertNotNull(table, "tableName=" + tableName + ", timestamp=" + timestamp);

        return table;
    }

    /**
     * Returns table ID form catalog, {@code null} if table is absent.
     *
     * @param catalogService Catalog service.
     * @param tableName Table name.
     * @param timestamp Timestamp.
     */
    public static @Nullable Integer getTableId(CatalogService catalogService, String tableName, long timestamp) {
        CatalogTableDescriptor table = getTable(catalogService, tableName, timestamp);

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
        return getTableStrict(catalogService, tableName, timestamp).id();
    }

    /**
     * Returns index ID form catalog, {@code null} if table is absent.
     *
     * @param catalogService Catalog service.
     * @param indexName Index name.
     * @param timestamp Timestamp.
     */
    public static @Nullable Integer getIndexId(CatalogService catalogService, String indexName, long timestamp) {
        CatalogIndexDescriptor index = getIndex(catalogService, indexName, timestamp);

        return index == null ? null : index.id();
    }

    /**
     * Returns index ID from catalog.
     *
     * @param catalogService Catalog service.
     * @param indexName Index name.
     * @param timestamp Timestamp.
     * @throws AssertionError If table is absent.
     */
    public static int getIndexIdStrict(CatalogService catalogService, String indexName, long timestamp) {
        return getIndexStrict(catalogService, indexName, timestamp).id();
    }

    /**
     * Returns index descriptor form catalog, {@code null} if table is absent.
     *
     * @param catalogService Catalog service.
     * @param indexName Index name.
     * @param timestamp Timestamp.
     */
    public static @Nullable CatalogIndexDescriptor getIndex(CatalogService catalogService, String indexName, long timestamp) {
        return catalogService.index(indexName, timestamp);
    }

    /**
     * Returns index descriptor form catalog.
     *
     * @param catalogService Catalog service.
     * @param indexName Index name.
     * @param timestamp Timestamp.
     * @throws AssertionError If table descriptor is absent.
     */
    public static CatalogIndexDescriptor getIndexStrict(CatalogService catalogService, String indexName, long timestamp) {
        CatalogIndexDescriptor index = catalogService.index(indexName, timestamp);

        assertNotNull(index, "indexName=" + indexName + ", timestamp=" + timestamp);

        return index;
    }
}
