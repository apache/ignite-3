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

package org.apache.ignite.internal.runner.app;

import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.util.List;
import org.apache.ignite.internal.catalog.CatalogCommand;
import org.apache.ignite.internal.catalog.CatalogManager;
import org.apache.ignite.internal.catalog.commands.ColumnParams;
import org.apache.ignite.internal.catalog.commands.CreateTableCommand;

/**
 * Utils to manage tables inside tests.
 */
// TODO: IGNITE-19502 - remove after switching to the Catalog.
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
}
