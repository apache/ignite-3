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

package org.apache.ignite.internal.sql.engine;

import static org.apache.ignite.internal.catalog.descriptors.CatalogIndexDescriptor.CatalogIndexDescriptorType.HASH;

import java.util.Collection;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.catalog.Catalog;
import org.apache.ignite.internal.catalog.CatalogManager;
import org.apache.ignite.internal.catalog.descriptors.CatalogIndexDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableDescriptor;
import org.apache.ignite.internal.sql.BaseSqlIntegrationTest;
import org.apache.ignite.internal.sql.SqlCommon;
import org.apache.ignite.internal.testframework.IgniteTestUtils;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

/** End-to-end tests to verify indexes system view. */
public class ItIndexesSystemViewTest extends BaseSqlIntegrationTest {
    private static final String TABLE_NAME = "TEST_TABLE";

    private static final String COLUMNS = "ID, NAME";

    private static final String COLUMNS_COLLATIONS = "ID DESC, NAME ASC";

    @Override
    @BeforeAll
    protected void beforeAll(TestInfo testInfo) {
        super.beforeAll(testInfo);

        IgniteTestUtils.await(systemViewManager().completeRegistration());
    }

    @Test
    public void multipleIndexes() {
        sql(String.format("CREATE TABLE %S (ID INT, NAME VARCHAR, CONSTRAINT PK PRIMARY KEY USING HASH(ID, NAME));", TABLE_NAME));

        sql(createIndexSql("TEST_INDEX_HASH", TABLE_NAME, HASH.name(), COLUMNS));
        sql(createIndexSql("TEST_INDEX_SORTED", TABLE_NAME, "SORTED", COLUMNS_COLLATIONS));

        IgniteImpl ignite = CLUSTER.aliveNode();
        CatalogManager catalogManager = ignite.catalogManager();
        int version = catalogManager.latestCatalogVersion();
        Catalog catalog = catalogManager.catalog(version);

        CatalogTableDescriptor tableDescriptor = catalog.tables().stream()
                .filter(table -> table.name().equals(TABLE_NAME))
                .findAny()
                .orElseThrow();

        Collection<CatalogIndexDescriptor> tableIndexes = catalog.indexes();

        tableIndexes.forEach(index ->
                assertQuery(selectFromIndexesSystemView(index.name())).returns(
                        index.id(),
                        index.name(),
                        index.indexType().name(),
                        index.tableId(),
                        TABLE_NAME,
                        tableDescriptor.schemaId(),
                        SqlCommon.DEFAULT_SCHEMA_NAME,
                        index.unique(),
                        index.indexType() == HASH ? COLUMNS : COLUMNS_COLLATIONS,
                        index.status().name()
                ).check()
        );
    }

    private static String createIndexSql(String name, String tableName, String type, String columnString) {
        return String.format("CREATE INDEX %s ON %s USING %S (%s);", name, tableName, type, columnString);
    }

    private static String selectFromIndexesSystemView(String indexName) {
        String sqlFormat = "SELECT INDEX_ID, INDEX_NAME, TYPE, TABLE_ID, TABLE_NAME, SCHEMA_ID, SCHEMA_NAME, IS_UNIQUE, COLUMNS, STATUS "
                + " FROM SYSTEM.INDEXES WHERE INDEX_NAME = '%s'";

        return String.format(sqlFormat, indexName);
    }
}
