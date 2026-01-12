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

package org.apache.ignite.internal.sql.engine.systemviews;

import static org.apache.ignite.internal.TestWrappers.unwrapIgniteImpl;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.DEFAULT_VARLEN_LENGTH;
import static org.apache.ignite.internal.catalog.descriptors.CatalogIndexDescriptor.CatalogIndexDescriptorType.HASH;

import java.util.Collection;
import org.apache.ignite.internal.catalog.Catalog;
import org.apache.ignite.internal.catalog.descriptors.CatalogIndexDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableDescriptor;
import org.apache.ignite.internal.sql.SqlCommon;
import org.apache.ignite.internal.sql.engine.util.MetadataMatcher;
import org.apache.ignite.sql.ColumnType;
import org.junit.jupiter.api.Test;

/** End-to-end tests to verify indexes system view. */
public class ItIndexesSystemViewTest extends AbstractSystemViewTest {
    private static final String TABLE_NAME = "TEST_TABLE";

    private static final String COLUMNS = "ID, NAME";

    private static final String COLUMNS_COLLATIONS = "ID DESC, NAME ASC";

    @Test
    public void testMetadata() {
        assertQuery("SELECT * FROM SYSTEM.INDEXES")
                .columnMetadata(
                        new MetadataMatcher().name("INDEX_ID").type(ColumnType.INT32).nullable(true),
                        new MetadataMatcher().name("INDEX_NAME").type(ColumnType.STRING).precision(DEFAULT_VARLEN_LENGTH).nullable(true),
                        new MetadataMatcher().name("TABLE_ID").type(ColumnType.INT32).nullable(true),
                        new MetadataMatcher().name("TABLE_NAME").precision(DEFAULT_VARLEN_LENGTH).nullable(true),
                        new MetadataMatcher().name("SCHEMA_ID").type(ColumnType.INT32).nullable(true),
                        new MetadataMatcher().name("SCHEMA_NAME").type(ColumnType.STRING).precision(DEFAULT_VARLEN_LENGTH).nullable(true),
                        new MetadataMatcher().name("INDEX_TYPE").type(ColumnType.STRING).precision(DEFAULT_VARLEN_LENGTH).nullable(true),
                        new MetadataMatcher().name("IS_UNIQUE_INDEX").type(ColumnType.BOOLEAN).nullable(true),
                        new MetadataMatcher().name("INDEX_COLUMNS").type(ColumnType.STRING).precision(DEFAULT_VARLEN_LENGTH).nullable(true),
                        new MetadataMatcher().name("INDEX_STATE").type(ColumnType.STRING).precision(DEFAULT_VARLEN_LENGTH).nullable(true),

                        // Legacy columns
                        new MetadataMatcher().name("TYPE").type(ColumnType.STRING).precision(DEFAULT_VARLEN_LENGTH).nullable(true),
                        new MetadataMatcher().name("IS_UNIQUE").type(ColumnType.BOOLEAN).nullable(true),
                        new MetadataMatcher().name("COLUMNS").type(ColumnType.STRING).precision(DEFAULT_VARLEN_LENGTH).nullable(true),
                        new MetadataMatcher().name("STATUS").type(ColumnType.STRING).precision(DEFAULT_VARLEN_LENGTH).nullable(true)
                )
                .check();
    }

    @Test
    public void multipleIndexes() {
        sql(String.format("CREATE TABLE %S (ID INT, NAME VARCHAR, CONSTRAINT PK PRIMARY KEY USING HASH(ID, NAME));", TABLE_NAME));

        sql(createIndexSql("TEST_INDEX_HASH", TABLE_NAME, HASH.name(), COLUMNS));
        sql(createIndexSql("TEST_INDEX_SORTED", TABLE_NAME, "SORTED", COLUMNS_COLLATIONS));

        Catalog catalog = unwrapIgniteImpl(CLUSTER.aliveNode()).catalogManager().latestCatalog();

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
        String sqlFormat = "SELECT INDEX_ID, INDEX_NAME, INDEX_TYPE, TABLE_ID, TABLE_NAME, SCHEMA_ID, SCHEMA_NAME, IS_UNIQUE_INDEX, "
                + "INDEX_COLUMNS, INDEX_STATE "
                + " FROM SYSTEM.INDEXES WHERE INDEX_NAME = '%s'";

        return String.format(sqlFormat, indexName);
    }
}
