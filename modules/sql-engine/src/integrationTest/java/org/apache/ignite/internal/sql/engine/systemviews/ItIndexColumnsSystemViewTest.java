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
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.util.Arrays;
import java.util.List;
import org.apache.ignite.internal.catalog.Catalog;
import org.apache.ignite.internal.catalog.commands.CatalogUtils;
import org.apache.ignite.internal.catalog.descriptors.CatalogIndexDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogSchemaDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableDescriptor;
import org.apache.ignite.internal.sql.engine.util.MetadataMatcher;
import org.apache.ignite.internal.sql.engine.util.QueryChecker;
import org.apache.ignite.sql.ColumnType;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * End-to-end tests to verify {@code INDEX_COLUMNS} system view.
 */
public class ItIndexColumnsSystemViewTest extends AbstractSystemViewTest {

    @BeforeAll
    void beforeAll() {
        sql("CREATE SCHEMA test_schema1");
        sql("CREATE TABLE test_schema1.test_table1(id INT PRIMARY KEY, col1 VARCHAR, col2 VARCHAR, col3 VARCHAR, col4 VARCHAR)");

        sql("CREATE SCHEMA test_schema2");
        sql("CREATE TABLE test_schema2.test_table2(id INT PRIMARY KEY, col_a VARCHAR, col_b VARCHAR, col_c VARCHAR)");
    }

    @Test
    public void metadata() {
        assertQuery("SELECT * FROM system.index_columns")
                .columnMetadata(
                        new MetadataMatcher()
                                .name("SCHEMA_ID")
                                .type(ColumnType.INT32)
                                .nullable(true),
                        new MetadataMatcher()
                                .name("SCHEMA_NAME")
                                .type(ColumnType.STRING)
                                .precision(CatalogUtils.DEFAULT_VARLEN_LENGTH)
                                .nullable(true),
                        new MetadataMatcher()
                                .name("TABLE_ID")
                                .type(ColumnType.INT32)
                                .nullable(true),
                        new MetadataMatcher()
                                .name("TABLE_NAME")
                                .type(ColumnType.STRING)
                                .precision(CatalogUtils.DEFAULT_VARLEN_LENGTH)
                                .nullable(true),
                        new MetadataMatcher()
                                .name("INDEX_ID")
                                .type(ColumnType.INT32)
                                .nullable(true),
                        new MetadataMatcher()
                                .name("INDEX_NAME")
                                .type(ColumnType.STRING)
                                .precision(CatalogUtils.DEFAULT_VARLEN_LENGTH)
                                .nullable(true),
                        new MetadataMatcher()
                                .name("COLUMN_NAME")
                                .type(ColumnType.STRING)
                                .precision(CatalogUtils.DEFAULT_VARLEN_LENGTH)
                                .nullable(true),
                        new MetadataMatcher()
                                .name("COLUMN_ORDINAL")
                                .type(ColumnType.INT32)
                                .nullable(true),
                        new MetadataMatcher()
                                .name("COLUMN_COLLATION")
                                .type(ColumnType.STRING)
                                .precision(CatalogUtils.DEFAULT_VARLEN_LENGTH)
                                .nullable(true)
                )
                .check();
    }

    @Test
    public void hashIndex() {
        sql("CREATE INDEX test_table1_hash_idx ON test_schema1.test_table1 USING HASH (col2, col1, col3)");
        checkHashIndex("TEST_SCHEMA1", "TEST_TABLE1_HASH_IDX", List.of("COL2", "COL1", "COL3"));

        sql("CREATE INDEX test_table2_hash_idx ON test_schema2.test_table2 USING HASH (col_c, col_a)");
        checkHashIndex("TEST_SCHEMA2", "TEST_TABLE2_HASH_IDX", List.of("COL_C", "COL_A"));
    }

    private static void checkHashIndex(String schemaName, String indexName, List<String> columns) {
        Catalog catalog = unwrapIgniteImpl(CLUSTER.aliveNode()).catalogManager().latestCatalog();

        CatalogSchemaDescriptor schema = catalog.schema(schemaName);
        assertNotNull(schema);

        CatalogIndexDescriptor index = Arrays.stream(schema.indexes())
                .filter(idx -> indexName.equals(idx.name()))
                .findFirst()
                .orElseThrow();

        CatalogTableDescriptor table = catalog.table(index.tableId());
        assertNotNull(table);

        QueryChecker queryChecker = assertQuery(
                "SELECT * FROM system.index_columns WHERE index_name=? ORDER BY table_name ASC, column_ordinal ASC")
                .withParams(indexName);

        for (int i = 0; i < columns.size(); i++) {
            String column = columns.get(i);

            queryChecker = queryChecker.returns(schema.id(), schema.name(), table.id(), table.name(), index.id(), index.name(),
                    column, i, null);
        }

        queryChecker.check();
    }

    @Test
    public void sortedIndex() {
        sql("CREATE INDEX test_table1_sorted_idx ON test_schema1.test_table1 USING SORTED (col3, col1 ASC, col2 DESC)");
        checkSortedIndex(
                "TEST_SCHEMA1", "TEST_TABLE1_SORTED_IDX",
                List.of("COL3", "COL1", "COL2"),
                List.of("ASC_NULLS_LAST", "ASC_NULLS_LAST", "DESC_NULLS_FIRST")
        );

        sql("CREATE INDEX test_table2_sorted_idx ON test_schema2.test_table2 USING SORTED (col_c DESC, col_b ASC)");
        checkSortedIndex(
                "TEST_SCHEMA2", "TEST_TABLE2_SORTED_IDX",
                List.of("COL_C", "COL_B"),
                List.of("DESC_NULLS_FIRST", "ASC_NULLS_LAST")
        );
    }

    private static void checkSortedIndex(String schemaName, String indexName,
            List<String> columns,
            List<String> collations
    ) {
        Catalog catalog = unwrapIgniteImpl(CLUSTER.aliveNode()).catalogManager().latestCatalog();

        CatalogSchemaDescriptor schema = catalog.schema(schemaName);
        assertNotNull(schema);

        CatalogIndexDescriptor index = Arrays.stream(schema.indexes())
                .filter(idx -> indexName.equals(idx.name()))
                .findFirst()
                .orElseThrow();

        CatalogTableDescriptor table = catalog.table(index.tableId());
        assertNotNull(table);

        QueryChecker queryChecker = assertQuery(
                "SELECT * FROM system.index_columns WHERE index_name=? ORDER BY table_name ASC, column_ordinal ASC")
                .withParams(indexName);

        for (int i = 0; i < columns.size(); i++) {
            String column = columns.get(i);
            String collation = collations.get(i);

            queryChecker = queryChecker.returns(schema.id(), schema.name(), table.id(), table.name(), index.id(), index.name(),
                    column, i, collation);
        }

        queryChecker.check();
    }
}
