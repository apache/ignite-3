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

import org.apache.ignite.internal.sql.engine.util.MetadataMatcher;
import org.apache.ignite.sql.ColumnType;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/** End-to-end tests to verify tables system views. */
public class ItTablesSystemViewTest extends AbstractSystemViewTest {
    @BeforeAll
    void beforeAll() {
        sql("CREATE SCHEMA TEST_SCHEMA");
        sql("CREATE TABLE table_name(ID INT PRIMARY KEY, NAME VARCHAR, SALARY DECIMAL(12,2))");
        sql("CREATE TABLE TEST_SCHEMA.TABLE_NAME_2(FIRST_NAME VARCHAR, LAST_NAME VARCHAR, ID INT, "
                + "PRIMARY KEY (FIRST_NAME, LAST_NAME)) COLOCATE BY (LAST_NAME)");
    }

    @Test
    public void tablesViewMetadataTest() {
        assertQuery("SELECT * FROM SYSTEM.TABLES")
                .columnMetadata(
                        new MetadataMatcher().name("SCHEMA_NAME").type(ColumnType.STRING).precision(DEFAULT_VARLEN_LENGTH).nullable(true),
                        new MetadataMatcher().name("TABLE_NAME").precision(DEFAULT_VARLEN_LENGTH).nullable(true),
                        new MetadataMatcher().name("TABLE_ID").type(ColumnType.INT32).nullable(true),
                        new MetadataMatcher().name("TABLE_PK_INDEX_ID").type(ColumnType.INT32).nullable(true),
                        new MetadataMatcher().name("ZONE_NAME").type(ColumnType.STRING).precision(DEFAULT_VARLEN_LENGTH).nullable(true),
                        new MetadataMatcher().name("STORAGE_PROFILE").type(ColumnType.STRING).precision(DEFAULT_VARLEN_LENGTH)
                                .nullable(true),
                        new MetadataMatcher().name("TABLE_COLOCATION_COLUMNS").type(ColumnType.STRING).precision(DEFAULT_VARLEN_LENGTH)
                                .nullable(true),
                        new MetadataMatcher().name("SCHEMA_ID").type(ColumnType.INT32).nullable(true),
                        new MetadataMatcher().name("ZONE_ID").type(ColumnType.INT32).nullable(true),

                        // Legacy column.
                        new MetadataMatcher().name("SCHEMA").type(ColumnType.STRING).precision(DEFAULT_VARLEN_LENGTH).nullable(true),
                        new MetadataMatcher().name("NAME").type(ColumnType.STRING).precision(DEFAULT_VARLEN_LENGTH).nullable(true),
                        new MetadataMatcher().name("ID").type(ColumnType.INT32).nullable(true),
                        new MetadataMatcher().name("PK_INDEX_ID").type(ColumnType.INT32).nullable(true),
                        new MetadataMatcher().name("COLOCATION_KEY_INDEX").type(ColumnType.STRING).precision(DEFAULT_VARLEN_LENGTH),
                        new MetadataMatcher().name("ZONE").type(ColumnType.STRING).precision(DEFAULT_VARLEN_LENGTH).nullable(true)
                )
                .check();
    }

    @Test
    public void tables() {
        unwrapIgniteImpl(CLUSTER.aliveNode()).catalogManager().latestCatalog().tables().forEach(table ->
                assertQuery("SELECT schema, name FROM system.tables order by schema")
                        .returns("PUBLIC", "TABLE_NAME")
                        .returns("TEST_SCHEMA", "TABLE_NAME_2")
                        .check()
        );
    }

    @Test
    public void tableColumnsViewMetadataTest() {
        assertQuery("SELECT * FROM SYSTEM.TABLE_COLUMNS")
                .columnMetadata(
                        new MetadataMatcher().name("SCHEMA_NAME").type(ColumnType.STRING).precision(DEFAULT_VARLEN_LENGTH).nullable(true),
                        new MetadataMatcher().name("TABLE_NAME").type(ColumnType.STRING).precision(DEFAULT_VARLEN_LENGTH).nullable(true),
                        new MetadataMatcher().name("TABLE_ID").type(ColumnType.INT32).nullable(true),
                        new MetadataMatcher().name("COLUMN_NAME").type(ColumnType.STRING).precision(DEFAULT_VARLEN_LENGTH).nullable(true),
                        new MetadataMatcher().name("COLUMN_TYPE").type(ColumnType.STRING).precision(DEFAULT_VARLEN_LENGTH).nullable(true),
                        new MetadataMatcher().name("IS_NULLABLE_COLUMN").type(ColumnType.BOOLEAN).precision(1).nullable(true),
                        new MetadataMatcher().name("COLUMN_PRECISION").type(ColumnType.INT32).nullable(true),
                        new MetadataMatcher().name("COLUMN_SCALE").type(ColumnType.INT32).nullable(true),
                        new MetadataMatcher().name("COLUMN_LENGTH").type(ColumnType.INT32).nullable(true),
                        new MetadataMatcher().name("COLUMN_ORDINAL").type(ColumnType.INT32).nullable(true),
                        new MetadataMatcher().name("SCHEMA_ID").type(ColumnType.INT32).nullable(true),
                        new MetadataMatcher().name("PK_COLUMN_ORDINAL").type(ColumnType.INT32).nullable(true),
                        new MetadataMatcher().name("COLOCATION_COLUMN_ORDINAL").type(ColumnType.INT32).nullable(true),

                        // Legacy columns.
                        new MetadataMatcher().name("SCHEMA").type(ColumnType.STRING).precision(DEFAULT_VARLEN_LENGTH).nullable(true),
                        new MetadataMatcher().name("TYPE").type(ColumnType.STRING).precision(DEFAULT_VARLEN_LENGTH).nullable(true),
                        new MetadataMatcher().name("NULLABLE").type(ColumnType.BOOLEAN).precision(1).nullable(true),
                        new MetadataMatcher().name("PREC").type(ColumnType.INT32).nullable(true),
                        new MetadataMatcher().name("SCALE").type(ColumnType.INT32).nullable(true),
                        new MetadataMatcher().name("LENGTH").type(ColumnType.INT32).nullable(true)
                )
                .check();
    }

    @Test
    public void tableColumns() {
        assertQuery("SELECT schema, table_name, column_name, column_ordinal, type, pk_column_ordinal, colocation_column_ordinal "
                + "FROM system.table_columns "
                + "ORDER BY schema, table_name, column_name")
                .returns("PUBLIC", "TABLE_NAME", "ID", 0, "INT32", 0, 0)
                .returns("PUBLIC", "TABLE_NAME", "NAME", 1, "STRING", null, null)
                .returns("PUBLIC", "TABLE_NAME", "SALARY", 2, "DECIMAL", null, null)
                .returns("TEST_SCHEMA", "TABLE_NAME_2", "FIRST_NAME", 0, "STRING", 0, null)
                .returns("TEST_SCHEMA", "TABLE_NAME_2", "ID", 2, "INT32", null, null)
                .returns("TEST_SCHEMA", "TABLE_NAME_2", "LAST_NAME", 1, "STRING", 1, 0)
                .check();
    }
}
