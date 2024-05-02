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

import static org.apache.ignite.internal.TestWrappers.unwrapTableViewInternal;
import static org.apache.ignite.internal.catalog.CatalogService.DEFAULT_STORAGE_PROFILE;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.SYSTEM_SCHEMAS;
import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;
import static org.apache.ignite.internal.sql.engine.util.SqlTestUtils.assertThrowsSqlException;
import static org.apache.ignite.internal.table.TableTestUtils.getTableStrict;
import static org.apache.ignite.lang.ErrorGroups.Sql.STMT_VALIDATION_ERR;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Stream;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.catalog.Catalog;
import org.apache.ignite.internal.catalog.CatalogManager;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogZoneDescriptor;
import org.apache.ignite.internal.lang.IgniteStringBuilder;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.SchemaTestUtils;
import org.apache.ignite.internal.sql.BaseSqlIntegrationTest;
import org.apache.ignite.internal.sql.engine.util.Commons;
import org.apache.ignite.internal.sql.engine.util.TypeUtils;
import org.apache.ignite.internal.type.NativeType;
import org.apache.ignite.internal.type.NativeTypeSpec;
import org.apache.ignite.lang.ErrorGroups.Sql;
import org.apache.ignite.sql.SqlException;
import org.apache.ignite.tx.Transaction;
import org.apache.ignite.tx.TransactionOptions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Integration tests for DDL statements that affect tables.
 */
public class ItCreateTableDdlTest extends BaseSqlIntegrationTest {
    @AfterEach
    public void dropTables() {
        dropAllTables();
        dropAllZonesExceptDefaultOne();
    }

    @Test
    public void pkWithNullableColumns() {
        assertThrowsSqlException(
                STMT_VALIDATION_ERR,
                "Primary key cannot contain nullable column [col=ID0].",
                () -> sql("CREATE TABLE T0(ID0 INT NULL, ID1 INT NOT NULL, VAL INT, PRIMARY KEY (ID1, ID0))")
        );

        assertThrowsSqlException(
                STMT_VALIDATION_ERR,
                "Primary key cannot contain nullable column [col=ID].",
                () -> sql("CREATE TABLE T0(ID INT NULL PRIMARY KEY, VAL INT)")
        );
    }

    @Test
    public void pkWithDuplicatesColumn() {
        assertThrowsSqlException(
                STMT_VALIDATION_ERR,
                "PK column 'ID1' specified more that once.",
                () -> sql("CREATE TABLE T0(ID0 INT, ID1 INT, VAL INT, PRIMARY KEY (ID1, ID0, ID1))")
        );
    }

    @Test
    public void pkWithInvalidColumns() {
        assertThrowsSqlException(
                STMT_VALIDATION_ERR,
                "Primary key constraint contains undefined columns: [cols=[ID2]].",
                () -> sql("CREATE TABLE T0(ID0 INT, ID1 INT, VAL INT, PRIMARY KEY (ID2, ID0))")
        );
    }

    @Test
    public void emptyPk() {
        assertThrowsSqlException(
                Sql.STMT_PARSE_ERR,
                "Failed to parse query: Encountered \")\"",
                () -> sql("CREATE TABLE T0(ID0 INT, ID1 INT, VAL INT, PRIMARY KEY ())")
        );

        assertThrowsSqlException(
                STMT_VALIDATION_ERR,
                "Table without PRIMARY KEY is not supported",
                () -> sql("CREATE TABLE T0(ID0 INT, ID1 INT, VAL INT)")
        );
    }

    @Test
    public void tableWithInvalidColumns() {
        assertThrowsSqlException(
                Sql.STMT_PARSE_ERR,
                "Failed to parse query: Encountered \")\"",
                () -> sql("CREATE TABLE T0()")
        );

        assertThrowsSqlException(
                STMT_VALIDATION_ERR,
                "Column with name 'ID0' specified more than once",
                () -> sql("CREATE TABLE T0(ID0 INT PRIMARY KEY, ID1 INT, ID0 INT)")
        );
    }

    @Test
    public void pkWithFunctionalDefault() {
        sql("create table t (id varchar default gen_random_uuid primary key, val int)");
        sql("insert into t (val) values (1), (2)");

        var result = sql("select * from t");

        assertThat(result, hasSize(2)); // both rows are inserted without conflict
    }

    @Test
    public void undefinedColumnsInPrimaryKey() {
        assertThrowsSqlException(
                STMT_VALIDATION_ERR,
                "Primary key constraint contains undefined columns: [cols=[ID1, ID0, ID2]].",
                () -> sql("CREATE TABLE T0(ID INT, VAL INT, PRIMARY KEY (ID1, ID0, ID2))")
        );
    }

    /**
     * Check invalid colocation columns configuration: - not PK columns; - duplicates colocation columns.
     */
    @Test
    public void invalidColocationColumns() {
        assertThrowsSqlException(
                STMT_VALIDATION_ERR,
                "Failed to validate query. Colocation column 'VAL' is not part of PK",
                () -> sql("CREATE TABLE T0(ID0 INT, ID1 INT, VAL INT, PRIMARY KEY (ID1, ID0)) COLOCATE (ID0, VAL)")
        );

        assertThrowsSqlException(
                STMT_VALIDATION_ERR,
                "Failed to validate query. Colocation column 'ID1' specified more that once",
                () -> sql("CREATE TABLE T0(ID0 INT, ID1 INT, VAL INT, PRIMARY KEY (ID1, ID0)) COLOCATE (ID1, ID0, ID1)")
        );
    }

    /**
     * Check implicit colocation columns configuration (defined by PK)..
     */
    @Test
    public void implicitColocationColumns() {
        sql("CREATE TABLE T0(ID0 INT, ID1 INT, VAL INT, PRIMARY KEY (ID1, ID0))");

        List<Column> colocationColumns = unwrapTableViewInternal(table("T0")).schemaView().lastKnownSchema().colocationColumns();

        assertEquals(2, colocationColumns.size());
        assertEquals("ID1", colocationColumns.get(0).name());
        assertEquals("ID0", colocationColumns.get(1).name());
    }

    /** Test correct mapping schema after alter columns. */
    @Test
    public void testDropAndAddColumns() {
        sql("CREATE TABLE my (c1 INT PRIMARY KEY, c2 INT, c3 VARCHAR)");

        sql("INSERT INTO my VALUES (1, 2, '3')");

        List<List<Object>> res = sql("SELECT c1, c3 FROM my");

        assertFalse(res.isEmpty());

        sql("ALTER TABLE my DROP COLUMN c2");

        res = sql("SELECT c1, c3 FROM my");

        assertFalse(res.isEmpty());

        sql("ALTER TABLE my ADD COLUMN (c2 INT, c4 VARCHAR)");

        sql("INSERT INTO my VALUES (2, '2', 2, '3')");

        res = sql("SELECT c2, c4 FROM my WHERE c1=2");

        assertEquals(2, res.get(0).get(0));

        sql("ALTER TABLE my DROP COLUMN c4");
        sql("ALTER TABLE my ADD COLUMN (c4 INT)");
        sql("INSERT INTO my VALUES (3, '2', 3, 3)");

        res = sql("SELECT c4 FROM my WHERE c1=3");

        assertEquals(3, res.get(0).get(0));

        // Checking the correctness of reading a row created on a different version of the schema.
        sql("ALTER TABLE my ADD COLUMN (c5 VARCHAR, c6 BOOLEAN)");
        sql("ALTER TABLE my DROP COLUMN c4");
        assertQuery("SELECT * FROM my WHERE c1=3")
                .returns(3, "2", 3, null, null)
                .check();
    }

    /** Test that adding nullable column via ALTER TABLE ADD name type NULL works. */
    @Test
    public void testNullableColumn() {
        sql("CREATE TABLE my (c1 INT PRIMARY KEY, c2 INT)");
        sql("INSERT INTO my VALUES (1, 1)");
        sql("ALTER TABLE my ADD COLUMN c3 INT NULL");
        sql("INSERT INTO my VALUES (2, 2, NULL)");

        assertQuery("SELECT * FROM my ORDER by c1 ASC")
                .returns(1, 1, null)
                .returns(2, 2, null)
                .check();
    }

    /**
     * Adds columns of all supported types and checks that the row
     * created on the old schema version is read correctly.
     */
    @Test
    public void testDropAndAddColumnsAllTypes() {
        List<NativeType> allTypes = SchemaTestUtils.ALL_TYPES;

        Set<NativeTypeSpec> unsupportedTypes = Set.of(
                // TODO https://issues.apache.org/jira/browse/IGNITE-18431
                NativeTypeSpec.BITMASK
        );

        // List of columns for 'ADD COLUMN' statement.
        IgniteStringBuilder addColumnsList = new IgniteStringBuilder();
        // List of columns for 'DROP COLUMN' statement.
        IgniteStringBuilder dropColumnsList = new IgniteStringBuilder();

        for (int i = 0; i < allTypes.size(); i++) {
            NativeType type = allTypes.get(i);

            if (unsupportedTypes.contains(type.spec())) {
                continue;
            }

            RelDataType relDataType = TypeUtils.native2relationalType(Commons.typeFactory(), type);

            if (addColumnsList.length() > 0) {
                addColumnsList.app(',');
                dropColumnsList.app(',');
            }

            addColumnsList.app("c").app(i).app(' ').app(relDataType.getSqlTypeName().getSpaceName());
            dropColumnsList.app("c").app(i);
        }

        sql("CREATE TABLE test (id INT PRIMARY KEY, val INT)");
        sql("INSERT INTO test VALUES (0, 1)");
        sql(format("ALTER TABLE test ADD COLUMN ({})", addColumnsList.toString()));

        List<List<Object>> res = sql("SELECT * FROM test");
        assertThat(res.size(), is(1));
        assertThat(res.get(0).size(), is(allTypes.size() - unsupportedTypes.size() + /* initial columns */ 2));

        sql(format("ALTER TABLE test DROP COLUMN ({})", dropColumnsList.toString()));
        assertQuery("SELECT * FROM test")
                .returns(0, 1)
                .check();
    }

    /**
     * Check explicit colocation columns configuration.
     */
    @Test
    public void explicitColocationColumns() {
        sql("CREATE TABLE T0(ID0 INT, ID1 INT, VAL INT, PRIMARY KEY (ID1, ID0)) COLOCATE BY (id0)");

        List<Column> colocationColumns = unwrapTableViewInternal(table("T0")).schemaView().lastKnownSchema().colocationColumns();

        assertEquals(1, colocationColumns.size());
        assertEquals("ID0", colocationColumns.get(0).name());
    }

    /**
     * Check explicit colocation columns configuration.
     */
    @Test
    public void explicitColocationColumnsCaseSensitive() {
        sql("CREATE TABLE T0(\"Id0\" INT, ID1 INT, VAL INT, PRIMARY KEY (ID1, \"Id0\")) COLOCATE BY (\"Id0\")");

        List<Column> colocationColumns = unwrapTableViewInternal(table("T0")).schemaView().lastKnownSchema().colocationColumns();

        assertEquals(1, colocationColumns.size());
        assertEquals("Id0", colocationColumns.get(0).name());
    }

    @Test
    public void doNotAllowFunctionsInNonPkColumns() {
        assertThrowsSqlException(
                STMT_VALIDATION_ERR,
                "Functional defaults are not supported for non-primary key columns",
                () -> sql("create table t (id varchar primary key, val varchar default gen_random_uuid)")
        );
    }

    @ParameterizedTest
    @MethodSource("reservedSchemaNames")
    public void testItIsNotPossibleToCreateTablesInSystemSchema(String schema) {
        assertThrowsSqlException(
                STMT_VALIDATION_ERR,
                "Operations with reserved schemas are not allowed",
                () -> sql(format("CREATE TABLE {}.SYS_TABLE (NAME VARCHAR PRIMARY KEY, SIZE BIGINT)", schema.toLowerCase())));
    }

    private static Stream<Arguments> reservedSchemaNames() {
        return SYSTEM_SCHEMAS.stream().map(Arguments::of);
    }

    @Test
    public void concurrentDrop() {
        sql("CREATE TABLE test (key INT PRIMARY KEY)");

        IgniteImpl node = CLUSTER.node(0);
        Transaction tx = node.transactions().begin(new TransactionOptions().readOnly(true));

        sql("DROP TABLE test");

        sql(tx, "SELECT COUNT(*) FROM test");
    }

    @Test
    public void testPrimaryKeyIndexTypes() {
        sql("CREATE TABLE test1 (id1 INT, id2 INT, val INT, PRIMARY KEY (id2, id1))");
        sql("CREATE TABLE test2 (id1 INT, id2 INT, val INT, PRIMARY KEY USING SORTED (id1 DESC, id2 ASC))");
        sql("CREATE TABLE test3 (id1 INT, id2 INT, val INT, PRIMARY KEY USING HASH (id2, id1))");

        assertQuery("SELECT index_name, type, COLUMNS FROM SYSTEM.INDEXES ORDER BY INDEX_ID")
                .returns("TEST1_PK", "HASH", "ID2, ID1")
                .returns("TEST2_PK", "SORTED", "ID1 DESC, ID2 ASC")
                .returns("TEST3_PK", "HASH", "ID2, ID1")
                .check();
    }

    @Test
    public void testSuccessfulCreateTableWithZoneIdentifier() {
        sql("CREATE ZONE test_zone WITH STORAGE_PROFILES='" + DEFAULT_STORAGE_PROFILE + "'");
        sql("CREATE TABLE test_table (id INT PRIMARY KEY, val INT) WITH PRIMARY_ZONE=test_zone");
    }

    @Test
    public void testSuccessfulCreateTableWithZoneLiteral() {
        sql("CREATE ZONE test_zone WITH STORAGE_PROFILES='" + DEFAULT_STORAGE_PROFILE + "'");
        sql("CREATE TABLE test_table (id INT PRIMARY KEY, val INT) WITH PRIMARY_ZONE='TEST_ZONE'");
    }

    @Test
    public void testSuccessfulCreateTableWithZoneQuotedLiteral() {
        sql("CREATE ZONE \"test_zone\" WITH STORAGE_PROFILES='" + DEFAULT_STORAGE_PROFILE + "'");
        sql("CREATE TABLE test_table (id INT PRIMARY KEY, val INT) WITH PRIMARY_ZONE='test_zone'");
        sql("DROP TABLE test_table");
        sql("DROP ZONE \"test_zone\"");
    }

    @Test
    public void testExceptionalCreateTableWithZoneUnquotedLiteral() {

        sql("CREATE ZONE test_zone WITH STORAGE_PROFILES='" + DEFAULT_STORAGE_PROFILE + "'");
        assertThrowsSqlException(
                SqlException.class,
                STMT_VALIDATION_ERR,
                "Failed to validate query. Distribution zone with name 'test_zone' not found",
                () -> sql("CREATE TABLE test_table (id INT PRIMARY KEY, val INT) WITH PRIMARY_ZONE='test_zone'"));
    }

    @Test
    public void tableStorageProfileWithoutSettingItExplicitly() {
        sql("CREATE TABLE TEST(ID INT PRIMARY KEY, VAL0 INT)");

        IgniteImpl node = CLUSTER.aliveNode();

        CatalogTableDescriptor table = node.catalogManager().table("TEST", node.clock().nowLong());

        CatalogZoneDescriptor zone = getDefaultZone(node);

        assertEquals(zone.storageProfiles().defaultProfile().storageProfile(), table.storageProfile());
    }

    @Test
    public void tableStorageProfileExceptionIfZoneDoesntContainProfile() {
        String defaultZoneName = getDefaultZone(CLUSTER.aliveNode()).name();

        assertThrowsSqlException(
                STMT_VALIDATION_ERR,
                "Zone with name '" + defaultZoneName + "' does not contain table's storage profile",
                () -> sql("CREATE TABLE TEST(ID INT PRIMARY KEY, VAL0 INT) WITH STORAGE_PROFILE='profile1'")
        );
    }

    @Test
    public void tableStorageProfile() {
        sql("CREATE TABLE TEST(ID INT PRIMARY KEY, VAL0 INT) WITH STORAGE_PROFILE='" + DEFAULT_STORAGE_PROFILE + "'");

        IgniteImpl node = CLUSTER.aliveNode();

        CatalogTableDescriptor table = node.catalogManager().table("TEST", node.clock().nowLong());

        assertEquals(DEFAULT_STORAGE_PROFILE, table.storageProfile());

        CatalogZoneDescriptor defaultZone = getDefaultZone(node);

        assertEquals(defaultZone.storageProfiles().defaultProfile().storageProfile(), table.storageProfile());
    }

    @Test
    public void tableStorageProfileWithCustomZoneDefaultProfile() {
        sql("CREATE ZONE ZONE1 WITH PARTITIONS = 1, STORAGE_PROFILES = '" + DEFAULT_STORAGE_PROFILE + "'");

        sql("CREATE TABLE TEST(ID INT PRIMARY KEY, VAL0 INT) WITH PRIMARY_ZONE='ZONE1'");

        IgniteImpl node = CLUSTER.aliveNode();

        CatalogTableDescriptor table = node.catalogManager().table("TEST", node.clock().nowLong());

        assertEquals(DEFAULT_STORAGE_PROFILE, table.storageProfile());

        sql("DROP TABLE TEST");

        sql("DROP ZONE ZONE1");
    }

    @Test
    public void tableStorageProfileWithCustomZoneExplicitProfile() {
        sql("CREATE ZONE ZONE1 WITH PARTITIONS = 1, STORAGE_PROFILES = '" + DEFAULT_STORAGE_PROFILE + "'");

        sql("CREATE TABLE TEST(ID INT PRIMARY KEY, VAL0 INT) WITH PRIMARY_ZONE='ZONE1', STORAGE_PROFILE='" + DEFAULT_STORAGE_PROFILE + "'");

        IgniteImpl node = CLUSTER.aliveNode();

        CatalogTableDescriptor table = node.catalogManager().table("TEST", node.clock().nowLong());

        assertEquals(DEFAULT_STORAGE_PROFILE, table.storageProfile());

        sql("DROP TABLE TEST");

        sql("DROP ZONE ZONE1");
    }

    private static CatalogZoneDescriptor getDefaultZone(IgniteImpl node) {
        CatalogManager catalogManager = node.catalogManager();
        Catalog catalog = catalogManager.catalog(catalogManager.activeCatalogVersion(node.clock().nowLong()));

        assert catalog != null;

        return Objects.requireNonNull(catalog.defaultZone());
    }
}
