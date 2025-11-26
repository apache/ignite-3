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

import static org.apache.ignite.internal.TestWrappers.unwrapIgniteImpl;
import static org.apache.ignite.internal.TestWrappers.unwrapTableViewInternal;
import static org.apache.ignite.internal.catalog.CatalogService.DEFAULT_STORAGE_PROFILE;
import static org.apache.ignite.internal.catalog.CatalogService.DEFINITION_SCHEMA;
import static org.apache.ignite.internal.catalog.CatalogService.INFORMATION_SCHEMA;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.SYSTEM_SCHEMAS;
import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;
import static org.apache.ignite.internal.sql.engine.util.SqlTestUtils.assertThrowsSqlException;
import static org.apache.ignite.lang.ErrorGroups.Sql.STMT_PARSE_ERR;
import static org.apache.ignite.lang.ErrorGroups.Sql.STMT_VALIDATION_ERR;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;
import org.apache.ignite.Ignite;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.catalog.Catalog;
import org.apache.ignite.internal.catalog.CatalogManager;
import org.apache.ignite.internal.catalog.commands.CreateSchemaCommand;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogZoneDescriptor;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.sql.BaseSqlIntegrationTest;
import org.apache.ignite.internal.sql.SqlCommon;
import org.apache.ignite.internal.table.partition.HashPartition;
import org.apache.ignite.internal.testframework.IgniteTestUtils;
import org.apache.ignite.internal.testframework.WithSystemProperty;
import org.apache.ignite.sql.SqlException;
import org.apache.ignite.table.Table;
import org.apache.ignite.table.Tuple;
import org.apache.ignite.tx.Transaction;
import org.apache.ignite.tx.TransactionOptions;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Integration tests for DDL statements that affect tables.
 *
 * <p>SQL F031-01 feature. CREATE TABLE statement to create persistent base tables
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
                STMT_PARSE_ERR,
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
                STMT_PARSE_ERR,
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
        {
            sql("create table t1 (id uuid default rand_uuid primary key, val int)");
            sql("insert into t1 (val) values (1), (2)");

            var result = sql("select * from t1");

            assertThat(result, hasSize(2)); // both rows are inserted without conflict
        }

        {
            sql("create table t2 (id uuid default rand_uuid() primary key, val int)");
            sql("insert into t2 (val) values (1), (2)");

            var result = sql("select * from t2");

            assertThat(result, hasSize(2)); // both rows are inserted without conflict
        }
    }

    @Test
    public void pkWithInvalidFunctionalDefault() {
        assertThrowsSqlException(
                STMT_VALIDATION_ERR,
                "Functional default contains unsupported function: [col=ID, functionName=INVALID_FUNC]",
                () -> sql("create table t (id varchar default invalid_func primary key, val int)")
        );
    }

    @Test
    public void pkWithNotMatchingFunctionalDefault() {
        assertThrowsSqlException(
                STMT_VALIDATION_ERR,
                "Functional default type mismatch: [col=ID, functionName=RAND_UUID, expectedType=UUID, actualType=STRING]",
                () ->  sql("create table tdd(id varchar default rand_uuid, val int, primary key (id) )")
        );
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
     * Check implicit colocation columns configuration (defined by PK).
     */
    @Test
    public void implicitColocationColumns() {
        sql("CREATE TABLE T0(ID0 INT, ID1 INT, VAL INT, PRIMARY KEY (ID1, ID0))");

        List<Column> colocationColumns = unwrapTableViewInternal(table("T0")).schemaView().lastKnownSchema().colocationColumns();

        assertEquals(2, colocationColumns.size());
        assertEquals("ID1", colocationColumns.get(0).name());
        assertEquals("ID0", colocationColumns.get(1).name());
    }

    @Test
    @WithSystemProperty(key = "IMPLICIT_PK_ENABLED", value = "true")
    public void reservedColumnNames() {
        assertThrowsSqlException(
                STMT_VALIDATION_ERR,
                "Failed to validate query. Column '__p_key' is reserved name.",
                () -> sql("CREATE TABLE T0(\"__p_key\" INT PRIMARY KEY, VAL INT)")
        );

        assertThrowsSqlException(
                STMT_VALIDATION_ERR,
                "Failed to validate query. Column '__part' is reserved name.",
                () -> sql("CREATE TABLE T0(\"__part\" INT PRIMARY KEY, VAL INT)")
        );

        sql("CREATE TABLE T0(id INT PRIMARY KEY)");

        assertThrowsSqlException(
                STMT_VALIDATION_ERR,
                "Failed to validate query. Column '__p_key' is reserved name.",
                () -> sql("ALTER TABLE T0 ADD COLUMN \"__p_key\" INT")
        );

        assertThrowsSqlException(
                STMT_VALIDATION_ERR,
                "Failed to validate query. Column '__part' is reserved name.",
                () -> sql("ALTER TABLE T0 ADD COLUMN \"__part\" INT")
        );
    }

    /**
     * Check implicit partition column configuration (defined by PK).
     */
    @Test
    public void implicitPartitionColumn() throws Exception {
        sql("CREATE TABLE T0(ID BIGINT PRIMARY KEY, VAL VARCHAR)");

        List<Column> columns = unwrapTableViewInternal(table("T0")).schemaView().lastKnownSchema().columns();

        assertEquals(2, columns.size());
        assertEquals("ID", columns.get(0).name());
        assertEquals("VAL", columns.get(1).name());

        Tuple key1 = Tuple.create().set("id", 101L);
        Tuple key2 = Tuple.create().set("id", 102L);

        // Add data
        sql("insert into t0 values (101, 'v1')");

        Table table = CLUSTER.node(0).tables().table("T0");
        table.keyValueView().put(null, Tuple.create().set("id", 102L), Tuple.create().set("val", "v2"));

        assertEquals(Tuple.create().set("val", "v1"), table.keyValueView().get(null, Tuple.create().set("id", 101L)));

        assertQuery("SELECT * FROM t0")
                .returns(101L, "v1")
                .returns(102L, "v2")
                .check();

        assertQuery("SELECT \"__part\" FROM t0")
                .returns(partitionForKey(table, key1))
                .returns(partitionForKey(table, key2))
                .check();

        assertQuery("SELECT \"__part\", id FROM t0")
                .returns(partitionForKey(table, key1), 101L)
                .returns(partitionForKey(table, key2), 102L)
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
    public void literalAsColumDefault() {
        // SQL Standard 2016 feature E141-07 - Basic integrity constraints. Column defaults
        sql("CREATE TABLE T0("
                + "id BIGINT DEFAULT 1 PRIMARY KEY, "
                + "valdate DATE DEFAULT DATE '2001-12-21',"
                + "valtime TIME DEFAULT TIME '11:22:33.444',"
                + "valts TIMESTAMP DEFAULT TIMESTAMP '2001-12-21 11:22:33.444',"
                + "valstr VARCHAR DEFAULT 'string',"
                + "valbin VARBINARY DEFAULT x'ff'"
                + ")");

        List<Column> columns = unwrapTableViewInternal(table("T0")).schemaView().lastKnownSchema().columns();

        assertEquals(6, columns.size());
    }

    @Test
    public void doNotAllowFunctionsInNonPkColumns() {
        // SQL Standard 2016 feature E141-07 - Basic integrity constraints. Column defaults
        assertThrowsSqlException(
                STMT_VALIDATION_ERR,
                "Functional defaults are not supported for non-primary key columns",
                () -> sql("CREATE TABLE t (id VARCHAR PRIMARY KEY, val UUID DEFAULT rand_uuid)")
        );
    }

    @ParameterizedTest
    @MethodSource("reservedSchemaNames")
    public void testItIsNotPossibleToCreateTablesInSystemSchema(String schema) {
        if (DEFINITION_SCHEMA.equals(schema) || INFORMATION_SCHEMA.equals(schema)) {
            IgniteImpl igniteImpl = unwrapIgniteImpl(CLUSTER.aliveNode());

            IgniteTestUtils.await(igniteImpl.catalogManager().execute(CreateSchemaCommand.systemSchemaBuilder().name(schema).build()));
        }

        assertThrowsSqlException(
                STMT_VALIDATION_ERR,
                "Operations with system schemas are not allowed",
                () -> sql(format("CREATE TABLE {}.SYS_TABLE (NAME VARCHAR PRIMARY KEY, SIZE BIGINT)", schema.toLowerCase())));
    }

    @ParameterizedTest
    @MethodSource("reservedSchemaNames")
    public void testCreateSystemSchemas(String schema) {
        assertThrowsSqlException(
                STMT_VALIDATION_ERR,
                format("Reserved system schema with name '{}' can't be created.", schema),
                () -> sql(format("CREATE SCHEMA {}", schema.toLowerCase())));

        assertThrowsSqlException(
                STMT_VALIDATION_ERR,
                format("Reserved system schema with name '{}' can't be created.", schema),
                () -> sql(format("CREATE SCHEMA {}", schema)));

        assertThrowsSqlException(
                STMT_VALIDATION_ERR,
                format("Reserved system schema with name '{}' can't be created.", schema),
                () -> sql(format("CREATE SCHEMA \"{}\"", schema)));
    }

    @ParameterizedTest
    @MethodSource("reservedSchemaNames")
    public void testDropSystemSchemas(String schema) {
        assertThrowsSqlException(
                STMT_VALIDATION_ERR,
                format("System schema can't be dropped [name={}]", schema),
                () -> sql(format("DROP SCHEMA {}", schema.toLowerCase())));

        assertThrowsSqlException(
                STMT_VALIDATION_ERR,
                format("System schema can't be dropped [name={}]", schema),
                () -> sql(format("DROP SCHEMA {}", schema)));

        assertThrowsSqlException(
                STMT_VALIDATION_ERR,
                format("System schema can't be dropped [name={}]", schema),
                () -> sql(format("DROP SCHEMA \"{}\"", schema)));
    }

    private static Stream<Arguments> reservedSchemaNames() {
        return SYSTEM_SCHEMAS.stream().map(Arguments::of);
    }

    @Test
    public void concurrentDrop() {
        sql("CREATE TABLE test (key INT PRIMARY KEY)");

        Ignite node = CLUSTER.node(0);
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
        sql("CREATE ZONE test_zone STORAGE PROFILES ['" + DEFAULT_STORAGE_PROFILE + "']");
        sql("CREATE TABLE test_table (id INT PRIMARY KEY, val INT) ZONE test_zone");
    }

    @Test
    public void testSuccessfulCreateTableWithZoneQuotedLiteral() {
        sql("CREATE ZONE \"test_zone\" STORAGE PROFILES ['" + DEFAULT_STORAGE_PROFILE + "']");
        sql("CREATE TABLE test_table (id INT PRIMARY KEY, val INT) ZONE \"test_zone\"");
        sql("DROP TABLE test_table");
        sql("DROP ZONE \"test_zone\"");
    }

    @Test
    public void testExceptionalCreateTableWithZoneUnquotedLiteral() {
        sql("CREATE ZONE test_zone STORAGE PROFILES ['" + DEFAULT_STORAGE_PROFILE + "']");
        assertThrowsSqlException(
                SqlException.class,
                STMT_VALIDATION_ERR,
                "Failed to validate query. Distribution zone with name 'test_zone' not found",
                () -> sql("CREATE TABLE test_table (id INT PRIMARY KEY, val INT) ZONE \"test_zone\""));
    }

    @Test
    public void tableStorageProfileWithoutSettingItExplicitly() {
        sql("CREATE TABLE TEST(ID INT PRIMARY KEY, VAL0 INT)");

        IgniteImpl node = unwrapIgniteImpl(CLUSTER.aliveNode());

        CatalogTableDescriptor table = getTable(node, "TEST");

        CatalogZoneDescriptor zone = getDefaultZone(node);

        assertEquals(zone.storageProfiles().defaultProfile().storageProfile(), table.storageProfile());
    }

    @Test
    public void tableStorageProfileExceptionIfZoneDoesntContainProfile() {
        String defaultZoneName = getDefaultZone(unwrapIgniteImpl(CLUSTER.aliveNode())).name();

        assertThrowsSqlException(
                STMT_VALIDATION_ERR,
                "Zone with name '" + defaultZoneName + "' does not contain table's storage profile",
                () -> sql("CREATE TABLE TEST(ID INT PRIMARY KEY, VAL0 INT) STORAGE PROFILE 'profile1'")
        );
    }

    @Test
    public void tableStorageProfile() {
        sql("CREATE TABLE TEST(ID INT PRIMARY KEY, VAL0 INT) STORAGE PROFILE '" + DEFAULT_STORAGE_PROFILE + "'");

        IgniteImpl node = unwrapIgniteImpl(CLUSTER.aliveNode());

        CatalogTableDescriptor table = getTable(node, "TEST");

        assertEquals(DEFAULT_STORAGE_PROFILE, table.storageProfile());

        CatalogZoneDescriptor defaultZone = getDefaultZone(node);

        assertEquals(defaultZone.storageProfiles().defaultProfile().storageProfile(), table.storageProfile());
    }

    @Test
    public void tableStorageProfileWithCustomZoneDefaultProfile() {
        sql("CREATE ZONE ZONE1 (PARTITIONS 1) STORAGE PROFILES ['" + DEFAULT_STORAGE_PROFILE + "']");

        sql("CREATE TABLE TEST(ID INT PRIMARY KEY, VAL0 INT) ZONE ZONE1");

        IgniteImpl node = unwrapIgniteImpl(CLUSTER.aliveNode());

        CatalogTableDescriptor table = getTable(node, "TEST");

        assertEquals(DEFAULT_STORAGE_PROFILE, table.storageProfile());

        sql("DROP TABLE TEST");

        sql("DROP ZONE ZONE1");
    }

    @Test
    public void tableStorageProfileWithCustomZoneExplicitProfile() {
        sql("CREATE ZONE ZONE1 (PARTITIONS 1) STORAGE PROFILES ['" + DEFAULT_STORAGE_PROFILE + "']");

        sql("CREATE TABLE TEST(ID INT PRIMARY KEY, VAL0 INT) ZONE ZONE1 STORAGE PROFILE '" + DEFAULT_STORAGE_PROFILE + "'");

        IgniteImpl node = unwrapIgniteImpl(CLUSTER.aliveNode());

        CatalogTableDescriptor table = getTable(node, "TEST");

        assertEquals(DEFAULT_STORAGE_PROFILE, table.storageProfile());

        sql("DROP TABLE TEST");

        sql("DROP ZONE ZONE1");
    }

    @Test
    public void testStringZeroLength() {
        assertThrowsSqlException(
                STMT_PARSE_ERR,
                "CHAR datatype is not supported in table",
                () -> sql("CREATE TABLE TEST(ID CHAR(0) PRIMARY KEY, VAL0 INT)")
        );

        assertThrowsSqlException(
                STMT_VALIDATION_ERR,
                "VARCHAR length 0 must be between 1 and 2147483647. [column=ID]",
                () -> sql("CREATE TABLE TEST(ID VARCHAR(0) PRIMARY KEY, VAL0 INT)")
        );

        assertThrowsSqlException(
                STMT_VALIDATION_ERR,
                "VARBINARY length 0 must be between 1 and 2147483647. [column=ID]",
                () -> sql("CREATE TABLE TEST(ID VARBINARY(0) PRIMARY KEY, VAL0 INT)")
        );
    }

    @Test
    public void quotedTableName() {
        sql("CREATE TABLE \"table Test\" (key INT PRIMARY KEY, \"Col 1\" INT)");
        sql("CREATE TABLE \"table\"\"Test\"\"\" (key INT PRIMARY KEY, \"Col\"\"1\"\"\" INT)");

        sql("INSERT INTO \"table Test\" VALUES (1, 1)");
        sql("INSERT INTO \"table\"\"Test\"\"\" VALUES (1, 2)");

        Ignite node = CLUSTER.node(0);

        assertThrows(IllegalArgumentException.class, () -> node.tables().table("table Test"));
        assertThrows(IllegalArgumentException.class, () -> node.tables().table("table\"Test\""));
        assertThrows(IllegalArgumentException.class, () -> node.tables().table("table\"\"Test\"\""));

        Table table = node.tables().table("\"table Test\"");
        assertNotNull(table);
        assertThat(table.keyValueView().get(null, Tuple.create().set("key", 1)), equalTo(Tuple.create().set("\"Col 1\"", 1)));

        table = node.tables().table("\"table\"\"Test\"\"\"");
        assertNotNull(table);
        assertThat(table.keyValueView().get(null, Tuple.create().set("key", 1)), equalTo(Tuple.create().set("\"Col\"\"1\"\"\"", 2)));

        sql("DROP TABLE \"table Test\"");
        sql("DROP TABLE \"table\"\"Test\"\"\"");
    }

    // TODO: https://issues.apache.org/jira/browse/IGNITE-17373
    //  Remove this after interval type support is added.

    @Test
    public void testCreateTableDoesNotAllowIntervals() {
        assertThrowsSqlException(
                STMT_VALIDATION_ERR,
                "Type INTERVAL YEAR cannot be used in a column definition [column=ID]",
                () -> sql("CREATE TABLE test(id INTERVAL YEAR PRIMARY KEY, val INT)")
        );

        assertThrowsSqlException(
                STMT_VALIDATION_ERR,
                "Type INTERVAL YEAR cannot be used in a column definition [column=P]",
                () -> sql("CREATE TABLE test(id INTEGER PRIMARY KEY, p INTERVAL YEAR)")
        );
    }
    // TODO: https://issues.apache.org/jira/browse/IGNITE-17373
    //  Remove this after interval type support is added.

    @Test
    public void testAlterTableDoesNotAllowIntervals() {
        sql("CREATE TABLE test(id INTEGER PRIMARY KEY, val INTEGER)");

        assertThrowsSqlException(
                STMT_VALIDATION_ERR,
                "Type INTERVAL YEAR cannot be used in a column definition [column=P]",
                () -> sql("ALTER TABLE TEST ADD COLUMN p INTERVAL YEAR")
        );
    }

    @Test
    public void testCreateTableWithIncorrectType() {
        // Char

        assertThrowsSqlException(
                STMT_PARSE_ERR,
                "Literal '2147483648' can not be parsed to type",
                () -> sql("CREATE TABLE test (id INT PRIMARY KEY, val VARCHAR(2147483648) )")
        );

        // Binary

        assertThrowsSqlException(
                STMT_PARSE_ERR,
                "Literal '2147483648' can not be parsed to type",
                () -> sql("CREATE TABLE test (id INT PRIMARY KEY, val VARBINARY(2147483648) )")
        );

        // Decimal

        assertThrowsSqlException(
                STMT_VALIDATION_ERR,
                "DECIMAL precision 10000000 must be between 1 and 32767. [column=VAL]",
                () -> sql("CREATE TABLE test (id INT PRIMARY KEY, val DECIMAL(10000000) )")
        );

        assertThrowsSqlException(
                STMT_VALIDATION_ERR,
                "DECIMAL scale 10000000 must be between 0 and 32767. [column=VAL]",
                () -> sql("CREATE TABLE test (id INT PRIMARY KEY, val DECIMAL(100, 10000000) )")
        );

        // Time

        assertThrowsSqlException(
                STMT_VALIDATION_ERR,
                "TIME precision 10000000 must be between 0 and 9. [column=VAL]",
                () -> sql("CREATE TABLE test (id INT PRIMARY KEY, val TIME(10000000) )")
        );

        // Timestamp

        assertThrowsSqlException(
                STMT_VALIDATION_ERR,
                "TIMESTAMP precision 10000000 must be between 0 and 9. [column=VAL]",
                () -> sql("CREATE TABLE test (id INT PRIMARY KEY, val TIMESTAMP(10000000) )")
        );
    }

    @Test
    public void testNotFittingDefaultValues() {
        // Char

        String longString = "1".repeat(101);
        assertThrowsSqlException(
                STMT_VALIDATION_ERR,
                "Invalid default value for column 'VAL'",
                () -> sql("CREATE TABLE test (id INT PRIMARY KEY, val VARCHAR(100) DEFAULT '" + longString + "' )")
        );

        // Binary

        String longByteString = "01".repeat(101);

        assertThrowsSqlException(
                STMT_VALIDATION_ERR,
                "Invalid default value for column 'VAL'",
                () -> sql("CREATE TABLE test (id INT PRIMARY KEY, val VARBINARY(100) DEFAULT x'" + longByteString + "' )")
        );

        // Decimal

        assertThrowsSqlException(
                STMT_VALIDATION_ERR,
                "Invalid default value for column 'VAL'",
                () -> sql("CREATE TABLE test (id INT PRIMARY KEY, val DECIMAL(5) DEFAULT 1000000 )")
        );

        assertThrowsSqlException(
                STMT_VALIDATION_ERR,
                "Invalid default value for column 'VAL'",
                () -> sql("CREATE TABLE test (id INT PRIMARY KEY, val DECIMAL(3, 2) DEFAULT 333.123 )")
        );

        // Time

        sql("CREATE TABLE test_time (id INT PRIMARY KEY, val TIME(2) DEFAULT '00:00:00.1234' )");
        sql("INSERT INTO test_time VALUES (1, DEFAULT)");
        assertQuery("SELECT val FROM test_time")
                .returns(LocalTime.of(0, 0, 0, 120_000_000))
                .check();

        // Timestamp

        sql("CREATE TABLE test_ts (id INT PRIMARY KEY, val TIMESTAMP(2) DEFAULT '2000-01-01 00:00:00.1234' )");
        sql("INSERT INTO test_ts VALUES (1, DEFAULT)");
        assertQuery("SELECT val FROM test_ts")
                .returns(LocalDateTime.of(2000, 1, 1, 0, 0, 0, 120_000_000))
                .check();
    }

    @Test
    public void testRejectNotSupportedDefaults() {
        // Compound id
        assertThrowsSqlException(
                STMT_VALIDATION_ERR,
                "Unsupported default expression: A.B.C",
                () -> sql("CREATE TABLE test (id INT, val INT DEFAULT a.b.c, primary key (id) )")
        );

        // Expression
        assertThrowsSqlException(
                STMT_VALIDATION_ERR,
                "Unsupported default expression: 1 / 0",
                () -> sql("CREATE TABLE test (id INT, val INT DEFAULT (1/0), primary key (id) )")
        );

        assertThrowsSqlException(
                STMT_VALIDATION_ERR,
                "Unsupported default expression: 1 / 0",
                () -> sql("CREATE TABLE test (id INT, val INT DEFAULT 1/0, primary key (id) )")
        );

        // SELECT

        assertThrowsSqlException(
                STMT_PARSE_ERR,
                "Query expression encountered in illegal context",
                () -> sql("CREATE TABLE test (id INT, val INT DEFAULT (SELECT 1000), primary key (id) )")
        );

        assertThrowsSqlException(
                STMT_PARSE_ERR,
                "Query expression encountered in illegal context",
                () -> sql("CREATE TABLE test (id INT, val INT DEFAULT (SELECT count(*) FROM xyz), primary key (id) )")
        );
    }

    private static @Nullable CatalogTableDescriptor getTable(IgniteImpl node, String tableName) {
        CatalogManager catalogManager = node.catalogManager();
        HybridClock clock = node.clock();

        Catalog catalog = catalogManager.activeCatalog(clock.nowLong());
        return catalog.table(SqlCommon.DEFAULT_SCHEMA_NAME, tableName);
    }

    private static CatalogZoneDescriptor getDefaultZone(IgniteImpl node) {
        CatalogManager catalogManager = node.catalogManager();
        Catalog catalog = catalogManager.catalog(catalogManager.activeCatalogVersion(node.clock().nowLong()));

        assert catalog != null;

        return Objects.requireNonNull(catalog.defaultZone());
    }

    private static int partitionForKey(Table table, Tuple keyTuple) throws Exception {
        return ((HashPartition) table.partitionManager().partitionAsync(keyTuple).get()).partitionId();
    }

    @Test
    public void creatingTableOnZoneReferencingNonExistingProfile() {
        String zoneName = "test_zone";
        String tableName = "test_table";
        String nonExistingProfileName = "no-such-profile";

        // Try to create zone with not existed storage profile.
        assertThrowsSqlException(
                STMT_VALIDATION_ERR,
                "Some storage profiles don't exist [missedProfileNames=[" + nonExistingProfileName + "]].",
                () -> sql("CREATE ZONE \"" + zoneName + "\" STORAGE PROFILES ['" + nonExistingProfileName + "']")
        );

        // Check that the zone wasn't created and table creation fails with zone not found reason.
        assertThrowsSqlException(
                STMT_VALIDATION_ERR,
                "Distribution zone with name '" + zoneName + "' not found.",
                () -> sql("CREATE TABLE " + tableName + " (id INT PRIMARY KEY, val INT) ZONE \"" + zoneName + "\"")
        );

        // Try to create table with default zone and wrong storage profile.
        assertThrowsSqlException(
                STMT_VALIDATION_ERR,
                "Zone with name 'Default' does not contain table's storage profile [storageProfile='" + nonExistingProfileName + "'].",
                () -> sql("CREATE TABLE " + tableName + " (id INT PRIMARY KEY, val INT) STORAGE PROFILE '" + nonExistingProfileName + "'")
        );

        // Verify that there still no the desired table.
        Table table = CLUSTER.aliveNode().tables().table(tableName);
        assertThat(table, is(nullValue()));
    }
}
