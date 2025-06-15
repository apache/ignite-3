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

package org.apache.ignite.internal.jdbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.StringJoiner;
import java.util.UUID;
import org.apache.ignite.internal.client.proto.ProtocolVersion;
import org.apache.ignite.internal.jdbc.proto.event.JdbcColumnMeta;
import org.apache.ignite.internal.sql.engine.util.SqlTestUtils;
import org.apache.ignite.internal.type.NativeType;
import org.apache.ignite.jdbc.AbstractJdbcSelfTest;
import org.apache.ignite.jdbc.util.JdbcTestUtils;
import org.apache.ignite.sql.ColumnType;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

/**
 * Metadata tests.
 */
public class ItJdbcMetadataSelfTest extends AbstractJdbcSelfTest {
    /** Creates tables. */
    @BeforeAll
    public static void createTables() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE SCHEMA IF NOT EXISTS PUBLIC;"
                    + "CREATE SCHEMA IF NOT EXISTS META;"
                    + "CREATE SCHEMA IF NOT EXISTS USER2;"
                    + "CREATE SCHEMA IF NOT EXISTS \"user0\";"
                    + "CREATE SCHEMA IF NOT EXISTS USER1;"
                    + "CREATE TABLE person(name VARCHAR(32), age INT, orgid INT PRIMARY KEY);"
                    + "CREATE TABLE organization(id INT PRIMARY KEY, name VARCHAR, bigdata DECIMAL(20, 10));"
                    + "CREATE TABLE user1.table1(id INT PRIMARY KEY);"
                    + "CREATE TABLE user2.\"table2\"(id INT PRIMARY KEY);"
                    + "CREATE TABLE \"user0\".\"table0\"(\"id\" INT PRIMARY KEY);"
                    + "CREATE TABLE \"user0\".table0(id INT PRIMARY KEY);"
                    + "INSERT INTO person (orgid, name, age) VALUES (1, '111', 111);"
                    + "INSERT INTO organization (id, name, bigdata) VALUES (1, 'AAA', 10);"
            );
        }
    }

    @Test
    public void testNullValuesMetaData() throws Exception {
        ResultSet rs = stmt.executeQuery(
                "select NULL, substring(null, 1, 2)");

        assertNotNull(rs);

        ResultSetMetaData meta = rs.getMetaData();

        assertNotNull(meta);

        assertEquals(2, meta.getColumnCount());

        assertEquals(Types.NULL, meta.getColumnType(1));
        assertEquals("NULL", meta.getColumnTypeName(1));
        assertEquals("java.lang.Void", meta.getColumnClassName(1));

        assertEquals(Types.NULL, meta.getColumnType(2));
        assertEquals("NULL", meta.getColumnTypeName(2));
        assertEquals("java.lang.Void", meta.getColumnClassName(2));
    }

    @Test
    public void testResultSetMetaData() throws Exception {
        ResultSet rs = stmt.executeQuery(
                "select p.name, o.id as orgId, p.age from PERSON p, ORGANIZATION o where p.orgId = o.id");

        assertNotNull(rs);

        ResultSetMetaData meta = rs.getMetaData();

        assertNotNull(meta);

        assertEquals(3, meta.getColumnCount());

        assertEquals("Person".toUpperCase(), meta.getTableName(1).toUpperCase());
        assertEquals("name".toUpperCase(), meta.getColumnName(1).toUpperCase());
        assertEquals("name".toUpperCase(), meta.getColumnLabel(1).toUpperCase());
        assertEquals(Types.VARCHAR, meta.getColumnType(1));
        assertEquals("VARCHAR", meta.getColumnTypeName(1));
        assertEquals("java.lang.String", meta.getColumnClassName(1));

        assertEquals("Organization".toUpperCase(), meta.getTableName(2).toUpperCase());
        assertEquals("id".toUpperCase(), meta.getColumnName(2).toUpperCase());
        assertEquals("orgId".toUpperCase(), meta.getColumnLabel(2).toUpperCase());
        assertEquals(Types.INTEGER, meta.getColumnType(2));
        assertEquals("INTEGER", meta.getColumnTypeName(2));
        assertEquals("java.lang.Integer", meta.getColumnClassName(2));
    }

    @Test
    public void testDatabaseMetaDataColumns() throws Exception {
        createMetaTable();

        try {
            DatabaseMetaData dbMeta = conn.getMetaData();

            List<JdbcColumnMeta> columnsMeta = new ArrayList<>();
            try (ResultSet rs = dbMeta.getColumns(null, "META", "TEST", null)) {
                while (rs.next()) {
                    JdbcColumnMeta meta = new JdbcColumnMeta(
                            rs.getString("COLUMN_NAME"),
                            rs.getString("TABLE_SCHEM"),
                            rs.getString("TABLE_NAME"),
                            rs.getString("COLUMN_NAME"),
                            dataTypeToColumnType(rs.getInt("DATA_TYPE"), rs.getString("TYPE_NAME")),
                            rs.getShort("COLUMN_SIZE"),
                            rs.getShort("DECIMAL_DIGITS"),
                            "YES".equals(rs.getString("IS_NULLABLE"))
                    );
                    columnsMeta.add(meta);
                }
            }

            ResultSetMetaData rsMeta = new JdbcResultSetMetadata(columnsMeta);
            checkMeta(rsMeta);
        } finally {
            stmt.execute("DROP TABLE META.TEST;");
        }
    }

    private ColumnType dataTypeToColumnType(int dataType, String typeName) {
        ColumnType type = null;

        switch (dataType) {
            case Types.BOOLEAN:
                type = ColumnType.BOOLEAN;
                break;
            case Types.TINYINT:
                type = ColumnType.INT8;
                break;
            case Types.SMALLINT:
                type = ColumnType.INT16;
                break;
            case Types.INTEGER:
                type = ColumnType.INT32;
                break;
            case Types.BIGINT:
                type = ColumnType.INT64;
                break;
            case Types.REAL:
                type = ColumnType.FLOAT;
                break;
            case Types.DOUBLE:
                type = ColumnType.DOUBLE;
                break;
            case Types.DECIMAL:
                type = ColumnType.DECIMAL;
                break;
            case Types.DATE:
                type = ColumnType.DATE;
                break;
            case Types.TIME:
                type = ColumnType.TIME;
                break;
            case Types.TIMESTAMP:
                type = ColumnType.DATETIME;
                break;
            case Types.OTHER:
                if (typeName.equals("UUID")) {
                    type = ColumnType.UUID;
                } else if (typeName.equals("TIMESTAMP WITH LOCAL TIME ZONE")) {
                    type = ColumnType.TIMESTAMP;
                }
                break;
            case Types.VARCHAR:
                type = ColumnType.STRING;
                break;
            case Types.VARBINARY:
                type = ColumnType.BYTE_ARRAY;
                break;
            default:
                break;
        }

        assertNotNull(type, "Not supported type " + dataType + " " + typeName);

        return type;
    }

    @Test
    public void testResultSetMetaDataColumns() throws Exception {
        createMetaTable();

        try {
            ResultSet rs = stmt.executeQuery("SELECT * FROM META.TEST t");

            assertNotNull(rs);

            ResultSetMetaData meta = rs.getMetaData();

            checkMeta(meta);
        } finally {
            stmt.execute("DROP TABLE META.TEST;");
        }
    }

    private void checkMeta(ResultSetMetaData meta) throws SQLException {
        assertNotNull(meta);

        assertEquals(16, meta.getColumnCount());

        assertEquals("META", meta.getSchemaName(1));
        assertEquals("TEST", meta.getTableName(1).toUpperCase());

        int i = 1;
        checkMeta(meta, i++, "BOOLEAN_COL", Types.BOOLEAN, "BOOLEAN", Boolean.class);
        checkMeta(meta, i++, "TINYINT_COL", Types.TINYINT, "TINYINT", Byte.class);
        checkMeta(meta, i++, "SMALLINT_COL", Types.SMALLINT, "SMALLINT", Short.class);
        checkMeta(meta, i++, "INTEGER_COL", Types.INTEGER, "INTEGER", Integer.class);
        checkMeta(meta, i++, "BIGINT_COL", Types.BIGINT, "BIGINT", Long.class);
        checkMeta(meta, i++, "REAL_COL", Types.REAL, "REAL", Float.class);
        checkMeta(meta, i++, "DOUBLE_COL", Types.DOUBLE, "DOUBLE", Double.class);
        checkMeta(meta, i++, "DECIMAL_COL", Types.DECIMAL, "DECIMAL", BigDecimal.class);
        checkMeta(meta, i++, "DATE_COL", Types.DATE, "DATE", java.sql.Date.class);
        checkMeta(meta, i++, "TIME_COL", Types.TIME, "TIME", java.sql.Time.class);
        checkMeta(meta, i++, "TIMESTAMP_COL", Types.TIMESTAMP, "TIMESTAMP", java.sql.Timestamp.class);
        checkMeta(meta, i++, "TIMESTAMP_WITH_LOCAL_TIME_ZONE_COL", Types.OTHER, "TIMESTAMP WITH LOCAL TIME ZONE", java.sql.Timestamp.class);
        checkMeta(meta, i++, "UUID_COL", Types.OTHER, "UUID", UUID.class);
        checkMeta(meta, i++, "VARCHAR_COL", Types.VARCHAR, "VARCHAR", String.class);
        checkMeta(meta, i++, "VARBINARY_COL", Types.VARBINARY, "VARBINARY", byte[].class);

        assertEquals(i, meta.getColumnCount(), "There are not checked columns");
    }

    private void checkMeta(ResultSetMetaData meta, int idx, String columnName, int expType, String expTypeName, Class expClass)
            throws SQLException {
        assertEquals(columnName, meta.getColumnName(idx).toUpperCase());
        assertEquals(columnName, meta.getColumnLabel(idx).toUpperCase());
        assertEquals(expType, meta.getColumnType(idx));
        assertEquals(expTypeName, meta.getColumnTypeName(idx));
        assertEquals(expClass.getName(), meta.getColumnClassName(idx));
    }

    private void createMetaTable() {
        try {
            StringJoiner joiner = new StringJoiner(",");

            Arrays.stream(NativeType.nativeTypes())
                    .forEach(t -> {
                        String type = SqlTestUtils.toSqlType(t);
                        joiner.add(type.replace(' ', '_') + "_COL " + type);
                    });
            joiner.add("id INT PRIMARY KEY");

            stmt.executeUpdate("CREATE TABLE meta.test(" + joiner + ")");
        } catch (SQLException ex) {
            throw new RuntimeException(ex);
        }
    }

    @Test
    public void testGetTables() throws Exception {
        DatabaseMetaData meta = conn.getMetaData();

        // PUBLIC tables.
        {
            try (ResultSet rs = meta.getTables("IGNITE", "PUBLIC", "%", new String[]{"TABLE"})) {
                assertNotNull(rs);
                assertTrue(rs.next());
                assertEquals("PUBLIC", rs.getString("TABLE_SCHEM"));
                assertEquals("TABLE", rs.getString("TABLE_TYPE"));
                assertEquals("ORGANIZATION", rs.getString("TABLE_NAME"));
                assertTrue(rs.next());
                assertEquals("PUBLIC", rs.getString("TABLE_SCHEM"));
                assertEquals("TABLE", rs.getString("TABLE_TYPE"));
                assertEquals("PERSON", rs.getString("TABLE_NAME"));
            }

            try (ResultSet rs = meta.getTables("IGNITE", "PUBLIC", "%", null)) {
                assertNotNull(rs);
                assertTrue(rs.next());
                assertEquals("PUBLIC", rs.getString("TABLE_SCHEM"));
                assertEquals("TABLE", rs.getString("TABLE_TYPE"));
                assertEquals("ORGANIZATION", rs.getString("TABLE_NAME"));
                assertTrue(rs.next());
                assertEquals("PUBLIC", rs.getString("TABLE_SCHEM"));
                assertEquals("TABLE", rs.getString("TABLE_TYPE"));
                assertEquals("PERSON", rs.getString("TABLE_NAME"));
            }

            try (ResultSet rs = meta.getTables("IGNITE", "PUBLIC", "ORGANIZATION", new String[]{"VIEW"})) {
                assertFalse(rs.next());
            }

            try (ResultSet rs = meta.getTables("IGNITE", "PUBLIC", "", new String[]{"WRONG"})) {
                assertFalse(rs.next());
            }
        }

        // All tables.
        try (ResultSet rs = meta.getTables("IGNITE", "%", "%", new String[]{"TABLE"})) {
            assertNotNull(rs);
            assertTrue(rs.next());
            assertEquals("PUBLIC", rs.getString("TABLE_SCHEM"));
            assertEquals("TABLE", rs.getString("TABLE_TYPE"));
            assertEquals("ORGANIZATION", rs.getString("TABLE_NAME"));
            assertTrue(rs.next());
            assertEquals("PUBLIC", rs.getString("TABLE_SCHEM"));
            assertEquals("TABLE", rs.getString("TABLE_TYPE"));
            assertEquals("PERSON", rs.getString("TABLE_NAME"));
            assertTrue(rs.next());
            assertEquals("USER1", rs.getString("TABLE_SCHEM"));
            assertEquals("TABLE", rs.getString("TABLE_TYPE"));
            assertEquals("TABLE1", rs.getString("TABLE_NAME"));
            assertTrue(rs.next());
            assertEquals("USER2", rs.getString("TABLE_SCHEM"));
            assertEquals("TABLE", rs.getString("TABLE_TYPE"));
            assertEquals("table2", rs.getString("TABLE_NAME"));
            assertTrue(rs.next());
            assertEquals("user0", rs.getString("TABLE_SCHEM"));
            assertEquals("TABLE", rs.getString("TABLE_TYPE"));
            assertEquals("TABLE0", rs.getString("TABLE_NAME"));
            assertTrue(rs.next());
            assertEquals("user0", rs.getString("TABLE_SCHEM"));
            assertEquals("TABLE", rs.getString("TABLE_TYPE"));
            assertEquals("table0", rs.getString("TABLE_NAME"));
            assertFalse(rs.next());
        }

        // Case sensitive table name.
        try (ResultSet rs = meta.getTables("IGNITE", "USER2", "table%", null)) {
            assertTrue(rs.next());
            assertEquals("USER2", rs.getString("TABLE_SCHEM"));
            assertEquals("TABLE", rs.getString("TABLE_TYPE"));
            assertEquals("table2", rs.getString("TABLE_NAME"));
            assertFalse(rs.next());
        }

        // Case sensitive schema name.
        try (ResultSet rs = meta.getTables("IGNITE", "user%", "%", null)) {
            assertTrue(rs.next());
            assertEquals("user0", rs.getString("TABLE_SCHEM"));
            assertEquals("TABLE", rs.getString("TABLE_TYPE"));
            assertEquals("TABLE0", rs.getString("TABLE_NAME"));
            assertTrue(rs.next());
            assertEquals("user0", rs.getString("TABLE_SCHEM"));
            assertEquals("TABLE", rs.getString("TABLE_TYPE"));
            assertEquals("table0", rs.getString("TABLE_NAME"));
            assertFalse(rs.next());
        }

        // System views.
        {
            try (ResultSet rs = meta.getTables("IGNITE", "%", "TABLES", new String[]{"VIEW"})) {
                assertTrue(rs.next());
                assertEquals("SYSTEM", rs.getString("TABLE_SCHEM"));
                assertEquals("VIEW", rs.getString("TABLE_TYPE"));
                assertEquals("TABLES", rs.getString("TABLE_NAME"));
            }

            try (ResultSet rs = meta.getTables("IGNITE", "SYSTEM", "TABLES", new String[]{"VIEW"})) {
                assertTrue(rs.next());
                assertEquals("SYSTEM", rs.getString("TABLE_SCHEM"));
                assertEquals("VIEW", rs.getString("TABLE_TYPE"));
                assertEquals("TABLES", rs.getString("TABLE_NAME"));
            }

            try (ResultSet rs = meta.getTables("IGNITE", "%", "TABLES", new String[]{"TABLE"})) {
                assertFalse(rs.next());
            }
        }
    }

    @Test
    public void testGetColumns() throws Exception {
        DatabaseMetaData meta = conn.getMetaData();

        // Tables.
        {
            ResultSet rs = meta.getColumns("IGNITE", "PUBLIC", "%", "%");

            checkOrgTableColumns(rs);
            checkPersonTableColumns(rs);
            assertFalse(rs.next());

            rs = meta.getColumns("IGNITE", "PUBLIC", "PERSON", "%");

            checkPersonTableColumns(rs);
            assertFalse(rs.next());

            rs = meta.getColumns(null, "PUBLIC", "PERSON", null);

            checkPersonTableColumns(rs);
            assertFalse(rs.next());

            rs = meta.getColumns("IGNITE", "PUBLIC", "ORGANIZATION", "%");

            checkOrgTableColumns(rs);
            assertFalse(rs.next());

            rs = meta.getColumns(null, "PUBLIC", "ORGANIZATION", null);

            checkOrgTableColumns(rs);
            assertFalse(rs.next());

            rs = meta.getColumns(null, "USER%", "%", null);

            checkUser1Columns(rs);
            checkUser2Columns(rs);

            assertFalse(rs.next());

            // Case sensitive column name.
            {
                rs = meta.getColumns(null, "user%", "%", "id");

                assertTrue(rs.next());
                assertEquals("user0", rs.getString("TABLE_SCHEM"));
                assertEquals("table0", rs.getString("TABLE_NAME"));
                assertEquals("id", rs.getString("COLUMN_NAME"));
                assertEquals(Types.INTEGER, rs.getInt("DATA_TYPE"));
                assertEquals("INTEGER", rs.getString("TYPE_NAME"));
                assertEquals(0, rs.getInt("NULLABLE"));
                assertFalse(rs.next());
            }
        }

        // System view.
        {
            ResultSet rs = meta.getColumns("IGNITE", "SYSTEM", "TRANSACTIONS", "TRANSACTION_%");

            checkTxViewColumns(rs);
        }
    }

    /**
     * Checks organisation table column names and types.
     *
     * @param rs ResultSet.
     * */
    private static void checkOrgTableColumns(ResultSet rs) throws SQLException {
        assertNotNull(rs);

        assertTrue(rs.next());
        assertEquals("ID", rs.getString("COLUMN_NAME"));
        assertEquals(Types.INTEGER, rs.getInt("DATA_TYPE"));
        assertEquals("INTEGER", rs.getString("TYPE_NAME"));
        assertEquals(0, rs.getInt("NULLABLE"));

        assertTrue(rs.next());
        assertEquals("NAME", rs.getString("COLUMN_NAME"));
        assertEquals(Types.VARCHAR, rs.getInt("DATA_TYPE"));
        assertEquals("VARCHAR", rs.getString("TYPE_NAME"));
        assertEquals(1, rs.getInt("NULLABLE"));

        assertTrue(rs.next());
        assertEquals("BIGDATA", rs.getString("COLUMN_NAME"));
        assertEquals(Types.DECIMAL, rs.getInt("DATA_TYPE"));
        assertEquals("DECIMAL", rs.getString("TYPE_NAME"));
        assertEquals(1, rs.getInt("NULLABLE"));
        assertEquals(10, rs.getInt("DECIMAL_DIGITS"));
        assertEquals(20, rs.getInt("COLUMN_SIZE"));
    }

    /**
     * Checks person table column names and types.
     *
     * @param rs ResultSet.
     * */
    private static void checkPersonTableColumns(ResultSet rs) throws SQLException {
        assertNotNull(rs);

        assertTrue(rs.next());
        assertEquals("PUBLIC", rs.getString("TABLE_SCHEM"));
        assertEquals("PERSON", rs.getString("TABLE_NAME"));
        assertEquals("NAME", rs.getString("COLUMN_NAME"));
        assertEquals(Types.VARCHAR, rs.getInt("DATA_TYPE"));
        assertEquals("VARCHAR", rs.getString("TYPE_NAME"));
        assertEquals(1, rs.getInt("NULLABLE"));
        assertEquals(32, rs.getInt("COLUMN_SIZE"));

        assertTrue(rs.next());

        assertEquals("PUBLIC", rs.getString("TABLE_SCHEM"));
        assertEquals("PERSON", rs.getString("TABLE_NAME"));
        assertEquals("AGE", rs.getString("COLUMN_NAME"));
        assertEquals(Types.INTEGER, rs.getInt("DATA_TYPE"));
        assertEquals("INTEGER", rs.getString("TYPE_NAME"));
        assertEquals(1, rs.getInt("NULLABLE"));
        assertEquals(10, rs.getInt("COLUMN_SIZE"));

        assertTrue(rs.next());
        assertEquals("PUBLIC", rs.getString("TABLE_SCHEM"));
        assertEquals("PERSON", rs.getString("TABLE_NAME"));
        assertEquals("ORGID", rs.getString("COLUMN_NAME"));
        assertEquals(Types.INTEGER, rs.getInt("DATA_TYPE"));
        assertEquals("INTEGER", rs.getString("TYPE_NAME"));
        assertEquals(0, rs.getInt("NULLABLE"));
        assertEquals(10, rs.getInt("COLUMN_SIZE"));
    }

    private static void checkUser1Columns(ResultSet rs) throws SQLException {
        assertTrue(rs.next());
        assertEquals("USER1", rs.getString("TABLE_SCHEM"));
        assertEquals("TABLE1", rs.getString("TABLE_NAME"));
        assertEquals("ID", rs.getString("COLUMN_NAME"));
        assertEquals(Types.INTEGER, rs.getInt("DATA_TYPE"));
        assertEquals("INTEGER", rs.getString("TYPE_NAME"));
        assertEquals(0, rs.getInt("NULLABLE"));
    }

    private static void checkUser2Columns(ResultSet rs) throws SQLException {
        assertTrue(rs.next());
        assertEquals("USER2", rs.getString("TABLE_SCHEM"));
        assertEquals("table2", rs.getString("TABLE_NAME"));
        assertEquals("ID", rs.getString("COLUMN_NAME"));
        assertEquals(Types.INTEGER, rs.getInt("DATA_TYPE"));
        assertEquals("INTEGER", rs.getString("TYPE_NAME"));
        assertEquals(0, rs.getInt("NULLABLE"));
    }

    private static void checkTxViewColumns(ResultSet rs) throws SQLException {
        assertTrue(rs.next());
        assertEquals("SYSTEM", rs.getString("TABLE_SCHEM"));
        assertEquals("TRANSACTIONS", rs.getString("TABLE_NAME"));
        assertEquals("TRANSACTION_STATE", rs.getString("COLUMN_NAME"));
        assertEquals(Types.VARCHAR, rs.getInt("DATA_TYPE"));
        assertEquals("VARCHAR", rs.getString("TYPE_NAME"));
        assertEquals(1, rs.getInt("NULLABLE"));
        assertEquals(64, rs.getInt("COLUMN_SIZE"));

        assertTrue(rs.next());
        assertEquals("SYSTEM", rs.getString("TABLE_SCHEM"));
        assertEquals("TRANSACTIONS", rs.getString("TABLE_NAME"));
        assertEquals("TRANSACTION_ID", rs.getString("COLUMN_NAME"));
        assertEquals(Types.VARCHAR, rs.getInt("DATA_TYPE"));
        assertEquals("VARCHAR", rs.getString("TYPE_NAME"));
        assertEquals(1, rs.getInt("NULLABLE"));
        assertEquals(64, rs.getInt("COLUMN_SIZE"));

        assertTrue(rs.next());
        assertEquals("SYSTEM", rs.getString("TABLE_SCHEM"));
        assertEquals("TRANSACTIONS", rs.getString("TABLE_NAME"));
        assertEquals("TRANSACTION_START_TIME", rs.getString("COLUMN_NAME"));
        assertEquals(Types.OTHER, rs.getInt("DATA_TYPE"));
        assertEquals("TIMESTAMP WITH LOCAL TIME ZONE", rs.getString("TYPE_NAME"));
        assertEquals(1, rs.getInt("NULLABLE"));
        assertEquals(9, rs.getInt("COLUMN_SIZE"));

        assertTrue(rs.next());
        assertEquals("SYSTEM", rs.getString("TABLE_SCHEM"));
        assertEquals("TRANSACTIONS", rs.getString("TABLE_NAME"));
        assertEquals("TRANSACTION_TYPE", rs.getString("COLUMN_NAME"));
        assertEquals(Types.VARCHAR, rs.getInt("DATA_TYPE"));
        assertEquals("VARCHAR", rs.getString("TYPE_NAME"));
        assertEquals(1, rs.getInt("NULLABLE"));
        assertEquals(64, rs.getInt("COLUMN_SIZE"));

        assertTrue(rs.next());
        assertEquals("SYSTEM", rs.getString("TABLE_SCHEM"));
        assertEquals("TRANSACTIONS", rs.getString("TABLE_NAME"));
        assertEquals("TRANSACTION_PRIORITY", rs.getString("COLUMN_NAME"));
        assertEquals(Types.VARCHAR, rs.getInt("DATA_TYPE"));
        assertEquals("VARCHAR", rs.getString("TYPE_NAME"));
        assertEquals(1, rs.getInt("NULLABLE"));
        assertEquals(64, rs.getInt("COLUMN_SIZE"));

        assertFalse(rs.next());
    }

    /**
     * Check JDBC support flags.
     */
    @Test
    public void testCheckSupports() throws SQLException {
        DatabaseMetaData meta = conn.getMetaData();

        assertTrue(meta.supportsANSI92EntryLevelSQL());
        assertTrue(meta.supportsAlterTableWithAddColumn());
        assertTrue(meta.supportsAlterTableWithDropColumn());
        assertTrue(meta.nullPlusNonNullIsNull());
    }

    @Test
    public void testVersions() throws Exception {
        assertEquals(conn.getMetaData().getDatabaseProductVersion(), ProtocolVersion.LATEST_VER.toString(),
                "Unexpected ignite database product version.");
        assertEquals(conn.getMetaData().getDriverVersion(), ProtocolVersion.LATEST_VER.toString(),
                "Unexpected ignite driver version.");
    }

    @Test
    public void testSchemasMetadata() throws Exception {
        try (ResultSet rs = conn.getMetaData().getSchemas()) {
            List<String> schemas = new ArrayList<>();

            while (rs.next()) {
                schemas.add(rs.getString(1));
            }

            assertEquals(List.of("META", "PUBLIC", "SYSTEM", "USER1", "USER2", "user0"), schemas);
        }

        try (ResultSet rs = conn.getMetaData().getSchemas("IGNITE", "USER%")) {
            List<String> schemas = new ArrayList<>();

            while (rs.next()) {
                schemas.add(rs.getString(1));
            }

            assertEquals(List.of("USER1", "USER2"), schemas);
        }
    }

    @Test
    public void testEmptySchemasMetadata() throws Exception {
        ResultSet rs = conn.getMetaData().getSchemas(null, "qqq");

        assertFalse(rs.next(), "Empty result set is expected");
    }

    @Test
    public void testPrimaryKeyMetadata() throws Exception {
        ResultSet rs = conn.getMetaData().getPrimaryKeys(null, "PUBLIC", "PERSON");

        int cnt = 0;

        while (rs.next()) {
            assertEquals("ORGID", rs.getString("COLUMN_NAME"));

            cnt++;
        }

        assertEquals(1, cnt);
    }

    @Test
    public void testGetAllPrimaryKeys() throws Exception {
        ResultSet rs = conn.getMetaData().getPrimaryKeys(null, null, null);

        List<String> expectedPks = Arrays.asList(
                "PUBLIC.ORGANIZATION.PK_ORGANIZATION.ID",
                "PUBLIC.PERSON.PK_PERSON.ORGID",
                "USER1.TABLE1.PK_TABLE1.ID",
                "USER2.table2.PK_table2.ID",
                "user0.TABLE0.PK_TABLE0.ID",
                "user0.table0.PK_table0.id"
        );

        List<String> actualPks = new ArrayList<>(expectedPks.size());

        while (rs.next()) {
            actualPks.add(rs.getString("TABLE_SCHEM")
                    + '.' + rs.getString("TABLE_NAME")
                    + '.' + rs.getString("PK_NAME")
                    + '.' + rs.getString("COLUMN_NAME"));
        }

        assertEquals(expectedPks, actualPks, "Metadata contains unexpected primary keys info.");
    }

    @Test
    public void testInvalidCatalog() throws Exception {
        DatabaseMetaData meta = conn.getMetaData();

        ResultSet rs = meta.getSchemas("q", null);

        assertFalse(rs.next(), "Results must be empty");

        rs = meta.getTables("q", null, null, null);

        assertFalse(rs.next(), "Results must be empty");

        rs = meta.getColumns("q", null, null, null);

        assertFalse(rs.next(), "Results must be empty");

        rs = meta.getIndexInfo("q", null, null, false, false);

        assertFalse(rs.next(), "Results must be empty");

        rs = meta.getPrimaryKeys("q", null, null);

        assertFalse(rs.next(), "Results must be empty");
    }

    @Test
    public void testGetTableTypes() throws Exception {
        DatabaseMetaData meta = conn.getMetaData();

        ResultSet rs = meta.getTableTypes();

        assertTrue(rs.next());

        assertEquals("TABLE", rs.getString("TABLE_TYPE"));

        assertFalse(rs.next());
    }

    @Test
    @Disabled("https://issues.apache.org/jira/browse/IGNITE-16203")
    public void testParametersMetadata() throws Exception {
        // Perform checks few times due to query/plan caching.
        for (int i = 0; i < 3; i++) {
            // No parameters statement.
            try (Connection conn = DriverManager.getConnection(URL)) {
                conn.setSchema("\"pers\"");

                PreparedStatement noParams = conn.prepareStatement("select * from Person;");
                ParameterMetaData params = noParams.getParameterMetaData();

                assertEquals(0, params.getParameterCount(), "Parameters should be empty.");
            }

            // Selects.
            try (Connection conn = DriverManager.getConnection(URL)) {
                conn.setSchema("\"pers\"");

                PreparedStatement selectStmt = conn.prepareStatement("select orgId from Person p where p.name > ? and p.orgId > ?");

                ParameterMetaData meta = selectStmt.getParameterMetaData();

                assertNotNull(meta);

                assertEquals(2, meta.getParameterCount());

                assertEquals(Types.VARCHAR, meta.getParameterType(1));
                assertEquals(ParameterMetaData.parameterNullableUnknown, meta.isNullable(1));
                assertEquals(Integer.MAX_VALUE, meta.getPrecision(1));

                assertEquals(Types.INTEGER, meta.getParameterType(2));
                assertEquals(ParameterMetaData.parameterNullableUnknown, meta.isNullable(2));
            }

            // Updates.
            try (Connection conn = DriverManager.getConnection(URL)) {
                conn.setSchema("\"pers\"");

                PreparedStatement updateStmt = conn.prepareStatement("update Person p set orgId = 42 where p.name > ? and p.orgId > ?");

                ParameterMetaData meta = updateStmt.getParameterMetaData();

                assertNotNull(meta);

                assertEquals(2, meta.getParameterCount());

                assertEquals(Types.VARCHAR, meta.getParameterType(1));
                assertEquals(ParameterMetaData.parameterNullableUnknown, meta.isNullable(1));
                assertEquals(Integer.MAX_VALUE, meta.getPrecision(1));

                assertEquals(Types.INTEGER, meta.getParameterType(2));
                assertEquals(ParameterMetaData.parameterNullableUnknown, meta.isNullable(2));
            }

            // Multistatement
            try (Connection conn = DriverManager.getConnection(URL)) {
                conn.setSchema("\"pers\"");

                PreparedStatement updateStmt = conn.prepareStatement(
                        "update Person p set orgId = 42 where p.name > ? and p.orgId > ?;"
                                + "select orgId from Person p where p.name > ? and p.orgId > ?");

                ParameterMetaData meta = updateStmt.getParameterMetaData();

                assertNotNull(meta);

                assertEquals(4, meta.getParameterCount());

                assertEquals(Types.VARCHAR, meta.getParameterType(1));
                assertEquals(ParameterMetaData.parameterNullableUnknown, meta.isNullable(1));
                assertEquals(Integer.MAX_VALUE, meta.getPrecision(1));

                assertEquals(Types.INTEGER, meta.getParameterType(2));
                assertEquals(ParameterMetaData.parameterNullableUnknown, meta.isNullable(2));

                assertEquals(Types.VARCHAR, meta.getParameterType(3));
                assertEquals(ParameterMetaData.parameterNullableUnknown, meta.isNullable(3));
                assertEquals(Integer.MAX_VALUE, meta.getPrecision(3));

                assertEquals(Types.INTEGER, meta.getParameterType(4));
                assertEquals(ParameterMetaData.parameterNullableUnknown, meta.isNullable(4));
            }
        }
    }

    /**
     * Check that parameters metadata throws correct exception on non-parsable statement.
     */
    @Test
    @Disabled("https://issues.apache.org/jira/browse/IGNITE-16203")
    public void testParametersMetadataNegative() throws Exception {
        try (Connection conn = DriverManager.getConnection(URL)) {
            conn.setSchema("\"pers\"");

            PreparedStatement notCorrect = conn.prepareStatement("select * from NotExistingTable;");

            JdbcTestUtils.assertThrowsSqlException("Table NOTEXISTINGTABLE not found", notCorrect::getParameterMetaData);
        }
    }

    /**
     * Negative scenarios for catalog name. Perform metadata lookups, that use incorrect catalog names.
     */
    @Test
    public void testCatalogWithNotExistingName() throws SQLException {
        checkNoEntitiesFoundForCatalog("");
        checkNoEntitiesFoundForCatalog("NOT_EXISTING_CATALOG");
    }

    // IgniteCustomType: Add JDBC metadata test for your type.

    /**
     * Check that lookup in the metadata have been performed using specified catalog name (that is neither {@code null} nor correct catalog
     * name), empty result set is returned.
     *
     * @param invalidCat catalog name that is not either
     */
    private void checkNoEntitiesFoundForCatalog(String invalidCat) throws SQLException {
        DatabaseMetaData meta = conn.getMetaData();

        // Intention: we set the other arguments that way, the values to have as many results as possible.
        assertIsEmpty(meta.getTables(invalidCat, null, "%", new String[]{"TABLE"}));
        assertIsEmpty(meta.getColumns(invalidCat, null, "%", "%"));
        assertIsEmpty(meta.getColumnPrivileges(invalidCat, "pers", "PERSON", "%"));
        assertIsEmpty(meta.getTablePrivileges(invalidCat, null, "%"));
        assertIsEmpty(meta.getPrimaryKeys(invalidCat, "pers", "PERSON"));
        assertIsEmpty(meta.getImportedKeys(invalidCat, "pers", "PERSON"));
        assertIsEmpty(meta.getExportedKeys(invalidCat, "pers", "PERSON"));
        // meta.getCrossReference(...) doesn't make sense because we don't have FK constraint.
        assertIsEmpty(meta.getIndexInfo(invalidCat, null, "%", false, true));
        assertIsEmpty(meta.getSuperTables(invalidCat, "%", "%"));
        assertIsEmpty(meta.getSchemas(invalidCat, null));
        assertIsEmpty(meta.getPseudoColumns(invalidCat, null, "%", ""));
    }

    /**
     * Assert that specified ResultSet contains no rows.
     *
     * @param rs result set to check.
     * @throws SQLException on error.
     */
    private static void assertIsEmpty(ResultSet rs) throws SQLException {
        try (rs) {
            boolean empty = !rs.next();

            assertTrue(empty, "Result should be empty because invalid catalog is specified.");
        }
    }
}
