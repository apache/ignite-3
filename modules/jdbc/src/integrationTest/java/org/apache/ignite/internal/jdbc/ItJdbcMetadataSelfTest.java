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
import org.apache.ignite.internal.sql.ColumnMetadataImpl;
import org.apache.ignite.internal.sql.ColumnMetadataImpl.ColumnOriginImpl;
import org.apache.ignite.internal.sql.ResultSetMetadataImpl;
import org.apache.ignite.internal.sql.engine.util.SqlTestUtils;
import org.apache.ignite.internal.type.NativeType;
import org.apache.ignite.jdbc.AbstractJdbcSelfTest;
import org.apache.ignite.sql.ColumnMetadata;
import org.apache.ignite.sql.ColumnMetadata.ColumnOrigin;
import org.apache.ignite.sql.ColumnType;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link DatabaseMetaData}.
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
    public void testDatabaseMetaDataColumns() throws Exception {
        createMetaTable();

        try {
            DatabaseMetaData dbMeta = conn.getMetaData();

            List<ColumnMetadata> columnsMeta = new ArrayList<>();
            try (ResultSet rs = dbMeta.getColumns(null, "META", "TEST", null)) {
                while (rs.next()) {
                    ColumnOrigin origin = new ColumnOriginImpl(
                            rs.getString("TABLE_SCHEM"),
                            rs.getString("TABLE_NAME"),
                            rs.getString("COLUMN_NAME")
                    );
                    ColumnMetadata meta = new ColumnMetadataImpl(
                            rs.getString("COLUMN_NAME"),
                            dataTypeToColumnType(rs.getInt("DATA_TYPE"), rs.getString("TYPE_NAME")),
                            rs.getInt("COLUMN_SIZE"),
                            rs.getInt("DECIMAL_DIGITS"),
                            "YES".equals(rs.getString("IS_NULLABLE")),
                            origin
                    );
                    columnsMeta.add(meta);
                }
            }

            ResultSetMetaData rsMeta = new JdbcResultSetMetadata(new ResultSetMetadataImpl(columnsMeta));
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
            assertFalse(rs.isClosed());

            ResultSetMetaData meta = rs.getMetaData();

            checkMeta(meta);

            rs.close();
            assertTrue(rs.isClosed());
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

        try (ResultSet rs = meta.getTables(null, null, null, null)) {
            ResultSetMetaData metaData = rs.getMetaData();

            expectColumn(metaData, 1, "TABLE_CAT", Types.VARCHAR);
            expectColumn(metaData, 2, "TABLE_SCHEM", Types.VARCHAR);
            expectColumn(metaData, 3, "TABLE_NAME", Types.VARCHAR);
            expectColumn(metaData, 4, "TABLE_TYPE", Types.VARCHAR);
            expectColumn(metaData, 5, "REMARKS", Types.VARCHAR);
            expectColumn(metaData, 6, "TYPE_CAT", Types.VARCHAR);
            expectColumn(metaData, 7, "TYPE_SCHEM", Types.VARCHAR);
            expectColumn(metaData, 8, "TYPE_NAME", Types.VARCHAR);
            expectColumn(metaData, 9, "SELF_REFERENCING_COL_NAME", Types.VARCHAR);
            expectColumn(metaData, 10, "REF_GENERATION", Types.VARCHAR);
        }

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

        try (ResultSet rs = meta.getColumns(null, null, null, null)) {
            ResultSetMetaData metaData = rs.getMetaData();

            expectColumn(metaData, 1, "TABLE_CAT", Types.VARCHAR);
            expectColumn(metaData, 2, "TABLE_SCHEM", Types.VARCHAR);
            expectColumn(metaData, 3, "TABLE_NAME", Types.VARCHAR);
            expectColumn(metaData, 4, "COLUMN_NAME", Types.VARCHAR);
            expectColumn(metaData, 5, "DATA_TYPE", Types.INTEGER);
            expectColumn(metaData, 6, "TYPE_NAME", Types.VARCHAR);
            expectColumn(metaData, 7, "COLUMN_SIZE", Types.INTEGER);
            expectColumn(metaData, 8, "BUFFER_LENGTH", Types.INTEGER);
            expectColumn(metaData, 9, "DECIMAL_DIGITS", Types.INTEGER);
            expectColumn(metaData, 10, "NUM_PREC_RADIX", Types.SMALLINT);
            expectColumn(metaData, 11, "NULLABLE", Types.INTEGER);
            expectColumn(metaData, 12, "REMARKS", Types.VARCHAR);
            expectColumn(metaData, 13, "COLUMN_DEF", Types.VARCHAR);
            expectColumn(metaData, 14, "SQL_DATA_TYPE", Types.INTEGER);
            expectColumn(metaData, 15, "SQL_DATETIME_SUB", Types.INTEGER);
            expectColumn(metaData, 16, "CHAR_OCTET_LENGTH", Types.INTEGER);
            expectColumn(metaData, 17, "ORDINAL_POSITION", Types.INTEGER);
            expectColumn(metaData, 18, "IS_NULLABLE", Types.VARCHAR);
            expectColumn(metaData, 19, "SCOPE_CATLOG", Types.VARCHAR);
            expectColumn(metaData, 20, "SCOPE_SCHEMA", Types.VARCHAR);
            expectColumn(metaData, 21, "SCOPE_TABLE", Types.VARCHAR);
            expectColumn(metaData, 22, "SOURCE_DATA_TYPE", Types.SMALLINT);
            expectColumn(metaData, 23, "IS_AUTOINCREMENT", Types.VARCHAR);
            expectColumn(metaData, 24, "IS_GENERATEDCOLUMN", Types.VARCHAR);

            assertEquals(24, metaData.getColumnCount());
        }

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

    @Test
    public void testGetColumnPrivileges() throws SQLException {
        DatabaseMetaData meta = conn.getMetaData();

        try (ResultSet rs = meta.getColumnPrivileges(null, null, null, null)) {
            ResultSetMetaData metaData = rs.getMetaData();

            expectColumn(metaData, 1, "TABLE_CAT", Types.VARCHAR);
            expectColumn(metaData, 2, "TABLE_SCHEM", Types.VARCHAR);
            expectColumn(metaData, 3, "TABLE_NAME", Types.VARCHAR);
            expectColumn(metaData, 4, "COLUMN_NAME", Types.VARCHAR);
            expectColumn(metaData, 5, "GRANTOR", Types.VARCHAR);
            expectColumn(metaData, 6, "GRANTEE", Types.VARCHAR);
            expectColumn(metaData, 7, "PRIVILEGE", Types.VARCHAR);
            expectColumn(metaData, 8, "IS_GRANTABLE", Types.VARCHAR);

            assertEquals(8, metaData.getColumnCount());

            // The driver does not provide this information
            assertFalse(rs.next());
        }
    }

    @Test
    public void testGetTablePrivileges() throws SQLException {
        DatabaseMetaData meta = conn.getMetaData();

        try (ResultSet rs = meta.getTablePrivileges(null, null, null)) {
            ResultSetMetaData metaData = rs.getMetaData();

            expectColumn(metaData, 1, "TABLE_CAT", Types.VARCHAR);
            expectColumn(metaData, 2, "TABLE_SCHEM", Types.VARCHAR);
            expectColumn(metaData, 3, "TABLE_NAME", Types.VARCHAR);
            expectColumn(metaData, 4, "GRANTOR", Types.VARCHAR);
            expectColumn(metaData, 5, "GRANTEE", Types.VARCHAR);
            expectColumn(metaData, 6, "PRIVILEGE", Types.VARCHAR);
            expectColumn(metaData, 7, "IS_GRANTABLE", Types.VARCHAR);

            assertEquals(7, metaData.getColumnCount());

            // The driver does not provide this information
            assertFalse(rs.next());
        }
    }

    @Test
    public void testGetBestRowIdentifier() throws SQLException {
        DatabaseMetaData meta = conn.getMetaData();

        try (ResultSet rs = meta.getBestRowIdentifier(null, null, null, DatabaseMetaData.bestRowUnknown, true)) {
            ResultSetMetaData metaData = rs.getMetaData();

            expectColumn(metaData, 1, "SCOPE", Types.SMALLINT);
            expectColumn(metaData, 2, "COLUMN_NAME", Types.VARCHAR);
            expectColumn(metaData, 3, "DATA_TYPE", Types.INTEGER);
            expectColumn(metaData, 4, "TYPE_NAME", Types.VARCHAR);
            expectColumn(metaData, 5, "COLUMN_SIZE", Types.INTEGER);
            expectColumn(metaData, 6, "BUFFER_LENGTH", Types.INTEGER);
            expectColumn(metaData, 7, "DECIMAL_DIGITS", Types.SMALLINT);
            expectColumn(metaData, 8, "PSEUDO_COLUMN", Types.SMALLINT);

            assertEquals(8, metaData.getColumnCount());

            // The driver does not provide this information
            assertFalse(rs.next());
        }
    }

    @Test
    public void testGetVersionColumns() throws SQLException {
        DatabaseMetaData meta = conn.getMetaData();

        try (ResultSet rs = meta.getVersionColumns(null, null, null)) {
            ResultSetMetaData metaData = rs.getMetaData();

            expectColumn(metaData, 1, "SCOPE", Types.SMALLINT);
            expectColumn(metaData, 2, "COLUMN_NAME", Types.VARCHAR);
            expectColumn(metaData, 3, "DATA_TYPE", Types.INTEGER);
            expectColumn(metaData, 4, "TYPE_NAME", Types.VARCHAR);
            expectColumn(metaData, 5, "COLUMN_SIZE", Types.INTEGER);
            expectColumn(metaData, 6, "BUFFER_LENGTH", Types.INTEGER);
            expectColumn(metaData, 7, "DECIMAL_DIGITS", Types.SMALLINT);
            expectColumn(metaData, 8, "PSEUDO_COLUMN", Types.SMALLINT);

            assertEquals(8, metaData.getColumnCount());

            // The driver does not provide this information
            assertFalse(rs.next());
        }
    }

    // getIndexInfo throws java.lang.UnsupportedOperationException: Index info is not supported yet.
    @Disabled("https://issues.apache.org/jira/browse/IGNITE-26422")
    @Test
    public void testGetIndexInfo() throws SQLException {
        DatabaseMetaData meta = conn.getMetaData();

        try (ResultSet rs = meta.getIndexInfo(null, null, null, false, false)) {
            ResultSetMetaData metaData = rs.getMetaData();

            expectColumn(metaData, 1, "TABLE_CAT", Types.VARCHAR);
            expectColumn(metaData, 2, "TABLE_SCHEM", Types.VARCHAR);
            expectColumn(metaData, 3, "TABLE_NAME", Types.VARCHAR);
            expectColumn(metaData, 4, "NON_UNIQUE", Types.BOOLEAN);
            expectColumn(metaData, 5, "INDEX_QUALIFIER", Types.VARCHAR);
            expectColumn(metaData, 6, "INDEX_NAME", Types.VARCHAR);
            expectColumn(metaData, 7, "TYPE", Types.SMALLINT);
            expectColumn(metaData, 8, "ORDINAL_POSITION", Types.SMALLINT);
            expectColumn(metaData, 9, "COLUMN_NAME", Types.VARCHAR);
            expectColumn(metaData, 10, "ASC_OR_DESC", Types.VARCHAR);
            expectColumn(metaData, 11, "CARDINALITY", Types.INTEGER);
            expectColumn(metaData, 12, "PAGES", Types.INTEGER);
            expectColumn(metaData, 13, "FILTER_CONDITION", Types.VARCHAR);

            assertEquals(13, metaData.getColumnCount());
        }
    }

    /**
     * Checks organisation table column names and types.
     *
     * @param rs ResultSet.
     *
     */
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
     *
     */
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

        assertTrue(rs.next());
        assertEquals("SYSTEM", rs.getString("TABLE_SCHEM"));
        assertEquals("TRANSACTIONS", rs.getString("TABLE_NAME"));
        assertEquals("TRANSACTION_LABEL", rs.getString("COLUMN_NAME"));
        assertEquals(Types.VARCHAR, rs.getInt("DATA_TYPE"));
        assertEquals("VARCHAR", rs.getString("TYPE_NAME"));
        assertEquals(1, rs.getInt("NULLABLE"));
        assertEquals(64, rs.getInt("COLUMN_SIZE"));

        assertFalse(rs.next());
    }

    @Test
    public void testSchemas() throws Exception {
        DatabaseMetaData meta = conn.getMetaData();

        try (ResultSet rs = meta.getSchemas()) {
            ResultSetMetaData metaData = rs.getMetaData();

            expectColumn(metaData, 1, "TABLE_SCHEM", Types.VARCHAR);
            expectColumn(metaData, 2, "TABLE_CATALOG", Types.VARCHAR);
        }

        try (ResultSet rs = meta.getSchemas()) {
            List<String> schemas = new ArrayList<>();

            while (rs.next()) {
                schemas.add(rs.getString(1));
            }

            assertEquals(List.of("META", "PUBLIC", "SYSTEM", "USER1", "USER2", "user0"), schemas);
        }

        try (ResultSet rs = meta.getSchemas("IGNITE", "USER%")) {
            List<String> schemas = new ArrayList<>();

            while (rs.next()) {
                schemas.add(rs.getString(1));
            }

            assertEquals(List.of("USER1", "USER2"), schemas);
        }

        try (ResultSet rs = meta.getSchemas(null, "qqq")) {
            assertFalse(rs.next(), "Empty result set is expected");
        }
    }

    @Test
    public void testGetPrimaryKeys() throws Exception {
        DatabaseMetaData meta = conn.getMetaData();

        try (ResultSet rs = meta.getPrimaryKeys(null, null, null)) {
            ResultSetMetaData metaData = rs.getMetaData();

            expectColumn(metaData, 1, "TABLE_CAT", Types.VARCHAR);
            expectColumn(metaData, 2, "TABLE_SCHEM", Types.VARCHAR);
            expectColumn(metaData, 3, "TABLE_NAME", Types.VARCHAR);
            expectColumn(metaData, 4, "COLUMN_NAME", Types.VARCHAR);
            expectColumn(metaData, 5, "KEY_SEQ", Types.SMALLINT);
            expectColumn(metaData, 6, "PK_NAME", Types.VARCHAR);

            assertEquals(6, metaData.getColumnCount());
        }

        {
            ResultSet rs = meta.getPrimaryKeys(null, "PUBLIC", "PERSON");

            int cnt = 0;

            while (rs.next()) {
                assertEquals("ORGID", rs.getString("COLUMN_NAME"));

                cnt++;
            }

            assertEquals(1, cnt);
        }

        {
            ResultSet rs = meta.getPrimaryKeys(null, null, null);

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
    }

    @Test
    public void testGetExportedKeys() throws SQLException {
        DatabaseMetaData meta = conn.getMetaData();

        try (ResultSet rs = meta.getExportedKeys(null, null, null)) {
            ResultSetMetaData metaData = rs.getMetaData();

            // According to DatabaseMetaData#getExportedKeys javadoc
            expectColumn(metaData, 1, "PKTABLE_CAT", Types.VARCHAR);
            expectColumn(metaData, 2, "PKTABLE_SCHEM", Types.VARCHAR);
            expectColumn(metaData, 3, "PKTABLE_NAME", Types.VARCHAR);
            expectColumn(metaData, 4, "PKCOLUMN_NAME", Types.VARCHAR);
            expectColumn(metaData, 5, "FKTABLE_CAT", Types.VARCHAR);
            expectColumn(metaData, 6, "FKTABLE_SCHEM", Types.VARCHAR);
            expectColumn(metaData, 7, "FKTABLE_NAME", Types.VARCHAR);
            expectColumn(metaData, 8, "FKCOLUMN_NAME", Types.VARCHAR);
            expectColumn(metaData, 9, "KEY_SEQ", Types.SMALLINT);
            expectColumn(metaData, 10, "UPDATE_RULE", Types.SMALLINT);
            expectColumn(metaData, 11, "DELETE_RULE", Types.SMALLINT);
            expectColumn(metaData, 12, "FK_NAME", Types.VARCHAR);
            expectColumn(metaData, 13, "PK_NAME", Types.VARCHAR);
            expectColumn(metaData, 14, "DEFERRABILITY", Types.SMALLINT);

            assertEquals(14, metaData.getColumnCount());

            // The driver does not provide this information
            assertFalse(rs.next());
        }
    }

    @Test
    public void testGetImportedKeys() throws SQLException {
        DatabaseMetaData meta = conn.getMetaData();

        try (ResultSet rs = meta.getImportedKeys(null, null, null)) {
            ResultSetMetaData metaData = rs.getMetaData();

            expectColumn(metaData, 1, "PKTABLE_CAT", Types.VARCHAR);
            expectColumn(metaData, 2, "PKTABLE_SCHEM", Types.VARCHAR);
            expectColumn(metaData, 3, "PKTABLE_NAME", Types.VARCHAR);
            expectColumn(metaData, 4, "PKCOLUMN_NAME", Types.VARCHAR);
            expectColumn(metaData, 5, "FKTABLE_CAT", Types.VARCHAR);
            expectColumn(metaData, 6, "FKTABLE_SCHEM", Types.VARCHAR);
            expectColumn(metaData, 7, "FKTABLE_NAME", Types.VARCHAR);
            expectColumn(metaData, 8, "FKCOLUMN_NAME", Types.VARCHAR);
            expectColumn(metaData, 9, "KEY_SEQ", Types.SMALLINT);
            expectColumn(metaData, 10, "UPDATE_RULE", Types.SMALLINT);
            expectColumn(metaData, 11, "DELETE_RULE", Types.SMALLINT);
            expectColumn(metaData, 12, "FK_NAME", Types.VARCHAR);
            expectColumn(metaData, 13, "PK_NAME", Types.VARCHAR);
            expectColumn(metaData, 14, "DEFERRABILITY", Types.SMALLINT);

            assertEquals(14, metaData.getColumnCount());

            // The driver does not provide this information
            assertFalse(rs.next());
        }
    }

    @Test
    public void testGetCrossReference() throws SQLException {
        try (ResultSet rs = conn.getMetaData().getCrossReference(null, null, null, null, null, null)) {
            assertFalse(rs.next());

            ResultSetMetaData metaData = rs.getMetaData();

            expectColumn(metaData, 1, "PKTABLE_CAT", Types.VARCHAR);
            expectColumn(metaData, 2, "PKTABLE_SCHEM", Types.VARCHAR);
            expectColumn(metaData, 3, "PKTABLE_NAME", Types.VARCHAR);
            expectColumn(metaData, 4, "PKCOLUMN_NAME", Types.VARCHAR);
            expectColumn(metaData, 5, "FKTABLE_CAT", Types.VARCHAR);
            expectColumn(metaData, 6, "FKTABLE_SCHEM", Types.VARCHAR);
            expectColumn(metaData, 7, "FKTABLE_NAME", Types.VARCHAR);
            expectColumn(metaData, 8, "FKCOLUMN_NAME", Types.VARCHAR);
            expectColumn(metaData, 9, "KEY_SEQ", Types.SMALLINT);
            expectColumn(metaData, 10, "UPDATE_RULE", Types.SMALLINT);
            expectColumn(metaData, 11, "DELETE_RULE", Types.SMALLINT);
            expectColumn(metaData, 12, "FK_NAME", Types.VARCHAR);
            expectColumn(metaData, 13, "PK_NAME", Types.VARCHAR);
            expectColumn(metaData, 14, "DEFERRABILITY", Types.SMALLINT);

            assertEquals(14, metaData.getColumnCount());

            // The driver does not provide this information
            assertFalse(rs.next());
        }
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

        try (ResultSet rs = meta.getTableTypes()) {
            ResultSetMetaData metaData = rs.getMetaData();

            expectColumn(metaData, 1, "TABLE_TYPE", Types.VARCHAR);

            assertTrue(rs.next());

            assertEquals("TABLE", rs.getString("TABLE_TYPE"));

            assertTrue(rs.next());

            assertEquals("VIEW", rs.getString("TABLE_TYPE"));

            assertFalse(rs.next());
        }
    }

    @Test
    public void testGetTypeInfo() throws SQLException {
        DatabaseMetaData meta = conn.getMetaData();

        try (ResultSet rs = meta.getTypeInfo()) {
            ResultSetMetaData metaData = rs.getMetaData();

            expectColumn(metaData, 1, "TYPE_NAME", Types.VARCHAR);
            expectColumn(metaData, 2, "DATA_TYPE", Types.INTEGER);
            expectColumn(metaData, 3, "PRECISION", Types.INTEGER);
            expectColumn(metaData, 4, "LITERAL_PREFIX", Types.VARCHAR);
            expectColumn(metaData, 5, "LITERAL_SUFFIX", Types.VARCHAR);
            expectColumn(metaData, 6, "CREATE_PARAMS", Types.VARCHAR);
            expectColumn(metaData, 7, "NULLABLE", Types.SMALLINT);
            expectColumn(metaData, 8, "CASE_SENSITIVE", Types.BOOLEAN);
            expectColumn(metaData, 9, "SEARCHABLE", Types.SMALLINT);
            expectColumn(metaData, 10, "UNSIGNED_ATTRIBUTE", Types.BOOLEAN);
            expectColumn(metaData, 11, "FIXED_PREC_SCALE", Types.BOOLEAN);
            expectColumn(metaData, 12, "AUTO_INCREMENT", Types.BOOLEAN);
            expectColumn(metaData, 13, "LOCAL_TYPE_NAME", Types.VARCHAR);
            expectColumn(metaData, 14, "MINIMUM_SCALE", Types.SMALLINT);
            expectColumn(metaData, 15, "MAXIMUM_SCALE", Types.SMALLINT);
            expectColumn(metaData, 16, "SQL_DATA_TYPE", Types.INTEGER);
            expectColumn(metaData, 17, "SQL_DATETIME_SUB", Types.INTEGER);
            expectColumn(metaData, 18, "NUM_PREC_RADIX", Types.INTEGER);

            assertEquals(18, metaData.getColumnCount());
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

    @Test
    public void testGetProduces() throws SQLException {
        DatabaseMetaData meta = conn.getMetaData();

        try (ResultSet rs = meta.getProcedures(null, null, null)) {
            ResultSetMetaData metaData = rs.getMetaData();

            expectColumn(metaData, 1, "PROCEDURE_CAT", Types.VARCHAR);
            expectColumn(metaData, 2, "PROCEDURE_SCHEM", Types.VARCHAR);
            expectColumn(metaData, 3, "PROCEDURE_NAME", Types.VARCHAR);
            // 4, 5, 6 are reserved for future use
            expectColumn(metaData, 4, "", Types.NULL);
            expectColumn(metaData, 5, "", Types.NULL);
            expectColumn(metaData, 6, "", Types.NULL);
            expectColumn(metaData, 7, "REMARKS", Types.VARCHAR);
            expectColumn(metaData, 8, "PROCEDURE_TYPE", Types.SMALLINT);
            expectColumn(metaData, 9, "SPECIFIC_NAME", Types.VARCHAR);

            assertEquals(9, metaData.getColumnCount());

            // The driver does not provide this information
            assertFalse(rs.next());
        }
    }

    @Test
    public void testGetProcedureColumns() throws SQLException {
        DatabaseMetaData meta = conn.getMetaData();

        try (ResultSet rs = meta.getProcedureColumns(null, null, null, null)) {
            ResultSetMetaData metaData = rs.getMetaData();

            expectColumn(metaData, 1, "PROCEDURE_CAT", Types.VARCHAR);
            expectColumn(metaData, 2, "PROCEDURE_SCHEM", Types.VARCHAR);
            expectColumn(metaData, 3, "PROCEDURE_NAME", Types.VARCHAR);
            expectColumn(metaData, 4, "COLUMN_NAME", Types.VARCHAR);
            expectColumn(metaData, 5, "COLUMN_TYPE", Types.SMALLINT);
            expectColumn(metaData, 6, "DATA_TYPE", Types.INTEGER);
            expectColumn(metaData, 7, "TYPE_NAME", Types.VARCHAR);
            expectColumn(metaData, 8, "PRECISION", Types.INTEGER);
            expectColumn(metaData, 8, "PRECISION", Types.INTEGER);
            expectColumn(metaData, 9, "LENGTH", Types.INTEGER);
            expectColumn(metaData, 10, "SCALE", Types.SMALLINT);
            expectColumn(metaData, 11, "RADIX", Types.SMALLINT);
            expectColumn(metaData, 12, "NULLABLE", Types.SMALLINT);
            expectColumn(metaData, 13, "REMARKS", Types.VARCHAR);
            expectColumn(metaData, 14, "COLUMN_DEF", Types.VARCHAR);
            expectColumn(metaData, 15, "SQL_DATA_TYPE", Types.INTEGER);
            expectColumn(metaData, 16, "SQL_DATETIME_SUB", Types.INTEGER);
            expectColumn(metaData, 17, "CHAR_OCTET_LENGTH", Types.INTEGER);
            expectColumn(metaData, 18, "ORDINAL_POSITION", Types.INTEGER);
            expectColumn(metaData, 19, "IS_NULLABLE", Types.VARCHAR);
            expectColumn(metaData, 20, "SPECIFIC_NAME", Types.VARCHAR);

            assertEquals(20, metaData.getColumnCount());

            // The driver does not provide this information
            assertFalse(rs.next());
        }
    }

    @Test
    public void testGetCatalogs() throws SQLException {
        try (ResultSet rs = conn.getMetaData().getCatalogs()) {
            ResultSetMetaData metaData = rs.getMetaData();
            assertEquals("TABLE_CAT", metaData.getColumnName(1));
            assertEquals(Types.VARCHAR, metaData.getColumnType(1));

            assertEquals(1, metaData.getColumnCount());

            assertTrue(rs.next());
            assertEquals("IGNITE", rs.getString(1));
            assertFalse(rs.next());
        }
    }

    @Test
    public void testGetUdts() throws SQLException {
        DatabaseMetaData meta = conn.getMetaData();

        try (ResultSet rs = meta.getUDTs(null, null, null, null)) {
            ResultSetMetaData metaData = rs.getMetaData();

            expectColumn(metaData, 1, "TYPE_CAT", Types.VARCHAR);
            expectColumn(metaData, 2, "TYPE_SCHEM", Types.VARCHAR);
            expectColumn(metaData, 3, "TYPE_NAME", Types.VARCHAR);
            expectColumn(metaData, 4, "CLASS_NAME", Types.VARCHAR);
            expectColumn(metaData, 5, "DATA_TYPE", Types.INTEGER);
            expectColumn(metaData, 6, "REMARKS", Types.VARCHAR);
            expectColumn(metaData, 7, "BASE_TYPE", Types.SMALLINT);

            assertEquals(7, metaData.getColumnCount());

            // The driver does not provide this information
            assertFalse(rs.next());
        }
    }

    @Test
    public void testGetSuperTypes() throws SQLException {
        DatabaseMetaData meta = conn.getMetaData();

        try (ResultSet rs = meta.getSuperTypes(null, null, null)) {
            ResultSetMetaData metaData = rs.getMetaData();

            expectColumn(metaData, 1, "TYPE_CAT", Types.VARCHAR);
            expectColumn(metaData, 2, "TYPE_SCHEM", Types.VARCHAR);
            expectColumn(metaData, 3, "TYPE_NAME", Types.VARCHAR);
            expectColumn(metaData, 4, "SUPERTYPE_CAT", Types.VARCHAR);
            expectColumn(metaData, 5, "SUPERTYPE_SCHEM", Types.VARCHAR);
            expectColumn(metaData, 6, "SUPERTYPE_NAME", Types.VARCHAR);

            assertEquals(6, metaData.getColumnCount());

            // The driver does not provide this information
            assertFalse(rs.next());
        }
    }

    @Test
    public void testGetSuperTables() throws SQLException {
        DatabaseMetaData meta = conn.getMetaData();

        try (ResultSet rs = meta.getSuperTables(null, null, null)) {
            ResultSetMetaData metaData = rs.getMetaData();

            expectColumn(metaData, 1, "TABLE_CAT", Types.VARCHAR);
            expectColumn(metaData, 2, "TABLE_SCHEM", Types.VARCHAR);
            expectColumn(metaData, 3, "TABLE_NAME", Types.VARCHAR);
            expectColumn(metaData, 4, "SUPERTABLE_NAME", Types.VARCHAR);

            assertEquals(4, metaData.getColumnCount());

            // The driver does not provide this information
            assertFalse(rs.next());
        }
    }

    @Test
    public void testGetAttributes() throws SQLException {
        DatabaseMetaData meta = conn.getMetaData();

        try (ResultSet rs = meta.getAttributes(null, null, null, null)) {
            ResultSetMetaData metaData = rs.getMetaData();

            expectColumn(metaData, 1, "TYPE_CAT", Types.VARCHAR);
            expectColumn(metaData, 2, "TYPE_SCHEM", Types.VARCHAR);
            expectColumn(metaData, 3, "TYPE_NAME", Types.VARCHAR);
            expectColumn(metaData, 4, "ATTR_NAME", Types.VARCHAR);
            expectColumn(metaData, 5, "DATA_TYPE", Types.INTEGER);
            expectColumn(metaData, 6, "ATTR_TYPE_NAME", Types.VARCHAR);
            expectColumn(metaData, 7, "ATTR_SIZE", Types.INTEGER);
            expectColumn(metaData, 8, "DECIMAL_DIGITS", Types.INTEGER);
            expectColumn(metaData, 9, "NUM_PREC_RADIX", Types.INTEGER);
            expectColumn(metaData, 10, "NULLABLE", Types.INTEGER);
            expectColumn(metaData, 11, "REMARKS", Types.VARCHAR);
            expectColumn(metaData, 12, "ATTR_DEF", Types.VARCHAR);
            expectColumn(metaData, 13, "SQL_DATA_TYPE", Types.INTEGER);
            expectColumn(metaData, 14, "SQL_DATETIME_SUB", Types.INTEGER);
            expectColumn(metaData, 15, "CHAR_OCTET_LENGTH", Types.INTEGER);
            expectColumn(metaData, 16, "ORDINAL_POSITION", Types.INTEGER);
            expectColumn(metaData, 17, "IS_NULLABLE", Types.VARCHAR);
            expectColumn(metaData, 18, "SCOPE_CATALOG", Types.VARCHAR);
            expectColumn(metaData, 19, "SCOPE_SCHEMA", Types.VARCHAR);
            expectColumn(metaData, 20, "SCOPE_TABLE", Types.VARCHAR);
            expectColumn(metaData, 21, "SOURCE_DATA_TYPE", Types.SMALLINT);

            assertEquals(21, metaData.getColumnCount());

            // The driver does not provide this information
            assertFalse(rs.next());
        }
    }

    @Test
    public void testGetClientInfoProperties() throws SQLException {
        try (Connection connection = DriverManager.getConnection(URL)) {
            DatabaseMetaData meta = connection.getMetaData();

            try (ResultSet rs = meta.getClientInfoProperties()) {
                ResultSetMetaData metaData = rs.getMetaData();

                expectColumn(metaData, 1, "NAME", Types.VARCHAR);
                expectColumn(metaData, 2, "MAX_LEN", Types.INTEGER);
                expectColumn(metaData, 3, "DEFAULT_VALUE", Types.VARCHAR);
                expectColumn(metaData, 4, "DESCRIPTION", Types.VARCHAR);

                assertEquals(4, metaData.getColumnCount());

                // The driver does not provide this information
                assertFalse(rs.next());
            }

            connection.close();

            // Works on a closed connection since this is a client-side operation.
            try (ResultSet rs = meta.getClientInfoProperties()) {
                ResultSetMetaData metaData = rs.getMetaData();
                assertEquals(4, metaData.getColumnCount());
            }
        }
    }

    @Test
    public void testGetFunctions() throws SQLException {
        DatabaseMetaData meta = conn.getMetaData();

        try (ResultSet rs = meta.getFunctions(null, null, null)) {
            ResultSetMetaData metaData = rs.getMetaData();

            expectColumn(metaData, 1, "FUNCTION_CAT", Types.VARCHAR);
            expectColumn(metaData, 2, "FUNCTION_SCHEM", Types.VARCHAR);
            expectColumn(metaData, 3, "FUNCTION_NAME", Types.VARCHAR);
            expectColumn(metaData, 4, "REMARKS", Types.VARCHAR);
            // Note: Implementation currently exposes FUNCTION_TYPE as VARCHAR
            expectColumn(metaData, 5, "FUNCTION_TYPE", Types.VARCHAR);
            expectColumn(metaData, 6, "SPECIFIC_NAME", Types.VARCHAR);

            assertEquals(6, metaData.getColumnCount());

            // The driver does not provide this information
            assertFalse(rs.next());
        }
    }

    @Test
    public void testGetFunctionColumns() throws SQLException {
        DatabaseMetaData meta = conn.getMetaData();

        try (ResultSet rs = meta.getFunctionColumns(null, null, null, null)) {
            ResultSetMetaData metaData = rs.getMetaData();

            expectColumn(metaData, 1, "FUNCTION_CAT", Types.VARCHAR);
            expectColumn(metaData, 2, "FUNCTION_SCHEM", Types.VARCHAR);
            expectColumn(metaData, 3, "FUNCTION_NAME", Types.VARCHAR);
            expectColumn(metaData, 4, "COLUMN_NAME", Types.VARCHAR);
            expectColumn(metaData, 5, "COLUMN_TYPE", Types.SMALLINT);
            expectColumn(metaData, 6, "DATA_TYPE", Types.INTEGER);
            expectColumn(metaData, 7, "TYPE_NAME", Types.VARCHAR);
            expectColumn(metaData, 8, "PRECISION", Types.INTEGER);
            expectColumn(metaData, 9, "LENGTH", Types.INTEGER);
            expectColumn(metaData, 10, "SCALE", Types.SMALLINT);
            expectColumn(metaData, 11, "RADIX", Types.SMALLINT);
            expectColumn(metaData, 12, "NULLABLE", Types.SMALLINT);
            expectColumn(metaData, 13, "REMARKS", Types.VARCHAR);
            expectColumn(metaData, 14, "CHAR_OCTET_LENGTH", Types.INTEGER);
            expectColumn(metaData, 15, "ORDINAL_POSITION", Types.INTEGER);
            expectColumn(metaData, 16, "IS_NULLABLE", Types.VARCHAR);
            expectColumn(metaData, 17, "SPECIFIC_NAME", Types.VARCHAR);

            assertEquals(17, metaData.getColumnCount());

            // The driver does not provide this information
            assertFalse(rs.next());
        }
    }

    @Test
    public void testGetPseudoColumns() throws SQLException {
        DatabaseMetaData meta = conn.getMetaData();

        try (ResultSet rs = meta.getPseudoColumns(null, null, null, null)) {
            ResultSetMetaData metaData = rs.getMetaData();

            expectColumn(metaData, 1, "TABLE_CAT", Types.VARCHAR);
            expectColumn(metaData, 2, "TABLE_SCHEM", Types.VARCHAR);
            expectColumn(metaData, 3, "TABLE_NAME", Types.VARCHAR);
            expectColumn(metaData, 4, "COLUMN_NAME", Types.VARCHAR);
            expectColumn(metaData, 5, "DATA_TYPE", Types.INTEGER);
            expectColumn(metaData, 6, "COLUMN_SIZE", Types.INTEGER);
            expectColumn(metaData, 7, "DECIMAL_DIGITS", Types.INTEGER);
            expectColumn(metaData, 8, "NUM_PREC_RADIX", Types.INTEGER);
            expectColumn(metaData, 9, "COLUMN_USAGE", Types.INTEGER);
            expectColumn(metaData, 10, "REMARKS", Types.VARCHAR);
            expectColumn(metaData, 11, "CHAR_OCTET_LENGTH", Types.INTEGER);
            expectColumn(metaData, 12, "IS_NULLABLE", Types.VARCHAR);

            assertEquals(12, metaData.getColumnCount());

            // The driver does not provide this information
            assertFalse(rs.next());
        }
    }

    private static void expectColumn(ResultSetMetaData metaData, int column, String name, int type) throws SQLException {
        assertEquals(name, metaData.getColumnName(column), column + " name");
        assertEquals(type, metaData.getColumnType(column), column + " type");
    }
}
