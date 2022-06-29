/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.runner.app.jdbc;

import static java.sql.Types.BIGINT;
import static java.sql.Types.BINARY;
import static java.sql.Types.DATE;
import static java.sql.Types.DECIMAL;
import static java.sql.Types.DOUBLE;
import static java.sql.Types.FLOAT;
import static java.sql.Types.INTEGER;
import static java.sql.Types.OTHER;
import static java.sql.Types.SMALLINT;
import static java.sql.Types.TINYINT;
import static java.sql.Types.VARCHAR;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.ignite.Ignite;
import org.apache.ignite.internal.client.proto.ProtocolVersion;
import org.apache.ignite.internal.schema.configuration.SchemaConfigurationConverter;
import org.apache.ignite.internal.util.Pair;
import org.apache.ignite.schema.SchemaBuilders;
import org.apache.ignite.schema.definition.ColumnDefinition;
import org.apache.ignite.schema.definition.ColumnType;
import org.apache.ignite.schema.definition.ColumnType.ColumnTypeSpec;
import org.apache.ignite.schema.definition.TableDefinition;
import org.apache.ignite.table.Table;
import org.apache.ignite.table.Tuple;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

/**
 * Metadata tests.
 */
public class ItJdbcMetadataSelfTest extends AbstractJdbcSelfTest {
    static List<Pair<ColumnDefinition, Integer>> allColumns;

    /** Creates tables. */
    @BeforeAll
    public static void createTables() {
        assert !clusterNodes.isEmpty();

        TableDefinition perTbl = SchemaBuilders.tableBuilder("PUBLIC", "PERSON").columns(
                SchemaBuilders.column("NAME", ColumnType.string()).asNullable(true).build(),
                SchemaBuilders.column("AGE", ColumnType.INT32).asNullable(true).build(),
                SchemaBuilders.column("ORGID", ColumnType.INT32).build()
        ).withPrimaryKey("ORGID").build();

        TableDefinition orgTbl = SchemaBuilders.tableBuilder("PUBLIC", "ORGANIZATION").columns(
                SchemaBuilders.column("ID", ColumnType.INT32).build(),
                SchemaBuilders.column("NAME", ColumnType.string()).asNullable(true).build(),
                SchemaBuilders.column("BIGDATA", ColumnType.decimalOf(20, 10)).asNullable(true).build(),
                SchemaBuilders.column("DUR", ColumnType.duration(3)).asNullable(true).build(),
                SchemaBuilders.column("PER", ColumnType.PERIOD).asNullable(true).build()
        ).withPrimaryKey("ID").build();

        allColumns = new ArrayList<>();

        allColumns.add(new Pair<>(SchemaBuilders.column("BYTEVAL", ColumnType.INT8).asNullable(true).build(), TINYINT));
        allColumns.add(new Pair<>(SchemaBuilders.column("SHORTVAL", ColumnType.INT16).asNullable(true).build(), SMALLINT));
        allColumns.add(new Pair<>(SchemaBuilders.column("ID", ColumnType.INT32).build(), INTEGER));
        allColumns.add(new Pair<>(SchemaBuilders.column("INTVAL", ColumnType.INT32).asNullable(true).build(), INTEGER));
        allColumns.add(new Pair<>(SchemaBuilders.column("LONGVAL", ColumnType.INT64).asNullable(true).build(), BIGINT));
        allColumns.add(new Pair<>(SchemaBuilders.column("FLOATVAL", ColumnType.FLOAT).asNullable(true).build(), FLOAT));
        allColumns.add(new Pair<>(SchemaBuilders.column("DOUBLEVAL", ColumnType.DOUBLE).asNullable(true).build(), DOUBLE));
        //not supported yet
        //SchemaBuilders.column("UUIDVAL" ...
        //SchemaBuilders.column("BITSET" ...
        allColumns.add(new Pair<>(SchemaBuilders.column("DATEVAL", ColumnType.DATE).asNullable(true).build(), OTHER));
        allColumns.add(new Pair<>(SchemaBuilders.column("PERVAL", ColumnType.PERIOD).asNullable(true).build(), OTHER));
        allColumns.add(new Pair<>(SchemaBuilders.column("BIGVAL", ColumnType.decimalOf()).asNullable(true).build(), DECIMAL));
        allColumns.add(new Pair<>(SchemaBuilders.column("STRVAL", ColumnType.string()).asNullable(true).build(), VARCHAR));
        allColumns.add(new Pair<>(SchemaBuilders.column("ARRVAL", ColumnType.blobOf()).asNullable(true).build(), BINARY));
        allColumns.add(new Pair<>(SchemaBuilders.column("TIMEVAL", ColumnType.TemporalColumnType.time()).asNullable(true).build(), OTHER));
        allColumns.add(new Pair<>(SchemaBuilders.column("DATETIMEVAL",
                ColumnType.TemporalColumnType.datetime()).asNullable(true).build(), OTHER));
        allColumns.add(new Pair<>(SchemaBuilders.column("TSVAL",
                ColumnType.TemporalColumnType.timestamp()).asNullable(true).build(), OTHER));
        allColumns.add(new Pair<>(SchemaBuilders.column("URLVAL", ColumnType.blobOf()).asNullable(true).build(), BINARY));
        allColumns.add(new Pair<>(SchemaBuilders.column("DURVAL", ColumnType.duration()).asNullable(true).build(), OTHER));

        // exclude types : ColumnType.UINTx and not supported
        assertEquals(allColumns.size() + 4 + 1, ColumnTypeSpec.values().length, "Some data types are not covered.");

        List<ColumnDefinition> columns = allColumns.stream().map(Pair::getFirst).collect(Collectors.toList());

        TableDefinition allTypesTbl = SchemaBuilders.tableBuilder("PUBLIC", "ALLTYPES").columns(columns)
                .withPrimaryKey("ID").build();

        clusterNodes.get(0).tables().createTable(perTbl.canonicalName(), tblCh ->
                SchemaConfigurationConverter.convert(perTbl, tblCh)
                        .changeReplicas(1)
                        .changePartitions(10)
        );

        clusterNodes.get(0).tables().createTable(orgTbl.canonicalName(), tblCh ->
                SchemaConfigurationConverter.convert(orgTbl, tblCh)
                        .changeReplicas(1)
                        .changePartitions(10)
        );

        clusterNodes.get(0).tables().createTable(allTypesTbl.canonicalName(), tblCh ->
                SchemaConfigurationConverter.convert(allTypesTbl, tblCh)
        );

        Table tbl1 = clusterNodes.get(0).tables().table(perTbl.canonicalName());
        Table tbl2 = clusterNodes.get(0).tables().table(orgTbl.canonicalName());
        Table tbl3 = clusterNodes.get(0).tables().table(allTypesTbl.canonicalName());

        tbl1.recordView().insert(null, Tuple.create().set("ORGID", 1).set("NAME", "111").set("AGE", 111));
        tbl2.recordView().insert(null, Tuple.create().set("ID", 1).set("NAME", "AAA").set("BIGDATA", BigDecimal.valueOf(10)));
        tbl3.recordView().insert(null, Tuple.create().set("ID", 1));
    }

    @Test
    public void testColumnTypesDescribed() throws Exception {
        Statement stmt = DriverManager.getConnection(URL).createStatement();

        ResultSet rs = stmt.executeQuery("select * from ALLTYPES");

        assertNotNull(rs);

        ResultSetMetaData meta = rs.getMetaData();

        assertNotNull(meta);

        assertEquals(allColumns.size(), meta.getColumnCount());

        int pos = 0;

        for (Pair<ColumnDefinition, Integer> colDef : allColumns) {
            assertEquals(colDef.getSecond(), meta.getColumnType(++pos), "Unexpected column type: " + meta.getColumnType(pos));

            rs = stmt.executeQuery("select " + colDef.getFirst().name() + " from ALLTYPES");

            assertTrue(rs.next());
        }
    }

    @Test
    public void testResultSetMetaData() throws Exception {
        Statement stmt = DriverManager.getConnection(URL).createStatement();

        ResultSet rs = stmt.executeQuery(
                "select p.name, o.id as orgId, p.age, o.dur, o.per from PERSON p, ORGANIZATION o where p.orgId = o.id");

        assertNotNull(rs);

        ResultSetMetaData meta = rs.getMetaData();

        assertNotNull(meta);

        assertEquals(5, meta.getColumnCount());

        assertEquals("Person".toUpperCase(), meta.getTableName(1).toUpperCase());
        assertEquals("name".toUpperCase(), meta.getColumnName(1).toUpperCase());
        assertEquals("name".toUpperCase(), meta.getColumnLabel(1).toUpperCase());
        assertEquals(VARCHAR, meta.getColumnType(1));
        assertEquals("VARCHAR", meta.getColumnTypeName(1));
        assertEquals("java.lang.String", meta.getColumnClassName(1));

        assertEquals("Organization".toUpperCase(), meta.getTableName(2).toUpperCase());
        assertEquals("id".toUpperCase(), meta.getColumnName(2).toUpperCase());
        assertEquals("orgId".toUpperCase(), meta.getColumnLabel(2).toUpperCase());
        assertEquals(INTEGER, meta.getColumnType(2));
        assertEquals("INTEGER", meta.getColumnTypeName(2));
        assertEquals("java.lang.Integer", meta.getColumnClassName(2));

        assertEquals("dur".toUpperCase(), meta.getColumnName(4).toUpperCase());
        assertEquals("dur".toUpperCase(), meta.getColumnLabel(4).toUpperCase());
        assertEquals(OTHER, meta.getColumnType(4));
        assertEquals("INTERVAL", meta.getColumnTypeName(4));
        assertEquals("java.time.Duration", meta.getColumnClassName(4));

        assertEquals("per".toUpperCase(), meta.getColumnName(5).toUpperCase());
        assertEquals("per".toUpperCase(), meta.getColumnLabel(5).toUpperCase());
        assertEquals(OTHER, meta.getColumnType(5));
        assertEquals("INTERVAL", meta.getColumnTypeName(5));
        assertEquals("java.time.Period", meta.getColumnClassName(5));
    }

    @Test
    @Disabled("https://issues.apache.org/jira/browse/IGNITE-15507")
    public void testDecimalAndDateTypeMetaData() throws Exception {
        createMetaTable();

        try {
            ResultSet rs = stmt.executeQuery("SELECT t.DECIMAL_COL, t.DATE FROM PUBLIC.METATEST t;");

            assertNotNull(rs);

            ResultSetMetaData meta = rs.getMetaData();

            assertNotNull(meta);

            assertEquals(2, meta.getColumnCount());

            assertEquals("METATEST", meta.getTableName(1).toUpperCase());
            assertEquals("DECIMAL_COL", meta.getColumnName(1).toUpperCase());
            assertEquals("DECIMAL_COL", meta.getColumnLabel(1).toUpperCase());
            assertEquals(DECIMAL, meta.getColumnType(1));
            assertEquals(meta.getColumnTypeName(1), "DECIMAL");
            assertEquals(meta.getColumnClassName(1), "java.math.BigDecimal");

            assertEquals("METATEST", meta.getTableName(2).toUpperCase());
            assertEquals("DATE_COL", meta.getColumnName(2).toUpperCase());
            assertEquals("DATE_COL", meta.getColumnLabel(2).toUpperCase());
            assertEquals(DATE, meta.getColumnType(2));
            assertEquals(meta.getColumnTypeName(2), "DATE");
            assertEquals(meta.getColumnClassName(2), "java.sql.Date");
        } finally {
            stmt.execute("DROP TABLE METATEST;");
        }
    }

    private void createMetaTable() {
        Ignite ignite = clusterNodes.get(0);

        TableDefinition metaTableDef = SchemaBuilders.tableBuilder("PUBLIC", "METATEST")
                .columns(
                        SchemaBuilders.column("DECIMAL_COL", ColumnType.decimalOf()).build(),
                        SchemaBuilders.column("DATE_COL", ColumnType.DATE).build(),
                        SchemaBuilders.column("ID", ColumnType.INT32).asNullable(false).build())
                .withPrimaryKey("ID")
                .build();

        ignite.tables().createTable(metaTableDef.canonicalName(), (tableChange) -> {
            SchemaConfigurationConverter.convert(metaTableDef, tableChange).changeReplicas(1).changePartitions(10);
        });
    }

    @Test
    public void testGetTables() throws Exception {
        DatabaseMetaData meta = conn.getMetaData();

        ResultSet rs = meta.getTables("IGNITE", "PUBLIC", "%", new String[]{"TABLE"});
        assertNotNull(rs);
        assertTrue(rs.next());
        assertEquals("TABLE", rs.getString("TABLE_TYPE"));
        assertEquals("ALLTYPES", rs.getString("TABLE_NAME"));
        assertTrue(rs.next());
        assertEquals("TABLE", rs.getString("TABLE_TYPE"));
        assertEquals("ORGANIZATION", rs.getString("TABLE_NAME"));
        assertTrue(rs.next());
        assertEquals("TABLE", rs.getString("TABLE_TYPE"));
        assertEquals("PERSON", rs.getString("TABLE_NAME"));

        rs = meta.getTables("IGNITE", "PUBLIC", "%", null);
        assertNotNull(rs);
        assertTrue(rs.next());
        assertEquals("TABLE", rs.getString("TABLE_TYPE"));
        assertEquals("ALLTYPES", rs.getString("TABLE_NAME"));

        rs = meta.getTables("IGNITE", "PUBLIC", "", new String[]{"WRONG"});
        assertFalse(rs.next());
    }

    @Test
    public void testGetColumns() throws Exception {
        DatabaseMetaData meta = conn.getMetaData();

        ResultSet rs = meta.getColumns("IGNITE", "PUBLIC", "PERSON", "%");

        checkPersonTableColumns(rs);

        rs = meta.getColumns(null, "PUBLIC", "PERSON", null);

        checkPersonTableColumns(rs);

        rs = meta.getColumns("IGNITE", "PUBLIC", "ORGANIZATION", "%");

        checkOrgTableColumns(rs);

        rs = meta.getColumns(null, "PUBLIC", "ORGANIZATION", null);

        checkOrgTableColumns(rs);
    }

    /**
     * Checks organisation table column names and types.
     *
     * @param rs ResultSet.
     * */
    private void checkOrgTableColumns(ResultSet rs) throws SQLException {
        assertNotNull(rs);

        Collection<String> names = new ArrayList<>();

        names.add("ID");
        names.add("NAME");
        names.add("BIGDATA");
        names.add("DUR");
        names.add("PER");

        int cnt = 0;

        while (rs.next()) {
            String name = rs.getString("COLUMN_NAME");

            assertTrue(names.remove(name));

            if ("ID".equals(name)) {
                assertEquals(INTEGER, rs.getInt("DATA_TYPE"));
                assertEquals(rs.getString("TYPE_NAME"), "INTEGER");
                assertEquals(0, rs.getInt("NULLABLE"));
            } else if ("NAME".equals(name)) {
                assertEquals(VARCHAR, rs.getInt("DATA_TYPE"));
                assertEquals(rs.getString("TYPE_NAME"), "VARCHAR");
                assertEquals(1, rs.getInt("NULLABLE"));
            } else if ("BIGDATA".equals(name)) {
                assertEquals(DECIMAL, rs.getInt("DATA_TYPE"));
                assertEquals(rs.getString("TYPE_NAME"), "DECIMAL");
                assertEquals(1, rs.getInt("NULLABLE"));
                assertEquals(10, rs.getInt("DECIMAL_DIGITS"));
                assertEquals(20, rs.getInt("COLUMN_SIZE"));
            }

            cnt++;
        }

        assertTrue(names.isEmpty());
        assertEquals(5, cnt);
    }

    /**
     * Checks person table column names and types.
     *
     * @param rs ResultSet.
     * */
    private void checkPersonTableColumns(ResultSet rs) throws SQLException {
        assertNotNull(rs);

        Collection<String> names = new ArrayList<>(3);

        names.add("NAME");
        names.add("AGE");
        names.add("ORGID");

        int cnt = 0;

        while (rs.next()) {
            String name = rs.getString("COLUMN_NAME");

            assertTrue(names.remove(name));

            if ("NAME".equals(name)) {
                assertEquals(VARCHAR, rs.getInt("DATA_TYPE"));
                assertEquals(rs.getString("TYPE_NAME"), "VARCHAR");
                assertEquals(1, rs.getInt("NULLABLE"));
            } else if ("AGE".equals(name)) {
                assertEquals(INTEGER, rs.getInt("DATA_TYPE"));
                assertEquals(rs.getString("TYPE_NAME"), "INTEGER");
                assertEquals(1, rs.getInt("NULLABLE"));
            } else if ("ORGID".equals(name)) {
                assertEquals(INTEGER, rs.getInt("DATA_TYPE"));
                assertEquals(rs.getString("TYPE_NAME"), "INTEGER");
                assertEquals(0, rs.getInt("NULLABLE"));

            }
            cnt++;
        }

        assertTrue(names.isEmpty());
        assertEquals(3, cnt);
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
        ResultSet rs = conn.getMetaData().getSchemas();

        Set<String> expectedSchemas = new HashSet<>(Arrays.asList("PUBLIC", "PUBLIC"));

        Set<String> schemas = new HashSet<>();

        while (rs.next()) {
            schemas.add(rs.getString(1));
        }

        assertEquals(schemas, expectedSchemas);
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
            assertEquals(rs.getString("COLUMN_NAME"), "ORGID");

            cnt++;
        }

        assertEquals(1, cnt);
    }

    @Test
    public void testGetAllPrimaryKeys() throws Exception {
        ResultSet rs = conn.getMetaData().getPrimaryKeys(null, null, null);

        Set<String> expectedPks = new HashSet<>(Arrays.asList(
                "PUBLIC.ORGANIZATION.PK_ORGANIZATION.ID",
                "PUBLIC.PERSON.PK_PERSON.ORGID", "PUBLIC.ALLTYPES.PK_ALLTYPES.ID"));

        Set<String> actualPks = new HashSet<>(expectedPks.size());

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

                assertEquals(VARCHAR, meta.getParameterType(1));
                assertEquals(ParameterMetaData.parameterNullableUnknown, meta.isNullable(1));
                assertEquals(Integer.MAX_VALUE, meta.getPrecision(1));

                assertEquals(INTEGER, meta.getParameterType(2));
                assertEquals(ParameterMetaData.parameterNullableUnknown, meta.isNullable(2));
            }

            // Updates.
            try (Connection conn = DriverManager.getConnection(URL)) {
                conn.setSchema("\"pers\"");

                PreparedStatement updateStmt = conn.prepareStatement("update Person p set orgId = 42 where p.name > ? and p.orgId > ?");

                ParameterMetaData meta = updateStmt.getParameterMetaData();

                assertNotNull(meta);

                assertEquals(2, meta.getParameterCount());

                assertEquals(VARCHAR, meta.getParameterType(1));
                assertEquals(ParameterMetaData.parameterNullableUnknown, meta.isNullable(1));
                assertEquals(Integer.MAX_VALUE, meta.getPrecision(1));

                assertEquals(INTEGER, meta.getParameterType(2));
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

                assertEquals(VARCHAR, meta.getParameterType(1));
                assertEquals(ParameterMetaData.parameterNullableUnknown, meta.isNullable(1));
                assertEquals(Integer.MAX_VALUE, meta.getPrecision(1));

                assertEquals(INTEGER, meta.getParameterType(2));
                assertEquals(ParameterMetaData.parameterNullableUnknown, meta.isNullable(2));

                assertEquals(VARCHAR, meta.getParameterType(3));
                assertEquals(ParameterMetaData.parameterNullableUnknown, meta.isNullable(3));
                assertEquals(Integer.MAX_VALUE, meta.getPrecision(3));

                assertEquals(INTEGER, meta.getParameterType(4));
                assertEquals(ParameterMetaData.parameterNullableUnknown, meta.isNullable(4));
            }
        }
    }

    /**
     * Check that parameters metadata throws correct exception on non-parsable statement.
     */
    @Test
    public void testParametersMetadataNegative() throws Exception {
        try (Connection conn = DriverManager.getConnection(URL)) {
            conn.setSchema("\"pers\"");

            PreparedStatement notCorrect = conn.prepareStatement("select * from NotExistingTable;");

            assertThrows(SQLException.class, notCorrect::getParameterMetaData, "Table \"NOTEXISTINGTABLE\" not found");
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

    /**
     * Check that lookup in the metadata have been performed using specified catalog name (that is neither {@code null} nor correct catalog
     * name), empty result set is returned.
     *
     * @param invalidCat catalog name that is not either
     */
    private void checkNoEntitiesFoundForCatalog(String invalidCat) throws SQLException {
        try (Connection conn = DriverManager.getConnection(URL)) {
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
