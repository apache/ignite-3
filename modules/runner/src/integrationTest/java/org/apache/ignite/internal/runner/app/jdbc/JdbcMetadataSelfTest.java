/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.runner.app.jdbc;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import org.apache.ignite.internal.schema.configuration.SchemaConfigurationConverter;
import org.apache.ignite.schema.ColumnType;
import org.apache.ignite.schema.SchemaBuilders;
import org.apache.ignite.schema.SchemaTable;
import org.apache.ignite.table.Table;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import static java.sql.Types.INTEGER;
import static java.sql.Types.VARCHAR;
import static java.sql.Types.DECIMAL;
import static java.sql.Types.DATE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Metadata tests.
 */
public class JdbcMetadataSelfTest extends AbstractJdbcSelfTest {
    /**
     * Create the connection ant statement.
     *
     * @throws Exception if failed.
     */
    @BeforeEach
    public void beforeTest() {
        // Create table on node 0.
        SchemaTable persTbl = SchemaBuilders.tableBuilder("pers", "PERSON").columns(
            SchemaBuilders.column("NAME", ColumnType.string()).asNullable().build(),
            SchemaBuilders.column("AGE", ColumnType.INT32).asNullable().build(),
            SchemaBuilders.column("ORGID", ColumnType.INT32).asNonNull().build()
        ).withPrimaryKey("ORGID").build();

        SchemaTable orgTbl = SchemaBuilders.tableBuilder("org", "ORGANIZATION").columns(
            SchemaBuilders.column("ID", ColumnType.INT32).asNonNull().build(),
            SchemaBuilders.column("NAME", ColumnType.string()).asNullable().build()
        ).withPrimaryKey("ID").build();

        if (clusterNodes.get(0).tables().table(persTbl.canonicalName()) != null)
            return;

        clusterNodes.get(0).tables().createTable(persTbl.canonicalName(), tblCh ->
            SchemaConfigurationConverter.convert(persTbl, tblCh)
                .changeReplicas(1)
                .changePartitions(10)
        );

        clusterNodes.get(0).tables().createTable(orgTbl.canonicalName(), tblCh ->
            SchemaConfigurationConverter.convert(orgTbl, tblCh)
                .changeReplicas(1)
                .changePartitions(10)
        );

        Table tbl1 = clusterNodes.get(0).tables().table(persTbl.canonicalName());
        Table tbl2 = clusterNodes.get(0).tables().table(orgTbl.canonicalName());

        tbl1.insert(tbl1.tupleBuilder().set("ORGID", 1).set("NAME", "111").set("AGE", 111).build());
        tbl2.insert(tbl2.tupleBuilder().set("ID", 1).set("NAME", "AAA").build());
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    @Disabled
    public void testResultSetMetaData() throws Exception {
        Statement stmt = DriverManager.getConnection(URL).createStatement();

        ResultSet rs = stmt.executeQuery(
            "select p.name, o.id as orgId from \"pers\".Person p, \"org\".Organization o where p.orgId = o.id");

        assert rs != null;

        ResultSetMetaData meta = rs.getMetaData();

        assert meta != null;

        assert meta.getColumnCount() == 2;

        assert "Person".equalsIgnoreCase(meta.getTableName(1));
        assert "name".equalsIgnoreCase(meta.getColumnName(1));
        assert "name".equalsIgnoreCase(meta.getColumnLabel(1));
        assert meta.getColumnType(1) == VARCHAR;
        assert "VARCHAR".equals(meta.getColumnTypeName(1));
        assert "java.lang.String".equals(meta.getColumnClassName(1));

        assert "Organization".equalsIgnoreCase(meta.getTableName(2));
        assert "orgId".equalsIgnoreCase(meta.getColumnName(2));
        assert "orgId".equalsIgnoreCase(meta.getColumnLabel(2));
        assert meta.getColumnType(2) == INTEGER;
        assert "INTEGER".equals(meta.getColumnTypeName(2));
        assert "java.lang.Integer".equals(meta.getColumnClassName(2));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    @Disabled
    public void testDecimalAndDateTypeMetaData() throws Exception {
        try (Connection conn = DriverManager.getConnection(URL)) {
            Statement stmt = conn.createStatement();

            ResultSet rs = stmt.executeQuery(
                    "select t.decimal, t.date from \"metaTest\".MetaTest as t");

            assert rs != null;

            ResultSetMetaData meta = rs.getMetaData();

            assert meta != null;

            assert meta.getColumnCount() == 2;

            assert "METATEST".equalsIgnoreCase(meta.getTableName(1));
            assert "DECIMAL".equalsIgnoreCase(meta.getColumnName(1));
            assert "DECIMAL".equalsIgnoreCase(meta.getColumnLabel(1));
            assert meta.getColumnType(1) == DECIMAL;
            assert "DECIMAL".equals(meta.getColumnTypeName(1));
            assert "java.math.BigDecimal".equals(meta.getColumnClassName(1));

            assert "METATEST".equalsIgnoreCase(meta.getTableName(2));
            assert "DATE".equalsIgnoreCase(meta.getColumnName(2));
            assert "DATE".equalsIgnoreCase(meta.getColumnLabel(2));
            assert meta.getColumnType(2) == DATE;
            assert "DATE".equals(meta.getColumnTypeName(2));
            assert "java.sql.Date".equals(meta.getColumnClassName(2));
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testGetTables() throws Exception {
        try (Connection conn = DriverManager.getConnection(URL)) {
            DatabaseMetaData meta = conn.getMetaData();

            ResultSet rs = meta.getTables("IGNITE", "pers", "%", new String[]{"TABLE"});
            assertNotNull(rs);
            assertTrue(rs.next());
            assertEquals("TABLE", rs.getString("TABLE_TYPE"));
            assertEquals("PERSON", rs.getString("TABLE_NAME"));

            rs = meta.getTables("IGNITE", "org", "%", new String[]{"TABLE"});
            assertNotNull(rs);
            assertTrue(rs.next());
            assertEquals("TABLE", rs.getString("TABLE_TYPE"));
            assertEquals("ORGANIZATION", rs.getString("TABLE_NAME"));

            rs = meta.getTables("IGNITE", "pers", "%", null);
            assertNotNull(rs);
            assertTrue(rs.next());
            assertEquals("TABLE", rs.getString("TABLE_TYPE"));
            assertEquals("PERSON", rs.getString("TABLE_NAME"));

            rs = meta.getTables("IGNITE", "org", "%", null);
            assertNotNull(rs);
            assertTrue(rs.next());
            assertEquals("TABLE", rs.getString("TABLE_TYPE"));
            assertEquals("ORGANIZATION", rs.getString("TABLE_NAME"));

            rs = meta.getTables("IGNITE", "PUBLIC", "", new String[]{"WRONG"});
            assertFalse(rs.next());
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testGetColumns() throws Exception {
        try (Connection conn = DriverManager.getConnection(URL)) {
            DatabaseMetaData meta = conn.getMetaData();

            ResultSet rs = meta.getColumns("IGNITE", "pers", "PERSON", "%");

            assert rs != null;

            Collection<String> names = new ArrayList<>(2);

            names.add("NAME");
            names.add("AGE");
            names.add("ORGID");

            int cnt = 0;

            while (rs.next()) {
                String name = rs.getString("COLUMN_NAME");

                assert names.remove(name);

                if ("NAME".equals(name)) {
                    assert rs.getInt("DATA_TYPE") == VARCHAR;
                    assert "VARCHAR".equals(rs.getString("TYPE_NAME"));
                    assert rs.getInt("NULLABLE") == 1;
                } else if ("AGE".equals(name)) {
                    assert rs.getInt("DATA_TYPE") == INTEGER;
                    assert "INTEGER".equals(rs.getString("TYPE_NAME"));
                    assert rs.getInt("NULLABLE") == 1;
                } else if ("ORGID".equals(name)) {
                    assert rs.getInt("DATA_TYPE") == INTEGER;
                    assert "INTEGER".equals(rs.getString("TYPE_NAME"));
                    assert rs.getInt("NULLABLE") == 0;
                }
                cnt++;
            }

            assert names.isEmpty();
            assert cnt == 3;

            rs = meta.getColumns("IGNITE", "org", "ORGANIZATION", "%");

            assert rs != null;

            names.add("ID");
            names.add("NAME");

            cnt = 0;

            while (rs.next()) {
                String name = rs.getString("COLUMN_NAME");

                assert names.remove(name);

                if ("ID".equals(name)) {
                    assert rs.getInt("DATA_TYPE") == INTEGER;
                    assert "INTEGER".equals(rs.getString("TYPE_NAME"));
                    assert rs.getInt("NULLABLE") == 0;
                } else if ("NAME".equals(name)) {
                    assert rs.getInt("DATA_TYPE") == VARCHAR;
                    assert "VARCHAR".equals(rs.getString("TYPE_NAME"));
                    assert rs.getInt("NULLABLE") == 1;
                }

                cnt++;
            }

            assert names.isEmpty();
            assert cnt == 2;
        }
    }

    /**
     * Check JDBC support flags.
     */
    @Test
    public void testCheckSupports() throws SQLException {
        try (Connection conn = DriverManager.getConnection(URL)) {
            DatabaseMetaData meta = conn.getMetaData();

            assertTrue(meta.supportsANSI92EntryLevelSQL());
            assertTrue(meta.supportsAlterTableWithAddColumn());
            assertTrue(meta.supportsAlterTableWithDropColumn());
            assertTrue(meta.nullPlusNonNullIsNull());
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testSchemasMetadata() throws Exception {
        try (Connection conn = DriverManager.getConnection(URL)) {
            ResultSet rs = conn.getMetaData().getSchemas();

            Set<String> expectedSchemas = new HashSet<>(Arrays.asList("pers", "org"));

            Set<String> schemas = new HashSet<>();

            while (rs.next())
                schemas.add(rs.getString(1));

            assert expectedSchemas.equals(schemas) : "Unexpected schemas: " + schemas +
                ". Expected schemas: " + expectedSchemas;
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testEmptySchemasMetadata() throws Exception {
        try (Connection conn = DriverManager.getConnection(URL)) {
            ResultSet rs = conn.getMetaData().getSchemas(null, "qqq");

            assert !rs.next() : "Empty result set is expected";
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPrimaryKeyMetadata() throws Exception {
        try (Connection conn = DriverManager.getConnection(URL);
             ResultSet rs = conn.getMetaData().getPrimaryKeys(null, "pers", "PERSON")) {

            int cnt = 0;

            while (rs.next()) {
                assert "ORGID".equals(rs.getString("COLUMN_NAME"));

                cnt++;
            }

            assert cnt == 1;
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testGetAllPrimaryKeys() throws Exception {
        try (Connection conn = DriverManager.getConnection(URL)) {
            ResultSet rs = conn.getMetaData().getPrimaryKeys(null, null, null);

            Set<String> expectedPks = new HashSet<>(Arrays.asList(
                "org.ORGANIZATION.PK_ORGANIZATION.ID",
                "pers.PERSON.PK_PERSON.ORGID"));

            Set<String> actualPks = new HashSet<>(expectedPks.size());

            while (rs.next()) {
                actualPks.add(rs.getString("TABLE_SCHEM") +
                    '.' + rs.getString("TABLE_NAME") +
                    '.' + rs.getString("PK_NAME") +
                    '.' + rs.getString("COLUMN_NAME"));
            }

            assertEquals(expectedPks, actualPks, "Metadata contains unexpected primary keys info.");
        }
    }
}
