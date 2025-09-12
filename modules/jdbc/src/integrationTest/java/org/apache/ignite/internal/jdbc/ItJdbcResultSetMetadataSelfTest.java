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
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.jdbc.AbstractJdbcSelfTest;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link ResultSet}.
 */
public class ItJdbcResultSetMetadataSelfTest extends AbstractJdbcSelfTest {

    @BeforeAll
    public void beforeAll() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE person(name VARCHAR(32), age INT, orgid INT PRIMARY KEY);");
            stmt.execute("CREATE TABLE organization(id INT PRIMARY KEY, name VARCHAR, bigdata DECIMAL(20, 10));");
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
        // This delays make this test case less susceptible to https://issues.apache.org/jira/browse/IGNITE-25935
        // Mandatory nodes was excluded from mapping: []
        // TODO https://issues.apache.org/jira/browse/IGNITE-25935 Remove this delay.
        TimeUnit.SECONDS.sleep(5);

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
}
