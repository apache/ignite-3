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

package org.apache.ignite.jdbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

/**
 * Tests that involve non-default schema and DDL schema commands.
 */
public class ItJdbcSchemaTest extends AbstractJdbcSelfTest {

    @AfterEach
    public void dropSchemas() {
        dropAllSchemas();
    }

    @Test
    public void createSchema() throws SQLException  {
        try (Statement stmt = conn.createStatement()) {
            stmt.executeUpdate("CREATE SCHEMA schema1");

            stmt.executeUpdate("CREATE TABLE schema1.t1(ID INT PRIMARY KEY, val INT)");
            stmt.executeUpdate("INSERT INTO schema1.t1 VALUES (1, 1)");

            try (ResultSet rs = stmt.executeQuery("SELECT * FROM schema1.T1")) {
                assertTrue(rs.next());
                assertEquals(1, rs.getLong(1));

                ResultSetMetaData metaData = rs.getMetaData();
                assertEquals("SCHEMA1", metaData.getSchemaName(1));
            }
        }
    }

    @Test
    public void dropSchema() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.executeUpdate("CREATE SCHEMA schema1");

            stmt.executeUpdate("CREATE TABLE schema1.t1(ID INT PRIMARY KEY, val INT)");
            try (ResultSet rs = stmt.executeQuery("SELECT * FROM schema1.T1")) {
                assertFalse(rs.next());
            }

            stmt.executeUpdate("DROP SCHEMA schema1 CASCADE");
        }
    }

    @Test
    public void useSchemaWithQuery() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            // Schema 1
            stmt.executeUpdate("CREATE SCHEMA schema1");
            stmt.executeUpdate("CREATE TABLE schema1.t1(ID INT PRIMARY KEY, val INT)");
            stmt.executeUpdate("INSERT INTO schema1.t1 VALUES (1, 1)");

            // Schema 2
            stmt.executeUpdate("CREATE SCHEMA schema2");
            stmt.executeUpdate("CREATE TABLE schema2.t1(ID INT PRIMARY KEY, val INT)");
            stmt.executeUpdate("INSERT INTO schema2.t1 VALUES (2, 2)");
        }

        // Check schema1
        conn.setSchema("schema1");
        try (Statement stmt = conn.createStatement()) {
            try (ResultSet rs = stmt.executeQuery("SELECT * FROM T1")) {
                assertTrue(rs.next());
                assertEquals(1, rs.getLong(1));

                ResultSetMetaData metaData = rs.getMetaData();
                assertEquals("SCHEMA1", metaData.getSchemaName(1));
            }
        }

        // Check schema2
        conn.setSchema("schema2");
        try (Statement stmt = conn.createStatement()) {
            try (ResultSet rs = stmt.executeQuery("SELECT * FROM T1")) {
                assertTrue(rs.next());
                assertEquals(2, rs.getLong(1));

                ResultSetMetaData metaData = rs.getMetaData();
                assertEquals("SCHEMA2", metaData.getSchemaName(1));
            }
        }
    }

    @Test
    public void useSchemaWithUpdate() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            // Schema 1
            stmt.executeUpdate("CREATE SCHEMA schema1");
            stmt.executeUpdate("CREATE TABLE schema1.t1(ID INT PRIMARY KEY, val INT)");

            // Schema 2
            stmt.executeUpdate("CREATE SCHEMA schema2");
            stmt.executeUpdate("CREATE TABLE schema2.t1(ID INT PRIMARY KEY, val INT)");
        }

        // Update schema1
        conn.setSchema("schema1");
        try (Statement stmt = conn.createStatement()) {
            stmt.executeUpdate("INSERT INTO t1 VALUES (1, 1), (2, 2)");
        }

        // Update schema2
        conn.setSchema("schema2");
        try (Statement stmt = conn.createStatement()) {
            stmt.executeUpdate("INSERT INTO t1 VALUES (1, 1), (2, 2), (3, 3)");
        }

        // Check schema 1
        try (ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM schema1.T1")) {
            assertTrue(rs.next());
            assertEquals(2, rs.getLong(1));
        }

        // Check schema 2
        try (ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM schema2.T1")) {
            assertTrue(rs.next());
            assertEquals(3, rs.getLong(1));
        }
    }

    @Test
    @Disabled("https://issues.apache.org/jira/browse/IGNITE-26143")
    public void useSchemaWithBatch() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.executeUpdate("CREATE SCHEMA schema1");
            stmt.executeUpdate("CREATE TABLE schema1.t1(ID INT PRIMARY KEY, val INT)");

            stmt.executeUpdate("CREATE SCHEMA schema2");
            stmt.executeUpdate("CREATE TABLE schema2.t1(ID INT PRIMARY KEY, val INT)");
        }

        // Insert into schema 1
        conn.setSchema("schema1");
        try (Statement stmt = conn.createStatement()) {
            stmt.addBatch("INSERT INTO t1 VALUES (1, 1), (2, 1)");
            stmt.addBatch("INSERT INTO t1 VALUES (3, 1), (4, 1)");
            stmt.executeBatch();
        }

        // Insert into schema 2
        conn.setSchema("schema2");
        try (Statement stmt = conn.createStatement()) {
            stmt.addBatch("INSERT INTO t1 VALUES (1, 2)");
            stmt.addBatch("INSERT INTO t1 VALUES (2, 2), (3, 2)");
            stmt.executeBatch();
        }

        // Check schema 1
        try (ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM schema1.T1")) {
            assertTrue(rs.next());
            assertEquals(4, rs.getLong(1));
        }

        // Check schema 2
        try (ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM schema2.T1")) {
            assertTrue(rs.next());
            assertEquals(3, rs.getLong(1));
        }
    }

    @Test
    public void quotedSchemaTest() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.executeUpdate("CREATE SCHEMA \"ScheMa1\"");

            stmt.executeUpdate("CREATE TABLE \"ScheMa1\".t1(ID INT PRIMARY KEY, val INT)");
            stmt.executeUpdate("INSERT INTO \"ScheMa1\".t1 VALUES (1, 1)");

            try (ResultSet rs = stmt.executeQuery("SELECT * FROM \"ScheMa1\".T1")) {
                assertTrue(rs.next());
                assertEquals(1, rs.getLong(1));

                ResultSetMetaData metaData = rs.getMetaData();
                assertEquals("ScheMa1", metaData.getSchemaName(1));
            }

            stmt.executeUpdate("DROP TABLE \"ScheMa1\".t1");
            stmt.executeUpdate("DROP SCHEMA \"ScheMa1\"");

            stmt.executeUpdate("CREATE SCHEMA \"ScheMa2\"");
            stmt.executeUpdate("CREATE TABLE \"ScheMa2\".t1(ID INT PRIMARY KEY, val INT)");
            stmt.executeUpdate("DROP SCHEMA \"ScheMa2\" CASCADE");
        }
    }

    @Test
    public void quotedSchemaAndTableTest() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.executeUpdate("CREATE SCHEMA \"Sche Ma1\"");

            stmt.executeUpdate("CREATE TABLE \"Sche Ma1\".\"Ta ble1\"(ID INT PRIMARY KEY, val INT)");
            stmt.executeUpdate("INSERT INTO \"Sche Ma1\".\"Ta ble1\" VALUES (1, 1)");

            try (ResultSet rs = stmt.executeQuery("SELECT * FROM \"Sche Ma1\".\"Ta ble1\"")) {
                assertTrue(rs.next());
                assertEquals(1, rs.getLong(1));

                ResultSetMetaData metaData = rs.getMetaData();
                assertEquals("Ta ble1", metaData.getTableName(1));
                assertEquals("Sche Ma1", metaData.getSchemaName(1));
            }

            stmt.executeUpdate("DROP TABLE \"Sche Ma1\".\"Ta ble1\"");
            stmt.executeUpdate("DROP SCHEMA \"Sche Ma1\"");
        }
    }
}
