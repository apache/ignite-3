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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * Multiple JDBC connections test.
 */
public class ItJdbcMultipleConnectionsTest extends AbstractJdbcSelfTest {
    private static final String URL1 = "jdbc:ignite:thin://127.0.0.1:10800";
    private static final String URL2 = "jdbc:ignite:thin://127.0.0.1:10801";

    @Override
    protected int initialNodes() {
        return 2;
    }

    @AfterEach
    public void dropTable() {
        sql("DROP TABLE IF EXISTS T1");
    }

    @Test
    public void testMultipleConnectionsSingleServer() throws SQLException {
        try (Connection conn1 = DriverManager.getConnection(URL1)) {
            try (Connection conn2 = DriverManager.getConnection(URL1)) {
                checkUpdatesVisibility(conn1, conn2);
            }
        }
    }

    @Test
    public void testMultipleConnectionsMultipleServer() throws SQLException {
        try (Connection conn1 = DriverManager.getConnection(URL1)) {
            try (Connection conn2 = DriverManager.getConnection(URL2)) {
                checkUpdatesVisibility(conn1, conn2);
            }
        }
    }

    private static void checkUpdatesVisibility(Connection conn1, Connection conn2) throws SQLException {
        try (Statement stmt1 = conn1.createStatement()) {
            try (Statement stmt2 = conn2.createStatement()) {
                stmt1.executeUpdate("CREATE TABLE T1(id INT PRIMARY KEY)");

                int rowsCount = 100;
                int i = 0;

                do {
                    assertThat(stmt1.executeUpdate("INSERT INTO T1 VALUES (" + i + ")"), is(1));

                    try (ResultSet rs = stmt2.executeQuery("SELECT count(*) FROM T1 WHERE id >= 0")) {
                        assertTrue(rs.next());
                        assertThat("i=" + i, rs.getLong(1), is(i + 1L));
                    }
                } while (i++ < rowsCount);
            }
        }
    }
}
