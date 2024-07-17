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

import static org.apache.ignite.internal.jdbc.proto.SqlStateCode.CLIENT_CONNECTION_FAILED;
import static org.apache.ignite.internal.jdbc.proto.SqlStateCode.INVALID_TRANSACTION_LEVEL;
import static org.apache.ignite.internal.jdbc.proto.SqlStateCode.UNSUPPORTED_OPERATION;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import org.apache.ignite.jdbc.util.JdbcTestUtils;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

/**
 * Test SQLSTATE codes propagation with thin client driver.
 */
public class ItJdbcErrorsSelfTest extends ItJdbcErrorsAbstractSelfTest {
    /** {@inheritDoc} */
    @Override protected Connection getConnection() throws SQLException {
        return DriverManager.getConnection(URL);
    }

    /**
     * Test error code for the case when connection string is fine but client can't reach server
     * due to <b>communication problems</b> (not due to clear misconfiguration).
     */
    @Test
    public void testConnectionError() {
        checkErrorState(() -> DriverManager.getConnection("jdbc:ignite:thin://unknown.host?connectionTimeout=1000"),
                CLIENT_CONNECTION_FAILED, "Failed to connect to server");
    }

    /**
     * Test that execution of erroneous queries are not stopping execution.
     * Also check correctness of exception messages.
     *
     * @throws SQLException If connection can`t be established.
     */
    @Test
    public void processMixedQueries() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.executeUpdate(
                    "CREATE TABLE CITIES ("
                            + "ID   INT PRIMARY KEY,"
                            + "NAME VARCHAR)"
            );

            JdbcTestUtils.assertThrowsSqlException("Failed to parse query", () -> stmt.executeUpdate("non sql stuff"));
        }

        try (Statement stmt = conn.createStatement()) {
            stmt.executeUpdate(
                    "CREATE TABLE ACCOUNTS ("
                            + "    ACCOUNT_ID INT PRIMARY KEY,"
                            + "    CITY_ID    INT,"
                            + "    FIRST_NAME VARCHAR,"
                            + "    LAST_NAME  VARCHAR,"
                            + "    BALANCE    DOUBLE)"
            );

            JdbcTestUtils.assertThrowsSqlException(
                    "Table with name 'PUBLIC.ACCOUNTS' already exists",
                    () -> stmt.executeUpdate("CREATE TABLE ACCOUNTS (ACCOUNT_ID INT PRIMARY KEY)")

            );
        }
    }

    /**
     * Test error code for the case when connection string is a mess.
     */
    @Test
    public void testInvalidConnectionStringFormat() {
        checkErrorState(() -> DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1:1000000"),
                CLIENT_CONNECTION_FAILED, "invalid port 1000000");
    }

    /**
     * Test error code for the case when user attempts to set an invalid isolation level to a connection.
     */
    @SuppressWarnings("MagicConstant")
    @Test
    public void testInvalidIsolationLevel() {
        checkErrorState(() -> conn.setTransactionIsolation(1000),
                INVALID_TRANSACTION_LEVEL, "Invalid transaction isolation level.");
    }

    /**
     * Test error code for the case when error is caused on batch execution.
     *
     * @throws SQLException if failed.
     */
    @Test
    public void testBatchUpdateException() throws SQLException {

        stmt.executeUpdate("CREATE TABLE test2 (id int primary key, val varchar)");

        stmt.addBatch("insert into test2 (id, val) values (1, 'val1')");
        stmt.addBatch("insert into test2 (id, val) values (2, 'val2')");
        stmt.addBatch("insert into test2 (id1, val1) values (3, 'val3')");

        BatchUpdateException batchException = JdbcTestUtils.assertThrowsSqlException(
                BatchUpdateException.class,
                "Unknown target column 'ID1'",
                () -> stmt.executeBatch());

        assertEquals(2, batchException.getUpdateCounts().length);

        assertArrayEquals(new int[]{1, 1}, batchException.getUpdateCounts());
    }

    /**
     * Test error code when trying to execute DDL query within a transaction.
     *
     * @throws SQLException if failed.
     */
    @Test
    public void testDdlWithDisabledAutoCommit() throws SQLException {
        conn.setAutoCommit(false);

        try (Statement stmt = conn.createStatement()) {
            JdbcTestUtils.assertThrowsSqlException("DDL doesn't support transactions.",
                    () -> stmt.executeUpdate("CREATE TABLE test2 (id int primary key, val varchar)"));
        }
    }

    /**
     * Check that unsupported explain of update operation causes Exception on the driver side with correct code and
     * message.
     */
    @Test
    @Disabled("https://issues.apache.org/jira/browse/IGNITE-15247")
    public void testExplainUpdatesUnsupported() {
        checkErrorState(() -> {
            stmt.executeUpdate("CREATE TABLE TEST_EXPLAIN (ID INT PRIMARY KEY, VAL INT)");

            stmt.executeUpdate("EXPLAIN INSERT INTO TEST_EXPLAIN VALUES (1, 2)");
        }, UNSUPPORTED_OPERATION, "Explains of update queries are not supported.");
    }
}
