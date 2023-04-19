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

import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import org.hamcrest.MatcherAssert;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Verifies that SQL DML statements can use an explicit transaction using the jdbc API.
 */
public class ItJdbcTransactionTest extends AbstractJdbcSelfTest {
    /** Insert query. */
    private static final String SQL_INSERT = "insert into TEST values (%d, '%s')";

    /** Insert query for a prepared statement. */
    private static final String SQL_INSERT_PREPARED = "insert into TEST values (?, ?)";

    @BeforeAll
    public static void beforeClass() throws Exception {
        try (Statement statement = conn.createStatement()) {
            statement.executeUpdate("create table TEST(ID int primary key, NAME varchar(20));");
        }
    }

    @AfterEach
    public void afterEach() throws Exception {
        try (Statement statement = conn.createStatement()) {
            statement.executeUpdate("delete from TEST;");
        }
    }

    /**
     * Tests {@link Connection#setAutoCommit(boolean)} and {@link Connection#getAutoCommit()} methods behaviour.
     * <ul>
     * <li>Auto-commit mode is enabled by default.
     * <li>If {@code setAutoCommit} is called and the auto-commit mode is not changed, the call is a no-op.
     * <li>If {@code setAutoCommit} is called during transaction and auto-commit mode changed then active transaction is committed.
     * <li>Call to {@code setAutoCommit} or {@code getAutoCommit} on a closed connection throws exception.
     * </ul>
     *
     * @throws SQLException If failed.
     */
    @Test
    public void testAutoCommitModeChange() throws SQLException {
        try (Connection conn = DriverManager.getConnection(URL)) {
            assertTrue(conn.getAutoCommit());

            int expRowsCount = 3;
            String sqlUpdate = "insert into TEST (ID) values (1), (2), (3)";

            try (Statement stmt = conn.createStatement()) {
                conn.setAutoCommit(false);

                assertEquals(expRowsCount, stmt.executeUpdate(sqlUpdate));
                assertEquals(expRowsCount, rowsCount(conn));
                assertEquals(0, rowsCount(AbstractJdbcSelfTest.conn));

                // Ensures that switching to the same mode will not change anything.
                conn.setAutoCommit(false);
                assertEquals(expRowsCount, rowsCount(conn));
                assertEquals(0, rowsCount(AbstractJdbcSelfTest.conn));

                // Verify that switching to auto-commit mode again commits started transaction.
                conn.setAutoCommit(true);
                assertEquals(expRowsCount, rowsCount(AbstractJdbcSelfTest.conn));
            }

            conn.close();

            // Exception when called on closed connection
            checkConnectionClosed(conn::getAutoCommit);
            checkConnectionClosed(() -> conn.setAutoCommit(true));
            checkConnectionClosed(() -> conn.setAutoCommit(false));
        }
    }

    /**
     * Tests {@link Connection#commit()} method behaviour.
     *
     * <p>Calling {@code commit} is expected to throw an exception if called in auto-commit mode or on a closed connection,
     *
     * @throws SQLException If failed.
     */
    @Test
    public void testCommit() throws Exception {
        try (Connection conn = DriverManager.getConnection(URL)) {
            SQLException ex = assertThrows(SQLException.class, conn::commit);
            MatcherAssert.assertThat(ex.getMessage(), containsString("Transaction cannot be committed explicitly in auto-commit mode."));

            conn.setAutoCommit(false);
            conn.commit();

            conn.close();

            // Exception when called on closed connection.
            checkConnectionClosed(conn::commit);
        }
    }

    /**
     * Tests {@link Connection#rollback()} method behaviour.
     *
     * <p>Calling {@code rollback} is expected to throw an exception if called in auto-commit mode or on a closed connection,
     *
     * @throws SQLException If failed.
     */
    @Test
    public void testRollback() throws Exception {
        try (Connection conn = DriverManager.getConnection(URL)) {
            // Should not be called in auto-commit mode.
            SQLException ex = assertThrows(SQLException.class, conn::rollback);
            MatcherAssert.assertThat(ex.getMessage(), containsString("Transaction cannot be rolled back explicitly in auto-commit mode."));

            conn.setAutoCommit(false);
            conn.rollback();

            conn.close();

            // Exception when called on closed connection.
            checkConnectionClosed(conn::rollback);
        }
    }

    /**
     * Ensures that {@link Statement#executeUpdate(String)} supports explicit transaction.
     *
     * <p>It is expected that in non auto-commit mode, data updates can be rolled back and
     * are invisible outside of an active transaction until they are committed.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testExecuteUpdate() throws Exception {
        checkUpdate((conn, start, cnt) -> {
            try (Statement stmt = conn.createStatement()) {
                for (int i = start; i < start + cnt; i++) {
                    stmt.executeUpdate(String.format(SQL_INSERT, i, "name-" + i));
                }
            }
        });
    }

    /**
     * Ensures that {@link PreparedStatement#executeUpdate()} supports explicit transaction.
     *
     * <p>It is expected that in non auto-commit mode, data updates can be rolled back and
     * are invisible outside of an active transaction until they are committed.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testExecuteUpdatePrepared() throws Exception {
        checkUpdate((conn, start, cnt) -> {
            try (PreparedStatement pstmt = conn.prepareStatement(SQL_INSERT_PREPARED)) {
                for (int i = start; i < start + cnt; i++) {
                    pstmt.setInt(1, i);
                    pstmt.setString(2, "name-" + i);

                    pstmt.executeUpdate();
                }
            }
        });
    }

    /**
     * Ensures that {@link Statement#executeBatch()} supports explicit transaction.
     *
     * <p>It is expected that in non auto-commit mode, data updates can be rolled back and
     * are invisible outside of an active transaction until they are committed.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testBatch() throws Exception {
        checkUpdate((conn, start, cnt) -> {
            try (Statement stmt = conn.createStatement()) {
                for (int i = start; i < start + cnt; i++) {
                    stmt.addBatch(String.format(SQL_INSERT, i, "name-" + i));
                }

                stmt.executeBatch();
            }
        });
    }

    /**
     * Ensures that {@link PreparedStatement#executeBatch()} supports explicit transaction.
     *
     * <p>It is expected that in non auto-commit mode, data updates can be rolled back and
     * are invisible outside of an active transaction until they are committed.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testBatchPrepared() throws Exception {
        checkUpdate((conn, start, cnt) -> {
            try (PreparedStatement pstmt = conn.prepareStatement(SQL_INSERT_PREPARED)) {
                for (int i = start; i < start + cnt; i++) {
                    pstmt.setInt(1, i);
                    pstmt.setString(2, "name-" + i);

                    pstmt.addBatch();
                }

                pstmt.executeBatch();
            }
        });
    }

    /**
     * Ensures that data updates made in an explicit transaction can be rolled back and are
     * not visible outside of an active transaction until they are committed.
     *
     * @param batchInsert Batch insert operation.
     * @throws SQLException If failed.
     */
    private void checkUpdate(TestJdbcBatchInsertOperation batchInsert) throws SQLException {
        // Check rollback.
        checkTxResult(batchInsert, false);

        // Check commit.
        checkTxResult(batchInsert, true);
    }

    /**
     * Performs update operations and checks the result of committing or rolling back a transaction.
     *
     * @param batchInsert Batch insert operation.
     * @param commit {@code True} to check transaction commit, {@code false} to check rollback.
     * @throws SQLException If failed.
     */
    private void checkTxResult(TestJdbcBatchInsertOperation batchInsert, boolean commit) throws SQLException {
        try (Connection conn = DriverManager.getConnection(URL)) {
            conn.setAutoCommit(false);

            int rowsCnt = 5;
            int iterations = 2;

            for (int id = 0; id < iterations; id++) {
                int offset = rowsCnt * id;

                batchInsert.run(conn, offset, rowsCnt);
                assertEquals(offset + rowsCnt, rowsCount(conn));

                // Ensures that the changes are not visible outside of the transaction.
                assertEquals(0, rowsCount(AbstractJdbcSelfTest.conn));
            }

            if (commit) {
                conn.commit();
            } else {
                conn.rollback();
            }

            int expCnt = commit ? iterations * rowsCnt : 0;

            assertEquals(expCnt, rowsCount(conn));
            assertEquals(expCnt, rowsCount(AbstractJdbcSelfTest.conn));
        }
    }

    /**
     * Gets the number of rows in the test table using specified connection.
     *
     * @param conn Active connection.
     * @return Number of rows in the test table.
     * @throws SQLException If failed.
     */
    private int rowsCount(Connection conn) throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            try (ResultSet rs = stmt.executeQuery("select count(*) from TEST")) {
                rs.next();

                return rs.getInt(1);
            }
        }
    }

    @FunctionalInterface
    private interface TestJdbcBatchInsertOperation {
        void run(Connection conn, Integer startRowId, Integer rowsCount) throws SQLException;
    }
}
