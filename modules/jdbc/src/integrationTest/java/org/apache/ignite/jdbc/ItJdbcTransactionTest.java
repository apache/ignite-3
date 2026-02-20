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

import static org.apache.ignite.internal.TestWrappers.unwrapIgniteImpl;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureExceptionMatcher.willThrowFast;
import static org.apache.ignite.jdbc.util.JdbcTestUtils.assertThrowsSqlException;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.internal.testframework.IgniteTestUtils;
import org.awaitility.Awaitility;
import org.hamcrest.Matcher;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

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
     * Ensures that an active transaction does not end after an attempt to execute a DDL statement within this transaction.
     *
     * <p>DDL statements do not currently support transactions, so an error should occur when attempting
     * to execute DDL within a transaction. And it is expected that a previously started transaction
     * will not be completed when this error occurs.
     *
     * @throws SQLException If failed.
     */
    @Test
    public void testTransactionSurvivesDdlError() throws SQLException {
        try (Connection conn = DriverManager.getConnection(URL)) {
            conn.setAutoCommit(false);

            try (Statement stmt = conn.createStatement()) {
                // Starting transaction.
                stmt.executeUpdate("insert into TEST (ID) values (1)");

                assertThrowsSqlException("DDL doesn't support transactions.", () -> stmt.executeUpdate("drop table TEST"));

                assertEquals(1, rowsCount(conn));
                assertEquals(0, rowsCount(ItJdbcTransactionTest.conn));

                conn.commit();
                assertEquals(1, rowsCount(ItJdbcTransactionTest.conn));
            }
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
        checkRollbackAndCommit((conn, start, cnt) -> {
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
        checkRollbackAndCommit((conn, start, cnt) -> {
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
        checkRollbackAndCommit((conn, start, cnt) -> {
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
        checkRollbackAndCommit((conn, start, cnt) -> {
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
     * Ensures that explicit commits in auto-commit mode are not allowed.
     */
    @Test
    public void testCommitInAutoCommitMode() throws SQLException  {
        try (Connection conn = DriverManager.getConnection(URL)) {
            conn.setAutoCommit(true);

            assertThrowsSqlException("Transaction cannot be committed explicitly in auto-commit mode.", conn::commit);
        }
    }

    /**
     * Ensures that explicit rollbacks in auto-commit mode are not allowed.
     */
    @Test
    public void testRollbackInAutoCommitMode() throws SQLException  {
        try (Connection conn = DriverManager.getConnection(URL)) {
            conn.setAutoCommit(true);

            assertThrowsSqlException("Transaction cannot be rolled back explicitly in auto-commit mode.", conn::rollback);
        }
    }

    /**
     * Ensure that explicit transaction can not be used, after it encounters an error.
     */
    @ParameterizedTest
    @CsvSource({
            // dml or not | SQL statement
            "true,insert into TEST (ID) values (2)",
            "false,SELECT * FROM test",
    })
    public void testOperationsFailsWhenTransactionEncoutersAnError(boolean dml, String sqlStmt) throws SQLException {
        try (Connection conn = DriverManager.getConnection(URL)) {
            conn.setAutoCommit(false);

            try (Statement stmt = conn.createStatement()) {
                assertThrowsSqlException("Division by zero", () -> stmt.executeQuery("SELECT 1/0").next());

                assertThrowsSqlException("Transaction is already finished",
                        () -> {
                            if (dml) {
                                stmt.executeUpdate(sqlStmt);
                            } else {
                                try (ResultSet rs = stmt.executeQuery(sqlStmt)) {
                                    rs.next();
                                }
                            }
                        });
            }
        }
    }

    /**
     * Ensures that the explicit transaction is rolled back on disconnect.
     */
    @Test
    public void transactionIsRolledBackOnDisconnect() throws SQLException {
        try (Connection conn = DriverManager.getConnection(URL)) {
            conn.setAutoCommit(false);

            try (Statement stmt = conn.createStatement()) {
                int updateCount = stmt.executeUpdate("INSERT INTO test VALUES (0, '0'), (1, '1'), (2, '2')");

                assertThat(updateCount, is(3));
                expectPendingTransactions(is(1));
            }
        }

        expectPendingTransactions(is(0));

        try (Connection conn = DriverManager.getConnection(URL)) {
            assertThat(rowsCount(conn), is(0));
        }
    }

    /**
     * Ensures that the current operation will be aborted and the explicit transaction will be rolled back on disconnect.
     */
    @Test
    public void transactionIsRolledBackOnDisconnectDuringQueryExecution() throws Exception {
        CompletableFuture<?> updateFuture;

        try (Connection conn = DriverManager.getConnection(URL)) {
            conn.setAutoCommit(false);

            expectPendingTransactions(is(0));

            try (Statement stmt = conn.createStatement()) {
                updateFuture = IgniteTestUtils.runAsync(
                        () -> stmt.executeUpdate("INSERT INTO test(id) SELECT x FROM system_range(0, 10000000000)")
                );

                waitUntilPendingTransactions(is(1));
            }
        }

        assertThat(updateFuture, willThrowFast(SQLException.class));

        waitUntilPendingTransactions(is(0));

        try (Connection conn = DriverManager.getConnection(URL)) {
            assertThat(rowsCount(conn), is(0));
        }
    }

    /**
     * Ensures that data updates made in an explicit transaction can be rolled back and are
     * not visible outside of an active transaction until they are committed.
     *
     * @param batchInsert Batch insert operation.
     * @throws SQLException If failed.
     */
    private void checkRollbackAndCommit(TestJdbcBatchInsertOperation batchInsert) throws SQLException {
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

    private static void waitUntilPendingTransactions(Matcher<Integer> matcher) {
        Awaitility.await().timeout(5, TimeUnit.SECONDS).untilAsserted(
                () -> expectPendingTransactions(matcher)
        );
    }

    private static void expectPendingTransactions(Matcher<Integer> matcher) {
        int pending = CLUSTER.runningNodes().mapToInt(node -> unwrapIgniteImpl(node).txManager().pending()).sum();

        assertThat(pending, matcher);
    }

    @FunctionalInterface
    private interface TestJdbcBatchInsertOperation {
        void run(Connection conn, Integer startRowId, Integer rowsCount) throws SQLException;
    }
}
