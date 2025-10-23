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

import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.apache.ignite.jdbc.util.JdbcTestUtils.assertThrowsSqlException;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import org.apache.ignite.internal.jdbc2.JdbcStatement2;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Tests for queries containing multiple sql statements, separated by ";".
 */
public class ItJdbcMultiStatementSelfTest extends AbstractJdbcSelfTest {
    /**
     * Setup tables.
     */
    @BeforeEach
    public void setupTables() throws Exception {
        execute("DROP TABLE IF EXISTS TEST_TX; "
                + "DROP TABLE IF EXISTS PUBLIC.TRANSACTIONS; "
                + "DROP TABLE IF EXISTS ONE;"
                + "DROP TABLE IF EXISTS TWO;");

        execute("CREATE TABLE TEST_TX (ID INT PRIMARY KEY, AGE INT, NAME VARCHAR) ");

        execute("INSERT INTO TEST_TX VALUES "
                + "(1, 17, 'James'), "
                + "(2, 43, 'Valery'), "
                + "(3, 25, 'Michel'), "
                + "(4, 19, 'Nick');");
    }

    @AfterEach
    void tearDown() throws Exception {
        int openCursorResources = openResources(CLUSTER);
        // connection + not closed result set
        assertTrue(openResources(CLUSTER) <= 2, "Open cursors: " + openCursorResources);

        stmt.close();

        openCursorResources = openResources(CLUSTER);

        // only connection context or 0 if already closed.
        assertTrue(openResources(CLUSTER) <= 1, "Open cursors: " + openCursorResources);
        assertTrue(waitForCondition(() -> openCursors(CLUSTER) == 0, 5_000));
    }

    @Test
    public void testAllStatementsAppliedIfExecutedWithFailure() throws Exception {
        stmt.execute("SELECT COUNT(*) FROM TEST_TX");
        try (ResultSet rs = stmt.getResultSet()) {
            assertTrue(rs.next());
            assertEquals(4, rs.getInt(1));
        }

        // pk violation exception
        // TODO: https://issues.apache.org/jira/browse/IGNITE-21133
        stmt.execute("START TRANSACTION; INSERT INTO TEST_TX VALUES (1, 1, '1'); COMMIT");
        assertEquals(0, stmt.getUpdateCount());
        assertThrowsSqlException("PK unique constraint is violated", () -> stmt.getMoreResults());

        stmt.execute("SELECT COUNT(*) FROM TEST_TX");
        try (ResultSet rs = stmt.getResultSet()) {
            assertTrue(rs.next());
            assertEquals(4, rs.getInt(1));
        }
    }

    @Test
    public void testAllStatementsAppliedIfExecutedWithoutFailure() throws Exception {
        // no pk violation
        stmt.execute("START TRANSACTION; INSERT INTO TEST_TX VALUES (5, 5, '5'); COMMIT");
        stmt.execute("SELECT COUNT(*) FROM TEST_TX");
        try (ResultSet rs = stmt.getResultSet()) {
            assertTrue(rs.next());
            assertEquals(5, rs.getInt(1));
        }
    }

    @Test
    public void testEmptyResults() throws Exception {
        boolean res = stmt.execute("SELECT 1; SELECT 1 FROM table(system_range(1, 0))");
        assertTrue(res);
        assertEquals(-1, stmt.getUpdateCount());
        assertTrue(stmt.getMoreResults());
        assertFalse(stmt.getMoreResults());
    }

    @Test
    public void testSimpleQueryExecute() throws Exception {
        boolean res = stmt.execute("INSERT INTO TEST_TX VALUES (5, 5, '5');");
        assertFalse(res);
        assertNull(stmt.getResultSet());
        assertFalse(stmt.getMoreResults());
        assertNull(stmt.getResultSet());
        assertEquals(-1, stmt.getUpdateCount());

        stmt.execute("INSERT INTO TEST_TX VALUES (6, 5, '5');");
        assertEquals(1, stmt.getUpdateCount());
        assertTrue(checkNoMoreResults());

        // empty result
        res = stmt.execute("SELECT ID FROM TEST_TX WHERE ID=1000;");
        assertTrue(res);
        assertNotNull(stmt.getResultSet());
    }

    @Test
    public void testSimpleQueryError() throws Exception {
        boolean res = stmt.execute("SELECT 1; SELECT 1/0; SELECT 2");
        assertTrue(res);
        assertThrowsSqlException("Division by zero", () -> stmt.getMoreResults());
        // Next after exception.
        // assertFalse(stmt.getMoreResults());
        // Do not move past the first result.
        assertThrowsSqlException("Division by zero", () -> stmt.getMoreResults());

        stmt.closeOnCompletion();
    }

    @Test
    public void testSimpleQueryErrorMustReleaseServerResources() throws Exception {
        // The script fails, the user does not retrieve any result sets.
        stmt.execute("SELECT 1; SELECT 2/0; SELECT 3");
        // But the resources must be released in after test callbacks.
    }

    @Test
    public void testSimpleQueryErrorCloseRs() throws Exception {
        stmt.execute("SELECT 1; SELECT 2/0; SELECT 2");
        ResultSet rs = stmt.getResultSet();
        assertThrowsSqlException("Division by zero", () -> stmt.getMoreResults());
        stmt.closeOnCompletion();

        rs.close();
    }

    @ParameterizedTest(name = "closeOnCompletion = {0}")
    @ValueSource(booleans = {true, false})
    @Disabled("https://issues.apache.org/jira/browse/IGNITE-21129")
    public void testCloseOnCompletionFirstRsClosed(boolean closeOnCompletion) throws Exception {
        stmt.execute("SELECT 1; DROP TABLE IF EXISTS TEST_TX; SELECT 1; ");
        ResultSet rs = stmt.getResultSet();

        if (closeOnCompletion) {
            stmt.closeOnCompletion();
        }

        rs.close();
        assertFalse(stmt.isClosed());

        stmt.getMoreResults();
        stmt.getResultSet();

        stmt.getMoreResults();
        rs = stmt.getResultSet();

        rs.close();

        if (closeOnCompletion) {
            assertTrue(stmt.isClosed());
        } else {
            assertFalse(stmt.isClosed());
        }
    }

    @ParameterizedTest(name = "closeOnCompletion = {0}")
    @ValueSource(booleans = {true, false})
    @Disabled("https://issues.apache.org/jira/browse/IGNITE-21129")
    public void testCloseOnCompletionFirstRsClosed2(boolean closeOnCompletion) throws Exception {
        stmt.execute("SELECT 1; DROP TABLE IF EXISTS TEST_TX;");
        ResultSet rs = stmt.getResultSet();

        if (closeOnCompletion) {
            stmt.closeOnCompletion();
        }

        rs.close();

        if (closeOnCompletion) {
            assertTrue(stmt.isClosed());
        } else {
            assertFalse(stmt.isClosed());
        }
    }

    @Test
    public void noMoreResultsArePossibleAfterCloseOnCompletion() throws Exception {
        stmt.execute("SELECT 1; SELECT 2; SELECT 3");

        ResultSet rs1 = stmt.getResultSet();
        // SELECT 2;
        assertTrue(stmt.getMoreResults());
        assertTrue(rs1.isClosed());

        ResultSet rs2 = stmt.getResultSet();
        stmt.closeOnCompletion();

        // SELECT 3;
        assertTrue(stmt.getMoreResults());
        assertTrue(rs2.isClosed());
        assertFalse(stmt.isClosed());

        // no more results, auto close statement
        assertFalse(stmt.getMoreResults());
        assertThrowsSqlException("Statement is closed", () -> stmt.getMoreResults());
        assertTrue(stmt.isClosed());
    }

    @Test
    public void requestMoreThanOneFetch() throws Exception {
        int range = stmt.getFetchSize() + 100;
        stmt.execute(format("START TRANSACTION; SELECT * FROM TABLE(system_range(0, {})); COMMIT;", range));
        assertEquals(range + 1, getResultSetSize());

        stmt.execute("START TRANSACTION; SELECT * FROM TABLE(system_range(0, 2000)); COMMIT;");
        stmt.getMoreResults();
        ResultSet rs = stmt.getResultSet();
        rs.close();
        stmt.getMoreResults();
        assertTrue(checkNoMoreResults());
    }

    @Test
    public void moreResultsAfterClosedRs() throws Exception {
        stmt.execute("START TRANSACTION; SELECT 1; SELECT 2; COMMIT;");
        stmt.getMoreResults();
        ResultSet rs = stmt.getResultSet();
        rs.close();
        assertTrue(stmt.getMoreResults());
        stmt.getResultSet().next();
        assertEquals(2, stmt.getResultSet().getInt(1));
    }

    @Test
    public void testMixedDmlQueryExecute() throws Exception {
        boolean res = stmt.execute("INSERT INTO TEST_TX VALUES (6, 5, '5'); DELETE FROM TEST_TX WHERE ID=6; SELECT 1;");
        assertFalse(res);
        assertEquals(1, getResultSetSize());

        res = stmt.execute("SELECT 1; INSERT INTO TEST_TX VALUES (7, 5, '5'); DELETE FROM TEST_TX WHERE ID=6;");
        assertEquals(true, res);
        assertEquals(1, getResultSetSize());

        // empty results set in the middle
        res = stmt.execute("SELECT * FROM TEST_TX; INSERT INTO TEST_TX VALUES (6, 6, '6'); SELECT * FROM TEST_TX;");
        assertEquals(true, res);
        assertEquals(11, getResultSetSize());
    }

    @Test
    public void testMiscDmlExecute() throws Exception {
        boolean res = stmt.execute("DROP TABLE IF EXISTS TEST_TX; DROP TABLE IF EXISTS SOME_UNEXISTING_TBL;");
        assertFalse(res);
        assertEquals(0, stmt.getUpdateCount());
        assertFalse(stmt.getMoreResults());
        assertEquals(0, stmt.getUpdateCount());

        res = stmt.execute("CREATE TABLE TEST_TX (ID INT PRIMARY KEY, AGE INT, NAME VARCHAR) ");
        assertFalse(res);
        assertEquals(0, stmt.getUpdateCount());
        assertFalse(stmt.getMoreResults());
        assertNull(stmt.getResultSet());

        res = stmt.execute("INSERT INTO TEST_TX VALUES (1, 17, 'James'), (2, 43, 'Valery');");
        assertFalse(res);
        assertEquals(2, stmt.getUpdateCount());
        assertFalse(stmt.getMoreResults());
        assertNull(stmt.getResultSet());

        res = stmt.execute("DROP TABLE IF EXISTS PUBLIC.TRANSACTIONS; INSERT INTO TEST_TX VALUES (3, 25, 'Michel');");
        assertFalse(res);
        assertEquals(0, stmt.getUpdateCount());
        assertFalse(stmt.getMoreResults());
        assertEquals(1, stmt.getUpdateCount());
    }

    @Test
    public void testPureTransaction() throws Exception {
        boolean res = stmt.execute("START TRANSACTION; COMMIT");
        assertFalse(res);
        assertNull(stmt.getResultSet());
        assertEquals(0, stmt.getUpdateCount());
        assertFalse(stmt.getMoreResults());
        assertEquals(0, stmt.getUpdateCount());
        assertFalse(stmt.getMoreResults());
        assertEquals(-1, stmt.getUpdateCount());
    }

    @Test
    public void testBrokenTransaction() throws Exception {
        //noinspection ThrowableNotThrown
        assertThrowsSqlException(
                "Transaction block doesn't have a COMMIT statement at the end.",
                () -> stmt.execute("START TRANSACTION;")
        );

        boolean res = stmt.execute("COMMIT;");
        assertFalse(res);
        assertNull(stmt.getResultSet());
        assertEquals(0, stmt.getUpdateCount());
        assertFalse(stmt.getMoreResults());
        assertEquals(-1, stmt.getUpdateCount());
    }

    @Test
    public void testTransactionQueryInside() throws Exception {
        stmt.execute("START TRANSACTION; SELECT 1; COMMIT");
        ResultSet resultSet = stmt.getResultSet();
        assertNull(resultSet);
        assertEquals(0, stmt.getUpdateCount());

        // SELECT 1
        assertTrue(stmt.getMoreResults());
        resultSet = stmt.getResultSet();
        assertNotNull(resultSet);

        // COMMIT
        assertFalse(stmt.getMoreResults());
        resultSet = stmt.getResultSet();
        assertNull(resultSet);
        assertEquals(0, stmt.getUpdateCount());

        // after commit
        assertFalse(stmt.getMoreResults());
        assertEquals(-1, stmt.getUpdateCount());
    }

    @Test
    public void testTransactionQueryInsideOutside() throws Exception {
        stmt.execute("START TRANSACTION; SELECT 1; COMMIT; SELECT 2;");
        ResultSet resultSet = stmt.getResultSet();
        assertNull(resultSet);
        assertEquals(0, stmt.getUpdateCount());

        // SELECT 1;
        assertTrue(stmt.getMoreResults());
        resultSet = stmt.getResultSet();
        assertNotNull(resultSet);

        // COMMIT;
        assertFalse(stmt.getMoreResults());
        resultSet = stmt.getResultSet();
        assertNull(resultSet);
        assertEquals(0, stmt.getUpdateCount());

        // SELECT 2;
        assertTrue(stmt.getMoreResults());
        resultSet = stmt.getResultSet();
        assertNotNull(resultSet);
        assertEquals(-1, stmt.getUpdateCount());

        // after
        assertFalse(stmt.getMoreResults());
        assertEquals(-1, stmt.getUpdateCount());
    }

    @Test
    public void testDmlInsideTransaction() throws Exception {
        stmt.execute("START TRANSACTION; INSERT INTO TEST_TX VALUES (5, 19, 'Nick'); COMMIT");
        assertEquals(0, stmt.getUpdateCount());
        stmt.getMoreResults();
        assertEquals(1, stmt.getUpdateCount());
        stmt.getMoreResults();
        assertEquals(0, stmt.getUpdateCount());
        assertTrue(checkNoMoreResults());
    }

    @Test
    public void testAutoCommitFalse() throws Exception {
        conn.setAutoCommit(false);

        stmt.execute("SELECT 1;");
        ResultSet rs = stmt.getResultSet();
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));

        stmt.execute("INSERT INTO TEST_TX VALUES (5, 19, 'Nick');");
        conn.rollback();

        stmt.execute("SELECT COUNT(ID) FROM TEST_TX WHERE ID=5;");
        rs = stmt.getResultSet();
        assertTrue(rs.next());
        assertEquals(0, rs.getInt(1));

        stmt.execute("INSERT INTO TEST_TX VALUES (5, 19, 'Nick');");
        conn.commit();

        stmt.execute("SELECT COUNT(ID) FROM TEST_TX WHERE ID=5;");
        rs = stmt.getResultSet();
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
    }

    @Test
    @SuppressWarnings("ThrowableNotThrown")
    public void testAutoCommitFalseTxControlStatementsNotSupported() throws Exception {
        String txErrMsg = "Transaction control statements are not supported when autocommit mode is disabled.";
        conn.setAutoCommit(false);
        assertThrowsSqlException(txErrMsg, () -> stmt.execute("START TRANSACTION; SELECT 1; COMMIT"));
        assertThrowsSqlException(txErrMsg, () -> stmt.execute("COMMIT"));
        assertThrowsSqlException(txErrMsg, () -> stmt.execute("START TRANSACTION; COMMIT;"));

        boolean res = stmt.execute("SELECT 1;COMMIT");
        assertTrue(res);
        assertNotNull(stmt.getResultSet());
        assertThrowsSqlException(txErrMsg, () -> stmt.getMoreResults());

        // Even though TX control statements don't affect a JDBC managed transaction directly,
        // exceptions during execution of previous statements may cause the transaction to rollback.
        assertThrowsSqlException(
                "Transaction is already finished",
                () -> stmt.executeQuery("SELECT COUNT(1) FROM TEST_TX")
        );

        // Let's recover connection.
        conn.rollback();

        {
            long initialRowsCount;

            try (ResultSet rs = stmt.executeQuery("SELECT COUNT(1) FROM TEST_TX")) {
                assertTrue(rs.next());

                initialRowsCount = rs.getLong(1);
            }

            stmt.execute("INSERT INTO TEST_TX VALUES (5, 5, '5'); COMMIT; INSERT INTO TEST_TX VALUES (6, 6, '6')");
            assertEquals(1, stmt.getUpdateCount());

            // Next statement throws the expected exception.
            assertThrowsSqlException(txErrMsg, () -> stmt.getMoreResults());

            stmt.close();

            // JDBC managed transaction was not rolled back or committed.
            try (Connection conn0 = DriverManager.getConnection(URL)) {
                Statement stmt0 = conn0.createStatement();

                try (ResultSet rs = stmt0.executeQuery("SELECT COUNT(1) FROM TEST_TX")) {
                    assertTrue(rs.next());
                    assertEquals(initialRowsCount, rs.getLong(1));
                }
            }

            // Commit JDBC managed transaction.
            conn.commit();

            try (Connection conn0 = DriverManager.getConnection(URL)) {
                Statement stmt0 = conn0.createStatement();

                try (ResultSet rs = stmt0.executeQuery("SELECT COUNT(1) FROM TEST_TX")) {
                    assertTrue(rs.next());

                    assertEquals(initialRowsCount, rs.getLong(1));
                }
            }
        }
    }

    @Test
    public void testPreviousResultSetIsClosedExecute() throws Exception {
        boolean res = stmt.execute("SELECT ID FROM TEST_TX; SELECT 1;");
        assertEquals(true, res);
        stmt.getResultSet();
        ResultSet rs = stmt.getResultSet();

        res = stmt.getMoreResults();
        assertEquals(true, res);

        assertTrue(rs.isClosed());
        assertNotNull(stmt.getResultSet());
        assertTrue(checkNoMoreResults());

        stmt.execute("SELECT 1; SELECT 2; SELECT 3;");
        ResultSet rs1 = stmt.getResultSet();
        stmt.getMoreResults();
        assertTrue(rs1.isClosed());
        ResultSet rs2 = stmt.getResultSet();
        stmt.getMoreResults();
        assertTrue(rs2.isClosed());
        rs = stmt.getResultSet();
        assertTrue(rs.next());
        assertEquals(3, rs.getObject(1));
        rs.close();
    }

    @Test
    public void testPreviousResultsNotInvolvedExecute() throws Exception {
        boolean res = stmt.execute("SELECT ID FROM TEST_TX; SELECT 1;");
        assertEquals(true, res);
        assertEquals(5, getResultSetSize());

        res = stmt.execute("SELECT 1; SELECT 1;");
        assertEquals(true, res);
        assertEquals(2, getResultSetSize());
    }

    /** Check update count invariants. */
    @Test
    public void testUpdCountMisc() throws Exception {
        // pure select case
        stmt.execute("SELECT 1; SELECT 1;");
        assertEquals(-1, stmt.getUpdateCount());

        ResultSet rs = stmt.getResultSet();
        assertEquals(-1, stmt.getUpdateCount());

        rs.next();
        assertEquals(-1, stmt.getUpdateCount());

        stmt.getMoreResults();
        assertTrue(rs.isClosed());
        rs = stmt.getResultSet();
        assertEquals(-1, stmt.getUpdateCount());

        rs.next();
        assertEquals(-1, stmt.getUpdateCount());

        // empty result
        stmt.execute("SELECT ID FROM TEST_TX WHERE ID=1000;");
        assertEquals(-1, stmt.getUpdateCount());
    }

    @Test
    public void testUpdCountNoMoreResults() throws Exception {
        stmt.execute("INSERT INTO TEST_TX VALUES (5, 5, '5'), (6, 5, '5');");
        assertEquals(2, stmt.getUpdateCount());
        stmt.getMoreResults();
        assertEquals(-1, stmt.getUpdateCount());
        stmt.getResultSet();
        assertEquals(-1, stmt.getUpdateCount());
    }

    @Test
    public void testMixedQueriesUpdCount() throws Exception {
        stmt.execute("SELECT 1; INSERT INTO TEST_TX VALUES (7, 5, '5');");
        stmt.getMoreResults();
        assertEquals(1, stmt.getUpdateCount());
        assertEquals(1, stmt.getUpdateCount());
        stmt.getMoreResults();
        assertEquals(-1, stmt.getUpdateCount());

        stmt.execute("DROP TABLE IF EXISTS TEST_TX; DROP TABLE IF EXISTS PUBLIC.TRANSACTIONS;");
        assertEquals(0, stmt.getUpdateCount());

        stmt.execute("CREATE TABLE TEST_TX (ID INT PRIMARY KEY, AGE INT, NAME VARCHAR) ");
        assertEquals(0, stmt.getUpdateCount());
    }

    @Test
    public void testUpdCountAfterError() throws Exception {
        try {
            stmt.execute("INSERT INTO NOT_EXIST VALUES (3, 17, 'James');");
        } catch (Throwable ignored) {
            // No op.
        }
        ResultSet rs = stmt.getResultSet();
        assertNull(rs);
        assertEquals(-1, stmt.getUpdateCount());
    }

    /**
     * Sanity test for scripts, containing empty statements are handled correctly.
     */
    @Test
    @Disabled("https://issues.apache.org/jira/browse/IGNITE-21081")
    public void testEmptyStatements() throws Exception {
        execute(";;;SELECT 1 + 2");
        execute(" ;; ;;;; ");
        execute("CREATE TABLE ONE (id INT PRIMARY KEY, VAL VARCHAR);;"
                + "CREATE INDEX T_IDX ON ONE(val)"
                + ";;UPDATE ONE SET VAL = 'SOME';;;  ");

        execute("DROP INDEX T_IDX ;;  ;;"
                + "UPDATE ONE SET VAL = 'SOME'");
    }

    /**
     * Check multiple statements execution through prepared statement.
     */
    @Test
    public void testMultiStatementPreparedStatement() throws Exception {
        int leoAge = 28;

        String nickolas = "Nickolas";

        int gabAge = 84;
        String gabName = "Gab";

        int delYounger = 19;

        String complexQuery =
                "INSERT INTO TEST_TX VALUES (5, ?, 'Leo'); "
                        + "START TRANSACTION ; "
                        + "UPDATE TEST_TX SET name = ? WHERE name = 'Nick' ;"
                        + "INSERT INTO TEST_TX VALUES (6, ?, ?); "
                        + "DELETE FROM TEST_TX WHERE age < ?; "
                        + "COMMIT;";

        try (PreparedStatement p = conn.prepareStatement(complexQuery)) {
            p.setInt(1, leoAge);
            p.setString(2, nickolas);
            p.setInt(3, gabAge);
            p.setString(4, gabName);
            p.setInt(5, delYounger);
        }

        complexQuery = "UPDATE TEST_TX SET name = ? WHERE name = 'James';";

        try (PreparedStatement p = conn.prepareStatement(complexQuery)) {
            p.setString(1, nickolas);

            p.execute();
        }

        try (PreparedStatement sel = conn.prepareStatement("SELECT * FROM TEST_TX ORDER BY ID LIMIT 1;")) {
            try (ResultSet pers = sel.executeQuery()) {
                assertTrue(pers.next());
                assertEquals(17, age(pers));
                assertEquals(nickolas, name(pers));

                assertFalse(pers.next());
            }
        }
    }

    @Test
    public void testTimeout() throws SQLException {
        JdbcStatement2 igniteStmt = stmt.unwrap(JdbcStatement2.class);
        igniteStmt.setQueryTimeout(1);

        int attempts = 10;

        for (int i = 0; i < attempts; i++) {
            stmt.execute("SELECT 1; SELECT * FROM TABLE(SYSTEM_RANGE(1, 1000000000)); SELECT * FROM TABLE(SYSTEM_RANGE(1, 10));");

            // The first statement should succeed, if it times out, retry.
            try {
                try (ResultSet rs = stmt.getResultSet()) {
                    while (rs.next()) {
                        assertNotNull(rs.getObject(1));
                    }
                }
            } catch (SQLException e) {
                // Ignore timeout for the first statement, if takes too long.
                // Skip both planning and execution timeouts.
                assertThat("Unexpected error", e.getMessage(), containsString("timeout"));
                continue;
            }

            assertTrue(stmt.getMoreResults(), "Expected more results");

            // The second statement should always fail.
            assertThrowsSqlException(SQLException.class,
                    "Query timeout", () -> {
                        try (ResultSet rs = stmt.getResultSet()) {
                            while (rs.next()) {
                                assertNotNull(rs.getObject(1));
                            }
                        }
                    });

            // Script timed out. We should also get a timeout.
            assertThrowsSqlException(SQLException.class, "Query timeout", () -> stmt.getMoreResults());

            return;
        }

        fail("Failed to get expected timeout error");
    }

    /**
     * Extract person's name from result set.
     */
    private static String name(ResultSet rs) throws SQLException {
        return rs.getString("NAME");
    }

    /**
     * Extract person's age from result set.
     */
    private static int age(ResultSet rs) throws SQLException {
        return rs.getInt("AGE");
    }

    /**
     * Execute sql script using thin driver.
     */
    private boolean execute(String sql) throws Exception {
        return stmt.execute(sql);
    }

    /**
     * Check summary results returned from statement.
     *
     * @throws SQLException If failed.
     */
    private int getResultSetSize() throws SQLException {
        ResultSet rs = stmt.getResultSet();
        int size = -1;
        boolean more;
        int updCount;

        do {
            if (rs != null) {
                if (size == -1) {
                    size = 0;
                }
                while (rs.next()) {
                    ++size;
                }
            }

            if (stmt.getMoreResults()) {
                rs = stmt.getResultSet();
                more = true;
            } else {
                rs = null;
                more = false;
            }
            updCount = stmt.getUpdateCount();
        } while (more || updCount != -1);
        return size;
    }

    private boolean checkNoMoreResults() throws SQLException {
        boolean more = stmt.getMoreResults();
        int updCnt = stmt.getUpdateCount();
        return !more && updCnt == -1;
    }
}
