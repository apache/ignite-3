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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

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
        // only connection context or 0 if already closed.
        assertTrue(openCursorsRegistered() <= 1);
    }

    @Test
    public void testSimpleQueryExecute() throws Exception {
        boolean res = stmt.execute("INSERT INTO TEST_TX VALUES (5, 5, '5');");
        assertFalse(res);
        assertNull(stmt.getResultSet());
        assertFalse(stmt.getMoreResults());
        assertEquals(-1, getResultSetSize());

        stmt.execute("INSERT INTO TEST_TX VALUES (6, 5, '5');");
        assertEquals(-1, getResultSetSize());

        // empty result
        res = stmt.execute("SELECT ID FROM TEST_TX WHERE ID=1000;");
        assertTrue(res);
        assertNotNull(stmt.getResultSet());
    }

    @Test
    public void testSimpleQueryError() throws Exception {
        boolean res = stmt.execute("SELECT 1; SELECT 1/0");
        assertTrue(res);
        assertThrows(SQLException.class, () -> stmt.getMoreResults());
    }

    @Test
    public void testCloseOnCompletion() throws Exception {
        stmt.execute("SELECT 1; SELECT 2");
        ResultSet rs1 = stmt.getResultSet();

        assertFalse(stmt.isCloseOnCompletion());
        stmt.closeOnCompletion();
        assertTrue(stmt.isCloseOnCompletion());

        assertFalse(stmt.isClosed());
        rs1.close();
        assertTrue(stmt.isClosed());

        assertThrows(SQLException.class, () -> stmt.getMoreResults(), "Statement is closed");
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
        assertEquals(-1, getResultSetSize());

        res = stmt.execute("CREATE TABLE TEST_TX (ID INT PRIMARY KEY, AGE INT, NAME VARCHAR) ");
        assertFalse(res);
        assertEquals(-1, getResultSetSize());

        res = stmt.execute("INSERT INTO TEST_TX VALUES (1, 17, 'James'), (2, 43, 'Valery');");
        assertFalse(res);
        assertEquals(-1, getResultSetSize());

        res = stmt.execute("DROP TABLE IF EXISTS PUBLIC.TRANSACTIONS; INSERT INTO TEST_TX VALUES (3, 25, 'Michel');");
        assertFalse(res);
        assertEquals(-1, getResultSetSize());
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
        boolean res = stmt.execute("START TRANSACTION;");
        assertFalse(res);
        assertNull(stmt.getResultSet());
        assertEquals(0, stmt.getUpdateCount());
        assertFalse(stmt.getMoreResults());
        assertEquals(-1, stmt.getUpdateCount());

        res = stmt.execute("COMMIT;");
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
        assertEquals(-1, getResultSetSize());
    }

    @Test
    public void testAutoCommitFalse() throws Exception {
        conn.setAutoCommit(false);
        assertThrows(SQLException.class, () -> stmt.execute("COMMIT"));

        boolean res = stmt.execute("SELECT 1;COMMIT");
        assertTrue(res);
        assertNotNull(stmt.getResultSet());
        assertThrows(SQLException.class, () -> stmt.getMoreResults());
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
        assertEquals(1, getResultSetSize());

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

    @Test
    public void testResultsFromExecuteBatch() throws Exception {
        stmt.addBatch("INSERT INTO TEST_TX VALUES (7, 25, 'Michel');");
        stmt.addBatch("INSERT INTO TEST_TX VALUES (8, 25, 'Michel');");
        int[] arr = stmt.executeBatch();

        assertEquals(2, arr.length);
        assertArrayEquals(new int[]{1, 1}, arr);
        assertEquals(-1, stmt.getUpdateCount());
        assertFalse(stmt.getMoreResults());
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
     * Check statement return result sets and update counter. <br>
     * Returns: <br>
     *  -1 if all statement datasets are null, or <br>
     *  &gt= 0 summary results count<br>
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
}
