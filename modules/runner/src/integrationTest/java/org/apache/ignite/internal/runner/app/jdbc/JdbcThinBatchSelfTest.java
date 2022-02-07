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

import static org.apache.ignite.internal.runner.app.jdbc.AbstractJdbcSelfTest.conn;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.concurrent.Callable;
import org.apache.ignite.client.proto.query.IgniteQueryErrorCode;
import org.apache.ignite.client.proto.query.SqlStateCode;
import org.junit.jupiter.api.Test;

/**
 * Statement test.
 */
public class JdbcThinBatchSelfTest extends AbstractJdbcSelfTest {
    /** SQL query. */
    private static final String SQL_PREPARED = "insert into Person(_key, id, firstName, lastName, age) values " +
        "(?, ?, ?, ?, ?)";

    /** Prepared statement. */
    private PreparedStatement pstmt;

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        pstmt = conn.prepareStatement(SQL_PREPARED);

        assertNotNull(pstmt);
        assertFalse(pstmt.isClosed());
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        if (pstmt != null && !pstmt.isClosed())
            pstmt.close();

        assertTrue(pstmt.isClosed());

        super.afterTest();
    }

    /**
     * @throws SQLException If failed.
     */
    @Test
    public void testBatch() throws SQLException {
        final int BATCH_SIZE = 10;

        for (int idx = 0, i = 0; i < BATCH_SIZE; ++i, idx += i) {
            stmt.addBatch("insert into Person (_key, id, firstName, lastName, age) values "
                + generateValues(idx, i + 1));
        }

        int[] updCnts = stmt.executeBatch();

        assertEquals(BATCH_SIZE, updCnts.length, "Invalid update counts size");

        for (int i = 0; i < BATCH_SIZE; ++i)
            assertEquals(i + 1, updCnts[i], "Invalid update count");
    }

    /**
     * @throws SQLException If failed.
     */
    @Test
    public void testBatchOnClosedStatement() throws SQLException {
        final Statement stmt2 = conn.createStatement();
        final PreparedStatement pstmt2 = conn.prepareStatement("");

        stmt2.close();
        pstmt2.close();

        assertThrows(SQLException.class, () -> stmt2.addBatch(""), "Statement is closed.");

        assertThrows(SQLException.class, stmt2::clearBatch, "Statement is closed.");

        assertThrows(SQLException.class, stmt2::executeBatch, "Statement is closed.");

        assertThrows(SQLException.class, pstmt2::addBatch, "Statement is closed.");

        assertThrows(SQLException.class, pstmt2::clearBatch, "Statement is closed.");

        assertThrows(SQLException.class, pstmt2::executeBatch, "Statement is closed.");
    }

    /**
     * @throws SQLException If failed.
     */
    @Test
    public void testBatchException() throws Exception {
        final int BATCH_SIZE = 7;

        final int FAILED_IDX = 5;

        for (int idx = 0, i = 0; i < FAILED_IDX; ++i, idx += i) {
            stmt.addBatch("insert into Person (_key, id, firstName, lastName, age) values "
                + generateValues(idx, i + 1));
        }

        stmt.addBatch("select * from Person");

        stmt.addBatch("insert into Person (_key, id, firstName, lastName, age) values "
            + generateValues(100, 7));

        try {
            stmt.executeBatch();

            fail("BatchUpdateException must be thrown");
        }
        catch (BatchUpdateException e) {
            checkThereAreNotUsedConnections();

            int[] updCnts = e.getUpdateCounts();

            assertEquals(BATCH_SIZE, updCnts.length, "Invalid update counts size");

            for (int i = 0; i < BATCH_SIZE; ++i)
                assertEquals(i != FAILED_IDX ? i + 1 : Statement.EXECUTE_FAILED, updCnts[i], "Invalid update count");

            if (!e.getMessage().contains("Given statement type does not match that declared by JDBC driver")) {
                log.error("Invalid exception: ", e);

                fail();
            }

            assertEquals(SqlStateCode.PARSING_EXCEPTION, e.getSQLState(), "Invalid SQL state.");
//            assertEquals(IgniteQueryErrorCode.STMT_TYPE_MISMATCH, e.getErrorCode(), "Invalid error code.");
        }
    }

    /**
     * @throws SQLException If failed.
     */
    @Test
    public void testBatchParseException() throws Exception {
        final int BATCH_SIZE = 7;

        final int FAILED_IDX = 5;

        for (int idx = 0, i = 0; i < FAILED_IDX; ++i, idx += i) {
            stmt.addBatch("insert into Person (_key, id, firstName, lastName, age) values "
                + generateValues(idx, i + 1));
        }

        stmt.addBatch("insert into Person (_key, id, firstName, lastName, age) values (4444, 'fail', 1, 1, 1)");

        stmt.addBatch("insert into Person (_key, id, firstName, lastName, age) values "
            + generateValues(100, 7));

        try {
            stmt.executeBatch();

            fail("BatchUpdateException must be thrown");
        }
        catch (BatchUpdateException e) {
            checkThereAreNotUsedConnections();

            int[] updCnts = e.getUpdateCounts();

            assertEquals(BATCH_SIZE, updCnts.length, "Invalid update counts size");

            for (int i = 0; i < BATCH_SIZE; ++i)
                assertEquals(i != FAILED_IDX ? i + 1 : Statement.EXECUTE_FAILED, updCnts[i], "Invalid update count: " + i);

            if (!e.getMessage().contains("Value conversion failed")) {
                log.error("Invalid exception: ", e);

                fail();
            }

            assertEquals(SqlStateCode.CONVERSION_FAILED, e.getSQLState(), "Invalid SQL state.");
//            assertEquals(IgniteQueryErrorCode.CONVERSION_FAILED, e.getErrorCode(), "Invalid error code.");
        }
    }

    /**
     * @throws SQLException If failed.
     */
    @Test
    public void testBatchMerge() throws SQLException {
        final int BATCH_SIZE = 7;

        for (int idx = 0, i = 0; i < BATCH_SIZE; ++i, idx += i) {
            stmt.addBatch("merge into Person (_key, id, firstName, lastName, age) values "
                + generateValues(idx, i + 1));
        }

        int[] updCnts = stmt.executeBatch();

        assertEquals(BATCH_SIZE, updCnts.length, "Invalid update counts size");

        for (int i = 0; i < BATCH_SIZE; ++i)
            assertEquals(i + 1, updCnts[i], "Invalid update count");
    }

    /**
     * @throws SQLException If failed.
     */
    @Test
    public void testBatchMergeParseException() throws Exception {
        final int BATCH_SIZE = 7;

        final int FAILED_IDX = 5;

        for (int idx = 0, i = 0; i < FAILED_IDX; ++i, idx += i) {
            stmt.addBatch("merge into Person (_key, id, firstName, lastName, age) values "
                + generateValues(idx, i + 1));
        }

        stmt.addBatch("merge into Person (_key, id, firstName, lastName, age) values (4444, 'FAIL', 1, 1, 1)");

        stmt.addBatch("merge into Person (_key, id, firstName, lastName, age) values "
            + generateValues(100, 7));

        try {
            stmt.executeBatch();

            fail("BatchUpdateException must be thrown");
        }
        catch (BatchUpdateException e) {
            checkThereAreNotUsedConnections();

            int[] updCnts = e.getUpdateCounts();

            assertEquals(BATCH_SIZE, updCnts.length, "Invalid update counts size");

            for (int i = 0; i < BATCH_SIZE; ++i)
                assertEquals(i != FAILED_IDX ? i + 1 : Statement.EXECUTE_FAILED, updCnts[i], "Invalid update count: " + i);

            if (!e.getMessage().contains("Value conversion failed")) {
                log.error("Invalid exception: ", e);

                fail();
            }

            assertEquals(SqlStateCode.CONVERSION_FAILED, e.getSQLState(), "Invalid SQL state.");
//            assertEquals(IgniteQueryErrorCode.CONVERSION_FAILED, e.getErrorCode(), "Invalid error code.");
        }
    }

    /**
     * @throws SQLException If failed.
     */
    @Test
    public void testBatchKeyDuplicatesException() throws Exception {
        final int BATCH_SIZE = 7;

        final int FAILED_IDX = 5;

        int idx = 0;

        for (int i = 0; i < FAILED_IDX; ++i, idx += i) {
            stmt.addBatch("insert into Person (_key, id, firstName, lastName, age) values "
                + generateValues(idx, i + 1));
        }

        stmt.addBatch("insert into Person (_key, id, firstName, lastName, age) values ('p0', 0, 'Name0', 'Lastname0', 20)");

        stmt.addBatch("insert into Person (_key, id, firstName, lastName, age) values "
            + generateValues(++idx, 7));

        try {
            stmt.executeBatch();

            fail("BatchUpdateException must be thrown");
        }
        catch (BatchUpdateException e) {
            checkThereAreNotUsedConnections();

            int[] updCnts = e.getUpdateCounts();

            assertEquals(BATCH_SIZE, updCnts.length, "Invalid update counts size");

            for (int i = 0; i < BATCH_SIZE; ++i)
                assertEquals(i != FAILED_IDX ? i + 1 : Statement.EXECUTE_FAILED, updCnts[i], "Invalid update count: " + i);

            if (!e.getMessage().contains("Failed to INSERT some keys because they are already in cache [keys=[p0]")) {
                log.error("Invalid exception: ", e);

                fail();
            }

            assertEquals(SqlStateCode.CONSTRAINT_VIOLATION, e.getSQLState(), "Invalid SQL state.");
//            assertEquals(IgniteQueryErrorCode.DUPLICATE_KEY, e.getErrorCode(), "Invalid error code.");
        }
    }

    /**
     * @throws SQLException If failed.
     */
    @Test
    public void testHeterogeneousBatch() throws SQLException {
        stmt.addBatch("insert into Person (_key, id, firstName, lastName, age) values ('p0', 0, 'Name0', 'Lastname0', 10)");
        stmt.addBatch("insert into Person (_key, id, firstName, lastName, age) " +
            "values ('p1', 1, 'Name1', 'Lastname1', 20), ('p2', 2, 'Name2', 'Lastname2', 30)");
        stmt.addBatch("merge into Person (_key, id, firstName, lastName, age) values ('p3', 3, 'Name3', 'Lastname3', 40)");
        stmt.addBatch("update Person set id = 5 where age >= 30");
        stmt.addBatch("merge into Person (_key, id, firstName, lastName, age) values ('p0', 2, 'Name2', 'Lastname2', 50)");
        stmt.addBatch("delete from Person where age <= 40");

        int[] updCnts = stmt.executeBatch();

        assertEquals(6, updCnts.length, "Invalid update counts size");
        assertArrayEquals(new int[] {1, 2, 1, 2, 1, 3}, updCnts, "Invalid update count");
    }

    /**
     * @throws SQLException If failed.
     */
    @Test
    public void testHeterogeneousBatchException() throws Exception {
        stmt.addBatch("insert into Person (_key, id, firstName, lastName, age) values ('p0', 0, 'Name0', 'Lastname0', 10)");
        stmt.addBatch("insert into Person (_key, id, firstName, lastName, age) " +
            "values ('p1', 1, 'Name1', 'Lastname1', 20), ('p2', 2, 'Name2', 'Lastname2', 30)");
        stmt.addBatch("merge into Person (_key, id, firstName, lastName, age) values ('p3', 3, 'Name3', 'Lastname3', 40)");
        stmt.addBatch("update Person set id = 'FAIL' where age >= 30"); // Fail.
        stmt.addBatch("merge into Person (_key, id, firstName, lastName, age) values ('p0', 2, 'Name2', 'Lastname2', 50)");
        stmt.addBatch("delete from Person where FAIL <= 40"); // Fail.

        try {
            stmt.executeBatch();

            fail("BatchUpdateException must be thrown");
        }
        catch (BatchUpdateException e) {
            checkThereAreNotUsedConnections();

            int[] updCnts = e.getUpdateCounts();

            if (!e.getMessage().contains("Value conversion failed")) {
                log.error("Invalid exception: ", e);

                fail();
            }

            assertEquals(6, updCnts.length, "Invalid update counts size");
            assertArrayEquals(new int[] {1, 2, 1, Statement.EXECUTE_FAILED, 1, Statement.EXECUTE_FAILED}, updCnts, "Invalid update count");
        }
    }

    /**
     * @throws SQLException If failed.
     */
    @Test
    public void testBatchClear() throws SQLException {
        final int BATCH_SIZE = 7;

        for (int idx = 0, i = 0; i < BATCH_SIZE; ++i, idx += i) {
            stmt.addBatch("insert into Person (_key, id, firstName, lastName, age) values "
                + generateValues(idx, i + 1));
        }

        stmt.clearBatch();

        int[] updates = stmt.executeBatch();

        assertEquals(0, updates.length, "Returned update counts array should have no elements for empty batch.");
    }

    /**
     * Check that user can execute empty batch using Statement. Such behaviour is more user friendly than forbidding
     * empty batches.
     *
     * @throws SQLException on error.
     */
    @Test
    public void testEmptyBatchStreaming() throws SQLException {
        executeUpdateOn(conn, "SET STREAMING ON");

        int[] updates = stmt.executeBatch();

        assertEquals(0, updates.length, "Returned update counts array should have no elements for empty batch.");

        executeUpdateOn(conn, "SET STREAMING OFF");

        assertEquals(0L, personsCount(), "Test table should be empty after empty batch is performed.");
    }

    /**
     * Same as {@link #testEmptyBatchStreaming()} but for PreparedStatement.
     *
     * @throws SQLException on error.
     */
    @Test
    public void testEmptyBatchStreamingPrepared() throws SQLException {
        try (Statement statement = conn.createStatement()) {
            statement.executeUpdate("SET STREAMING ON");
        }

        int[] updates = pstmt.executeBatch();

        assertEquals(0, updates.length, "Returned update counts array should have no elements for empty batch.");

        try (Statement statement = conn.createStatement()) {
            statement.executeUpdate("SET STREAMING OFF");
        }

        assertEquals(0L, personsCount(), "Test table should be empty after empty batch is performed.");
    }

    /**
     * @throws SQLException If failed.
     */
    @Test
    public void testBatchPrepared() throws SQLException {
        final int BATCH_SIZE = 10;

        for (int i = 0; i < BATCH_SIZE; ++i) {
            int paramCnt = 1;

            pstmt.setString(paramCnt++, "p" + i);
            pstmt.setInt(paramCnt++, i);
            pstmt.setString(paramCnt++, "Name" + i);
            pstmt.setString(paramCnt++, "Lastname" + i);
            pstmt.setInt(paramCnt++, 20 + i);

            pstmt.addBatch();
        }

        int[] updCnts = pstmt.executeBatch();

        assertEquals(BATCH_SIZE, updCnts.length, "Invalid update counts size");

        for (int i = 0; i < BATCH_SIZE; ++i)
            assertEquals(1, updCnts[i], "Invalid update count");
    }

    /**
     * @throws SQLException If failed.
     */
    @Test
    public void testBatchExceptionPrepared() throws Exception {
        final int BATCH_SIZE = 7;

        final int FAILED_IDX = 5;

        assert FAILED_IDX + 2 == BATCH_SIZE;

        for (int i = 0; i < FAILED_IDX; ++i) {
            int paramCnt = 1;

            pstmt.setString(paramCnt++, "p" + i);
            pstmt.setInt(paramCnt++, i);
            pstmt.setString(paramCnt++, "Name" + i);
            pstmt.setString(paramCnt++, "Lastname" + i);
            pstmt.setInt(paramCnt++, 20 + i);

            pstmt.addBatch();
        }

        int paramCnt = 1;
        pstmt.setString(paramCnt++, "p" + FAILED_IDX);
        pstmt.setString(paramCnt++, "FAIL");
        pstmt.setString(paramCnt++, "Name" + FAILED_IDX);
        pstmt.setString(paramCnt++, "Lastname" + FAILED_IDX);
        pstmt.setInt(paramCnt++, 20 + FAILED_IDX);

        pstmt.addBatch();

        paramCnt = 1;
        pstmt.setString(paramCnt++, "p" + FAILED_IDX + 1);
        pstmt.setInt(paramCnt++, FAILED_IDX + 1);
        pstmt.setString(paramCnt++, "Name" + FAILED_IDX + 1);
        pstmt.setString(paramCnt++, "Lastname" + FAILED_IDX + 1);
        pstmt.setInt(paramCnt++, 20 + FAILED_IDX + 1);

        pstmt.addBatch();

        try {
            pstmt.executeBatch();

            fail("BatchUpdateException must be thrown");
        }
        catch (BatchUpdateException e) {
            checkThereAreNotUsedConnections();

            int[] updCnts = e.getUpdateCounts();

            assertEquals(BATCH_SIZE, updCnts.length, "Invalid update counts size");

            for (int i = 0; i < BATCH_SIZE; ++i)
                assertEquals(i != FAILED_IDX ? 1 : Statement.EXECUTE_FAILED, updCnts[i], "Invalid update count");

            if (!e.getMessage().contains("Value conversion failed")) {
                log.error("Invalid exception: ", e);

                fail();
            }

            assertEquals(SqlStateCode.CONVERSION_FAILED, e.getSQLState(), "Invalid SQL state.");
//            assertEquals(IgniteQueryErrorCode.CONVERSION_FAILED, e.getErrorCode(), "Invalid error code.");
        }
    }

    /**
     * @throws SQLException If failed.
     */
    @Test
    public void testBatchMergePrepared() throws SQLException {
        final int BATCH_SIZE = 10;

        pstmt = conn.prepareStatement("merge into Person(_key, id, firstName, lastName, age) values " +
            "(?, ?, ?, ?, ?)");

        for (int i = 0; i < BATCH_SIZE; ++i) {
            int paramCnt = 1;

            pstmt.setString(paramCnt++, "p" + i);
            pstmt.setInt(paramCnt++, i);
            pstmt.setString(paramCnt++, "Name" + i);
            pstmt.setString(paramCnt++, "Lastname" + i);
            pstmt.setInt(paramCnt++, 20 + i);

            pstmt.addBatch();
        }

        int[] updCnts = pstmt.executeBatch();

        assertEquals(BATCH_SIZE, updCnts.length, "Invalid update counts size");

        for (int i = 0; i < BATCH_SIZE; ++i)
            assertEquals(1, updCnts[i], "Invalid update count");
    }

    /**
     * @throws SQLException If failed.
     */
    @Test
    public void testBatchMergeExceptionPrepared() throws Exception {
        final int BATCH_SIZE = 7;

        final int FAILED_IDX = 5;

        pstmt = conn.prepareStatement("merge into Person(_key, id, firstName, lastName, age) values " +
            "(?, ?, ?, ?, ?)");

        assert FAILED_IDX + 2 == BATCH_SIZE;

        for (int i = 0; i < FAILED_IDX; ++i) {
            int paramCnt = 1;

            pstmt.setString(paramCnt++, "p" + i);
            pstmt.setInt(paramCnt++, i);
            pstmt.setString(paramCnt++, "Name" + i);
            pstmt.setString(paramCnt++, "Lastname" + i);
            pstmt.setInt(paramCnt++, 20 + i);

            pstmt.addBatch();
        }

        int paramCnt = 1;
        pstmt.setString(paramCnt++, "p" + FAILED_IDX);
        pstmt.setString(paramCnt++, "FAIL");
        pstmt.setString(paramCnt++, "Name" + FAILED_IDX);
        pstmt.setString(paramCnt++, "Lastname" + FAILED_IDX);
        pstmt.setInt(paramCnt++, 20 + FAILED_IDX);

        pstmt.addBatch();

        paramCnt = 1;
        pstmt.setString(paramCnt++, "p" + FAILED_IDX + 1);
        pstmt.setInt(paramCnt++, FAILED_IDX + 1);
        pstmt.setString(paramCnt++, "Name" + FAILED_IDX + 1);
        pstmt.setString(paramCnt++, "Lastname" + FAILED_IDX + 1);
        pstmt.setInt(paramCnt++, 20 + FAILED_IDX + 1);

        pstmt.addBatch();

        try {
            int[] res = pstmt.executeBatch();

            fail("BatchUpdateException must be thrown res=" + Arrays.toString(res));
        }
        catch (BatchUpdateException e) {
            checkThereAreNotUsedConnections();

            int[] updCnts = e.getUpdateCounts();

            assertEquals(BATCH_SIZE, updCnts.length, "Invalid update counts size");

            for (int i = 0; i < BATCH_SIZE; ++i)
                assertEquals(i != FAILED_IDX ? 1 : Statement.EXECUTE_FAILED, updCnts[i], "Invalid update count");

            if (!e.getMessage().contains("Value conversion failed")) {
                log.error("Invalid exception: ", e);

                fail();
            }

            assertEquals(SqlStateCode.CONVERSION_FAILED, e.getSQLState(), "Invalid SQL state.");
//            assertEquals(IgniteQueryErrorCode.CONVERSION_FAILED, e.getErrorCode(), "Invalid error code.");
        }
    }

    /**
     * Populates table 'Person' with entities.
     *
     * @param size Number of entities.
     * @throws SQLException If failed.
     */
    private void populateTable(int size) throws SQLException {
        stmt.addBatch("insert into Person (_key, id, firstName, lastName, age) values "
            + generateValues(0, size));

        stmt.executeBatch();
    }

    /**
     * @throws SQLException If failed.
     */
    @Test
    public void testBatchUpdatePrepared() throws SQLException {
        final int BATCH_SIZE = 10;

        populateTable(BATCH_SIZE);

        pstmt = conn.prepareStatement("update Person set age = 100 where id = ?;");

        for (int i = 0; i < BATCH_SIZE; ++i) {
            pstmt.setInt(1, i);

            pstmt.addBatch();
        }

        int[] updCnts = pstmt.executeBatch();

        assertEquals(BATCH_SIZE, updCnts.length, "Invalid update counts size");

        for (int i = 0; i < BATCH_SIZE; ++i)
            assertEquals(1, updCnts[i], "Invalid update count");
    }

    /**
     * @throws SQLException If failed.
     */
    @Test
    public void testBatchUpdateExceptionPrepared() throws Exception {
        final int BATCH_SIZE = 7;

        final int FAILED_IDX = 5;

        populateTable(BATCH_SIZE);

        pstmt = conn.prepareStatement("update Person set age = 100 where id = ?;");

        assert FAILED_IDX + 2 == BATCH_SIZE;

        for (int i = 0; i < FAILED_IDX; ++i) {
            pstmt.setInt(1, i);

            pstmt.addBatch();
        }

        pstmt.setString(1, "FAIL");

        pstmt.addBatch();

        pstmt.setInt(1, FAILED_IDX + 1);

        pstmt.addBatch();

        try {
            int[] res = pstmt.executeBatch();

            fail("BatchUpdateException must be thrown res=" + Arrays.toString(res));
        }
        catch (BatchUpdateException e) {
            checkThereAreNotUsedConnections();

            int[] updCnts = e.getUpdateCounts();

            assertEquals(BATCH_SIZE, updCnts.length, "Invalid update counts size");

            for (int i = 0; i < BATCH_SIZE; ++i) {
                assertEquals(i != FAILED_IDX ? 1 : Statement.EXECUTE_FAILED, updCnts[i], "Invalid update count[" + i + ']');
            }

            if (!e.getMessage().contains("Data conversion error converting \"FAIL\"")) {
                log.error("Invalid exception: ", e);

                fail();
            }

            assertEquals(SqlStateCode.INTERNAL_ERROR, e.getSQLState(), "Invalid SQL state.");
            assertEquals(IgniteQueryErrorCode.UNKNOWN, e.getErrorCode(), "Invalid error code.");

            //assertEquals("Invalid SQL state.", SqlStateCode.CONVERSION_FAILED, e.getSQLState());
            //assertEquals("Invalid error code.", IgniteQueryErrorCode.CONVERSION_FAILED, e.getErrorCode());
        }
    }

    /**
     * @throws SQLException If failed.
     */
    @Test
    public void testBatchDeletePrepared() throws SQLException {
        final int BATCH_SIZE = 10;

        populateTable(BATCH_SIZE);

        pstmt = conn.prepareStatement("delete from Person where id = ?;");

        for (int i = 0; i < BATCH_SIZE; ++i) {
            pstmt.setInt(1, i);

            pstmt.addBatch();
        }

        int[] updCnts = pstmt.executeBatch();

        assertEquals(BATCH_SIZE, updCnts.length, "Invalid update counts size");

        for (int i = 0; i < BATCH_SIZE; ++i)
            assertEquals(1, updCnts[i], "Invalid update count");
    }

    /**
     * @throws SQLException If failed.
     */
    @Test
    public void testBatchDeleteExceptionPrepared() throws Exception {
        final int BATCH_SIZE = 7;

        final int FAILED_IDX = 5;

        populateTable(BATCH_SIZE);

        pstmt = conn.prepareStatement("delete from Person where id = ?;");

        assert FAILED_IDX + 2 == BATCH_SIZE;

        for (int i = 0; i < FAILED_IDX; ++i) {
            pstmt.setInt(1, i);

            pstmt.addBatch();
        }

        pstmt.setString(1, "FAIL");

        pstmt.addBatch();

        pstmt.setInt(1, FAILED_IDX + 1);

        pstmt.addBatch();

        try {
            int[] res = pstmt.executeBatch();

            fail("BatchUpdateException must be thrown res=" + Arrays.toString(res));
        }
        catch (BatchUpdateException e) {
            checkThereAreNotUsedConnections();

            int[] updCnts = e.getUpdateCounts();

            assertEquals(BATCH_SIZE, updCnts.length, "Invalid update counts size");

            for (int i = 0; i < BATCH_SIZE; ++i)
                assertEquals(i != FAILED_IDX ? 1 : Statement.EXECUTE_FAILED, updCnts[i], "Invalid update count");

            if (!e.getMessage().contains("Data conversion error converting \"FAIL\"")) {
                log.error("Invalid exception: ", e);

                fail();
            }

            assertEquals(SqlStateCode.INTERNAL_ERROR, e.getSQLState(), "Invalid SQL state.");
            assertEquals(IgniteQueryErrorCode.UNKNOWN, e.getErrorCode(), "Invalid error code.");

            //assertEquals("Invalid SQL state.", SqlStateCode.CONVERSION_FAILED, e.getSQLState());
            //assertEquals("Invalid error code.", IgniteQueryErrorCode.CONVERSION_FAILED, e.getErrorCode());
        }
    }

    private void checkThereAreNotUsedConnections() {

    }

    /**
     * @throws SQLException If failed.
     */
    @Test
    public void testBatchClearPrepared() throws SQLException {
        final int BATCH_SIZE = 10;

        for (int persIdx = 0; persIdx < BATCH_SIZE; ++persIdx) {
            fillParamsWithPerson(pstmt, persIdx);

            pstmt.addBatch();
        }

        pstmt.clearBatch();

        int[] updates = pstmt.executeBatch();

        assertEquals(0, updates.length, "Returned update counts array should have no elements for empty batch.");

        assertEquals(0L, personsCount(), "Test table should be empty after empty batch is performed.");
    }

    /**
     * @param beginIndex Begin row index.
     * @param cnt Count of rows.
     * @return String contains values for 'cnt' rows.
     */
    private String generateValues(int beginIndex, int cnt) {
        StringBuilder sb = new StringBuilder();

        int lastIdx = beginIndex + cnt - 1;

        for (int i = beginIndex; i < lastIdx; ++i)
            sb.append(valuesRow(i)).append(',');

        sb.append(valuesRow(lastIdx));

        return sb.toString();
    }

    /**
     * @param idx Index of the row.
     * @return String with row values.
     */
    private String valuesRow(int idx) {
        return String.format("('p%d', %d, 'Name%d', 'Lastname%d', %d)", idx, idx, idx, idx, 20 + idx);
    }

    /**
     * Fills PreparedStatement's parameters with fields of some Person generated by index.
     *
     * @param stmt PreparedStatement to fill
     * @param personIdx number to generate Person's fields.
     * @throws SQLException on error.
     */
    private static void fillParamsWithPerson(PreparedStatement stmt, int personIdx) throws SQLException {
        int paramCnt = 1;

        stmt.setString(paramCnt++, "p" + personIdx);
        stmt.setInt(paramCnt++, personIdx);
        stmt.setString(paramCnt++, "Name" + personIdx);
        stmt.setString(paramCnt++, "Lastname" + personIdx);
        stmt.setInt(paramCnt++, 20 + personIdx);
    }

    /**
     * @return How many rows Person table contains.
     * @throws SQLException on error.
     */
    private long personsCount() throws SQLException {
        try (ResultSet cnt = stmt.executeQuery("SELECT COUNT(*) FROM PERSON;")) {
            cnt.next();

            return cnt.getLong(1);
        }
    }

    /**
     * Executes update query on specified connection.
     *
     * @param conn Connection to use.
     * @param updQry sql update query.
     * @throws SQLException on error.
     */
    private static void executeUpdateOn(Connection conn, String updQry) throws SQLException {
        try (Statement statement = conn.createStatement()) {
            statement.executeUpdate(updQry);
        }
    }
}
