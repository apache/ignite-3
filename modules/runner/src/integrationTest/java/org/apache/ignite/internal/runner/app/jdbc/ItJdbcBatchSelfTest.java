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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.sql.BatchUpdateException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import org.apache.ignite.internal.jdbc.proto.IgniteQueryErrorCode;
import org.apache.ignite.internal.jdbc.proto.SqlStateCode;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

/**
 * Statement test.
 */
public class ItJdbcBatchSelfTest extends AbstractJdbcSelfTest {
    /** SQL CREATE TABLE query. */
    private static final String SQL_CREATE = "CREATE TABLE Person(id INT PRIMARY KEY, firstName VARCHAR, lastName VARCHAR, age INT)";

    /** SQL INSERT query. */
    private static final String SQL_PREPARED = "INSERT INTO Person(id, firstName, lastName, age) VALUES "
            + "(?, ?, ?, ?)";

    /** SQL DELETE FROM TABLE query. */
    private static final String SQL_DELETE = "DELETE FROM Person;";

    /** SQL DROP TABLE query. */
    private static final String SQL_DROP_TABLE = "DROP TABLE Person;";

    /** Prepared statement. */
    private PreparedStatement pstmt;

    @BeforeAll
    public static void beforeAll() throws Exception {
        try (Statement statement = conn.createStatement()) {
            statement.executeUpdate(SQL_CREATE);
        }
    }

    @AfterAll
    public static void afterAll() throws Exception {
        try (Statement statement = conn.createStatement()) {
            statement.executeUpdate(SQL_DROP_TABLE);
        }
    }

    /** {@inheritDoc} */
    @BeforeEach
    @Override
    protected void beforeTest(TestInfo testInfo) throws Exception {
        super.beforeTest(testInfo);

        pstmt = conn.prepareStatement(SQL_PREPARED);

        assertNotNull(pstmt);
        assertFalse(pstmt.isClosed());

        try (Statement statement = conn.createStatement()) {
            statement.executeUpdate(SQL_DELETE);
        }
    }

    /** {@inheritDoc} */
    @AfterEach
    @Override
    protected void afterTest(TestInfo testInfo) throws Exception {
        if (pstmt != null && !pstmt.isClosed()) {
            pstmt.close();
        }

        assertTrue(pstmt.isClosed());

        super.afterTest(testInfo);
    }

    @Test
    public void testBatch() throws SQLException {
        final int batchSize = 10;

        for (int idx = 0, i = 0; i < batchSize; ++i, idx += i) {
            stmt.addBatch("insert into Person (id, firstName, lastName, age) values "
                    + generateValues(idx, i + 1));
        }

        int[] updCnts = stmt.executeBatch();

        assertEquals(batchSize, updCnts.length, "Invalid update counts size");

        for (int i = 0; i < batchSize; ++i) {
            assertEquals(i + 1, updCnts[i], "Invalid update count");
        }
    }

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

    @Test
    public void testPreparedStatementBatchException() throws Exception {
        PreparedStatement preparedStatement = conn.prepareStatement("SELECT * FROM Person;");

        preparedStatement.setString(1, "broken");
        preparedStatement.addBatch();

        try {
            preparedStatement.executeBatch();

            fail("BatchUpdateException must be thrown");
        } catch (BatchUpdateException e) {
            int[] updCnts = e.getUpdateCounts();

            assertEquals(0, updCnts.length, "Invalid update counts size");

            if (!e.getMessage().contains("Given statement type does not match that declared by JDBC driver")) {
                log.error("Invalid exception: ", e);

                fail();
            }

            assertEquals(SqlStateCode.INTERNAL_ERROR, e.getSQLState(), "Invalid SQL state.");
            assertEquals(IgniteQueryErrorCode.UNKNOWN, e.getErrorCode(), "Invalid error code.");
        }
    }

    @Test
    public void testBatchException() throws Exception {
        final int successUpdates = 5;

        for (int idx = 0, i = 0; i < successUpdates; ++i, idx += i) {
            stmt.addBatch("insert into Person (id, firstName, lastName, age) values "
                    + generateValues(idx, i + 1));
        }

        stmt.addBatch("select * from Person");

        stmt.addBatch("insert into Person (id, firstName, lastName, age) values "
                + generateValues(100, 7));

        try {
            stmt.executeBatch();

            fail("BatchUpdateException must be thrown");
        } catch (BatchUpdateException e) {
            int[] updCnts = e.getUpdateCounts();

            assertEquals(successUpdates, updCnts.length, "Invalid update counts size");

            for (int i = 0; i < successUpdates; ++i) {
                assertEquals(i + 1, updCnts[i], "Invalid update count");
            }

            if (!e.getMessage().contains("Given statement type does not match that declared by JDBC driver")) {
                log.error("Invalid exception: ", e);

                fail();
            }

            assertEquals(SqlStateCode.INTERNAL_ERROR, e.getSQLState(), "Invalid SQL state.");
            assertEquals(IgniteQueryErrorCode.UNKNOWN, e.getErrorCode(), "Invalid error code.");
        }
    }

    @Test
    public void testBatchParseException() throws Exception {
        final int successUpdates = 5;

        for (int idx = 0, i = 0; i < successUpdates; ++i, idx += i) {
            stmt.addBatch("insert into Person (id, firstName, lastName, age) values "
                    + generateValues(idx, i + 1));
        }

        stmt.addBatch("insert into Person (id, firstName, lastName, age) values ('fail', 1, 1, 1)");

        stmt.addBatch("insert into Person (id, firstName, lastName, age) values "
                + generateValues(100, 7));

        try {
            stmt.executeBatch();

            fail("BatchUpdateException must be thrown");
        } catch (BatchUpdateException e) {
            int[] updCnts = e.getUpdateCounts();

            assertEquals(successUpdates, updCnts.length, "Invalid update counts size");

            for (int i = 0; i < successUpdates; ++i) {
                assertEquals(i + 1, updCnts[i], "Invalid update count: " + i);
            }

            assertEquals(SqlStateCode.INTERNAL_ERROR, e.getSQLState(), "Invalid SQL state.");
            assertEquals(IgniteQueryErrorCode.UNKNOWN, e.getErrorCode(), "Invalid error code.");
        }
    }

    @Test
    public void testBatchMerge() throws SQLException {
        final int batchSize = 5;

        try (Statement statement = conn.createStatement()) {
            statement.executeUpdate("CREATE TABLE Src(id INT PRIMARY KEY, firstName VARCHAR, lastName VARCHAR, age INT)");
        }

        try {
            for (int idx = 0, i = 0; i < batchSize; ++i, idx += i) {
                stmt.addBatch("INSERT INTO Person (id, firstName, lastName, age) values "
                        + generateValues(idx, i + 1));

                stmt.addBatch("INSERT INTO Src (id, firstName, lastName, age) values "
                        + generateValues(idx + 1, i + 1));
            }

            stmt.executeBatch();

            String sql = "MERGE INTO Person dst USING Src src ON dst.id = src.id "
                    + "WHEN MATCHED THEN UPDATE SET firstName = src.firstName "
                    + "WHEN NOT MATCHED THEN INSERT (id, firstName, lastName) VALUES (src.id, src.firstName, src.lastName)";

            stmt.addBatch(sql);

            stmt.addBatch("INSERT INTO Person (id, firstName, lastName, age) values " + valuesRow(100));

            stmt.addBatch(sql);

            int[] updCnts = stmt.executeBatch();

            assertEquals(3, updCnts.length, "Invalid update counts size");

            // result size is equal to mathematical progression:
            assertEquals((1 + batchSize) * batchSize / 2, updCnts[0], "Invalid update counts");
            assertEquals((1 + batchSize) * batchSize / 2, updCnts[2], "Invalid update counts");
        } finally {
            try (Statement statement = conn.createStatement()) {
                statement.executeUpdate("DROP TABLE Src;");
            }
        }
    }

    @Test
    public void testBatchKeyDuplicatesException() throws Exception {
        final int successUpdates = 5;

        int idx = 0;

        for (int i = 0; i < successUpdates; ++i, idx += i) {
            stmt.addBatch("insert into Person (id, firstName, lastName, age) values "
                    + generateValues(idx, i + 1));
        }

        stmt.addBatch("insert into Person (id, firstName, lastName, age) values (0, 'Name0', 'Lastname0', 20)");

        stmt.addBatch("insert into Person (id, firstName, lastName, age) values "
                + generateValues(++idx, 7));

        try {
            stmt.executeBatch();

            fail("BatchUpdateException must be thrown");
        } catch (BatchUpdateException e) {
            int[] updCnts = e.getUpdateCounts();

            assertEquals(successUpdates, updCnts.length, "Invalid update counts size");

            for (int i = 0; i < successUpdates; ++i) {
                assertEquals(i + 1, updCnts[i], "Invalid update count: " + i);
            }

            if (!e.getMessage().contains("Failed to INSERT some keys because they are already in cache")) {
                log.error("Invalid exception: ", e);

                fail();
            }

            assertEquals(SqlStateCode.INTERNAL_ERROR, e.getSQLState(), "Invalid SQL state.");
            assertEquals(IgniteQueryErrorCode.UNKNOWN, e.getErrorCode(), "Invalid error code.");
        }
    }

    @Test
    public void testHeterogeneousBatch() throws SQLException {
        stmt.addBatch("insert into Person (id, firstName, lastName, age) values (0, 'Name0', 'Lastname0', 10)");
        stmt.addBatch("insert into Person (id, firstName, lastName, age) "
                + "values (1, 'Name1', 'Lastname1', 20), (2, 'Name2', 'Lastname2', 30)");
        stmt.addBatch("update Person set firstName = 'AnotherName' where age >= 30");
        stmt.addBatch("delete from Person where age <= 40");

        int[] updCnts = stmt.executeBatch();

        assertEquals(4, updCnts.length, "Invalid update counts size");
        assertArrayEquals(new int[] {1, 2, 1, 3}, updCnts, "Invalid update count");
    }

    @Test
    public void testHeterogeneousBatchException() throws Exception {
        stmt.addBatch("insert into Person (id, firstName, lastName, age) values (0, 'Name0', 'Lastname0', 10)");
        stmt.addBatch("insert into Person (id, firstName, lastName, age) "
                + "values (1, 'Name1', 'Lastname1', 20), (2, 'Name2', 'Lastname2', 30)");
        stmt.addBatch("update Person set id = 'FAIL' where age >= 30"); // Fail.
        stmt.addBatch("delete from Person where FAIL <= 40"); // Fail.

        try {
            stmt.executeBatch();

            fail("BatchUpdateException must be thrown");
        } catch (BatchUpdateException e) {
            int[] updCnts = e.getUpdateCounts();

            assertEquals(2, updCnts.length, "Invalid update counts size");
            assertArrayEquals(new int[] {1, 2}, updCnts, "Invalid update count");
        }
    }

    @Test
    public void testBatchClear() throws SQLException {
        final int batchSize = 7;

        for (int idx = 0, i = 0; i < batchSize; ++i, idx += i) {
            stmt.addBatch("insert into Person (id, firstName, lastName, age) values "
                    + generateValues(idx, i + 1));
        }

        stmt.clearBatch();

        int[] updates = stmt.executeBatch();

        assertEquals(0, updates.length, "Returned update counts array should have no elements for empty batch.");
    }

    @Test
    public void testBatchPrepared() throws SQLException {
        final int batchSize = 10;

        for (int i = 0; i < batchSize; ++i) {
            int paramCnt = 1;

            pstmt.setInt(paramCnt++, i);
            pstmt.setString(paramCnt++, "Name" + i);
            pstmt.setString(paramCnt++, "Lastname" + i);
            pstmt.setInt(paramCnt++, 20 + i);

            pstmt.addBatch();
        }

        int[] updCnts = pstmt.executeBatch();

        assertEquals(batchSize, updCnts.length, "Invalid update counts size");

        for (int i = 0; i < batchSize; ++i) {
            assertEquals(1, updCnts[i], "Invalid update count");
        }
    }

    @Test
    public void testBatchExceptionPrepared() throws Exception {
        final int failedIdx = 5;

        for (int i = 0; i < failedIdx; ++i) {
            int paramCnt = 1;

            pstmt.setInt(paramCnt++, i);
            pstmt.setString(paramCnt++, "Name" + i);
            pstmt.setString(paramCnt++, "Lastname" + i);
            pstmt.setInt(paramCnt++, 20 + i);

            pstmt.addBatch();
        }

        int paramCnt = 1;
        pstmt.setString(paramCnt++, "FAIL");
        pstmt.setString(paramCnt++, "Name" + failedIdx);
        pstmt.setString(paramCnt++, "Lastname" + failedIdx);
        pstmt.setInt(paramCnt++, 20 + failedIdx);

        pstmt.addBatch();

        paramCnt = 1;
        pstmt.setInt(paramCnt++, failedIdx + 1);
        pstmt.setString(paramCnt++, "Name" + failedIdx + 1);
        pstmt.setString(paramCnt++, "Lastname" + failedIdx + 1);
        pstmt.setInt(paramCnt++, 20 + failedIdx + 1);

        pstmt.addBatch();

        try {
            pstmt.executeBatch();

            fail("BatchUpdateException must be thrown");
        } catch (BatchUpdateException e) {
            int[] updCnts = e.getUpdateCounts();

            assertEquals(failedIdx, updCnts.length, "Invalid update counts size");

            for (int i = 0; i < failedIdx; ++i) {
                assertEquals(1, updCnts[i], "Invalid update count");
            }

            assertEquals(SqlStateCode.INTERNAL_ERROR, e.getSQLState(), "Invalid SQL state.");
            assertEquals(IgniteQueryErrorCode.UNKNOWN, e.getErrorCode(), "Invalid error code.");
        }
    }

    /**
     * Populates table 'Person' with entities.
     *
     * @param size Number of entities.
     * @throws SQLException If failed.
     */
    private void populateTable(int size) throws SQLException {
        stmt.addBatch("insert into Person (id, firstName, lastName, age) values "
                + generateValues(0, size));

        stmt.executeBatch();
    }

    @Test
    public void testBatchUpdatePrepared() throws SQLException {
        final int batchSize = 10;

        populateTable(batchSize);

        pstmt = conn.prepareStatement("update Person set age = 100 where id = ?;");

        for (int i = 0; i < batchSize; ++i) {
            pstmt.setInt(1, i);

            pstmt.addBatch();
        }

        int[] updCnts = pstmt.executeBatch();

        assertEquals(batchSize, updCnts.length, "Invalid update counts size");

        for (int i = 0; i < batchSize; ++i) {
            assertEquals(1, updCnts[i], "Invalid update count");
        }
    }

    @Test
    public void testBatchUpdateExceptionPrepared() throws Exception {
        final int batchSize = 7;

        final int successUpdates = 5;

        populateTable(batchSize);

        pstmt = conn.prepareStatement("update Person set age = 100 where id = ?;");

        assert successUpdates + 2 == batchSize;

        for (int i = 0; i < successUpdates; ++i) {
            pstmt.setInt(1, i);

            pstmt.addBatch();
        }

        pstmt.setString(1, "FAIL");

        pstmt.addBatch();

        pstmt.setInt(1, successUpdates + 1);

        pstmt.addBatch();

        try {
            int[] res = pstmt.executeBatch();

            fail("BatchUpdateException must be thrown res=" + Arrays.toString(res));
        } catch (BatchUpdateException e) {
            int[] updCnts = e.getUpdateCounts();

            assertEquals(successUpdates, updCnts.length, "Invalid update counts size");

            for (int i = 0; i < successUpdates; ++i) {
                assertEquals(1, updCnts[i], "Invalid update count[" + i + ']');
            }

            assertEquals(SqlStateCode.INTERNAL_ERROR, e.getSQLState(), "Invalid SQL state.");
            assertEquals(IgniteQueryErrorCode.UNKNOWN, e.getErrorCode(), "Invalid error code.");
        }
    }

    @Test
    public void testBatchDeletePrepared() throws SQLException {
        final int batchSize = 10;

        populateTable(batchSize);

        pstmt = conn.prepareStatement("delete from Person where id = ?;");

        for (int i = 0; i < batchSize; ++i) {
            pstmt.setInt(1, i);

            pstmt.addBatch();
        }

        int[] updCnts = pstmt.executeBatch();

        assertEquals(batchSize, updCnts.length, "Invalid update counts size");

        for (int i = 0; i < batchSize; ++i) {
            assertEquals(1, updCnts[i], "Invalid update count");
        }
    }

    @Test
    public void testBatchDeleteExceptionPrepared() throws Exception {
        final int batchSize = 7;

        final int successUpdates = 5;

        populateTable(batchSize);

        pstmt = conn.prepareStatement("delete from Person where id = ?;");

        assert successUpdates + 2 == batchSize;

        for (int i = 0; i < successUpdates; ++i) {
            pstmt.setInt(1, i);

            pstmt.addBatch();
        }

        pstmt.setString(1, "FAIL");

        pstmt.addBatch();

        pstmt.setInt(1, successUpdates + 1);

        pstmt.addBatch();

        try {
            int[] res = pstmt.executeBatch();

            fail("BatchUpdateException must be thrown res=" + Arrays.toString(res));
        } catch (BatchUpdateException e) {
            int[] updCnts = e.getUpdateCounts();

            assertEquals(successUpdates, updCnts.length, "Invalid update counts size");

            for (int i = 0; i < successUpdates; ++i) {
                assertEquals(1, updCnts[i], "Invalid update count");
            }

            assertEquals(SqlStateCode.INTERNAL_ERROR, e.getSQLState(), "Invalid SQL state.");
            assertEquals(IgniteQueryErrorCode.UNKNOWN, e.getErrorCode(), "Invalid error code.");
        }
    }

    @Test
    public void testBatchClearPrepared() throws SQLException {
        final int batchSize = 10;

        for (int persIdx = 0; persIdx < batchSize; ++persIdx) {
            fillParamsWithPerson(pstmt, persIdx);

            pstmt.addBatch();
        }

        pstmt.clearBatch();

        int[] updates = pstmt.executeBatch();

        assertEquals(0, updates.length, "Returned update counts array should have no elements for empty batch.");

        assertEquals(0L, personsCount(), "Test table should be empty after empty batch is performed.");
    }

    /**
     * Generate values for insert query.
     *
     * @param beginIndex Begin row index.
     * @param cnt Count of rows.
     * @return String contains values for 'cnt' rows.
     */
    private String generateValues(int beginIndex, int cnt) {
        StringBuilder sb = new StringBuilder();

        int lastIdx = beginIndex + cnt - 1;

        for (int i = beginIndex; i < lastIdx; ++i) {
            sb.append(valuesRow(i)).append(',');
        }

        sb.append(valuesRow(lastIdx));

        return sb.toString();
    }

    /**
     * Constructs a string with row values.
     *
     * @param idx Index of the row.
     * @return String with row values.
     */
    private String valuesRow(int idx) {
        return String.format("(%d, 'Name%d', 'Lastname%d', %d)", idx, idx, idx, 20 + idx);
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

        stmt.setInt(paramCnt++, personIdx);
        stmt.setString(paramCnt++, "Name" + personIdx);
        stmt.setString(paramCnt++, "Lastname" + personIdx);
        stmt.setInt(paramCnt++, 20 + personIdx);
    }

    /**
     * Counts persons in table.
     *
     * @return How many rows Person table contains.
     * @throws SQLException on error.
     */
    private long personsCount() throws SQLException {
        try (ResultSet cnt = stmt.executeQuery("SELECT COUNT(*) FROM PERSON;")) {
            cnt.next();

            return cnt.getLong(1);
        }
    }
}
