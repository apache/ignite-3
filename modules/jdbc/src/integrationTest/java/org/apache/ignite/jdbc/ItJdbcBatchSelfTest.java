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

import static org.apache.ignite.jdbc.util.JdbcTestUtils.assertThrowsSqlException;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.sql.BatchUpdateException;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.Arrays;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.ignite.internal.jdbc.JdbcPreparedStatement;
import org.apache.ignite.internal.jdbc.JdbcStatement;
import org.apache.ignite.internal.jdbc.proto.IgniteQueryErrorCode;
import org.apache.ignite.internal.jdbc.proto.SqlStateCode;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

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
    protected void beforeTest() throws Exception {
        pstmt = conn.prepareStatement(SQL_PREPARED);

        assertNotNull(pstmt);
        assertFalse(pstmt.isClosed());

        try (Statement statement = conn.createStatement()) {
            statement.executeUpdate(SQL_DELETE);
        }
    }

    /** {@inheritDoc} */
    @AfterEach
    protected void afterTest() throws Exception {
        if (pstmt != null && !pstmt.isClosed()) {
            pstmt.close();
        }

        assertTrue(pstmt.isClosed());
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
    public void testMultipleStatementForBatchIsNotAllowed() throws SQLException {
        String insertStmt = "insert into Person (id, firstName, lastName, age) values";
        String ins1 = insertStmt + valuesRow(1);
        String ins2 = insertStmt + valuesRow(2);

        stmt.addBatch(ins1 + ";" + ins2);

        assertThrowsSqlException(
                BatchUpdateException.class,
                "Multiple statements are not allowed.",
                () -> stmt.executeBatch());
    }

    @Test
    public void testBatchOnClosedStatement() throws SQLException {
        Statement stmt2 = conn.createStatement();
        PreparedStatement pstmt2 = conn.prepareStatement("");

        stmt2.close();
        pstmt2.close();

        assertThrowsSqlException("Statement is closed.", () -> stmt2.addBatch(""));

        assertThrowsSqlException("Statement is closed.", stmt2::clearBatch);

        assertThrowsSqlException("Statement is closed.", stmt2::executeBatch);

        assertThrowsSqlException("Statement is closed.", pstmt2::addBatch);

        assertThrowsSqlException("Statement is closed.", pstmt2::clearBatch);

        assertThrowsSqlException("Statement is closed.", pstmt2::executeBatch);
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

            assertThat(e.getMessage(), containsString("Invalid SQL statement type"));

            assertEquals(SqlStateCode.INTERNAL_ERROR, e.getSQLState(), "Invalid SQL state.");
            assertEquals(IgniteQueryErrorCode.UNKNOWN, e.getErrorCode(), "Invalid error code.");
        }
    }

    @Test
    public void testPreparedStatementSupplyParametersToQueryWithoutParameters() throws Exception {
        PreparedStatement preparedStatement = conn.prepareStatement("UPDATE Person SET firstName='NONE'");

        preparedStatement.setString(1, "broken");
        preparedStatement.addBatch();

        try {
            preparedStatement.executeBatch();

            fail("BatchUpdateException must be thrown");
        } catch (BatchUpdateException e) {
            int[] updCnts = e.getUpdateCounts();

            assertEquals(0, updCnts.length, "Invalid update counts size");

            assertThat(e.getMessage(),
                    containsString("Unexpected number of query parameters. Provided 1 but there is only 0 dynamic parameter(s)."));

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

        BatchUpdateException e = assertThrowsSqlException(
                BatchUpdateException.class,
                "Invalid SQL statement type",
                stmt::executeBatch);

        int[] updCnts = e.getUpdateCounts();

        assertEquals(successUpdates, updCnts.length, "Invalid update counts size");

        for (int i = 0; i < successUpdates; ++i) {
            assertEquals(i + 1, updCnts[i], "Invalid update count");
        }

        assertEquals(SqlStateCode.INTERNAL_ERROR, e.getSQLState(), "Invalid SQL state.");
        assertEquals(IgniteQueryErrorCode.UNKNOWN, e.getErrorCode(), "Invalid error code.");
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

            assertThat(e.toString(), e.getMessage(), containsString("PK unique constraint is violated"));

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
        assertArrayEquals(new int[]{1, 2, 1, 3}, updCnts, "Invalid update count");
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
            assertArrayEquals(new int[]{1, 2}, updCnts, "Invalid update count");
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
    public void testDataTypes() throws SQLException {
        Statement stmt0 = conn.createStatement();

        stmt0.executeUpdate("CREATE TABLE timetypes (tt_id int, "
                + "tt_date date, "
                + "tt_time time, "
                + "tt_timestamp timestamp, "
                + "tt_timestamp_tz timestamp with local time zone, "
                + "PRIMARY KEY (tt_id));");

        PreparedStatement prepStmt = conn.prepareStatement(
                "INSERT INTO timetypes(tt_id, tt_date, tt_time, tt_timestamp, tt_timestamp_tz)"
                        + " VALUES (?, ?, ?, ?, ?)");

        Date date = Date.valueOf(LocalDate.now());
        Time time = Time.valueOf(LocalTime.now());
        Timestamp ts = new Timestamp(System.currentTimeMillis());

        int idx = 1;
        prepStmt.setLong(idx++, 1);
        prepStmt.setDate(idx++, date);
        prepStmt.setTime(idx++, time);
        prepStmt.setTimestamp(idx++, ts);
        prepStmt.setTimestamp(idx, ts);
        prepStmt.addBatch();

        prepStmt.executeBatch();
        prepStmt.close();

        ResultSet res = stmt0.executeQuery("SELECT * FROM timetypes");
        res.next();
        assertEquals(date, res.getDate(2));
        assertEquals(time, res.getTime(3));
        assertEquals(ts, res.getTimestamp(4));
        assertEquals(ts, res.getTimestamp(5));

        stmt0.execute("DROP TABLE timetypes");
        stmt0.close();
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

    @Test
    public void testPreparedBatchTimeout() throws SQLException {
        JdbcPreparedStatement igniteStmt = pstmt.unwrap(JdbcPreparedStatement.class);

        {
            // Disable timeout
            igniteStmt.timeout(0);

            for (int persIdx = 100; persIdx < 103; ++persIdx) {
                fillParamsWithPerson(pstmt, persIdx);

                igniteStmt.addBatch();
            }

            int[] updated = igniteStmt.executeBatch();
            assertEquals(3, updated.length);
        }

        // Each statement in a batch is executed separately, and timeout is applied to each statement.
        {
            int timeoutMillis = ThreadLocalRandom.current().nextInt(1, 5);
            igniteStmt.timeout(timeoutMillis);

            for (int persIdx = 200; persIdx < 300; ++persIdx) {
                fillParamsWithPerson(pstmt, persIdx);

                igniteStmt.addBatch();
            }

            assertThrowsSqlException(SQLException.class,
                    "Query timeout", igniteStmt::executeBatch);
        }

        {
            // Disable timeout
            igniteStmt.timeout(0);

            for (int persIdx = 300; persIdx < 303; ++persIdx) {
                fillParamsWithPerson(pstmt, persIdx);

                igniteStmt.addBatch();
            }

            int[] updated = igniteStmt.executeBatch();
            assertEquals(3, updated.length);
        }
    }

    @Test
    public void testBatchTimeout() throws SQLException {
        JdbcStatement igniteStmt = stmt.unwrap(JdbcStatement.class);

        {
            // Disable timeout
            igniteStmt.timeout(0);

            for (int persIdx = 0; persIdx < 3; persIdx++) {
                String stmt = "insert into Person (id, firstName, lastName, age) values " + generateValues(persIdx, 1);
                igniteStmt.addBatch(stmt);
            }

            int[] updated = igniteStmt.executeBatch();
            assertEquals(3, updated.length);
        }

        // Each statement in a batch is executed separately, so a timeout is applied to each statement.
        {
            igniteStmt.timeout(1);

            for (int persIdx = 200; persIdx < 300; ++persIdx) {
                String stmt = "insert into Person (id, firstName, lastName, age) values " + generateValues(persIdx, 1);
                igniteStmt.addBatch(stmt);
            }

            assertThrowsSqlException(SQLException.class,
                    "Query timeout", igniteStmt::executeBatch);
        }

        {
            // Disable timeout
            igniteStmt.timeout(0);

            for (int persIdx = 10; persIdx < 13; persIdx++) {
                String stmt = "insert into Person (id, firstName, lastName, age) values " + generateValues(persIdx, 1);
                igniteStmt.addBatch(stmt);
            }

            int[] updated = igniteStmt.executeBatch();
            assertEquals(3, updated.length);
        }
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
