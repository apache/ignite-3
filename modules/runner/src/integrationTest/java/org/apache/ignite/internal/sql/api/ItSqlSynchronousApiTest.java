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

package org.apache.ignite.internal.sql.api;

import static org.apache.ignite.internal.testframework.IgniteTestUtils.assertThrowsWithCause;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.cause;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.hasCause;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.collect.Streams;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.sql.engine.AbstractBasicIntegrationTest;
import org.apache.ignite.internal.testframework.IgniteTestUtils;
import org.apache.ignite.internal.tx.TxManager;
import org.apache.ignite.internal.tx.TxState;
import org.apache.ignite.internal.tx.impl.TxManagerImpl;
import org.apache.ignite.lang.ColumnAlreadyExistsException;
import org.apache.ignite.lang.ColumnNotFoundException;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.lang.IgniteInternalException;
import org.apache.ignite.lang.IndexAlreadyExistsException;
import org.apache.ignite.lang.TableAlreadyExistsException;
import org.apache.ignite.lang.TableNotFoundException;
import org.apache.ignite.sql.BatchedArguments;
import org.apache.ignite.sql.CursorClosedException;
import org.apache.ignite.sql.IgniteSql;
import org.apache.ignite.sql.NoRowSetExpectedException;
import org.apache.ignite.sql.ResultSet;
import org.apache.ignite.sql.Session;
import org.apache.ignite.sql.SqlBatchException;
import org.apache.ignite.sql.SqlException;
import org.apache.ignite.table.Table;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

/**
 * Tests for synchronous SQL API.
 */
@SuppressWarnings("ThrowableNotThrown")
public class ItSqlSynchronousApiTest extends AbstractBasicIntegrationTest {
    private static final int ROW_COUNT = 16;

    /**
     * Clear tables after each test.
     *
     * @param testInfo Test information object.
     * @throws Exception If failed.
     */
    @AfterEach
    @Override
    public void tearDown(TestInfo testInfo) throws Exception {
        for (Table t : CLUSTER_NODES.get(0).tables().tables()) {
            sql("DROP TABLE " + t.name());
        }

        tearDownBase(testInfo);
    }

    /**
     * Gets the SQL API.
     *
     * @return SQL API.
     */
    protected IgniteSql igniteSql() {
        return CLUSTER_NODES.get(0).sql();
    }

    @Test
    public void ddl() {
        IgniteSql sql = igniteSql();
        Session ses = sql.createSession();

        // CREATE TABLE
        checkDdl(true, ses, "CREATE TABLE TEST(ID INT PRIMARY KEY, VAL0 INT)");
        checkError(
                TableAlreadyExistsException.class,
                "Table already exists [name=PUBLIC.TEST]",
                ses,
                "CREATE TABLE TEST(ID INT PRIMARY KEY, VAL0 INT)"
        );
        checkDdl(false, ses, "CREATE TABLE IF NOT EXISTS TEST(ID INT PRIMARY KEY, VAL VARCHAR)");

        // ADD COLUMN
        checkDdl(true, ses, "ALTER TABLE TEST ADD COLUMN IF NOT EXISTS VAL1 VARCHAR");
        checkError(
                TableNotFoundException.class,
                "Table does not exist [name=PUBLIC.NOT_EXISTS_TABLE]",
                ses,
                "ALTER TABLE NOT_EXISTS_TABLE ADD COLUMN VAL1 VARCHAR"
        );
        checkDdl(false, ses, "ALTER TABLE IF EXISTS NOT_EXISTS_TABLE ADD COLUMN VAL1 VARCHAR");
        checkError(
                ColumnAlreadyExistsException.class,
                "Column already exists [name=VAL1]",
                ses,
                "ALTER TABLE TEST ADD COLUMN VAL1 INT"
        );
        checkDdl(false, ses, "ALTER TABLE TEST ADD COLUMN IF NOT EXISTS VAL1 INT");

        // CREATE INDEX
        checkDdl(true, ses, "CREATE INDEX TEST_IDX ON TEST(VAL0)");
        checkError(
                IndexAlreadyExistsException.class,
                "Index already exists [name=TEST_IDX]",
                ses,
                "CREATE INDEX TEST_IDX ON TEST(VAL1)"
        );
        checkDdl(false, ses, "CREATE INDEX IF NOT EXISTS TEST_IDX ON TEST(VAL1)");

        // DROP COLUMNS
        checkDdl(true, ses, "ALTER TABLE TEST DROP COLUMN VAL1");
        checkError(
                TableNotFoundException.class,
                "Table does not exist [name=PUBLIC.NOT_EXISTS_TABLE]",
                ses,
                "ALTER TABLE NOT_EXISTS_TABLE DROP COLUMN VAL1"
        );
        checkDdl(false, ses, "ALTER TABLE IF EXISTS NOT_EXISTS_TABLE DROP COLUMN VAL1");
        checkError(
                ColumnNotFoundException.class,
                "Column 'VAL1' does not exist in table '\"PUBLIC\".\"TEST\"'",
                ses,
                "ALTER TABLE TEST DROP COLUMN VAL1"
        );
        checkDdl(false, ses, "ALTER TABLE TEST DROP COLUMN IF EXISTS VAL1");

        // DROP TABLE
        checkDdl(false, ses, "DROP TABLE IF EXISTS NOT_EXISTS_TABLE");
        checkDdl(true, ses, "DROP TABLE TEST");
        checkError(
                TableNotFoundException.class,
                "Table does not exist [name=PUBLIC.TEST]",
                ses,
                "DROP TABLE TEST"
        );
    }

    @Test
    public void dml() {
        sql("CREATE TABLE TEST(ID INT PRIMARY KEY, VAL0 INT)");

        IgniteSql sql = igniteSql();
        Session ses = sql.createSession();

        TxManager txManagerInternal = (TxManager) IgniteTestUtils.getFieldValue(CLUSTER_NODES.get(0), IgniteImpl.class, "txManager");

        int txPrevCnt = txManagerInternal.finished();

        for (int i = 0; i < ROW_COUNT; ++i) {
            checkDml(1, ses, "INSERT INTO TEST VALUES (?, ?)", i, i);
        }

        assertEquals(ROW_COUNT, txManagerInternal.finished() - txPrevCnt);

        var states = (Map<UUID, TxState>) IgniteTestUtils.getFieldValue(txManagerInternal, TxManagerImpl.class, "states");

        states.forEach((k, v) -> assertNotSame(v, TxState.PENDING));

        checkDml(ROW_COUNT, ses, "UPDATE TEST SET VAL0 = VAL0 + ?", 1);

        checkDml(ROW_COUNT, ses, "DELETE FROM TEST WHERE VAL0 >= 0");
    }

    @SuppressWarnings("UnstableApiUsage")
    @Test
    public void select() throws ExecutionException, InterruptedException {
        sql("CREATE TABLE TEST(ID INT PRIMARY KEY, VAL0 INT)");
        for (int i = 0; i < ROW_COUNT; ++i) {
            sql("INSERT INTO TEST VALUES (?, ?)", i, i);
        }

        IgniteSql sql = igniteSql();
        Session ses = sql.sessionBuilder().defaultPageSize(ROW_COUNT / 4).build();

        ResultSet rs = ses.execute(null, "SELECT ID FROM TEST");

        Set<Integer> set = Streams.stream(rs).map(r -> r.intValue(0)).collect(Collectors.toSet());

        rs.close();

        for (int i = 0; i < ROW_COUNT; ++i) {
            assertTrue(set.remove(i), "Results invalid: " + rs);
        }

        assertTrue(set.isEmpty());
    }

    @Test
    public void errors() throws InterruptedException {
        sql("CREATE TABLE TEST(ID INT PRIMARY KEY, VAL0 INT)");
        for (int i = 0; i < ROW_COUNT; ++i) {
            sql("INSERT INTO TEST VALUES (?, ?)", i, i);
        }

        IgniteSql sql = igniteSql();
        Session ses = sql.sessionBuilder().defaultPageSize(2).build();

        // Parse error.
        assertThrowsWithCause(
                () -> ses.execute(null, "SELECT ID FROM"),
                SqlException.class,
                "Failed to parse query"
        );

        // Multiple statements error.
        assertThrowsWithCause(
                () -> ses.execute(null, "SELECT 1; SELECT 2"),
                SqlException.class,
                "Multiple statements aren't allowed"
        );

        // Planning error.
        assertThrowsWithCause(
                () -> ses.execute(null, "CREATE TABLE TEST2 (VAL INT)"),
                SqlException.class,
                "Table without PRIMARY KEY is not supported"
        );

        // Execute error.
        assertThrowsWithCause(
                () -> ses.execute(null, "SELECT 1 / ?", 0),
                IgniteException.class,
                "/ by zero"
        );

        // No result set error.
        {
            ResultSet rs = ses.execute(null, "CREATE TABLE TEST3 (ID INT PRIMARY KEY)");
            assertThrowsWithCause(rs::next, NoRowSetExpectedException.class, "Query has no result set");
        }

        // Cursor closed error.
        {
            ResultSet rs = ses.execute(null, "SELECT * FROM TEST");
            Thread.sleep(300); // ResultSetImpl fetches next page in background, wait to it to complete to avoid flakiness.
            rs.close();
            assertThrowsWithCause(() -> rs.forEachRemaining(Object::hashCode), CursorClosedException.class);
        }
    }

    @Test
    public void batch() {
        sql("CREATE TABLE TEST(ID INT PRIMARY KEY, VAL0 INT)");

        IgniteSql sql = CLUSTER_NODES.get(0).sql();
        Session ses = sql.sessionBuilder().defaultPageSize(ROW_COUNT / 2).build();

        BatchedArguments args = BatchedArguments.of(0, 0);

        for (int i = 1; i < ROW_COUNT; ++i) {
            args.add(i, i);
        }

        long[] batchRes = ses.executeBatch(null, "INSERT INTO TEST VALUES (?, ?)", args);

        Arrays.stream(batchRes).forEach(r -> assertEquals(1L, r));

        // Check that data are inserted OK
        List<List<Object>> res = sql("SELECT ID FROM TEST ORDER BY ID");
        IntStream.range(0, ROW_COUNT).forEach(i -> assertEquals(i, res.get(i).get(0)));

        // Check invalid query type
        assertThrowsWithCause(
                () -> ses.executeBatch(null, "SELECT * FROM TEST", args),
                SqlException.class,
                "Invalid SQL statement type in the batch"
        );

        assertThrowsWithCause(
                () -> ses.executeBatch(null, "CREATE TABLE TEST1(ID INT PRIMARY KEY, VAL0 INT)", args),
                SqlException.class,
                "Invalid SQL statement type in the batch"
        );
    }

    @Test
    public void batchIncomplete() {
        int err = ROW_COUNT / 2;

        sql("CREATE TABLE TEST(ID INT PRIMARY KEY, VAL0 INT)");

        IgniteSql sql = CLUSTER_NODES.get(0).sql();
        Session ses = sql.sessionBuilder().defaultPageSize(ROW_COUNT / 2).build();

        BatchedArguments args = BatchedArguments.of(0, 0);

        for (int i = 1; i < ROW_COUNT; ++i) {
            if (i == err) {
                args.add(1, 1);
            } else {
                args.add(i, i);
            }
        }

        Throwable ex = assertThrowsWithCause(
                () -> ses.executeBatch(null, "INSERT INTO TEST VALUES (?, ?)", args),
                SqlBatchException.class
        );
        assertTrue(hasCause(ex, IgniteInternalException.class, "Failed to INSERT some keys because they are already in cache"));
        SqlBatchException batchEx = cause(ex, SqlBatchException.class, null);

        assertEquals(err, batchEx.updateCounters().length);
        IntStream.range(0, batchEx.updateCounters().length).forEach(i -> assertEquals(1, batchEx.updateCounters()[i]));
    }

    private static void checkDdl(boolean expectedApplied, Session ses, String sql) {
        ResultSet res = ses.execute(
                null,
                sql
        );

        assertEquals(expectedApplied, res.wasApplied());
        assertFalse(res.hasRowSet());
        assertEquals(-1, res.affectedRows());

        res.close();
    }

    private static void checkError(Class<? extends Throwable> expectedException, String msg, Session ses, String sql) {
        assertThrowsWithCause(() -> ses.execute(null, sql), expectedException, msg);
    }

    protected static void checkDml(int expectedAffectedRows, Session ses, String sql, Object... args) {
        ResultSet res = ses.execute(
                null,
                sql,
                args
        );

        assertFalse(res.wasApplied());
        assertFalse(res.hasRowSet());
        assertEquals(expectedAffectedRows, res.affectedRows());

        res.close();
    }
}
