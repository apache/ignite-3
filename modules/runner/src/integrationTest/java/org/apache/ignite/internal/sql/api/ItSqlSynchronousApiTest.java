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

package org.apache.ignite.internal.sql.api;

import static org.apache.ignite.internal.sql.api.ItSqlAsynchronousApiTest.assertThrowsPublicException;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.assertThrowsWithCause;
import static org.apache.ignite.lang.ErrorGroups.Sql.CURSOR_CLOSED_ERR;
import static org.apache.ignite.lang.ErrorGroups.Sql.QUERY_NO_RESULT_SET_ERR;
import static org.apache.ignite.lang.ErrorGroups.Sql.RUNTIME_ERR;
import static org.apache.ignite.lang.ErrorGroups.Sql.STMT_PARSE_ERR;
import static org.apache.ignite.lang.ErrorGroups.Sql.STMT_VALIDATION_ERR;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.collect.Streams;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import javax.annotation.Nullable;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.sql.engine.ClusterPerClassIntegrationTest;
import org.apache.ignite.internal.tx.TxManager;
import org.apache.ignite.lang.ColumnAlreadyExistsException;
import org.apache.ignite.lang.ColumnNotFoundException;
import org.apache.ignite.lang.ErrorGroups;
import org.apache.ignite.lang.ErrorGroups.Index;
import org.apache.ignite.lang.ErrorGroups.Sql;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.lang.IndexAlreadyExistsException;
import org.apache.ignite.lang.IndexNotFoundException;
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
import org.apache.ignite.sql.SqlRow;
import org.apache.ignite.table.Table;
import org.apache.ignite.tx.Transaction;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

/**
 * Tests for synchronous SQL API.
 */
@SuppressWarnings("ThrowableNotThrown")
public class ItSqlSynchronousApiTest extends ClusterPerClassIntegrationTest {
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

    @Test
    @Disabled("https://issues.apache.org/jira/browse/IGNITE-20096")
    public void ddl() throws Exception {
        IgniteSql sql = igniteSql();
        Session ses = sql.createSession();

        // CREATE TABLE
        checkDdl(true, ses, "CREATE TABLE TEST(ID INT PRIMARY KEY, VAL0 INT)");
        checkError(
                TableAlreadyExistsException.class,
                "Table already exists [name=\"PUBLIC\".\"TEST\"]",
                ses,
                "CREATE TABLE TEST(ID INT PRIMARY KEY, VAL0 INT)"
        );
        checkError(
                SqlException.class,
                ErrorGroups.Table.TABLE_DEFINITION_ERR,
                "Can't create table with duplicate columns: ID, VAL, VAL",
                ses,
                "CREATE TABLE TEST1(ID INT PRIMARY KEY, VAL INT, VAL INT)"
        );
        checkDdl(false, ses, "CREATE TABLE IF NOT EXISTS TEST(ID INT PRIMARY KEY, VAL VARCHAR)");

        // ADD COLUMN
        checkDdl(true, ses, "ALTER TABLE TEST ADD COLUMN VAL1 VARCHAR");
        checkError(
                TableNotFoundException.class,
                "The table does not exist [name=\"PUBLIC\".\"NOT_EXISTS_TABLE\"]",
                ses,
                "ALTER TABLE NOT_EXISTS_TABLE ADD COLUMN VAL1 VARCHAR"
        );
        checkDdl(false, ses, "ALTER TABLE IF EXISTS NOT_EXISTS_TABLE ADD COLUMN VAL1 VARCHAR");
        checkError(
                ColumnAlreadyExistsException.class,
                "Column already exists [name=\"VAL1\"]",
                ses,
                "ALTER TABLE TEST ADD COLUMN VAL1 INT"
        );

        // CREATE INDEX
        checkDdl(true, ses, "CREATE INDEX TEST_IDX ON TEST(VAL0)");
        checkError(
                IndexAlreadyExistsException.class,
                "Index already exists [name=\"PUBLIC\".\"TEST_IDX\"]",
                ses,
                "CREATE INDEX TEST_IDX ON TEST(VAL1)"
        );
        checkDdl(false, ses, "CREATE INDEX IF NOT EXISTS TEST_IDX ON TEST(VAL1)");

        // TODO: IGNITE-19150 We are waiting for schema synchronization to avoid races to create and destroy indexes
        waitForIndexBuild("TEST", "TEST_IDX");

        checkDdl(true, ses, "DROP INDEX TESt_iDX");
        checkDdl(true, ses, "CREATE INDEX TEST_IDX1 ON TEST(VAL0)");
        checkDdl(true, ses, "CREATE INDEX TEST_IDX2 ON TEST(VAL0)");
        checkDdl(true, ses, "CREATE INDEX TEST_IDX3 ON TEST(ID, VAL0, VAL1)");
        checkError(
                SqlException.class,
                Index.INVALID_INDEX_DEFINITION_ERR,
                "Can't create index on duplicate columns: VAL0, VAL0",
                ses,
                "CREATE INDEX TEST_IDX4 ON TEST(VAL0, VAL0)"
        );

        checkError(
                SqlException.class,
                STMT_VALIDATION_ERR,
                "Can`t delete column(s). Column VAL1 is used by indexes [TEST_IDX3].",
                ses,
                "ALTER TABLE TEST DROP COLUMN val1"
        );

        SqlException ex = checkError(
                SqlException.class,
                STMT_VALIDATION_ERR,
                "Can`t delete column(s).",
                ses,
                "ALTER TABLE TEST DROP COLUMN (val0, val1)"
        );

        String msg = ex.getMessage();
        String explainMsg = "Unexpected error message: " + msg;

        assertTrue(msg.contains("Column VAL0 is used by indexes ["), explainMsg);
        assertTrue(msg.contains("TEST_IDX1") && msg.contains("TEST_IDX2") && msg.contains("TEST_IDX3"), explainMsg);
        assertTrue(msg.contains("Column VAL1 is used by indexes [TEST_IDX3]"), explainMsg);

        checkError(
                SqlException.class,
                STMT_VALIDATION_ERR,
                "Can`t delete column, belongs to primary key: [name=ID]",
                ses,
                "ALTER TABLE TEST DROP COLUMN id"
        );

        // TODO: IGNITE-19150 We are waiting for schema synchronization to avoid races to create and destroy indexes
        waitForIndexBuild("TEST", "TEST_IDX3");
        checkDdl(true, ses, "DROP INDEX TESt_iDX3");

        // DROP COLUMNS
        checkDdl(true, ses, "ALTER TABLE TEST DROP COLUMN VAL1");
        checkError(
                TableNotFoundException.class,
                "The table does not exist [name=\"PUBLIC\".\"NOT_EXISTS_TABLE\"]",
                ses,
                "ALTER TABLE NOT_EXISTS_TABLE DROP COLUMN VAL1"
        );
        checkDdl(false, ses, "ALTER TABLE IF EXISTS NOT_EXISTS_TABLE DROP COLUMN VAL1");
        checkError(
                ColumnNotFoundException.class,
                "Column does not exist [tableName=\"PUBLIC\".\"TEST\", columnName=\"VAL1\"]",
                ses,
                "ALTER TABLE TEST DROP COLUMN VAL1"
        );

        // DROP TABLE
        checkDdl(false, ses, "DROP TABLE IF EXISTS NOT_EXISTS_TABLE");

        checkDdl(true, ses, "DROP TABLE TEST");
        checkError(
                TableNotFoundException.class,
                "The table does not exist [name=\"PUBLIC\".\"TEST\"]",
                ses,
                "DROP TABLE TEST"
        );

        checkDdl(false, ses, "DROP INDEX IF EXISTS TEST_IDX");

        checkError(
                IndexNotFoundException.class,
                "Index does not exist [name=\"PUBLIC\".\"TEST_IDX\"]", ses,
                "DROP INDEX TEST_IDX"
        );
    }

    @Test
    public void dml() {
        sql("CREATE TABLE TEST(ID INT PRIMARY KEY, VAL0 INT)");

        IgniteSql sql = igniteSql();
        Session ses = sql.createSession();

        TxManager txManagerInternal = txManager();

        int txPrevCnt = txManagerInternal.finished();

        for (int i = 0; i < ROW_COUNT; ++i) {
            checkDml(1, ses, "INSERT INTO TEST VALUES (?, ?)", i, i);
        }

        assertEquals(ROW_COUNT, txManagerInternal.finished() - txPrevCnt);

        assertEquals(0, txManagerInternal.pending());

        checkDml(ROW_COUNT, ses, "UPDATE TEST SET VAL0 = VAL0 + ?", 1);

        checkDml(ROW_COUNT, ses, "DELETE FROM TEST WHERE VAL0 >= 0");
    }

    @SuppressWarnings("UnstableApiUsage")
    @Test
    public void select() throws Exception {
        sql("CREATE TABLE TEST(ID INT PRIMARY KEY, VAL0 INT)");
        for (int i = 0; i < ROW_COUNT; ++i) {
            sql("INSERT INTO TEST VALUES (?, ?)", i, i);
        }

        IgniteSql sql = igniteSql();
        Session ses = sql.sessionBuilder().defaultPageSize(ROW_COUNT / 4).build();

        ResultSet<SqlRow> rs = ses.execute(null, "SELECT ID FROM TEST");

        Set<Integer> set = Streams.stream(rs).map(r -> r.intValue(0)).collect(Collectors.toSet());

        rs.close();

        for (int i = 0; i < ROW_COUNT; ++i) {
            assertTrue(set.remove(i), "Results invalid: " + rs);
        }

        assertTrue(set.isEmpty());
    }

    @Test
    public void errors() throws InterruptedException {
        sql("CREATE TABLE TEST(ID INT PRIMARY KEY, VAL0 INT NOT NULL)");
        for (int i = 0; i < ROW_COUNT; ++i) {
            sql("INSERT INTO TEST VALUES (?, ?)", i, i);
        }

        IgniteSql sql = igniteSql();
        Session ses = sql.sessionBuilder().defaultPageSize(2).build();

        // Parse error.
        checkError(SqlException.class, STMT_PARSE_ERR, "Failed to parse query", ses, "SELECT ID FROM");

        // Validation errors.
        checkError(SqlException.class, STMT_VALIDATION_ERR, "Column 'VAL0' does not allow NULLs", ses,
                "INSERT INTO TEST VALUES (2, NULL)");

        checkError(SqlException.class, STMT_VALIDATION_ERR, "Object 'NOT_EXISTING_TABLE' not found", ses,
                "SELECT * FROM NOT_EXISTING_TABLE");

        checkError(SqlException.class, STMT_VALIDATION_ERR, "Column 'NOT_EXISTING_COLUMN' not found", ses,
                "SELECT NOT_EXISTING_COLUMN FROM TEST");

        checkError(SqlException.class, STMT_VALIDATION_ERR, "Multiple statements are not allowed", ses, "SELECT 1; SELECT 2");

        checkError(SqlException.class, STMT_VALIDATION_ERR, "Table without PRIMARY KEY is not supported", ses,
                "CREATE TABLE TEST2 (VAL INT)");

        // Execute error.
        checkError(SqlException.class, RUNTIME_ERR, "/ by zero", ses, "SELECT 1 / ?", 0);
        checkError(SqlException.class, RUNTIME_ERR, "negative substring length not allowed", ses, "SELECT SUBSTRING('foo', 1, -3)");

        // No result set error.
        {
            ResultSet rs = ses.execute(null, "CREATE TABLE TEST3 (ID INT PRIMARY KEY)");
            assertThrowsPublicException(rs::next, NoRowSetExpectedException.class, QUERY_NO_RESULT_SET_ERR, "Query has no result set");
        }

        // Cursor closed error.
        {
            ResultSet rs = ses.execute(null, "SELECT * FROM TEST");
            Thread.sleep(300); // ResultSetImpl fetches next page in background, wait to it to complete to avoid flakiness.
            rs.close();
            assertThrowsPublicException(() -> rs.forEachRemaining(Object::hashCode),
                    CursorClosedException.class, CURSOR_CLOSED_ERR, null);
        }
    }

    /**
     * DDL is non-transactional.
     */
    @Test
    public void ddlInTransaction() {
        Session ses = igniteSql().createSession();
        sql("CREATE TABLE TEST(ID INT PRIMARY KEY, VAL0 INT)");

        {
            Transaction tx = igniteTx().begin();
            try {
                assertThrowsWithCause(() -> ses.execute(tx, "CREATE TABLE TEST2(ID INT PRIMARY KEY, VAL0 INT)"),
                        SqlException.class,
                        "DDL doesn't support transactions."
                );
            } finally {
                tx.rollback();
            }
        }
        {
            Transaction tx = igniteTx().begin();
            ResultSet<SqlRow> res = ses.execute(tx, "INSERT INTO TEST VALUES (?, ?)", -1, -1);
            assertEquals(1, res.affectedRows());

            assertThrowsWithCause(() -> ses.execute(tx, "CREATE TABLE TEST2(ID INT PRIMARY KEY, VAL0 INT)"),
                    SqlException.class,
                    "DDL doesn't support transactions."
            );
            tx.commit();

            assertEquals(1, sql("SELECT ID FROM TEST WHERE ID = -1").size());
        }

        assertEquals(0, ((IgniteImpl) CLUSTER_NODES.get(0)).txManager().pending());
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

        SqlBatchException batchEx = assertThrows(
                SqlBatchException.class,
                () -> ses.executeBatch(null, "INSERT INTO TEST VALUES (?, ?)", args)
        );

        assertEquals(Sql.CONSTRAINT_VIOLATION_ERR, batchEx.code());
        assertEquals(err, batchEx.updateCounters().length);
        IntStream.range(0, batchEx.updateCounters().length).forEach(i -> assertEquals(1, batchEx.updateCounters()[i]));
    }

    @Test
    public void resultSetCloseShouldFinishImplicitTransaction() {
        sql("CREATE TABLE TEST(ID INT PRIMARY KEY, VAL0 INT)");
        for (int i = 0; i < ROW_COUNT; ++i) {
            sql("INSERT INTO TEST VALUES (?, ?)", i, i);
        }

        IgniteSql sql = igniteSql();
        Session ses = sql.sessionBuilder().defaultPageSize(2).build();

        ResultSet<?> rs = ses.execute(null, "SELECT * FROM TEST");
        assertEquals(1, txManager().pending());
        rs.close();
        assertEquals(0, txManager().pending(), "Expected no pending transactions");
    }

    @Test
    public void resultSetFullReadShouldFinishImplicitTransaction() {
        sql("CREATE TABLE TEST(ID INT PRIMARY KEY, VAL0 INT)");
        for (int i = 0; i < ROW_COUNT; ++i) {
            sql("INSERT INTO TEST VALUES (?, ?)", i, i);
        }

        IgniteSql sql = igniteSql();

        // Fetch all data in one read.
        Session ses = sql.sessionBuilder().defaultPageSize(100).build();
        ResultSet<SqlRow> rs = ses.execute(null, "SELECT * FROM TEST");

        while (rs.hasNext()) {
            rs.next();
        }

        assertEquals(0, txManager().pending(), "Expected no pending transactions");
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

    private static <T extends IgniteException> T checkError(Class<T> expCls, String msg, Session ses, String sql, Object... args) {
        return checkError(expCls, null, msg, ses, sql, args);
    }

    private static <T extends IgniteException> T checkError(
            Class<T> expCls,
            @Nullable Integer code,
            @Nullable String msg,
            Session ses,
            String sql,
            Object... args
    ) {
        return assertThrowsPublicException(() -> ses.execute(null, sql, args), expCls, code, msg);
    }

    static void checkDml(int expectedAffectedRows, Session ses, String sql, Object... args) {
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
