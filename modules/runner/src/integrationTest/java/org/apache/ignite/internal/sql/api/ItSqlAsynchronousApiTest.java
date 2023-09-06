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

import static org.apache.ignite.internal.sql.engine.util.QueryChecker.containsIndexScan;
import static org.apache.ignite.internal.sql.engine.util.QueryChecker.containsTableScan;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.assertThrowsWithCause;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.await;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.apache.ignite.lang.ErrorGroups.Sql.CONSTRAINT_VIOLATION_ERR;
import static org.apache.ignite.lang.ErrorGroups.Sql.CURSOR_CLOSED_ERR;
import static org.apache.ignite.lang.ErrorGroups.Sql.EXECUTION_CANCELLED_ERR;
import static org.apache.ignite.lang.ErrorGroups.Sql.QUERY_NO_RESULT_SET_ERR;
import static org.apache.ignite.lang.ErrorGroups.Sql.RUNTIME_ERR;
import static org.apache.ignite.lang.ErrorGroups.Sql.SESSION_CLOSED_ERR;
import static org.apache.ignite.lang.ErrorGroups.Sql.STMT_PARSE_ERR;
import static org.apache.ignite.lang.ErrorGroups.Sql.STMT_VALIDATION_ERR;
import static org.apache.ignite.lang.ErrorGroups.Transactions.TX_FAILED_READ_WRITE_OPERATION_ERR;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.apache.ignite.internal.catalog.commands.CatalogUtils;
import org.apache.ignite.internal.client.sql.ClientSql;
import org.apache.ignite.internal.sql.api.ColumnMetadataImpl.ColumnOriginImpl;
import org.apache.ignite.internal.sql.engine.ClusterPerClassIntegrationTest;
import org.apache.ignite.internal.sql.engine.exec.ExecutionCancelledException;
import org.apache.ignite.internal.testframework.IgniteTestUtils;
import org.apache.ignite.internal.testframework.IgniteTestUtils.RunnableX;
import org.apache.ignite.internal.tx.TxManager;
import org.apache.ignite.internal.util.CollectionUtils;
import org.apache.ignite.lang.ColumnAlreadyExistsException;
import org.apache.ignite.lang.ColumnNotFoundException;
import org.apache.ignite.lang.ErrorGroups;
import org.apache.ignite.lang.ErrorGroups.Index;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.lang.IndexAlreadyExistsException;
import org.apache.ignite.lang.IndexNotFoundException;
import org.apache.ignite.lang.TableAlreadyExistsException;
import org.apache.ignite.lang.TableNotFoundException;
import org.apache.ignite.sql.BatchedArguments;
import org.apache.ignite.sql.ColumnMetadata;
import org.apache.ignite.sql.ColumnType;
import org.apache.ignite.sql.CursorClosedException;
import org.apache.ignite.sql.IgniteSql;
import org.apache.ignite.sql.NoRowSetExpectedException;
import org.apache.ignite.sql.ResultSetMetadata;
import org.apache.ignite.sql.Session;
import org.apache.ignite.sql.SqlBatchException;
import org.apache.ignite.sql.SqlException;
import org.apache.ignite.sql.SqlRow;
import org.apache.ignite.sql.async.AsyncResultSet;
import org.apache.ignite.table.Table;
import org.apache.ignite.tx.Transaction;
import org.apache.ignite.tx.TransactionOptions;
import org.hamcrest.Matcher;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

/**
 * Tests for asynchronous SQL API.
 */
@SuppressWarnings("ThrowableNotThrown")
public class ItSqlAsynchronousApiTest extends ClusterPerClassIntegrationTest {
    private static final int ROW_COUNT = 16;

    @AfterEach
    public void dropTables() {
        for (Table t : CLUSTER_NODES.get(0).tables().tables()) {
            sql("DROP TABLE " + t.name());
        }
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

        for (int i = 0; i < ROW_COUNT; ++i) {
            checkDml(1, ses, "INSERT INTO TEST VALUES (?, ?)", i, i);
        }

        checkDml(ROW_COUNT, ses, "UPDATE TEST SET VAL0 = VAL0 + ?", 1);

        checkDml(ROW_COUNT, ses, "DELETE FROM TEST WHERE VAL0 >= 0");
    }

    /** Check all transactions are processed correctly even with case of sql Exception raised. */
    @Test
    public void implicitTransactionsStates() {
        IgniteSql sql = igniteSql();

        if (sql instanceof ClientSql) {
            return;
        }

        sql("CREATE TABLE TEST(ID INT PRIMARY KEY, VAL0 INT)");

        Session ses = sql.createSession();

        TxManager txManager = txManager();

        for (int i = 0; i < ROW_COUNT; ++i) {
            CompletableFuture<AsyncResultSet<SqlRow>> fut = ses.executeAsync(null, "CREATE TABLE TEST(ID INT PRIMARY KEY, VAL0 INT)", i, i);

            AsyncResultSet asyncRes = null;

            try {
                asyncRes = await(fut);
            } catch (Throwable ignore) {
                // No op.
            }

            if (asyncRes != null) {
                await(asyncRes.closeAsync());
            }
        }

        // No new transactions through ddl.
        assertEquals(0, txManager.pending());
    }

    /** Check correctness of explicit transaction rollback. */
    @Test
    public void checkExplicitTxRollback() {
        IgniteSql sql = igniteSql();

        sql("CREATE TABLE TEST(ID INT PRIMARY KEY, VAL0 INT)");

        Session ses = sql.createSession();

        // Outer tx with further commit.
        Transaction outerTx = igniteTx().begin();

        for (int i = 0; i < ROW_COUNT; ++i) {
            checkDml(1, ses, "INSERT INTO TEST VALUES (?, ?)", outerTx, i, i);
        }

        await(outerTx.rollbackAsync());

        AsyncResultSet rs = await(ses.executeAsync(null, "SELECT VAL0 FROM TEST ORDER BY VAL0"));

        assertEquals(0, StreamSupport.stream(rs.currentPage().spliterator(), false).count());

        await(rs.closeAsync());
    }

    /** Check correctness of implicit and explicit transactions. */
    @Test
    public void checkTransactionsWithDml() {
        IgniteSql sql = igniteSql();

        if (sql instanceof ClientSql) {
            return;
        }

        sql("CREATE TABLE TEST(ID INT PRIMARY KEY, VAL0 INT)");

        Session ses = sql.createSession();

        TxManager txManagerInternal = txManager();

        int txPrevCnt = txManagerInternal.finished();

        for (int i = 0; i < ROW_COUNT; ++i) {
            checkDml(1, ses, "INSERT INTO TEST VALUES (?, ?)", i, i);
        }

        // Outer tx with further commit.
        Transaction outerTx = igniteTx().begin();

        for (int i = ROW_COUNT; i < 2 * ROW_COUNT; ++i) {
            checkDml(1, ses, "INSERT INTO TEST VALUES (?, ?)", outerTx, i, i);
        }

        outerTx.commit();

        // Outdated tx.
        Transaction outerTx0 = outerTx;
        IgniteException e = assertThrows(IgniteException.class,
                () -> checkDml(1, ses, "INSERT INTO TEST VALUES (?, ?)", outerTx0, ROW_COUNT, Integer.MAX_VALUE));
        assertEquals(TX_FAILED_READ_WRITE_OPERATION_ERR, e.code());

        e = assertThrows(SqlException.class,
                () -> checkDml(1, ses, "INSERT INTO TEST VALUES (?, ?)", ROW_COUNT, Integer.MAX_VALUE));
        assertEquals(CONSTRAINT_VIOLATION_ERR, e.code());

        AsyncResultSet rs = await(ses.executeAsync(null, "SELECT VAL0 FROM TEST ORDER BY VAL0"));

        assertEquals(2 * ROW_COUNT, StreamSupport.stream(rs.currentPage().spliterator(), false).count());

        rs.closeAsync();

        outerTx = igniteTx().begin();

        rs = await(ses.executeAsync(outerTx, "SELECT VAL0 FROM TEST ORDER BY VAL0"));

        assertEquals(2 * ROW_COUNT, StreamSupport.stream(rs.currentPage().spliterator(), false).count());

        rs.closeAsync();

        outerTx.commit();

        checkDml(2 * ROW_COUNT, ses, "UPDATE TEST SET VAL0 = VAL0 + ?", 1);

        checkDml(2 * ROW_COUNT, ses, "DELETE FROM TEST WHERE VAL0 >= 0");

        assertEquals(ROW_COUNT + 1 + 1 + 1 + 1 + 1 + 1, txManagerInternal.finished() - txPrevCnt);

        assertEquals(0, txManagerInternal.pending());
    }

    /** Check correctness of rw and ro transactions for table scan. */
    @Test
    public void checkMixedTransactionsForTable() {
        sql("CREATE TABLE TEST(ID INT PRIMARY KEY, VAL0 INT)");

        Matcher<String> planMatcher = containsTableScan("PUBLIC", "TEST");

        checkMixedTransactions(planMatcher);
    }

    /** Check correctness of rw and ro transactions for index scan. */
    @Test
    public void checkMixedTransactionsForIndex() throws Exception {
        sql("CREATE TABLE TEST(ID INT PRIMARY KEY, VAL0 INT)");
        sql("CREATE INDEX TEST_IDX ON TEST(VAL0)");

        Matcher<String> planMatcher = containsIndexScan("PUBLIC", "TEST", "TEST_IDX");

        checkMixedTransactions(planMatcher);
    }

    private void checkMixedTransactions(Matcher<String> planMatcher) {
        IgniteSql sql = igniteSql();

        if (sql instanceof ClientSql) {
            return;
        }

        Session ses = sql.createSession();

        for (int i = 0; i < ROW_COUNT; ++i) {
            sql("INSERT INTO TEST VALUES (?, ?)", i, i);
        }

        List<Boolean> booleanList = List.of(Boolean.TRUE, Boolean.FALSE);
        for (boolean roTx : booleanList) {
            for (boolean commit : booleanList) {
                for (boolean explicit : booleanList) {
                    checkTx(ses, roTx, commit, explicit, planMatcher);
                }
            }
        }
    }

    private void checkTx(Session ses, boolean readOnly, boolean commit, boolean explicit, Matcher<String> planMatcher) {
        Transaction outerTx = explicit ? (readOnly ? igniteTx().begin(new TransactionOptions().readOnly(true)) : igniteTx().begin()) : null;

        String query = "SELECT VAL0 FROM TEST ORDER BY VAL0";

        assertQuery(outerTx, query).matches(planMatcher).check();

        AsyncResultSet rs = await(ses.executeAsync(outerTx, query));

        assertEquals(ROW_COUNT, StreamSupport.stream(rs.currentPage().spliterator(), false).count());

        rs.closeAsync();

        if (outerTx != null) {
            if (commit) {
                outerTx.commit();
            } else {
                outerTx.rollback();
            }
        }
    }

    @Test
    public void select() {
        sql("CREATE TABLE TEST(ID INT PRIMARY KEY, VAL0 INT)");

        IgniteSql sql = igniteSql();
        Session ses = sql.sessionBuilder().defaultPageSize(ROW_COUNT / 4).build();

        for (int i = 0; i < ROW_COUNT; ++i) {
            ses.execute(null, "INSERT INTO TEST VALUES (?, ?)", i, i);
        }

        TestPageProcessor pageProc = new TestPageProcessor(4);
        await(ses.executeAsync(null, "SELECT ID FROM TEST").thenCompose(pageProc));

        Set<Integer> rs = pageProc.result().stream().map(r -> r.intValue(0)).collect(Collectors.toSet());

        for (int i = 0; i < ROW_COUNT; ++i) {
            assertTrue(rs.remove(i), "Results invalid: " + pageProc.result());
        }

        assertTrue(rs.isEmpty());
    }

    @Test
    public void metadata() {
        sql("CREATE TABLE TEST(COL0 BIGINT PRIMARY KEY, COL1 VARCHAR NOT NULL)");

        IgniteSql sql = igniteSql();
        Session ses = sql.sessionBuilder().build();

        ses.execute(null, "INSERT INTO TEST VALUES (?, ?)", 1L, "some string");

        AsyncResultSet<SqlRow> rs = await(ses.executeAsync(null, "SELECT COL1, COL0 FROM TEST"));

        // Validate columns metadata.
        ResultSetMetadata meta = rs.metadata();

        assertNotNull(meta);
        assertEquals(-1, meta.indexOf("COL"));
        assertEquals(0, meta.indexOf("COL1"));
        assertEquals(1, meta.indexOf("COL0"));

        checkMetadata(new ColumnMetadataImpl(
                        "COL1",
                        ColumnType.STRING,
                        CatalogUtils.DEFAULT_VARLEN_LENGTH,
                        ColumnMetadata.UNDEFINED_SCALE,
                        false,
                        new ColumnOriginImpl("PUBLIC", "TEST", "COL1")),
                meta.columns().get(0));
        checkMetadata(new ColumnMetadataImpl(
                        "COL0",
                        ColumnType.INT64,
                        19,
                        0,
                        false,
                        new ColumnOriginImpl("PUBLIC", "TEST", "COL0")),
                meta.columns().get(1));

        // Validate result columns types.
        assertTrue(rs.hasRowSet());
        assertEquals(1, rs.currentPageSize());

        SqlRow row = rs.currentPage().iterator().next();

        await(rs.closeAsync());

        assertInstanceOf(meta.columns().get(0).valueClass(), row.value(0));
        assertInstanceOf(meta.columns().get(1).valueClass(), row.value(1));
    }

    @Test
    public void sqlRow() {
        IgniteSql sql = igniteSql();
        Session ses = sql.sessionBuilder().build();

        AsyncResultSet<SqlRow> ars = await(ses.executeAsync(null, "SELECT 1 as COL_A, 2 as COL_B"));

        SqlRow r = CollectionUtils.first(ars.currentPage());

        assertEquals(2, r.columnCount());
        assertEquals(0, r.columnIndex("COL_A"));
        assertEquals(1, r.columnIndex("COL_B"));
        assertEquals(-1, r.columnIndex("notExistColumn"));

        assertEquals(1, r.intValue("COL_A"));
        // Unmute after https://issues.apache.org/jira/browse/IGNITE-19894
        //assertEquals(1, r.intValue("COL_a"));
        assertEquals(2, r.intValue("COL_B"));

        assertThrowsWithCause(
                () -> r.intValue("notExistColumn"),
                IllegalArgumentException.class,
                "Column doesn't exist [name=notExistColumn]"
        );

        assertEquals(1, r.intValue(0));
        assertEquals(2, r.intValue(1));
        assertThrowsWithCause(() -> r.intValue(-2), IndexOutOfBoundsException.class);
        assertThrowsWithCause(() -> r.intValue(10), IndexOutOfBoundsException.class);

        await(ars.closeAsync());
    }

    @Test
    public void pageSequence() {
        sql("CREATE TABLE TEST(ID INT PRIMARY KEY, VAL0 INT)");

        IgniteSql sql = igniteSql();
        Session ses = sql.sessionBuilder().defaultPageSize(1).build();

        for (int i = 0; i < ROW_COUNT; ++i) {
            ses.execute(null, "INSERT INTO TEST VALUES (?, ?)", i, i);
        }

        AsyncResultSet<SqlRow> ars0 = await(ses.executeAsync(null, "SELECT ID FROM TEST ORDER BY ID"));
        var p0 = ars0.currentPage();
        AsyncResultSet<SqlRow> ars1 = await(ars0.fetchNextPage());
        var p1 = ars1.currentPage();
        AsyncResultSet<SqlRow> ars2 = await(ars1.fetchNextPage().toCompletableFuture());
        var p2 = ars2.currentPage();
        AsyncResultSet<SqlRow> ars3 = await(ars1.fetchNextPage());
        var p3 = ars3.currentPage();
        AsyncResultSet<SqlRow> ars4 = await(ars0.fetchNextPage());
        var p4 = ars4.currentPage();

        assertSame(ars0, ars1);
        assertSame(ars0, ars2);
        assertSame(ars0, ars3);
        assertSame(ars0, ars4);

        List<SqlRow> res = Stream.of(p0, p1, p2, p3, p4)
                .flatMap(p -> StreamSupport.stream(p.spliterator(), false))
                .collect(Collectors.toList());

        TestPageProcessor pageProc = new TestPageProcessor(ROW_COUNT - res.size());
        await(ars4.fetchNextPage().thenCompose(pageProc));

        res.addAll(pageProc.result());

        for (int i = 0; i < ROW_COUNT; ++i) {
            assertEquals(i, res.get(i).intValue(0));
        }
    }

    @Test
    public void errors() {
        sql("CREATE TABLE TEST(ID INT PRIMARY KEY, VAL0 INT NOT NULL)");

        IgniteSql sql = igniteSql();
        Session ses = sql.sessionBuilder().defaultPageSize(ROW_COUNT / 2).build();

        for (int i = 0; i < ROW_COUNT; ++i) {
            ses.execute(null, "INSERT INTO TEST VALUES (?, ?)", i, i);
        }

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
            AsyncResultSet ars = await(ses.executeAsync(null, "CREATE TABLE TEST3 (ID INT PRIMARY KEY)"));
            assertThrowsPublicException(() -> await(ars.fetchNextPage()),
                    NoRowSetExpectedException.class, QUERY_NO_RESULT_SET_ERR, "Query has no result set");
        }

        // Cursor closed error.
        {
            AsyncResultSet ars = await(ses.executeAsync(null, "SELECT * FROM TEST"));
            await(ars.closeAsync());
            assertThrowsPublicException(() -> await(ars.fetchNextPage()),
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
                assertThrowsPublicException(() -> await(ses.executeAsync(tx, "CREATE TABLE TEST2(ID INT PRIMARY KEY, VAL0 INT)")),
                        SqlException.class,
                        STMT_VALIDATION_ERR,
                        "DDL doesn't support transactions."
                );
            } finally {
                tx.rollback();
            }
        }
        {
            Transaction tx = igniteTx().begin();
            AsyncResultSet<SqlRow> res = await(ses.executeAsync(tx, "INSERT INTO TEST VALUES (?, ?)", -1, -1));
            assertEquals(1, res.affectedRows());

            assertThrowsPublicException(() -> await(ses.executeAsync(tx, "CREATE TABLE TEST2(ID INT PRIMARY KEY, VAL0 INT)")),
                    SqlException.class,
                    STMT_VALIDATION_ERR,
                    "DDL doesn't support transactions."
            );
            tx.commit();

            assertTrue(await(ses.executeAsync(null, "SELECT ID FROM TEST WHERE ID = -1")).currentPage().iterator().hasNext());
        }
    }

    @Test
    public void closeSession() {
        sql("CREATE TABLE TEST(ID INT PRIMARY KEY, VAL0 INT)");

        IgniteSql sql = igniteSql();
        Session ses = sql.sessionBuilder().defaultPageSize(2).build();

        for (int i = 0; i < ROW_COUNT; ++i) {
            ses.execute(null, "INSERT INTO TEST VALUES (?, ?)", i, i);
        }

        AsyncResultSet ars0 = await(ses.executeAsync(null, "SELECT ID FROM TEST"));

        await(ses.closeAsync());

        // Fetched page is available after cancel.
        ars0.currentPage();

        SqlException sqlEx = assertThrowsPublicException(() -> await(ars0.fetchNextPage()),
                SqlException.class, EXECUTION_CANCELLED_ERR, null);
        assertTrue(IgniteTestUtils.hasCause(sqlEx, ExecutionCancelledException.class, null));

        assertThrowsPublicException(() -> await(ses.executeAsync(null, "SELECT ID FROM TEST")),
                SqlException.class, SESSION_CLOSED_ERR, "Session is closed");
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

        long[] batchRes = await(ses.executeBatchAsync(null, "INSERT INTO TEST VALUES (?, ?)", args));

        Arrays.stream(batchRes).forEach(r -> assertEquals(1L, r));

        // Check that data are inserted OK
        List<List<Object>> res = sql("SELECT ID FROM TEST ORDER BY ID");
        IntStream.range(0, ROW_COUNT).forEach(i -> assertEquals(i, res.get(i).get(0)));

        // Check invalid query type
        assertThrowsPublicException(() -> await(ses.executeBatchAsync(null, "SELECT * FROM TEST", args)),
                SqlBatchException.class, STMT_VALIDATION_ERR, "Invalid SQL statement type in the batch");

        assertThrowsPublicException(() -> await(ses.executeBatchAsync(null, "CREATE TABLE TEST1(ID INT PRIMARY KEY, VAL0 INT)", args)),
                SqlBatchException.class, STMT_VALIDATION_ERR, "Invalid SQL statement type in the batch");
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

        SqlBatchException ex = assertThrowsPublicException(
                () -> await(ses.executeBatchAsync(null, "INSERT INTO TEST VALUES (?, ?)", args)),
                SqlBatchException.class,
                CONSTRAINT_VIOLATION_ERR,
                null
        );

        assertEquals(err, ex.updateCounters().length);
        IntStream.range(0, ex.updateCounters().length).forEach(i -> assertEquals(1, ex.updateCounters()[i]));
    }

    @Test
    public void resultSetCloseShouldFinishImplicitTransaction() throws InterruptedException {
        sql("CREATE TABLE TEST(ID INT PRIMARY KEY, VAL0 INT)");

        IgniteSql sql = igniteSql();
        Session ses = sql.sessionBuilder().defaultPageSize(2).build();

        for (int i = 0; i < ROW_COUNT; ++i) {
            ses.execute(null, "INSERT INTO TEST VALUES (?, ?)", i, i);
        }

        CompletableFuture<AsyncResultSet<SqlRow>> f = ses.executeAsync(null, "SELECT * FROM TEST");

        AsyncResultSet<SqlRow> ars = f.join();
        // There should be a pending transaction since not all data was read.
        boolean txStarted = waitForCondition(() -> txManager().pending() == 1, 5000);
        assertTrue(txStarted, "No pending transactions");

        ars.closeAsync().join();
        assertEquals(0, txManager().pending(), "Expected no pending transactions");
    }

    @Test
    public void resultSetFullReadShouldFinishImplicitTransaction() {
        sql("CREATE TABLE TEST(ID INT PRIMARY KEY, VAL0 INT)");

        IgniteSql sql = igniteSql();

        // Fetch all data in one read.
        Session ses = sql.sessionBuilder().defaultPageSize(100).build();

        for (int i = 0; i < ROW_COUNT; ++i) {
            ses.execute(null, "INSERT INTO TEST VALUES (?, ?)", i, i);
        }

        CompletableFuture<AsyncResultSet<SqlRow>> f = ses.executeAsync(null, "SELECT * FROM TEST");

        AsyncResultSet<SqlRow> ars = f.join();
        assertFalse(ars.hasMorePages());

        assertEquals(0, txManager().pending(), "Expected no pending transactions");
    }

    private static void checkDdl(boolean expectedApplied, Session ses, String sql, Transaction tx) {
        CompletableFuture<AsyncResultSet<SqlRow>> fut = ses.executeAsync(
                tx,
                sql
        );

        AsyncResultSet<SqlRow> asyncRes = await(fut);

        assertEquals(expectedApplied, asyncRes.wasApplied());
        assertFalse(asyncRes.hasMorePages());
        assertFalse(asyncRes.hasRowSet());
        assertEquals(-1, asyncRes.affectedRows());

        assertNull(asyncRes.metadata());

        await(asyncRes.closeAsync());
    }

    private static void checkDdl(boolean expectedApplied, Session ses, String sql) {
        checkDdl(expectedApplied, ses, sql, null);
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
        return assertThrowsPublicException(() -> await(ses.executeAsync(null, sql, args)), expCls, code, msg);
    }

    protected static void checkDml(int expectedAffectedRows, Session ses, String sql, Transaction tx, Object... args) {
        AsyncResultSet asyncRes = await(ses.executeAsync(tx, sql, args));

        assertFalse(asyncRes.wasApplied());
        assertFalse(asyncRes.hasMorePages());
        assertFalse(asyncRes.hasRowSet());
        assertEquals(expectedAffectedRows, asyncRes.affectedRows());

        assertNull(asyncRes.metadata());

        await(asyncRes.closeAsync());
    }

    private static void checkDml(int expectedAffectedRows, Session ses, String sql, Object... args) {
        checkDml(expectedAffectedRows, ses, sql, null, args);
    }

    static <T extends IgniteException> T assertThrowsPublicException(
            RunnableX executable,
            Class<T> expCls,
            @Nullable Integer code,
            @Nullable String msgPart
    ) {
        T ex = assertThrows(expCls, executable::run);

        if (code != null) {
            assertEquals(new IgniteException(code).codeAsString(), ex.codeAsString());
        }

        if (msgPart != null) {
            assertThat(ex.getMessage(), containsString(msgPart));
        }

        return ex;
    }

    static class TestPageProcessor implements
            Function<AsyncResultSet<SqlRow>, CompletionStage<AsyncResultSet<SqlRow>>> {
        private int expectedPages;

        private final List<SqlRow> res = new ArrayList<>();

        TestPageProcessor(int expectedPages) {
            this.expectedPages = expectedPages;
        }

        @Override
        public CompletionStage<AsyncResultSet<SqlRow>> apply(AsyncResultSet<SqlRow> rs) {
            expectedPages--;

            assertTrue(rs.hasRowSet());
            assertFalse(rs.wasApplied());
            assertEquals(-1L, rs.affectedRows());
            assertEquals(expectedPages > 0, rs.hasMorePages(),
                    "hasMorePages(): [expected=" + (expectedPages > 0) + ", actual=" + rs.hasMorePages() + ']');

            rs.currentPage().forEach(res::add);

            if (rs.hasMorePages()) {
                return rs.fetchNextPage().thenCompose(this);
            }

            return rs.closeAsync().thenApply(v -> rs);
        }

        public List<SqlRow> result() {
            //noinspection AssignmentOrReturnOfFieldWithMutableType
            return res;
        }
    }
}
