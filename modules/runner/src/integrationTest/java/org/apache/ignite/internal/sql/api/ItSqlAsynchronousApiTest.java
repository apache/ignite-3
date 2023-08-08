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
import static org.apache.ignite.lang.ErrorGroups.Sql.STMT_VALIDATION_ERR;
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
import org.apache.ignite.internal.client.sql.ClientSql;
import org.apache.ignite.internal.sql.api.ColumnMetadataImpl.ColumnOriginImpl;
import org.apache.ignite.internal.sql.engine.ClusterPerClassIntegrationTest;
import org.apache.ignite.internal.sql.engine.exec.ExecutionCancelledException;
import org.apache.ignite.internal.testframework.IgniteTestUtils;
import org.apache.ignite.internal.tx.TxManager;
import org.apache.ignite.internal.util.CollectionUtils;
import org.apache.ignite.lang.ColumnAlreadyExistsException;
import org.apache.ignite.lang.ColumnNotFoundException;
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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

/**
 * Tests for asynchronous SQL API.
 */
@SuppressWarnings("ThrowableNotThrown")
public class ItSqlAsynchronousApiTest extends ClusterPerClassIntegrationTest {
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
    public void ddl() throws Exception {
        IgniteSql sql = CLUSTER_NODES.get(0).sql();
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
                IgniteException.class,
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
                "Can't create index on duplicate columns: VAL0, VAL0",
                ses,
                "CREATE INDEX TEST_IDX4 ON TEST(VAL0, VAL0)"
        );

        checkError(
                SqlException.class,
                "Can`t delete column(s). Column VAL1 is used by indexes [TEST_IDX3].",
                ses,
                "ALTER TABLE TEST DROP COLUMN val1"
        );

        SqlException ex = IgniteTestUtils.cause(assertThrows(Throwable.class,
                () -> await(ses.executeAsync(null, "ALTER TABLE TEST DROP COLUMN (val0, val1)"))), SqlException.class);
        assertNotNull(ex);
        assertEquals(STMT_VALIDATION_ERR, ex.code());

        String msg = ex.getMessage();
        String explainMsg = "Unexpected error message: " + msg;

        assertTrue(msg.contains("Column VAL0 is used by indexes ["), explainMsg);
        assertTrue(msg.contains("TEST_IDX1") && msg.contains("TEST_IDX2") && msg.contains("TEST_IDX3"), explainMsg);
        assertTrue(msg.contains("Column VAL1 is used by indexes [TEST_IDX3]"), explainMsg);

        checkError(
                SqlException.class,
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

        IgniteSql sql = CLUSTER_NODES.get(0).sql();
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

        int txPrevCnt = txManager.finished();

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
        assertEquals(0, txManager.finished() - txPrevCnt);
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

        assertThrows(SqlException.class,
                () -> checkDml(1, ses, "INSERT INTO TEST VALUES (?, ?)", outerTx0, ROW_COUNT, Integer.MAX_VALUE));

        assertThrows(SqlException.class,
                () -> checkDml(1, ses, "INSERT INTO TEST VALUES (?, ?)", ROW_COUNT, Integer.MAX_VALUE));

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
        for (int i = 0; i < ROW_COUNT; ++i) {
            sql("INSERT INTO TEST VALUES (?, ?)", i, i);
        }

        IgniteSql sql = igniteSql();
        Session ses = sql.sessionBuilder().defaultPageSize(ROW_COUNT / 4).build();

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
        sql("INSERT INTO TEST VALUES (?, ?)", 1L, "some string");

        IgniteSql sql = igniteSql();
        Session ses = sql.sessionBuilder().build();

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
                        2 << 15,
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
        for (int i = 0; i < ROW_COUNT; ++i) {
            sql("INSERT INTO TEST VALUES (?, ?)", i, i);
        }

        IgniteSql sql = igniteSql();
        Session ses = sql.sessionBuilder().defaultPageSize(1).build();

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
        sql("CREATE TABLE TEST(ID INT PRIMARY KEY, VAL0 INT)");
        for (int i = 0; i < ROW_COUNT; ++i) {
            sql("INSERT INTO TEST VALUES (?, ?)", i, i);
        }

        IgniteSql sql = igniteSql();
        Session ses = sql.sessionBuilder().defaultPageSize(ROW_COUNT / 2).build();

        // Parse error.
        assertThrowsWithCause(() -> await(ses.executeAsync(null, "SELECT ID FROM")),
                SqlException.class, "Failed to parse query");

        // Multiple statements error.
        assertThrowsWithCause(() -> await(ses.executeAsync(null, "SELECT 1; SELECT 2")),
                SqlException.class, "Multiple statements are not allowed");

        // Planning error.
        assertThrowsWithCause(() -> await(ses.executeAsync(null, "CREATE TABLE TEST2 (VAL INT)")),
                SqlException.class, "Table without PRIMARY KEY is not supported");

        // Execute error.
        assertThrowsWithCause(() -> await(ses.executeAsync(null, "SELECT 1 / ?", 0)),
                IgniteException.class, "/ by zero");

        // No result set error.
        {
            AsyncResultSet ars = await(ses.executeAsync(null, "CREATE TABLE TEST3 (ID INT PRIMARY KEY)"));
            assertThrowsWithCause(() -> await(ars.fetchNextPage()), NoRowSetExpectedException.class,
                    "Query has no result set");
        }

        // Cursor closed error.
        {
            AsyncResultSet ars = await(ses.executeAsync(null, "SELECT * FROM TEST"));
            await(ars.closeAsync());
            assertThrowsWithCause(() -> await(ars.fetchNextPage()), CursorClosedException.class);
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
                assertThrowsWithCause(() -> await(ses.executeAsync(tx, "CREATE TABLE TEST2(ID INT PRIMARY KEY, VAL0 INT)")),
                        SqlException.class,
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

            assertThrowsWithCause(() -> await(ses.executeAsync(tx, "CREATE TABLE TEST2(ID INT PRIMARY KEY, VAL0 INT)")),
                    SqlException.class,
                    "DDL doesn't support transactions."
            );
            tx.commit();

            assertEquals(1, sql("SELECT ID FROM TEST WHERE ID = -1").size());
        }
    }

    @Test
    public void closeSession() {
        sql("CREATE TABLE TEST(ID INT PRIMARY KEY, VAL0 INT)");
        for (int i = 0; i < ROW_COUNT; ++i) {
            sql("INSERT INTO TEST VALUES (?, ?)", i, i);
        }

        IgniteSql sql = igniteSql();
        Session ses = sql.sessionBuilder().defaultPageSize(2).build();

        AsyncResultSet ars0 = await(ses.executeAsync(null, "SELECT ID FROM TEST"));

        await(ses.closeAsync());

        // Fetched page is available after cancel.
        ars0.currentPage();

        assertThrowsWithCause(
                () -> await(ars0.fetchNextPage()),
                ExecutionCancelledException.class
        );

        assertThrowsWithCause(
                () -> await(ses.executeAsync(null, "SELECT ID FROM TEST")),
                SqlException.class,
                "Session is closed"
        );
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
        assertThrowsWithCause(
                () -> await(ses.executeBatchAsync(null, "SELECT * FROM TEST", args)),
                SqlException.class,
                "Invalid SQL statement type in the batch"
        );

        assertThrowsWithCause(
                () -> await(ses.executeBatchAsync(null, "CREATE TABLE TEST1(ID INT PRIMARY KEY, VAL0 INT)", args)),
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

        SqlBatchException ex = assertThrows(
                SqlBatchException.class,
                () -> await(ses.executeBatchAsync(null, "INSERT INTO TEST VALUES (?, ?)", args))
        );

        assertEquals(CONSTRAINT_VIOLATION_ERR, ex.code());
        assertEquals(err, ex.updateCounters().length);
        IntStream.range(0, ex.updateCounters().length).forEach(i -> assertEquals(1, ex.updateCounters()[i]));
    }

    @Test
    public void resultSetCloseShouldFinishImplicitTransaction() throws InterruptedException {
        sql("CREATE TABLE TEST(ID INT PRIMARY KEY, VAL0 INT)");
        for (int i = 0; i < ROW_COUNT; ++i) {
            sql("INSERT INTO TEST VALUES (?, ?)", i, i);
        }

        IgniteSql sql = igniteSql();

        Session ses = sql.sessionBuilder().defaultPageSize(2).build();
        CompletableFuture<AsyncResultSet<SqlRow>> f = ses.executeAsync(null, "SELECT * FROM TEST WHERE VAL0 >= ?", -1000);

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
        for (int i = 0; i < ROW_COUNT; ++i) {
            sql("INSERT INTO TEST VALUES (?, ?)", i, i);
        }

        IgniteSql sql = igniteSql();

        // Fetch all data in one read.
        Session ses = sql.sessionBuilder().defaultPageSize(100).build();
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

    private static void checkError(Class<? extends Throwable> expectedException, String msg, Session ses, String sql, Object... args) {
        assertThrowsWithCause(() -> await(ses.executeAsync(
                null,
                sql,
                args
        )), expectedException, msg);
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

    protected static void checkDml(int expectedAffectedRows, Session ses, String sql, Object... args) {
        checkDml(expectedAffectedRows, ses, sql, null, args);
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
