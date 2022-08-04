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
import static org.apache.ignite.internal.testframework.IgniteTestUtils.await;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.cause;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.hasCause;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.client.sql.ClientSql;
import org.apache.ignite.internal.sql.api.ColumnMetadataImpl.ColumnOriginImpl;
import org.apache.ignite.internal.sql.engine.AbstractBasicIntegrationTest;
import org.apache.ignite.internal.testframework.IgniteTestUtils;
import org.apache.ignite.internal.tx.TxManager;
import org.apache.ignite.internal.tx.TxState;
import org.apache.ignite.internal.tx.impl.TxManagerImpl;
import org.apache.ignite.internal.util.CollectionUtils;
import org.apache.ignite.lang.ColumnAlreadyExistsException;
import org.apache.ignite.lang.ColumnNotFoundException;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.lang.IgniteInternalException;
import org.apache.ignite.lang.IndexAlreadyExistsException;
import org.apache.ignite.lang.TableAlreadyExistsException;
import org.apache.ignite.lang.TableNotFoundException;
import org.apache.ignite.sql.BatchedArguments;
import org.apache.ignite.sql.ColumnMetadata;
import org.apache.ignite.sql.CursorClosedException;
import org.apache.ignite.sql.IgniteSql;
import org.apache.ignite.sql.NoRowSetExpectedException;
import org.apache.ignite.sql.ResultSetMetadata;
import org.apache.ignite.sql.Session;
import org.apache.ignite.sql.SqlBatchException;
import org.apache.ignite.sql.SqlColumnType;
import org.apache.ignite.sql.SqlException;
import org.apache.ignite.sql.SqlRow;
import org.apache.ignite.sql.async.AsyncResultSet;
import org.apache.ignite.table.Table;
import org.apache.ignite.tx.IgniteTransactions;
import org.apache.ignite.tx.Transaction;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

/**
 * Tests for asynchronous SQL API.
 */
@SuppressWarnings("ThrowableNotThrown")
public class ItSqlAsynchronousApiTest extends AbstractBasicIntegrationTest {
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

    protected IgniteTransactions igniteTx() {
        return CLUSTER_NODES.get(0).transactions();
    }

    @Test
    public void ddl() throws Exception {
        IgniteSql sql = CLUSTER_NODES.get(0).sql();
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
    public void dml() throws ExecutionException, InterruptedException {
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
    public void implicitTransactionsStates() throws Exception {
        IgniteSql sql = igniteSql();

        if (sql instanceof ClientSql) {
            return;
        }

        sql("CREATE TABLE TEST(ID INT PRIMARY KEY, VAL0 INT)");

        Session ses = sql.createSession();

        TxManager txManagerInternal = (TxManager) IgniteTestUtils.getFieldValue(CLUSTER_NODES.get(0), IgniteImpl.class, "txManager");

        int txPrevCnt = txManagerInternal.finished();

        for (int i = 0; i < ROW_COUNT; ++i) {
            CompletableFuture<AsyncResultSet> fut = ses.executeAsync(null, "CREATE TABLE TEST(ID INT PRIMARY KEY, VAL0 INT)", i, i);

            AsyncResultSet asyncRes = null;

            try {
                asyncRes = fut.get();
            } catch (Throwable ignore) {
                // No op.
            }

            if (asyncRes != null) {
                asyncRes.closeAsync().toCompletableFuture().get();
            }
        }

        // No new transactions through ddl.
        assertEquals(0, txManagerInternal.finished() - txPrevCnt);
    }

    /** Check correctness of explicit transaction rollback. */
    @Test
    public void checkExplicitTxRollback() throws Exception {
        IgniteSql sql = igniteSql();

        sql("CREATE TABLE TEST(ID INT PRIMARY KEY, VAL0 INT)");

        Session ses = sql.createSession();

        // Outer tx with further commit.
        Transaction outerTx = igniteTx().begin();

        for (int i = 0; i < ROW_COUNT; ++i) {
            checkDml(1, ses, "INSERT INTO TEST VALUES (?, ?)", outerTx, i, i);
        }

        outerTx.rollbackAsync().get();

        AsyncResultSet rs = ses.executeAsync(null, "SELECT VAL0 FROM TEST ORDER BY VAL0").get();

        assertEquals(0, StreamSupport.stream(rs.currentPage().spliterator(), false).count());

        rs.closeAsync();
    }

    /** Check correctness of implicit and explicit transactions. */
    @Test
    public void checkTransactionsWithDml() throws Exception {
        IgniteSql sql = igniteSql();

        if (sql instanceof ClientSql) {
            return;
        }

        sql("CREATE TABLE TEST(ID INT PRIMARY KEY, VAL0 INT)");

        Session ses = sql.createSession();

        TxManager txManagerInternal = (TxManager) IgniteTestUtils.getFieldValue(CLUSTER_NODES.get(0), IgniteImpl.class, "txManager");

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

        assertThrows(ExecutionException.class,
                () -> checkDml(1, ses, "INSERT INTO TEST VALUES (?, ?)", outerTx0, ROW_COUNT, Integer.MAX_VALUE));

        assertThrows(ExecutionException.class,
                () -> checkDml(1, ses, "INSERT INTO TEST VALUES (?, ?)", ROW_COUNT, Integer.MAX_VALUE));

        AsyncResultSet rs = ses.executeAsync(null, "SELECT VAL0 FROM TEST ORDER BY VAL0").get();

        assertEquals(2 * ROW_COUNT, StreamSupport.stream(rs.currentPage().spliterator(), false).count());

        rs.closeAsync();

        checkDml(2 * ROW_COUNT, ses, "UPDATE TEST SET VAL0 = VAL0 + ?", 1);

        checkDml(2 * ROW_COUNT, ses, "DELETE FROM TEST WHERE VAL0 >= 0");

        assertEquals(ROW_COUNT + 1 + 1 + 1 + 1, txManagerInternal.finished() - txPrevCnt);

        var states = (Map<UUID, TxState>) IgniteTestUtils.getFieldValue(txManagerInternal, TxManagerImpl.class, "states");

        states.forEach((k, v) -> assertNotSame(v, TxState.PENDING));
    }

    @Test
    public void select() throws ExecutionException, InterruptedException {
        sql("CREATE TABLE TEST(ID INT PRIMARY KEY, VAL0 INT)");
        for (int i = 0; i < ROW_COUNT; ++i) {
            sql("INSERT INTO TEST VALUES (?, ?)", i, i);
        }

        IgniteSql sql = igniteSql();
        Session ses = sql.sessionBuilder().defaultPageSize(ROW_COUNT / 4).build();

        TestPageProcessor pageProc = new TestPageProcessor(4);
        ses.executeAsync(null, "SELECT ID FROM TEST").thenCompose(pageProc).get();

        Set<Integer> rs = pageProc.result().stream().map(r -> r.intValue(0)).collect(Collectors.toSet());

        for (int i = 0; i < ROW_COUNT; ++i) {
            assertTrue(rs.remove(i), "Results invalid: " + pageProc.result());
        }

        assertTrue(rs.isEmpty());
    }

    @Test
    public void metadata() throws ExecutionException, InterruptedException {
        sql("CREATE TABLE TEST(COL0 BIGINT PRIMARY KEY, COL1 VARCHAR NOT NULL)");
        sql("INSERT INTO TEST VALUES (?, ?)", 1L, "some string");

        IgniteSql sql = igniteSql();
        Session ses = sql.sessionBuilder().build();

        AsyncResultSet rs = ses.executeAsync(null, "SELECT COL1, COL0 FROM TEST").get();

        // Validate columns metadata.
        ResultSetMetadata meta = rs.metadata();

        assertNotNull(meta);
        assertEquals(-1, meta.indexOf("COL"));
        assertEquals(0, meta.indexOf("COL1"));
        assertEquals(1, meta.indexOf("COL0"));

        checkMetadata(new ColumnMetadataImpl(
                        "COL1",
                        SqlColumnType.STRING,
                        2 << 15,
                        ColumnMetadata.UNDEFINED_SCALE,
                        false,
                        new ColumnOriginImpl("PUBLIC", "TEST", "COL1")),
                meta.columns().get(0));
        checkMetadata(new ColumnMetadataImpl(
                        "COL0",
                        SqlColumnType.INT64,
                        19,
                        0,
                        false,
                        new ColumnOriginImpl("PUBLIC", "TEST", "COL0")),
                meta.columns().get(1));

        // Validate result columns types.
        assertTrue(rs.hasRowSet());
        assertEquals(1, rs.currentPageSize());

        SqlRow row = rs.currentPage().iterator().next();

        rs.closeAsync().toCompletableFuture().get();

        assertInstanceOf(meta.columns().get(0).valueClass(), row.value(0));
        assertInstanceOf(meta.columns().get(1).valueClass(), row.value(1));
    }

    @Test
    public void sqlRow() throws ExecutionException, InterruptedException {
        IgniteSql sql = igniteSql();
        Session ses = sql.sessionBuilder().build();

        AsyncResultSet ars = ses.executeAsync(null, "SELECT 1 as COL_A, 2 as COL_B").get();

        SqlRow r = CollectionUtils.first(ars.currentPage());

        assertEquals(2, r.columnCount());
        assertEquals(0, r.columnIndex("COL_A"));
        assertEquals(1, r.columnIndex("COL_B"));
        assertEquals(-1, r.columnIndex("notExistColumn"));

        assertEquals(1, r.intValue("COL_A"));
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
    public void pageSequence() throws ExecutionException, InterruptedException {
        sql("CREATE TABLE TEST(ID INT PRIMARY KEY, VAL0 INT)");
        for (int i = 0; i < ROW_COUNT; ++i) {
            sql("INSERT INTO TEST VALUES (?, ?)", i, i);
        }

        IgniteSql sql = igniteSql();
        Session ses = sql.sessionBuilder().defaultPageSize(1).build();

        AsyncResultSet ars0 = ses.executeAsync(null, "SELECT ID FROM TEST ORDER BY ID").get();
        var p0 = ars0.currentPage();
        AsyncResultSet ars1 = ars0.fetchNextPage().toCompletableFuture().get();
        var p1 = ars1.currentPage();
        AsyncResultSet ars2 = ars1.fetchNextPage().toCompletableFuture().get();
        var p2 = ars2.currentPage();
        AsyncResultSet ars3 = ars1.fetchNextPage().toCompletableFuture().get();
        var p3 = ars3.currentPage();
        AsyncResultSet ars4 = ars0.fetchNextPage().toCompletableFuture().get();
        var p4 = ars4.currentPage();

        assertSame(ars0, ars1);
        assertSame(ars0, ars2);
        assertSame(ars0, ars3);
        assertSame(ars0, ars4);

        List<SqlRow> res = Stream.of(p0, p1, p2, p3, p4)
                .flatMap(p -> StreamSupport.stream(p.spliterator(), false))
                .collect(Collectors.toList());

        TestPageProcessor pageProc = new TestPageProcessor(ROW_COUNT - res.size());
        ars4.fetchNextPage().thenCompose(pageProc).toCompletableFuture().get();

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
        {
            CompletableFuture<AsyncResultSet> f = ses.executeAsync(null, "SELECT ID FROM");
            assertThrowsWithCause(f::get, SqlException.class, "Failed to parse query");
        }

        // Multiple statements error.
        {
            CompletableFuture<AsyncResultSet> f = ses.executeAsync(null, "SELECT 1; SELECT 2");
            assertThrowsWithCause(f::get, SqlException.class, "Multiple statements aren't allowed");
        }

        // Planning error.
        {
            CompletableFuture<AsyncResultSet> f = ses.executeAsync(null, "CREATE TABLE TEST2 (VAL INT)");
            assertThrowsWithCause(f::get, SqlException.class, "Table without PRIMARY KEY is not supported");
        }

        // Execute error.
        {
            CompletableFuture<AsyncResultSet> f = ses.executeAsync(null, "SELECT 1 / ?", 0);
            assertThrowsWithCause(f::get, IgniteException.class, "/ by zero");
        }

        // No result set error.
        {
            AsyncResultSet ars = ses.executeAsync(null, "CREATE TABLE TEST3 (ID INT PRIMARY KEY)").join();
            assertThrowsWithCause(() -> ars.fetchNextPage().toCompletableFuture().get(), NoRowSetExpectedException.class,
                    "Query has no result set");
        }

        // Cursor closed error.
        {
            AsyncResultSet ars = ses.executeAsync(null, "SELECT * FROM TEST").join();
            ars.closeAsync().toCompletableFuture().join();
            assertThrowsWithCause(() -> ars.fetchNextPage().toCompletableFuture().get(), CursorClosedException.class);
        }
    }

    @Test
    public void closeSession() throws ExecutionException, InterruptedException {
        sql("CREATE TABLE TEST(ID INT PRIMARY KEY, VAL0 INT)");
        for (int i = 0; i < ROW_COUNT; ++i) {
            sql("INSERT INTO TEST VALUES (?, ?)", i, i);
        }

        IgniteSql sql = igniteSql();
        Session ses = sql.sessionBuilder().defaultPageSize(2).build();

        AsyncResultSet ars0 = ses.executeAsync(null, "SELECT ID FROM TEST").get();

        ses.closeAsync().get();

        // Fetched page is available after cancel.
        ars0.currentPage();

        assertThrowsWithCause(
                () -> ars0.fetchNextPage().toCompletableFuture().get(),
                SqlException.class
        );

        assertThrowsWithCause(
                () -> ses.executeAsync(null, "SELECT ID FROM TEST").get(),
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

        long[] batchRes = ses.executeBatchAsync(null, "INSERT INTO TEST VALUES (?, ?)", args).join();

        Arrays.stream(batchRes).forEach(r -> assertEquals(1L, r));

        // Check that data are inserted OK
        List<List<Object>> res = sql("SELECT ID FROM TEST ORDER BY ID");
        IntStream.range(0, ROW_COUNT).forEach(i -> assertEquals(i, res.get(i).get(0)));

        // Check invalid query type
        assertThrowsWithCause(
                () -> ses.executeBatchAsync(null, "SELECT * FROM TEST", args).get(),
                SqlException.class,
                "Invalid SQL statement type in the batch"
        );

        assertThrowsWithCause(
                () -> ses.executeBatchAsync(null, "CREATE TABLE TEST1(ID INT PRIMARY KEY, VAL0 INT)", args).get(),
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
                () -> ses.executeBatchAsync(null, "INSERT INTO TEST VALUES (?, ?)", args).join(),
                SqlBatchException.class
        );
        assertTrue(hasCause(ex, IgniteInternalException.class, "Failed to INSERT some keys because they are already in cache"));
        SqlBatchException batchEx = cause(ex, SqlBatchException.class, null);

        assertEquals(err, batchEx.updateCounters().length);
        IntStream.range(0, batchEx.updateCounters().length).forEach(i -> assertEquals(1, batchEx.updateCounters()[i]));
    }

    private static void checkDdl(boolean expectedApplied, Session ses, String sql, Transaction tx) throws Exception {
        CompletableFuture<AsyncResultSet> fut = ses.executeAsync(
                tx,
                sql
        );

        AsyncResultSet asyncRes = fut.get();

        assertEquals(expectedApplied, asyncRes.wasApplied());
        assertFalse(asyncRes.hasMorePages());
        assertFalse(asyncRes.hasRowSet());
        assertEquals(-1, asyncRes.affectedRows());

        assertNull(asyncRes.metadata());

        asyncRes.closeAsync().toCompletableFuture().get();
    }

    private static void checkDdl(boolean expectedApplied, Session ses, String sql) throws Exception {
        checkDdl(expectedApplied, ses, sql, null);
    }

    private static void checkError(Class<? extends Throwable> expectedException, String msg, Session ses, String sql, Object... args) {
        CompletableFuture<AsyncResultSet> fut = ses.executeAsync(
                null,
                sql,
                args
        );

        assertThrowsWithCause(fut::get, expectedException, msg);
    }

    protected static void checkDml(int expectedAffectedRows, Session ses, String sql, Transaction tx, Object... args)
            throws ExecutionException, InterruptedException {
        CompletableFuture<AsyncResultSet> fut = ses.executeAsync(tx, sql, args);

        AsyncResultSet asyncRes = fut.get();

        assertFalse(asyncRes.wasApplied());
        assertFalse(asyncRes.hasMorePages());
        assertFalse(asyncRes.hasRowSet());
        assertEquals(expectedAffectedRows, asyncRes.affectedRows());

        assertNull(asyncRes.metadata());

        asyncRes.closeAsync().toCompletableFuture().get();
    }

    protected static void checkDml(int expectedAffectedRows, Session ses, String sql, Object... args)
            throws ExecutionException, InterruptedException {
        checkDml(expectedAffectedRows, ses, sql, null, args);
    }

    static class TestPageProcessor implements
            Function<AsyncResultSet, CompletionStage<AsyncResultSet>> {
        private int expectedPages;

        private final List<SqlRow> res = new ArrayList<>();

        TestPageProcessor(int expectedPages) {
            this.expectedPages = expectedPages;
        }

        @Override
        public CompletionStage<AsyncResultSet> apply(AsyncResultSet rs) {
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
