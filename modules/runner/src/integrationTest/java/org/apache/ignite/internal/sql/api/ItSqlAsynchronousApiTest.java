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
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.apache.ignite.internal.sql.engine.AbstractBasicIntegrationTest;
import org.apache.ignite.internal.sql.engine.ClosedCursorException;
import org.apache.ignite.internal.testframework.IgniteTestUtils;
import org.apache.ignite.internal.util.CollectionUtils;
import org.apache.ignite.lang.ColumnAlreadyExistsException;
import org.apache.ignite.lang.ColumnNotFoundException;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.lang.IgniteInternalException;
import org.apache.ignite.lang.IndexAlreadyExistsException;
import org.apache.ignite.lang.TableAlreadyExistsException;
import org.apache.ignite.lang.TableNotFoundException;
import org.apache.ignite.sql.IgniteSql;
import org.apache.ignite.sql.Session;
import org.apache.ignite.sql.SqlRow;
import org.apache.ignite.sql.async.AsyncResultSet;
import org.apache.ignite.table.Table;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

/**
 * Tests for asynchronous SQL API.
 */
@Disabled("https://issues.apache.org/jira/browse/IGNITE-15655")
public class ItSqlAsynchronousApiTest extends AbstractBasicIntegrationTest {
    private static final int ROW_COUNT = 16;

    /**
     * Clear tables after each test.
     *
     * @param testInfo Test information oject.
     * @throws Exception If failed.
     */
    @AfterEach
    @Override
    public void tearDown(TestInfo testInfo) throws Exception {
        for (Table t : CLUSTER_NODES.get(0).tables().tables()) {
            sql("DROP TABLE " + t.name());
        }

        super.tearDownBase(testInfo);
    }

    @Test
    public void ddl() throws ExecutionException, InterruptedException {
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

        checkSession(ses);
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

        checkSession(ses);
    }

    @Test
    public void select() throws ExecutionException, InterruptedException {
        sql("CREATE TABLE TEST(ID INT PRIMARY KEY, VAL0 INT)");
        for (int i = 0; i < ROW_COUNT; ++i) {
            sql("INSERT INTO TEST VALUES (?, ?)", i, i);
        }

        IgniteSql sql = CLUSTER_NODES.get(0).sql();
        Session ses = sql.sessionBuilder().defaultPageSize(ROW_COUNT / 4).build();

        TestPageProcessor pageProc = new TestPageProcessor(4);
        ses.executeAsync(null, "SELECT ID FROM TEST").thenCompose(pageProc).get();

        Set<Integer> rs = pageProc.result().stream().map(r -> r.intValue(0)).collect(Collectors.toSet());

        for (int i = 0; i < ROW_COUNT; ++i) {
            assertTrue(rs.remove(i), "Results invalid: " + pageProc.result());
        }

        assertTrue(rs.isEmpty());

        checkSession(ses);
    }

    @Test
    public void sqlRow() throws ExecutionException, InterruptedException {
        IgniteSql sql = CLUSTER_NODES.get(0).sql();
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

        checkSession(ses);
    }

    @Test
    public void pageSequence() throws ExecutionException, InterruptedException {
        sql("CREATE TABLE TEST(ID INT PRIMARY KEY, VAL0 INT)");
        for (int i = 0; i < ROW_COUNT; ++i) {
            sql("INSERT INTO TEST VALUES (?, ?)", i, i);
        }

        IgniteSql sql = CLUSTER_NODES.get(0).sql();
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
        IgniteSql sql = CLUSTER_NODES.get(0).sql();
        Session ses = sql.sessionBuilder().defaultPageSize(ROW_COUNT / 2).build();

        // Parse error.
        {
            CompletableFuture<AsyncResultSet> f = ses.executeAsync(null, "SELECT ID FROM");
            assertThrowsWithCause(() -> f.get(), IgniteInternalException.class, "Failed to parse query");
        }

        // Multiple statements error.
        {
            CompletableFuture<AsyncResultSet> f = ses.executeAsync(null, "SELECT 1; SELECT 2");
            assertThrowsWithCause(() -> f.get(), IgniteSqlException.class, "Multiple statements aren't allowed");
        }

        // Planning error.
        {
            CompletableFuture<AsyncResultSet> f = ses.executeAsync(null, "CREATE TABLE TEST (VAL INT)");
            assertThrowsWithCause(() -> f.get(), IgniteException.class, "Table without PRIMARY KEY is not supported");
        }

        // Execute error.
        {
            CompletableFuture<AsyncResultSet> f = ses.executeAsync(null, "SELECT 1 / ?", 0);
            assertThrowsWithCause(() -> f.get(), ArithmeticException.class, "/ by zero");
        }

        checkSession(ses);
    }

    @Test
    public void closeSession() throws ExecutionException, InterruptedException {
        sql("CREATE TABLE TEST(ID INT PRIMARY KEY, VAL0 INT)");
        for (int i = 0; i < ROW_COUNT; ++i) {
            sql("INSERT INTO TEST VALUES (?, ?)", i, i);
        }

        IgniteSql sql = CLUSTER_NODES.get(0).sql();
        Session ses = sql.sessionBuilder().defaultPageSize(2).build();

        AsyncResultSet ars0 = ses.executeAsync(null, "SELECT ID FROM TEST").get();

        ses.closeAsync().get();

        // Fetched page  is available after cancel.
        ars0.currentPage();

        assertThrowsWithCause(
                () -> ars0.fetchNextPage().toCompletableFuture().get(),
                ClosedCursorException.class
        );

        assertThrowsWithCause(
                () -> ses.executeAsync(null, "SELECT ID FROM TEST").get(),
                IgniteSqlException.class,
                "Session is closed"
        );

        checkSession(ses);
    }

    private void checkDdl(boolean expectedApplied, Session ses, String sql) throws ExecutionException, InterruptedException {
        CompletableFuture<AsyncResultSet> fut = ses.executeAsync(
                null,
                sql
        );

        AsyncResultSet asyncRes = fut.get();

        assertEquals(expectedApplied, asyncRes.wasApplied());
        assertFalse(asyncRes.hasMorePages());
        assertFalse(asyncRes.hasRowSet());
        assertEquals(-1, asyncRes.affectedRows());

        asyncRes.closeAsync().toCompletableFuture().get();
    }

    private void checkError(Class<? extends Throwable> expectedException, String msg, Session ses, String sql, Object... args) {
        CompletableFuture<AsyncResultSet> fut = ses.executeAsync(
                null,
                sql,
                args
        );

        assertThrowsWithCause(fut::get, expectedException, msg);
    }

    private void checkDml(int expectedAffectedRows, Session ses, String sql, Object... args)
            throws ExecutionException, InterruptedException {
        CompletableFuture<AsyncResultSet> fut = ses.executeAsync(
                null,
                sql,
                args
        );

        AsyncResultSet asyncRes = fut.get();

        assertFalse(asyncRes.wasApplied());
        assertFalse(asyncRes.hasMorePages());
        assertFalse(asyncRes.hasRowSet());
        assertEquals(expectedAffectedRows, asyncRes.affectedRows());

        asyncRes.closeAsync().toCompletableFuture().get();
    }

    private void checkSession(Session s) {
        assertTrue(((Set<?>) IgniteTestUtils.getFieldValue(s, "futsToClose")).isEmpty());
        assertTrue(((Set<?>) IgniteTestUtils.getFieldValue(s, "cursToClose")).isEmpty());
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
            return res;
        }
    }
}
