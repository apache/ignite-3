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

import static org.apache.ignite.internal.testframework.IgniteTestUtils.await;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.apache.ignite.internal.sql.SyncResultSetAdapter;
import org.apache.ignite.internal.wrapper.Wrappers;
import org.apache.ignite.lang.CancelHandle;
import org.apache.ignite.lang.CancellationToken;
import org.apache.ignite.lang.ErrorGroups.Sql;
import org.apache.ignite.sql.BatchedArguments;
import org.apache.ignite.sql.IgniteSql;
import org.apache.ignite.sql.ResultSet;
import org.apache.ignite.sql.SqlException;
import org.apache.ignite.sql.SqlRow;
import org.apache.ignite.sql.Statement;
import org.apache.ignite.sql.async.AsyncResultSet;
import org.apache.ignite.tx.Transaction;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

/**
 * Tests for asynchronous SQL API.
 */
public class ItSqlAsynchronousApiTest extends ItSqlApiBaseTest {
    @Test
    public void pageSequence() {
        sql("CREATE TABLE TEST(ID INT PRIMARY KEY, VAL0 INT)");

        IgniteSql sql = igniteSql();

        for (int i = 0; i < ROW_COUNT; ++i) {
            sql.execute("INSERT INTO TEST VALUES (?, ?)", i, i);
        }

        Statement statement = sql.statementBuilder()
                .query("SELECT ID FROM TEST ORDER BY ID")
                .pageSize(1)
                .build();

        AsyncResultSet<SqlRow> ars0 = await(sql.executeAsync((Transaction) null, statement));
        var p0 = ars0.currentPage();
        AsyncResultSet<SqlRow> ars1 = await(ars0.fetchNextPage());
        var p1 = ars1.currentPage();
        AsyncResultSet<SqlRow> ars2 = await(ars1.fetchNextPage());
        var p2 = ars2.currentPage();
        AsyncResultSet<SqlRow> ars3 = await(ars1.fetchNextPage());
        var p3 = ars3.currentPage();
        AsyncResultSet<SqlRow> ars4 = await(ars0.fetchNextPage());
        var p4 = ars4.currentPage();

        AsyncResultSet<?> ars0unwrapped = Wrappers.unwrap(ars0, AsyncResultSet.class);

        assertSame(ars0unwrapped, Wrappers.unwrap(ars1, AsyncResultSet.class));
        assertSame(ars0unwrapped, Wrappers.unwrap(ars2, AsyncResultSet.class));
        assertSame(ars0unwrapped, Wrappers.unwrap(ars3, AsyncResultSet.class));
        assertSame(ars0unwrapped, Wrappers.unwrap(ars4, AsyncResultSet.class));

        List<SqlRow> res = Stream.of(p0, p1, p2, p3, p4)
                .flatMap(p -> StreamSupport.stream(p.spliterator(), false))
                .collect(Collectors.toList());

        AsyncResultProcessor pageProc = new AsyncResultProcessor(ROW_COUNT - res.size());
        await(ars4.fetchNextPage().thenCompose(pageProc));

        res.addAll(pageProc.result());

        for (int i = 0; i < ROW_COUNT; ++i) {
            assertEquals(i, res.get(i).intValue(0));
        }
    }

    @Test
    @Override
    public void cancelQueryString() throws InterruptedException {
        IgniteSql sql = igniteSql();
        String query = "SELECT * FROM system_range(0, 10000000000)";

        // no transaction
        executeAndCancel((token) -> {
            return sql.executeAsync((Transaction) null, token, query);
        });

        // with transaction
        executeAndCancel((token) -> {
            Transaction transaction = igniteTx().begin();

            return sql.executeAsync(transaction, token, query);
        });
    }

    @Test
    @Override
    public void cancelStatement() throws InterruptedException {
        IgniteSql sql = igniteSql();

        String query = "SELECT * FROM system_range(0, 10000000000)";

        // no transaction
        executeAndCancel((token) -> {
            Statement statement = sql.statementBuilder()
                    .query(query)
                    .build();

            return sql.executeAsync((Transaction) null, token, statement);
        });

        // with transaction
        executeAndCancel((token) -> {
            Statement statement = sql.statementBuilder()
                    .query(query)
                    .build();

            Transaction transaction = igniteTx().begin();

            return sql.executeAsync(transaction, token, statement);
        });
    }

    @Override
    @Test
    public void cancelBatch() {
        IgniteSql sql = igniteSql();

        sql("CREATE TABLE TEST(ID INT PRIMARY KEY, VAL INT)");

        BatchedArguments args = BatchedArguments.of(0, 0);
        for (int i = 1; i < ROW_COUNT; ++i) {
            args.add(i, i);
        }

        String query = "UPDATE TEST SET val = ? + (SELECT COUNT(*) FROM system_range(?, 10000000000))";

        // no transaction
        executeBatchAndCancel((token) -> sql.executeBatchAsync(null, token, query, args));

        // with transaction
        executeBatchAndCancel((token) -> {
            Transaction transaction = igniteTx().begin();

            return sql.executeBatchAsync(transaction, token, query, args);
        });
    }

    private void executeAndCancel(
            Function<CancellationToken, CompletableFuture<AsyncResultSet<SqlRow>>> execute
    ) throws InterruptedException {

        CancelHandle cancelHandle = CancelHandle.create();
        CountDownLatch firstResultSetLatch = new CountDownLatch(1);

        CompletableFuture<AsyncResultSet<SqlRow>> resultSetFut = execute.apply(cancelHandle.token());

        // Wait for the first result set to become available and then cancel the query
        resultSetFut.whenComplete((r, t) -> firstResultSetLatch.countDown());
        firstResultSetLatch.await();

        cancelHandle.cancelAsync();

        CompletionException err = assertThrows(CompletionException.class, new DrainResultSet(resultSetFut));
        SqlException sqlErr = assertInstanceOf(SqlException.class, err.getCause());
        assertEquals(Sql.EXECUTION_CANCELLED_ERR, sqlErr.code());

        await(cancelHandle.cancelAsync());

        // Expect all transactions to be rolled back.
        assertThat(txManager().pending(), is(0));
    }

    private void executeBatchAndCancel(Function<CancellationToken, CompletableFuture<long[]>> execute) {
        CancelHandle cancelHandle = CancelHandle.create();

        // Run statement in another thread
        CompletableFuture<long[]> f = execute.apply(cancelHandle.token());

        waitUntilQueriesInCursorPublicationPhaseCount(greaterThan(0));
        assertThat(f.isDone(), is(false));

        cancelHandle.cancelAsync();

        CompletionException err = assertThrows(CompletionException.class, f::join);
        SqlException sqlErr = assertInstanceOf(SqlException.class, err.getCause());
        assertEquals(Sql.EXECUTION_CANCELLED_ERR, sqlErr.code());

        await(cancelHandle.cancelAsync());

        // Expect all transactions to be rolled back.
        assertThat(txManager().pending(), is(0));

        // Cancellation future is completed before query is deregistered.
        // Let's wait until all signs of query are wiped out to avoid interference
        // between several executions of this method.
        waitUntilRunningQueriesCount(equalTo(0));
    }

    private static class DrainResultSet implements Executable {
        CompletableFuture<? extends AsyncResultSet<SqlRow>> rs;

        DrainResultSet(CompletableFuture<? extends AsyncResultSet<SqlRow>> rs) {
            this.rs = rs;
        }

        @Override
        public void execute() {
            AsyncResultSet<SqlRow> current;
            do {
                current = rs.join();
                if (current.hasRowSet() && current.hasMorePages()) {
                    rs = current.fetchNextPage();
                } else {
                    return;
                }

            } while (current.hasMorePages());
        }
    }

    @Override
    protected ResultSet<SqlRow> executeForRead(IgniteSql sql, Transaction tx, Statement statement, Object... args) {
        return new SyncResultSetAdapter(await(sql.executeAsync(tx, statement, args)));
    }

    @Override
    protected long[] executeBatch(String query, BatchedArguments args) {
        return await(igniteSql().executeBatchAsync(null, query, args));
    }

    @Override
    protected long[] executeBatch(Statement statement, BatchedArguments args) {
        return await(igniteSql().executeBatchAsync(null, statement, args));
    }

    @Override
    protected ResultProcessor execute(Integer expectedPages, Transaction tx, IgniteSql sql, Statement statement, Object... args) {
        AsyncResultProcessor asyncProcessor = new AsyncResultProcessor(expectedPages);
        await(sql.executeAsync(tx, statement, args).thenCompose(asyncProcessor));

        return asyncProcessor;
    }

    @Override
    protected void execute(IgniteSql sql, @Nullable Transaction tx, @Nullable CancellationToken token, String query) {
        sql.execute(tx, token, query);
    }

    @Override
    protected ResultSet<SqlRow> executeLazy(IgniteSql sql, @Nullable CancellationToken token, Statement statement, Object... args) {
        return new SyncResultSetAdapter<>(await(sql.executeAsync(null, token, statement, args)));
    }

    @Override
    protected void executeScript(IgniteSql sql, String query, Object... args) {
        await(sql.executeScriptAsync(query, args));
    }

    @Override
    protected void executeScript(IgniteSql sql, CancellationToken cancellationToken, String query, Object... args) {
        await(sql.executeScriptAsync(cancellationToken, query, args));
    }

    @Override
    protected void rollback(Transaction tx) {
        await(tx.rollbackAsync());
    }

    @Override
    protected void commit(Transaction tx) {
        await(tx.commitAsync());
    }

    @Override
    protected void checkDml(int expectedAffectedRows, Transaction tx, IgniteSql sql, String query, Object... args) {
        AsyncResultSet asyncRes = await(sql.executeAsync(tx, query, args));

        assertFalse(asyncRes.wasApplied());
        assertFalse(asyncRes.hasMorePages());
        assertFalse(asyncRes.hasRowSet());
        assertEquals(expectedAffectedRows, asyncRes.affectedRows());

        assertNull(asyncRes.metadata());

        await(asyncRes.closeAsync());
    }

    @Override
    protected void checkDdl(boolean expectedApplied, IgniteSql sql, String query, Transaction tx) {
        CompletableFuture<AsyncResultSet<SqlRow>> fut = sql.executeAsync(
                tx,
                query
        );

        AsyncResultSet<SqlRow> asyncRes = await(fut);

        assertEquals(expectedApplied, asyncRes.wasApplied());
        assertFalse(asyncRes.hasMorePages());
        assertFalse(asyncRes.hasRowSet());
        assertEquals(-1, asyncRes.affectedRows());

        assertNull(asyncRes.metadata());

        await(asyncRes.closeAsync());
    }

    static class AsyncResultProcessor implements ResultProcessor,
            Function<AsyncResultSet<SqlRow>, CompletionStage<AsyncResultSet<SqlRow>>> {
        private Integer expectedPages;

        private long affectedRows;
        private final List<SqlRow> res = new ArrayList<>();

        AsyncResultProcessor(Integer expectedPages) {
            this.expectedPages = expectedPages;
        }

        @Override
        public CompletionStage<AsyncResultSet<SqlRow>> apply(AsyncResultSet<SqlRow> rs) {
            assertFalse(rs.wasApplied());
            // SELECT
            if (rs.hasRowSet()) {
                assertEquals(-1L, rs.affectedRows());

                if (expectedPages != null) {
                    expectedPages--;
                    assertEquals(expectedPages > 0, rs.hasMorePages(),
                            "hasMorePages(): [expected=" + (expectedPages > 0) + ", actual=" + rs.hasMorePages() + ']');
                }

                rs.currentPage().forEach(res::add);

                if (rs.hasMorePages()) {
                    return rs.fetchNextPage().thenCompose(this);
                }
            } else { // DML/DDL
                affectedRows = rs.affectedRows();
                assertNotEquals(-1L, affectedRows());
            }

            return rs.closeAsync().thenApply(v -> rs);
        }

        @Override
        public List<SqlRow> result() {
            //noinspection AssignmentOrReturnOfFieldWithMutableType
            if (expectedPages != null) {
                assertEquals(0, expectedPages, "Expected to be read more pages");
            }

            return res;
        }

        @Override
        public long affectedRows() {
            return affectedRows;
        }
    }
}
