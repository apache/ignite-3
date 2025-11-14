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
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.function.Function;
import org.apache.ignite.internal.testframework.IgniteTestUtils;
import org.apache.ignite.lang.CancelHandle;
import org.apache.ignite.lang.CancellationToken;
import org.apache.ignite.lang.ErrorGroups.Sql;
import org.apache.ignite.sql.BatchedArguments;
import org.apache.ignite.sql.IgniteSql;
import org.apache.ignite.sql.ResultSet;
import org.apache.ignite.sql.SqlException;
import org.apache.ignite.sql.SqlRow;
import org.apache.ignite.sql.Statement;
import org.apache.ignite.tx.Transaction;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for synchronous SQL API.
 */
public class ItSqlSynchronousApiTest extends ItSqlApiBaseTest {

    private final List<Transaction> transactions = new ArrayList<>();

    @AfterEach
    public void rollbackTransactions() {
        transactions.forEach(Transaction::rollback);
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

            return sql.execute(null, token, statement);
        });

        // with transaction
        executeAndCancel((token) -> {
            Statement statement = sql.statementBuilder()
                    .query(query)
                    .build();

            Transaction transaction = igniteTx().begin();

            transactions.add(transaction);

            return sql.execute(transaction, token, statement);
        });
    }

    @Test
    @Override
    public void cancelQueryString() throws InterruptedException {
        IgniteSql sql = igniteSql();
        String query = "SELECT * FROM system_range(0, 10000000000)";

        // no transaction
        executeAndCancel((token) -> {
            return sql.execute(null, token, query);
        });

        // with transaction
        executeAndCancel((token) -> {
            Transaction transaction = igniteTx().begin();

            transactions.add(transaction);

            return sql.execute(transaction, token, query);
        });
    }

    @Override
    @Test
    public void cancelBatch() throws InterruptedException {
        IgniteSql sql = igniteSql();

        sql("CREATE TABLE TEST(ID INT PRIMARY KEY, VAL INT)");

        BatchedArguments args = BatchedArguments.of(0, 0);
        for (int i = 1; i < ROW_COUNT; ++i) {
            args.add(i, i);
        }

        String query = "UPDATE TEST SET val = ? + (SELECT COUNT(*) FROM system_range(?, 10000000000))";

        // no transaction
        executeBatchAndCancel((token) -> sql.executeBatch(null, token, query, args));

        // with transaction
        executeBatchAndCancel((token) -> {
            Transaction transaction = igniteTx().begin();

            transactions.add(transaction);

            return sql.executeBatch(transaction, token, query, args);
        });
    }

    @Test
    void closeCursorWithoutReadingAllPages() {
        IgniteSql sql = igniteSql();

        Statement stmt = sql.statementBuilder()
                .query("SELECT * FROM TABLE(SYSTEM_RANGE(0, 1))")
                .pageSize(1)
                .build();

        try (var ignored = sql.execute(null, stmt)) {
            // No-op.
        }
    }

    private void executeAndCancel(Function<CancellationToken, ResultSet<SqlRow>> execute) throws InterruptedException {
        CancelHandle cancelHandle = CancelHandle.create();

        CountDownLatch latch = new CountDownLatch(1);

        // Run statement in another thread
        CompletableFuture<Void> f = CompletableFuture.runAsync(() -> {
            try (ResultSet<SqlRow> resultSet = execute.apply(cancelHandle.token())) {
                // Start a query and cancel it later.
                latch.countDown();
                while (resultSet.hasNext()) {
                    resultSet.next();
                }
            }
        });

        latch.await();
        cancelHandle.cancelAsync();

        CompletionException err = assertThrows(CompletionException.class, f::join);
        SqlException sqlErr = assertInstanceOf(SqlException.class, err.getCause());
        assertEquals(Sql.EXECUTION_CANCELLED_ERR, sqlErr.code());

        await(cancelHandle.cancelAsync());

        // Expect all transactions to be rolled back.
        assertThat(txManager().pending(), is(0));
    }

    private void executeBatchAndCancel(Function<CancellationToken, long[]> execute) throws InterruptedException {
        CancelHandle cancelHandle = CancelHandle.create();

        // Run statement in another thread
        CompletableFuture<Void> f = IgniteTestUtils.runAsync(() -> {
            execute.apply(cancelHandle.token());
        });

        waitUntilRunningQueriesCount(greaterThan(0));
        assertThat(f.isDone(), is(false));

        cancelHandle.cancelAsync();

        CompletionException err = assertThrows(CompletionException.class, f::join);
        SqlException sqlErr = assertInstanceOf(SqlException.class, err.getCause());
        assertEquals(Sql.EXECUTION_CANCELLED_ERR, sqlErr.code());

        await(cancelHandle.cancelAsync());

        // Expect all transactions to be rolled back.
        assertThat(txManager().pending(), is(0));
    }

    @Override
    protected ResultSet<SqlRow> executeForRead(IgniteSql sql, Transaction tx, Statement statement, Object... args) {
        return sql.execute(tx, statement, args);
    }

    @Override
    protected long[] executeBatch(String query, BatchedArguments args) {
        return igniteSql().executeBatch(null, query, args);
    }

    @Override
    protected long[] executeBatch(Statement statement, BatchedArguments args) {
        return igniteSql().executeBatch(null, statement, args);
    }

    @Override
    protected ResultProcessor execute(Integer expectedPages, Transaction tx, IgniteSql sql, Statement statement, Object... args) {
        SyncPageProcessor syncProcessor = new SyncPageProcessor();

        ResultSet<SqlRow> rs = sql.execute(tx, statement, args);
        syncProcessor.process(rs);

        return syncProcessor;
    }

    @Override
    protected void execute(IgniteSql sql, @Nullable Transaction tx, @Nullable CancellationToken token, String query) {
        sql.executeAsync(tx, token, query).join();
    }

    protected ResultProcessor execute(int expectedPages, Transaction tx, IgniteSql sql, String query, Object... args) {
        SyncPageProcessor syncProcessor = new SyncPageProcessor();

        ResultSet<SqlRow> rs = sql.execute(tx, query, args);
        syncProcessor.process(rs);

        return syncProcessor;
    }

    @Override
    protected ResultSet<SqlRow> executeLazy(IgniteSql sql, @Nullable CancellationToken token, Statement statement, Object... args) {
        return sql.execute(null, token, statement, args);
    }

    @Override
    protected void executeScript(IgniteSql sql, String query, Object... args) {
        sql.executeScript(query, args);
    }

    @Override
    protected void executeScript(IgniteSql sql, CancellationToken cancellationToken, String query, Object... args) {
        sql.executeScript(cancellationToken, query, args);
    }

    @Override
    protected void rollback(Transaction tx) {
        tx.rollback();
    }

    @Override
    protected void commit(Transaction tx) {
        tx.commit();
    }

    @Override
    protected void checkDml(int expectedAffectedRows, Transaction tx, IgniteSql sql, String query, Object... args) {
        ResultSet res = sql.execute(
                tx,
                query,
                args
        );

        assertFalse(res.wasApplied());
        assertFalse(res.hasRowSet());
        assertEquals(expectedAffectedRows, res.affectedRows());

        res.close();
    }

    @Override
    protected void checkDdl(boolean expectedApplied, IgniteSql sql, String query, Transaction tx) {
        ResultSet res = sql.execute(
                tx,
                query
        );

        assertEquals(expectedApplied, res.wasApplied());
        assertFalse(res.hasRowSet());
        assertEquals(-1, res.affectedRows());

        res.close();
    }

    static class SyncPageProcessor implements ResultProcessor {
        private final List<SqlRow> res = new ArrayList<>();
        private long affectedRows;

        @Override
        public List<SqlRow> result() {
            //noinspection AssignmentOrReturnOfFieldWithMutableType
            return res;
        }

        @Override
        public long affectedRows() {
            return affectedRows;
        }

        public void process(ResultSet<SqlRow> resultSet) {
            affectedRows = resultSet.affectedRows();
            resultSet.forEachRemaining(res::add);
        }
    }
}
