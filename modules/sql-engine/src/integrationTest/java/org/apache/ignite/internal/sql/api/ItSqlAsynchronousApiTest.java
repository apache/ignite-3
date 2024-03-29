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
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.apache.ignite.internal.sql.SyncResultSetAdapter;
import org.apache.ignite.sql.BatchedArguments;
import org.apache.ignite.sql.IgniteSql;
import org.apache.ignite.sql.ResultSet;
import org.apache.ignite.sql.SqlRow;
import org.apache.ignite.sql.Statement;
import org.apache.ignite.sql.async.AsyncResultSet;
import org.apache.ignite.tx.Transaction;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

/**
 * Tests for asynchronous SQL API.
 */
@SuppressWarnings("ThrowableNotThrown")
public class ItSqlAsynchronousApiTest extends ItSqlApiBaseTest {
    @Test
    @Disabled("https://issues.apache.org/jira/browse/IGNITE-18647")
    public void pageSequence() {
        sql("CREATE TABLE TEST(ID INT PRIMARY KEY, VAL0 INT)");

        IgniteSql sql = igniteSql();

        for (int i = 0; i < ROW_COUNT; ++i) {
            sql.execute(null, "INSERT INTO TEST VALUES (?, ?)", i, i);
        }

        Statement statement = sql.statementBuilder()
                .query("SELECT ID FROM TEST ORDER BY ID")
                .pageSize(1)
                .build();

        AsyncResultSet<SqlRow> ars0 = await(sql.executeAsync(null, statement));
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

        AsyncResultProcessor pageProc = new AsyncResultProcessor(ROW_COUNT - res.size());
        await(ars4.fetchNextPage().thenCompose(pageProc));

        res.addAll(pageProc.result());

        for (int i = 0; i < ROW_COUNT; ++i) {
            assertEquals(i, res.get(i).intValue(0));
        }
    }

    @Override
    @Test
    @Disabled("https://issues.apache.org/jira/browse/IGNITE-18647")
    public void select() {
        super.select();
    }

    @Override
    @Test
    @Disabled("https://issues.apache.org/jira/browse/IGNITE-18647")
    public void resultSetCloseShouldFinishImplicitTransaction() {
        super.resultSetCloseShouldFinishImplicitTransaction();
    }

    @Override
    @Test
    @Disabled("https://issues.apache.org/jira/browse/IGNITE-18647")
    public void errors() throws InterruptedException {
        super.errors();
    }

    @Override
    protected ResultSet<SqlRow> executeForRead(IgniteSql sql, Transaction tx, Statement statement, Object... args) {
        return new SyncResultSetAdapter(await(sql.executeAsync(tx, statement, args)));
    }

    @Override
    protected long[] executeBatch(IgniteSql sql, String query, BatchedArguments args) {
        return await(sql.executeBatchAsync(null, query, args));
    }

    @Override
    protected ResultProcessor execute(Integer expectedPages, Transaction tx, IgniteSql sql, Statement statement, Object... args) {
        AsyncResultProcessor asyncProcessor = new AsyncResultProcessor(expectedPages);
        await(sql.executeAsync(tx, statement, args).thenCompose(asyncProcessor));

        return asyncProcessor;
    }

    @Override
    protected void executeScript(IgniteSql sql, String query, Object... args) {
        await(sql.executeScriptAsync(query, args));
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
