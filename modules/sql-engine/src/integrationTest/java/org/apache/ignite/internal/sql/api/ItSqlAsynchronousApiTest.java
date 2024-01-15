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
import org.apache.ignite.sql.Session;
import org.apache.ignite.sql.SqlRow;
import org.apache.ignite.sql.async.AsyncResultSet;
import org.apache.ignite.tx.Transaction;
import org.junit.jupiter.api.Test;

/**
 * Tests for asynchronous SQL API.
 */
@SuppressWarnings("ThrowableNotThrown")
public class ItSqlAsynchronousApiTest extends ItSqlApiBaseTest {
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

        AsyncResultProcessor pageProc = new AsyncResultProcessor(ROW_COUNT - res.size());
        await(ars4.fetchNextPage().thenCompose(pageProc));

        res.addAll(pageProc.result());

        for (int i = 0; i < ROW_COUNT; ++i) {
            assertEquals(i, res.get(i).intValue(0));
        }
    }

    @Override
    protected ResultSet<SqlRow> executeForRead(Session ses, Transaction tx, String query, Object... args) {
        return new SyncResultSetAdapter(await(ses.executeAsync(tx, query, args)));
    }

    @Override
    protected long[] executeBatch(Session ses, String sql, BatchedArguments args) {
        return await(ses.executeBatchAsync(null, sql, args));
    }

    @Override
    protected ResultProcessor execute(Integer expectedPages, Transaction tx, Session ses, String sql, Object... args) {
        AsyncResultProcessor asyncProcessor = new AsyncResultProcessor(expectedPages);
        await(ses.executeAsync(tx, sql, args).thenCompose(asyncProcessor));

        return asyncProcessor;
    }

    @Override
    protected void executeScript(Session ses, String sql, Object... args) {
        await(ses.executeScriptAsync(sql, args));
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
    protected void checkDml(int expectedAffectedRows, Transaction tx, Session ses, String sql, Object... args) {
        AsyncResultSet asyncRes = await(ses.executeAsync(tx, sql, args));

        assertFalse(asyncRes.wasApplied());
        assertFalse(asyncRes.hasMorePages());
        assertFalse(asyncRes.hasRowSet());
        assertEquals(expectedAffectedRows, asyncRes.affectedRows());

        assertNull(asyncRes.metadata());

        await(asyncRes.closeAsync());
    }

    @Override
    protected void checkDdl(boolean expectedApplied, Session ses, String sql, Transaction tx) {
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
            //SELECT
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
            } else { //DML/DDL
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
