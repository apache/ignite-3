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

package org.apache.ignite.internal.sql.engine;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Flow;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.NativeTypes;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.SchemaRegistry;
import org.apache.ignite.internal.util.Constants;
import org.apache.ignite.sql.IgniteSql;
import org.apache.ignite.sql.ResultSet;
import org.apache.ignite.sql.ResultSetMetadata;
import org.apache.ignite.sql.Session;
import org.apache.ignite.sql.SqlRow;
import org.apache.ignite.sql.Statement;
import org.apache.ignite.sql.async.AsyncResultSet;
import org.apache.ignite.sql.reactive.ReactiveResultSet;
import org.apache.ignite.table.RecordView;
import org.apache.ignite.table.Tuple;
import org.apache.ignite.tx.IgniteTransactions;
import org.apache.ignite.tx.Transaction;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

/**
 * Tests IgniteSQL facade API.
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
public class IgniteSqlApiTest {
    @Mock
    IgniteSql queryMgr;

    @Mock
    private IgniteTransactions igniteTx;

    @Mock
    private Transaction transaction;

    @BeforeEach
    void setUp() {
        initMock();
    }

    @Test
    public void testSyncSql() {
        igniteTx.runInTransaction(tx -> {
            Session sess = queryMgr.createSession();

            sess.defaultTimeout(10_000, TimeUnit.MILLISECONDS); // Set default timeout.
            sess.property("memoryQuota", 10 * Constants.MiB); // Set default quota.

            // Execute in TX.
            ResultSet rs = sess.execute(tx, "INSERT INTO tbl VALUES (?, ?)", 10, "str");

            assertEquals(1, rs.affectedRows());

            // Execute outside TX.
            rs = sess.execute(null, "SELECT id, val FROM tbl WHERE id < {};", 10);

            for (SqlRow r : rs) {
                assertTrue(10 > r.longValue("id"));
                assertTrue((r.stringValue("val")).startsWith("str"));
            }

            tx.commit();
        });

        Mockito.verify(transaction).commit();
    }

    @Test
    public void testSyncSql2() {
        RecordView<Tuple> tbl = getTable();

        Session sess = queryMgr.createSession();

        // Execute outside TX.
        ResultSet rs = sess.execute(null, "SELECT id, val FROM tbl WHERE id < {};", 10);
        SqlRow row = rs.iterator().next();

        igniteTx.beginAsync().thenAccept(tx -> {
            // Execute in TX.
            tbl.insertAsync(tx, Tuple.create().set("val", "NewValue"))
                    .thenAccept(r -> tx.rollback());

            Mockito.verify(tx, Mockito.times(1)).rollback();
        }).join();
    }

    @Test
    public void testSyncMultiStatementSql() {
        Session sess = queryMgr.createSession();

        sess.executeScript(
                "CREATE TABLE tbl(id INTEGER PRIMARY KEY, val VARCHAR);"
                        + "INSERT INTO tbl VALUES (1, 2);"
                        + "INSERT INTO tbl2 (SELECT id, val FROM tbl WHERE id == {});"
                        + "DROP TABLE tbl", 10);
    }

    @Disabled
    @Test
    public void testStatement() {
        igniteTx.runInTransaction(tx -> {
            // Do the same as query "INSERT INTO tbl VALUES (1, "string 1") (2, "string 2) ... (5, "string 5");"
            final Statement stmt = queryMgr.createStatement("INSERT INTO tbl VALUES (?, ?)");

            final int[] rs = queryMgr.createSession().executeBatch(tx, stmt,
                   List.of(
                            List.of(1, "string 1"),
                            List.of(2, "string 2"),
                            List.of(3, "string 3"),
                            List.of(4, "string 4"),
                            List.of(5, "string 5")
                   ));

            assertEquals(5, rs.length);

            tx.commit();
        });

        Mockito.verify(transaction).commitAsync();
    }


    @Test
    public void testAsyncSql() throws ExecutionException, InterruptedException {
        RecordView<Tuple> table = getTable();

        class AsyncPageProcessor implements
                Function<AsyncResultSet, CompletionStage<AsyncResultSet>> {
            private final Transaction tx0;

            public AsyncPageProcessor(Transaction tx0) {
                this.tx0 = tx0;
            }

            @Override
            public CompletionStage<AsyncResultSet> apply(AsyncResultSet rs) {
                for (SqlRow row : rs.currentPage()) {
                    table.getAsync(null, Tuple.create().set("id", row.intValue(0)));
                }

                if (rs.hasMorePages()) {
                    return rs.fetchNextPage().thenCompose(this);
                }

                return CompletableFuture.completedFuture(rs);
            }
        }

        igniteTx.beginAsync()
                .thenCompose(tx0 -> queryMgr.createSession()
                                            .executeAsync(tx0, "SELECT val FROM tbl where val LIKE {};", "val%")
                                            .thenCompose(new AsyncPageProcessor(tx0))
                                            .thenApply(ignore -> tx0.commitAsync())
                                            .thenApply(ignore -> tx0))
                .get();

        Mockito.verify(transaction).commitAsync();
        Mockito.verify(table, Mockito.times(6)).getAsync(Mockito.any(), Mockito.any());
    }

    @Test
    public void testReactiveSql() {
        SqlRowSubscriber subscriber = new SqlRowSubscriber(row -> {
            assertTrue(10 > row.longValue("id"));
            assertTrue(row.stringValue("val").startsWith("str"));
        });

        igniteTx.beginAsync().thenApply(tx -> {
            final Session session = queryMgr.createSession();

            session.executeReactive(
                    tx, "SELECT id, val FROM tbl WHERE id < {} AND val LIKE {};", 10,
                    "str%")
                    .subscribe(subscriber);

            return subscriber.exceptionally(th -> {
                tx.rollbackAsync();

                return null;
            }).thenApply(ignore -> tx.commitAsync());
        });

        Mockito.verify(transaction).commitAsync();
    }

    @Disabled
    @Test
    public void testMetadata() {
        ResultSet rs = queryMgr.createSession()
                               .execute(null, "SELECT id, val FROM tbl WHERE id < {} AND val LIKE {}; ", 10,
                                       "str%");

        SqlRow row = rs.iterator().next();

        ResultSetMetadata meta = rs.metadata();

        assertEquals(rs.metadata().columnsCount(), row.columnCount());

        assertEquals(0, meta.indexOf("id"));
        assertEquals(1, meta.indexOf("val"));

        assertEquals("id", meta.column(0).name());
        assertEquals("val", meta.column(1).name());

        assertEquals(Long.class, meta.column(0).valueClass());
        assertEquals(String.class, meta.column(1).valueClass());

        assertFalse(meta.column(0).nullable());
        assertTrue(meta.column(1).nullable());
    }

    @NotNull
    private RecordView<Tuple> getTable() {
        SchemaDescriptor schema = new SchemaDescriptor(42,
                new Column[]{new Column("id", NativeTypes.INT64, false)},
                new Column[]{new Column("val", NativeTypes.STRING, true)}
        );

        SchemaRegistry schemaReg = Mockito.mock(SchemaRegistry.class);
        Mockito.when(schemaReg.schema()).thenReturn(schema);

        RecordView<Tuple> tbl = (RecordView<Tuple>) Mockito.mock(RecordView.class);
        Mockito.when(tbl.insertAsync(Mockito.any(), Mockito.any()))
                .thenReturn(CompletableFuture.completedFuture(null));
        Mockito.when(tbl.getAsync(Mockito.any(), Mockito.any()))
                .thenAnswer(ans -> CompletableFuture.completedFuture(ans.getArgument(0)));

        return tbl;
    }

    private void initMock() {
        Session session = Mockito.mock(Session.class);

        Mockito.when(queryMgr.createSession()).thenReturn(session);

        List<SqlRow> query1Resuls = List.of(
                new TestRow().set("id", 1L).set("val", "string 1").build(),
                new TestRow().set("id", 2L).set("val", "string 2").build(),
                new TestRow().set("id", 5L).set("val", "string 3").build()
        );

        Mockito.when(session.execute(Mockito.nullable(Transaction.class), Mockito.eq("INSERT INTO tbl VALUES (?, ?)"),
                Mockito.any(),
                Mockito.any()))
                .thenAnswer(ans -> Mockito.when(Mockito.mock(ResultSet.class).affectedRows())
                                           .thenReturn(1).getMock());

        Mockito.when(session.executeBatch(Mockito.nullable(Transaction.class), Mockito.any(String.class), Mockito.any()))
                .thenReturn(new int[]{1, 1, 1, 1, 1});

        Mockito.when(session.execute(Mockito.nullable(Transaction.class), Mockito.eq("SELECT id, val FROM tbl WHERE id < {};"),
                Mockito.any()))
                .thenAnswer(ans -> Mockito.when(Mockito.mock(ResultSet.class).iterator())
                                           .thenReturn(query1Resuls.iterator()).getMock());

        Mockito.when(session.executeAsync(Mockito.nullable(Transaction.class), Mockito.eq("SELECT id, val FROM tbl WHERE id == {};"),
                Mockito.any()))
                .thenAnswer(ans -> CompletableFuture.completedFuture(
                        Mockito.when(Mockito.mock(AsyncResultSet.class).currentPage())
                                .thenReturn(
                                        List.of(new TestRow().set("id", 1L).set("val", "string 1")
                                                        .build()))
                                .getMock())
                );

        Mockito.when(session.executeAsync(Mockito.nullable(Transaction.class), Mockito.eq("SELECT val FROM tbl where val LIKE {};"),
                Mockito.any()))
                .thenAnswer(ans -> {
                    AsyncResultSet page2 = Mockito.mock(AsyncResultSet.class);
                    Mockito.when(page2.currentPage()).thenReturn(query1Resuls);
                    Mockito.when(page2.hasMorePages()).thenReturn(false);

                    AsyncResultSet page1 = Mockito.mock(AsyncResultSet.class);
                    Mockito.when(page1.currentPage()).thenReturn(query1Resuls);
                    Mockito.when(page1.hasMorePages()).thenReturn(true);
                    Mockito.when(page1.fetchNextPage())
                            .thenReturn((CompletionStage) CompletableFuture.completedFuture(page2));

                    return CompletableFuture.completedFuture(page1);
                });

        Mockito.when(session.executeReactive(
                Mockito.nullable(Transaction.class), Mockito.startsWith("SELECT id, val FROM tbl WHERE id < {} AND val LIKE {};"),
                Mockito.any(), Mockito.any()))
                .thenAnswer(invocation -> {
                    ReactiveResultSet mock = Mockito.mock(ReactiveResultSet.class);

                    Mockito.doAnswer(ans -> {
                        Flow.Subscriber subscrber = ans.getArgument(0);

                        subscrber.onSubscribe(Mockito.mock(Flow.Subscription.class));

                        query1Resuls.forEach(i -> subscrber.onNext(i));

                        subscrber.onComplete();

                        return ans;
                    }).when(mock).subscribe(Mockito.any(Flow.Subscriber.class));

                    return mock;
                });

        Mockito.doAnswer(invocation -> {
            Consumer<Transaction> argument = invocation.getArgument(0);

            argument.accept(transaction);

            return null;
        }).when(igniteTx).runInTransaction(Mockito.any(Consumer.class));

        Mockito.when(igniteTx.beginAsync()).thenReturn(CompletableFuture.completedFuture(transaction));
    }


    static class SqlRowSubscriber extends CompletableFuture<Void> implements
            Flow.Subscriber<SqlRow> {
        private Consumer<SqlRow> rowConsumer;

        SqlRowSubscriber(Consumer<SqlRow> rowConsumer) {
            this.rowConsumer = rowConsumer;
        }

        @Override
        public void onSubscribe(Flow.Subscription subscription) {
            whenCompleteAsync((res, th) -> {
                if (th != null) {
                    subscription.cancel();
                }
            });

            subscription.request(Long.MAX_VALUE); // Unbounded.
        }

        @Override
        public void onNext(SqlRow row) {
            rowConsumer.accept(row);
        }

        @Override
        public void onError(Throwable throwable) {
            completeExceptionally(throwable);
        }

        @Override
        public void onComplete() {
            complete(null);
        }
    }
}
