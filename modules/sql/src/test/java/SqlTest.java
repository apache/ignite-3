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

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Flow;
import java.util.function.Consumer;
import org.apache.ignite.query.sql.IgniteSql;
import org.apache.ignite.query.sql.SqlResultSet;
import org.apache.ignite.query.sql.SqlResultSetMeta;
import org.apache.ignite.query.sql.SqlRow;
import org.apache.ignite.query.sql.SqlSession;
import org.apache.ignite.query.sql.reactive.ReactiveSqlResultSet;
import org.apache.ignite.schema.ColumnType;
import org.apache.ignite.tx.IgniteTransactions;
import org.apache.ignite.tx.Transaction;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Disabled // TODO: create a ticket to fix this.
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
public class SqlTest {
    @Mock
    IgniteSql queryMgr;

    @Mock
    private IgniteTransactions igniteTx;

    @Mock
    private Transaction tx;

    @BeforeEach
    void setUp() {
        initMock();
    }

    @Test
    public void testSynchronousSql() {
        igniteTx.runInTransaction(tx -> {
            SqlSession sess = queryMgr.session().withTransaction(tx);

            SqlResultSet rs = sess.execute("SELECT id, val FROM table WHERE id < {} AND val LIKE {};", 10, "str%");

            for (SqlRow r : rs) {
                assertTrue(10 > r.longValue("id"));
                assertTrue((r.stringValue("val")).startsWith("str"));
            }

            tx.commit();
        });

        Mockito.verify(tx).commit();
    }

    @Test
    public void testAsyncSql() {
        igniteTx.beginAsync().thenApply(tx -> queryMgr.session().withTransaction(tx))
            .thenCompose(sess -> sess.executeAsync("SELECT id, val FROM table WHERE id == {};", 10)
                .thenCompose(rs -> {
                    String str = rs.iterator().next().stringValue("val");

                    return sess.executeAsync("SELECT val FROM table where val LIKE {};", str);
                })
                .thenApply(ignore -> sess.transaction())
            ).thenAccept(Transaction::commitAsync);

        Mockito.verify(tx).commitAsync();
    }

    @Test
    public void testReactiveSql() {
        SqlRowSubscriber subscriber = new SqlRowSubscriber(row -> {
            assertTrue(10 > row.longValue("id"));
            assertTrue(row.stringValue("val").startsWith("str"));
        });

        igniteTx.beginAsync().thenApply(tx -> queryMgr.session().withTransaction(tx))
            .thenCompose(session -> {
                session.executeReactive("SELECT id, val FROM table WHERE id < {} AND val LIKE {};", 10, "str%")
                    .subscribe(subscriber);

                return subscriber.exceptionally(th -> {
                    return session.transaction().rollbackAsync();
                }).thenApply(ignore -> session.transaction().commitAsync());
            });

        Mockito.verify(tx).commitAsync();
    }

    @Disabled
    @Test
    public void testMetadata() {
        SqlResultSet rs = queryMgr.session().execute("SELECT id, val FROM table WHERE id < {} AND val LIKE {}; ", 10, "str%");

        SqlRow row = rs.iterator().next();

        SqlResultSetMeta meta = rs.metadata();

        assertEquals(rs.metadata().columnsCount(), row.columnCount());

        assertEquals(0, meta.indexOf("id"));
        assertEquals(1, meta.indexOf("val"));

        assertEquals("id", meta.column(0).name());
        assertEquals("val", meta.column(1).name());

        assertEquals(ColumnType.INT64, meta.column(0).columnType());
        assertEquals(ColumnType.string(), meta.column(1).columnType());

        assertFalse(meta.column(0).nullable());
        assertTrue(meta.column(1).nullable());
    }

    private void initMock() {
        SqlSession session = Mockito.mock(SqlSession.class);

        Mockito.when(queryMgr.session()).thenReturn(session);

        Mockito.when(session.withTransaction(tx)).thenReturn(session);
        Mockito.when(session.transaction()).thenReturn(tx);

        Mockito.when(session.execute(Mockito.eq("SELECT id, val FROM table WHERE id < {} AND val LIKE {};"), Mockito.any())).
            thenAnswer(ans -> Mockito.when(Mockito.mock(SqlResultSet.class).iterator())
                .thenReturn(List.of(
                    new TestRow().set("id", 1L).set("val", "string 1").build(),
                    new TestRow().set("id", 2L).set("val", "string 2").build(),
                    new TestRow().set("id", 5L).set("val", "string 3").build()
                ).iterator()).getMock());

        Mockito.when(session.executeAsync(Mockito.eq("SELECT id, val FROM table WHERE id == {};"), Mockito.any()))
            .thenAnswer(ans -> {
                Object mock = Mockito.when(Mockito.mock(SqlResultSet.class).iterator())
                    .thenReturn(List.of(new TestRow().set("id", 1L).set("val", "string 1").build()).iterator())
                    .getMock();

                return CompletableFuture.completedFuture(mock);
            });

        Mockito.when(session.executeAsync(Mockito.eq("SELECT val FROM table where val LIKE {};"), Mockito.any()))
            .thenAnswer(ans -> {
                Object mock = Mockito.when(Mockito.mock(SqlResultSet.class).iterator())
                    .thenReturn(List.of(new TestRow().set("id", 10L).set("val", "string 10").build()).iterator())
                    .getMock();

                return CompletableFuture.completedFuture(mock);
            });

        Mockito.when(session.executeReactive(Mockito.startsWith("SELECT id, val FROM table WHERE id < {} AND val LIKE {};"), Mockito.any()))
            .thenAnswer(invocation -> {
                ReactiveSqlResultSet mock = Mockito.mock(ReactiveSqlResultSet.class);

                Mockito.doAnswer(ans -> {
                    Flow.Subscriber subscrber = ans.getArgument(0);

                    subscrber.onSubscribe(Mockito.mock(Flow.Subscription.class));

                    List.of(
                        new TestRow().set("id", 1L).set("val", "string 1").build(),
                        new TestRow().set("id", 2L).set("val", "string 2").build(),
                        new TestRow().set("id", 5L).set("val", "string 3").build()
                    ).forEach(i -> subscrber.onNext(i));

                    subscrber.onComplete();

                    return ans;
                }).when(mock).subscribe(Mockito.any(Flow.Subscriber.class));

                return mock;
            });

        Mockito.doAnswer(invocation -> {
            Consumer<Transaction> argument = invocation.getArgument(0);

            argument.accept(tx);

            return null;
        }).when(igniteTx).runInTransaction(Mockito.any());

        Mockito.when(igniteTx.beginAsync()).thenReturn(CompletableFuture.completedFuture(tx));
    }

    /**
     * Dummy subsctiber for test purposes.
     */
    static class SqlRowSubscriber extends CompletableFuture implements Flow.Subscriber<SqlRow> {
        private Consumer<SqlRow> rowConsumer;

        SqlRowSubscriber(Consumer<SqlRow> rowConsumer) {
            this.rowConsumer = rowConsumer;
        }

        @Override public void onSubscribe(Flow.Subscription subscription) {
            whenCompleteAsync((res, th) -> {
                if (th != null)
                    subscription.cancel();
            });

            subscription.request(Long.MAX_VALUE); // Unbounded.
        }

        @Override public void onNext(SqlRow row) {
            rowConsumer.accept(row);
        }

        @Override public void onError(Throwable throwable) {
            completeExceptionally(throwable);
        }

        @Override public void onComplete() {
            complete(null);
        }
    }
}
