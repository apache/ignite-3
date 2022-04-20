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

import com.google.common.collect.Streams;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Flow;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.NativeTypes;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.SchemaRegistry;
import org.apache.ignite.internal.util.Constants;
import org.apache.ignite.sql.BatchedArguments;
import org.apache.ignite.sql.IgniteSql;
import org.apache.ignite.sql.ResultSet;
import org.apache.ignite.sql.ResultSetMetadata;
import org.apache.ignite.sql.Session;
import org.apache.ignite.sql.Session.SessionBuilder;
import org.apache.ignite.sql.SqlRow;
import org.apache.ignite.sql.Statement;
import org.apache.ignite.sql.Statement.StatementBuilder;
import org.apache.ignite.sql.async.AsyncResultSet;
import org.apache.ignite.sql.reactive.ReactiveResultSet;
import org.apache.ignite.table.KeyValueView;
import org.apache.ignite.table.Tuple;
import org.apache.ignite.tx.IgniteTransactions;
import org.apache.ignite.tx.Transaction;
import org.apache.ignite.tx.TransactionException;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Answers;
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
    IgniteSql igniteSql;

    @Mock
    private IgniteTransactions igniteTx;

    private Transaction transaction;

    private Map<Integer, String> txState;

    private Map<Integer, String> globalState;

    @BeforeEach
    void setUp() {
        initMock();
    }


    @Test
    public void statementApi() {
        // Default session.
        Session session = igniteSql.createSession();

        // Simple statement.
        Statement simpleStatement = igniteSql.createStatement("SELECT id, val FROM tbl WHERE id > ?");

        // Create session with params.
        Session sessionWithParams = igniteSql.sessionBuilder()
                .defaultTimeout(10_000, TimeUnit.MILLISECONDS) // Set default timeout.
                .property("memoryQuota", 10 * Constants.MiB) // Set default quota.
                .build();

        // Statement with params. Prepared statement.
        Statement preparedStatement = igniteSql.statementBuilder()
                .query("SELECT id, val FROM tbl WHERE id > ?")
                .defaultSchema("PUBLIC")
                .prepared()
                .build();

        // Execute statement.
        session.execute(null, simpleStatement,  /* args */ 1);

        // Execute same statement in different sessions is allowed.
        session.execute(null, preparedStatement,  /* args */ 1);
        sessionWithParams.execute(null, preparedStatement,  /* args */ 1);

        // Releasing session resources.
        session.close();
    }

    @Test
    public void syncSqlApi() {
        final Session sess = igniteSql.createSession();

        // Execute DDL.
        ResultSet rs = sess.execute(null, "CREATE TABLE IF NOT EXITS tbl (id INT PRIMARY KEY, val VARCHAR)");

        assertTrue(rs.wasApplied());
        assertFalse(rs.hasRowSet());
        assertEquals(-1, rs.affectedRows());

        // Execute DML.
        rs = sess.execute(null, "INSERT INTO tbl VALUES (?, ?)", 1, "str1");

        assertEquals(1, rs.affectedRows());
        assertFalse(rs.wasApplied());
        assertFalse(rs.hasRowSet());

        // Execute batched DML query.
        int[] res = sess.executeBatch(null, "INSERT INTO tbl VALUES (?, ?)",
                BatchedArguments.of(2, "str2").add(3, "str3").add(4, "str4"));

        assertEquals(3, res.length);
        assertTrue(Arrays.stream(res).allMatch(i -> i == 1));

        // Execute query.
        rs = sess.execute(null, "SELECT id, val FROM tbl WHERE id > ?", 0);

        assertTrue(rs.hasRowSet());
        assertFalse(rs.wasApplied());
        assertEquals(-1, rs.affectedRows());

        assertTrue(rs.iterator().hasNext());
        for (SqlRow r : rs) {
            assertEquals("str" + r.intValue("id"), r.stringValue("val"));
        }

        // Execute DML.
        rs = sess.execute(null, "DELETE FROM tbl");

        assertEquals(4, rs.affectedRows());
        assertFalse(rs.wasApplied());
        assertFalse(rs.hasRowSet());
    }

    @Test
    public void syncApiWithTx() {
        final Session sess = igniteSql.createSession();

        // Create table.
        sess.execute(null, "CREATE TABLE IF NOT EXITS tbl (id INT PRIMARY KEY, val VARCHAR)");

        igniteTx.runInTransaction(tx -> {
            // Execute DML in tx.
            ResultSet rs = sess.execute(tx, "INSERT INTO tbl VALUES (?, ?)", 1, "str1");

            assertEquals(1, rs.affectedRows());

            // Execute batched DML query.
            int[] res = sess.executeBatch(tx, "INSERT INTO tbl VALUES (?, ?)",
                    BatchedArguments.of(2, "str2").add(3, "str3").add(4, "str4"));

            assertTrue(Arrays.stream(res).allMatch(i -> i == 1));

            // Execute query in TX.
            rs = sess.execute(tx, "SELECT id, val FROM tbl WHERE id > ?", 0);

            assertTrue(rs.iterator().hasNext());
            for (SqlRow r : rs) {
                assertEquals("str" + r.intValue("id"), r.stringValue("val"));
            }

            // Execute query outside TX in the same session.
            rs = sess.execute(null, "SELECT id, val FROM tbl WHERE id > ?", 1);

            assertFalse(rs.iterator().hasNext()); // No data found before TX is commited.

            tx.commit();

            // Execute DML outside tx in same session after tx commit.
            rs = sess.execute(null, "DELETE FROM tbl");

            assertEquals(4, rs.affectedRows());
        });

        Mockito.verify(transaction).commit();
    }

    @Test
    public void mixSqlAndTableApiInTx() {
        // Create table.
        Session sess = igniteSql.createSession();
        sess.execute(null, "CREATE TABLE IF NOT EXITS tbl (id INT PRIMARY KEY, val VARCHAR)");
        sess.execute(null, "INSERT INTO tbl VALUES (?, ?)", 1, "str1");

        KeyValueView<Tuple, Tuple> tbl = getTable();

        igniteTx.beginAsync().thenAccept(tx -> {
            // Execute in TX.
            tbl.putAsync(tx, Tuple.create().set("id", 2), Tuple.create().set("val", "str2"))
                    .thenAccept(r -> tx.commit());
        }).join();

        ResultSet rs = sess.execute(null, "SELECT id, val FROM tbl WHERE id > ?", 1);
        assertTrue(rs.iterator().hasNext());

        igniteTx.beginAsync().thenAccept(tx -> {
            // Execute in TX.
            tbl.putAsync(tx, Tuple.create().set("id", 3), Tuple.create().set("val", "NewValue"))
                    .thenApply(f -> {
                        ResultSet rs0 = sess.execute(tx, "SELECT id, val FROM tbl WHERE id > ?", 2);
                        assertTrue(rs0.iterator().hasNext());

                        return f;
                    })
                    .thenAccept(r -> tx.rollback());

            Mockito.verify(tx, Mockito.times(1)).rollback();
        }).join();

        rs = sess.execute(null, "SELECT id, val FROM tbl WHERE id > ?", 2);
        assertFalse(rs.iterator().hasNext());
    }

    @Test
    public void multiStatementQuery() {
        Session sess = igniteSql.createSession();

        sess.executeScript(
                "CREATE TABLE tbl(id INTEGER PRIMARY KEY, val VARCHAR);"
                        + "CREATE TABLE tbl2(id INTEGER PRIMARY KEY, val VARCHAR);"
                        + "INSERT INTO tbl VALUES (1, \"str1\"), (2, \"str2\"), (3, \"str3\");"
                        + "INSERT INTO tbl2 (SELECT id, val FROM tbl WHERE id > ?);"
                        + "DROP TABLE tbl", 1);

        ResultSet rs = sess.execute(null, "SELECT id, val FROM tbl2");

        assertEquals(2, Streams.stream(rs.iterator()).count());
    }

    @Test
    public void testAsyncSql() throws ExecutionException, InterruptedException {
        { // Init table
            Session sess = igniteSql.createSession();
            sess.execute(null, "CREATE TABLE IF NOT EXITS tbl (id INT PRIMARY KEY, val VARCHAR)");
            sess.executeBatch(null, "INSERT INTO tbl VALUES (?, ?)",
                    BatchedArguments.of(1, "str1").add(2, "str2").add(3, "str3").add(4, "str4").add(5, "str5"));
        }

        KeyValueView<Tuple, Tuple> table = getTable();

        class AsyncPageProcessor implements
                Function<AsyncResultSet, CompletionStage<AsyncResultSet>> {
            private final Transaction tx0;

            public AsyncPageProcessor(Transaction tx0) {
                this.tx0 = tx0;
            }

            @Override
            public CompletionStage<AsyncResultSet> apply(AsyncResultSet rs) {
                for (SqlRow row : rs.currentPage()) {
                    table.getAsync(tx0, Tuple.create().set("id", row.intValue(0)));
                }

                if (rs.hasMorePages()) {
                    return rs.fetchNextPage().thenCompose(this);
                }

                return CompletableFuture.completedFuture(rs);
            }
        }

        igniteTx.beginAsync()
                .thenCompose(tx0 -> igniteSql.createSession()
                        .executeAsync(tx0, "SELECT id, val FROM tbl WHERE id > ?", 1)
                        .thenCompose(new AsyncPageProcessor(tx0))
                        .thenCompose(ignore -> tx0.commitAsync())
                        .thenApply(ignore -> tx0))
                .get();

        Mockito.verify(transaction).commitAsync();
        Mockito.verify(table, Mockito.times(4)).getAsync(Mockito.any(), Mockito.any());
    }

    @Test
    public void testReactiveSql() {
        { // Init table
            Session sess = igniteSql.createSession();
            sess.execute(null, "CREATE TABLE IF NOT EXITS tbl (id INT PRIMARY KEY, val VARCHAR)");
            sess.executeBatch(null, "INSERT INTO tbl VALUES (?, ?)",
                    BatchedArguments.of(1, "str1").add(2, "str2").add(3, "str3").add(4, "str4"));
        }

        SqlRowSubscriber subscriber = new SqlRowSubscriber(row -> {
            assertTrue(10 > row.intValue("id"));
            assertTrue(row.stringValue("val").startsWith("str"));
        });

        igniteTx.beginAsync().thenApply(tx -> {
            final Session session = igniteSql.createSession();

            session.executeReactive(tx, "SELECT id, val FROM tbl WHERE id > ?", 1)
                    .subscribe(subscriber);

            return subscriber.exceptionally(th -> {
                tx.rollbackAsync();

                return null;
            }).thenApply(ignore -> tx.commitAsync());
        }).join();

        Mockito.verify(transaction).commitAsync();
    }

    @Disabled
    @Test
    public void testMetadata() {
        ResultSet rs = igniteSql.createSession()
                .execute(null, "SELECT id, val FROM tbl");

        SqlRow row = rs.iterator().next();

        ResultSetMetadata meta = rs.metadata();

        assertEquals(rs.metadata().columns().size(), row.columnCount());

        assertEquals(0, meta.indexOf("id"));
        assertEquals(1, meta.indexOf("val"));

        assertEquals("id", meta.columns().get(0).name());
        assertEquals("val", meta.columns().get(1).name());

        assertEquals(Integer.class, meta.columns().get(0).valueClass());
        assertEquals(String.class, meta.columns().get(1).valueClass());

        assertFalse(meta.columns().get(0).nullable());
        assertTrue(meta.columns().get(1).nullable());
    }

    @NotNull
    private KeyValueView<Tuple, Tuple> getTable() {
        BatchedArguments of = BatchedArguments.of();
        BatchedArguments of2 = BatchedArguments.of((Object) null);

        SchemaDescriptor schema = new SchemaDescriptor(42,
                new Column[]{new Column("id", NativeTypes.INT32, false)},
                new Column[]{new Column("val", NativeTypes.STRING, true)}
        );

        SchemaRegistry schemaReg = Mockito.mock(SchemaRegistry.class);
        Mockito.when(schemaReg.schema()).thenReturn(schema);

        KeyValueView<Tuple, Tuple> tbl = (KeyValueView<Tuple, Tuple>) Mockito.mock(KeyValueView.class);
        Mockito.when(tbl.putAsync(Mockito.nullable(Transaction.class), Mockito.any(Tuple.class), Mockito.any(Tuple.class)))
                .then(ans -> {
                    state(ans.getArgument(0)).put(
                            ((Tuple) ans.getArgument(1)).intValue("id"),
                            ((Tuple) ans.getArgument(2)).stringValue("val"));
                    return CompletableFuture.completedFuture(null);
                });

        Mockito.when(tbl.getAsync(Mockito.any(Transaction.class), Mockito.any(Tuple.class)))
                .thenAnswer(ans -> CompletableFuture.completedFuture(
                        state(ans.getArgument(0))
                                .get(((Tuple) ans.getArgument(1)).intValue("id"))));

        return tbl;
    }

    private void initMock() {
        globalState = new HashMap<>();
        txState = new HashMap<>();

        Session session = Mockito.mock(Session.class);
        Statement statement = Mockito.mock(Statement.class);

        SessionBuilder sessionBuilder = Mockito.mock(SessionBuilder.class);

        Mockito.when(sessionBuilder.defaultSchema(Mockito.anyString())).thenAnswer(Answers.RETURNS_SELF);
        Mockito.when(sessionBuilder.defaultTimeout(Mockito.anyLong(), Mockito.any(TimeUnit.class))).thenAnswer(Answers.RETURNS_SELF);
        Mockito.when(sessionBuilder.property(Mockito.anyString(), Mockito.any())).thenAnswer(Answers.RETURNS_SELF);
        Mockito.when(sessionBuilder.build()).thenReturn(session);

        StatementBuilder stmtBuilder = Mockito.mock(StatementBuilder.class);

        Mockito.when(stmtBuilder.query(Mockito.anyString())).thenAnswer(Answers.RETURNS_SELF);
        Mockito.when(stmtBuilder.defaultSchema(Mockito.anyString())).thenAnswer(Answers.RETURNS_SELF);
        Mockito.when(stmtBuilder.pageSize(Mockito.anyInt())).thenAnswer(Answers.RETURNS_SELF);
        Mockito.when(stmtBuilder.queryTimeout(Mockito.anyLong(), Mockito.any(TimeUnit.class))).thenAnswer(Answers.RETURNS_SELF);
        Mockito.when(stmtBuilder.property(Mockito.anyString(), Mockito.any())).thenAnswer(Answers.RETURNS_SELF);
        Mockito.when(stmtBuilder.prepared()).thenAnswer(Answers.RETURNS_SELF);
        Mockito.when(stmtBuilder.build()).thenReturn(statement);

        Mockito.when(igniteSql.createSession()).thenReturn(session);
        Mockito.when(igniteSql.sessionBuilder()).thenReturn(sessionBuilder);
        Mockito.when(igniteSql.statementBuilder()).thenReturn(stmtBuilder);
        Mockito.when(igniteSql.createStatement(Mockito.anyString())).thenReturn(statement);

        transaction = Mockito.spy(new DummyTx());
        Mockito.doCallRealMethod().when(transaction).commitAsync();
        Mockito.doCallRealMethod().when(transaction).rollback();

        Mockito.when(igniteTx.beginAsync()).then(ans -> {
                    txState.clear();
                    txState.putAll(globalState);
                    return CompletableFuture.completedFuture(transaction);
                }
        );

        Mockito.doAnswer(invocation -> {
            Consumer<Transaction> argument = invocation.getArgument(0);

            argument.accept(transaction);

            return null;
        }).when(igniteTx).runInTransaction(Mockito.any(Consumer.class));

        Mockito.when(session.execute(Mockito.nullable(Transaction.class), Mockito.eq("INSERT INTO tbl VALUES (?, ?)"),
                Mockito.anyInt(),
                Mockito.anyString()))
                .thenAnswer(ans -> {
                    state(ans.getArgument(0)).put(ans.getArgument(2), ans.getArgument(3));

                    ResultSet res = Mockito.mock(ResultSet.class);
                    Mockito.when(res.iterator()).thenThrow(AssertionError.class);
                    Mockito.when(res.wasApplied()).thenReturn(false);
                    Mockito.when(res.hasRowSet()).thenReturn(false);
                    Mockito.when(res.affectedRows()).thenReturn(1);

                    return res;
                });

        Mockito.when(session.executeBatch(Mockito.nullable(Transaction.class), Mockito.eq("INSERT INTO tbl VALUES (?, ?)"),
                Mockito.any(BatchedArguments.class)))
                .then(ans -> {
                    BatchedArguments args = ans.getArgument(2);

                    args.forEach(a -> state(ans.getArgument(0)).put((Integer) a.get(0), (String) a.get(1)));

                    int[] res = new int[args.size()];
                    Arrays.fill(res, 1);
                    return res;
                });

        Mockito.when(session.execute(Mockito.nullable(Transaction.class), Mockito.eq("SELECT id, val FROM tbl WHERE id > ?"),
                Mockito.anyInt()))
                .thenAnswer(ans -> {
                    ResultSet res = Mockito.mock(ResultSet.class);
                    Mockito.when(res.wasApplied()).thenReturn(false);
                    Mockito.when(res.hasRowSet()).thenReturn(true);
                    Mockito.when(res.affectedRows()).thenReturn(-1);

                    Transaction txArg = ans.getArgument(0);
                    Integer filterArg = ans.getArgument(2);

                    Mockito.when(res.iterator()).thenReturn(stateAsRowSet(txArg, filterArg).iterator());
                    return res;
                });

        Mockito.when(session.execute(Mockito.nullable(Transaction.class), Mockito.eq("DELETE FROM tbl")))
                .thenAnswer(ans -> Mockito.when(Mockito.mock(ResultSet.class).affectedRows())
                        .then(ans0 -> {
                            Map<Integer, String> state = state(ans.getArgument(0));
                            HashMap<Integer, String> oldState = new HashMap<>(state);

                            oldState.forEach((k, v) -> state.put(k, null));

                            return oldState.size();
                        })
                        .getMock());

        Mockito.when(session.execute(Mockito.isNull(), Mockito.eq("SELECT id, val FROM tbl2")))
                .thenAnswer(ans -> {
                    ResultSet res = Mockito.mock(ResultSet.class);
                    Mockito.when(res.wasApplied()).thenReturn(false);
                    Mockito.when(res.hasRowSet()).thenReturn(true);
                    Mockito.when(res.affectedRows()).thenReturn(-1);
                    Mockito.when(res.iterator())
                            .thenReturn(List.of(
                                    createRow(2, "str2").build(),
                                    createRow(3, "str3").build()
                            ).iterator());
                    return res;
                });

        Mockito.when(session.execute(Mockito.isNull(), Mockito.eq("CREATE TABLE IF NOT EXITS tbl (id INT PRIMARY KEY, val VARCHAR)")))
                .thenAnswer(ans -> {
                    ResultSet res = Mockito.mock(ResultSet.class);
                    Mockito.when(res.iterator()).thenThrow(AssertionError.class);
                    Mockito.when(res.wasApplied()).thenReturn(true);
                    Mockito.when(res.hasRowSet()).thenReturn(false);
                    Mockito.when(res.affectedRows()).thenReturn(-1);
                    return res;
                });

        // Async API.
        Mockito.when(session.executeAsync(
                Mockito.nullable(Transaction.class),
                Mockito.eq("SELECT id, val FROM tbl WHERE id > ?"),
                Mockito.anyInt())
        ).thenAnswer(ans -> {
            AsyncResultSet page2 = Mockito.mock(AsyncResultSet.class);

            Transaction txArg = ans.getArgument(0);
            Integer filterArg = ans.getArgument(2);

            List<SqlRow> rows = stateAsRowSet(txArg, filterArg);

            assert rows.size() > 3 : "need more data";

            Mockito.when(page2.currentPage()).thenReturn(rows.subList(2, rows.size()));
            Mockito.when(page2.hasMorePages()).thenReturn(false);

            AsyncResultSet page1 = Mockito.mock(AsyncResultSet.class);
            Mockito.when(page1.currentPage()).thenReturn(rows.subList(0, 2));
            Mockito.when(page1.hasMorePages()).thenReturn(true);
            Mockito.when(page1.fetchNextPage())
                    .thenReturn((CompletionStage) CompletableFuture.completedFuture(page2));

            return CompletableFuture.completedFuture(page1);
        });

        // Reactive API.
        Mockito.when(session.executeReactive(
                Mockito.nullable(Transaction.class),
                Mockito.eq("SELECT id, val FROM tbl WHERE id > ?"),
                Mockito.anyInt())
        ).thenAnswer(invocation -> {
            ReactiveResultSet mock = Mockito.mock(ReactiveResultSet.class);

            Transaction txArg = invocation.getArgument(0);
            Integer filterArg = invocation.getArgument(2);

            Mockito.doAnswer(ans -> {
                Flow.Subscriber<SqlRow> subscrber = ans.getArgument(0);

                subscrber.onSubscribe(Mockito.mock(Flow.Subscription.class));

                List<SqlRow> rows = stateAsRowSet(txArg, filterArg);

                rows.forEach(subscrber::onNext);

                subscrber.onComplete();

                return ans;
            }).when(mock).subscribe(Mockito.any(Flow.Subscriber.class));

            return mock;
        });
    }

    private List<SqlRow> stateAsRowSet(Transaction tx, Integer filterArg) {
        return state(tx).entrySet().stream()
                .filter(e -> e.getKey() > filterArg)
                .map(e -> createRow(e.getKey(), e.getValue()).build())
                .collect(Collectors.toList());
    }

    private TestRow createRow(Integer key, String value) {
        return new TestRow().set("id", key)
                .set("val", value);
    }

    private Map<Integer, String> state(Transaction tx) {
        return tx == null ? globalState : txState;
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

    class DummyTx implements Transaction {
        @Override
        public void commit() throws TransactionException {
            txState.forEach((k, v) -> globalState.put(k, v));
            txState.clear();
        }

        @Override
        public CompletableFuture<Void> commitAsync() {
            commit();
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public void rollback() throws TransactionException {
            txState.clear();
        }

        @Override
        public CompletableFuture<Void> rollbackAsync() {
            rollback();
            return CompletableFuture.completedFuture(null);
        }
    }
}
