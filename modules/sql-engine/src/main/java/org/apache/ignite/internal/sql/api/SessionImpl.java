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

import it.unimi.dsi.fastutil.longs.LongArrayList;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Flow.Publisher;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.internal.sql.engine.AsyncCursor;
import org.apache.ignite.internal.sql.engine.AsyncSqlCursor;
import org.apache.ignite.internal.sql.engine.QueryContext;
import org.apache.ignite.internal.sql.engine.QueryProcessor;
import org.apache.ignite.internal.sql.engine.QueryTimeout;
import org.apache.ignite.internal.sql.engine.QueryValidator;
import org.apache.ignite.internal.sql.engine.prepare.QueryPlan.Type;
import org.apache.ignite.internal.util.ArrayUtils;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.sql.BatchedArguments;
import org.apache.ignite.sql.Session;
import org.apache.ignite.sql.SqlBatchException;
import org.apache.ignite.sql.SqlException;
import org.apache.ignite.sql.Statement;
import org.apache.ignite.sql.async.AsyncResultSet;
import org.apache.ignite.sql.reactive.ReactiveResultSet;
import org.apache.ignite.tx.Transaction;
import org.jetbrains.annotations.Nullable;

/**
 * Embedded implementation of the SQL session.
 */
public class SessionImpl implements Session {
    /** Busy lock for close synchronisation. */
    private final IgniteSpinBusyLock busyLock = new IgniteSpinBusyLock();

    private final QueryProcessor qryProc;

    private final long timeout;

    private final String schema;

    private final int pageSize;

    private final Set<CompletableFuture<?>> futsToClose = Collections.newSetFromMap(new ConcurrentHashMap<>());

    private final Set<AsyncSqlCursor<List<Object>>> cursToClose = Collections.newSetFromMap(new ConcurrentHashMap<>());

    private final Map<String, Object> props;

    /**
     * Constructor.
     *
     * @param qryProc Query processor.
     * @param schema  Query default schema.
     * @param timeout Query default timeout.
     * @param pageSize Query fetch page size.
     * @param props Session's properties.
     */
    SessionImpl(
            QueryProcessor qryProc,
            String schema,
            long timeout,
            int pageSize,
            Map<String, Object> props
    ) {
        this.qryProc = qryProc;
        this.schema = schema;
        this.timeout = timeout;
        this.pageSize = pageSize;
        this.props = props;
    }

    /** {@inheritDoc} */
    @Override
    public long[] executeBatch(@Nullable Transaction transaction, Statement dmlStatement, BatchedArguments batch) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /** {@inheritDoc} */
    @Override
    public void executeScript(String query, @Nullable Object... arguments) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /** {@inheritDoc} */
    @Override
    public long defaultTimeout(TimeUnit timeUnit) {
        return timeUnit.convert(timeout, TimeUnit.NANOSECONDS);
    }

    /** {@inheritDoc} */
    @Override
    public String defaultSchema() {
        return schema;
    }

    /** {@inheritDoc} */
    @Override
    public int defaultPageSize() {
        return pageSize;
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable Object property(String name) {
        return props.get(name);
    }

    /** {@inheritDoc} */
    @Override
    public SessionBuilder toBuilder() {
        if (!busyLock.enterBusy()) {
            throw new SqlException("Session is closed");
        }

        try {
            return new SessionBuilderImpl(qryProc, new HashMap<>(props))
                    .defaultPageSize(pageSize)
                    .defaultTimeout(timeout, TimeUnit.NANOSECONDS)
                    .defaultSchema(schema);
        } finally {
            busyLock.leaveBusy();
        }
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<AsyncResultSet> executeAsync(@Nullable Transaction transaction, String query, @Nullable Object... arguments) {
        if (!busyLock.enterBusy()) {
            return CompletableFuture.failedFuture(new SqlException("Session is closed."));
        }

        try {
            QueryContext ctx = QueryContext.of(transaction, new QueryTimeout(timeout, TimeUnit.NANOSECONDS));

            final CompletableFuture<AsyncSqlCursor<List<Object>>> f = qryProc.querySingleAsync(ctx, schema, query, arguments);

            futsToClose.add(f);

            return f.whenComplete(
                    (cur, ex0) -> futsToClose.remove(f))
                    .thenCompose(cur -> {
                        if (!busyLock.enterBusy()) {
                            return cur.closeAsync()
                                    .thenCompose((v) -> CompletableFuture.failedFuture(new SqlException("Session is closed")));
                        }

                        try {
                            cursToClose.add(cur);

                            return cur.requestNextAsync(pageSize)
                                    .<AsyncResultSet>thenApply(
                                            batchRes -> new AsyncResultSetImpl(
                                                    cur,
                                                    batchRes,
                                                    pageSize,
                                                    () -> cursToClose.remove(cur)
                                            )
                                    )
                                    .whenComplete((ars, ex1) -> {
                                        if (ex1 != null) {
                                            cursToClose.remove(cur);

                                            cur.closeAsync();
                                        }
                                    });
                        } catch (Throwable e) {
                            cursToClose.remove(cur);

                            return cur.closeAsync()
                                    .thenCompose((v) -> CompletableFuture.failedFuture(e));
                        } finally {
                            busyLock.leaveBusy();
                        }
                    }
            );
        } catch (Exception e) {
            return CompletableFuture.failedFuture(e);
        } finally {
            busyLock.leaveBusy();
        }
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<AsyncResultSet> executeAsync(
            @Nullable Transaction transaction,
            Statement statement,
            @Nullable Object... arguments
    ) {
        // TODO: IGNITE-16967 use all statement properties.
        return executeAsync(transaction, statement.query(), arguments);
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<long[]> executeBatchAsync(@Nullable Transaction transaction, String query, BatchedArguments batch) {
        if (!busyLock.enterBusy()) {
            return CompletableFuture.failedFuture(new SqlException("Session is closed."));
        }

        try {
            QueryContext ctx = QueryContext.of(
                    transaction,
                    new QueryTimeout(timeout, TimeUnit.NANOSECONDS),
                    (QueryValidator) plan -> {
                        if (plan.type() != Type.DML) {
                            throw new SqlException("Invalid SQL statement type in the batch [plan=" + plan + ']');
                        }
                    }
            );

            final var counters = new LongArrayList(batch.size());
            CompletableFuture<Void> tail = CompletableFuture.completedFuture(null);
            ArrayList<CompletableFuture<Void>> batchFuts = new ArrayList<>(batch.size());

            for (int i = 0; i < batch.size(); ++i) {
                Object[] args = batch.get(i).toArray();

                tail = tail.thenCompose(v -> qryProc.querySingleAsync(ctx, schema, query, args))
                        .thenCompose(cur -> cur.requestNextAsync(1))
                        .thenAccept(page -> {
                            if (page == null
                                    || page.items() == null
                                    || page.items().size() != 1
                                    || page.items().get(0).size() != 1
                                    || page.hasMore()) {
                                throw new SqlException("Invalid DML results: " + page);
                            }

                            counters.add((long) page.items().get(0).get(0));
                        })
                        .whenComplete((v, ex) -> {
                            if (ex instanceof CancellationException) {
                                // TODO
                            }
                        });

                batchFuts.add(tail);
            }

            CompletableFuture<long[]> resFut = tail
                    .exceptionally((ex) -> {
                        throw new SqlBatchException(counters.toArray(ArrayUtils.LONG_EMPTY_ARRAY), ex);
                    })
                    .thenApply(v -> counters.toArray(ArrayUtils.LONG_EMPTY_ARRAY));

            resFut.whenComplete((cur, ex) -> {
                if (ex instanceof CancellationException) {
                    batchFuts.forEach(f -> f.cancel(false));
                }
            });

            return resFut;
        } catch (Exception e) {
            return CompletableFuture.failedFuture(e);
        } finally {
            busyLock.leaveBusy();
        }
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<long[]> executeBatchAsync(@Nullable Transaction transaction, Statement statement, BatchedArguments batch) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Void> executeScriptAsync(String query, @Nullable Object... arguments) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /** {@inheritDoc} */
    @Override
    public ReactiveResultSet executeReactive(@Nullable Transaction transaction, String query, @Nullable Object... arguments) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /** {@inheritDoc} */
    @Override
    public ReactiveResultSet executeReactive(@Nullable Transaction transaction, Statement statement, @Nullable Object... arguments) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /** {@inheritDoc} */
    @Override
    public Publisher<Long> executeBatchReactive(@Nullable Transaction transaction, String query, BatchedArguments batch) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /** {@inheritDoc} */
    @Override
    public Publisher<Long> executeBatchReactive(@Nullable Transaction transaction, Statement statement, BatchedArguments batch) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /** {@inheritDoc} */
    @Override
    public void close() {
        await(closeAsync());
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Void> closeAsync() {
        return CompletableFuture
                .runAsync(busyLock::block)
                .thenCompose(
                        v0 -> {
                            futsToClose.forEach(f -> f.cancel(false));

                            return CompletableFuture.allOf(
                                            cursToClose.stream().map(AsyncCursor::closeAsync).toArray(CompletableFuture[]::new)
                                    )
                                    .whenComplete((v, e) -> cursToClose.clear());
                        }
                );
    }

    /** {@inheritDoc} */
    @Override
    public Publisher<Void> closeReactive() {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /**
     * Awaits completion of the given stage and returns its result.
     *
     * @param stage The stage.
     * @param <T> Type of the result returned by the stage.
     * @return A result of the stage.
     */
    @SuppressWarnings("UnusedReturnValue")
    public static <T> T await(CompletionStage<T> stage) {
        try {
            return stage.toCompletableFuture().get();
        } catch (ExecutionException e) {
            throw new IgniteException(e.getCause());
        } catch (Throwable e) {
            throw new IgniteException(e);
        }
    }
}
