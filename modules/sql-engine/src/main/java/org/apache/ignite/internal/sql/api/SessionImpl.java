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

import static org.apache.ignite.internal.lang.IgniteExceptionMapperUtil.mapToPublicException;
import static org.apache.ignite.internal.util.ExceptionUtils.sneakyThrow;
import static org.apache.ignite.lang.ErrorGroups.Common.INTERNAL_ERR;
import static org.apache.ignite.lang.ErrorGroups.Sql.SESSION_CLOSED_ERR;

import it.unimi.dsi.fastutil.longs.LongArrayList;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Flow.Publisher;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.internal.sql.AbstractSession;
import org.apache.ignite.internal.sql.engine.QueryContext;
import org.apache.ignite.internal.sql.engine.QueryProcessor;
import org.apache.ignite.internal.sql.engine.QueryProperty;
import org.apache.ignite.internal.sql.engine.SqlQueryType;
import org.apache.ignite.internal.sql.engine.property.PropertiesHolder;
import org.apache.ignite.internal.sql.engine.property.Property;
import org.apache.ignite.internal.sql.engine.session.SessionId;
import org.apache.ignite.internal.sql.engine.session.SessionNotFoundException;
import org.apache.ignite.internal.sql.engine.session.SessionProperty;
import org.apache.ignite.internal.util.ArrayUtils;
import org.apache.ignite.internal.util.AsyncCursor;
import org.apache.ignite.internal.util.ExceptionUtils;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.lang.TraceableException;
import org.apache.ignite.sql.BatchedArguments;
import org.apache.ignite.sql.SqlBatchException;
import org.apache.ignite.sql.SqlException;
import org.apache.ignite.sql.SqlRow;
import org.apache.ignite.sql.Statement;
import org.apache.ignite.sql.async.AsyncResultSet;
import org.apache.ignite.sql.reactive.ReactiveResultSet;
import org.apache.ignite.table.mapper.Mapper;
import org.apache.ignite.tx.IgniteTransactions;
import org.apache.ignite.tx.Transaction;
import org.jetbrains.annotations.Nullable;

/**
 * Embedded implementation of the SQL session.
 */
public class SessionImpl implements AbstractSession {
    /** Busy lock for close synchronisation. */
    private final IgniteSpinBusyLock busyLock = new IgniteSpinBusyLock();

    private final AtomicBoolean closed = new AtomicBoolean(false);

    private final QueryProcessor qryProc;

    private final IgniteTransactions transactions;

    private final SessionId sessionId;

    private final int pageSize;

    private final PropertiesHolder props;

    /**
     * Constructor.
     *
     * @param qryProc Query processor.
     * @param transactions Transactions facade.
     * @param pageSize Query fetch page size.
     * @param props Session's properties.
     */
    SessionImpl(
            SessionId sessionId,
            QueryProcessor qryProc,
            IgniteTransactions transactions,
            int pageSize,
            PropertiesHolder props
    ) {
        this.qryProc = qryProc;
        this.transactions = transactions;
        this.sessionId = sessionId;
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
    public long defaultQueryTimeout(TimeUnit timeUnit) {
        return timeUnit.convert(props.get(QueryProperty.QUERY_TIMEOUT), TimeUnit.MILLISECONDS);
    }

    /** {@inheritDoc} */
    @Override
    public long idleTimeout(TimeUnit timeUnit) {
        return timeUnit.convert(props.get(SessionProperty.IDLE_TIMEOUT), TimeUnit.MILLISECONDS);
    }

    /** {@inheritDoc} */
    @Override
    public String defaultSchema() {
        return props.get(QueryProperty.DEFAULT_SCHEMA);
    }

    /** {@inheritDoc} */
    @Override
    public int defaultPageSize() {
        return pageSize;
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable Object property(String name) {
        var prop = QueryProperty.byName(name);

        if (prop == null) {
            return null;
        }

        return props.get(prop);
    }

    /** {@inheritDoc} */
    @Override
    public SessionBuilder toBuilder() {
        Map<String, Object> propertyMap = new HashMap<>();

        for (Map.Entry<Property<?>, Object> entry : props) {
            propertyMap.put(entry.getKey().name, entry.getValue());
        }

        return new SessionBuilderImpl(qryProc, transactions, propertyMap)
                .defaultPageSize(pageSize);
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<AsyncResultSet<SqlRow>> executeAsync(
            @Nullable Transaction transaction,
            String query,
            @Nullable Object... arguments) {
        if (!busyLock.enterBusy()) {
            return CompletableFuture.failedFuture(new SqlException(SESSION_CLOSED_ERR, "Session is closed."));
        }

        CompletableFuture<AsyncResultSet<SqlRow>> result;

        try {
            QueryContext ctx = QueryContext.create(SqlQueryType.SINGLE_STMT_TYPES, transaction);

            result = qryProc.querySingleAsync(sessionId, ctx, transactions, query, arguments)
                    .thenCompose(cur -> cur.requestNextAsync(pageSize)
                            .thenApply(
                                    batchRes -> new AsyncResultSetImpl<>(
                                            cur,
                                            batchRes,
                                            pageSize,
                                            () -> {}
                                    )
                            )
            );
        } catch (Exception e) {
            return CompletableFuture.failedFuture(mapToPublicException(e));
        } finally {
            busyLock.leaveBusy();
        }

        // Closing a session must be done outside of the lock.
        return result.exceptionally((th) -> {
            Throwable cause = ExceptionUtils.unwrapCause(th);

            if (cause instanceof SessionNotFoundException) {
                closeInternal();
            }

            throw new CompletionException(mapToPublicException(cause));
        });
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<AsyncResultSet<SqlRow>> executeAsync(
            @Nullable Transaction transaction,
            Statement statement,
            @Nullable Object... arguments
    ) {
        // TODO: IGNITE-17440 use all statement properties.
        return executeAsync(transaction, statement.query(), arguments);
    }

    /** {@inheritDoc} */
    @Override
    public <T> CompletableFuture<AsyncResultSet<T>> executeAsync(@Nullable Transaction transaction, @Nullable Mapper<T> mapper,
            String query, @Nullable Object... arguments) {
        // TODO: IGNITE-18695.
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /** {@inheritDoc} */
    @Override
    public <T> CompletableFuture<AsyncResultSet<T>> executeAsync(
            @Nullable Transaction transaction,
            @Nullable Mapper<T> mapper,
            Statement statement,
            @Nullable Object... arguments) {
        // TODO: IGNITE-18695.
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<long[]> executeBatchAsync(@Nullable Transaction transaction, String query, BatchedArguments batch) {
        if (!busyLock.enterBusy()) {
            return CompletableFuture.failedFuture(new SqlException(SESSION_CLOSED_ERR, "Session is closed."));
        }

        try {
            QueryContext ctx = QueryContext.create(Set.of(SqlQueryType.DML), transaction);

            var counters = new LongArrayList(batch.size());
            CompletableFuture<Void> tail = CompletableFuture.completedFuture(null);
            ArrayList<CompletableFuture<Void>> batchFuts = new ArrayList<>(batch.size());

            for (int i = 0; i < batch.size(); ++i) {
                Object[] args = batch.get(i).toArray();

                final var qryFut = tail
                        .thenCompose(v -> qryProc.querySingleAsync(sessionId, ctx, transactions, query, args));

                tail = qryFut.thenCompose(cur -> cur.requestNextAsync(1))
                        .thenAccept(page -> {
                            validateDmlResult(page);

                            counters.add((long) page.items().get(0).get(0));
                        })
                        .whenComplete((v, ex) -> {
                            if (ExceptionUtils.unwrapCause(ex) instanceof CancellationException) {
                                qryFut.cancel(false);
                            }
                        });

                batchFuts.add(tail);
            }

            CompletableFuture<long[]> resFut = tail
                    .exceptionally((ex) -> {
                        Throwable cause = ExceptionUtils.unwrapCause(ex);

                        if (cause instanceof CancellationException) {
                            throw (CancellationException) cause;
                        }

                        Throwable t = mapToPublicException(cause);

                        if (t instanceof TraceableException) {
                            throw new SqlBatchException(
                                    ((TraceableException) t).traceId(),
                                    ((TraceableException) t).code(),
                                    counters.toArray(ArrayUtils.LONG_EMPTY_ARRAY),
                                    t);
                        }

                        // JVM error.
                        throw new CompletionException(cause);
                    })
                    .thenApply(v -> counters.toArray(ArrayUtils.LONG_EMPTY_ARRAY));

            resFut.whenComplete((cur, ex) -> {
                if (ExceptionUtils.unwrapCause(ex) instanceof CancellationException) {
                    batchFuts.forEach(f -> f.cancel(false));
                }
            });

            return resFut;
        } catch (Exception e) {
            return CompletableFuture.failedFuture(mapToPublicException(e));
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
        try {
            closeAsync().get();
        } catch (ExecutionException e) {
            sneakyThrow(ExceptionUtils.copyExceptionWithCause(e));
        } catch (InterruptedException e) {
            throw new SqlException(SESSION_CLOSED_ERR, e);
        }
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Void> closeAsync() {
        try {
            closeInternal();

            return qryProc.closeSession(sessionId);
        } catch (Exception e) {
            return CompletableFuture.failedFuture(mapToPublicException(e));
        }
    }

    /** {@inheritDoc} */
    @Override
    public Publisher<Void> closeReactive() {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    private void closeInternal() {
        if (closed.compareAndSet(false, true)) {
            busyLock.block();
        }
    }

    private static void validateDmlResult(AsyncCursor.BatchedResult<List<Object>> page) {
        if (page == null
                || page.items() == null
                || page.items().size() != 1
                || page.items().get(0).size() != 1
                || page.hasMore()) {
            throw new IgniteInternalException(INTERNAL_ERR, "Invalid DML results: " + page);
        }
    }
}
