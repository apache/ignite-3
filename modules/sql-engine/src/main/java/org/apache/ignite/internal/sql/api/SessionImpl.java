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

import static org.apache.ignite.internal.lang.SqlExceptionMapperUtil.mapToPublicSqlException;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.apache.ignite.internal.util.ExceptionUtils.sneakyThrow;
import static org.apache.ignite.lang.ErrorGroups.Common.INTERNAL_ERR;
import static org.apache.ignite.lang.ErrorGroups.Sql.SESSION_CLOSED_ERR;

import it.unimi.dsi.fastutil.longs.LongArrayList;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Flow.Publisher;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.internal.sql.AbstractSession;
import org.apache.ignite.internal.sql.engine.AsyncSqlCursor;
import org.apache.ignite.internal.sql.engine.CurrentTimeProvider;
import org.apache.ignite.internal.sql.engine.InternalSqlRow;
import org.apache.ignite.internal.sql.engine.QueryProcessor;
import org.apache.ignite.internal.sql.engine.QueryProperty;
import org.apache.ignite.internal.sql.engine.SqlQueryType;
import org.apache.ignite.internal.sql.engine.property.Property;
import org.apache.ignite.internal.sql.engine.property.SqlProperties;
import org.apache.ignite.internal.sql.engine.property.SqlPropertiesHelper;
import org.apache.ignite.internal.tx.InternalTransaction;
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
import org.jetbrains.annotations.TestOnly;

/**
 * Embedded implementation of the SQL session.
 */
public class SessionImpl implements AbstractSession {
    /** Busy lock for close synchronisation. */
    private final IgniteSpinBusyLock busyLock = new IgniteSpinBusyLock();

    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final CompletableFuture<Void> closedFut = new CompletableFuture<>();

    private final AtomicInteger cursorIdGen = new AtomicInteger();

    private final ConcurrentMap<Integer, AsyncSqlCursor<?>> openedCursors = new ConcurrentHashMap<>();

    private final SessionBuilderFactory sessionBuilderFactory;

    private final QueryProcessor qryProc;

    private final IgniteTransactions transactions;

    private final SessionId sessionId;

    private final int pageSize;

    private final SqlProperties props;

    private final long idleTimeoutMs;

    private final IdleExpirationTracker expirationTracker;

    private final Runnable onClose;

    /**
     * Constructor.
     *
     * @param sessionId Identifier of the session.
     * @param sessionBuilderFactory Map of created sessions.
     * @param qryProc Query processor.
     * @param transactions Transactions facade.
     * @param pageSize Query fetch page size.
     * @param idleTimeoutMs Duration in milliseconds after which the session will be considered expired if no action have been
     *                      performed on behalf of this session during this period.
     * @param props Session's properties.
     * @param timeProvider The time provider used to update the timestamp on every touch of this object.
     */
    SessionImpl(
            SessionId sessionId,
            SessionBuilderFactory sessionBuilderFactory,
            QueryProcessor qryProc,
            IgniteTransactions transactions,
            int pageSize,
            long idleTimeoutMs,
            SqlProperties props,
            CurrentTimeProvider timeProvider,
            Runnable onClose
    ) {
        this.qryProc = qryProc;
        this.transactions = transactions;
        this.sessionId = sessionId;
        this.pageSize = pageSize;
        this.props = props;
        this.idleTimeoutMs = idleTimeoutMs;
        this.sessionBuilderFactory = sessionBuilderFactory;
        this.onClose = onClose;

        expirationTracker = new IdleExpirationTracker(
                idleTimeoutMs,
                timeProvider
        );
    }

    /** {@inheritDoc} */
    @Override
    public long[] executeBatch(@Nullable Transaction transaction, Statement dmlStatement, BatchedArguments batch) {
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
        return timeUnit.convert(idleTimeoutMs, TimeUnit.MILLISECONDS);
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

        return sessionBuilderFactory.fromProperties(propertyMap)
                .defaultPageSize(pageSize);
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<AsyncResultSet<SqlRow>> executeAsync(
            @Nullable Transaction transaction,
            String query,
            @Nullable Object... arguments
    ) {
        touchAndCloseIfExpired();

        if (!busyLock.enterBusy()) {
            return CompletableFuture.failedFuture(sessionIsClosedException());
        }

        CompletableFuture<AsyncResultSet<SqlRow>> result;

        try {
            SqlProperties properties = SqlPropertiesHelper.newBuilder()
                    .set(QueryProperty.ALLOWED_QUERY_TYPES, SqlQueryType.SINGLE_STMT_TYPES)
                    .build();

            result = qryProc.querySingleAsync(properties, transactions, (InternalTransaction) transaction, query, arguments)
                    .thenCompose(cur -> {
                        if (!busyLock.enterBusy()) {
                            cur.closeAsync();

                            return CompletableFuture.failedFuture(sessionIsClosedException());
                        }

                        try {
                            int cursorId = registerCursor(cur);

                            cur.onClose().whenComplete((r, e) -> openedCursors.remove(cursorId));

                            return cur.requestNextAsync(pageSize)
                                    .thenApply(
                                            batchRes -> new AsyncResultSetImpl<>(
                                                    cur,
                                                    batchRes,
                                                    pageSize,
                                                    expirationTracker
                                            )
                                    );
                        } finally {
                            busyLock.leaveBusy();
                        }
                    }
            );
        } catch (Exception e) {
            return CompletableFuture.failedFuture(mapToPublicSqlException(e));
        } finally {
            busyLock.leaveBusy();
        }

        // Closing a session must be done outside of the lock.
        return result.exceptionally((th) -> {
            Throwable cause = ExceptionUtils.unwrapCause(th);

            throw new CompletionException(mapToPublicSqlException(cause));
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
        touchAndCloseIfExpired();

        if (!busyLock.enterBusy()) {
            return CompletableFuture.failedFuture(sessionIsClosedException());
        }

        try {
            SqlProperties properties = SqlPropertiesHelper.newBuilder()
                    .set(QueryProperty.ALLOWED_QUERY_TYPES, EnumSet.of(SqlQueryType.DML))
                    .build();

            var counters = new LongArrayList(batch.size());
            CompletableFuture<?> tail = nullCompletedFuture();
            ArrayList<CompletableFuture<?>> batchFuts = new ArrayList<>(batch.size());

            for (int i = 0; i < batch.size(); ++i) {
                Object[] args = batch.get(i).toArray();

                tail = tail.thenCompose(v -> {
                    if (!busyLock.enterBusy()) {
                        return CompletableFuture.failedFuture(sessionIsClosedException());
                    }

                    try {
                        return qryProc.querySingleAsync(properties, transactions, (InternalTransaction) transaction, query, args)
                                .thenCompose(cursor -> {
                                    if (!busyLock.enterBusy()) {
                                        cursor.closeAsync();

                                        return CompletableFuture.failedFuture(sessionIsClosedException());
                                    }

                                    try {
                                        int cursorId = registerCursor(cursor);

                                        return cursor.requestNextAsync(1)
                                                .handle((page, th) -> {
                                                    openedCursors.remove(cursorId);
                                                    cursor.closeAsync();

                                                    if (th != null) {
                                                        return CompletableFuture.failedFuture(th);
                                                    }

                                                    validateDmlResult(page);

                                                    counters.add((long) page.items().get(0).get(0));

                                                    return nullCompletedFuture();
                                                }).thenCompose(Function.identity());
                                    } finally {
                                        busyLock.leaveBusy();
                                    }
                                });
                    } finally {
                        busyLock.leaveBusy();
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

                        Throwable t = mapToPublicSqlException(cause);

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
            return CompletableFuture.failedFuture(mapToPublicSqlException(e));
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
        touchAndCloseIfExpired();

        if (!busyLock.enterBusy()) {
            return CompletableFuture.failedFuture(sessionIsClosedException());
        }

        CompletableFuture<Void> resFut = new CompletableFuture<>();
        try {
            SqlProperties properties = SqlPropertiesHelper.emptyProperties();

            CompletableFuture<AsyncSqlCursor<InternalSqlRow>> f =
                    qryProc.queryScriptAsync(properties, transactions, null, query, arguments);

            ScriptHandler handler = new ScriptHandler(resFut);
            f.whenComplete(handler::processCursor);
        } finally {
            busyLock.leaveBusy();
        }

        return resFut.exceptionally((th) -> {
            Throwable cause = ExceptionUtils.unwrapCause(th);

            throw new CompletionException(mapToPublicSqlException(cause));
        });
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
            return closeInternal()
                    .exceptionally(e -> {
                        sneakyThrow(mapToPublicSqlException(e));

                        return null;
                    });
        } catch (Exception e) {
            return CompletableFuture.failedFuture(mapToPublicSqlException(e));
        }
    }

    /** {@inheritDoc} */
    @Override
    public Publisher<Void> closeReactive() {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /** {@inheritDoc} */
    @Override
    public boolean closed() {
        return closed.get();
    }

    boolean expired() {
        return expirationTracker.expired();
    }

    SessionId id() {
        return sessionId;
    }

    @SuppressWarnings("rawtypes")
    private CompletableFuture<Void> closeInternal() {
        if (closed.compareAndSet(false, true)) {
            busyLock.block();

            List<AsyncSqlCursor<?>> cursorsToClose = new ArrayList<>(openedCursors.values());

            openedCursors.clear();

            CompletableFuture[] closeCursorFutures = new CompletableFuture[cursorsToClose.size()];

            int idx = 0;
            for (AsyncSqlCursor<?> cursor : cursorsToClose) {
                closeCursorFutures[idx++] = cursor.closeAsync();
            }

            CompletableFuture.allOf(closeCursorFutures)
                    .whenComplete((r, e) -> {
                        Throwable error = null;

                        if (e != null) {
                            for (CompletableFuture<?> fut : closeCursorFutures) {
                                if (!fut.isCompletedExceptionally()) {
                                    continue;
                                }

                                try {
                                    fut.getNow(null);
                                } catch (Throwable th) {
                                    Throwable unwrapped = ExceptionUtils.unwrapCause(th);

                                    if (error == null) {
                                        error = unwrapped;
                                    } else {
                                        error.addSuppressed(unwrapped);
                                    }
                                }
                            }
                        }

                        try {
                            onClose.run();
                        } catch (Throwable th) {
                            if (error == null) {
                                error = th;
                            } else {
                                error.addSuppressed(th);
                            }
                        }

                        if (error != null) {
                            closedFut.completeExceptionally(error);
                        } else {
                            closedFut.complete(null);
                        }
                    });
        }

        return closedFut;
    }

    private static void validateDmlResult(AsyncCursor.BatchedResult<InternalSqlRow> page) {
        if (page == null
                || page.items() == null
                || page.items().size() != 1
                || page.items().get(0).fieldCount() != 1
                || page.hasMore()) {
            throw new IgniteInternalException(INTERNAL_ERR, "Invalid DML results: " + page);
        }
    }

    private void touchAndCloseIfExpired() {
        if (!expirationTracker.touch()) {
            closeAsync();
        }
    }

    private int registerCursor(AsyncSqlCursor<?> cursor) {
        int cursorId = cursorIdGen.incrementAndGet();

        Object old = openedCursors.put(cursorId, cursor);

        assert old == null;

        return cursorId;
    }

    private static SqlException sessionIsClosedException() {
        return new SqlException(SESSION_CLOSED_ERR, "Session is closed.");
    }

    @FunctionalInterface
    interface SessionBuilderFactory {
        SessionBuilder fromProperties(Map<String, Object> properties);
    }

    @TestOnly
    List<AsyncSqlCursor<?>> openedCursors() {
        return List.copyOf(openedCursors.values());
    }

    private class ScriptHandler {
        private final CompletableFuture<Void> resFut;
        private final List<Throwable> cursorCloseErrors = Collections.synchronizedList(new ArrayList<>());

        ScriptHandler(CompletableFuture<Void> resFut) {
            this.resFut = resFut;
        }

        void processCursor(AsyncSqlCursor<InternalSqlRow> cursor, Throwable scriptError) {
            if (scriptError != null) {
                // Stopping script execution.
                onFail(scriptError);

                return;
            }

            cursor.closeAsync().whenComplete((ignored, cursorCloseError) -> {
                if (cursorCloseError != null) {
                    // Just save the error for later and continue fetching cursors.
                    cursorCloseErrors.add(cursorCloseError);
                }

                if (!busyLock.enterBusy()) {
                    onFail(sessionIsClosedException());
                    return;
                }

                try {
                    if (cursor.hasNextResult()) {
                        cursor.nextResult().whenCompleteAsync(this::processCursor);
                        return;
                    }
                } finally {
                    busyLock.leaveBusy();
                }

                onComplete();
            });
        }

        private void onComplete() {
            if (!cursorCloseErrors.isEmpty()) {
                onFail(new IllegalStateException("The script was completed with errors."));

                return;
            }

            resFut.complete(null);
        }

        private void onFail(Throwable err) {
            for (Throwable cursorCloseErr : cursorCloseErrors) {
                err.addSuppressed(cursorCloseErr);
            }

            resFut.completeExceptionally(err);
        }
    }
}
