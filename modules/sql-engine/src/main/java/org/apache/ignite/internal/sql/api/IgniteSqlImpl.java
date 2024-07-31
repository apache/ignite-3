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
import static org.apache.ignite.lang.ErrorGroups.Common.INTERNAL_ERR;
import static org.apache.ignite.lang.ErrorGroups.Common.NODE_STOPPING_ERR;

import it.unimi.dsi.fastutil.longs.LongArrayList;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.manager.ComponentContext;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.sql.StatementBuilderImpl;
import org.apache.ignite.internal.sql.StatementImpl;
import org.apache.ignite.internal.sql.SyncResultSetAdapter;
import org.apache.ignite.internal.sql.engine.AsyncSqlCursor;
import org.apache.ignite.internal.sql.engine.InternalSqlRow;
import org.apache.ignite.internal.sql.engine.QueryProcessor;
import org.apache.ignite.internal.sql.engine.QueryProperty;
import org.apache.ignite.internal.sql.engine.SqlQueryProcessor;
import org.apache.ignite.internal.sql.engine.SqlQueryType;
import org.apache.ignite.internal.sql.engine.property.SqlProperties;
import org.apache.ignite.internal.sql.engine.property.SqlPropertiesHelper;
import org.apache.ignite.internal.tx.HybridTimestampTracker;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.internal.util.ArrayUtils;
import org.apache.ignite.internal.util.AsyncCursor;
import org.apache.ignite.internal.util.ExceptionUtils;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.lang.TraceableException;
import org.apache.ignite.sql.BatchedArguments;
import org.apache.ignite.sql.IgniteSql;
import org.apache.ignite.sql.ResultSet;
import org.apache.ignite.sql.SqlBatchException;
import org.apache.ignite.sql.SqlException;
import org.apache.ignite.sql.SqlRow;
import org.apache.ignite.sql.Statement;
import org.apache.ignite.sql.Statement.StatementBuilder;
import org.apache.ignite.sql.async.AsyncResultSet;
import org.apache.ignite.table.mapper.Mapper;
import org.apache.ignite.tx.Transaction;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

/**
 * Embedded implementation of the Ignite SQL query facade.
 */
@SuppressWarnings("rawtypes")
public class IgniteSqlImpl implements IgniteSql, IgniteComponent {
    private static final IgniteLogger LOG = Loggers.forClass(IgniteSqlImpl.class);

    private static final int AWAIT_CURSOR_CLOSE_ON_STOP_IN_SECONDS = 10;

    private final IgniteSpinBusyLock busyLock = new IgniteSpinBusyLock();

    private final AtomicBoolean closed = new AtomicBoolean(false);

    private final AtomicInteger cursorIdGen = new AtomicInteger();

    private final ConcurrentMap<Integer, AsyncSqlCursor<?>> openedCursors = new ConcurrentHashMap<>();

    private final QueryProcessor queryProcessor;

    private final HybridTimestampTracker observableTimestampTracker;

    /**
     * Constructor.
     *
     * @param queryProcessor Query processor.
     * @param observableTimestampTracker Tracker of the latest time observed by client.
     */
    public IgniteSqlImpl(
            QueryProcessor queryProcessor,
            HybridTimestampTracker observableTimestampTracker
    ) {
        this.queryProcessor = queryProcessor;
        this.observableTimestampTracker = observableTimestampTracker;
    }

    /** {@inheritDoc} */
    @Override
    public Statement createStatement(String query) {
        return new StatementImpl(query);
    }

    /** {@inheritDoc} */
    @Override
    public StatementBuilder statementBuilder() {
        return new StatementBuilderImpl();
    }

    @Override
    public CompletableFuture<Void> startAsync(ComponentContext componentContext) {
        return nullCompletedFuture();
    }

    @Override
    public CompletableFuture<Void> stopAsync(ComponentContext componentContext) {
        if (!closed.compareAndSet(false, true)) {
            return nullCompletedFuture();
        }

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
                    if (e == null) {
                        return;
                    }

                    Throwable error = gatherExceptions(closeCursorFutures);

                    assert error != null;

                    LOG.warn("Some cursors were closed abruptly", mapToPublicSqlException(error));
                })
                .orTimeout(AWAIT_CURSOR_CLOSE_ON_STOP_IN_SECONDS, TimeUnit.SECONDS)
                .handle((ignored, error) -> {
                    if (error instanceof TimeoutException) {
                        LOG.warn("Cursors weren't be closed in {} seconds.", AWAIT_CURSOR_CLOSE_ON_STOP_IN_SECONDS);
                    }

                    return null;
                })
                // this future has timeout of AWAIT_CURSOR_CLOSE_ON_STOP_IN_SECONDS,
                // so we won't be waiting forever on this join() call
                .join();

        return nullCompletedFuture();
    }

    private static @Nullable Throwable gatherExceptions(CompletableFuture<?>... futures) {
        Throwable error = null;

        for (CompletableFuture<?> fut : futures) {
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

        return error;
    }

    /** {@inheritDoc} */
    @Override
    public ResultSet<SqlRow> execute(@Nullable Transaction transaction, String query, @Nullable Object... arguments) {
        Objects.requireNonNull(query);

        try {
            return new SyncResultSetAdapter<>(executeAsync(transaction, query, arguments).join());
        } catch (CompletionException e) {
            throw ExceptionUtils.sneakyThrow(ExceptionUtils.copyExceptionWithCause(e));
        }
    }

    /** {@inheritDoc} */
    @Override
    public ResultSet<SqlRow> execute(@Nullable Transaction transaction, Statement statement, @Nullable Object... arguments) {
        Objects.requireNonNull(statement);

        try {
            return new SyncResultSetAdapter<>(executeAsync(transaction, statement, arguments).join());
        } catch (CompletionException e) {
            throw ExceptionUtils.sneakyThrow(ExceptionUtils.copyExceptionWithCause(e));
        }
    }

    /** {@inheritDoc} */
    @Override
    public  <T> ResultSet<T> execute(
            @Nullable Transaction transaction,
            @Nullable Mapper<T> mapper,
            String query,
            @Nullable Object... arguments) {
        Objects.requireNonNull(query);

        try {
            return new SyncResultSetAdapter<>(executeAsync(transaction, mapper, query, arguments).join());
        } catch (CompletionException e) {
            throw ExceptionUtils.sneakyThrow(ExceptionUtils.copyExceptionWithCause(e));
        }
    }

    /** {@inheritDoc} */
    @Override
    public  <T> ResultSet<T> execute(
            @Nullable Transaction transaction,
            @Nullable Mapper<T> mapper,
            Statement statement,
            @Nullable Object... arguments) {
        Objects.requireNonNull(statement);

        try {
            return new SyncResultSetAdapter<>(executeAsync(transaction, mapper, statement, arguments).join());
        } catch (CompletionException e) {
            throw ExceptionUtils.sneakyThrow(ExceptionUtils.copyExceptionWithCause(e));
        }
    }

    /** {@inheritDoc} */
    @Override
    public long[] executeBatch(@Nullable Transaction transaction, String dmlQuery, BatchedArguments batch) {
        try {
            return executeBatchAsync(transaction, dmlQuery, batch).join();
        } catch (CompletionException e) {
            throw ExceptionUtils.sneakyThrow(ExceptionUtils.copyExceptionWithCause(e));
        }
    }

    /** {@inheritDoc} */
    @Override
    public long[] executeBatch(@Nullable Transaction transaction, Statement dmlStatement, BatchedArguments batch) {
        try {
            return executeBatchAsync(transaction, dmlStatement, batch).join();
        } catch (CompletionException e) {
            throw ExceptionUtils.sneakyThrow(ExceptionUtils.copyExceptionWithCause(e));
        }
    }

    /** {@inheritDoc} */
    @Override
    public void executeScript(String query, @Nullable Object... arguments) {
        Objects.requireNonNull(query);

        try {
            executeScriptAsync(query, arguments).join();
        } catch (CompletionException e) {
            throw ExceptionUtils.sneakyThrow(ExceptionUtils.copyExceptionWithCause(e));
        }
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<AsyncResultSet<SqlRow>> executeAsync(
            @Nullable Transaction transaction,
            String query,
            @Nullable Object... arguments
    ) {
        return executeAsyncInternal(transaction, createStatement(query), arguments);
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<AsyncResultSet<SqlRow>> executeAsync(
            @Nullable Transaction transaction,
            Statement statement,
            @Nullable Object... arguments
    ) {
        return executeAsyncInternal(transaction, statement, arguments);
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

    private CompletableFuture<AsyncResultSet<SqlRow>> executeAsyncInternal(
            @Nullable Transaction transaction,
            Statement statement,
            @Nullable Object... arguments
    ) {
        assert statement.pageSize() > 0 : statement.pageSize();

        int pageSize = statement.pageSize();

        if (!busyLock.enterBusy()) {
            return CompletableFuture.failedFuture(nodeIsStoppingException());
        }

        CompletableFuture<AsyncResultSet<SqlRow>> result;

        try {
            SqlProperties properties = toPropertiesBuilder(statement)
                    .set(QueryProperty.ALLOWED_QUERY_TYPES, SqlQueryType.SINGLE_STMT_TYPES)
                    .build();

            result = queryProcessor.queryAsync(
                    properties, observableTimestampTracker, (InternalTransaction) transaction, statement.query(), arguments
            ).thenCompose(cur -> {
                if (!busyLock.enterBusy()) {
                    cur.closeAsync();

                    return CompletableFuture.failedFuture(nodeIsStoppingException());
                }

                try {
                    int cursorId = registerCursor(cur);

                    cur.onClose().whenComplete((r, e) -> openedCursors.remove(cursorId));

                    return cur.requestNextAsync(pageSize)
                            .thenApply(batchRes -> new AsyncResultSetImpl<>(cur, batchRes, pageSize));
                } finally {
                    busyLock.leaveBusy();
                }
            });
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
    public CompletableFuture<long[]> executeBatchAsync(@Nullable Transaction transaction, String query, BatchedArguments batch) {
        return executeBatchAsync(transaction, createStatement(query), batch);
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<long[]> executeBatchAsync(@Nullable Transaction transaction, Statement statement, BatchedArguments batch) {
        if (!busyLock.enterBusy()) {
            return CompletableFuture.failedFuture(nodeIsStoppingException());
        }

        try {
            SqlProperties properties = toPropertiesBuilder(statement).build();

            return executeBatchCore(
                    queryProcessor,
                    observableTimestampTracker,
                    (InternalTransaction) transaction,
                    statement.query(),
                    batch,
                    properties,
                    busyLock::enterBusy,
                    busyLock::leaveBusy,
                    this::registerCursor,
                    openedCursors::remove);
        } catch (Exception e) {
            return CompletableFuture.failedFuture(mapToPublicSqlException(e));
        } finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * Execute batch of DML statements.
     *
     * @param queryProcessor Query processor.
     * @param observableTimestampTracker Tracker of the latest time observed by client.
     * @param transaction Transaction.
     * @param query Query.
     * @param batch Batch of arguments.
     * @param properties Properties.
     * @param enterBusy Enter busy lock action.
     * @param leaveBusy Leave busy lock action.
     * @param registerCursor Register cursor action.
     * @param removeCursor Remove cursor action.
     * @return Operation Future completed with the number of rows affected by each query in the batch
     *         (if the batch succeeds), future completed with the {@link SqlBatchException} (if the batch fails).
     */
    public static CompletableFuture<long[]> executeBatchCore(
            QueryProcessor queryProcessor,
            HybridTimestampTracker observableTimestampTracker,
            @Nullable InternalTransaction transaction,
            String query,
            BatchedArguments batch,
            SqlProperties properties,
            Supplier<Boolean> enterBusy,
            Runnable leaveBusy,
            Function<AsyncSqlCursor<?>, Integer> registerCursor,
            Consumer<Integer> removeCursor) {

        SqlProperties properties0 = SqlPropertiesHelper.chain(properties, SqlPropertiesHelper.newBuilder()
                .set(QueryProperty.ALLOWED_QUERY_TYPES, EnumSet.of(SqlQueryType.DML))
                .build());

        var counters = new LongArrayList(batch.size());
        CompletableFuture<?> tail = nullCompletedFuture();
        ArrayList<CompletableFuture<?>> batchFuts = new ArrayList<>(batch.size());

        for (int i = 0; i < batch.size(); ++i) {
            Object[] args = batch.get(i).toArray();

            tail = tail.thenCompose(v -> {
                if (!enterBusy.get()) {
                    return CompletableFuture.failedFuture(nodeIsStoppingException());
                }

                try {
                    return queryProcessor.queryAsync(properties0, observableTimestampTracker, transaction, query, args)
                            .thenCompose(cursor -> {
                                if (!enterBusy.get()) {
                                    cursor.closeAsync();

                                    return CompletableFuture.failedFuture(nodeIsStoppingException());
                                }

                                try {
                                    int cursorId = registerCursor.apply(cursor);

                                    return cursor.requestNextAsync(1)
                                            .handle((page, th) -> {
                                                removeCursor.accept(cursorId);
                                                cursor.closeAsync();

                                                if (th != null) {
                                                    return CompletableFuture.failedFuture(th);
                                                }

                                                validateDmlResult(page);

                                                counters.add((long) page.items().get(0).get(0));

                                                return nullCompletedFuture();
                                            }).thenCompose(Function.identity());
                                } finally {
                                    leaveBusy.run();
                                }
                            });
                } finally {
                    leaveBusy.run();
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
                                t.getMessage(),
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
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Void> executeScriptAsync(String query, @Nullable Object... arguments) {
        if (!busyLock.enterBusy()) {
            return CompletableFuture.failedFuture(nodeIsStoppingException());
        }

        try {
            SqlProperties properties = SqlQueryProcessor.DEFAULT_PROPERTIES;

            return executeScriptCore(
                    queryProcessor,
                    observableTimestampTracker,
                    busyLock::enterBusy,
                    busyLock::leaveBusy,
                    query,
                    arguments,
                    properties);
        } finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * Execute SQL script.
     *
     * @param queryProcessor Query processor.
     * @param observableTimestampTracker Tracker of the latest time observed by client.
     * @param enterBusy Enter busy lock action.
     * @param leaveBusy Leave busy lock action.
     * @param query SQL script.
     * @param arguments Arguments.
     * @param properties Properties.
     * @return Operation future.
     */
    public static CompletableFuture<Void> executeScriptCore(
            QueryProcessor queryProcessor,
            HybridTimestampTracker observableTimestampTracker,
            Supplier<Boolean> enterBusy,
            Runnable leaveBusy,
            String query,
            @Nullable Object[] arguments,
            SqlProperties properties) {

        SqlProperties properties0 = SqlPropertiesHelper.chain(properties, SqlPropertiesHelper.newBuilder()
                .set(QueryProperty.ALLOWED_QUERY_TYPES, SqlQueryType.ALL)
                .build());

        CompletableFuture<AsyncSqlCursor<InternalSqlRow>> f =
                queryProcessor.queryAsync(properties0, observableTimestampTracker, null, query, arguments);

        CompletableFuture<Void> resFut = new CompletableFuture<>();
        ScriptHandler handler = new ScriptHandler(resFut, enterBusy, leaveBusy);
        f.whenComplete(handler::processCursor);

        return resFut.exceptionally((th) -> {
            Throwable cause = ExceptionUtils.unwrapCause(th);

            throw new CompletionException(mapToPublicSqlException(cause));
        });
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

    private static SqlProperties.Builder toPropertiesBuilder(Statement statement) {
        return SqlPropertiesHelper.newBuilder()
                .set(QueryProperty.TIME_ZONE_ID, statement.timeZoneId())
                .set(QueryProperty.DEFAULT_SCHEMA, statement.defaultSchema())
                .set(QueryProperty.QUERY_TIMEOUT, statement.queryTimeout(TimeUnit.MILLISECONDS));
    }

    private int registerCursor(AsyncSqlCursor<?> cursor) {
        int cursorId = cursorIdGen.incrementAndGet();

        Object old = openedCursors.put(cursorId, cursor);

        assert old == null;

        return cursorId;
    }

    @TestOnly
    List<AsyncSqlCursor<?>> openedCursors() {
        return List.copyOf(openedCursors.values());
    }

    private static SqlException nodeIsStoppingException() {
        return new SqlException(NODE_STOPPING_ERR, "Node is stopping");
    }

    private static class ScriptHandler {
        private final CompletableFuture<Void> resFut;
        private final List<Throwable> cursorCloseErrors = Collections.synchronizedList(new ArrayList<>());
        private final Supplier<Boolean> enterBusy;
        private final Runnable leaveBusy;

        ScriptHandler(
                CompletableFuture<Void> resFut,
                Supplier<Boolean> enterBusy,
                Runnable leaveBusy) {
            this.resFut = resFut;
            this.enterBusy = enterBusy;
            this.leaveBusy = leaveBusy;
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

                if (!enterBusy.get()) {
                    onFail(nodeIsStoppingException());
                    return;
                }

                try {
                    if (cursor.hasNextResult()) {
                        cursor.nextResult().whenCompleteAsync(this::processCursor);
                        return;
                    }
                } finally {
                    leaveBusy.run();
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
