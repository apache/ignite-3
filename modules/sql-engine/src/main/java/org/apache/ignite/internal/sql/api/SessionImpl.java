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

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Flow.Publisher;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.internal.sql.engine.AsyncSqlCursor;
import org.apache.ignite.internal.sql.engine.QueryContext;
import org.apache.ignite.internal.sql.engine.QueryProcessor;
import org.apache.ignite.internal.sql.engine.QueryTimeout;
import org.apache.ignite.sql.BatchedArguments;
import org.apache.ignite.sql.ResultSet;
import org.apache.ignite.sql.Session;
import org.apache.ignite.sql.Statement;
import org.apache.ignite.sql.async.AsyncResultSet;
import org.apache.ignite.sql.reactive.ReactiveResultSet;
import org.apache.ignite.tx.Transaction;
import org.jetbrains.annotations.Nullable;

/**
 * Embedded implementation of the SQL session.
 */
public class SessionImpl implements Session {
    private final QueryProcessor qryProc;

    private final long timeout;

    private final String schema;

    private final int pageSize;

    private final Set<CompletableFuture<AsyncSqlCursor<List<Object>>>> futsToClose = Collections.newSetFromMap(new ConcurrentHashMap<>());

    private final Set<AsyncSqlCursor<List<Object>>> cursToClose = Collections.newSetFromMap(new ConcurrentHashMap<>());

    /**
     * Constructor.
     *
     * @param qryProc Query processor.
     * @param schema  Query default schema.
     */
    SessionImpl(
            QueryProcessor qryProc,
            String schema,
            long timeout,
            int pageSize
    ) {
        this.qryProc = qryProc;
        this.schema = schema;
        this.timeout = timeout;
        this.pageSize = pageSize;
    }

    /** {@inheritDoc} */
    @Override
    public ResultSet execute(@Nullable Transaction transaction, String query, @Nullable Object... arguments) {
        return new ResultSetImpl(await(executeAsync(transaction, query, arguments)));
    }

    /** {@inheritDoc} */
    @Override
    public ResultSet execute(@Nullable Transaction transaction, Statement statement, @Nullable Object... arguments) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /** {@inheritDoc} */
    @Override
    public int[] executeBatch(@Nullable Transaction transaction, String dmlQuery, BatchedArguments batch) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /** {@inheritDoc} */
    @Override
    public int[] executeBatch(@Nullable Transaction transaction, Statement dmlStatement, BatchedArguments batch) {
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
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /** {@inheritDoc} */
    @Override
    public void close() {

    }

    /** {@inheritDoc} */
    @Override
    public SessionBuilder toBuilder() {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<AsyncResultSet> executeAsync(@Nullable Transaction transaction, String query, @Nullable Object... arguments) {
        QueryContext ctx = QueryContext.of(transaction, new QueryTimeout(timeout, TimeUnit.NANOSECONDS));

        try {
            List<CompletableFuture<AsyncSqlCursor<List<Object>>>> futs = qryProc.queryAsync(ctx, schema, query, arguments);

            futsToClose.addAll(futs);

            if (futs.size() != 1) {
                return CompletableFuture.failedFuture(new IgniteSqlException("Multiple statements aren't allowed."));
            }

            return futs.get(0).thenCompose(cur -> {
                        futsToClose.remove(futs.get(0));
                        cursToClose.add(cur);

                        return cur.requestNextAsync(pageSize)
                                .thenApply(
                                        batchRes -> new AsyncResultSetImpl(
                                                cur,
                                                batchRes,
                                                pageSize,
                                                () -> cursToClose.remove(cur)
                                        )
                                );
                    }
            );
        } catch (Exception e) {
            return CompletableFuture.failedFuture(e);
        }
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<AsyncResultSet> executeAsync(@Nullable Transaction transaction, Statement statement) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Integer> executeBatchAsync(@Nullable Transaction transaction, String query, BatchedArguments batch) {
        return null;
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Integer> executeBatchAsync(@Nullable Transaction transaction, Statement statement, BatchedArguments batch) {
        return null;
    }

    /** {@inheritDoc} */
    @Override
    public ReactiveResultSet executeReactive(@Nullable Transaction transaction, String query, @Nullable Object... arguments) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /** {@inheritDoc} */
    @Override
    public ReactiveResultSet executeReactive(@Nullable Transaction transaction, Statement statement) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /** {@inheritDoc} */
    @Override
    public Publisher<Integer> executeBatchReactive(@Nullable Transaction transaction, String query, BatchedArguments batch) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /** {@inheritDoc} */
    @Override
    public Publisher<Integer> executeBatchReactive(@Nullable Transaction transaction, Statement statement, BatchedArguments batch) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /**
     * Throw an exception as if it were unchecked.
     *
     * <p>This method erases type of the exception in the thrown clause, so checked exception could be thrown without need to wrap it with
     * unchecked one or adding a similar throws clause to the upstream methods.
     */
    @SuppressWarnings("unchecked")
    public static <E extends Throwable> void sneakyThrow(Throwable e) throws E {
        throw (E) e;
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
        } catch (Throwable e) {
            if (e instanceof ExecutionException) {
                e = e.getCause();
            } else if (e instanceof CompletionException) {
                e = e.getCause();
            }

            sneakyThrow(e);
        }

        assert false;

        return null;
    }
}
