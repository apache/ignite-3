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
import java.util.concurrent.ConcurrentHashMap;
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
    public static final int DEFAULT_PAGE_SIZE = 1024;

    public static final long DEFAULT_TIMEOUT = 0;

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
        throw new UnsupportedOperationException("Not implemented yet.");
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
        throw new UnsupportedOperationException("Not implemented yet.");
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

        List<CompletableFuture<AsyncSqlCursor<List<Object>>>> futs = qryProc.queryAsync(ctx, schema, query, arguments);

        futsToClose.addAll(futs);

        if (futs.size() != 1) {
            throw new IgniteSqlException("Multiple statements aren't allowed.");
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
     * Session builder implementation.
     */
    public static class SessionBuilderImpl implements SessionBuilder {
        private final QueryProcessor qryProc;

        private long timeout = DEFAULT_TIMEOUT;

        private String schema;

        private int pageSize = DEFAULT_PAGE_SIZE;

        SessionBuilderImpl(QueryProcessor qryProc) {
            this.qryProc = qryProc;
        }

        /** {@inheritDoc} */
        @Override
        public long defaultTimeout(TimeUnit timeUnit) {
            return timeUnit.convert(timeout, TimeUnit.NANOSECONDS);
        }

        /** {@inheritDoc} */
        @Override
        public SessionBuilder defaultTimeout(long timeout, TimeUnit timeUnit) {
            this.timeout = timeUnit.toNanos(timeout);

            return this;
        }

        @Override
        public String defaultSchema() {
            return schema;
        }

        @Override
        public SessionBuilder defaultSchema(String schema) {
            this.schema = schema;

            return this;
        }

        @Override
        public int defaultPageSize() {
            return pageSize;
        }

        @Override
        public SessionBuilder defaultPageSize(int pageSize) {
            this.pageSize = pageSize;

            return this;
        }

        @Override
        public @Nullable Object property(String name) {
            return null;
        }

        @Override
        public SessionBuilder property(String name, @Nullable Object value) {
            return null;
        }

        @Override
        public Session build() {
            return new SessionImpl(
                    qryProc,
                    schema,
                    timeout,
                    pageSize
            );
        }
    }
}
