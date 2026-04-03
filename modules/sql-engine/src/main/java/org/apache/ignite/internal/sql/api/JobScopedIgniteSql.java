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

import static org.apache.ignite.internal.util.IgniteUtils.getInterruptibly;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.hlc.HybridTimestampTracker;
import org.apache.ignite.internal.sql.SyncResultSetAdapter;
import org.apache.ignite.internal.wrapper.Wrapper;
import org.apache.ignite.lang.CancellationToken;
import org.apache.ignite.sql.BatchedArguments;
import org.apache.ignite.sql.IgniteSql;
import org.apache.ignite.sql.ResultSet;
import org.apache.ignite.sql.SqlRow;
import org.apache.ignite.sql.Statement;
import org.apache.ignite.sql.Statement.StatementBuilder;
import org.apache.ignite.sql.async.AsyncResultSet;
import org.apache.ignite.table.mapper.Mapper;
import org.apache.ignite.tx.Transaction;
import org.jetbrains.annotations.Nullable;

/**
 * A scoped view of {@link IgniteSqlImpl} that uses a per-job {@link HybridTimestampTracker} instead of the node's global one.
 * Delegates all operations to the original {@link IgniteSqlImpl} instance, sharing its lifecycle, busy lock, and cursor tracking.
 */
public class JobScopedIgniteSql implements IgniteSql, Wrapper {
    private final IgniteSqlImpl delegate;

    private final HybridTimestampTracker jobTracker;

    public JobScopedIgniteSql(IgniteSqlImpl delegate, HybridTimestampTracker jobTracker) {
        this.delegate = delegate;
        this.jobTracker = jobTracker;
    }

    @Override
    public Statement createStatement(String query) {
        return delegate.createStatement(query);
    }

    @Override
    public StatementBuilder statementBuilder() {
        return delegate.statementBuilder();
    }

    @Override
    public ResultSet<SqlRow> execute(
            @Nullable Transaction transaction,
            @Nullable CancellationToken cancellationToken,
            String query,
            @Nullable Object... arguments
    ) {
        Objects.requireNonNull(query);

        return new SyncResultSetAdapter<>(sync(executeAsync(transaction, cancellationToken, query, arguments)));
    }

    @Override
    public ResultSet<SqlRow> execute(
            @Nullable Transaction transaction,
            @Nullable CancellationToken cancellationToken,
            Statement statement,
            @Nullable Object... arguments
    ) {
        Objects.requireNonNull(statement);

        return new SyncResultSetAdapter<>(sync(executeAsync(transaction, cancellationToken, statement, arguments)));
    }

    @Override
    public <T> ResultSet<T> execute(
            @Nullable Transaction transaction,
            @Nullable Mapper<T> mapper,
            @Nullable CancellationToken cancellationToken,
            String query,
            @Nullable Object... arguments
    ) {
        Objects.requireNonNull(query);

        return new SyncResultSetAdapter<>(sync(executeAsync(transaction, mapper, cancellationToken, query, arguments)));
    }

    @Override
    public <T> ResultSet<T> execute(
            @Nullable Transaction transaction,
            @Nullable Mapper<T> mapper,
            @Nullable CancellationToken cancellationToken,
            Statement statement,
            @Nullable Object... arguments
    ) {
        Objects.requireNonNull(statement);

        return new SyncResultSetAdapter<>(sync(executeAsync(transaction, mapper, cancellationToken, statement, arguments)));
    }

    @Override
    public CompletableFuture<AsyncResultSet<SqlRow>> executeAsync(
            @Nullable Transaction transaction,
            @Nullable CancellationToken cancellationToken,
            String query,
            @Nullable Object... arguments
    ) {
        return executeAsync(transaction, cancellationToken, createStatement(query), arguments);
    }

    @Override
    public CompletableFuture<AsyncResultSet<SqlRow>> executeAsync(
            @Nullable Transaction transaction,
            @Nullable CancellationToken cancellationToken,
            Statement statement,
            @Nullable Object... arguments
    ) {
        return delegate.executeAsyncInternal(jobTracker, transaction, cancellationToken, statement, arguments);
    }

    @Override
    public <T> CompletableFuture<AsyncResultSet<T>> executeAsync(
            @Nullable Transaction transaction,
            @Nullable Mapper<T> mapper,
            @Nullable CancellationToken cancellationToken,
            String query,
            @Nullable Object... arguments
    ) {
        // TODO: IGNITE-18695.
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    @Override
    public <T> CompletableFuture<AsyncResultSet<T>> executeAsync(
            @Nullable Transaction transaction,
            @Nullable Mapper<T> mapper,
            @Nullable CancellationToken cancellationToken,
            Statement statement,
            @Nullable Object... arguments
    ) {
        // TODO: IGNITE-18695.
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    @Override
    public long[] executeBatch(
            @Nullable Transaction transaction,
            @Nullable CancellationToken cancellationToken,
            String dmlQuery,
            BatchedArguments batch
    ) {
        return sync(executeBatchAsync(transaction, cancellationToken, dmlQuery, batch));
    }

    @Override
    public long[] executeBatch(
            @Nullable Transaction transaction,
            @Nullable CancellationToken cancellationToken,
            Statement dmlStatement,
            BatchedArguments batch
    ) {
        return sync(executeBatchAsync(transaction, cancellationToken, dmlStatement, batch));
    }

    @Override
    public CompletableFuture<long[]> executeBatchAsync(
            @Nullable Transaction transaction,
            @Nullable CancellationToken cancellationToken,
            String query,
            BatchedArguments batch
    ) {
        return executeBatchAsync(transaction, cancellationToken, createStatement(query), batch);
    }

    @Override
    public CompletableFuture<long[]> executeBatchAsync(
            @Nullable Transaction transaction,
            @Nullable CancellationToken cancellationToken,
            Statement statement,
            BatchedArguments batch
    ) {
        return delegate.executeBatchAsyncInternal(jobTracker, transaction, cancellationToken, statement, batch);
    }

    @Override
    public void executeScript(String query, @Nullable Object... arguments) {
        executeScript(null, query, arguments);
    }

    @Override
    public void executeScript(@Nullable CancellationToken cancellationToken, String query, @Nullable Object... arguments) {
        Objects.requireNonNull(query);

        sync(executeScriptAsync(cancellationToken, query, arguments));
    }

    @Override
    public CompletableFuture<Void> executeScriptAsync(String query, @Nullable Object... arguments) {
        return executeScriptAsync(null, query, arguments);
    }

    @Override
    public CompletableFuture<Void> executeScriptAsync(
            @Nullable CancellationToken cancellationToken,
            String query,
            @Nullable Object... arguments
    ) {
        return delegate.executeScriptAsyncInternal(jobTracker, cancellationToken, query, arguments);
    }

    private static <T> T sync(CompletableFuture<T> future) {
        return getInterruptibly(future);
    }

    @Override
    public <T> T unwrap(Class<T> classToUnwrap) {
        return delegate.unwrap(classToUnwrap);
    }
}
