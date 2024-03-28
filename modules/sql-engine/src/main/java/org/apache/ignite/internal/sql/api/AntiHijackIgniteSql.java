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

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Flow.Publisher;
import org.apache.ignite.internal.thread.PublicApiThreading;
import org.apache.ignite.internal.wrapper.Wrapper;
import org.apache.ignite.sql.BatchedArguments;
import org.apache.ignite.sql.IgniteSql;
import org.apache.ignite.sql.ResultSet;
import org.apache.ignite.sql.SqlRow;
import org.apache.ignite.sql.Statement;
import org.apache.ignite.sql.Statement.StatementBuilder;
import org.apache.ignite.sql.async.AsyncResultSet;
import org.apache.ignite.sql.reactive.ReactiveResultSet;
import org.apache.ignite.table.mapper.Mapper;
import org.apache.ignite.tx.Transaction;
import org.jetbrains.annotations.Nullable;

/**
 * Wrapper around {@link IgniteSql} that adds protection agains thread hijacking by users.
 */
public class AntiHijackIgniteSql implements IgniteSql, Wrapper {
    private final IgniteSql sql;
    private final Executor asyncContinuationExecutor;

    public AntiHijackIgniteSql(IgniteSql sql, Executor asyncContinuationExecutor) {
        this.sql = sql;
        this.asyncContinuationExecutor = asyncContinuationExecutor;
    }

    @Override
    public Statement createStatement(String query) {
        return sql.createStatement(query);
    }

    @Override
    public StatementBuilder statementBuilder() {
        return sql.statementBuilder();
    }

    @Override
    public ResultSet<SqlRow> execute(@Nullable Transaction transaction, String query, @Nullable Object... arguments) {
        return sql.execute(transaction, query, arguments);
    }

    @Override
    public ResultSet<SqlRow> execute(@Nullable Transaction transaction, Statement statement, @Nullable Object... arguments) {
        return sql.execute(transaction, statement, arguments);
    }

    @Override
    public <T> ResultSet<T> execute(
            @Nullable Transaction transaction,
            @Nullable Mapper<T> mapper,
            String query,
            @Nullable Object... arguments
    ) {
        return sql.execute(transaction, mapper, query, arguments);
    }

    @Override
    public <T> ResultSet<T> execute(
            @Nullable Transaction transaction,
            @Nullable Mapper<T> mapper,
            Statement statement,
            @Nullable Object... arguments
    ) {
        return sql.execute(transaction, mapper, statement, arguments);
    }

    @Override
    public CompletableFuture<AsyncResultSet<SqlRow>> executeAsync(
            @Nullable Transaction transaction,
            String query,
            @Nullable Object... arguments
    ) {
        return preventThreadHijackForResultSet(sql.executeAsync(transaction, query, arguments));
    }

    @Override
    public CompletableFuture<AsyncResultSet<SqlRow>> executeAsync(
            @Nullable Transaction transaction,
            Statement statement,
            @Nullable Object... arguments
    ) {
        return preventThreadHijackForResultSet(sql.executeAsync(transaction, statement, arguments));
    }

    @Override
    public <T> CompletableFuture<AsyncResultSet<T>> executeAsync(
            @Nullable Transaction transaction,
            @Nullable Mapper<T> mapper,
            String query,
            @Nullable Object... arguments
    ) {
        return preventThreadHijackForResultSet(sql.executeAsync(transaction, mapper, query, arguments));
    }

    @Override
    public <T> CompletableFuture<AsyncResultSet<T>> executeAsync(
            @Nullable Transaction transaction,
            @Nullable Mapper<T> mapper,
            Statement statement,
            @Nullable Object... arguments
    ) {
        return preventThreadHijackForResultSet(sql.executeAsync(transaction, mapper, statement, arguments));
    }

    @Override
    public ReactiveResultSet executeReactive(@Nullable Transaction transaction, String query, @Nullable Object... arguments) {
        return sql.executeReactive(transaction, query, arguments);
    }

    @Override
    public ReactiveResultSet executeReactive(@Nullable Transaction transaction, Statement statement, @Nullable Object... arguments) {
        return sql.executeReactive(transaction, statement, arguments);
    }

    @Override
    public long[] executeBatch(@Nullable Transaction transaction, String dmlQuery, BatchedArguments batch) {
        return sql.executeBatch(transaction, dmlQuery, batch);
    }

    @Override
    public long[] executeBatch(@Nullable Transaction transaction, Statement dmlStatement, BatchedArguments batch) {
        return sql.executeBatch(transaction, dmlStatement, batch);
    }

    @Override
    public CompletableFuture<long[]> executeBatchAsync(@Nullable Transaction transaction, String query, BatchedArguments batch) {
        return preventThreadHijack(sql.executeBatchAsync(transaction, query, batch));
    }

    @Override
    public CompletableFuture<long[]> executeBatchAsync(@Nullable Transaction transaction, Statement statement, BatchedArguments batch) {
        return preventThreadHijack(sql.executeBatchAsync(transaction, statement, batch));
    }

    @Override
    public Publisher<Long> executeBatchReactive(@Nullable Transaction transaction, String query, BatchedArguments batch) {
        return sql.executeBatchReactive(transaction, query, batch);
    }

    @Override
    public Publisher<Long> executeBatchReactive(@Nullable Transaction transaction, Statement statement, BatchedArguments batch) {
        return sql.executeBatchReactive(transaction, statement, batch);
    }

    @Override
    public void executeScript(String query, @Nullable Object... arguments) {
        sql.executeScript(query, arguments);
    }

    @Override
    public CompletableFuture<Void> executeScriptAsync(String query, @Nullable Object... arguments) {
        return preventThreadHijack(sql.executeScriptAsync(query, arguments));
    }

    private <T> CompletableFuture<AsyncResultSet<T>> preventThreadHijackForResultSet(CompletableFuture<AsyncResultSet<T>> originalFuture) {
        return preventThreadHijack(originalFuture)
                .thenApply(resultSet -> new AntiHijackAsyncResultSet<>(resultSet, asyncContinuationExecutor));
    }

    private <T> CompletableFuture<T> preventThreadHijack(CompletableFuture<T> originalFuture) {
        return PublicApiThreading.preventThreadHijack(originalFuture, asyncContinuationExecutor);
    }

    @Override
    public <T> T unwrap(Class<T> classToUnwrap) {
        return classToUnwrap.cast(sql);
    }
}
