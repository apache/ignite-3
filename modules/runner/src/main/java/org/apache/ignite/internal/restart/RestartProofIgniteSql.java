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

package org.apache.ignite.internal.restart;

import java.util.concurrent.CompletableFuture;
import org.apache.ignite.Ignite;
import org.apache.ignite.internal.wrapper.Wrapper;
import org.apache.ignite.internal.wrapper.Wrappers;
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
 * Reference to {@link IgniteSql} under a swappable {@link Ignite} instance. When a restart happens, this switches to
 * the new Ignite instance.
 *
 * <p>API operations on this are linearized with respect to node restarts. Normally (except for situations when timeouts trigger), user
 * operations will not interact with detached objects.
 */
// TODO; IGNITE-23064 - make returned cursors restart-proof.
class RestartProofIgniteSql implements IgniteSql, Wrapper {
    private final IgniteAttachmentLock attachmentLock;

    RestartProofIgniteSql(IgniteAttachmentLock attachmentLock) {
        this.attachmentLock = attachmentLock;
    }

    @Override
    public Statement createStatement(String query) {
        return attachmentLock.attached(ignite -> ignite.sql().createStatement(query));
    }

    @Override
    public StatementBuilder statementBuilder() {
        return attachmentLock.attached(ignite -> ignite.sql().statementBuilder());
    }

    @Override
    public ResultSet<SqlRow> execute(@Nullable Transaction transaction, String query, @Nullable Object... arguments) {
        return attachmentLock.attached(ignite -> ignite.sql().execute(transaction, query, arguments));
    }

    @Override
    public ResultSet<SqlRow> execute(@Nullable Transaction transaction, Statement statement, @Nullable Object... arguments) {
        return attachmentLock.attached(ignite -> ignite.sql().execute(transaction, statement, arguments));
    }

    @Override
    public <T> ResultSet<T> execute(
            @Nullable Transaction transaction,
            @Nullable Mapper<T> mapper,
            String query,
            @Nullable Object... arguments
    ) {
        return attachmentLock.attached(ignite -> ignite.sql().execute(transaction, mapper, query, arguments));
    }

    @Override
    public <T> ResultSet<T> execute(
            @Nullable Transaction transaction,
            @Nullable Mapper<T> mapper,
            Statement statement,
            @Nullable Object... arguments
    ) {
        return attachmentLock.attached(ignite -> ignite.sql().execute(transaction, mapper, statement, arguments));
    }

    @Override
    public CompletableFuture<AsyncResultSet<SqlRow>> executeAsync(
            @Nullable Transaction transaction,
            String query,
            @Nullable Object... arguments
    ) {
        return attachmentLock.attachedAsync(ignite -> ignite.sql().executeAsync(transaction, query, arguments));
    }

    @Override
    public CompletableFuture<AsyncResultSet<SqlRow>> executeAsync(
            @Nullable Transaction transaction,
            Statement statement,
            @Nullable Object... arguments
    ) {
        return attachmentLock.attachedAsync(ignite -> ignite.sql().executeAsync(transaction, statement, arguments));
    }

    @Override
    public <T> CompletableFuture<AsyncResultSet<T>> executeAsync(
            @Nullable Transaction transaction,
            @Nullable Mapper<T> mapper,
            String query,
            @Nullable Object... arguments
    ) {
        return attachmentLock.attachedAsync(ignite -> ignite.sql().executeAsync(transaction, mapper, query, arguments));
    }

    @Override
    public <T> CompletableFuture<AsyncResultSet<T>> executeAsync(
            @Nullable Transaction transaction,
            @Nullable Mapper<T> mapper,
            Statement statement,
            @Nullable Object... arguments
    ) {
        return attachmentLock.attachedAsync(ignite -> ignite.sql().executeAsync(transaction, mapper, statement, arguments));
    }

    @Override
    public long[] executeBatch(@Nullable Transaction transaction, String dmlQuery, BatchedArguments batch) {
        return attachmentLock.attached(ignite -> ignite.sql().executeBatch(transaction, dmlQuery, batch));
    }

    @Override
    public long[] executeBatch(@Nullable Transaction transaction, Statement dmlStatement, BatchedArguments batch) {
        return attachmentLock.attached(ignite -> ignite.sql().executeBatch(transaction, dmlStatement, batch));
    }

    @Override
    public CompletableFuture<long[]> executeBatchAsync(@Nullable Transaction transaction, String query, BatchedArguments batch) {
        return attachmentLock.attachedAsync(ignite -> ignite.sql().executeBatchAsync(transaction, query, batch));
    }

    @Override
    public CompletableFuture<long[]> executeBatchAsync(@Nullable Transaction transaction, Statement statement, BatchedArguments batch) {
        return attachmentLock.attachedAsync(ignite -> ignite.sql().executeBatchAsync(transaction, statement, batch));
    }

    @Override
    public void executeScript(String query, @Nullable Object... arguments) {
        attachmentLock.consumeAttached(ignite -> ignite.sql().executeScript(query, arguments));
    }

    @Override
    public CompletableFuture<Void> executeScriptAsync(String query, @Nullable Object... arguments) {
        return attachmentLock.attachedAsync(ignite -> ignite.sql().executeScriptAsync(query, arguments));
    }

    @Override
    public <T> T unwrap(Class<T> classToUnwrap) {
        return attachmentLock.attached(ignite -> Wrappers.unwrap(ignite.sql(), classToUnwrap));
    }
}
