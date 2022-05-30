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

package org.apache.ignite.internal.client.sql;

import static org.apache.ignite.internal.client.ClientUtils.sync;
import static org.apache.ignite.internal.client.table.ClientTable.writeTx;

import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Flow.Publisher;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.internal.client.ReliableChannel;
import org.apache.ignite.internal.client.proto.ClientOp;
import org.apache.ignite.sql.BatchedArguments;
import org.apache.ignite.sql.ResultSet;
import org.apache.ignite.sql.Session;
import org.apache.ignite.sql.Statement;
import org.apache.ignite.sql.async.AsyncResultSet;
import org.apache.ignite.sql.reactive.ReactiveResultSet;
import org.apache.ignite.tx.Transaction;
import org.jetbrains.annotations.Nullable;

/**
 * Client SQL session.
 */
public class ClientSession implements Session {
    /** Channel. */
    private final ReliableChannel ch;

    @Nullable
    private final Integer defaultPageSize;

    @Nullable
    private final String defaultSchema;

    @Nullable
    private final Long defaultTimeout;

    @Nullable
    private final Map<String, Object> properties;

    /**
     * Constructor.
     *
     * @param ch Channel.
     * @param defaultPageSize Default page size.
     * @param defaultSchema Default schema.
     * @param defaultTimeout Default timeout.
     * @param properties Properties.
     */
    @SuppressWarnings("AssignmentOrReturnOfFieldWithMutableType")
    public ClientSession(
            ReliableChannel ch,
            @Nullable Integer defaultPageSize,
            @Nullable String defaultSchema,
            @Nullable Long defaultTimeout,
            @Nullable Map<String, Object> properties) {
        this.ch = ch;
        this.defaultPageSize = defaultPageSize;
        this.defaultSchema = defaultSchema;
        this.defaultTimeout = defaultTimeout;
        this.properties = properties;
    }

    /** {@inheritDoc} */
    @Override
    public ResultSet execute(@Nullable Transaction transaction, String query, @Nullable Object... arguments) {
        // TODO: Wrap AsyncResultSet.
        // return sync(executeAsync(transaction, query, arguments));
        return null;
    }

    /** {@inheritDoc} */
    @Override
    public ResultSet execute(@Nullable Transaction transaction, Statement statement, @Nullable Object... arguments) {
        return null;
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<AsyncResultSet> executeAsync(@Nullable Transaction transaction, String query, @Nullable Object... arguments) {
        return null;
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<AsyncResultSet> executeAsync(
            @Nullable Transaction transaction,
            Statement statement,
            @Nullable Object... arguments) {
        Objects.requireNonNull(statement);

        return ch.serviceAsync(ClientOp.SQL_EXEC, w -> {
            writeTx(transaction, w);

            w.out().packObject(defaultPageSize);
            w.out().packObject(defaultSchema);
            w.out().packObject(defaultTimeout);

            if (properties != null) {
                w.out().packMapHeader(0);
            } else {
                w.out().packMapHeader(properties.size());

                for (Entry<String, Object> entry : properties.entrySet()) {
                    w.out().packString(entry.getKey());
                    w.out().packObjectWithType(entry.getValue());
                }
            }

            w.out().packObject(statement.defaultSchema());
            w.out().packObject(statement.pageSize());
            w.out().packObject(statement.query());
            w.out().packObject(statement.queryTimeout(TimeUnit.MILLISECONDS));
            w.out().packBoolean(statement.prepared());

            // TODO: Pack statement properties.
            w.out().packMapHeader(0);
        }, r -> {
            Long resourceId = r.in().tryUnpackNil() ? null : r.in().unpackLong();
            boolean hasRowSet = r.in().unpackBoolean();
            boolean hasMorePages = r.in().unpackBoolean();
            boolean wasApplied = r.in().unpackBoolean();

            r.in().unpackArrayHeader(); // TODO: Metadata IGNITE-17052.

            if (hasRowSet) {
                // TODO: Unpack rows.
            }

            return new ClientAsyncResultSet();
        });
    }

    /** {@inheritDoc} */
    @Override
    public ReactiveResultSet executeReactive(@Nullable Transaction transaction, String query, @Nullable Object... arguments) {
        return null;
    }

    /** {@inheritDoc} */
    @Override
    public ReactiveResultSet executeReactive(@Nullable Transaction transaction, Statement statement, @Nullable Object... arguments) {
        return null;
    }

    /** {@inheritDoc} */
    @Override
    public int[] executeBatch(@Nullable Transaction transaction, String dmlQuery, BatchedArguments batch) {
        return new int[0];
    }

    /** {@inheritDoc} */
    @Override
    public int[] executeBatch(@Nullable Transaction transaction, Statement dmlStatement, BatchedArguments batch) {
        return new int[0];
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<int[]> executeBatchAsync(@Nullable Transaction transaction, String query, BatchedArguments batch) {
        return null;
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<int[]> executeBatchAsync(@Nullable Transaction transaction, Statement statement, BatchedArguments batch) {
        return null;
    }

    /** {@inheritDoc} */
    @Override
    public Publisher<Integer> executeBatchReactive(@Nullable Transaction transaction, String query, BatchedArguments batch) {
        return null;
    }

    /** {@inheritDoc} */
    @Override
    public Publisher<Integer> executeBatchReactive(@Nullable Transaction transaction, Statement statement, BatchedArguments batch) {
        return null;
    }

    /** {@inheritDoc} */
    @Override
    public void executeScript(String query, @Nullable Object... arguments) {

    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Void> executeScriptAsync(String query, @Nullable Object... arguments) {
        return null;
    }

    /** {@inheritDoc} */
    @Override
    public long defaultTimeout(TimeUnit timeUnit) {
        return 0;
    }

    /** {@inheritDoc} */
    @Override
    public String defaultSchema() {
        return null;
    }

    /** {@inheritDoc} */
    @Override
    public int defaultPageSize() {
        return 0;
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable Object property(String name) {
        return null;
    }

    /** {@inheritDoc} */
    @Override
    public void close() {
        sync(closeAsync());
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Void> closeAsync() {
        // TODO: Cancel/close all active futures.
        return null;
    }

    /** {@inheritDoc} */
    @Override
    public Publisher<Void> closeReactive() {
        // TODO: Future to Publisher.
        return null;
    }

    /** {@inheritDoc} */
    @Override
    public SessionBuilder toBuilder() {
        return null;
    }
}
