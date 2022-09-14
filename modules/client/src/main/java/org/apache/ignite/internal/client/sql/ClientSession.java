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
import org.apache.ignite.internal.client.PayloadOutputChannel;
import org.apache.ignite.internal.client.ReliableChannel;
import org.apache.ignite.internal.client.proto.ClientOp;
import org.apache.ignite.sql.BatchedArguments;
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
    private final ReliableChannel ch;

    @Nullable
    private final Integer defaultPageSize;

    @Nullable
    private final String defaultSchema;

    @Nullable
    private final Long defaultQueryTimeout;

    @Nullable
    private final Long defaultSessionTimeout;

    @Nullable
    private final Map<String, Object> properties;

    /**
     * Constructor.
     *
     * @param ch Channel.
     * @param defaultPageSize Default page size.
     * @param defaultSchema Default schema.
     * @param defaultQueryTimeout Default query timeout.
     * @param defaultSessionTimeout Default session timeout.
     * @param properties Properties.
     */
    @SuppressWarnings("AssignmentOrReturnOfFieldWithMutableType")
    public ClientSession(
            ReliableChannel ch,
            @Nullable Integer defaultPageSize,
            @Nullable String defaultSchema,
            @Nullable Long defaultQueryTimeout,
            @Nullable Long defaultSessionTimeout,
            @Nullable Map<String, Object> properties) {
        this.ch = ch;
        this.defaultPageSize = defaultPageSize;
        this.defaultSchema = defaultSchema;
        this.defaultQueryTimeout = defaultQueryTimeout;
        this.defaultSessionTimeout = defaultSessionTimeout;
        this.properties = properties;
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<AsyncResultSet> executeAsync(@Nullable Transaction transaction, String query, @Nullable Object... arguments) {
        Objects.requireNonNull(query);

        ClientStatement statement = new ClientStatement(query, null, null, null, null);

        return executeAsync(transaction, statement, arguments);
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<AsyncResultSet> executeAsync(
            @Nullable Transaction transaction,
            Statement statement,
            @Nullable Object... arguments) {
        Objects.requireNonNull(statement);

        if (!(statement instanceof ClientStatement)) {
            throw new IllegalArgumentException("Unsupported statement type: " + statement.getClass());
        }

        ClientStatement clientStatement = (ClientStatement) statement;

        return ch.serviceAsync(ClientOp.SQL_EXEC, w -> {
            writeTx(transaction, w);

            w.out().packObject(oneOf(clientStatement.defaultSchema(), defaultSchema));
            w.out().packObject(oneOf(clientStatement.pageSizeNullable(), defaultPageSize));
            w.out().packObject(oneOf(clientStatement.queryTimeoutNullable(), defaultQueryTimeout));

            w.out().packObject(defaultSessionTimeout);

            packProperties(w, clientStatement.properties());

            w.out().packString(clientStatement.query());

            w.out().packObjectArray(arguments);
        }, r -> new ClientAsyncResultSet(r.clientChannel(), r.in()));
    }

    /** {@inheritDoc} */
    @Override
    public ReactiveResultSet executeReactive(@Nullable Transaction transaction, String query, @Nullable Object... arguments) {
        // TODO IGNITE-17058.
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /** {@inheritDoc} */
    @Override
    public ReactiveResultSet executeReactive(@Nullable Transaction transaction, Statement statement, @Nullable Object... arguments) {
        // TODO IGNITE-17058.
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /** {@inheritDoc} */
    @Override
    public long[] executeBatch(@Nullable Transaction transaction, Statement dmlStatement, BatchedArguments batch) {
        // TODO IGNITE-17059.
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<long[]> executeBatchAsync(@Nullable Transaction transaction, String query, BatchedArguments batch) {
        // TODO IGNITE-17059.
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<long[]> executeBatchAsync(@Nullable Transaction transaction, Statement statement, BatchedArguments batch) {
        // TODO IGNITE-17059.
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /** {@inheritDoc} */
    @Override
    public Publisher<Long> executeBatchReactive(@Nullable Transaction transaction, String query, BatchedArguments batch) {
        // TODO IGNITE-17058.
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /** {@inheritDoc} */
    @Override
    public Publisher<Long> executeBatchReactive(@Nullable Transaction transaction, Statement statement, BatchedArguments batch) {
        // TODO IGNITE-17058.
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /** {@inheritDoc} */
    @Override
    public void executeScript(String query, @Nullable Object... arguments) {
        // TODO IGNITE-17060.
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Void> executeScriptAsync(String query, @Nullable Object... arguments) {
        // TODO IGNITE-17060.
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /** {@inheritDoc} */
    @Override
    public long defaultQueryTimeout(TimeUnit timeUnit) {
        Objects.requireNonNull(timeUnit);

        return defaultQueryTimeout == null ? 0 : timeUnit.convert(defaultQueryTimeout, TimeUnit.MILLISECONDS);
    }

    /** {@inheritDoc} */
    @Override
    public long idleTimeout(TimeUnit timeUnit) {
        Objects.requireNonNull(timeUnit);

        return defaultSessionTimeout == null ? 0 : timeUnit.convert(defaultSessionTimeout, TimeUnit.MILLISECONDS);
    }

    /** {@inheritDoc} */
    @Override
    public String defaultSchema() {
        return defaultSchema;
    }

    /** {@inheritDoc} */
    @Override
    public int defaultPageSize() {
        return defaultPageSize == null ? 0 : defaultPageSize;
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable Object property(String name) {
        return properties == null ? null : properties.get(name);
    }

    /** {@inheritDoc} */
    @Override
    public void close() {
        sync(closeAsync());
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Void> closeAsync() {
        // TODO IGNITE-17134 Cancel/close all active cursors, queries, futures.
        return CompletableFuture.completedFuture(null);
    }

    /** {@inheritDoc} */
    @Override
    public Publisher<Void> closeReactive() {
        // TODO IGNITE-17058.
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /** {@inheritDoc} */
    @Override
    public SessionBuilder toBuilder() {
        return null;
    }

    private void packProperties(PayloadOutputChannel w, Map<String, Object> props) {
        int size = 0;

        if (props != null) {
            size += props.size();
        }

        // Statement properties override session properties.
        if (properties != null) {
            if (props != null) {
                for (String k : properties.keySet()) {
                    if (!props.containsKey(k)) {
                        size++;
                    }
                }
            } else {
                size += properties.size();
            }
        }

        w.out().packMapHeader(size);

        if (props != null) {
            for (Entry<String, Object> entry : props.entrySet()) {
                w.out().packString(entry.getKey());
                w.out().packObjectWithType(entry.getValue());
            }
        }

        if (properties != null) {
            for (Entry<String, Object> entry : properties.entrySet()) {
                if (props == null || !props.containsKey(entry.getKey())) {
                    w.out().packString(entry.getKey());
                    w.out().packObjectWithType(entry.getValue());
                }
            }
        }
    }

    private static <T> T oneOf(T a, T b) {
        return a != null ? a : b;
    }
}
