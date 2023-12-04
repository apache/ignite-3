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

package org.apache.ignite.internal.client.sql;

import static org.apache.ignite.internal.client.ClientUtils.sync;
import static org.apache.ignite.internal.client.table.ClientTable.writeTx;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;

import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Flow.Publisher;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.internal.binarytuple.BinaryTupleBuilder;
import org.apache.ignite.internal.client.PayloadOutputChannel;
import org.apache.ignite.internal.client.PayloadReader;
import org.apache.ignite.internal.client.PayloadWriter;
import org.apache.ignite.internal.client.ReliableChannel;
import org.apache.ignite.internal.client.proto.ClientBinaryTupleUtils;
import org.apache.ignite.internal.client.proto.ClientOp;
import org.apache.ignite.internal.client.tx.ClientTransaction;
import org.apache.ignite.internal.sql.AbstractSession;
import org.apache.ignite.sql.BatchedArguments;
import org.apache.ignite.sql.SqlRow;
import org.apache.ignite.sql.Statement;
import org.apache.ignite.sql.async.AsyncResultSet;
import org.apache.ignite.sql.reactive.ReactiveResultSet;
import org.apache.ignite.table.mapper.Mapper;
import org.apache.ignite.tx.Transaction;
import org.jetbrains.annotations.Nullable;

/**
 * Client SQL session.
 */
public class ClientSession implements AbstractSession {
    private static final Mapper<SqlRow> sqlRowMapper = () -> SqlRow.class;

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
    ClientSession(
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
    public CompletableFuture<AsyncResultSet<SqlRow>> executeAsync(
            @Nullable Transaction transaction,
            String query,
            @Nullable Object... arguments) {
        Objects.requireNonNull(query);

        ClientStatement statement = new ClientStatement(query, null, null, null, null);

        return executeAsync(transaction, statement, arguments);
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<AsyncResultSet<SqlRow>> executeAsync(
            @Nullable Transaction transaction,
            Statement statement,
            @Nullable Object... arguments) {
        return executeAsync(transaction, sqlRowMapper, statement, arguments);
    }

    /** {@inheritDoc} */
    @Override
    public <T> CompletableFuture<AsyncResultSet<T>> executeAsync(
            @Nullable Transaction transaction,
            @Nullable Mapper<T> mapper,
            String query,
            @Nullable Object... arguments) {
        Objects.requireNonNull(query);

        ClientStatement statement = new ClientStatement(query, null, null, null, null);

        return executeAsync(transaction, mapper, statement, arguments);
    }

    /** {@inheritDoc} */
    @Override
    public <T> CompletableFuture<AsyncResultSet<T>> executeAsync(
            @Nullable Transaction transaction,
            @Nullable Mapper<T> mapper,
            Statement statement,
            @Nullable Object... arguments) {
        Objects.requireNonNull(statement);

        if (!(statement instanceof ClientStatement)) {
            throw new IllegalArgumentException("Unsupported statement type: " + statement.getClass());
        }

        ClientStatement clientStatement = (ClientStatement) statement;

        PayloadWriter payloadWriter = w -> {
            writeTx(transaction, w);

            w.out().packString(oneOf(clientStatement.defaultSchema(), defaultSchema));
            w.out().packIntNullable(oneOf(clientStatement.pageSizeNullable(), defaultPageSize));
            w.out().packLongNullable(oneOf(clientStatement.queryTimeoutNullable(), defaultQueryTimeout));

            w.out().packLongNullable(defaultSessionTimeout);

            packProperties(w, properties, clientStatement.properties());

            w.out().packString(clientStatement.query());

            w.out().packObjectArrayAsBinaryTuple(arguments);

            w.out().packLong(ch.observableTimestamp());
        };

        PayloadReader<AsyncResultSet<T>> payloadReader = r -> new ClientAsyncResultSet<>(r.clientChannel(), r.in(), mapper);

        if (transaction != null) {
            //noinspection resource
            return ClientTransaction.get(transaction).channel().serviceAsync(ClientOp.SQL_EXEC, payloadWriter, payloadReader);
        }

        return ch.serviceAsync(ClientOp.SQL_EXEC, payloadWriter, payloadReader);
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
    public CompletableFuture<Void> executeScriptAsync(String query, @Nullable Object... arguments) {
        Objects.requireNonNull(query);

        PayloadWriter payloadWriter = w -> {
            w.out().packString(defaultSchema);
            w.out().packLongNullable(defaultQueryTimeout);
            w.out().packLongNullable(defaultSessionTimeout);

            // TODO: Do we need properties for scripts?
            packProperties(w, properties, null);

            w.out().packString(query);
            w.out().packObjectArrayAsBinaryTuple(arguments);
            w.out().packLong(ch.observableTimestamp());
        };

        return ch.serviceAsync(ClientOp.SQL_EXEC_SCRIPT, payloadWriter, null);
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
        return nullCompletedFuture();
    }

    /** {@inheritDoc} */
    @Override
    public Publisher<Void> closeReactive() {
        // TODO IGNITE-17058.
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /** {@inheritDoc} */
    @Override
    public boolean closed() {
        // TODO IGNITE-17134 Cancel/close all active cursors, queries, futures.
        return false;
    }

    /** {@inheritDoc} */
    @Override
    public SessionBuilder toBuilder() {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    private static void packProperties(
            PayloadOutputChannel w,
            @Nullable Map<String, Object> sessionProps,
            @Nullable Map<String, Object> statementProps) {
        int size = 0;

        if (statementProps != null) {
            size += statementProps.size();
        }

        // Statement properties override session properties.
        if (sessionProps != null) {
            if (statementProps != null) {
                for (String k : sessionProps.keySet()) {
                    if (!statementProps.containsKey(k)) {
                        size++;
                    }
                }
            } else {
                size += sessionProps.size();
            }
        }

        w.out().packInt(size);
        var builder = new BinaryTupleBuilder(size * 4);

        if (statementProps != null) {
            for (Entry<String, Object> entry : statementProps.entrySet()) {
                builder.appendString(entry.getKey());
                ClientBinaryTupleUtils.appendObject(builder, entry.getValue());
            }
        }

        if (sessionProps != null) {
            for (Entry<String, Object> entry : sessionProps.entrySet()) {
                if (statementProps == null || !statementProps.containsKey(entry.getKey())) {
                    builder.appendString(entry.getKey());
                    ClientBinaryTupleUtils.appendObject(builder, entry.getValue());
                }
            }
        }

        w.out().packBinaryTuple(builder);
    }

    private static <T> @Nullable T oneOf(@Nullable T a, @Nullable T b) {
        return a != null ? a : b;
    }
}
