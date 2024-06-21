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

import static org.apache.ignite.internal.client.table.ClientTable.writeTx;
import static org.apache.ignite.internal.util.ExceptionUtils.unwrapCause;

import java.time.ZoneId;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.internal.binarytuple.BinaryTupleBuilder;
import org.apache.ignite.internal.client.PayloadOutputChannel;
import org.apache.ignite.internal.client.PayloadReader;
import org.apache.ignite.internal.client.PayloadWriter;
import org.apache.ignite.internal.client.ReliableChannel;
import org.apache.ignite.internal.client.proto.ClientBinaryTupleUtils;
import org.apache.ignite.internal.client.proto.ClientMessageUnpacker;
import org.apache.ignite.internal.client.proto.ClientOp;
import org.apache.ignite.internal.client.tx.ClientLazyTransaction;
import org.apache.ignite.internal.marshaller.MarshallersProvider;
import org.apache.ignite.internal.sql.StatementBuilderImpl;
import org.apache.ignite.internal.sql.StatementImpl;
import org.apache.ignite.internal.sql.SyncResultSetAdapter;
import org.apache.ignite.internal.util.ExceptionUtils;
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
import org.apache.ignite.tx.TransactionException;
import org.jetbrains.annotations.Nullable;

/**
 * Client SQL.
 */
public class ClientSql implements IgniteSql {
    private static final Mapper<SqlRow> sqlRowMapper = () -> SqlRow.class;

    /** Channel. */
    private final ReliableChannel ch;

    /** Marshallers provider. */
    private final MarshallersProvider marshallers;

    /**
     * Constructor.
     *
     * @param ch Channel.
     * @param marshallers Marshallers provider.
     */
    public ClientSql(ReliableChannel ch, MarshallersProvider marshallers) {
        this.ch = ch;
        this.marshallers = marshallers;
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
    public <T> ResultSet<T> execute(
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
    public <T> ResultSet<T> execute(
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
        return executeBatch(transaction, new StatementImpl(dmlQuery), batch);
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
            @Nullable Object... arguments) {
        Objects.requireNonNull(query);

        StatementImpl statement = new StatementImpl(query);

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

        StatementImpl statement = new StatementImpl(query);

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

        PayloadWriter payloadWriter = w -> {
            writeTx(transaction, w);

            w.out().packString(statement.defaultSchema());
            w.out().packInt(statement.pageSize());
            w.out().packLong(statement.queryTimeout(TimeUnit.MILLISECONDS));

            w.out().packLongNullable(0L); // defaultSessionTimeout
            w.out().packString(statement.timeZoneId().getId());

            packProperties(w, null);

            w.out().packString(statement.query());

            w.out().packObjectArrayAsBinaryTuple(arguments);

            w.out().packLong(ch.observableTimestamp());
        };

        PayloadReader<AsyncResultSet<T>> payloadReader = r -> new ClientAsyncResultSet<>(r.clientChannel(), marshallers, r.in(), mapper);

        if (transaction != null) {
            try {
                //noinspection resource
                return ClientLazyTransaction.ensureStarted(transaction, ch, null)
                        .thenCompose(tx -> tx.channel().serviceAsync(ClientOp.SQL_EXEC, payloadWriter, payloadReader))
                        .exceptionally(ClientSql::handleException);
            } catch (TransactionException e) {
                return CompletableFuture.failedFuture(new SqlException(e.traceId(), e.code(), e.getMessage(), e));
            }
        }

        return ch.serviceAsync(ClientOp.SQL_EXEC, payloadWriter, payloadReader);
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<long[]> executeBatchAsync(@Nullable Transaction transaction, String query, BatchedArguments batch) {
        return executeBatchAsync(transaction, new StatementImpl(query), batch);
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<long[]> executeBatchAsync(@Nullable Transaction transaction, Statement statement, BatchedArguments batch) {
        PayloadWriter payloadWriter = w -> {
            writeTx(transaction, w);

            w.out().packString(statement.defaultSchema());
            w.out().packInt(statement.pageSize());
            w.out().packLong(statement.queryTimeout(TimeUnit.MILLISECONDS));
            w.out().packNil(); // sessionTimeout
            w.out().packString(statement.timeZoneId().getId());

            packProperties(w, null);

            w.out().packString(statement.query());
            w.out().packObjectArrayAsBinaryTupleArray(batch);
            w.out().packLong(ch.observableTimestamp());
        };

        PayloadReader<BatchResultInternal> payloadReader = r -> {
            ClientMessageUnpacker unpacker = r.in();

            unpacker.skipValues(4); // skipping values that are not currently in use.

            long[] updateCounters = unpacker.unpackLongArray();

            if (unpacker.tryUnpackNil()) {
                unpacker.skipValues(2);

                return new BatchResultInternal(updateCounters);
            }

            int errCode = unpacker.unpackInt();
            String message = unpacker.tryUnpackNil() ? null : unpacker.unpackString();
            UUID traceId = unpacker.unpackUuid();

            return new BatchResultInternal(new SqlBatchException(traceId, errCode, updateCounters, message));
        };

        return ch.serviceAsync(ClientOp.SQL_EXEC_BATCH, payloadWriter, payloadReader)
                .thenApply((batchRes) -> {
                    if (batchRes.exception != null) {
                        throw batchRes.exception;
                    }

                    return batchRes.updCounters;
                })
                .exceptionally(ClientSql::handleException);
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Void> executeScriptAsync(String query, @Nullable Object... arguments) {
        Objects.requireNonNull(query);

        PayloadWriter payloadWriter = w -> {
            w.out().packNil(); // schemaName
            w.out().packNil(); // pageSize
            w.out().packNil(); // queryTimeout
            w.out().packNil(); // sessionTimeout
            w.out().packString(ZoneId.systemDefault().getId());

            packProperties(w, null);

            w.out().packString(query);
            w.out().packObjectArrayAsBinaryTuple(arguments);
            w.out().packLong(ch.observableTimestamp());
        };

        return ch.serviceAsync(ClientOp.SQL_EXEC_SCRIPT, payloadWriter, null);
    }

    private static void packProperties(
            PayloadOutputChannel w,
            @Nullable Map<String, Object> statementProps) {
        int size = 0;

        if (statementProps != null) {
            size += statementProps.size();
        }

        w.out().packInt(size);
        var builder = new BinaryTupleBuilder(size * 4);

        if (statementProps != null) {
            for (Entry<String, Object> entry : statementProps.entrySet()) {
                builder.appendString(entry.getKey());
                ClientBinaryTupleUtils.appendObject(builder, entry.getValue());
            }
        }

        w.out().packBinaryTuple(builder);
    }

    private static <T> T handleException(Throwable e) {
        Throwable ex = unwrapCause(e);
        if (ex instanceof TransactionException) {
            var te = (TransactionException) ex;
            throw new SqlException(te.traceId(), te.code(), te.getMessage(), te);
        }

        throw ExceptionUtils.sneakyThrow(ex);
    }

    private static class BatchResultInternal {
        final long[] updCounters;
        final SqlBatchException exception;

        BatchResultInternal(long[] updCounters) {
            this.updCounters = updCounters;
            this.exception = null;
        }

        BatchResultInternal(SqlBatchException exception) {
            this.updCounters = null;
            this.exception = exception;
        }
    }
}
