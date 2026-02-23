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

import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.function.Function.identity;
import static org.apache.ignite.internal.client.proto.ProtocolBitmaskFeature.SQL_DIRECT_TX_MAPPING;
import static org.apache.ignite.internal.client.proto.ProtocolBitmaskFeature.SQL_MULTISTATEMENT_SUPPORT;
import static org.apache.ignite.internal.client.proto.ProtocolBitmaskFeature.SQL_PARTITION_AWARENESS;
import static org.apache.ignite.internal.client.proto.ProtocolBitmaskFeature.TX_DELAYED_ACKS;
import static org.apache.ignite.internal.client.proto.ProtocolBitmaskFeature.TX_DIRECT_MAPPING;
import static org.apache.ignite.internal.client.proto.ProtocolBitmaskFeature.TX_PIGGYBACK;
import static org.apache.ignite.internal.util.ExceptionUtils.sneakyThrow;
import static org.apache.ignite.internal.util.ExceptionUtils.unwrapCause;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import java.time.ZoneId;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import org.apache.ignite.internal.binarytuple.BinaryTupleBuilder;
import org.apache.ignite.internal.client.ClientUtils;
import org.apache.ignite.internal.client.PartitionMapping;
import org.apache.ignite.internal.client.PayloadOutputChannel;
import org.apache.ignite.internal.client.PayloadReader;
import org.apache.ignite.internal.client.PayloadWriter;
import org.apache.ignite.internal.client.ReliableChannel;
import org.apache.ignite.internal.client.WriteContext;
import org.apache.ignite.internal.client.proto.ClientBinaryTupleUtils;
import org.apache.ignite.internal.client.proto.ClientMessageUnpacker;
import org.apache.ignite.internal.client.proto.ClientOp;
import org.apache.ignite.internal.client.table.ClientTable;
import org.apache.ignite.internal.client.tx.ClientLazyTransaction;
import org.apache.ignite.internal.client.tx.ClientTransaction;
import org.apache.ignite.internal.client.tx.DirectTxUtils;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.marshaller.MarshallersProvider;
import org.apache.ignite.internal.sql.StatementBuilderImpl;
import org.apache.ignite.internal.sql.StatementImpl;
import org.apache.ignite.internal.sql.SyncResultSetAdapter;
import org.apache.ignite.internal.util.ExceptionUtils;
import org.apache.ignite.lang.CancelHandleHelper;
import org.apache.ignite.lang.CancellationToken;
import org.apache.ignite.lang.ErrorGroups.Sql;
import org.apache.ignite.lang.TableNotFoundException;
import org.apache.ignite.sql.BatchedArguments;
import org.apache.ignite.sql.IgniteSql;
import org.apache.ignite.sql.ResultSet;
import org.apache.ignite.sql.SqlException;
import org.apache.ignite.sql.SqlRow;
import org.apache.ignite.sql.Statement;
import org.apache.ignite.sql.Statement.StatementBuilder;
import org.apache.ignite.sql.async.AsyncResultSet;
import org.apache.ignite.table.QualifiedName;
import org.apache.ignite.table.QualifiedNameHelper;
import org.apache.ignite.table.mapper.Mapper;
import org.apache.ignite.tx.Transaction;
import org.apache.ignite.tx.TransactionException;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

/**
 * Client SQL.
 */
public class ClientSql implements IgniteSql {
    private static final Mapper<SqlRow> sqlRowMapper = () -> SqlRow.class;

    private final IgniteLogger log;

    /** Channel. */
    private final ReliableChannel ch;

    /** Marshallers provider. */
    private final MarshallersProvider marshallers;

    private final boolean partitionAwarenessEnabled;
    private final Cache<PaCacheKey, PartitionMappingProvider> mappingProviderCache;
    private final Cache<Integer, ClientTable> tableCache;

    /**
     * Constructor.
     *
     * @param ch Channel.
     * @param marshallers Marshallers provider.
     * @param sqlPartitionAwarenessMetadataCacheSize Size of the cache for partition awareness-related metadata. If not positive, then 
     *      partition awareness will be disabled.
     */
    public ClientSql(
            ReliableChannel ch,
            MarshallersProvider marshallers,
            int sqlPartitionAwarenessMetadataCacheSize
    ) {
        this.ch = ch;
        this.marshallers = marshallers;
        this.log = ClientUtils.logger(ch.configuration(), ClientSql.class);

        partitionAwarenessEnabled = sqlPartitionAwarenessMetadataCacheSize > 0;

        mappingProviderCache = Caffeine.newBuilder()
                .maximumSize(sqlPartitionAwarenessMetadataCacheSize)
                .build();
        tableCache = Caffeine.newBuilder()
                .maximumSize(sqlPartitionAwarenessMetadataCacheSize)
                .build();
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
    public ResultSet<SqlRow> execute(
            @Nullable Transaction transaction,
            @Nullable CancellationToken cancellationToken,
            String query,
            @Nullable Object... arguments
    ) {
        Objects.requireNonNull(query);

        try {
            return new SyncResultSetAdapter<>(executeAsync(transaction, cancellationToken, query, arguments).join());
        } catch (CompletionException e) {
            throw sneakyThrow(ExceptionUtils.copyExceptionWithCause(e));
        }
    }

    /** {@inheritDoc} */
    @Override
    public ResultSet<SqlRow> execute(
            @Nullable Transaction transaction,
            @Nullable CancellationToken cancellationToken,
            Statement statement,
            @Nullable Object... arguments
    ) {
        Objects.requireNonNull(statement);

        try {
            return new SyncResultSetAdapter<>(executeAsync(transaction, cancellationToken, statement, arguments).join());
        } catch (CompletionException e) {
            throw sneakyThrow(ExceptionUtils.copyExceptionWithCause(e));
        }
    }

    /** {@inheritDoc} */
    @Override
    public <T> ResultSet<T> execute(
            @Nullable Transaction transaction,
            @Nullable Mapper<T> mapper,
            @Nullable CancellationToken cancellationToken,
            String query,
            @Nullable Object... arguments
    ) {
        Objects.requireNonNull(query);

        try {
            return new SyncResultSetAdapter<>(executeAsync(transaction, mapper, cancellationToken, query, arguments).join());
        } catch (CompletionException e) {
            throw sneakyThrow(ExceptionUtils.copyExceptionWithCause(e));
        }
    }

    /** {@inheritDoc} */
    @Override
    public <T> ResultSet<T> execute(
            @Nullable Transaction transaction,
            @Nullable Mapper<T> mapper,
            @Nullable CancellationToken cancellationToken,
            Statement statement,
            @Nullable Object... arguments
    ) {
        Objects.requireNonNull(statement);

        try {
            return new SyncResultSetAdapter<>(executeAsync(transaction, mapper, cancellationToken, statement, arguments).join());
        } catch (CompletionException e) {
            throw sneakyThrow(ExceptionUtils.copyExceptionWithCause(e));
        }
    }

    /** {@inheritDoc} */
    @Override
    public long[] executeBatch(
            @Nullable Transaction transaction,
            @Nullable CancellationToken cancellationToken,
            String dmlQuery,
            BatchedArguments batch
    ) {
        return executeBatch(transaction, cancellationToken, new StatementImpl(dmlQuery), batch);
    }

    /** {@inheritDoc} */
    @Override
    public long[] executeBatch(
            @Nullable Transaction transaction,
            @Nullable CancellationToken cancellationToken,
            Statement dmlStatement,
            BatchedArguments batch
    ) {
        try {
            return executeBatchAsync(transaction, cancellationToken, dmlStatement, batch).join();
        } catch (CompletionException e) {
            throw sneakyThrow(ExceptionUtils.copyExceptionWithCause(e));
        }
    }

    /** {@inheritDoc} */
    @Override
    public void executeScript(String query, @Nullable Object... arguments) {
        executeScript(null, query, arguments);
    }

    /** {@inheritDoc} */
    @Override
    public void executeScript(@Nullable CancellationToken cancellationToken, String query, @Nullable Object... arguments) {
        Objects.requireNonNull(query);

        try {
            executeScriptAsync(cancellationToken, query, arguments).join();
        } catch (CompletionException e) {
            throw sneakyThrow(ExceptionUtils.copyExceptionWithCause(e));
        }
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<AsyncResultSet<SqlRow>> executeAsync(
            @Nullable Transaction transaction,
            @Nullable CancellationToken cancellationToken,
            String query,
            @Nullable Object... arguments) {
        Objects.requireNonNull(query);

        StatementImpl statement = new StatementImpl(query);

        return executeAsync(transaction, cancellationToken, statement, arguments);
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<AsyncResultSet<SqlRow>> executeAsync(
            @Nullable Transaction transaction,
            @Nullable CancellationToken cancellationToken,
            Statement statement,
            @Nullable Object... arguments) {
        return executeAsync(transaction, sqlRowMapper, cancellationToken, statement, arguments);
    }

    /** {@inheritDoc} */
    @Override
    public <T> CompletableFuture<AsyncResultSet<T>> executeAsync(
            @Nullable Transaction transaction,
            @Nullable Mapper<T> mapper,
            @Nullable CancellationToken cancellationToken,
            String query,
            @Nullable Object... arguments) {
        Objects.requireNonNull(query);

        StatementImpl statement = new StatementImpl(query);

        return executeAsync(transaction, mapper, cancellationToken, statement, arguments);
    }

    /** {@inheritDoc} */
    @Override
    public <T> CompletableFuture<AsyncResultSet<T>> executeAsync(
            @Nullable Transaction transaction,
            @Nullable Mapper<T> mapper,
            @Nullable CancellationToken cancellationToken,
            Statement statement,
            @Nullable Object... arguments) {
        return executeAsyncInternal(
                transaction,
                mapper,
                cancellationToken,
                QueryModifier.SINGLE_STMT_MODIFIERS,
                statement,
                arguments
        );
    }

    /**
     * Executes SQL statement in an asynchronous way.
     *
     * <p>Note: This method isn't part of the public API, it is used to execute only specific types of queries.
     *
     * @param transaction Transaction to execute the statement within or {@code null}.
     * @param cancellationToken Cancellation token or {@code null}.
     * @param mapper Mapper that defines the row type and the way to map columns to the type members. See {@link Mapper#of}.
     * @param statement SQL statement to execute.
     * @param queryModifiers Query modifiers.
     * @param arguments Arguments for the statement.
     * @param <T> A type of object contained in result set.
     * @return Operation future.
     */
    public <T> CompletableFuture<AsyncResultSet<T>> executeAsyncInternal(
            @Nullable Transaction transaction,
            @Nullable Mapper<T> mapper,
            @Nullable CancellationToken cancellationToken,
            Set<QueryModifier> queryModifiers,
            Statement statement,
            @Nullable Object... arguments
    ) {
        assert mapper == null || mapper.targetType() == SqlRow.class
                || !queryModifiers.contains(QueryModifier.ALLOW_MULTISTATEMENT) : "Mapper is not supported for multi-statements.";

        Objects.requireNonNull(statement);

        PartitionMappingProvider mappingProvider = mappingProviderCache.getIfPresent(new PaCacheKey(statement));

        PartitionMapping mapping = resolveMapping(transaction, mappingProvider, arguments);

        // Write context carries request execution details over async chain.
        WriteContext ctx = new WriteContext(ch.observableTimestamp(), ClientOp.SQL_EXEC);

        boolean directTxSupported = mappingProvider != null
                && (mappingProvider.directTxMode() == ClientDirectTxMode.SUPPORTED
                || mappingProvider.directTxMode() == ClientDirectTxMode.SUPPORTED_TRACKING_REQUIRED);

        boolean shouldTrackOperation = directTxSupported
                && mappingProvider.directTxMode() == ClientDirectTxMode.SUPPORTED_TRACKING_REQUIRED;

        CompletableFuture<@Nullable ClientTransaction> txStartFut = DirectTxUtils.ensureStarted(
                ch, transaction, mapping, ctx, ch -> {
                    boolean supports = directTxSupported && mapping != null
                            // Enough to check only SQL_DIRECT_TX_MAPPING flag - other tx flags are set if this flag is set.
                            && ch.protocolContext().isFeatureSupported(SQL_DIRECT_TX_MAPPING)
                            && ch.protocolContext().clusterNode().name().equals(mapping.nodeConsistentId());

                    assert !supports || ch.protocolContext().allFeaturesSupported(TX_DIRECT_MAPPING, TX_DELAYED_ACKS, TX_PIGGYBACK);

                    return supports;
                }
        );

        return txStartFut.thenCompose(tx -> ch.serviceAsync(
                ClientOp.SQL_EXEC,
                payloadWriter(ctx, transaction, cancellationToken, queryModifiers, statement, arguments, shouldTrackOperation),
                payloadReader(ctx, mapper, tx, statement),
                () -> DirectTxUtils.resolveChannel(ctx, ch, shouldTrackOperation, tx, mapping),
                null,
                false
        ).handle((BiFunction<AsyncResultSet<T>, Throwable, CompletableFuture<AsyncResultSet<T>>>) (r, err) -> {
            if (err != null) {
                if (tx == null) {
                    throw sneakyThrow(err);
                }

                if (ctx.enlistmentToken != null) {
                    // In case of direct mapping error we need to rollback the tx on coordinator.
                    return tx.rollbackAsync().handle((ignored, err0) -> {
                        if (err0 != null) {
                            err.addSuppressed(err0);
                        }

                        throw sneakyThrow(err);
                    });
                } else {
                    // In case of unrecoverable error the tx is already rolled back on coordinator.
                    // We need to additionally cleanup directly mapped parts.
                    return tx.discardDirectMappings(false).handle((ignored, err0) -> {
                        if (err0 != null) {
                            err.addSuppressed(err0);
                        }

                        throw sneakyThrow(err);
                    });
                }
            }

            return completedFuture(r);
        })).thenCompose(identity()).exceptionally(ClientSql::handleException);
    }

    private static @Nullable PartitionMapping resolveMapping(
            @Nullable Transaction transaction,
            @Nullable PartitionMappingProvider provider,
            @Nullable Object... arguments
    ) {
        if (provider == null) {
            // Nothing to resolve.
            return null;
        }

        if (explicitRw(transaction) && provider.directTxMode() == ClientDirectTxMode.NOT_SUPPORTED) {
            // Current statement doesn't support direct transactions which is part of the explicit RW transactions handling.
            return null;
        }

        return provider.get(arguments);
    }

    private static boolean explicitRw(@Nullable Transaction transaction) {
        return transaction != null && !transaction.isReadOnly();
    }

    private <T> PayloadReader<AsyncResultSet<T>> payloadReader(
            WriteContext ctx,
            @Nullable Mapper<T> mapper,
            @Nullable ClientTransaction tx,
            Statement statement
    ) {
        return r -> {
            boolean tryUnpackPaMeta = partitionAwarenessEnabled 
                    && r.clientChannel().protocolContext().isFeatureSupported(SQL_PARTITION_AWARENESS);

            boolean sqlDirectMappingSupported = r.clientChannel().protocolContext().isFeatureSupported(SQL_DIRECT_TX_MAPPING);
            boolean sqlMultistatementsSupported = r.clientChannel().protocolContext().allFeaturesSupported(SQL_MULTISTATEMENT_SUPPORT);

            DirectTxUtils.readTx(r, ch, ctx, tx, ch.observableTimestamp());
            ClientAsyncResultSet<T> rs = new ClientAsyncResultSet<>(
                    r.clientChannel(), marshallers, r.in(), mapper, tryUnpackPaMeta, sqlDirectMappingSupported, sqlMultistatementsSupported
            );

            ClientPartitionAwarenessMetadata partitionAwarenessMetadata = rs.partitionAwarenessMetadata();

            if (partitionAwarenessEnabled && partitionAwarenessMetadata != null) {
                int tableId = partitionAwarenessMetadata.tableId();

                // The table being created is fake and used only to reuse code to derive table's schema and partition assignment.
                // Yet the name of the table may appear in error messages and/or logs, therefore let's put some meaning
                // in the fake name.
                QualifiedName tableName = QualifiedNameHelper.fromNormalized("DUMMY", String.valueOf(tableId));

                ClientTable table = tableCache.get(tableId, id -> new ClientTable(
                        ch,
                        marshallers,
                        tableId,
                        tableName,
                        0
                ));

                assert table != null;

                PaCacheKey key = new PaCacheKey(statement);
                mappingProviderCache.put(
                        key,
                        PartitionMappingProvider.create(
                                table,
                                partitionAwarenessMetadata,
                                th -> {
                                    log.debug("Unable to derive node for partition-aware query.", th);

                                    if (th instanceof TableNotFoundException) {
                                        tableCache.invalidate(tableId);
                                    }
                                    mappingProviderCache.invalidate(key);
                                }
                        )
                );
            }

            return rs;
        };
    }

    private PayloadWriter payloadWriter(
            WriteContext ctx,
            @Nullable Transaction transaction,
            @Nullable CancellationToken cancellationToken,
            Set<QueryModifier> queryModifiers,
            Statement statement,
            @Nullable Object[] arguments,
            boolean requestAck
    ) {
        return w -> {
            if (w.clientChannel().protocolContext().isFeatureSupported(SQL_DIRECT_TX_MAPPING)) {
                w.out().packBoolean(requestAck);
            }

            DirectTxUtils.writeTx(transaction, w, ctx);

            w.out().packString(statement.defaultSchema());
            w.out().packInt(statement.pageSize());
            w.out().packLong(statement.queryTimeout(TimeUnit.MILLISECONDS));

            w.out().packLongNullable(0L); // defaultSessionTimeout
            w.out().packString(statement.timeZoneId().getId());

            packProperties(w, null);

            if (w.clientChannel().protocolContext().isFeatureSupported(SQL_MULTISTATEMENT_SUPPORT)) {
                w.out().packByte(QueryModifier.pack(queryModifiers));
            }

            w.out().packString(statement.query());

            w.out().packObjectArrayAsBinaryTuple(arguments);

            w.out().packLong(ch.observableTimestamp().get().longValue());

            if (w.clientChannel().protocolContext().isFeatureSupported(SQL_PARTITION_AWARENESS)) {
                // Let's always request PA metadata from server, if enabled. Later we might introduce some throttling.
                w.out().packBoolean(partitionAwarenessEnabled);
            }

            if (cancellationToken != null) {
                addCancelAction(cancellationToken, w);
            }
        };
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<long[]> executeBatchAsync(
            @Nullable Transaction transaction,
            @Nullable CancellationToken cancellationToken,
            String query,
            BatchedArguments batch
    ) {
        return executeBatchAsync(transaction, cancellationToken, new StatementImpl(query), batch);
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<long[]> executeBatchAsync(
            @Nullable Transaction transaction,
            @Nullable CancellationToken cancellationToken,
            Statement statement,
            BatchedArguments batch
    ) {
        PayloadWriter payloadWriter = w -> {
            DirectTxUtils.writeTx(transaction, w, null);

            w.out().packString(statement.defaultSchema());
            w.out().packInt(statement.pageSize());
            w.out().packLong(statement.queryTimeout(TimeUnit.MILLISECONDS));
            w.out().packNil(); // sessionTimeout
            w.out().packString(statement.timeZoneId().getId());

            packProperties(w, null);

            w.out().packString(statement.query());
            w.out().packBatchedArgumentsAsBinaryTupleArray(batch);
            w.out().packLong(ch.observableTimestamp().get().longValue());

            if (cancellationToken != null) {
                addCancelAction(cancellationToken, w);
            }
        };

        PayloadReader<long[]> payloadReader = r -> {
            ClientMessageUnpacker unpacker = r.in();

            // skipping currently unused values:
            // 1. resourceId
            // 2. row set flag
            // 3. more pages flag
            // 4. was applied flag
            unpacker.skipValues(4);

            return unpacker.unpackLongArray(); // Update counters.
        };

        if (transaction != null) {
            try {
                //noinspection resource
                return ClientLazyTransaction.ensureStarted(transaction, ch).get1()
                        .thenCompose(tx -> tx.channel().serviceAsync(ClientOp.SQL_EXEC_BATCH, payloadWriter, payloadReader))
                        .exceptionally(ClientSql::handleException);
            } catch (TransactionException e) {
                return CompletableFuture.failedFuture(new SqlException(e.traceId(), e.code(), e.getMessage(), e));
            }
        }

        return ch.serviceAsync(ClientOp.SQL_EXEC_BATCH, payloadWriter, payloadReader);
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Void> executeScriptAsync(String query, @Nullable Object... arguments) {
        return executeScriptAsync(null, query, arguments);
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Void> executeScriptAsync(@Nullable CancellationToken cancellationToken, String query,
            @Nullable Object... arguments) {
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
            w.out().packLong(ch.observableTimestamp().get().longValue());

            if (cancellationToken != null) {
                addCancelAction(cancellationToken, w);
            }
        };

        return ch.serviceAsync(ClientOp.SQL_EXEC_SCRIPT, payloadWriter, null);
    }

    private static void addCancelAction(CancellationToken cancellationToken, PayloadOutputChannel ch) {
        CompletableFuture<Void> cancelFuture = new CompletableFuture<>();

        if (CancelHandleHelper.isCancelled(cancellationToken)) {
            throw new SqlException(Sql.EXECUTION_CANCELLED_ERR, "The query was cancelled while executing.");
        }

        long correlationToken = ch.requestId();

        Runnable cancelAction = () -> ch.clientChannel()
                .serviceAsync(ClientOp.OPERATION_CANCEL, w -> w.out().packLong(correlationToken), null)
                .whenComplete((r, e) -> {
                    if (e != null) {
                        cancelFuture.completeExceptionally(e);
                    } else {
                        cancelFuture.complete(null);
                    }
                });

        ch.onSent(() -> CancelHandleHelper.addCancelAction(cancellationToken, cancelAction, cancelFuture));
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

        throw sneakyThrow(ex);
    }

    private static class PaCacheKey {
        private final String defaultSchema;
        private final String query;
        private final int hash;

        private PaCacheKey(Statement statement) {
            this(statement.defaultSchema(), statement.query());
        }

        private PaCacheKey(String defaultSchema, String query) {
            this.defaultSchema = defaultSchema;
            this.query = query;
            this.hash = Objects.hash(defaultSchema, query);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }

            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            PaCacheKey that = (PaCacheKey) o;
            return hash == that.hash
                    && Objects.equals(query, that.query) 
                    && Objects.equals(defaultSchema, that.defaultSchema);
        }

        @Override
        public int hashCode() {
            return hash;
        }
    }

    @TestOnly
    public List<PartitionMappingProvider> partitionAwarenessCachedMetas() {
        return List.copyOf(mappingProviderCache.asMap().values());
    }
}
