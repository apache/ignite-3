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

package org.apache.ignite.internal.table;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.function.Function.identity;
import static org.apache.ignite.internal.lang.IgniteExceptionMapperUtil.convertToPublicFuture;
import static org.apache.ignite.internal.table.criteria.CriteriaExceptionMapperUtil.mapToPublicCriteriaException;
import static org.apache.ignite.internal.util.ExceptionUtils.isOrCausedBy;
import static org.apache.ignite.internal.util.ExceptionUtils.unwrapCause;
import static org.apache.ignite.internal.util.ViewUtils.sync;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.ignite.internal.lang.IgniteExceptionMapperUtil;
import org.apache.ignite.internal.marshaller.MarshallersProvider;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.SchemaRegistry;
import org.apache.ignite.internal.table.criteria.CursorAdapter;
import org.apache.ignite.internal.table.criteria.QueryCriteriaAsyncCursor;
import org.apache.ignite.internal.table.criteria.SqlSerializer;
import org.apache.ignite.internal.table.criteria.SqlSerializer.Builder;
import org.apache.ignite.internal.table.distributed.replicator.IncompatibleSchemaVersionException;
import org.apache.ignite.internal.table.distributed.replicator.InternalSchemaVersionMismatchException;
import org.apache.ignite.internal.table.distributed.schema.SchemaVersions;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.lang.AsyncCursor;
import org.apache.ignite.lang.Cursor;
import org.apache.ignite.sql.IgniteSql;
import org.apache.ignite.sql.ResultSetMetadata;
import org.apache.ignite.sql.SqlRow;
import org.apache.ignite.sql.Statement;
import org.apache.ignite.table.criteria.Criteria;
import org.apache.ignite.table.criteria.CriteriaQueryOptions;
import org.apache.ignite.table.criteria.CriteriaQuerySource;
import org.apache.ignite.tx.Transaction;
import org.jetbrains.annotations.Nullable;

/**
 * Base class for Table views.
 */
abstract class AbstractTableView<R> implements CriteriaQuerySource<R> {
    /** Internal table. */
    protected final InternalTable tbl;

    private final SchemaVersions schemaVersions;

    /** Table row view converter. */
    protected final TableViewRowConverter rowConverter;

    /** Ignite SQL facade. */
    protected final IgniteSql sql;

    /** Marshallers provider. */
    protected final MarshallersProvider marshallers;

    /**
     * Constructor.
     *
     * @param tbl Internal table.
     * @param schemaVersions Schema versions access.
     * @param schemaReg Schema registry.
     * @param sql Ignite SQL facade.
     * @param marshallers Marshallers provider.
     */
    AbstractTableView(
            InternalTable tbl,
            SchemaVersions schemaVersions,
            SchemaRegistry schemaReg,
            IgniteSql sql,
            MarshallersProvider marshallers
    ) {
        this.tbl = tbl;
        this.schemaVersions = schemaVersions;
        this.sql = sql;
        this.marshallers = marshallers;

        this.rowConverter = new TableViewRowConverter(schemaReg);
    }

    /**
     * Combines the effect of {@link #withSchemaSync(Transaction, KvAction)} and
     * {@link IgniteExceptionMapperUtil#convertToPublicFuture(CompletableFuture)}.
     *
     * @param <T> Type of the data the action returns.
     * @param tx Transaction or {@code null}.
     * @param action Action to execute.
     * @return Future of whatever the action returns.
     */
    protected final <T> CompletableFuture<T> doOperation(@Nullable Transaction tx, KvAction<T> action) {
        return convertToPublicFuture(withSchemaSync(tx, action));
    }

    /**
     * Executes the provided KV action in the given transaction, maintaining Schema Synchronization semantics: that is, before executing
     * the action, a check is made to make sure that the current node has schemas complete wrt timestamp corresponding to the operation
     * (if not, a wait is made till this condition is satisfied) and then the action is provided with the table schema version corresponding
     * to the operation.
     *
     * <p>If a transaction is given, the operation timestamp will be equal to the transaction timestamp; otherwise, 'now' will
     * be used.
     *
     * <p>If no transaction is provided, it might happen that schema version obtained at 'now' is different than a schema version
     * obtained when processing the implicit transaction (that will be created further). If this happens, we'll get an exception
     * saying that we need to make a retry; it will be handled by this method.
     *
     * @param <T> Type of the data the action returns.
     * @param tx Transaction or {@code null}.
     * @param action Action to execute.
     * @return Whatever the action returns.
     */
    protected final <T> CompletableFuture<T> withSchemaSync(@Nullable Transaction tx, KvAction<T> action) {
        return withSchemaSync(tx, null, action);
    }

    private <T> CompletableFuture<T> withSchemaSync(@Nullable Transaction tx, @Nullable Integer previousSchemaVersion, KvAction<T> action) {
        CompletableFuture<Integer> schemaVersionFuture = tx == null
                ? schemaVersions.schemaVersionAtNow(tbl.tableId())
                : schemaVersions.schemaVersionAt(((InternalTransaction) tx).startTimestamp(), tbl.tableId());

        return schemaVersionFuture
                .thenCompose(schemaVersion -> action.act(schemaVersion)
                        .handle((res, ex) -> {
                            if (ex == null) {
                                return completedFuture(res);
                            }

                            if (isOrCausedBy(InternalSchemaVersionMismatchException.class, ex)) {
                                // There is no transaction, and table version was changed between taking the table version (that was used
                                // to marshal inputs and would be used to unmarshal outputs) and starting an implicit transaction
                                // in InternalTable. A transaction must always work with binary rows of the same table version matching the
                                // version corresponding to the transaction creation moment, so this mismatch is not tolerable: we need
                                // to retry the operation here.

                                assert tx == null : "Only for implicit transactions a retry might be requested";
                                assertSchemaVersionIncreased(previousSchemaVersion, schemaVersion);

                                // Repeat.
                                return withSchemaSync(tx, schemaVersion, action);
                            } else if (tx == null && isOrCausedBy(IncompatibleSchemaVersionException.class, ex)) {
                                // Table version was changed while we were executing an implicit transaction (between it had been created
                                // and the moment when the operation actually touched the partition), let's retry.
                                assertSchemaVersionIncreased(previousSchemaVersion, schemaVersion);

                                return withSchemaSync(tx, schemaVersion, action);
                            } else {
                                return CompletableFuture.<T>failedFuture(ex);
                            }
                        }))
                .thenCompose(identity());
    }

    private static void assertSchemaVersionIncreased(@Nullable Integer previousSchemaVersion, Integer schemaVersion) {
        assert previousSchemaVersion == null || !Objects.equals(schemaVersion, previousSchemaVersion)
                : "Same schema version (" + schemaVersion
                + ") on a retry: something is wrong, is this caused by the test setup?";
    }

    /**
     * Map columns to it's names.
     *
     * @param columns Target columns.
     * @return Column names.
     */
    protected static String[] columnNames(List<Column> columns) {
        String[] columnNames = new String[columns.size()];

        for (int i = 0; i < columns.size(); i++) {
            columnNames[i] = columns.get(i).name();
        }

        return columnNames;
    }

    /**
     * Create conversion function for objects contained by result set to criteria query objects.
     *
     * @param meta Result set columns' metadata.
     * @param schema Schema.
     * @return Conversion function (if {@code null} conversions isn't required).
     */
    protected @Nullable Function<SqlRow, R> queryMapper(ResultSetMetadata meta, SchemaDescriptor schema) {
        return null;
    }

    /** {@inheritDoc} */
    @Override
    public Cursor<R> query(@Nullable Transaction tx, @Nullable Criteria criteria, @Nullable String indexName, CriteriaQueryOptions opts) {
        return new CursorAdapter<>(sync(queryAsync(tx, criteria, indexName, opts)));
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<AsyncCursor<R>> queryAsync(
            @Nullable Transaction tx,
            @Nullable Criteria criteria,
            @Nullable String indexName,
            @Nullable CriteriaQueryOptions opts
    ) {
        CriteriaQueryOptions opts0 = opts == null ? CriteriaQueryOptions.DEFAULT : opts;

        CompletableFuture<AsyncCursor<R>> future = doOperation(tx, (schemaVersion) -> {
            SchemaDescriptor schema = rowConverter.registry().schema(schemaVersion);

            SqlSerializer ser = new Builder()
                    .tableName(tbl.name())
                    .columns(
                            schema.columns().stream()
                                    .map(Column::name)
                                    .collect(Collectors.toList())
                    )
                    .indexName(indexName)
                    .where(criteria)
                    .build();

            Statement statement = sql.statementBuilder().query(ser.toString()).pageSize(opts0.pageSize()).build();

            return sql.executeAsync(tx, statement, ser.getArguments())
                    .thenApply(resultSet -> {
                        ResultSetMetadata meta = resultSet.metadata();

                        assert meta != null : "Metadata can't be null.";

                        return new QueryCriteriaAsyncCursor<>(
                                resultSet,
                                queryMapper(meta, schema),
                                () -> {/* NO-OP */}
                        );
                    });
        });

        return future.exceptionally(th -> {
            throw new CompletionException(mapToPublicCriteriaException(unwrapCause(th)));
        });
    }

    /**
     * Action representing some KV operation. When executed, the action is supplied with schema version corresponding
     * to the operation timestamp (see {@link #doOperation(Transaction, KvAction)} for details).
     *
     * @param <R> Type of the result.
     * @see #doOperation(Transaction, KvAction)
     */
    @FunctionalInterface
    protected interface KvAction<R> {
        /**
         * Executes the action.
         *
         * @param schemaVersion Schema version corresponding to the operation.
         * @return Action result.
         */
        CompletableFuture<R> act(int schemaVersion);
    }
}
