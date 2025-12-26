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

package org.apache.ignite.internal.sql.engine.prepare;

import static org.apache.ignite.internal.sql.engine.util.Commons.cast;
import static org.apache.ignite.internal.sql.engine.util.TypeUtils.convertStructuredType;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.sql.engine.InternalSqlRow;
import org.apache.ignite.internal.sql.engine.InternalSqlRowImpl;
import org.apache.ignite.internal.sql.engine.SchemaAwareConverter;
import org.apache.ignite.internal.sql.engine.SqlQueryType;
import org.apache.ignite.internal.sql.engine.api.expressions.RowFactory;
import org.apache.ignite.internal.sql.engine.exec.AsyncDataCursor;
import org.apache.ignite.internal.sql.engine.exec.ExecutablePlan;
import org.apache.ignite.internal.sql.engine.exec.ExecutableTable;
import org.apache.ignite.internal.sql.engine.exec.ExecutableTableRegistry;
import org.apache.ignite.internal.sql.engine.exec.ExecutionContext;
import org.apache.ignite.internal.sql.engine.exec.RowHandler;
import org.apache.ignite.internal.sql.engine.exec.ScannableTable;
import org.apache.ignite.internal.sql.engine.exec.exp.SqlPredicate;
import org.apache.ignite.internal.sql.engine.exec.exp.SqlProjection;
import org.apache.ignite.internal.sql.engine.exec.exp.SqlRowProvider;
import org.apache.ignite.internal.sql.engine.prepare.partitionawareness.PartitionAwarenessMetadata;
import org.apache.ignite.internal.sql.engine.prepare.pruning.PartitionPruningMetadata;
import org.apache.ignite.internal.sql.engine.rel.IgniteKeyValueGet;
import org.apache.ignite.internal.sql.engine.rel.IgniteRel;
import org.apache.ignite.internal.sql.engine.rel.explain.ExplainUtils;
import org.apache.ignite.internal.sql.engine.schema.IgniteTable;
import org.apache.ignite.internal.sql.engine.util.Cloner;
import org.apache.ignite.internal.sql.engine.util.Commons;
import org.apache.ignite.internal.sql.engine.util.IteratorToDataCursorAdapter;
import org.apache.ignite.internal.sql.engine.util.TypeUtils;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.internal.type.StructNativeType;
import org.apache.ignite.sql.ResultSetMetadata;
import org.jetbrains.annotations.Nullable;

/**
 * Plan representing single lookup by a primary key.
 */
public class KeyValueGetPlan implements ExplainablePlan, ExecutablePlan {
    private static final IgniteLogger LOG = Loggers.forClass(KeyValueGetPlan.class);

    private final PlanId id;
    private final int catalogVersion;
    private final IgniteKeyValueGet lookupNode;
    private final ResultSetMetadata meta;
    private final ParameterMetadata parameterMetadata;
    @Nullable
    private final PartitionAwarenessMetadata partitionAwarenessMetadata;
    @Nullable
    private final PartitionPruningMetadata partitionPruningMetadata;

    private volatile Performable<?> operation;

    KeyValueGetPlan(
            PlanId id,
            int catalogVersion,
            IgniteKeyValueGet lookupNode,
            ResultSetMetadata meta,
            ParameterMetadata parameterMetadata,
            @Nullable PartitionAwarenessMetadata partitionAwarenessMetadata,
            @Nullable PartitionPruningMetadata partitionPruningMetadata
    ) {
        this.id = id;
        this.catalogVersion = catalogVersion;
        this.lookupNode = lookupNode;
        this.meta = meta;
        this.parameterMetadata = parameterMetadata;
        this.partitionAwarenessMetadata = partitionAwarenessMetadata;
        this.partitionPruningMetadata = partitionPruningMetadata;
    }

    /** {@inheritDoc} */
    @Override
    public PlanId id() {
        return id;
    }

    /** {@inheritDoc} */
    @Override
    public SqlQueryType type() {
        return SqlQueryType.QUERY;
    }

    /** {@inheritDoc} */
    @Override
    public ResultSetMetadata metadata() {
        return meta;
    }

    /** {@inheritDoc} */
    @Override
    public ParameterMetadata parameterMetadata() {
        return parameterMetadata;
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable PartitionAwarenessMetadata partitionAwarenessMetadata() {
        return partitionAwarenessMetadata;
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable PartitionPruningMetadata partitionPruningMetadata() {
        return partitionPruningMetadata;
    }

    /** {@inheritDoc} */
    @Override
    public int numSources() {
        return 1;
    }

    /** Returns a table in question. */
    private IgniteTable table() {
        IgniteTable table = lookupNode.getTable().unwrap(IgniteTable.class);

        assert table != null : lookupNode.getTable();

        return table;
    }

    @Override
    public String explain() {
        IgniteRel clonedRoot = Cloner.clone(lookupNode, Commons.cluster());

        return ExplainUtils.toString(clonedRoot);
    }

    private <RowT> Performable<RowT> operation(ExecutionContext<RowT> ctx, ExecutableTableRegistry tableRegistry) {
        Performable<RowT> operation = cast(this.operation);

        if (operation != null) {
            return operation;
        }

        IgniteTable sqlTable = table();
        ExecutableTable executableTable = tableRegistry.getTable(catalogVersion, sqlTable.id());
        ScannableTable scannableTable = executableTable.scannableTable();

        ImmutableIntList requiredColumns = lookupNode.requiredColumns();
        RexNode filterExpr = lookupNode.condition();
        List<RexNode> projectionExpr = lookupNode.projects();

        RelDataType rowType = sqlTable.getRowType(Commons.typeFactory(), requiredColumns);

        SqlPredicate filter = filterExpr == null ? null : ctx.expressionFactory().predicate(filterExpr, rowType);
        SqlProjection projection = projectionExpr == null ? null : ctx.expressionFactory().project(projectionExpr, rowType);

        RowHandler<RowT> rowHandler = ctx.rowAccessor();
        StructNativeType nativeType = convertStructuredType(rowType);

        RowFactory<RowT> rowFactory = ctx.rowFactoryFactory().create(nativeType);

        List<RexNode> keyExpressions = lookupNode.keyExpressions();
        SqlRowProvider keySupplier = ctx.expressionFactory().rowSource(keyExpressions);

        RelDataType resultType = lookupNode.getRowType();
        SchemaAwareConverter<Object, Object> internalTypeConverter = TypeUtils.resultTypeConverter(resultType);

        operation = filter == null && projection == null ? new SimpleLookupExecution<>(scannableTable, rowHandler, rowFactory,
                keySupplier, requiredColumns, internalTypeConverter)
                : new FilterableProjectableLookupExecution<>(scannableTable, rowHandler, rowFactory, keySupplier,
                        filter, projection, requiredColumns, internalTypeConverter);

        this.operation = operation;

        return operation;
    }

    @Override
    public <RowT> AsyncDataCursor<InternalSqlRow> execute(
            ExecutionContext<RowT> ctx,
            InternalTransaction tx,
            ExecutableTableRegistry tableRegistry
    ) {
        Performable<RowT> operation = operation(ctx, tableRegistry);

        CompletableFuture<Iterator<InternalSqlRow>> result = operation.perform(ctx, tx);

        return new IteratorToDataCursorAdapter<>(result, Runnable::run);
    }

    @Override
    public IgniteKeyValueGet getRel() {
        return lookupNode;
    }

    private static class SimpleLookupExecution<RowT> extends Performable<RowT> {
        private final ScannableTable table;
        private final RowHandler<RowT> rowHandler;
        private final RowFactory<RowT> tableRowFactory;
        private final SqlRowProvider keySupplier;
        private final int @Nullable [] requiredColumns;
        private final SchemaAwareConverter<Object, Object> internalTypeConverter;

        private SimpleLookupExecution(
                ScannableTable table,
                RowHandler<RowT> rowHandler,
                RowFactory<RowT> tableRowFactory,
                SqlRowProvider keySupplier,
                @Nullable ImmutableIntList requiredColumns,
                SchemaAwareConverter<Object, Object> internalTypeConverter
        ) {
            this.table = table;
            this.rowHandler = rowHandler;
            this.tableRowFactory = tableRowFactory;
            this.keySupplier = keySupplier;
            this.requiredColumns = requiredColumns == null ? null : requiredColumns.toIntArray();
            this.internalTypeConverter = internalTypeConverter;
        }

        @Override
        CompletableFuture<Iterator<InternalSqlRow>> perform(ExecutionContext<RowT> ctx, InternalTransaction tx) {
            RowT key = keySupplier.get(ctx);
            return table.primaryKeyLookup(ctx, tx, tableRowFactory, key, requiredColumns).thenApply(row -> {
                if (row == null) {
                    return Collections.emptyIterator();
                }

                return List.<InternalSqlRow>of(new InternalSqlRowImpl<>(row, rowHandler, internalTypeConverter)).iterator();
            });
        }
    }

    private static class FilterableProjectableLookupExecution<RowT> extends Performable<RowT> {
        private final ScannableTable table;
        private final RowHandler<RowT> rowHandler;
        private final RowFactory<RowT> tableRowFactory;
        private final SqlRowProvider keySupplier;
        private final @Nullable SqlPredicate filter;
        private final @Nullable SqlProjection projection;
        private final int @Nullable [] requiredColumns;
        private final SchemaAwareConverter<Object, Object> internalTypeConverter;

        private FilterableProjectableLookupExecution(
                ScannableTable table,
                RowHandler<RowT> rowHandler,
                RowFactory<RowT> tableRowFactory,
                SqlRowProvider keySupplier,
                @Nullable SqlPredicate filter,
                @Nullable SqlProjection projection,
                @Nullable ImmutableIntList requiredColumns,
                SchemaAwareConverter<Object, Object> internalTypeConverter
        ) {
            this.table = table;
            this.rowHandler = rowHandler;
            this.tableRowFactory = tableRowFactory;
            this.keySupplier = keySupplier;
            this.filter = filter;
            this.projection = projection;
            this.requiredColumns = requiredColumns == null ? null : requiredColumns.toIntArray();
            this.internalTypeConverter = internalTypeConverter;
        }

        @Override
        CompletableFuture<Iterator<InternalSqlRow>> perform(ExecutionContext<RowT> ctx, InternalTransaction tx) {
            Executor executor = task -> ctx.execute(task::run, error -> {
                // this executor is used to process future chain, so any unhandled exception
                // should be wrapped with CompletionException and returned as a result, implying
                // no error handler should be called.
                // But just in case there is error in future processing pipeline let's log error
                LOG.error("Unexpected error", error);
            });

            RowT key = keySupplier.get(ctx);
            return table.primaryKeyLookup(ctx, tx, tableRowFactory, key, requiredColumns).thenApplyAsync(row -> {
                if (row == null) {
                    return Collections.emptyIterator();
                }

                if (filter != null && !filter.test(ctx, row)) {
                    return Collections.emptyIterator();
                }

                if (projection != null) {
                    row = projection.project(ctx, row);
                }

                return List.<InternalSqlRow>of(new InternalSqlRowImpl<>(row, rowHandler, internalTypeConverter)).iterator();
            }, executor);
        }
    }

    private abstract static class Performable<RowT> {
        abstract CompletableFuture<Iterator<InternalSqlRow>> perform(ExecutionContext<RowT> ctx, @Nullable InternalTransaction tx);
    }

    public int catalogVersion() {
        return catalogVersion;
    }
}
