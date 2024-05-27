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

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.sql.engine.InternalSqlRow;
import org.apache.ignite.internal.sql.engine.InternalSqlRowImpl;
import org.apache.ignite.internal.sql.engine.QueryPrefetchCallback;
import org.apache.ignite.internal.sql.engine.SqlQueryType;
import org.apache.ignite.internal.sql.engine.exec.ExecutablePlan;
import org.apache.ignite.internal.sql.engine.exec.ExecutableTableRegistry;
import org.apache.ignite.internal.sql.engine.exec.ExecutionContext;
import org.apache.ignite.internal.sql.engine.exec.RowHandler;
import org.apache.ignite.internal.sql.engine.exec.RowHandler.RowFactory;
import org.apache.ignite.internal.sql.engine.exec.ScannableTable;
import org.apache.ignite.internal.sql.engine.exec.row.RowSchema;
import org.apache.ignite.internal.sql.engine.rel.IgniteKeyValueGet;
import org.apache.ignite.internal.sql.engine.rel.IgniteRel;
import org.apache.ignite.internal.sql.engine.schema.IgniteTable;
import org.apache.ignite.internal.sql.engine.util.Cloner;
import org.apache.ignite.internal.sql.engine.util.Commons;
import org.apache.ignite.internal.sql.engine.util.TypeUtils;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.internal.util.AsyncCursor;
import org.apache.ignite.internal.util.AsyncWrapper;
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

    KeyValueGetPlan(
            PlanId id,
            int catalogVersion,
            IgniteKeyValueGet lookupNode,
            ResultSetMetadata meta,
            ParameterMetadata parameterMetadata
    ) {
        this.id = id;
        this.catalogVersion = catalogVersion;
        this.lookupNode = lookupNode;
        this.meta = meta;
        this.parameterMetadata = parameterMetadata;
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

    /** Returns a table in question. */
    private IgniteTable table() {
        IgniteTable table = lookupNode.getTable().unwrap(IgniteTable.class);

        assert table != null : lookupNode.getTable();

        return table;
    }

    @Override
    public String explain() {
        IgniteRel clonedRoot = Cloner.clone(lookupNode, Commons.cluster());

        return RelOptUtil.toString(clonedRoot, SqlExplainLevel.ALL_ATTRIBUTES);
    }

    public IgniteKeyValueGet lookupNode() {
        return lookupNode;
    }

    @Override
    public <RowT> AsyncCursor<InternalSqlRow> execute(
            ExecutionContext<RowT> ctx,
            @Nullable InternalTransaction tx,
            ExecutableTableRegistry tableRegistry,
            @Nullable QueryPrefetchCallback firstPageReadyCallback
    ) {
        IgniteTable sqlTable = table();

        CompletableFuture<Iterator<InternalSqlRow>> result = tableRegistry.getTable(catalogVersion, sqlTable.id())
                .thenCompose(execTable -> {

                    ImmutableBitSet requiredColumns = lookupNode.requiredColumns();
                    RexNode filterExpr = lookupNode.condition();
                    List<RexNode> projectionExpr = lookupNode.projects();
                    List<RexNode> keyExpressions = lookupNode.keyExpressions();

                    RelDataType rowType = sqlTable.getRowType(Commons.typeFactory(), requiredColumns);

                    Supplier<RowT> keySupplier = ctx.expressionFactory()
                            .rowSource(keyExpressions);
                    Predicate<RowT> filter = filterExpr == null ? null : ctx.expressionFactory()
                            .predicate(filterExpr, rowType);
                    Function<RowT, RowT> projection = projectionExpr == null ? null : ctx.expressionFactory()
                            .project(projectionExpr, rowType);

                    RowHandler<RowT> rowHandler = ctx.rowHandler();
                    RowSchema rowSchema = TypeUtils.rowSchemaFromRelTypes(RelOptUtil.getFieldTypeList(rowType));
                    RowFactory<RowT> rowFactory = rowHandler.factory(rowSchema);

                    RelDataType resultType = lookupNode.getRowType();
                    BiFunction<Integer, Object, Object> internalTypeConverter = TypeUtils.resultTypeConverter(ctx, resultType);

                    ScannableTable scannableTable = execTable.scannableTable();
                    Function<RowT, Iterator<InternalSqlRow>> postProcess = row -> {
                        if (row == null) {
                            return Collections.emptyIterator();
                        }

                        if (filter != null && !filter.test(row)) {
                            return Collections.emptyIterator();
                        }

                        if (projection != null) {
                            row = projection.apply(row);
                        }

                        return List.<InternalSqlRow>of(
                                new InternalSqlRowImpl<>(row, rowHandler, internalTypeConverter)
                        ).iterator();
                    };

                    CompletableFuture<RowT> lookupResult = scannableTable.primaryKeyLookup(
                            ctx, tx, rowFactory, keySupplier.get(), requiredColumns.toBitSet()
                    );

                    if (projection == null && filter == null) {
                        // no arbitrary computations, should be safe to proceed execution on
                        // thread that completes the future
                        return lookupResult.thenApply(postProcess);
                    } else {
                        Executor executor = task -> ctx.execute(task::run, error -> {
                            // this executor is used to process future chain, so any unhandled exception
                            // should be wrapped with CompletionException and returned as a result, implying
                            // no error handler should be called.
                            // But just in case there is error in future processing pipeline let's log error
                            LOG.error("Unexpected error", error);
                        });

                        return lookupResult.thenApplyAsync(postProcess, executor);
                    }
                });

        if (firstPageReadyCallback != null) {
            Executor executor = task -> ctx.execute(task::run, firstPageReadyCallback::onPrefetchComplete);

            result.whenCompleteAsync((res, err) -> firstPageReadyCallback.onPrefetchComplete(err), executor);
        }

        return new AsyncWrapper<>(result, Runnable::run);
    }

    public int catalogVersion() {
        return catalogVersion;
    }
}
