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

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Function;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.sql.engine.InternalSqlRow;
import org.apache.ignite.internal.sql.engine.InternalSqlRowImpl;
import org.apache.ignite.internal.sql.engine.SchemaAwareConverter;
import org.apache.ignite.internal.sql.engine.SqlQueryType;
import org.apache.ignite.internal.sql.engine.exec.AsyncDataCursor;
import org.apache.ignite.internal.sql.engine.exec.ExecutablePlan;
import org.apache.ignite.internal.sql.engine.exec.ExecutableTable;
import org.apache.ignite.internal.sql.engine.exec.ExecutableTableRegistry;
import org.apache.ignite.internal.sql.engine.exec.ExecutionContext;
import org.apache.ignite.internal.sql.engine.exec.RowHandler;
import org.apache.ignite.internal.sql.engine.exec.exp.SqlProjection;
import org.apache.ignite.internal.sql.engine.prepare.partitionawareness.PartitionAwarenessMetadata;
import org.apache.ignite.internal.sql.engine.prepare.pruning.PartitionPruningMetadata;
import org.apache.ignite.internal.sql.engine.rel.IgniteRel;
import org.apache.ignite.internal.sql.engine.rel.IgniteSelectCount;
import org.apache.ignite.internal.sql.engine.rel.explain.ExplainUtils;
import org.apache.ignite.internal.sql.engine.schema.IgniteTable;
import org.apache.ignite.internal.sql.engine.util.Cloner;
import org.apache.ignite.internal.sql.engine.util.Commons;
import org.apache.ignite.internal.sql.engine.util.IteratorToDataCursorAdapter;
import org.apache.ignite.internal.sql.engine.util.TypeUtils;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.internal.type.NativeTypes;
import org.apache.ignite.internal.type.StructNativeType;
import org.apache.ignite.sql.ResultSetMetadata;
import org.jetbrains.annotations.Nullable;

/**
 * Plan representing a COUNT(*) query.
 */
public class SelectCountPlan implements ExplainablePlan, ExecutablePlan {

    private static final IgniteLogger LOG = Loggers.forClass(SelectCountPlan.class);

    private final PlanId id;

    private final int catalogVersion;

    private final IgniteSelectCount selectCountNode;

    private final List<RexNode> expressions;

    private final ResultSetMetadata metadata;

    private final ParameterMetadata parameterMetadata;

    SelectCountPlan(
            PlanId id,
            int catalogVersion,
            IgniteSelectCount getCount,
            ResultSetMetadata resultSetMetadata,
            ParameterMetadata parameterMetadata
    ) {
        this.id = id;
        this.selectCountNode = getCount;
        this.expressions = getCount.expressions();
        this.catalogVersion = catalogVersion;
        this.metadata = resultSetMetadata;
        this.parameterMetadata = parameterMetadata;
    }

    @Override
    public <RowT> AsyncDataCursor<InternalSqlRow> execute(ExecutionContext<RowT> ctx,
            InternalTransaction ignored, ExecutableTableRegistry tableRegistry) {
        RelOptTable optTable = selectCountNode.getTable();
        IgniteTable igniteTable = optTable.unwrap(IgniteTable.class);
        assert igniteTable != null;

        ExecutableTable execTable = tableRegistry.getTable(catalogVersion, igniteTable.id());

        CompletableFuture<Long> countFut = execTable.scannableTable().estimatedSize();

        Executor resultExecutor = task -> ctx.execute(task::run, error -> {
            LOG.error("Unexpected error", error);
        });

        CompletableFuture<Iterator<InternalSqlRow>> result = countFut.thenApplyAsync(rs -> {
            Function<Long, Iterator<InternalSqlRow>> postProcess = createResultProjection(ctx);

            return postProcess.apply(rs);
        }, resultExecutor);

        return new IteratorToDataCursorAdapter<>(result, Runnable::run);
    }

    @Override
    public String explain() {
        IgniteRel clonedRoot = Cloner.clone(selectCountNode, Commons.cluster());

        return ExplainUtils.toString(clonedRoot);
    }

    @Override
    public PlanId id() {
        return id;
    }

    @Override
    public SqlQueryType type() {
        return SqlQueryType.QUERY;
    }

    @Override
    public ResultSetMetadata metadata() {
        return metadata;
    }

    @Override
    public IgniteSelectCount getRel() {
        return selectCountNode;
    }

    @Override
    public ParameterMetadata parameterMetadata() {
        return parameterMetadata;
    }

    @Override
    public @Nullable PartitionAwarenessMetadata partitionAwarenessMetadata() {
        return null;
    }

    @Override
    public @Nullable PartitionPruningMetadata partitionPruningMetadata() {
        return null;
    }

    @Override
    public int numSources() {
        return 1;
    }

    private <RowT> Function<Long, Iterator<InternalSqlRow>> createResultProjection(ExecutionContext<RowT> ctx) {
        RelDataType getCountType = new RelDataTypeFactory.Builder(ctx.getTypeFactory())
                .add("ROWCOUNT", SqlTypeName.BIGINT)
                .build();

        RelDataType resultType = selectCountNode.getRowType();
        SqlProjection<RowT> projection = ctx.expressionFactory().project(expressions, getCountType);

        RowHandler<RowT> rowHandler = ctx.rowHandler();
        SchemaAwareConverter<Object, Object> internalTypeConverter = TypeUtils.resultTypeConverter(resultType);
        StructNativeType rowType = NativeTypes.rowBuilder()
                .addField("COUNT", NativeTypes.INT64, false)
                .build();

        return rowCount -> {
            RowT rowCountRow = ctx.rowHandler().factory(rowType)
                    .rowBuilder()
                    .addField(rowCount)
                    .build();

            RowT projectRow = projection.project(ctx, rowCountRow);

            return List.<InternalSqlRow>of(
                    new InternalSqlRowImpl<>(projectRow, rowHandler, internalTypeConverter)
            ).iterator();
        };
    }
}
