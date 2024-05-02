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

import static org.apache.ignite.internal.sql.engine.util.TypeUtils.validateCharactersOverflow;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Supplier;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.ignite.internal.sql.engine.InternalSqlRow;
import org.apache.ignite.internal.sql.engine.InternalSqlRowSingleLong;
import org.apache.ignite.internal.sql.engine.QueryPrefetchCallback;
import org.apache.ignite.internal.sql.engine.SqlQueryType;
import org.apache.ignite.internal.sql.engine.exec.ExecutablePlan;
import org.apache.ignite.internal.sql.engine.exec.ExecutableTableRegistry;
import org.apache.ignite.internal.sql.engine.exec.ExecutionContext;
import org.apache.ignite.internal.sql.engine.exec.UpdatableTable;
import org.apache.ignite.internal.sql.engine.rel.IgniteKeyValueModify;
import org.apache.ignite.internal.sql.engine.rel.IgniteRel;
import org.apache.ignite.internal.sql.engine.schema.IgniteTable;
import org.apache.ignite.internal.sql.engine.util.Cloner;
import org.apache.ignite.internal.sql.engine.util.Commons;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.internal.util.AsyncCursor;
import org.apache.ignite.internal.util.AsyncWrapper;
import org.apache.ignite.sql.ResultSetMetadata;
import org.jetbrains.annotations.Nullable;

/**
 * Plan representing simple modify operation that can be executed by Key-Value API.
 */
public class KeyValueModifyPlan implements ExplainablePlan, ExecutablePlan {
    private final PlanId id;
    private final int schemaVersion;
    private final IgniteKeyValueModify modifyNode;
    private final ResultSetMetadata meta;
    private final ParameterMetadata parameterMetadata;

    KeyValueModifyPlan(
            PlanId id,
            int schemaVersion,
            IgniteKeyValueModify modifyNode,
            ResultSetMetadata meta,
            ParameterMetadata parameterMetadata
    ) {
        this.id = id;
        this.schemaVersion = schemaVersion;
        this.modifyNode = modifyNode;
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
        return SqlQueryType.DML;
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
        IgniteTable table = modifyNode.getTable().unwrap(IgniteTable.class);

        assert table != null : modifyNode.getTable();

        return table;
    }

    @Override
    public String explain() {
        IgniteRel clonedRoot = Cloner.clone(modifyNode, Commons.cluster());

        return RelOptUtil.toString(clonedRoot, SqlExplainLevel.ALL_ATTRIBUTES);
    }

    public IgniteKeyValueModify modifyNode() {
        return modifyNode;
    }

    @Override
    public <RowT> AsyncCursor<InternalSqlRow> execute(
            ExecutionContext<RowT> ctx,
            InternalTransaction tx,
            ExecutableTableRegistry tableRegistry,
            @Nullable QueryPrefetchCallback firstPageReadyCallback
    ) {
        IgniteTable sqlTable = table();

        CompletableFuture<Iterator<InternalSqlRow>> result = tableRegistry.getTable(schemaVersion, sqlTable.id())
                .thenCompose(execTable -> {
                    List<RexNode> expressions = modifyNode.expressions();

                    Supplier<RowT> rowSupplier = ctx.expressionFactory()
                            .rowSource(expressions);

                    UpdatableTable updatableTable = execTable.updatableTable();

                    RowT row = rowSupplier.get();

                    RelDataType rowType = table().getRowType(ctx.getTypeFactory());

                    validateCharactersOverflow(rowType, row, ctx.rowHandler());

                    return updatableTable.insert(
                            tx, ctx, rowSupplier.get()
                    ).thenApply(none -> List.<InternalSqlRow>of(new InternalSqlRowSingleLong(1L)).iterator());
                });

        if (firstPageReadyCallback != null) {
            Executor executor = task -> ctx.execute(task::run, firstPageReadyCallback::onPrefetchComplete);

            result.whenCompleteAsync((res, err) -> firstPageReadyCallback.onPrefetchComplete(err), executor);
        }

        return new AsyncWrapper<>(result, Runnable::run);
    }
}
