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

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.calcite.rex.RexNode;
import org.apache.ignite.internal.sql.engine.InternalSqlRow;
import org.apache.ignite.internal.sql.engine.InternalSqlRowSingleLong;
import org.apache.ignite.internal.sql.engine.SqlQueryType;
import org.apache.ignite.internal.sql.engine.exec.AsyncDataCursor;
import org.apache.ignite.internal.sql.engine.exec.ExecutablePlan;
import org.apache.ignite.internal.sql.engine.exec.ExecutableTable;
import org.apache.ignite.internal.sql.engine.exec.ExecutableTableRegistry;
import org.apache.ignite.internal.sql.engine.exec.ExecutionContext;
import org.apache.ignite.internal.sql.engine.exec.UpdatableTable;
import org.apache.ignite.internal.sql.engine.exec.exp.SqlRowProvider;
import org.apache.ignite.internal.sql.engine.prepare.partitionawareness.PartitionAwarenessMetadata;
import org.apache.ignite.internal.sql.engine.prepare.pruning.PartitionPruningMetadata;
import org.apache.ignite.internal.sql.engine.rel.IgniteKeyValueModify;
import org.apache.ignite.internal.sql.engine.rel.IgniteRel;
import org.apache.ignite.internal.sql.engine.rel.explain.ExplainUtils;
import org.apache.ignite.internal.sql.engine.schema.IgniteTable;
import org.apache.ignite.internal.sql.engine.util.Cloner;
import org.apache.ignite.internal.sql.engine.util.Commons;
import org.apache.ignite.internal.sql.engine.util.IteratorToDataCursorAdapter;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.lang.ErrorGroups.Common;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.sql.ResultSetMetadata;
import org.jetbrains.annotations.Nullable;

/**
 * Plan representing simple modify operation that can be executed by Key-Value API.
 */
public class KeyValueModifyPlan implements ExplainablePlan, ExecutablePlan {
    private final PlanId id;
    private final int catalogVersion;
    private final IgniteKeyValueModify modifyNode;
    private final ResultSetMetadata meta;
    private final ParameterMetadata parameterMetadata;
    @Nullable
    private final PartitionAwarenessMetadata partitionAwarenessMetadata;
    @Nullable
    private final PartitionPruningMetadata partitionPruningMetadata;

    private volatile Performable<?> operation;

    KeyValueModifyPlan(
            PlanId id,
            int catalogVersion,
            IgniteKeyValueModify modifyNode,
            ResultSetMetadata meta,
            ParameterMetadata parameterMetadata,
            @Nullable PartitionAwarenessMetadata partitionAwarenessMetadata,
            @Nullable PartitionPruningMetadata partitionPruningMetadata
    ) {
        this.id = id;
        this.catalogVersion = catalogVersion;
        this.modifyNode = modifyNode;
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
        IgniteTable table = modifyNode.getTable().unwrap(IgniteTable.class);

        assert table != null : modifyNode.getTable();

        return table;
    }

    @Override
    public String explain() {
        IgniteRel clonedRoot = Cloner.clone(modifyNode, Commons.cluster());

        return ExplainUtils.toString(clonedRoot);
    }

    private <RowT> Performable<RowT> operation(ExecutionContext<RowT> ctx, ExecutableTableRegistry tableRegistry) {
        Performable<RowT> operation = cast(this.operation);

        if (operation != null) {
            return operation;
        }

        IgniteTable sqlTable = table();
        ExecutableTable execTable = tableRegistry.getTable(catalogVersion, sqlTable.id());

        List<RexNode> expressions = modifyNode.expressions();

        SqlRowProvider<RowT> rowSupplier = ctx.expressionFactory()
                .rowSource(expressions);

        UpdatableTable table = execTable.updatableTable();

        switch (modifyNode.operation()) {
            case INSERT:
                operation = new InsertExecution<>(table, rowSupplier);
                break;
            case DELETE:
                operation = new DeleteExecution<>(table, rowSupplier);
                break;
            default:
                throw new IgniteException(Common.INTERNAL_ERR, "Unsupported operation " + modifyNode.operation());
        }

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
    public IgniteKeyValueModify getRel() {
        return modifyNode;
    }

    private abstract static class Performable<RowT> {
        abstract CompletableFuture<Iterator<InternalSqlRow>> perform(ExecutionContext<RowT> ctx, @Nullable InternalTransaction tx);
    }

    private static class InsertExecution<RowT> extends Performable<RowT> {
        private final UpdatableTable table;
        private final SqlRowProvider<RowT> rowSupplier;

        private InsertExecution(
                UpdatableTable table,
                SqlRowProvider<RowT> rowSupplier
        ) {
            this.table = table;
            this.rowSupplier = rowSupplier;
        }

        @Override
        CompletableFuture<Iterator<InternalSqlRow>> perform(ExecutionContext<RowT> ctx, InternalTransaction tx) {
            return table.insert(tx, ctx, rowSupplier.get(ctx))
                    .thenApply(none -> List.<InternalSqlRow>of(new InternalSqlRowSingleLong(1L)).iterator());
        }
    }

    private static class DeleteExecution<RowT> extends Performable<RowT> {
        private final UpdatableTable table;
        private final SqlRowProvider<RowT> rowSupplier;

        private DeleteExecution(
                UpdatableTable table,
                SqlRowProvider<RowT> rowSupplier
        ) {
            this.table = table;
            this.rowSupplier = rowSupplier;
        }

        @Override
        CompletableFuture<Iterator<InternalSqlRow>> perform(ExecutionContext<RowT> ctx, InternalTransaction tx) {
            return table.delete(tx, ctx, rowSupplier.get(ctx))
                    .thenApply(deleted -> List.<InternalSqlRow>of(new InternalSqlRowSingleLong(deleted ? 1L : 0L)).iterator());
        }
    }

    public int catalogVersion() {
        return catalogVersion;
    }
}
