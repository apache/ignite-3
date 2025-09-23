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

package org.apache.ignite.internal.sql.engine.rel;

import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.ignite.internal.sql.engine.rel.explain.IgniteRelWriter;
import org.apache.ignite.internal.sql.engine.schema.IgniteTable;
import org.apache.ignite.internal.sql.engine.util.Commons;
import org.jetbrains.annotations.Nullable;

/**
 * Relational operator that represents DML operation (such as INSERT, UPDATE, DELETE, etc.).
 */
public class IgniteTableModify extends TableModify implements SourceAwareIgniteRel {
    private static final String REL_TYPE_NAME = "TableModify";

    private final long sourceId;

    /**
     * Creates a {@code TableModify}.
     *
     * <p>The UPDATE operation has format like this:
     * <blockquote>
     * <pre>UPDATE table SET iden1 = exp1, ident2 = exp2  WHERE condition</pre>
     * </blockquote>
     *
     * @param cluster              Cluster this relational expression belongs to.
     * @param traitSet             Traits of this relational expression.
     * @param table                Target table to modify.
     * @param input                Sub-query or filter condition.
     * @param operation            Modify operation (INSERT, UPDATE, DELETE, MERGE).
     * @param updateColumnList     List of column identifiers to be updated (e.g. ident1, ident2); null if not UPDATE.
     * @param sourceExpressionList List of value expressions to be set (e.g. exp1, exp2); null if not UPDATE.
     * @param flattened            Whether set flattens the input row type.
     */
    public IgniteTableModify(
            RelOptCluster cluster,
            RelTraitSet traitSet,
            RelOptTable table,
            RelNode input,
            Operation operation,
            List<String> updateColumnList,
            List<RexNode> sourceExpressionList,
            boolean flattened
    ) {
        this(-1, cluster, traitSet, table, input, operation, updateColumnList,
                sourceExpressionList, flattened);
    }

    /**
     * Creates a {@code TableModify}.
     *
     * <p>The UPDATE operation has format like this:
     * <blockquote>
     * <pre>UPDATE table SET iden1 = exp1, ident2 = exp2  WHERE condition</pre>
     * </blockquote>
     *
     * @param sourceId             Source Id.
     * @param cluster              Cluster this relational expression belongs to.
     * @param traitSet             Traits of this relational expression.
     * @param table                Target table to modify.
     * @param input                Sub-query or filter condition.
     * @param operation            Modify operation (INSERT, UPDATE, DELETE, MERGE).
     * @param updateColumnList     List of column identifiers to be updated (e.g. ident1, ident2); null if not UPDATE.
     * @param sourceExpressionList List of value expressions to be set (e.g. exp1, exp2); null if not UPDATE.
     * @param flattened            Whether set flattens the input row type.
     */
    public IgniteTableModify(
            long sourceId,
            RelOptCluster cluster,
            RelTraitSet traitSet,
            RelOptTable table,
            RelNode input,
            Operation operation,
            @Nullable List<String> updateColumnList,
            @Nullable List<RexNode> sourceExpressionList,
            boolean flattened
    ) {
        super(cluster, traitSet, table, Commons.context(cluster).catalogReader(), input, operation, updateColumnList,
                sourceExpressionList, flattened);
        this.sourceId = sourceId;
    }

    /**
     * Creates a {@code TableModify} from serialized {@link RelInput input}.
     *
     * @param input The input to create node from.
     */
    public IgniteTableModify(RelInput input) {
        this(
                input.get("sourceId") != null ? ((Number) input.get("sourceId")).longValue() : -1,
                input.getCluster(),
                input.getTraitSet().replace(IgniteConvention.INSTANCE),
                input.getTable("table"),
                input.getInput(),
                input.getEnum("operation", Operation.class),
                input.getStringList("updateColumnList"),
                input.get("sourceExpressionList") != null ? input.getExpressionList("sourceExpressionList") : null,
                input.getBoolean("flattened", true)
        );
    }

    /** {@inheritDoc} */
    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new IgniteTableModify(
                sourceId,
                getCluster(),
                traitSet,
                getTable(),
                sole(inputs),
                getOperation(),
                getUpdateColumnList(),
                getSourceExpressionList(),
                isFlattened()
        );
    }

    /** {@inheritDoc} */
    @Override
    public <T> T accept(IgniteRelVisitor<T> visitor) {
        return visitor.visit(this);
    }

    /** {@inheritDoc} */
    @Override
    public RelNode accept(RexShuttle shuttle) {
        List<RexNode> sourceExprList = getSourceExpressionList();
        List<RexNode> newSourceExprList = shuttle.apply(sourceExprList);

        if (newSourceExprList == sourceExprList) {
            return this;
        }

        return new IgniteTableModify(
                sourceId,
                getCluster(),
                traitSet,
                getTable(),
                input,
                getOperation(),
                getUpdateColumnList(),
                newSourceExprList,
                isFlattened()
        );
    }

    /** {@inheritDoc} */
    @Override
    public IgniteRel clone(long sourceId) {
        return new IgniteTableModify(
                sourceId,
                getCluster(),
                traitSet,
                getTable(),
                input,
                getOperation(),
                getUpdateColumnList(),
                getSourceExpressionList(),
                isFlattened()
        );
    }

    /** {@inheritDoc} */
    @Override
    public IgniteRel clone(RelOptCluster cluster, List<IgniteRel> inputs) {
        return new IgniteTableModify(sourceId, cluster, getTraitSet(), getTable(), sole(inputs),
                getOperation(), getUpdateColumnList(), getSourceExpressionList(), isFlattened());
    }

    /** {@inheritDoc} */
    @Override
    public RelWriter explainTerms(RelWriter pw) {
        // for correct rel obtaining from ExecutionServiceImpl#physNodesCache.
        return super.explainTerms(pw)
                .item("tableId", Integer.toString(getTable().unwrap(IgniteTable.class).id()))
                .itemIf("sourceId", sourceId, sourceId != -1);

    }

    /** {@inheritDoc} */
    @Override
    public long sourceId() {
        return sourceId;
    }

    /** {@inheritDoc} */
    @Override
    public String getRelTypeName() {
        return REL_TYPE_NAME;
    }

    @Override
    public IgniteRelWriter explain(IgniteRelWriter writer) {
        return writer
                .addTable(table)
                .addModifyOperationType(getOperation());
    }
}
