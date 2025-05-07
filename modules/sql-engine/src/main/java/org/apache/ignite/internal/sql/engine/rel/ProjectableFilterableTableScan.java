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

import static org.apache.calcite.sql.validate.SqlValidatorUtil.ATTEMPT_SUGGESTER;
import static org.apache.ignite.internal.sql.engine.prepare.ExplainUtils.forExplain;
import static org.apache.ignite.internal.sql.engine.util.RexUtils.builder;
import static org.apache.ignite.internal.sql.engine.util.RexUtils.replaceLocalRefs;

import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLocalRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.util.ControlFlowException;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.mapping.Mappings;
import org.apache.ignite.internal.sql.engine.metadata.cost.IgniteCost;
import org.apache.ignite.internal.sql.engine.schema.IgniteDataSource;
import org.apache.ignite.internal.sql.engine.schema.IgniteTable;
import org.apache.ignite.internal.sql.engine.type.IgniteTypeFactory;
import org.apache.ignite.internal.sql.engine.util.Commons;
import org.apache.ignite.internal.sql.engine.util.RexUtils;
import org.jetbrains.annotations.Nullable;

/** Scan with projects and filters. */
public abstract class ProjectableFilterableTableScan extends TableScan {
    /** Filters. */
    protected final @Nullable RexNode condition;

    /** Projects. */
    protected final @Nullable List<RexNode> projects;

    protected final @Nullable List<String> names;

    /** Participating columns. */
    protected final ImmutableBitSet requiredColumns;

    protected ProjectableFilterableTableScan(
            RelOptCluster cluster,
            RelTraitSet traitSet,
            List<RelHint> hints,
            RelOptTable table,
            @Nullable List<String> names,
            @Nullable List<RexNode> projects,
            @Nullable RexNode condition,
            @Nullable ImmutableBitSet requiredColumns
    ) {
        super(cluster, traitSet, hints, table);

        this.names = names;
        this.projects = projects;
        this.condition = condition;
        this.requiredColumns = requiredColumns;
    }

    protected ProjectableFilterableTableScan(RelInput input) {
        super(input);
        condition = input.getExpression("filters");
        names = input.get("names") == null ? null : input.getStringList("names");
        projects = input.get("projects") == null ? null : input.getExpressionList("projects");
        requiredColumns = input.get("requiredColumns") == null ? null : input.getBitSet("requiredColumns");
    }

    /** Returns field names explicitly passed during object creation, if any. */
    public @Nullable List<String> fieldNames() {
        return names;
    }

    /**
     * Get projections.
     */
    public @Nullable List<RexNode> projects() {
        return projects;
    }

    /**
     * Get rex condition.
     */
    public @Nullable RexNode condition() {
        return condition;
    }

    /**
     * Get participating columns.
     */
    public ImmutableBitSet requiredColumns() {
        return requiredColumns;
    }

    /** {@inheritDoc} */
    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        assert inputs.isEmpty();

        return this;
    }

    /** {@inheritDoc} */
    @Override
    public RelWriter explainTerms(RelWriter pw) {
        return explainTerms0(pw
                .item("table", table.getQualifiedName())
                .itemIf("tableId", Integer.toString(table.unwrap(IgniteDataSource.class).id()), !forExplain(pw)));
    }

    /** {@inheritDoc} */
    @Override
    public RelNode accept(RexShuttle shuttle) {
        shuttle.apply(condition);
        shuttle.apply(projects);

        return super.accept(shuttle);
    }

    protected RelWriter explainTerms0(RelWriter pw) {
        if (condition != null) {
            pw.item("filters", pw.nest() ? condition :
                    RexUtil.expandSearch(getCluster().getRexBuilder(), null, condition));
        }

        return pw.itemIf("fields", getRowType().getFieldNames(), forExplain(pw))
                .itemIf("names", names, names != null && !forExplain(pw))
                .itemIf("projects", projects, projects != null)
                .itemIf("requiredColumns", requiredColumns, !forExplain(pw) && requiredColumns != null);
    }

    /** {@inheritDoc} */
    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        double rows = table.getRowCount();
        double cost = rows * IgniteCost.ROW_PASS_THROUGH_COST;

        if (condition != null) {
            cost += rows * IgniteCost.ROW_COMPARISON_COST;
        }

        return planner.getCostFactory().makeCost(rows, cost, 0);
    }

    /** {@inheritDoc} */
    @Override
    public double estimateRowCount(RelMetadataQuery mq) {
        return table.getRowCount() * mq.getSelectivity(this, null);
    }

    /** {@inheritDoc} */
    @Override
    public RelDataType deriveRowType() {
        RelDataTypeFactory typeFactory = Commons.typeFactory(getCluster());
        List<RexNode> projects = this.projects;

        if (projects == null && names != null) {
            // Let's create fake identity projection just to preserve field names provided explicitly.
            RelDataType type = table.unwrap(IgniteDataSource.class).getRowType(typeFactory, requiredColumns);

            projects = getCluster().getRexBuilder().identityProjects(type);
        }

        if (projects != null) {
            return RexUtil.createStructType(typeFactory, projects, names, ATTEMPT_SUGGESTER);
        } else {
            return table.unwrap(IgniteDataSource.class).getRowType(typeFactory, requiredColumns);
        }
    }

    /**
     * PushUpPredicate.
     * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
     */
    public RexNode pushUpPredicate() {
        if (condition == null || projects == null) {
            return replaceLocalRefs(condition);
        }

        IgniteTypeFactory typeFactory = Commons.typeFactory(getCluster());
        IgniteTable tbl = getTable().unwrap(IgniteTable.class);

        Mappings.TargetMapping mapping = RexUtils.inversePermutation(projects,
                tbl.getRowType(typeFactory, requiredColumns), true);

        RexShuttle shuttle = new RexShuttle() {
            @Override
            public RexNode visitLocalRef(RexLocalRef ref) {
                int targetRef = mapping.getSourceOpt(ref.getIndex());
                if (targetRef == -1) {
                    throw new ControlFlowException();
                }
                return new RexInputRef(targetRef, ref.getType());
            }
        };

        List<RexNode> conjunctions = new ArrayList<>();
        for (RexNode conjunction : RelOptUtil.conjunctions(condition)) {
            try {
                conjunctions.add(shuttle.apply(conjunction));
            } catch (ControlFlowException ignore) {
                // No-op
            }
        }

        return RexUtil.composeConjunction(builder(getCluster()), conjunctions, true);
    }
}
