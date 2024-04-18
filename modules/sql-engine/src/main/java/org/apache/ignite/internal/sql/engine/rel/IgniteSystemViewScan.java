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

import static org.apache.ignite.internal.sql.engine.trait.TraitUtils.changeTraits;

import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.ImmutableBitSet;
import org.jetbrains.annotations.Nullable;

/**
 * Relational operator that returns the contents of a system view.
 */
public class IgniteSystemViewScan extends ProjectableFilterableTableScan implements SourceAwareIgniteRel {
    private static final String REL_TYPE_NAME = "SystemViewScan";

    private final long sourceId;

    /**
     * Constructor.
     *
     * @param cluster Cluster this relational expression belongs to.
     * @param traitSet Traits of this relational expression.
     * @param hints Optimizer hints.
     * @param projections Projection list.
     * @param condition Filter condition.
     * @param table Target system view to read data from.
     * @param reqColumns Participating columns.
     */
    public IgniteSystemViewScan(RelOptCluster cluster, RelTraitSet traitSet,
            List<RelHint> hints,
            RelOptTable table,
            @Nullable List<RexNode> projections,
            @Nullable RexNode condition,
            @Nullable ImmutableBitSet reqColumns) {

        this(-1, cluster, traitSet, hints, table, projections, condition, reqColumns);
    }

    /**
     * Constructor used for deserialization.
     *
     * @param input Serialized representation.
     */
    public IgniteSystemViewScan(RelInput input) {
        super(changeTraits(input, IgniteConvention.INSTANCE));

        Object srcIdObj = input.get("sourceId");
        if (srcIdObj != null) {
            sourceId = ((Number) srcIdObj).longValue();
        } else {
            sourceId = -1;
        }
    }

    private IgniteSystemViewScan(long sourceId,
            RelOptCluster cluster,
            RelTraitSet traitSet,
            List<RelHint> hints,
            RelOptTable table,
            @Nullable List<RexNode> proj,
            @Nullable RexNode cond,
            @Nullable ImmutableBitSet reqColumns) {

        super(cluster, traitSet, hints, table, proj, cond, reqColumns);

        this.sourceId = sourceId;
    }

    /** {@inheritDoc} */
    @Override
    public <T> T accept(IgniteRelVisitor<T> visitor) {
        return visitor.visit(this);
    }

    /** {@inheritDoc} */
    @Override
    public long sourceId() {
        return sourceId;
    }

    /** {@inheritDoc} */
    @Override
    public IgniteRel clone(long sourceId) {
        return new IgniteSystemViewScan(sourceId, getCluster(), getTraitSet(), getHints(), getTable(),
                projects, condition, requiredColumns);
    }

    /** {@inheritDoc} */
    @Override
    public IgniteRel clone(RelOptCluster cluster, List<IgniteRel> inputs) {
        return new IgniteSystemViewScan(sourceId, cluster, getTraitSet(), getHints(), getTable(),
                projects, condition, requiredColumns);
    }

    /** {@inheritDoc} */
    @Override
    public IgniteSystemViewScan withHints(List<RelHint> hintList) {
        return new IgniteSystemViewScan(sourceId, getCluster(), getTraitSet(), hintList, getTable(),
                projects, condition, requiredColumns);
    }

    /** {@inheritDoc} */
    @Override
    protected RelWriter explainTerms0(RelWriter pw) {
        return super.explainTerms0(pw)
                .itemIf("sourceId", sourceId, sourceId != -1);
    }

    /** {@inheritDoc} */
    @Override
    public String getRelTypeName() {
        return REL_TYPE_NAME;
    }
}
