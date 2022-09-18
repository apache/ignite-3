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
 * Relational operator that returns the contents of a table.
 */
public class IgniteTableScan extends ProjectableFilterableTableScan implements SourceAwareIgniteRel {
    private final long sourceId;

    /**
     * Constructor used for deserialization.
     *
     * @param input Serialized representation.
     */
    public IgniteTableScan(RelInput input) {
        super(changeTraits(input, IgniteConvention.INSTANCE));

        Object srcIdObj = input.get("sourceId");
        if (srcIdObj != null) {
            sourceId = ((Number) srcIdObj).longValue();
        } else {
            sourceId = -1;
        }
    }

//    /**
//     * Creates a TableScan.
//     *
//     * @param cluster Cluster that this relational expression belongs to.
//     * @param traits  Traits of this relational expression.
//     * @param tbl     Table definition.
//     */
//    public IgniteTableScan(
//            RelOptCluster cluster,
//            RelTraitSet traits,
//            RelOptTable tbl
//    ) {
//        this(cluster, traits, tbl, null, null, null);
//    }

    /**
     * Creates a TableScan.
     *
     * @param cluster         Cluster that this relational expression belongs to.
     * @param traits          Traits of this relational expression.
     * @param tbl             Table definition.
     * @param hints           Table hints.
     * @param proj            Projects.
     * @param cond            Filters.
     * @param requiredColumns Participating columns.
     */
    public IgniteTableScan(
        RelOptCluster cluster,
        RelTraitSet traits,
        RelOptTable tbl,
        List<RelHint> hints,
        @Nullable List<RexNode> proj,
        @Nullable RexNode cond,
        @Nullable ImmutableBitSet requiredColumns
    ) {
        this(-1L, cluster, traits, tbl, hints, proj, cond, requiredColumns);
    }

    /**
     * Creates a TableScan.
     *
     * @param cluster         Cluster that this relational expression belongs to.
     * @param traits          Traits of this relational expression.
     * @param tbl             Table definition.
     * @param hints           Table hints.
     * @param proj            Projects.
     * @param cond            Filters.
     * @param requiredColumns Participating columns.
     */
    public IgniteTableScan(
        long sourceId,
        RelOptCluster cluster,
        RelTraitSet traits,
        RelOptTable tbl,
        List<RelHint> hints,
        @Nullable List<RexNode> proj,
        @Nullable RexNode cond,
        @Nullable ImmutableBitSet requiredColumns
    ) {
        super(cluster, traits, hints, tbl, proj, cond, requiredColumns);
        this.sourceId = sourceId;
    }

    /**
     * Get source id.
     * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
     */
    @Override
    public long sourceId() {
        return sourceId;
    }

    @Override
    protected RelWriter explainTerms0(RelWriter pw) {
        return super.explainTerms0(pw)
                .itemIf("sourceId", sourceId, sourceId != -1);
    }

    /** {@inheritDoc} */
    @Override
    public <T> T accept(IgniteRelVisitor<T> visitor) {
        return visitor.visit(this);
    }

    /** {@inheritDoc} */
    @Override
    public IgniteRel clone(long sourceId) {
        return new IgniteTableScan(sourceId, getCluster(), getTraitSet(), getTable(), getHints(), projects, condition, requiredColumns);
    }

    /** {@inheritDoc} */
    @Override
    public IgniteRel clone(RelOptCluster cluster, List<IgniteRel> inputs) {
        return new IgniteTableScan(sourceId, cluster, getTraitSet(), getTable(), getHints(), projects, condition, requiredColumns);
    }
}
