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


package org.apache.ignite.internal.sql.engine.rel.logical;

import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.ignite.internal.sql.engine.rel.ProjectableFilterableTableScan;
import org.jetbrains.annotations.Nullable;

/**
 * Logical relational expression for reading data from a system view.
 */
public class IgniteLogicalSystemViewScan extends ProjectableFilterableTableScan {
    private static final String REL_TYPE_NAME = "LogicalSystemViewScan";

    /**
     * Creates asSystem view scan.
     *
     * @param cluster Cluster that this relational expression belongs to.
     * @param hints Table hints.
     * @param table Table definition.
     * @param projections Projection list.
     * @param condition Filter condition.
     * @param reqColumns Participating columns.
     */
    private IgniteLogicalSystemViewScan(RelOptCluster cluster, RelTraitSet traitSet,
            List<RelHint> hints, RelOptTable table,
            @Nullable List<RexNode> projections, @Nullable RexNode condition, @Nullable ImmutableBitSet reqColumns) {
        super(cluster, traitSet, hints, table, projections, condition, reqColumns);
    }

    /** Creates an instance of a logical system view scan operator. */
    public static IgniteLogicalSystemViewScan create(
            RelOptCluster cluster,
            RelTraitSet traits,
            List<RelHint> hints,
            RelOptTable tbl,
            @Nullable List<RexNode> proj,
            @Nullable RexNode cond,
            @Nullable ImmutableBitSet requiredColumns
    ) {
        return new IgniteLogicalSystemViewScan(cluster, traits, hints, tbl, proj, cond, requiredColumns);
    }

    /** {@inheritDoc} */
    @Override
    public IgniteLogicalSystemViewScan withHints(List<RelHint> hintList) {
        return new IgniteLogicalSystemViewScan(getCluster(), getTraitSet(), hintList, getTable(),
                projects, condition, requiredColumns);
    }

    /** {@inheritDoc} */
    @Override
    public String getRelTypeName() {
        return REL_TYPE_NAME;
    }
}
