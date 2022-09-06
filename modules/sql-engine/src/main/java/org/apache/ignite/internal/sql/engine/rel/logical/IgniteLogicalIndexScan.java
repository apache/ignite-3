/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
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
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.mapping.Mappings;
import org.apache.ignite.internal.sql.engine.rel.AbstractIndexScan;
import org.apache.ignite.internal.sql.engine.schema.IgniteIndex;
import org.apache.ignite.internal.sql.engine.schema.IgniteIndex.Type;
import org.apache.ignite.internal.sql.engine.schema.InternalIgniteTable;
import org.apache.ignite.internal.sql.engine.trait.TraitUtils;
import org.apache.ignite.internal.sql.engine.type.IgniteTypeFactory;
import org.apache.ignite.internal.sql.engine.util.Commons;
import org.apache.ignite.internal.sql.engine.util.IndexConditions;
import org.apache.ignite.internal.sql.engine.util.RexUtils;
import org.jetbrains.annotations.Nullable;

/**
 * IgniteLogicalIndexScan.
 * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
 */
public class IgniteLogicalIndexScan extends AbstractIndexScan {
    /** Creates a IgniteLogicalIndexScan. */
    public static IgniteLogicalIndexScan create(
            RelOptCluster cluster,
            RelTraitSet traits,
            RelOptTable table,
            String idxName,
            @Nullable List<RexNode> proj,
            @Nullable RexNode cond,
            @Nullable ImmutableBitSet requiredColumns
    ) {
        InternalIgniteTable tbl = table.unwrap(InternalIgniteTable.class);
        IgniteTypeFactory typeFactory = Commons.typeFactory(cluster);
        IgniteIndex index = tbl.getIndex(idxName);
        RelCollation collation = TraitUtils.createCollation(index.columns(), index.collations(), tbl.descriptor());

        if (requiredColumns != null) {
            Mappings.TargetMapping targetMapping = Commons.mapping(requiredColumns,
                    tbl.getRowType(typeFactory).getFieldCount());
            collation = collation.apply(targetMapping);
        }

        IndexConditions idxCond;
        if (index.type() == Type.HASH) {
            idxCond = buildHashIndexConditions(cluster, tbl, index.columns(), cond, requiredColumns);
        } else if (index.type() == Type.SORTED) {
            idxCond = buildSortedIndexConditions(cluster, tbl, collation, cond, requiredColumns);
        } else {
            throw new AssertionError("Unknown index type [type=" + index.type() + "]");
        }

        return new IgniteLogicalIndexScan(
                cluster,
                traits,
                table,
                idxName,
                index.type(),
                proj,
                cond,
                idxCond,
                requiredColumns);
    }

    /**
     * Creates a IndexScan.
     *
     * @param cluster Cluster that this relational expression belongs to
     * @param traits Traits of this relational expression
     * @param tbl Table definition.
     * @param idxName Index name.
     * @param type Type of the index.
     * @param proj Projects.
     * @param cond Filters.
     * @param idxCond Index conditions.
     * @param requiredCols Participating columns.
     */
    private IgniteLogicalIndexScan(
            RelOptCluster cluster,
            RelTraitSet traits,
            RelOptTable tbl,
            String idxName,
            IgniteIndex.Type type,
            @Nullable List<RexNode> proj,
            @Nullable RexNode cond,
            @Nullable IndexConditions idxCond,
            @Nullable ImmutableBitSet requiredCols
    ) {
        super(cluster, traits, List.of(), tbl, idxName, type, proj, cond, idxCond, requiredCols);
    }

    private static IndexConditions buildSortedIndexConditions(
            RelOptCluster cluster,
            InternalIgniteTable table,
            RelCollation collation,
            @Nullable RexNode cond,
            @Nullable ImmutableBitSet requiredColumns
    ) {
        if (collation.getFieldCollations().isEmpty()) {
            return new IndexConditions();
        }

        return RexUtils.buildSortedIndexConditions(
                cluster,
                collation,
                cond,
                table.getRowType(Commons.typeFactory(cluster)),
                requiredColumns
        );
    }

    private static IndexConditions buildHashIndexConditions(
            RelOptCluster cluster,
            InternalIgniteTable table,
            List<String> indexedColumns,
            @Nullable RexNode cond,
            @Nullable ImmutableBitSet requiredColumns
    ) {
        return RexUtils.buildHashIndexConditions(cluster, indexedColumns, cond,
                table.getRowType(Commons.typeFactory(cluster)), requiredColumns);
    }
}
