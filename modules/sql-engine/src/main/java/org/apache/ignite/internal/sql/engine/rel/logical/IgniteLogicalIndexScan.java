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
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.mapping.Mappings;
import org.apache.ignite.internal.sql.engine.prepare.bounds.SearchBounds;
import org.apache.ignite.internal.sql.engine.rel.AbstractIndexScan;
import org.apache.ignite.internal.sql.engine.schema.IgniteIndex;
import org.apache.ignite.internal.sql.engine.schema.IgniteIndex.Type;
import org.apache.ignite.internal.sql.engine.schema.IgniteTable;
import org.apache.ignite.internal.sql.engine.type.IgniteTypeFactory;
import org.apache.ignite.internal.sql.engine.util.Commons;
import org.apache.ignite.internal.sql.engine.util.RexUtils;
import org.jetbrains.annotations.Nullable;

/**
 * Logical relational expression for reading data from an index.
 */
public class IgniteLogicalIndexScan extends AbstractIndexScan {
    private static final String REL_TYPE_NAME = "LogicalIndexScan";

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
        IgniteTable tbl = table.unwrap(IgniteTable.class);
        IgniteTypeFactory typeFactory = Commons.typeFactory(cluster);
        IgniteIndex index = tbl.indexes().get(idxName);
        RelCollation collation = index.collation();

        List<SearchBounds> searchBounds;
        if (index.type() == Type.HASH) {
            if (requiredColumns != null) {
                Mappings.TargetMapping targetMapping = Commons.trimmingMapping(
                        tbl.getRowType(typeFactory).getFieldCount(), requiredColumns);
                RelCollation outputCollation = collation.apply(targetMapping);

                searchBounds = (collation.getFieldCollations().size() == outputCollation.getFieldCollations().size())
                        ? buildHashIndexConditions(cluster, tbl, outputCollation, cond, requiredColumns)
                        : null;
            } else {
                searchBounds = buildHashIndexConditions(cluster, tbl, collation, cond, requiredColumns);
            }
        } else if (index.type() == Type.SORTED) {
            if (requiredColumns != null) {
                Mappings.TargetMapping targetMapping = Commons.trimmingMapping(
                        tbl.getRowType(typeFactory).getFieldCount(), requiredColumns);
                collation = collation.apply(targetMapping);
            }

            searchBounds = buildSortedIndexConditions(cluster, tbl, collation, cond, requiredColumns);
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
                searchBounds,
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
     * @param searchBounds Index search conditions.
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
            @Nullable List<SearchBounds> searchBounds,
            @Nullable ImmutableBitSet requiredCols
    ) {
        super(cluster, traits, List.of(), tbl, idxName, type, proj, cond, searchBounds, requiredCols);
    }

    private static @Nullable List<SearchBounds> buildSortedIndexConditions(
            RelOptCluster cluster,
            IgniteTable table,
            RelCollation collation,
            @Nullable RexNode cond,
            @Nullable ImmutableBitSet requiredColumns
    ) {
        if (collation.getFieldCollations().isEmpty()) {
            return null;
        }

        return RexUtils.buildSortedSearchBounds(
                cluster,
                collation,
                cond,
                table.getRowType(Commons.typeFactory(cluster)),
                requiredColumns
        );
    }

    private static List<SearchBounds> buildHashIndexConditions(
            RelOptCluster cluster,
            IgniteTable table,
            RelCollation collation,
            RexNode cond,
            @Nullable ImmutableBitSet requiredColumns
    ) {
        return RexUtils.buildHashSearchBounds(
                cluster,
                collation,
                cond,
                table.getRowType(Commons.typeFactory(cluster)),
                requiredColumns
        );
    }

    /** {@inheritDoc} */
    @Override
    public String getRelTypeName() {
        return REL_TYPE_NAME;
    }
}
