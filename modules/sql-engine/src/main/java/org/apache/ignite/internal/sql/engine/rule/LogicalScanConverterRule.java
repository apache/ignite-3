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

package org.apache.ignite.internal.sql.engine.rule;

import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.PhysicalNode;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexLocalRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.calcite.util.mapping.IntPair;
import org.apache.calcite.util.mapping.Mapping;
import org.apache.calcite.util.mapping.Mappings;
import org.apache.ignite.internal.sql.engine.rel.IgniteConvention;
import org.apache.ignite.internal.sql.engine.rel.IgniteIndexScan;
import org.apache.ignite.internal.sql.engine.rel.IgniteSystemViewScan;
import org.apache.ignite.internal.sql.engine.rel.IgniteTableScan;
import org.apache.ignite.internal.sql.engine.rel.ProjectableFilterableTableScan;
import org.apache.ignite.internal.sql.engine.rel.logical.IgniteLogicalIndexScan;
import org.apache.ignite.internal.sql.engine.rel.logical.IgniteLogicalSystemViewScan;
import org.apache.ignite.internal.sql.engine.rel.logical.IgniteLogicalTableScan;
import org.apache.ignite.internal.sql.engine.schema.IgniteIndex;
import org.apache.ignite.internal.sql.engine.schema.IgniteIndex.Type;
import org.apache.ignite.internal.sql.engine.schema.IgniteSystemView;
import org.apache.ignite.internal.sql.engine.schema.IgniteTable;
import org.jetbrains.annotations.Nullable;

/**
 * Converts a logical scan operator into its implementation.
 *
 * <ul>
 *     <li>{@link IgniteLogicalTableScan Table scan} to {@link IgniteTableScan}.</li>
 *     <li>{@link IgniteLogicalIndexScan Index scan} to {@link IgniteIndexScan}.</li>
 *     <li>{@link IgniteLogicalSystemViewScan System view scan} to {@link IgniteSystemViewScan}.</li>
 * </ul>
 */
public abstract class LogicalScanConverterRule<T extends ProjectableFilterableTableScan> extends AbstractIgniteConverterRule<T> {
    /** Instance. */
    public static final LogicalScanConverterRule<IgniteLogicalIndexScan> INDEX_SCAN =
            new LogicalScanConverterRule<>(IgniteLogicalIndexScan.class, "LogicalIndexScanConverterRule") {

                /** {@inheritDoc} */
                @Override
                protected PhysicalNode convert(
                        RelOptPlanner planner,
                        RelMetadataQuery mq,
                        IgniteLogicalIndexScan rel
                ) {
                    RelOptCluster cluster = rel.getCluster();
                    IgniteTable table = rel.getTable().unwrap(IgniteTable.class);
                    IgniteIndex index = table.indexes().get(rel.indexName());

                    RelDistribution distribution = table.distribution();
                    RelCollation collation = index.type() == Type.HASH 
                            ? RelCollations.EMPTY 
                            : index.collation();

                    if (rel.projects() != null || rel.requiredColumns() != null) {
                        Mappings.TargetMapping mapping = createMapping(
                                rel.projects(),
                                rel.requiredColumns(),
                                table.getRowType(cluster.getTypeFactory()).getFieldCount()
                        );

                        distribution = distribution.apply(mapping);
                        collation = collation.apply(mapping);
                    }

                    RelTraitSet traits = rel.getCluster().traitSetOf(IgniteConvention.INSTANCE)
                            .replace(distribution)
                            .replace(collation);

                    return new IgniteIndexScan(
                            cluster,
                            traits,
                            rel.getTable(),
                            rel.indexName(),
                            index.type(),
                            collation,
                            rel.fieldNames(),
                            rel.projects(),
                            rel.condition(),
                            rel.searchBounds(),
                            rel.requiredColumns()
                    );
                }
            };

    /** Instance. */
    public static final LogicalScanConverterRule<IgniteLogicalTableScan> TABLE_SCAN =
            new LogicalScanConverterRule<>(IgniteLogicalTableScan.class, "LogicalTableScanConverterRule") {
                /** {@inheritDoc} */
                @Override protected PhysicalNode convert(
                        RelOptPlanner planner,
                        RelMetadataQuery mq,
                        IgniteLogicalTableScan rel
                ) {
                    RelOptCluster cluster = rel.getCluster();
                    IgniteTable table = rel.getTable().unwrap(IgniteTable.class);

                    RelDistribution distribution = table.distribution();
                    if (rel.requiredColumns() != null) {
                        Mappings.TargetMapping mapping = createMapping(
                                rel.projects(),
                                rel.requiredColumns(),
                                table.getRowType(cluster.getTypeFactory()).getFieldCount()
                        );

                        distribution = distribution.apply(mapping);
                    }

                    RelTraitSet traits = cluster.traitSetOf(IgniteConvention.INSTANCE)
                            .replace(distribution);

                    return new IgniteTableScan(rel.getCluster(), traits, rel.getTable(), rel.getHints(),
                            rel.fieldNames(), rel.projects(), rel.condition(), rel.requiredColumns());
                }
            };

    /** Instance. */
    public static final LogicalScanConverterRule<IgniteLogicalSystemViewScan> SYSTEM_VIEW_SCAN =
            new LogicalScanConverterRule<>(IgniteLogicalSystemViewScan.class, "LogicalSystemViewScanConverterRule") {
                /** {@inheritDoc} */
                @Override protected PhysicalNode convert(
                        RelOptPlanner planner,
                        RelMetadataQuery mq,
                        IgniteLogicalSystemViewScan rel
                ) {
                    RelOptCluster cluster = rel.getCluster();
                    IgniteSystemView systemView = rel.getTable().unwrap(IgniteSystemView.class);

                    RelDistribution distribution = systemView.distribution();
                    if (rel.requiredColumns() != null) {
                        Mappings.TargetMapping mapping = createMapping(
                                rel.projects(),
                                rel.requiredColumns(),
                                systemView.getRowType(cluster.getTypeFactory()).getFieldCount()
                        );

                        distribution = distribution.apply(mapping);
                    }

                    RelTraitSet traits = cluster.traitSetOf(IgniteConvention.INSTANCE)
                            .replace(distribution);

                    return new IgniteSystemViewScan(rel.getCluster(), traits,  rel.getHints(), rel.getTable(),
                            rel.fieldNames(), rel.projects(), rel.condition(), rel.requiredColumns());
                }
            };

    private LogicalScanConverterRule(Class<T> clazz, String descPrefix) {
        super(clazz, descPrefix);
    }

    /** Creates column mapping regarding the projection. */
    public static Mappings.TargetMapping createMapping(
            @Nullable List<RexNode> projects,
            @Nullable ImmutableIntList requiredColumns,
            int tableRowSize
    ) {
        if (projects != null) {
            Mapping trimmingMapping = requiredColumns != null
                    ? Mappings.invert(Mappings.source(requiredColumns, tableRowSize))
                    : Mappings.createIdentity(tableRowSize);

            List<IntPair> pairs = new ArrayList<>(projects.size());
            for (int i = 0; i < projects.size(); i++) {
                RexNode rex = projects.get(i);
                if (!(rex instanceof RexLocalRef)) {
                    continue;
                }

                RexLocalRef ref = (RexLocalRef) rex;

                pairs.add(IntPair.of(trimmingMapping.getSource(ref.getIndex()), i));
            }

            return Mappings.target(
                pairs,
                tableRowSize,
                projects.size()
            );
        }

        if (requiredColumns != null) {
            return Mappings.target(requiredColumns, tableRowSize);
        }

        return Mappings.createIdentity(tableRowSize);
    }
}
