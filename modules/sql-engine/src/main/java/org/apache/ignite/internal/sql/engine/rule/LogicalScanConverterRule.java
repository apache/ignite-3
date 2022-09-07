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

package org.apache.ignite.internal.sql.engine.rule;

import static org.apache.ignite.internal.util.CollectionUtils.concat;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.PhysicalNode;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexLocalRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.mapping.Mapping;
import org.apache.calcite.util.mapping.Mappings;
import org.apache.ignite.internal.sql.engine.rel.IgniteConvention;
import org.apache.ignite.internal.sql.engine.rel.IgniteIndexScan;
import org.apache.ignite.internal.sql.engine.rel.IgniteTableScan;
import org.apache.ignite.internal.sql.engine.rel.ProjectableFilterableTableScan;
import org.apache.ignite.internal.sql.engine.rel.logical.IgniteLogicalIndexScan;
import org.apache.ignite.internal.sql.engine.rel.logical.IgniteLogicalTableScan;
import org.apache.ignite.internal.sql.engine.schema.IgniteIndex;
import org.apache.ignite.internal.sql.engine.schema.InternalIgniteTable;
import org.apache.ignite.internal.sql.engine.trait.CorrelationTrait;
import org.apache.ignite.internal.sql.engine.trait.RewindabilityTrait;
import org.apache.ignite.internal.sql.engine.trait.TraitUtils;
import org.apache.ignite.internal.sql.engine.util.RexUtils;
import org.apache.ignite.internal.util.CollectionUtils;

/**
 * LogicalScanConverterRule.
 * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
 */
public abstract class LogicalScanConverterRule<T extends ProjectableFilterableTableScan> extends AbstractIgniteConverterRule<T> {
    /** Instance. */
    public static final LogicalScanConverterRule<IgniteLogicalIndexScan> INDEX_SCAN =
            new LogicalScanConverterRule<IgniteLogicalIndexScan>(IgniteLogicalIndexScan.class, "LogicalIndexScanConverterRule") {

                /** {@inheritDoc} */
                @Override
                protected PhysicalNode convert(
                        RelOptPlanner planner,
                        RelMetadataQuery mq,
                        IgniteLogicalIndexScan rel
                ) {
                    RelOptCluster cluster = rel.getCluster();
                    InternalIgniteTable table = rel.getTable().unwrap(InternalIgniteTable.class);
                    IgniteIndex index = table.getIndex(rel.indexName());

                    RelDistribution distribution = table.distribution();
                    RelCollation collation = TraitUtils.createCollation(index.columns(), index.collations(), table.descriptor());

                    if (rel.projects() != null || rel.requiredColumns() != null) {
                        Mappings.TargetMapping mapping = createMapping(
                                rel.projects(),
                                rel.requiredColumns(),
                                table.getRowType(cluster.getTypeFactory()).getFieldCount()
                        );

                        distribution = distribution.apply(mapping);
                        collation = collation.apply(mapping);
                    }

                    Set<CorrelationId> corrIds = RexUtils.extractCorrelationIds(rel.condition());

                    if (!CollectionUtils.nullOrEmpty(rel.projects())) {
                        corrIds = new HashSet<>(concat(corrIds, RexUtils.extractCorrelationIds(rel.projects())));
                    }

                    RelTraitSet traits = rel.getCluster().traitSetOf(IgniteConvention.INSTANCE)
                            .replace(RewindabilityTrait.REWINDABLE)
                            .replace(distribution)
                            .replace(collation)
                            .replace(corrIds.isEmpty() ? CorrelationTrait.UNCORRELATED : CorrelationTrait.correlations(corrIds));

                    return new IgniteIndexScan(
                        cluster,
                        traits,
                        rel.getTable(),
                        rel.indexName(),
                        index.type(),
                        rel.projects(),
                        rel.condition(),
                        rel.indexConditions(),
                        rel.requiredColumns()
                    );
                }
            };

    /** Instance. */
    public static final LogicalScanConverterRule<IgniteLogicalTableScan> TABLE_SCAN =
            new LogicalScanConverterRule<IgniteLogicalTableScan>(IgniteLogicalTableScan.class, "LogicalTableScanConverterRule") {
                /** {@inheritDoc} */
                @Override protected PhysicalNode convert(
                        RelOptPlanner planner,
                        RelMetadataQuery mq,
                        IgniteLogicalTableScan rel
                ) {
                    RelOptCluster cluster = rel.getCluster();
                    InternalIgniteTable table = rel.getTable().unwrap(InternalIgniteTable.class);

                    RelDistribution distribution = table.distribution();
                    if (rel.requiredColumns() != null) {
                        Mappings.TargetMapping mapping = createMapping(
                                rel.projects(),
                                rel.requiredColumns(),
                                table.getRowType(cluster.getTypeFactory()).getFieldCount()
                        );

                        distribution = distribution.apply(mapping);
                    }

                    Set<CorrelationId> corrIds = RexUtils.extractCorrelationIds(rel.condition());

                    if (!CollectionUtils.nullOrEmpty(rel.projects())) {
                        corrIds = new HashSet<>(concat(corrIds, RexUtils.extractCorrelationIds(rel.projects())));
                    }

                    RelTraitSet traits = cluster.traitSetOf(IgniteConvention.INSTANCE)
                            .replace(RewindabilityTrait.REWINDABLE)
                            .replace(distribution)
                            .replace(corrIds.isEmpty() ? CorrelationTrait.UNCORRELATED : CorrelationTrait.correlations(corrIds));

                    return new IgniteTableScan(rel.getCluster(), traits,
                        rel.getTable(), rel.projects(), rel.condition(), rel.requiredColumns());
                }
            };


    private LogicalScanConverterRule(Class<T> clazz, String descPrefix) {
        super(clazz, descPrefix);
    }

    private static Mappings.TargetMapping createMapping(
            List<RexNode> projects,
            ImmutableBitSet requiredColumns,
            int tableRowSize
    ) {
        if (projects != null) {
            Mapping trimmingMapping = requiredColumns != null
                    ? Mappings.invert(Mappings.source(requiredColumns.asList(), tableRowSize))
                    : Mappings.createIdentity(tableRowSize);

            Map<Integer, Integer> mappingMap = new HashMap<>();

            for (int i = 0; i < projects.size(); i++) {
                RexNode rex = projects.get(i);
                if (!(rex instanceof RexLocalRef)) {
                    continue;
                }

                RexLocalRef ref = (RexLocalRef) rex;

                mappingMap.put(trimmingMapping.getSource(ref.getIndex()), i);
            }

            return Mappings.target(
                mappingMap,
                tableRowSize,
                projects.size()
            );
        }

        if (requiredColumns != null) {
            return Mappings.target(requiredColumns.asList(), tableRowSize);
        }

        return Mappings.createIdentity(tableRowSize);
    }
}
