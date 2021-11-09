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

package org.apache.ignite.internal.processors.query.calcite.rule.logical;

import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLocalRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.mapping.Mappings;
import org.apache.ignite.internal.processors.query.calcite.rel.ProjectableFilterableTableScan;
import org.apache.ignite.internal.processors.query.calcite.rel.logical.IgniteLogicalIndexScan;
import org.apache.ignite.internal.processors.query.calcite.rel.logical.IgniteLogicalTableScan;
import org.apache.ignite.internal.processors.query.calcite.schema.IgniteTable;
import org.apache.ignite.internal.processors.query.calcite.trait.TraitUtils;
import org.apache.ignite.internal.processors.query.calcite.type.IgniteTypeFactory;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;
import org.apache.ignite.internal.processors.query.calcite.util.RexUtils;

/**
 * ProjectScanMergeRule.
 * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
 */
public abstract class ProjectScanMergeRule<T extends ProjectableFilterableTableScan>
        extends RelRule<ProjectScanMergeRule.Config> {
    /** Instance. */
    public static final ProjectScanMergeRule<IgniteLogicalIndexScan> INDEX_SCAN =
            new ProjectScanMergeRule<IgniteLogicalIndexScan>(Config.INDEX_SCAN) {
                /** {@inheritDoc} */
                @Override protected IgniteLogicalIndexScan createNode(
                        RelOptCluster cluster,
                        IgniteLogicalIndexScan scan,
                        RelTraitSet traits,
                        List<RexNode> projections,
                        RexNode cond,
                        ImmutableBitSet requiredColumns
                ) {
                    return IgniteLogicalIndexScan.create(
                            cluster,
                            traits,
                            scan.getTable(),
                            scan.indexName(),
                            projections,
                            cond, requiredColumns
                    );
                }
            };

    /** Instance. */
    public static final ProjectScanMergeRule<IgniteLogicalTableScan> TABLE_SCAN =
            new ProjectScanMergeRule<IgniteLogicalTableScan>(Config.TABLE_SCAN) {
                /** {@inheritDoc} */
                @Override protected IgniteLogicalTableScan createNode(
                        RelOptCluster cluster,
                        IgniteLogicalTableScan scan,
                        RelTraitSet traits,
                        List<RexNode> projections,
                        RexNode cond,
                        ImmutableBitSet requiredColumns
                ) {
                    return IgniteLogicalTableScan.create(
                            cluster,
                            traits,
                            scan.getTable(),
                            projections,
                            cond,
                            requiredColumns
                    );
                }
            };

    protected abstract T createNode(
            RelOptCluster cluster,
            T scan,
            RelTraitSet traits,
            List<RexNode> projections,
            RexNode cond,
            ImmutableBitSet requiredColumns
    );

    /**
     * Constructor.
     *
     * @param config Project scan merge rule config,
     */
    private ProjectScanMergeRule(Config config) {
        super(config);
    }

    /** {@inheritDoc} */
    @Override
    public boolean matches(RelOptRuleCall call) {
        T rel = call.rel(1);
        return rel.requiredColumns() == null;
    }

    /** {@inheritDoc} */
    @Override
    public void onMatch(RelOptRuleCall call) {
        LogicalProject relProject = call.rel(0);
        T scan = call.rel(1);

        RelOptCluster cluster = scan.getCluster();
        List<RexNode> projects = relProject.getProjects();
        final RexNode cond = scan.condition();

        // projection changes input collation and distribution.
        RelTraitSet traits = scan.getTraitSet();

        traits = traits.replace(TraitUtils.projectCollation(
                TraitUtils.collation(traits), projects, scan.getRowType()));

        traits = traits.replace(TraitUtils.projectDistribution(
                TraitUtils.distribution(traits), projects, scan.getRowType()));

        IgniteTable tbl = scan.getTable().unwrap(IgniteTable.class);
        IgniteTypeFactory typeFactory = Commons.typeFactory(cluster);
        ImmutableBitSet.Builder builder = ImmutableBitSet.builder();

        new RexShuttle() {
            @Override
            public RexNode visitInputRef(RexInputRef ref) {
                builder.set(ref.getIndex());
                return ref;
            }
        }.apply(projects);

        new RexShuttle() {
            @Override
            public RexNode visitLocalRef(RexLocalRef inputRef) {
                builder.set(inputRef.getIndex());
                return inputRef;
            }
        }.apply(cond);

        ImmutableBitSet requiredColumns = builder.build();

        Mappings.TargetMapping targetMapping = Commons.mapping(requiredColumns,
                tbl.getRowType(typeFactory).getFieldCount());

        projects = new RexShuttle() {
            @Override
            public RexNode visitInputRef(RexInputRef ref) {
                return new RexLocalRef(targetMapping.getTarget(ref.getIndex()), ref.getType());
            }
        }.apply(projects);

        if (RexUtils.isIdentity(projects, tbl.getRowType(typeFactory, requiredColumns), true)) {
            projects = null;
        }

        final RexNode cond1 = new RexShuttle() {
            @Override
            public RexNode visitLocalRef(RexLocalRef ref) {
                return new RexLocalRef(targetMapping.getTarget(ref.getIndex()), ref.getType());
            }
        }.apply(cond);

        call.transformTo(createNode(cluster, scan, traits, projects, cond1, requiredColumns));
    }

    /** */
    @SuppressWarnings("ClassNameSameAsAncestorName")
    public interface Config extends RelRule.Config {
        /** */
        Config DEFAULT = EMPTY.withRelBuilderFactory(RelFactories.LOGICAL_BUILDER).as(Config.class);

        /** */
        Config TABLE_SCAN = DEFAULT.withScanRuleConfig(IgniteLogicalTableScan.class, "ProjectTableScanMergeRule");

        /** */
        Config INDEX_SCAN = DEFAULT.withScanRuleConfig(IgniteLogicalIndexScan.class, "ProjectIndexScanMergeRule");

        /** */
        default Config withScanRuleConfig(Class<? extends ProjectableFilterableTableScan> scanCls, String desc) {
            return withDescription(desc)
                .withOperandSupplier(b ->
                    b.operand(LogicalProject.class).oneInput(b1 -> b1.operand(scanCls).noInputs()))
                .as(Config.class);
        }
    }
}
