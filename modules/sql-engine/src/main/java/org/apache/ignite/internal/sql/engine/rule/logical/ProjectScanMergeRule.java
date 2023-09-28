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

package org.apache.ignite.internal.sql.engine.rule.logical;

import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLocalRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.mapping.Mappings;
import org.apache.ignite.internal.sql.engine.rel.ProjectableFilterableTableScan;
import org.apache.ignite.internal.sql.engine.rel.logical.IgniteLogicalIndexScan;
import org.apache.ignite.internal.sql.engine.rel.logical.IgniteLogicalSystemViewScan;
import org.apache.ignite.internal.sql.engine.rel.logical.IgniteLogicalTableScan;
import org.apache.ignite.internal.sql.engine.schema.IgniteDataSource;
import org.apache.ignite.internal.sql.engine.type.IgniteTypeFactory;
import org.apache.ignite.internal.sql.engine.util.Commons;
import org.apache.ignite.internal.sql.engine.util.RexUtils;
import org.immutables.value.Value;

/**
 * Rule that pushes filters and projections into a scan operation. This rule also prunes unused columns from a scan operator.
 * This rule operates on operators that support such options (see {@link ProjectableFilterableTableScan} and its subclasses).
 */
@Value.Enclosing
public abstract class ProjectScanMergeRule<T extends ProjectableFilterableTableScan>
        extends RelRule<ProjectScanMergeRule.Config> {

    public static final RelOptRule INDEX_SCAN = Config.INDEX_SCAN.toRule();

    public static final RelOptRule TABLE_SCAN = Config.TABLE_SCAN.toRule();

    public static final RelOptRule TABLE_SCAN_SKIP_CORRELATED = Config.TABLE_SCAN_SKIP_CORRELATED.toRule();

    public static final RelOptRule SYSTEM_VIEW_SCAN = Config.SYSTEM_VIEW_SCAN.toRule();

    public static final RelOptRule SYSTEM_VIEW_SCAN_SKIP_CORRELATED = Config.SYSTEM_VIEW_SCAN_SKIP_CORRELATED.toRule();

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
    @Override public void onMatch(RelOptRuleCall call) {
        LogicalProject relProject = call.rel(0);
        T scan = call.rel(1);

        RelOptCluster cluster = scan.getCluster();
        List<RexNode> projects = relProject.getProjects();
        RexNode cond = scan.condition();
        ImmutableBitSet requiredColumns = scan.requiredColumns();
        List<RexNode> scanProjects = scan.projects();

        // Set default traits, real traits will be calculated for physical node.
        RelTraitSet traits = cluster.traitSet();

        IgniteDataSource tbl = scan.getTable().unwrap(IgniteDataSource.class);
        IgniteTypeFactory typeFactory = Commons.typeFactory(cluster);

        if (requiredColumns == null) {
            assert scanProjects == null;

            ImmutableBitSet.Builder builder = ImmutableBitSet.builder();

            new RexShuttle() {
                @Override public RexNode visitInputRef(RexInputRef ref) {
                    builder.set(ref.getIndex());
                    return ref;
                }
            }.apply(projects);

            new RexShuttle() {
                @Override public RexNode visitLocalRef(RexLocalRef inputRef) {
                    builder.set(inputRef.getIndex());
                    return inputRef;
                }
            }.apply(cond);

            requiredColumns = builder.build();

            Mappings.TargetMapping targetMapping = Commons.trimmingMapping(
                    tbl.getRowType(typeFactory).getFieldCount(), requiredColumns);

            projects = new RexShuttle() {
                @Override public RexNode visitInputRef(RexInputRef ref) {
                    return new RexLocalRef(targetMapping.getTarget(ref.getIndex()), ref.getType());
                }
            }.apply(projects);

            cond = new RexShuttle() {
                @Override public RexNode visitLocalRef(RexLocalRef ref) {
                    return new RexLocalRef(targetMapping.getTarget(ref.getIndex()), ref.getType());
                }
            }.apply(cond);
        } else {
            projects = RexUtils.replaceInputRefs(projects);
        }

        if (scanProjects != null) {
            // Merge projects.
            projects = new RexShuttle() {
                @Override public RexNode visitLocalRef(RexLocalRef ref) {
                    return scanProjects.get(ref.getIndex());
                }
            }.apply(projects);
        }

        if (RexUtils.isIdentity(projects, tbl.getRowType(typeFactory, requiredColumns), true)) {
            projects = null;
        }

        call.transformTo(createNode(cluster, scan, traits, projects, cond, requiredColumns));

        if (!RexUtils.hasCorrelation(relProject.getProjects())) {
            cluster.getPlanner().prune(relProject);
        }
    }

    private static class ProjectTableScanMergeRule extends ProjectScanMergeRule<IgniteLogicalTableScan> {
        /**
         * Constructor.
         *
         * @param config Project scan merge rule config,
         */
        private ProjectTableScanMergeRule(ProjectScanMergeRule.Config config) {
            super(config);
        }

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
                    scan.getHints(),
                    scan.getTable(),
                    projections,
                    cond,
                    requiredColumns
            );
        }
    }

    private static class ProjectIndexScanMergeRule extends ProjectScanMergeRule<IgniteLogicalIndexScan> {
        /**
         * Constructor.
         *
         * @param config Project scan merge rule config,
         */
        private ProjectIndexScanMergeRule(ProjectScanMergeRule.Config config) {
            super(config);
        }

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
    }

    private static class ProjectSystemViewScanMergeRule extends ProjectScanMergeRule<IgniteLogicalSystemViewScan> {
        /**
         * Constructor.
         *
         * @param config Project scan merge rule config,
         */
        private ProjectSystemViewScanMergeRule(ProjectScanMergeRule.Config config) {
            super(config);
        }

        /** {@inheritDoc} */
        @Override protected IgniteLogicalSystemViewScan createNode(
                RelOptCluster cluster,
                IgniteLogicalSystemViewScan scan,
                RelTraitSet traits,
                List<RexNode> projections,
                RexNode cond,
                ImmutableBitSet requiredColumns
        ) {
            return IgniteLogicalSystemViewScan.create(
                    cluster,
                    traits,
                    scan.getHints(),
                    scan.getTable(),
                    projections,
                    cond,
                    requiredColumns
            );
        }
    }

    /**
     * Rule's configuration.
     */
    @SuppressWarnings("ClassNameSameAsAncestorName")
    @Value.Immutable(singleton = false)
    public interface Config extends RuleFactoryConfig<Config> {
        Config DEFAULT = ImmutableProjectScanMergeRule.Config.builder()
                .withRuleFactory(ProjectTableScanMergeRule::new)
                .build();

        Config TABLE_SCAN = DEFAULT.withScanRuleConfig(
                IgniteLogicalTableScan.class, "ProjectTableScanMergeRule", false);

        Config TABLE_SCAN_SKIP_CORRELATED = DEFAULT.withScanRuleConfig(
                IgniteLogicalTableScan.class, "ProjectTableScanMergeSkipCorrelatedRule", true);

        Config INDEX_SCAN = DEFAULT
                .withRuleFactory(ProjectIndexScanMergeRule::new)
                .withScanRuleConfig(IgniteLogicalIndexScan.class, "ProjectIndexScanMergeRule", false);

        Config SYSTEM_VIEW_SCAN = DEFAULT
                .withRuleFactory(ProjectSystemViewScanMergeRule::new)
                .withScanRuleConfig(IgniteLogicalSystemViewScan.class, "ProjectSystemViewScanMergeRule", false);

        Config SYSTEM_VIEW_SCAN_SKIP_CORRELATED = DEFAULT
                .withRuleFactory(ProjectSystemViewScanMergeRule::new)
                .withScanRuleConfig(IgniteLogicalSystemViewScan.class, "ProjectSystemViewScanMergeSkipCorrelatedRule", true);


        /**
         * Create rule's configuration.
         */
        default Config withScanRuleConfig(
                Class<? extends ProjectableFilterableTableScan> scanCls,
                String desc,
                boolean skipCorrelated
        ) {
            return withDescription(desc)
                    .withOperandSupplier(b -> b.operand(LogicalProject.class)
                            .predicate(p -> !skipCorrelated || !RexUtils.hasCorrelation(p.getProjects()))
                            .oneInput(b1 -> b1.operand(scanCls).noInputs()))
                    .as(Config.class);
        }
    }
}
