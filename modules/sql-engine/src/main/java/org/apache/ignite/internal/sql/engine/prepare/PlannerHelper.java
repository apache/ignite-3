/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.sql.engine.prepare;

import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.util.Pair;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.sql.engine.rel.IgniteConvention;
import org.apache.ignite.internal.sql.engine.rel.IgniteProject;
import org.apache.ignite.internal.sql.engine.rel.IgniteRel;
import org.apache.ignite.internal.sql.engine.trait.IgniteDistributions;
import org.apache.ignite.internal.sql.engine.util.HintUtils;

/**
 * PlannerHelper.
 * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
 */
public class PlannerHelper {

    private static final IgniteLogger LOG = Loggers.forClass(PlannerHelper.class);

    /**
     * Default constructor.
     */
    private PlannerHelper() {

    }

    /**
     * Optimize.
     * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
     *
     * @param sqlNode Sql node.
     * @param planner Planner.
     */
    public static IgniteRel optimize(SqlNode sqlNode, IgnitePlanner planner) {
        try {
            // Convert to Relational operators graph
            RelRoot root = planner.rel(sqlNode);

            RelNode rel = root.rel;

            if (HintUtils.containsDisabledRules(root.hints)) {
                planner.setDisabledRules(HintUtils.disabledRules(root.hints));
            }

            // Transformation chain
            rel = planner.transform(PlannerPhase.HEP_DECORRELATE, rel.getTraitSet(), rel);

            rel = planner.replaceCorrelatesCollisions(rel);

            rel = planner.trimUnusedFields(root.withRel(rel)).rel;

            rel = planner.transform(PlannerPhase.HEP_FILTER_PUSH_DOWN, rel.getTraitSet(), rel);

            rel = planner.transform(PlannerPhase.HEP_PROJECT_PUSH_DOWN, rel.getTraitSet(), rel);

            RelTraitSet desired = rel.getCluster().traitSet()
                    .replace(IgniteConvention.INSTANCE)
                    .replace(IgniteDistributions.single())
                    .replace(root.collation == null ? RelCollations.EMPTY : root.collation)
                    .simplify();

            IgniteRel igniteRel = planner.transform(PlannerPhase.OPTIMIZATION, desired, rel);

            if (!root.isRefTrivial()) {
                final List<RexNode> projects = new ArrayList<>();
                final RexBuilder rexBuilder = igniteRel.getCluster().getRexBuilder();

                for (int field : Pair.left(root.fields)) {
                    projects.add(rexBuilder.makeInputRef(igniteRel, field));
                }

                igniteRel = new IgniteProject(igniteRel.getCluster(), desired, igniteRel, projects, root.validatedRowType);
            }

            return igniteRel;
        } catch (Throwable ex) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Unexpected error at query optimizer", ex);
                LOG.debug(planner.dump());
            }

            throw ex;
        }
    }
}
