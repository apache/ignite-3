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

package org.apache.ignite.internal.sql.engine.prepare;

import static org.apache.ignite.internal.sql.engine.hint.IgniteHint.DISABLE_RULE;
import static org.apache.ignite.internal.sql.engine.hint.IgniteHint.ENFORCE_JOIN_ORDER;
import static org.apache.ignite.internal.sql.engine.util.Commons.shortRuleName;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelHomogeneousShuttle;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalCorrelate;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.util.Pair;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.sql.engine.hint.Hints;
import org.apache.ignite.internal.sql.engine.hint.IgniteHint;
import org.apache.ignite.internal.sql.engine.rel.IgniteConvention;
import org.apache.ignite.internal.sql.engine.rel.IgniteProject;
import org.apache.ignite.internal.sql.engine.rel.IgniteRel;
import org.apache.ignite.internal.sql.engine.trait.IgniteDistributions;

/**
 * Utility class that encapsulates the query optimization pipeline.
 */
public final class PlannerHelper {
    /**
     * Maximum number of tables in join supported for join order optimization.
     *
     * <p>If query joins more table than specified, then rules {@link CoreRules#JOIN_COMMUTE} and
     * {@link CoreRules#JOIN_COMMUTE_OUTER} will be disabled, tables will be joined in the order
     * of enumeration in the query.
     */
    private static final int MAX_SIZE_OF_JOIN_TO_OPTIMIZE = 5;

    private static final IgniteLogger LOG = Loggers.forClass(PlannerHelper.class);

    /**
     * Default constructor.
     */
    private PlannerHelper() {

    }

    /**
     * Optimizes a given query.
     *
     * <p>That is, it passes a given AST through the optimization pipeline,
     * applying different rule sets step by step, and returns the optimized
     * physical tree of Ignite relations.
     *
     * @param sqlNode Validated AST of the query to optimize.
     * @param planner A planner used to apply a rule set to the query tree.
     * @return An optimized physical tree of Ignite relations.
     */
    public static IgniteRel optimize(SqlNode sqlNode, IgnitePlanner planner) {
        try {
            // Convert to Relational operators graph
            RelRoot root = planner.rel(sqlNode);

            RelNode rel = root.rel;

            Hints hints = Hints.parse(root.hints);

            List<String> disableRuleParams = hints.params(DISABLE_RULE);
            if (!disableRuleParams.isEmpty()) {
                planner.setDisabledRules(Set.copyOf(disableRuleParams));
            }

            // Transformation chain
            rel = planner.transform(PlannerPhase.HEP_DECORRELATE, rel.getTraitSet(), rel);

            rel = RelOptUtil.propagateRelHints(rel, false);

            rel = planner.replaceCorrelatesCollisions(rel);

            rel = planner.trimUnusedFields(root.withRel(rel)).rel;

            rel = new SortedAlgorithmDisabler().visit(rel);

            boolean amountOfJoinsAreBig = hasTooMuchJoins(rel);
            boolean enforceJoinOrder = hints.present(ENFORCE_JOIN_ORDER);
            if (amountOfJoinsAreBig || enforceJoinOrder) {
                Set<String> disabledRules = new HashSet<>(disableRuleParams);

                disabledRules.add(shortRuleName(CoreRules.JOIN_COMMUTE));
                disabledRules.add(shortRuleName(CoreRules.JOIN_COMMUTE_OUTER));

                planner.setDisabledRules(Set.copyOf(disabledRules));
            }

            rel = planner.transform(PlannerPhase.HEP_FILTER_PUSH_DOWN, rel.getTraitSet(), rel);

            rel = planner.transform(PlannerPhase.HEP_PROJECT_PUSH_DOWN, rel.getTraitSet(), rel);

            {
                // the sole purpose of this code block is to limit scope of `simpleOperation` variable.
                // The result of `HEP_TO_SIMPLE_KEY_VALUE_OPERATION` phase MUST NOT be passed to next stage,
                // thus if result meets our expectation, then return the result, otherwise discard it and
                // proceed with regular flow
                RelNode simpleOperation = planner.transform(PlannerPhase.HEP_TO_SIMPLE_KEY_VALUE_OPERATION, rel.getTraitSet(), rel);

                if (simpleOperation instanceof IgniteRel) {
                    return (IgniteRel) simpleOperation;
                }
            }

            RelTraitSet desired = rel.getCluster().traitSet()
                    .replace(IgniteConvention.INSTANCE)
                    .replace(IgniteDistributions.single())
                    .replace(root.collation == null ? RelCollations.EMPTY : root.collation)
                    .simplify();

            IgniteRel igniteRel = planner.transform(PlannerPhase.OPTIMIZATION, desired, rel);

            if (!root.isRefTrivial()) {
                List<RexNode> projects = new ArrayList<>();
                RexBuilder rexBuilder = igniteRel.getCluster().getRexBuilder();

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

    private static boolean hasTooMuchJoins(RelNode rel) {
        JoinSizeFinder joinSizeFinder = new JoinSizeFinder();

        joinSizeFinder.visit(rel);

        return joinSizeFinder.sizeOfBiggestJoin() > MAX_SIZE_OF_JOIN_TO_OPTIMIZE;
    }

    /**
     * A shuttle to estimate a biggest join to optimize.
     *
     * <p>There are only two rules: <ol>
     *     <li>Each achievable leaf node contribute to join complexity, thus must be counted</li>
     *     <li>If this shuttle reach the {@link LogicalCorrelate} node, only left shoulder will be
     *         analysed by this shuttle. For right shoulder a new shuttle will be created, and maximum
     *         of previously found subquery and current one will be saved</li>
     * </ol>
     */
    private static class JoinSizeFinder extends RelHomogeneousShuttle {
        private int countOfSources = 0;
        private int maxCountOfSourcesInSubQuery = 0;

        /** {@inheritDoc} */
        @Override
        public RelNode visit(RelNode other) {
            if (other.getInputs().isEmpty()) {
                countOfSources++;

                return other;
            }

            return super.visit(other);
        }

        /** {@inheritDoc} */
        @Override
        public RelNode visit(LogicalCorrelate correlate) {
            JoinSizeFinder inSubquerySizeFinder = new JoinSizeFinder();
            inSubquerySizeFinder.visit(correlate.getInput(1));

            maxCountOfSourcesInSubQuery = Math.max(
                    maxCountOfSourcesInSubQuery,
                    inSubquerySizeFinder.sizeOfBiggestJoin()
            );

            return visitChild(correlate, 0, correlate.getInput(0));
        }

        int sizeOfBiggestJoin() {
            return Math.max(countOfSources, maxCountOfSourcesInSubQuery);
        }
    }

    /**
     * Traverses relational tree and assign {@link IgniteHint#DISABLE_SORTED_ALGORITHM} hint
     * to every {@link LogicalAggregate} and {@link LogicalJoin} node on right side of the
     * {@link LogicalCorrelate} node.
     *
     * <p>This is workaround of a deadlock caused by preserving sorting on a correlated path.
     *
     * <pre>
     * Legend:
     *      [f#1] -- fragment #1
     *      (n#1) -- node #1
     *      [f#2 (n#1) (n#2) ] -- fragment #2 mapped on nodes #1 and #2
     *
     * Problematic subtree:
     *       [f#1 (n#1) ]
     *            /   \
     *    [f#2 (n#1) (n#2) ]
     *           |  X  |
     *    [f#3 (n#1) (n#2) ]
     *
     * Problematic sequence of events:
     *      1) f#1n#1 sets the correlated variable and requests data from f#2.
     *      2) Request messages arrives at n#1 and n#2 accordingly. Since they are the first
     *      requests, the nodes start processing immediately.
     *      3) Processing of f#2 requires data to be pulled from f#3, thus requests will be sent.
     *      4) Here comes the first factor: fragment may process only one correlated request at a time.
     *      So, f#3 start processing of request from f#2n#1 immediately, while request from f#2n#2 will
     *      be postponed.
     *      5) f#3 prepares first batch of data, sends it to the f#2n#1 and waits until the next batch will be
     *      requested. Request from f#2n#2 still postponed until all data for correlate from f#2n#1 will be processed.
     *      6) f#2n#1 propagates received batch to f#1n#1 and waits for the next request.
     *      7) Here comes the second and final factor: since f#1n#1 expects data to be sorted, exchange between
     *      f#1 and f#2 will waits for batches from all parties (f#2n#1 and f#2n#2 in our case).
     *
     * </pre>
     */
    private static class SortedAlgorithmDisabler extends RelHomogeneousShuttle {
        private static final RelHint DISABLE_SORTED_ALGORITHM = RelHint.builder(IgniteHint.DISABLE_SORTED_ALGORITHM.name()).build();

        private int correlated = 0;

        @Override
        public RelNode visit(LogicalAggregate aggregate) {
            if (correlated > 0) {
                List<RelHint> hints = new ArrayList<>(aggregate.getHints());
                hints.add(DISABLE_SORTED_ALGORITHM);
                aggregate = (LogicalAggregate) aggregate.withHints(hints);
            }

            return super.visit(aggregate);
        }

        @Override
        public RelNode visit(LogicalJoin join) {
            if (correlated > 0) {
                List<RelHint> hints = new ArrayList<>(join.getHints());
                hints.add(DISABLE_SORTED_ALGORITHM);
                join = (LogicalJoin) join.withHints(hints);
            }

            return super.visit(join);
        }

        @Override
        public RelNode visit(LogicalCorrelate correlate) {
            RelNode output = visitChild(correlate, 0, correlate.getInput(0));

            correlated++;
            output = visitChild(output, 1, correlate.getInput(1));
            correlated--;

            return output;
        }
    }
}
