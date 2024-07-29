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
import static org.apache.ignite.internal.sql.engine.util.Commons.fastQueryOptimizationEnabled;
import static org.apache.ignite.internal.sql.engine.util.Commons.shortRuleName;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.calcite.plan.RelOptPlanner.CannotPlanException;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelHomogeneousShuttle;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.logical.LogicalCorrelate;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.util.SqlShuttle;
import org.apache.calcite.util.ControlFlowException;
import org.apache.calcite.util.Pair;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.sql.engine.hint.Hints;
import org.apache.ignite.internal.sql.engine.rel.IgniteConvention;
import org.apache.ignite.internal.sql.engine.rel.IgniteKeyValueModify;
import org.apache.ignite.internal.sql.engine.rel.IgniteKeyValueModify.Operation;
import org.apache.ignite.internal.sql.engine.rel.IgniteProject;
import org.apache.ignite.internal.sql.engine.rel.IgniteRel;
import org.apache.ignite.internal.sql.engine.schema.ColumnDescriptor;
import org.apache.ignite.internal.sql.engine.schema.IgniteTable;
import org.apache.ignite.internal.sql.engine.schema.TableDescriptor;
import org.apache.ignite.internal.sql.engine.trait.IgniteDistributions;
import org.apache.ignite.lang.ErrorGroups.Common;
import org.apache.ignite.sql.SqlException;
import org.jetbrains.annotations.Nullable;

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
            IgniteRel result = tryOptimizeFast(sqlNode, planner);

            if (result != null) {
                return result;
            }

            // Convert to Relational operators graph
            RelRoot root = planner.rel(sqlNode);

            RelNode rel = root.rel;

            Hints hints = Hints.parse(root.hints);

            List<String> disableRuleParams = hints.params(DISABLE_RULE);
            if (!disableRuleParams.isEmpty()) {
                planner.setDisabledRules(Set.copyOf(disableRuleParams));
            }

            // Transformation chain
            rel = planner.transform(PlannerPhase.HEP_SUBQUERIES_TO_CORRELATES, rel.getTraitSet(), rel);

            rel = RelOptUtil.propagateRelHints(rel, false);

            rel = planner.replaceCorrelatesCollisions(rel);

            rel = planner.trimUnusedFields(root.withRel(rel)).rel;

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

            result = planner.transform(PlannerPhase.OPTIMIZATION, desired, rel);

            if (!root.isRefTrivial()) {
                List<RexNode> projects = new ArrayList<>();
                RexBuilder rexBuilder = result.getCluster().getRexBuilder();

                for (int field : Pair.left(root.fields)) {
                    projects.add(rexBuilder.makeInputRef(result, field));
                }

                result = new IgniteProject(result.getCluster(), desired, result, projects, root.validatedRowType);
            }

            return result;
        } catch (Throwable ex) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Unexpected error at query optimizer", ex);
                LOG.debug(planner.dump());
            }

            if (ex instanceof CannotPlanException) {
                throw ex;
            } else if (ex.getClass() == RuntimeException.class && ex.getCause() instanceof SqlException) {
                SqlException sqlEx = (SqlException) ex.getCause();
                throw new SqlException(sqlEx.traceId(), sqlEx.code(), sqlEx.getMessage(), ex);
            } else {
                throw new SqlException(Common.INTERNAL_ERR, "Unable to optimize plan due to internal error", ex);
            }
        }
    }

    private static @Nullable IgniteRel tryOptimizeFast(SqlNode sqlNode, IgnitePlanner planner) {
        if (!fastQueryOptimizationEnabled()) {
            return null;
        }

        if (sqlNode instanceof SqlInsert) {
            return tryOptimizeInsert((SqlInsert) sqlNode, planner);
        }

        return null;
    }

    private static @Nullable IgniteRel tryOptimizeInsert(SqlInsert insertNode, IgnitePlanner planner) {
        SqlNode sourceNode = insertNode.getSource();

        if (!(sourceNode instanceof SqlBasicCall && sourceNode.getKind() == SqlKind.VALUES)) {
            // only simple `INSERT INTO ... VALUES (...)` statement is supported at the moment
            return null;
        }

        List<SqlNode> rowConstructors = ((SqlBasicCall) sourceNode).getOperandList();

        if (rowConstructors.size() != 1) {
            // multirow insert currently are not supported by IgniteKeyValueModify 
            return null;
        }

        IgniteSqlToRelConvertor converter = planner.sqlToRelConverter();
        RelOptTable targetTable = converter.getTargetTable(insertNode);
        IgniteTable igniteTable = targetTable.unwrap(IgniteTable.class);

        assert igniteTable != null;

        TableDescriptor descriptor = igniteTable.descriptor();
        SqlBasicCall rowConstructor = (SqlBasicCall) rowConstructors.get(0);

        Map<String, RexNode> columnToExpression = new HashMap<>();
        for (int i = 0; i < rowConstructor.getOperandList().size(); i++) {
            String columnName = ((SqlIdentifier) insertNode.getTargetColumnList().get(i)).getSimple();
            SqlNode operand = rowConstructor.operand(i);

            if (operand.getKind() == SqlKind.DEFAULT) {
                // We don't need special processing for explicit default.
                // Let's just skip it, default value will be resolved in the
                // next for-loop
                continue;
            }

            if (SuqQueryChecker.hasSubQuery(operand)) {
                // can't deal with sub-query
                return null;
            }

            RexNode expression = converter.convertExpression(operand);

            columnToExpression.put(columnName, expression);
        }

        List<RexNode> expressions = new ArrayList<>();
        for (ColumnDescriptor column : descriptor) {
            if (column.virtual()) {
                continue;
            }

            RexNode expression = columnToExpression.get(column.name());

            if (expression == null) {
                expression = descriptor.newColumnDefaultValue(targetTable, column.logicalIndex(), converter);
            }

            expressions.add(expression);
        }

        return new IgniteKeyValueModify(
                planner.cluster(),
                planner.cluster().traitSetOf(IgniteConvention.INSTANCE),
                targetTable,
                Operation.PUT,
                expressions
        );
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

    private static class SuqQueryChecker extends SqlShuttle {
        private static final SuqQueryChecker INSTANCE = new SuqQueryChecker();

        static boolean hasSubQuery(SqlNode node) {
            try {
                node.accept(INSTANCE);
            } catch (ControlFlowException e) {
                return true;
            }

            return false;
        }

        @Override public @Nullable SqlNode visit(SqlCall call) {
            if (call.getKind() == SqlKind.SCALAR_QUERY) {
                throw new ControlFlowException();
            }

            return super.visit(call);
        }
    }
}
