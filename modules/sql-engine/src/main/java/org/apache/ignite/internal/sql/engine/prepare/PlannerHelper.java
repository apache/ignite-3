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
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelHomogeneousShuttle;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.logical.LogicalCorrelate;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexSlot;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDynamicParam;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOrderBy;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlUtil;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
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
import org.apache.ignite.internal.sql.engine.rel.IgniteSelectCount;
import org.apache.ignite.internal.sql.engine.schema.ColumnDescriptor;
import org.apache.ignite.internal.sql.engine.schema.IgniteDataSource;
import org.apache.ignite.internal.sql.engine.schema.IgniteTable;
import org.apache.ignite.internal.sql.engine.schema.TableDescriptor;
import org.apache.ignite.internal.sql.engine.trait.IgniteDistributions;
import org.apache.ignite.internal.sql.engine.type.IgniteTypeFactory;
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

            if (ex.getClass() == RuntimeException.class && ex.getCause() instanceof SqlException) {
                SqlException sqlEx = (SqlException) ex.getCause();
                throw new SqlException(sqlEx.traceId(), sqlEx.code(), sqlEx.getMessage(), ex);
            }

            throw ex;
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

            if (SubQueryChecker.hasSubQuery(operand)) {
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

    private static class SubQueryChecker extends SqlShuttle {
        private static final SubQueryChecker INSTANCE = new SubQueryChecker();

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

    /**
     * Tries to optimize a query that looks like {@code SELECT count(*)}.
     *
     * @param planner Planner.
     * @param node Query node.
     * @return Plan node with list of aliases, if the optimization is applicable.
     */
    public static @Nullable Pair<IgniteRel, List<String>> tryOptimizeSelectCount(
            IgnitePlanner planner,
            SqlNode node
    ) {
        SqlSelect select = getSelectCountOptimizationNode(node);
        if (select == null) {
            return null;
        }

        assert select.getFrom() != null : "FROM is missing";

        IgniteSqlToRelConvertor converter = planner.sqlToRelConverter();

        // Convert PUBLIC.T AS X (a,b) to PUBLIC.T
        SqlNode from = SqlUtil.stripAs(select.getFrom());
        RelOptTable targetTable = converter.getTargetTable(from);

        IgniteDataSource dataSource = targetTable.unwrap(IgniteDataSource.class);
        if (!(dataSource instanceof IgniteTable)) {
            return null;
        }

        IgniteTypeFactory typeFactory = planner.getTypeFactory();

        // SELECT COUNT(*) ... row type
        RelDataType countResultType = typeFactory.createSqlType(SqlTypeName.BIGINT);

        // Build projection
        // Rewrites SELECT count(*) ... as Project(exprs = [lit, $0, ... ]), where $0 references a row that stores a count.
        // So we can feed results of get count operation into a projection to compute final results.

        List<RexNode> expressions = new ArrayList<>();
        List<String> expressionNames = new ArrayList<>();
        boolean countAdded = false;

        for (SqlNode selectItem : select.getSelectList()) {
            SqlNode expr = SqlUtil.stripAs(selectItem);

            if (isCountStar(expr)) {
                RexBuilder rexBuilder = planner.cluster().getRexBuilder();
                RexSlot countValRef = rexBuilder.makeInputRef(countResultType, 0);

                expressions.add(countValRef);

                countAdded = true;
            } else if (expr instanceof SqlLiteral || expr instanceof SqlDynamicParam) {
                RexNode rexNode = converter.convertExpression(expr);

                expressions.add(rexNode);
            } else {
                return null;
            }

            String alias = planner.validator().deriveAlias(selectItem, expressionNames.size());
            expressionNames.add(alias);
        }

        if (!countAdded) {
            return null;
        }

        IgniteSelectCount rel = new IgniteSelectCount(
                planner.cluster(),
                planner.cluster().traitSetOf(IgniteConvention.INSTANCE),
                targetTable,
                expressions
        );

        return new Pair<>(rel, expressionNames);
    }

    private static @Nullable SqlSelect getSelectCountOptimizationNode(SqlNode node) {
        // Unwrap SELECT .. from SELECT x FROM t ORDER BY ...
        if (node instanceof SqlOrderBy) {
            SqlOrderBy orderBy = (SqlOrderBy) node;

            // Skip ORDER BY with OFFSET/FETCH
            if (orderBy.fetch != null || orderBy.offset != null) {
                return null;
            }

            // Skip ORDER BY with non literals
            for (SqlNode arg : orderBy.orderList) {
                if (!SqlUtil.isLiteral(arg))  {
                    return null;
                }
            }

            assert orderBy.getOperandList().size() == 4 : "Expected 4 operands, but was " + orderBy.getOperandList().size();
            node = orderBy.query;
        }

        if (!(node instanceof SqlSelect)) {
            return null;
        }

        SqlSelect select = (SqlSelect) node;

        if (select.getGroup() != null
                || select.getFrom() == null
                || select.getWhere() != null
                || select.getHaving() != null
                || select.getQualify() != null
                || !select.getWindowList().isEmpty()
                || select.getOffset() != null
                || select.getFetch() != null) {

            return null;
        }

        // make sure that the following IF statement does not leave out any operand of the SELECT node
        assert select.getOperandList().size() == 12 : "Expected 12 operands, but was " + select.getOperandList().size();

        // Convert PUBLIC.T AS X (a,b) to PUBLIC.T
        SqlNode from = SqlUtil.stripAs(select.getFrom());
        // Skip non-references such as VALUES ..
        if (from.getKind() == SqlKind.IDENTIFIER) {
            return select;
        } else {
            return null;
        }
    }

    private static boolean isCountStar(SqlNode node) {
        if (!SqlUtil.isCallTo(node, SqlStdOperatorTable.COUNT)) {
            // The SQL node was checked by the validator and the call has correct number of arguments.
            return false;
        } else {
            SqlCall call = (SqlCall) node;
            // Reject COUNT(DISTINCT ...)
            if (call.getFunctionQuantifier() != null) {
                return false;
            }
            if (call.getOperandList().isEmpty()) {
                return false;
            }
            SqlNode operand = call.getOperandList().get(0);

            if (SqlUtil.isNull(operand)) {
                // COUNT(NULL) always returns 0
                return false;
            } else if (SqlUtil.isLiteral(operand)) {
                return true;
            } else {
                return operand instanceof SqlIdentifier && ((SqlIdentifier) operand).isStar();
            }
        }
    }
}
