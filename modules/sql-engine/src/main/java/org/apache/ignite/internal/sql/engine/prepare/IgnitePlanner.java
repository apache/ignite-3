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

import static java.util.Objects.requireNonNull;
import static org.apache.ignite.internal.util.CollectionUtils.nullOrEmpty;
import static org.apache.ignite.lang.ErrorGroups.Sql.STMT_PARSE_ERR;

import java.io.PrintWriter;
import java.io.Reader;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.apache.calcite.plan.Context;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptCostFactory;
import org.apache.calcite.plan.RelOptLattice;
import org.apache.calcite.plan.RelOptListener;
import org.apache.calcite.plan.RelOptMaterialization;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.RelHomogeneousShuttle;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.RelShuttle;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.logical.LogicalCorrelate;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.metadata.CachingRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexExecutor;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.SqlOrderBy;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlUtil;
import org.apache.calcite.sql.SqlWith;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.validate.SqlNonNullableAccessors;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql2rel.SqlRexConvertletTable;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Planner;
import org.apache.calcite.tools.Program;
import org.apache.calcite.util.Pair;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.sql.engine.metadata.IgniteMetadata;
import org.apache.ignite.internal.sql.engine.metadata.RelMetadataQueryEx;
import org.apache.ignite.internal.sql.engine.rex.IgniteRexBuilder;
import org.apache.ignite.internal.sql.engine.sql.IgniteSqlParser;
import org.apache.ignite.internal.sql.engine.sql.StatementParseResult;
import org.apache.ignite.internal.sql.engine.type.IgniteTypeFactory;
import org.apache.ignite.internal.sql.engine.util.Commons;
import org.apache.ignite.internal.util.FastTimestamps;
import org.apache.ignite.sql.SqlException;
import org.jetbrains.annotations.Nullable;

/**
 * Query planer.
 */
public class IgnitePlanner implements Planner, RelOptTable.ViewExpander {
    private final SqlOperatorTable operatorTbl;

    private final List<Program> programs;

    private final FrameworkConfig frameworkCfg;

    private final PlanningContext ctx;

    @SuppressWarnings("rawtypes")
    private final List<RelTraitDef> traitDefs;

    private final SqlParser.Config parserCfg;

    private final SqlToRelConverter.Config sqlToRelConverterCfg;

    private final SqlValidator.Config validatorCfg;

    private final SqlRexConvertletTable convertletTbl;

    private final RexBuilder rexBuilder;

    private final RexExecutor rexExecutor;

    private final IgniteTypeFactory typeFactory;

    private final CalciteCatalogReader catalogReader;

    private @Nullable SqlNode validatedSqlNode;

    private RelOptPlanner planner;

    private SqlValidator validator;

    private RelOptCluster cluster;

    /** Start planning timestamp in millis. */
    private final long startTs;

    static {
        warmup();
    }

    /**
     * Constructor.
     *
     * @param ctx Planner context.
     */
    IgnitePlanner(PlanningContext ctx) {
        this.ctx = ctx;

        typeFactory = ctx.typeFactory();
        catalogReader = ctx.catalogReader();
        operatorTbl = ctx.opTable();
        frameworkCfg = ctx.config();

        programs = frameworkCfg.getPrograms();
        parserCfg = frameworkCfg.getParserConfig();
        sqlToRelConverterCfg = frameworkCfg.getSqlToRelConverterConfig();
        validatorCfg = frameworkCfg.getSqlValidatorConfig();
        convertletTbl = frameworkCfg.getConvertletTable();
        rexExecutor = frameworkCfg.getExecutor();
        traitDefs = frameworkCfg.getTraitDefs();

        rexBuilder = IgniteRexBuilder.INSTANCE;
        startTs = FastTimestamps.coarseCurrentTimeMillis();
    }

    /** {@inheritDoc} */
    @Override
    public RelTraitSet getEmptyTraitSet() {
        return planner().emptyTraitSet();
    }

    /** {@inheritDoc} */
    @Override
    public void close() {
        reset();
    }

    /** {@inheritDoc} */
    @Override
    public void reset() {
        planner = null;
        validator = null;
        cluster = null;
    }

    /** {@inheritDoc} */
    @Override
    public SqlNode parse(Reader reader) throws SqlParseException {
        // This method is only used in tests.
        StatementParseResult parseResult = IgniteSqlParser.parse(reader, StatementParseResult.MODE);

        return parseResult.statement();
    }

    /** {@inheritDoc} */
    @Override
    public SqlNode validate(SqlNode sqlNode) {
        validatedSqlNode = validator().validate(sqlNode);
        return validatedSqlNode;
    }

    /** {@inheritDoc} */
    @Override
    public Pair<SqlNode, RelDataType> validateAndGetType(SqlNode sqlNode) {
        SqlNode validatedNode = validator().validate(sqlNode);
        RelDataType type = validator().getValidatedNodeType(validatedNode);
        this.validatedSqlNode = validatedNode;
        return Pair.of(validatedNode, type);
    }

    /** {@inheritDoc} */
    @Override
    public RelDataType getParameterRowType() {
        return requireNonNull(validator, "validator")
                .getParameterRowType(requireNonNull(validatedSqlNode, "validatedSqlNode"));
    }

    /**
     * Converts a SQL data type specification to a relational data type.
     *
     * @param typeSpec Spec to convert from.
     * @param nullable Whether the type is nullable if the type specification
     *      does not explicitly state.
     * @return Relational type representation of given SQL type.
     */
    public RelDataType convert(SqlDataTypeSpec typeSpec, boolean nullable) {
        // Validate data type first.
        validator().validateDataType(typeSpec);
        return typeSpec.deriveType(validator(), nullable);
    }

    /** {@inheritDoc} */
    @Override
    public RelNode convert(SqlNode sql) {
        throw new UnsupportedOperationException();
    }

    /** Derives hints from given relation node, if possible. */
    List<RelHint> deriveHints(SqlNode node) {
        SqlSelect select = null;
        if (node instanceof SqlSelect) {
            select = (SqlSelect) node;
        } else if (node instanceof SqlWith && ((SqlWith) node).body instanceof SqlSelect) {
            select = (SqlSelect) ((SqlWith) node).body;
        }

        if (select != null && select.hasHints()) {
            return SqlUtil.getRelHint(cluster.getHintStrategies(), select.getHints());
        }

        return List.of();
    }

    /**
     * Preload some classes so that the time spent is not taken
     * into account when measuring query planning timeout.
     */
    static void warmup() {
        //noinspection ResultOfMethodCallIgnored
        PlannerPhase.values();
    }

    /**
     * Validates a SQL statement and tries to build aliases for appropriate column names.
     *
     * @param sqlNode Root node of the SQL parse tree.
     * @return Validated node, its validated type and type's origins.
     */
    ValidationResult validateAndGetTypeMetadata(SqlNode sqlNode) {
        List<SqlNode> selectItems = null;
        List<SqlNode> selectItemsNoStar = null;
        SqlNode sqlNode0 = sqlNode instanceof SqlOrderBy ? ((SqlOrderBy) sqlNode).query : sqlNode;

        boolean starFound = false;

        if (sqlNode0 instanceof SqlSelect) {
            selectItems = SqlNonNullableAccessors.getSelectList((SqlSelect) sqlNode0);
            selectItemsNoStar = new ArrayList<>(selectItems.size());

            for (SqlNode node : selectItems) {
                if (node instanceof SqlIdentifier) {
                    SqlIdentifier id = (SqlIdentifier) node;
                    if (id.isStar()) {
                        starFound = true;
                    }
                } else {
                    if (node instanceof SqlBasicCall) {
                        SqlBasicCall node0 = (SqlBasicCall) node;
                        if (!identAsIdent(node0)) {
                            selectItemsNoStar.add(node);
                        }
                    } else {
                        selectItemsNoStar.add(node);
                    }
                }
            }
        }

        SqlNode validatedNode = validator().validate(sqlNode);
        RelDataType type = validator().getValidatedNodeType(validatedNode);
        List<List<String>> origins = validator().getFieldOrigins(validatedNode);

        List<String> derived = null;
        if (validatedNode instanceof SqlSelect && selectItems != null && !selectItemsNoStar.isEmpty()) {
            derived = new ArrayList<>(selectItems.size());

            if (starFound) {
                SqlNodeList expandedItems = ((SqlSelect) validatedNode).getSelectList();

                int resolved = 0;
                for (SqlNode node : expandedItems) {
                    if (node instanceof SqlIdentifier) {
                        derived.add(null);

                        continue;
                    }

                    if (node instanceof SqlBasicCall) {
                        SqlBasicCall node0 = (SqlBasicCall) node;

                        // case like: "select *, *" second star columns will be transformed into SqlBasicCall and AS
                        if (identAsIdent(node0)) {
                            derived.add(null);

                            continue;
                        }
                    }

                    Objects.checkIndex(resolved, selectItemsNoStar.size());
                    derived.add(validator().deriveAlias(selectItemsNoStar.get(resolved), resolved));
                    ++resolved;
                }
            } else {
                int cnt = 0;

                for (SqlNode node : selectItems) {
                    derived.add(validator().deriveAlias(node, cnt++));
                }
            }
        }

        this.validatedSqlNode = validatedNode;

        return new ValidationResult(validatedNode, type, origins, derived == null ? List.of() : derived);
    }

    private boolean identAsIdent(SqlBasicCall node) {
        return node.operandCount() == 2 && node.getKind() == SqlKind.AS
                && node.operand(0) instanceof SqlIdentifier
                && node.operand(1) instanceof SqlIdentifier;
    }

    /** {@inheritDoc} */
    @Override
    public RelRoot rel(SqlNode sql) {
        SqlToRelConverter sqlToRelConverter = sqlToRelConverter();

        return sqlToRelConverter.convertQuery(sql, false, true);
    }

    /** {@inheritDoc} */
    @Override
    public RelRoot expandView(
            RelDataType rowType,
            String qryStr,
            List<String> schemaPath,
            List<String> viewPath
    ) {
        SqlParser parser = SqlParser.create(qryStr, parserCfg);
        SqlNode sqlNode;
        try {
            sqlNode = parser.parseQuery();
        } catch (SqlParseException e) {
            throw new SqlException(STMT_PARSE_ERR, "parse failed", e);
        }

        CalciteCatalogReader catalogReader = this.catalogReader.withSchemaPath(schemaPath);
        SqlValidator validator = new IgniteSqlValidator(operatorTbl, catalogReader, typeFactory, validatorCfg, ctx.parameters());
        SqlToRelConverter sqlToRelConverter = sqlToRelConverter(validator, catalogReader, sqlToRelConverterCfg);
        RelRoot root = sqlToRelConverter.convertQuery(sqlNode, true, false);
        root = root.withRel(sqlToRelConverter.decorrelate(sqlNode, root.rel));

        return trimUnusedFields(root);
    }

    /** {@inheritDoc} */
    @Override
    public RelNode transform(int programIdx, RelTraitSet targetTraits, RelNode rel) {
        return programs.get(programIdx).run(planner(), rel, targetTraits.simplify(), materializations(), latices());
    }

    /**
     * Converts one relational nodes tree into another relational nodes tree based on a particular planner type, planning phase and required
     * set of traits.
     *
     * @param phase        Planner phase.
     * @param targetTraits Target traits.
     * @param rel          Root node of relational tree.
     * @return The root of the new RelNode tree.
     */
    public <T extends RelNode> T transform(PlannerPhase phase, RelTraitSet targetTraits, RelNode rel) {
        return (T) phase.getProgram(ctx).run(planner(), rel, targetTraits.simplify(), materializations(), latices());
    }

    /** {@inheritDoc} */
    @Override
    public IgniteTypeFactory getTypeFactory() {
        return typeFactory;
    }

    private RelOptPlanner planner() {
        if (planner == null) {
            VolcanoPlannerExt planner = new VolcanoPlannerExt(frameworkCfg.getCostFactory(), ctx, startTs);
            planner.setExecutor(rexExecutor);
            this.planner = planner;

            for (RelTraitDef<?> def : traitDefs) {
                this.planner.addRelTraitDef(def);
            }
        }

        return planner;
    }

    /**
     * Adds a rule listener to this planner.
     *
     * @param newListener new listener to be notified of events
     */
    public void addListener(RelOptListener newListener) {
        planner().addListener(newListener);
    }

    /**
     * Dump.
     * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
     */
    public String dump() {
        StringWriter w = new StringWriter();

        ((VolcanoPlanner) planner).dump(new PrintWriter(w));

        return w.toString();
    }

    /** Returns the validator. **/
    public SqlValidator validator() {
        if (validator == null) {
            validator = new IgniteSqlValidator(operatorTbl, catalogReader, typeFactory, validatorCfg, ctx.parameters());
        }

        return validator;
    }

    /** Creates a cluster. */
    RelOptCluster cluster() {
        if (cluster == null) {
            cluster = RelOptCluster.create(planner(), rexBuilder);
            cluster.setMetadataProvider(new CachingRelMetadataProvider(IgniteMetadata.METADATA_PROVIDER, planner()));
            cluster.setMetadataQuerySupplier(RelMetadataQueryEx::create);
        }

        return cluster;
    }

    private List<RelOptLattice> latices() {
        return List.of(); // TODO
    }

    /**
     * Returns all applicable materializations (i.e. secondary indexes) for the given rel node.
     *
     * @return Materializations.
     */
    private List<RelOptMaterialization> materializations() {
        return List.of(); // TODO
    }

    /**
     * Walks over a tree of relational expressions, replacing each {@link org.apache.calcite.rel.RelNode} with a
     * 'slimmed down' relational expression that projects only the columns required by its consumer.
     *
     * @param root Root of relational expression tree
     * @return Trimmed relational expression
     */
    protected RelRoot trimUnusedFields(RelRoot root) {
        // For now, don't trim if there are more than 3 joins. The projects
        // near the leaves created by trim migrate past joins and seem to
        // prevent join-reordering.
        final SqlToRelConverter.Config config = sqlToRelConverterCfg
                .withExpand(false)
                .withTrimUnusedFields(true);
        SqlToRelConverter converter = sqlToRelConverter(validator(), catalogReader, config);
        boolean ordered = !root.collation.getFieldCollations().isEmpty();
        boolean dml = SqlKind.DML.contains(root.kind);
        return root.withRel(converter.trimUnusedFields(dml || ordered, root.rel));
    }

    /**
     * When rewriting sub-queries to {@code LogicalCorrelate} instances, correlate nodes with the same correlation ids
     * can be created (if there was more then one sub-query using the same correlate table). It's not a problem, when
     * rows are processed one by one (like in the enumerable convension), but Ignite execution nodes process batches
     * of rows, and execution nodes in some cases can get unexpected values for correlated variables.
     *
     * <p>This method replaces collisions by variables in correlates. For the left hand of LogicalCorrelate duplicated
     * correlated variable and it's usages replaced with the new one. For example:
     *
     * <p>LogicalCorrelate(correlation=[$cor0])                       LogicalCorrelate(correlation=[$cor0])
     *   LogicalCorrelate(correlation=[$cor0])    transforms to      LogicalCorrelate(correlation=[$cor1])
     *     ... condition=[=($cor0.A, $0)] ...                          ... condition=[=($cor1.A, $0)] ...
     *   ... condition=[=($cor0.A, $0)] ...                          ... condition=[=($cor0.A, $0)] ...
     *
     * <p>For the right hand of LogicalCorrelate duplicated LogicalCorrelate is just replaced with regular join.
     * For example:
     *
     * <p>LogicalCorrelate(correlation=[$cor0])                       LogicalCorrelate(correlation=[$cor0])
     *   ...                                      transforms to      ...
     *   LogicalCorrelate(correlation=[$cor0])                       LogicalJoin(condition=true)
     *
     * @param rel Relational expression tree.
     * @return Relational expression without collisions in correlates.
     */
    public RelNode replaceCorrelatesCollisions(RelNode rel) {
        RelShuttle relShuttle = new RelHomogeneousShuttle() {
            /** Set of used correlates. */
            private final Set<CorrelationId> usedSet = new HashSet<>();

            /** Map to find correlates, that should be replaced (in the left hand of correlate). */
            private final Map<CorrelationId, CorrelationId> replaceMap = new HashMap<>();

            /** Multiset to find correlates, that should be removed (in the right hand of correlate). */
            private final Map<CorrelationId, Integer> removeMap = new HashMap<>();

            private final RexShuttle rexShuttle = new RexShuttle() {
                @Override public RexNode visitCorrelVariable(RexCorrelVariable variable) {
                    CorrelationId newCorId = replaceMap.get(variable.id);

                    if (newCorId != null) {
                        return cluster().getRexBuilder().makeCorrel(variable.getType(), newCorId);
                    } else {
                        return variable;
                    }
                }
            };

            /** {@inheritDoc} */
            @Override public RelNode visit(LogicalCorrelate correlate) {
                CorrelationId corId = correlate.getCorrelationId();

                if (usedSet.contains(corId)) {
                    if (removeMap.containsKey(corId)) {
                        // We are in the right hand of correlate by corId: replace correlate with join.
                        RelNode join = LogicalJoin.create(
                                correlate.getLeft(),
                                correlate.getRight(),
                                List.of(),
                                cluster().getRexBuilder().makeLiteral(true),
                                Set.of(),
                                correlate.getJoinType()
                        );

                        return super.visit(join);
                    } else {
                        // We are in the right hand of correlate by corId: replace correlate variable.
                        CorrelationId newCorId = cluster().createCorrel();
                        CorrelationId oldCorId = replaceMap.put(corId, newCorId);

                        try {
                            correlate = correlate.copy(
                                    correlate.getTraitSet(),
                                    correlate.getLeft(),
                                    correlate.getRight(),
                                    newCorId,
                                    correlate.getRequiredColumns(),
                                    correlate.getJoinType()
                            );

                            return visitLeftAndRightCorrelateHands(correlate, corId);
                        } finally {
                            if (oldCorId == null) {
                                replaceMap.remove(corId);
                            } else {
                                replaceMap.put(corId, oldCorId);
                            }
                        }
                    }
                } else {
                    usedSet.add(corId);

                    return visitLeftAndRightCorrelateHands(correlate, corId);
                }
            }

            /** {@inheritDoc} */
            @Override
            public RelNode visit(RelNode other) {
                RelNode next = super.visit(other);

                return replaceMap.isEmpty() ? next : next.accept(rexShuttle);
            }

            private RelNode visitLeftAndRightCorrelateHands(LogicalCorrelate correlate, CorrelationId corId) {
                RelNode node = correlate;

                node = visitChild(node, 0, correlate.getLeft());

                removeMap.compute(corId, (k, v) -> v == null ? 1 : v + 1);

                try {
                    node = visitChild(node, 1, correlate.getRight());
                } finally {
                    removeMap.compute(corId, (k, v) -> v == 1 ? null : v - 1);
                }

                return node;
            }
        };

        return relShuttle.visit(rel);
    }

    private IgniteSqlToRelConverter sqlToRelConverter(SqlValidator validator, CalciteCatalogReader reader,
            SqlToRelConverter.Config config) {
        return new IgniteSqlToRelConverter(this, validator, reader, cluster(), convertletTbl, config);
    }

    /** Returns converter from ast to rel node tree. */
    public IgniteSqlToRelConverter sqlToRelConverter() {
        return sqlToRelConverter(validator(), catalogReader, sqlToRelConverterCfg);
    }

    /**
     * Adds given rule to list of exclusion.
     *
     * @param ruleName Name of the rule to exclude. The name can be derived from rule by {@link Commons#shortRuleName(RelOptRule)}.
     */
    public void disableRule(String ruleName) {
        ctx.disableRule(ruleName);
    }

    /**
     * Adds all rules with name from given collections to list of exclusion.
     *
     * @param ruleNamesToDisable Names of the rules to exclude. The name can be derived from rule by
     *         {@link Commons#shortRuleName(RelOptRule)}.
     */
    public void disableRules(Collection<String> ruleNamesToDisable) {
        if (nullOrEmpty(ruleNamesToDisable)) {
            return;
        }

        ruleNamesToDisable.forEach(this::disableRule);
    }

    private static class VolcanoPlannerExt extends VolcanoPlanner {
        private static final IgniteLogger LOG = Loggers.forClass(IgnitePlanner.class);

        private final long startTs;

        protected VolcanoPlannerExt(RelOptCostFactory costFactory, Context externalCtx, long startTs) {
            super(costFactory, externalCtx);
            setTopDownOpt(true);

            this.startTs = startTs;
        }

        /** {@inheritDoc} */
        @Override
        public RelOptCost getCost(RelNode rel, RelMetadataQuery mq) {
            return mq.getCumulativeCost(rel);
        }

        /** {@inheritDoc} */
        @Override
        public void checkCancel() {
            PlanningContext ctx = getContext().unwrap(PlanningContext.class);

            long timeout = ctx.plannerTimeout();

            if (timeout > 0) {
                if (FastTimestamps.coarseCurrentTimeMillis() - startTs > timeout) {
                    LOG.debug("Planning of a query aborted due to planner timeout threshold is reached [timeout={}, query={}]",
                            timeout,
                            ctx.query());

                    ctx.abortByTimeout();
                    cancelFlag.set(true);
                }
            }

            super.checkCancel();
        }
    }
}
