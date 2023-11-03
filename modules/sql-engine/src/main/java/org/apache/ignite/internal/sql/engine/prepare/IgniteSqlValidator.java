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

import static org.apache.calcite.sql.type.SqlTypeName.INT_TYPES;
import static org.apache.calcite.sql.type.SqlTypeUtil.isNull;
import static org.apache.calcite.util.Static.RESOURCE;
import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;
import static org.apache.ignite.lang.ErrorGroups.Sql.STMT_PARSE_ERR;

import java.math.BigDecimal;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.JoinConditionType;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlDelete;
import org.apache.calcite.sql.SqlDynamicParam;
import org.apache.calcite.sql.SqlExplain;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlMerge;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlUpdate;
import org.apache.calcite.sql.SqlUtil;
import org.apache.calcite.sql.dialect.CalciteSqlDialect;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.sql.util.SqlBasicVisitor;
import org.apache.calcite.sql.validate.SelectScope;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorImpl;
import org.apache.calcite.sql.validate.SqlValidatorNamespace;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.sql.validate.SqlValidatorTable;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.util.Util;
import org.apache.ignite.internal.sql.engine.schema.IgniteDataSource;
import org.apache.ignite.internal.sql.engine.schema.IgniteSystemView;
import org.apache.ignite.internal.sql.engine.schema.IgniteTable;
import org.apache.ignite.internal.sql.engine.schema.TableDescriptor;
import org.apache.ignite.internal.sql.engine.type.IgniteCustomType;
import org.apache.ignite.internal.sql.engine.type.IgniteCustomTypeCoercionRules;
import org.apache.ignite.internal.sql.engine.type.IgniteTypeFactory;
import org.apache.ignite.internal.sql.engine.type.UuidType;
import org.apache.ignite.internal.sql.engine.util.Commons;
import org.apache.ignite.internal.sql.engine.util.IgniteResource;
import org.apache.ignite.internal.sql.engine.util.TypeUtils;
import org.apache.ignite.sql.SqlException;
import org.jetbrains.annotations.Nullable;

/** Validator. */
public class IgniteSqlValidator extends SqlValidatorImpl {
    /** Decimal of Integer.MAX_VALUE for fetch/offset bounding. */
    private static final BigDecimal DEC_INT_MAX = BigDecimal.valueOf(Integer.MAX_VALUE);

    public static final int MAX_LENGTH_OF_ALIASES = 256;

    private static final Set<SqlKind> HUMAN_READABLE_ALIASES_FOR;

    public static final String NUMERIC_FIELD_OVERFLOW_ERROR = "Numeric field overflow";

    static {
        EnumSet<SqlKind> kinds = EnumSet.noneOf(SqlKind.class);

        kinds.addAll(SqlKind.AGGREGATE);
        kinds.addAll(SqlKind.BINARY_ARITHMETIC);
        kinds.addAll(SqlKind.FUNCTION);

        kinds.add(SqlKind.CEIL);
        kinds.add(SqlKind.FLOOR);
        kinds.add(SqlKind.LITERAL);

        kinds.add(SqlKind.PROCEDURE_CALL);

        HUMAN_READABLE_ALIASES_FOR = Collections.unmodifiableSet(kinds);
    }

    /** Dynamic parameter values. */
    private final Object[] dynamicParamValues;

    /** Dynamic parameters SQL AST nodes for invariant checks - see {@link #validateInferredDynamicParameters()}. */
    private final SqlDynamicParam[] dynamicParamNodes;

    /**
     * Creates a validator.
     *
     * @param opTab         Operator table
     * @param catalogReader Catalog reader
     * @param typeFactory   Type factory
     * @param config        Config
     * @param parameters    Dynamic parameters
     */
    public IgniteSqlValidator(SqlOperatorTable opTab, CalciteCatalogReader catalogReader,
            IgniteTypeFactory typeFactory, SqlValidator.Config config, Object[] parameters) {
        super(opTab, catalogReader, typeFactory, config);

        this.dynamicParamValues = parameters;
        this.dynamicParamNodes = new SqlDynamicParam[parameters.length];
    }

    /** {@inheritDoc} */
    @Override
    public SqlNode validate(SqlNode topNode) {
        SqlNode result;

        // Calcite fails to validate a query when its top node is EXPLAIN PLAN FOR
        // java.lang.NullPointerException: namespace for <query>
        // at org.apache.calcite.sql.validate.SqlValidatorImpl.getNamespaceOrThrow(SqlValidatorImpl.java:1280)
        if (topNode instanceof SqlExplain) {
            SqlExplain explainNode = (SqlExplain) topNode;
            SqlNode topNodeToValidate = explainNode.getExplicandum();

            SqlNode validatedNode = super.validate(topNodeToValidate);
            explainNode.setOperand(0, validatedNode);
            result = explainNode;
        } else {
            result = super.validate(topNode);
        }

        validateInferredDynamicParameters();

        return result;
    }

    /** {@inheritDoc} */
    @Override
    public void validateInsert(SqlInsert insert) {
        SqlValidatorTable table = table(validatedNamespace(insert, unknownType));
        IgniteTable igniteTable = getIgniteTableForModification((SqlIdentifier) insert.getTargetTable(), table);

        if (insert.getTargetColumnList() == null) {
            insert.setOperand(3, inferColumnList(igniteTable));
        }

        super.validateInsert(insert);
    }

    /** {@inheritDoc} */
    @Override
    public void validateUpdate(SqlUpdate call) {
        validateUpdateFields(call);

        super.validateUpdate(call);

        SqlSelect select = call.getSourceSelect();
        assert select != null : "Update: SourceSelect has not been set";

        // Update creates a source expression list which is not updated
        // after type coercion adds CASTs to source expressions.
        syncSelectList(select, call);
    }

    /** {@inheritDoc} */
    @Override
    public void validateMerge(SqlMerge call) {
        super.validateMerge(call);

        SqlSelect select = call.getSourceSelect();
        SqlUpdate update = call.getUpdateCall();

        if (update != null) {
            assert select != null : "Merge: SourceSelect has not been set";

            // Merge creates a source expression list which is not updated after type coercion adds CASTs
            // to source expressions in Update.
            syncSelectList(select, update);
        }
    }

    private IgniteTable getTableForModification(SqlIdentifier identifier) {
        SqlValidatorTable table = getCatalogReader().getTable(identifier.names);

        if (table == null) {
            throw newValidationError(identifier, RESOURCE.objectNotFound(identifier.toString()));
        }

        return getIgniteTableForModification(identifier, table);
    }

    private IgniteTable getIgniteTableForModification(SqlIdentifier identifier, SqlValidatorTable table) {
        IgniteDataSource dataSource = table.unwrap(IgniteDataSource.class);
        assert dataSource != null;

        if (dataSource instanceof IgniteSystemView) {
            throw newValidationError(identifier, IgniteResource.INSTANCE.systemViewIsNotModifiable(identifier.toString()));
        }

        return (IgniteTable) dataSource;
    }

    private static void syncSelectList(SqlSelect select, SqlUpdate update) {
        //
        // If a table has N columns and update::SourceExpressionList has size = M
        // then select::SelectList has size = N + M:
        // col1, ... colN, value_expr1, ..., value_exprM
        //
        SqlNodeList sourceExpressionList = update.getSourceExpressionList();
        SqlNodeList selectList = select.getSelectList();
        int sourceExprListSize = sourceExpressionList.size();
        int startPosition = selectList.size() - sourceExprListSize;

        for (var i = 0; i < sourceExprListSize; i++) {
            SqlNode sourceExpr = sourceExpressionList.get(i);
            int position = startPosition + i;
            selectList.set(position, sourceExpr);
        }
    }

    /** {@inheritDoc} */
    @Override
    public void validateLiteral(SqlLiteral literal) {
        if (literal.getTypeName() != SqlTypeName.DECIMAL) {
            super.validateLiteral(literal);
        }
    }

    /** {@inheritDoc} */
    @Override
    protected SqlSelect createSourceSelectForUpdate(SqlUpdate call) {
        final SqlNodeList selectList = new SqlNodeList(SqlParserPos.ZERO);
        final SqlIdentifier targetTable = (SqlIdentifier) call.getTargetTable();

        IgniteTable igniteTable = getTableForModification(targetTable);
        TableDescriptor descriptor = igniteTable.descriptor();

        SqlIdentifier alias = call.getAlias() != null ? call.getAlias() :
                new SqlIdentifier(deriveAlias(targetTable, 0), SqlParserPos.ZERO);

        descriptor.selectForUpdateRowType((IgniteTypeFactory) typeFactory)
                .getFieldNames().stream()
                .map(name -> alias.plus(name, SqlParserPos.ZERO))
                .forEach(selectList::add);

        int ordinal = 0;
        // Force unique aliases to avoid a duplicate for Y with SET X=Y
        for (SqlNode exp : call.getSourceExpressionList()) {
            selectList.add(SqlValidatorUtil.addAlias(exp, SqlUtil.deriveAliasFromOrdinal(ordinal++)));
        }

        SqlNode sourceTable = call.getTargetTable();

        if (call.getAlias() != null) {
            sourceTable =
                    SqlValidatorUtil.addAlias(
                            sourceTable,
                            call.getAlias().getSimple());
        }

        return new SqlSelect(SqlParserPos.ZERO, null, selectList, sourceTable,
                call.getCondition(), null, null, null, null, null, null, null, null);
    }

    /** {@inheritDoc} */
    @Override protected void addToSelectList(List<SqlNode> list, Set<String> aliases,
            List<Map.Entry<String, RelDataType>> fieldList, SqlNode exp, SelectScope scope, boolean includeSystemVars) {
        if (includeSystemVars || exp.getKind() != SqlKind.IDENTIFIER || !isSystemFieldName(deriveAlias(exp, 0))) {
            super.addToSelectList(list, aliases, fieldList, exp, scope, includeSystemVars);
        }
    }

    /** {@inheritDoc} */
    @Override
    protected SqlSelect createSourceSelectForDelete(SqlDelete call) {
        final SqlNodeList selectList = new SqlNodeList(SqlParserPos.ZERO);
        final SqlIdentifier targetTable = (SqlIdentifier) call.getTargetTable();

        IgniteTable igniteTable = getTableForModification(targetTable);
        TableDescriptor descriptor = igniteTable.descriptor();

        descriptor.deleteRowType((IgniteTypeFactory) typeFactory)
                .getFieldNames().stream()
                .map(name -> new SqlIdentifier(name, SqlParserPos.ZERO))
                .forEach(selectList::add);

        SqlNode sourceTable = call.getTargetTable();

        if (call.getAlias() != null) {
            sourceTable =
                    SqlValidatorUtil.addAlias(
                            sourceTable,
                            call.getAlias().getSimple());
        }

        return new SqlSelect(SqlParserPos.ZERO, null, selectList, sourceTable,
                call.getCondition(), null, null, null, null, null, null, null, null);
    }

    /** {@inheritDoc} */
    @Override
    protected void validateSelect(SqlSelect select, RelDataType targetRowType) {
        super.validateSelect(select, targetRowType);

        checkIntegerLimit(select.getFetch(), "fetch / limit");
        checkIntegerLimit(select.getOffset(), "offset");
    }

    /**
     * Check integer limit.
     * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
     *
     * @param n        Node to check limit.
     * @param nodeName Node name.
     */
    private void checkIntegerLimit(@Nullable SqlNode n, String nodeName) {
        if (n == null) {
            return;
        }

        if (n instanceof SqlLiteral) {
            BigDecimal offFetchLimit = ((SqlLiteral) n).bigDecimalValue();

            if (offFetchLimit.compareTo(DEC_INT_MAX) > 0 || offFetchLimit.compareTo(BigDecimal.ZERO) < 0) {
                throw newValidationError(n, IgniteResource.INSTANCE.correctIntegerLimit(nodeName));
            }
        } else if (n instanceof SqlDynamicParam) {
            Object param = getDynamicParamValue((SqlDynamicParam) n);

            if (param instanceof Integer) {
                if ((Integer) param < 0) {
                    throw newValidationError(n, IgniteResource.INSTANCE.correctIntegerLimit(nodeName));
                }
            }
        }
    }

    /** {@inheritDoc} */
    @Override
    public String deriveAlias(SqlNode node, int ordinal) {
        if (node.isA(HUMAN_READABLE_ALIASES_FOR)) {
            String alias = node.toSqlString(c -> c.withDialect(CalciteSqlDialect.DEFAULT)
                    .withQuoteAllIdentifiers(false)
                    .withAlwaysUseParentheses(false)
                    .withClauseStartsLine(false)
            ).getSql();

            return alias.substring(0, Math.min(alias.length(), MAX_LENGTH_OF_ALIASES));
        }

        return super.deriveAlias(node, ordinal);
    }

    /** {@inheritDoc} */
    @Override
    public void validateAggregateParams(SqlCall aggCall,
            @Nullable SqlNode filter, @Nullable SqlNodeList distinctList,
            @Nullable SqlNodeList orderList, SqlValidatorScope scope) {
        validateAggregateFunction(aggCall, (SqlAggFunction) aggCall.getOperator());

        super.validateAggregateParams(aggCall, filter, null, orderList, scope);
    }

    /** {@inheritDoc} */
    @Override
    public RelDataType deriveType(SqlValidatorScope scope, SqlNode expr) {
        checkTypesInteroperability(scope, expr);

        RelDataType dataType = super.deriveType(scope, expr);

        // If type of dynamic parameter has not been inferred, use a type of its value.
        RelDataType paramType = expr instanceof SqlDynamicParam
                ? getDynamicParamType((SqlDynamicParam) expr) : null;

        if (dataType.equals(unknownType) && expr instanceof SqlDynamicParam) {
            // If paramType is unknown setValidatedNodeType is a no-op.
            setValidatedNodeType(expr, paramType);
            return paramType;
        } else if (!(expr instanceof SqlCall)) {
            return dataType;
        }

        SqlKind sqlKind = expr.getKind();
        // See the comments below.
        if (!SqlKind.BINARY_COMPARISON.contains(sqlKind)) {
            return dataType;
        }

        // Comparison and arithmetic operators are SqlCalls.
        SqlCall sqlCall = (SqlCall) expr;
        var lhs = getValidatedNodeType(sqlCall.operand(0));
        var rhs = getValidatedNodeType(sqlCall.operand(1));

        // IgniteCustomType:
        // Check compatibility for operands of binary comparison operation between custom data types vs built-in SQL types.
        // We get here because in calcite ANY type can be assigned/casted to all other types.
        // This check can be a part of some SqlOperandTypeChecker?

        if (lhs instanceof IgniteCustomType || rhs instanceof IgniteCustomType) {
            boolean lhsRhsCompatible = TypeUtils.typeFamiliesAreCompatible(typeFactory, lhs, rhs);
            boolean rhsLhsCompatible = TypeUtils.typeFamiliesAreCompatible(typeFactory, rhs, lhs);

            if (!lhsRhsCompatible && !rhsLhsCompatible) {
                SqlCallBinding callBinding = new SqlCallBinding(this, scope, (SqlCall) expr);
                throw callBinding.newValidationSignatureError();
            }
        }

        return dataType;
    }

    /** Check appropriate type cast availability. */
    private void checkTypesInteroperability(SqlValidatorScope scope, SqlNode expr) {
        boolean castOp = expr.getKind() == SqlKind.CAST;

        if (castOp || SqlKind.BINARY_COMPARISON.contains(expr.getKind())) {
            SqlBasicCall expr0 = (SqlBasicCall) expr;
            SqlNode first = expr0.getOperandList().get(0);
            SqlNode ret = expr0.getOperandList().get(1);

            RelDataType firstType;
            RelDataType returnType = super.deriveType(scope, ret);

            if (first instanceof SqlDynamicParam) {
                firstType = getDynamicParamType((SqlDynamicParam) first);
            } else {
                firstType = super.deriveType(scope, first);
            }

            boolean nullType = isNull(returnType) || isNull(firstType);

            // propagate null type validation
            if (nullType) {
                return;
            }

            RelDataType returnCustomType = returnType instanceof IgniteCustomType ? returnType : null;
            RelDataType fromCustomType = firstType instanceof IgniteCustomType ? firstType : null;

            IgniteCustomTypeCoercionRules coercionRules = typeFactory().getCustomTypeCoercionRules();
            boolean check;

            if (fromCustomType != null && returnCustomType != null) {
                // it`s not allowed to convert between different custom types for now.
                check = SqlTypeUtil.equalSansNullability(typeFactory, firstType, returnType);
            } else if (fromCustomType != null) {
                check = coercionRules.needToCast(returnType, (IgniteCustomType) fromCustomType);
            } else if (returnCustomType != null) {
                check = coercionRules.needToCast(firstType, (IgniteCustomType) returnCustomType);
            } else {
                check = SqlTypeUtil.canCastFrom(returnType, firstType, true);
            }

            if (!check) {
                if (castOp) {
                    throw newValidationError(expr,
                            RESOURCE.cannotCastValue(firstType.toString(), returnType.toString()));
                } else {
                    SqlBasicCall call = (SqlBasicCall) expr;
                    SqlOperator operator = call.getOperator();

                    var ex = RESOURCE.incompatibleValueType(operator.getName());
                    throw SqlUtil.newContextException(expr.getParserPosition(), ex);
                }
            }

            literalCanFitType(expr, returnType);
        }
    }

    /** Check literal can fit to declared exact numeric type, work only for single literal. */
    private static void literalCanFitType(SqlNode expr, RelDataType toType) {
        if (INT_TYPES.contains(toType.getSqlTypeName())) {
            LiteralExtractor litExtractor = new LiteralExtractor();

            SqlLiteral literal = litExtractor.getLiteral(expr);

            if (literal == null || literal.toValue() == null) {
                return;
            }

            long max = Commons.getMaxValue(toType);
            long min = Commons.getMinValue(toType);

            String litValue = Objects.requireNonNull(literal.toValue());

            try {
                if (Long.valueOf(litValue).compareTo(max) > 0 || Long.valueOf(litValue).compareTo(min) < 0) {
                    throw new SqlException(STMT_PARSE_ERR, NUMERIC_FIELD_OVERFLOW_ERROR);
                }
            } catch (NumberFormatException e) {
                throw new SqlException(STMT_PARSE_ERR, NUMERIC_FIELD_OVERFLOW_ERROR, e);
            }
        }
    }

    private static class LiteralExtractor extends SqlBasicVisitor<SqlNode> {
        private @Nullable SqlLiteral extracted = null;

        private @Nullable SqlLiteral getLiteral(SqlNode expr) {
            try {
                expr.accept(this);
            } catch (Util.FoundOne e) {
                Util.swallow(e, null);
            }
            return extracted;
        }

        /** {@inheritDoc} */
        @Override
        public SqlNode visit(SqlLiteral literal) {
            extracted = extracted != null ? null : literal;
            return literal;
        }

        /** {@inheritDoc} */
        @Override
        public SqlNode visit(SqlDynamicParam param) {
            extracted = null;
            throw new Util.FoundOne(param);
        }
    }

    /** {@inheritDoc} */
    @Override
    protected SqlNode performUnconditionalRewrites(SqlNode node, boolean underFrom) {
        // Workaround for https://issues.apache.org/jira/browse/CALCITE-4923
        if (node instanceof SqlSelect) {
            SqlSelect select = (SqlSelect) node;

            if (select.getFrom() instanceof SqlJoin) {
                boolean hasStar = false;

                for (SqlNode expr : select.getSelectList()) {
                    if (expr instanceof SqlIdentifier && ((SqlIdentifier) expr).isStar()
                            && ((SqlIdentifier) expr).names.size() == 1) {
                        hasStar = true;
                    }
                }

                performJoinRewrites((SqlJoin) select.getFrom(), hasStar);
            }
        }

        return super.performUnconditionalRewrites(node, underFrom);
    }

    /** Rewrites JOIN clause if required. */
    private void performJoinRewrites(SqlJoin join, boolean hasStar) {
        if (join.getLeft() instanceof SqlJoin) {
            performJoinRewrites((SqlJoin) join.getLeft(), hasStar || join.isNatural());
        }

        if (join.getRight() instanceof SqlJoin) {
            performJoinRewrites((SqlJoin) join.getRight(), hasStar || join.isNatural());
        }

        // Join with USING should be rewriten if SELECT conatins "star" in projects, NATURAL JOIN also has other issues
        // and should be rewritten in any case.
        if (join.isNatural() || (join.getConditionType() == JoinConditionType.USING && hasStar)) {
            // Default Calcite validator can't expand "star" for NATURAL joins and joins with USING if some columns
            // of join sources are filtered out by the addToSelectList method, and the count of columns in the
            // selectList not equals to the count of fields in the corresponding rowType. Since we do filtering in the
            // addToSelectList method (exclude _KEY and _VAL columns), to workaround the expandStar limitation we can
            // wrap each table to a subquery. In this case columns will be filtered out on the subquery level and
            // rowType of the subquery will have the same cardinality as selectList.
            join.setLeft(rewriteTableToQuery(join.getLeft()));
            join.setRight(rewriteTableToQuery(join.getRight()));
        }
    }

    /** Wrap table to subquery "SELECT * FROM table". */
    private SqlNode rewriteTableToQuery(SqlNode from) {
        SqlNode src = from.getKind() == SqlKind.AS ? ((SqlCall) from).getOperandList().get(0) : from;

        if (src.getKind() == SqlKind.IDENTIFIER || src.getKind() == SqlKind.TABLE_REF) {
            String alias = deriveAlias(from, 0);

            SqlSelect expandedQry = new SqlSelect(SqlParserPos.ZERO, null,
                    SqlNodeList.of(SqlIdentifier.star(SqlParserPos.ZERO)), src, null, null, null,
                    null, null, null, null, null);

            return SqlValidatorUtil.addAlias(expandedQry, alias);
        } else {
            return from;
        }
    }

    private void validateAggregateFunction(SqlCall call, SqlAggFunction aggFunction) {
        if (!SqlKind.AGGREGATE.contains(aggFunction.kind)) {
            throw newValidationError(call,
                    IgniteResource.INSTANCE.unsupportedAggregationFunction(aggFunction.getName()));
        }

        switch (aggFunction.kind) {
            case COUNT:
                if (call.operandCount() > 1) {
                    throw newValidationError(call, RESOURCE.invalidArgCount(aggFunction.getName(), 1));
                }

                return;
            case SUM:
            case AVG:
            case MIN:
            case MAX:
            case ANY_VALUE:

                return;
            default:
                throw newValidationError(call,
                        IgniteResource.INSTANCE.unsupportedAggregationFunction(aggFunction.getName()));
        }
    }

    private SqlNodeList inferColumnList(IgniteTable igniteTable) {
        SqlNodeList columnList = new SqlNodeList(SqlParserPos.ZERO);
        TableDescriptor descriptor = igniteTable.descriptor();

        for (RelDataTypeField field : descriptor.insertRowType(typeFactory()).getFieldList()) {
            columnList.add(new SqlIdentifier(field.getName(), SqlParserPos.ZERO));
        }

        return columnList;
    }

    private void validateUpdateFields(SqlUpdate call) {
        if (call.getTargetColumnList() == null) {
            return;
        }

        final SqlValidatorNamespace ns = validatedNamespace(call, unknownType);
        final SqlValidatorTable table = table(ns);
        IgniteTable igniteTable = getIgniteTableForModification((SqlIdentifier) call.getTargetTable(), table);
        TableDescriptor descriptor = igniteTable.descriptor();

        final RelDataType baseType = table.getRowType();
        final RelOptTable relOptTable = relOptTable(ns);

        for (SqlNode node : call.getTargetColumnList()) {
            SqlIdentifier id = (SqlIdentifier) node;

            RelDataTypeField target = SqlValidatorUtil.getTargetField(
                    baseType, typeFactory(), id, getCatalogReader(), relOptTable);

            if (target == null) {
                throw newValidationError(id,
                        RESOURCE.unknownTargetColumn(id.toString()));
            }

            if (!descriptor.isUpdateAllowed(relOptTable, target.getIndex())) {
                throw newValidationError(id,
                        IgniteResource.INSTANCE.cannotUpdateField(id.toString()));
            }
        }
    }

    private SqlValidatorTable table(SqlValidatorNamespace ns) {
        RelOptTable relOptTable = relOptTable(ns);

        if (relOptTable != null) {
            return relOptTable.unwrap(SqlValidatorTable.class);
        }

        return ns.getTable();
    }

    private RelOptTable relOptTable(SqlValidatorNamespace ns) {
        return SqlValidatorUtil.getRelOptTable(
                ns, getCatalogReader().unwrap(Prepare.CatalogReader.class), null, null);
    }

    private SqlValidatorNamespace validatedNamespace(SqlNode node, RelDataType targetType) {
        SqlValidatorNamespace ns = getNamespace(node);
        validateNamespace(ns, targetType);
        return ns;
    }

    private IgniteTypeFactory typeFactory() {
        return (IgniteTypeFactory) typeFactory;
    }

    private boolean isSystemFieldName(String alias) {
        return Commons.implicitPkEnabled() && Commons.IMPLICIT_PK_COL_NAME.equals(alias);
    }

    /** {@inheritDoc} */
    @Override
    protected void inferUnknownTypes(RelDataType inferredType, SqlValidatorScope scope, SqlNode node) {
        if (node instanceof SqlDynamicParam) {
            RelDataType result = inferDynamicParamType(inferredType, (SqlDynamicParam) node);

            this.setValidatedNodeType(node, result);
        } else {
            super.inferUnknownTypes(inferredType, scope, node);
        }
    }

    private RelDataType inferDynamicParamType(RelDataType inferredType, SqlDynamicParam dynamicParam) {
        RelDataType type = getDynamicParamType(dynamicParam);
        RelDataType paramTypeToUse;

        /*
         * If inferredType is unknown - use a type of dynamic parameter since there is no other source of type information.
         *
         * If parameter's type and the inferredType do not match - use parameter's type.
         * This makes CAST operations to work correctly. Otherwise cast's operand is going to have
         * the same type as a target type which is not correct as it
         * makes every CAST operation eligible to redundant type conversion elimination
         * at later stages:
         * E.g: CAST(? AS INTEGER) where ?='hello' operand is going to be inferred as INTEGER
         * although it is a string.
         *
         * In other cases use the inferredType and we rely on type inference rules provided by
         * operator's SqlOperandTypeInference and SqlOperandTypeCheckers.
         */

        if (inferredType.equals(unknownType) || (!SqlTypeUtil.equalSansNullability(type, inferredType))) {
            paramTypeToUse = type;
        } else {
            paramTypeToUse = inferredType;
        }

        return typeFactory.createTypeWithNullability(paramTypeToUse, true);
    }

    /** Returns type of the given dynamic parameter. */
    RelDataType getDynamicParamType(SqlDynamicParam dynamicParam) {
        IgniteTypeFactory typeFactory = (IgniteTypeFactory) this.getTypeFactory();
        Object value = getDynamicParamValue(dynamicParam);

        RelDataType parameterType;
        // IgniteCustomType: first we must check whether dynamic parameter is a custom data type.
        // If so call createCustomType with appropriate arguments.
        if (value instanceof UUID) {
            parameterType = typeFactory.createCustomType(UuidType.NAME);
        } else if (value == null) {
            parameterType = typeFactory.createSqlType(SqlTypeName.NULL);
        } else {
            parameterType = typeFactory.toSql(typeFactory.createType(value.getClass()));
        }

        // Dynamic parameters are always nullable.
        // Otherwise it seem to cause "Conversion to relational algebra failed to preserve datatypes" errors
        // in some cases.
        return typeFactory.createTypeWithNullability(parameterType, true);
    }

    private Object getDynamicParamValue(SqlDynamicParam dynamicParam) {
        Object value = dynamicParamValues[dynamicParam.getIndex()];
        // save dynamic parameter for later validation.
        dynamicParamNodes[dynamicParam.getIndex()] = dynamicParam;
        return value;
    }

    private void validateInferredDynamicParameters() {
        // Derived types of dynamic parameters should not change (current type inference behavior).

        for (int i = 0; i < dynamicParamValues.length; i++) {
            SqlDynamicParam param = dynamicParamNodes[i];
            assert param != null : format("Dynamic parameter#{} has not been checked", i);

            RelDataType paramType = getDynamicParamType(param);
            RelDataType derivedType = getValidatedNodeType(param);

            // We can check for nullability, but it was set to true.
            if (!SqlTypeUtil.equalSansNullability(derivedType, paramType)) {
                String message = format(
                        "Type of dynamic parameter#{} does not match. Expected: {} derived: {}", i, paramType.getFullTypeString(),
                        derivedType.getFullTypeString()
                );

                throw new AssertionError(message);
            }
        }
    }
}
