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
import static org.apache.calcite.sql.type.SqlTypeName.INTEGER;
import static org.apache.calcite.sql.type.SqlTypeName.INT_TYPES;
import static org.apache.calcite.sql.type.SqlTypeUtil.isNull;
import static org.apache.calcite.util.Static.RESOURCE;
import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;
import static org.apache.ignite.lang.ErrorGroups.Sql.STMT_PARSE_ERR;

import it.unimi.dsi.fastutil.ints.Int2ObjectArrayMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.IntArraySet;
import it.unimi.dsi.fastutil.ints.IntSet;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.regex.Pattern;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.schema.impl.ModifiableViewTable;
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
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeName.Limit;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.sql.util.SqlBasicVisitor;
import org.apache.calcite.sql.util.SqlShuttle;
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

    // Approximate and exact numeric types.
    private static final Pattern NUMERIC = Pattern.compile("^\\s*\\d+(\\.{1}\\d*)\\s*$");

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

    /** Dynamic parameters state. */
    private final Int2ObjectMap<DynamicParamState> dynamicParameters;

    /**
     * The same dynamic parameter can be used in the same SQL tree multiple types after a rewrite.
     * (E.g. COALESCE(?0, ?1) is rewritten into CASE WHEN ?0 IS NOT NULL THEN ?0 ELSE ?1 END)
     * We store them to check that every i-th parameter has the same type.
     */
    private final IdentityHashMap<SqlDynamicParam, SqlDynamicParam> dynamicParamNodes = new IdentityHashMap<>();

    /** Literal processing. */
    private final LiteralExtractor litExtractor = new LiteralExtractor();

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
            IgniteTypeFactory typeFactory, SqlValidator.Config config, Int2ObjectMap<Object> parameters) {
        super(opTab, catalogReader, typeFactory, config);

        this.dynamicParameters = new Int2ObjectArrayMap<>(parameters.size());
        for (Map.Entry<Integer, Object> param : parameters.int2ObjectEntrySet()) {
            Object value = param.getValue();
            dynamicParameters.put(param.getKey().intValue(), new DynamicParamState(value));
        }
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
    protected void checkTypeAssignment(
            SqlValidatorScope sourceScope,
            SqlValidatorTable table,
            RelDataType sourceRowType,
            RelDataType targetRowType,
            SqlNode query
    ) {
        boolean coerced = false;

        if (query instanceof SqlUpdate) {
            SqlNodeList targetColumnList =
                    requireNonNull(((SqlUpdate) query).getTargetColumnList());
            int targetColumnCount = targetColumnList.size();
            targetRowType =
                    SqlTypeUtil.extractLastNFields(typeFactory, targetRowType,
                            targetColumnCount);
            sourceRowType =
                    SqlTypeUtil.extractLastNFields(typeFactory, sourceRowType,
                            targetColumnCount);
        }

        // if BIGINT is present we need to preserve CAST from BIGINT to BIGINT for further overflow check possibility
        // TODO: need to be removed after https://issues.apache.org/jira/browse/IGNITE-20889
        if (config().typeCoercionEnabled()) {
            if (SqlTypeUtil.equalAsStructSansNullability(typeFactory,
                    sourceRowType, targetRowType, null)) {
                if ((query.getKind() == SqlKind.INSERT || query.getKind() == SqlKind.UPDATE)
                        && targetRowType.getFieldList().stream().anyMatch(fld -> fld.getType().getSqlTypeName() == SqlTypeName.BIGINT)
                        && sourceRowType.getFieldList().stream().anyMatch(fld -> fld.getType().getSqlTypeName() == SqlTypeName.BIGINT)) {
                    coerced = getTypeCoercion().querySourceCoercion(sourceScope, sourceRowType, targetRowType, query);
                }
            }
        }

        if (!coerced) {
            doCheckTypeAssignment(sourceScope, table, sourceRowType, targetRowType, query);
        }
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

    /**
     * The copy of {@link SqlValidatorImpl#checkTypeAssignment(SqlValidatorScope, SqlValidatorTable, RelDataType, RelDataType, SqlNode)}
     * with a fixed condition to skip dynamic parameters + this method does not try to find a location of a type error.
     */
    private void doCheckTypeAssignment(
            @Nullable SqlValidatorScope sourceScope,
            SqlValidatorTable table,
            RelDataType sourceRowType,
            RelDataType targetRowType,
            final SqlNode query) {
        // NOTE jvs 23-Feb-2006: subclasses may allow for extra targets
        // representing system-maintained columns, so stop after all sources
        // matched
        boolean isUpdateModifiableViewTable = false;
        if (query instanceof SqlUpdate) {
            final SqlNodeList targetColumnList =
                    requireNonNull(((SqlUpdate) query).getTargetColumnList());
            final int targetColumnCount = targetColumnList.size();
            targetRowType =
                    SqlTypeUtil.extractLastNFields(typeFactory, targetRowType,
                            targetColumnCount);
            sourceRowType =
                    SqlTypeUtil.extractLastNFields(typeFactory, sourceRowType,
                            targetColumnCount);
            isUpdateModifiableViewTable =
                    table.unwrap(ModifiableViewTable.class) != null;
        }
        if (SqlTypeUtil.equalAsStructSansNullability(typeFactory,
                sourceRowType, targetRowType, null)) {
            // Returns early if source and target row type equals sans nullability.
            return;
        }
        if (config().typeCoercionEnabled() && !isUpdateModifiableViewTable) {
            // Try type coercion first if implicit type coercion is allowed.
            boolean coerced =
                    getTypeCoercion().querySourceCoercion(sourceScope, sourceRowType,
                            targetRowType, query);
            if (coerced) {
                return;
            }
        }

        // Fall back to default behavior: compare the type families.
        List<RelDataTypeField> sourceFields = sourceRowType.getFieldList();
        List<RelDataTypeField> targetFields = targetRowType.getFieldList();
        final int sourceCount = sourceFields.size();
        for (int i = 0; i < sourceCount; ++i) {
            RelDataType sourceType = sourceFields.get(i).getType();
            RelDataType targetType = targetFields.get(i).getType();
            if (!SqlTypeUtil.canAssignFrom(targetType, sourceType)) {
                // A correct condition for skipping dynamic parameters.
                if (sourceType == unknownType) {
                    continue;
                }
                String targetTypeString;
                String sourceTypeString;
                if (SqlTypeUtil.areCharacterSetsMismatched(
                        sourceType,
                        targetType)) {
                    sourceTypeString = sourceType.getFullTypeString();
                    targetTypeString = targetType.getFullTypeString();
                } else {
                    sourceTypeString = sourceType.toString();
                    targetTypeString = targetType.toString();
                }
                // Always use a query as an error source.
                throw newValidationError(query,
                        RESOURCE.typeNotAssignable(
                                targetFields.get(i).getName(), targetTypeString,
                                sourceFields.get(i).getName(), sourceTypeString));
            }
        }
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

        SqlIdentifier alias = call.getAlias() != null ? call.getAlias() :
                new SqlIdentifier(deriveAlias(targetTable, 0), SqlParserPos.ZERO);

        igniteTable.getRowType(typeFactory)
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

        igniteTable.rowTypeForDelete((IgniteTypeFactory) typeFactory)
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
            SqlDynamicParam dynamicParam = (SqlDynamicParam) n;
            RelDataType intType = typeFactory.createSqlType(INTEGER);

            // Validate value, if present.
            if (!isUnspecified(dynamicParam)) {
                Object param = getDynamicParamValue(dynamicParam);

                if (param instanceof Integer) {
                    if ((Integer) param < 0) {
                        throw newValidationError(n, IgniteResource.INSTANCE.correctIntegerLimit(nodeName));
                    }
                } else {
                    String actualType = deriveDynamicParamType(dynamicParam).toString();
                    String expectedType = intType.toString();

                    var err = IgniteResource.INSTANCE.incorrectDynamicParameterType(expectedType, actualType);
                    throw newValidationError(n, err);
                }
            }

            // Dynamic parameters are nullable.
            setDynamicParamType(dynamicParam, typeFactory.createTypeWithNullability(intType, true));
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
        if (expr instanceof SqlDynamicParam) {
            return deriveDynamicParamType((SqlDynamicParam) expr);
        }

        checkTypesInteroperability(scope, expr);

        RelDataType dataType = super.deriveType(scope, expr);

        SqlKind sqlKind = expr.getKind();

        // TODO https://issues.apache.org/jira/browse/IGNITE-20163 Remove this exception after this issue is fixed
        if (sqlKind == SqlKind.JSON_VALUE_EXPRESSION) {
            String name = SqlStdOperatorTable.JSON_VALUE_EXPRESSION.getName();
            throw newValidationError(expr, IgniteResource.INSTANCE.unsupportedExpression(name));
        } else if (!SqlKind.BINARY_COMPARISON.contains(sqlKind)) {
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

    /** {@inheritDoc} */
    @Override public RelDataType getParameterRowType(SqlNode sqlQuery) {
        // We do not use calcite' version since it is contains a bug,
        // alreadyVisited visited uses object identity, but rewrites of NULLIF, COALESCE
        // into dynamic parameters may place the same parameter into multiple positions
        // in SQL tree.
        List<RelDataType> types = new ArrayList<>();
        IntSet alreadyVisited = new IntArraySet(dynamicParameters.size());
        sqlQuery.accept(
                new SqlShuttle() {
                    @Override public SqlNode visit(SqlDynamicParam param) {
                        if (alreadyVisited.add(param.getIndex())) {
                            RelDataType type = getValidatedNodeType(param);
                            types.add(type);
                        }
                        return param;
                    }
                });
        return typeFactory.createStructType(
                types,
                new AbstractList<String>() {
                    @Override public String get(int index) {
                        return "?" + index;
                    }

                    @Override public int size() {
                        return types.size();
                    }
                });
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
                SqlDynamicParam dynamicParam = (SqlDynamicParam) first;
                firstType = deriveDynamicParamType(dynamicParam);
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

            if (castOp) {
                literalCanFitType(expr, returnType);
            }
        }
    }

    /** Check literal can fit to declared exact numeric type, work only for single literal. */
    private void literalCanFitType(SqlNode expr, RelDataType toType) {
        if (INT_TYPES.contains(toType.getSqlTypeName())) {
            SqlLiteral literal = litExtractor.getLiteral(expr);

            if (literal == null || literal.toValue() == null) {
                return;
            }

            int precision = toType.getSqlTypeName().allowsPrec() ? toType.getPrecision() : -1;
            int scale = toType.getSqlTypeName().allowsScale() ? toType.getScale() : -1;

            BigDecimal max = (BigDecimal) toType.getSqlTypeName().getLimit(true, Limit.OVERFLOW, false, precision, scale);
            BigDecimal min = (BigDecimal) toType.getSqlTypeName().getLimit(false, Limit.OVERFLOW, false, precision, scale);

            String litValue = requireNonNull(literal.toValue());

            BigDecimal litValueToDecimal = null;

            try {
                litValueToDecimal = new BigDecimal(litValue).setScale(0, RoundingMode.HALF_UP);
            } catch (NumberFormatException e) {
                if (!NUMERIC.matcher(litValue).matches()) {
                    throw new SqlException(STMT_PARSE_ERR, e);
                }
            }

            if (max.compareTo(litValueToDecimal) < 0 || min.compareTo(litValueToDecimal) > 0) {
                throw new SqlException(STMT_PARSE_ERR, "Value '" + litValue + "'"
                        + " out of range for type " + toType.getSqlTypeName());
            }
        }
    }

    private static class LiteralExtractor extends SqlBasicVisitor<SqlNode> {
        private @Nullable SqlLiteral extracted;

        private @Nullable SqlLiteral getLiteral(SqlNode expr) {
            extracted = null;
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
            throw Util.FoundOne.NULL;
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

        for (RelDataTypeField field : igniteTable.rowTypeForInsert(typeFactory()).getFieldList()) {
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

            if (!igniteTable.isUpdateAllowed(target.getIndex())) {
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

        // See SqlStdOperatorTable::IS_NULL and  SqlStdOperatorTable::IS_NOT_NULL
        // Operators use VARCHAR_1024 for argument type inference, so we need to manually fix this.
        if (node.getKind() == SqlKind.IS_NULL || node.getKind() == SqlKind.IS_NOT_NULL) {
            SqlCall call = (SqlCall) node;

            if (isUnspecifiedDynamicParam(call.operand(0))) {
                SqlCallBinding binding = new SqlCallBinding(this, scope, call);
                String signature = IgniteResource.makeSignature(binding, List.of(unknownType));

                throw binding.newValidationError(IgniteResource.INSTANCE.ambiguousOperator1(signature));
            }
        } else if (node.getKind() == SqlKind.IN) {
            // TypeInference for IN operator fails with
            // java.lang.UnsupportedOperationException: class org.apache.calcite.sql.SqlNodeList: 1
            // if the first operand has unknown type.
            SqlCall call = (SqlCall) node;

            if (isUnspecifiedDynamicParam(call.operand(0))) {
                SqlDynamicParam dynamicParam = call.operand(0);
                throw newValidationError(dynamicParam, IgniteResource.INSTANCE.unableToResolveDynamicParameterType());
            }
        }

        if (node instanceof SqlDynamicParam) {
            SqlDynamicParam dynamicParam = (SqlDynamicParam) node;
            inferDynamicParamType(inferredType, dynamicParam);
        } else {
            super.inferUnknownTypes(inferredType, scope, node);
        }
    }

    private void inferDynamicParamType(RelDataType inferredType, SqlDynamicParam dynamicParam) {
        RelDataType type = deriveDynamicParamType(dynamicParam);

        /*
         * If inferredType type is unknown and parameter is not specified, then use unknown type.
         * Otherwise use parameter type if it set, or inferredType provided by operator's
         * SqlOperandTypeInference and SqlOperandTypeCheckers.
         *
         * If dynamic parameter is an operand of a CAST expression, its type is set to
         * the type equal to the target type. Although CAST(p:T AS T) is a no-op and later removed at the sql-to-rel conversion phase,
         * the runtime _should_ perform type conversion of unspecified dynamic parameter values to complete type checking.
         */

        if (inferredType == unknownType && type == unknownType) {
            setDynamicParamType(dynamicParam, unknownType);
        } else if (type != unknownType) {
            setDynamicParamType(dynamicParam, type);
        } else {
            // Make sure to set nullability to true, so types are nullable in all cases.
            RelDataType nullableType = typeFactory.createTypeWithNullability(inferredType, true);
            setDynamicParamType(dynamicParam, nullableType);
        }
    }

    /** Derives the type of the given dynamic parameter. */
    private RelDataType deriveDynamicParamType(SqlDynamicParam dynamicParam) {
        dynamicParamNodes.put(dynamicParam, dynamicParam);

        if (isUnspecified(dynamicParam)) {
            RelDataType validatedNodeType = getValidatedNodeTypeIfKnown(dynamicParam);

            if (validatedNodeType == null) {
                setDynamicParamType(dynamicParam, unknownType);
                return unknownType;
            } else {
                setDynamicParamType(dynamicParam, validatedNodeType);
                return validatedNodeType;
            }
        } else {
            Object value = getDynamicParamValue(dynamicParam);
            RelDataType parameterType = deriveTypeFromDynamicParamValue(value);

            // Dynamic parameters are always nullable.
            // Otherwise it seem to cause "Conversion to relational algebra failed to preserve datatypes" errors
            // in some cases.
            RelDataType nullableType = typeFactory.createTypeWithNullability(parameterType, true);

            setDynamicParamType(dynamicParam, nullableType);

            return nullableType;
        }
    }

    private RelDataType deriveTypeFromDynamicParamValue(@Nullable Object value) {
        IgniteTypeFactory typeFactory = typeFactory();

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

        return parameterType;
    }

    /** if dynamic parameter is not specified, set its type to the provided type, otherwise return the type of its value. */
    RelDataType resolveDynamicParameterType(SqlDynamicParam dynamicParam, RelDataType contextType) {
        if (isUnspecified(dynamicParam)) {
            RelDataType nullableContextType = typeFactory.createTypeWithNullability(contextType, true);

            setDynamicParamType(dynamicParam, nullableContextType);

            return nullableContextType;
        } else {
            return deriveDynamicParamType(dynamicParam);
        }
    }

    private void setDynamicParamType(SqlDynamicParam dynamicParam, RelDataType dataType) {
        setValidatedNodeType(dynamicParam, dataType);

        setDynamicParamResolvedType(dynamicParam, dataType);
    }

    /**
     * Returns the value of the given dynamic parameter. If the value is not specified,
     * this method throws {@link IllegalArgumentException}.
     */
    @Nullable
    private Object getDynamicParamValue(SqlDynamicParam dynamicParam) {
        int index = dynamicParam.getIndex();
        DynamicParamState paramState = dynamicParameters.computeIfAbsent(index, (i) -> new DynamicParamState());
        Object value = paramState.value;
        if (!paramState.hasValue) {
            throw new IllegalArgumentException(format("Value of dynamic parameter#{} is not specified", index));
        } else {
            return value;
        }
    }

    private void validateInferredDynamicParameters() {
        for (int i = 0; i < dynamicParameters.size(); i++) {
            DynamicParamState paramState = dynamicParameters.get(i);

            if (paramState == null || paramState.resolvedType == null || paramState.node == null) {
                throw new AssertionError("Dynamic parameter has not been validated: " + i);
            } else if (paramState.resolvedType == unknownType) {
                throw newValidationError(paramState.node, IgniteResource.INSTANCE.unableToResolveDynamicParameterType());
            }
        }

        // Ensure that all nodes for i-th parameter have the same type.
        for (SqlDynamicParam node : dynamicParamNodes.keySet()) {
            int i = node.getIndex();
            DynamicParamState state = dynamicParameters.get(i);

            if (!state.hasValue) {
                continue;
            }

            Object value = state.value;
            RelDataType valueType = deriveTypeFromDynamicParamValue(value);
            RelDataType derivedType = getValidatedNodeTypeIfKnown(node);
            RelDataType paramType = state.resolvedType;

            // Ensure that derived type matches parameter's value.
            if (!SqlTypeUtil.equalSansNullability(derivedType, valueType)) {
                String message = format(
                        "Type of dynamic parameter#{} value type does not match. Expected: {} derived: {}",
                        i, valueType.getFullTypeString(), derivedType.getFullTypeString()
                );

                throw new AssertionError(message);
            }

            if (!Objects.equals(paramType, derivedType)) {
                String message = format(
                        "Type of dynamic parameter node#{} does not match. Expected: {} derived: {}", i, paramType.getFullTypeString(),
                        derivedType != null ? derivedType.getFullTypeString() : null
                );

                throw new AssertionError(message);
            }
        }
    }

    private void setDynamicParamResolvedType(SqlDynamicParam param, RelDataType type) {
        int index = param.getIndex();
        DynamicParamState state = dynamicParameters.computeIfAbsent(index, (i) -> new DynamicParamState());

        state.node = param;
        state.resolvedType = type;
    }

    /** Returns {@code true} if the given dynamic parameter has no value set. */
    public boolean isUnspecified(SqlDynamicParam param) {
        int index = param.getIndex();
        DynamicParamState state = dynamicParameters.computeIfAbsent(index, (i) -> new DynamicParamState());

        return !state.hasValue;
    }

    /** Returns {@code true} if the given node is dynamic parameter that has no value set. */
    public boolean isUnspecifiedDynamicParam(SqlNode node) {
        if (node.getKind() != SqlKind.DYNAMIC_PARAM) {
            return false;
        } else {
            return isUnspecified((SqlDynamicParam) node);
        }
    }

    private static final class DynamicParamState {

        final Object value;

        final boolean hasValue;

        SqlDynamicParam node;

        /**
         * Resolved type of a dynamic parameter.
         *
         * <ul>
         *    <li>{@code null} - parameter has not been checked - this is a bug.</li>
         *    <li>{@code unknownType} means the type of this parameter has not been resolved due to ambiguity.</li>
         *    <li>Otherwise contains a type of a parameter.</li>
         * </ul>
         */
        RelDataType resolvedType;

        private DynamicParamState(@Nullable Object value) {
            this.value = value;
            this.hasValue = true;
        }

        private DynamicParamState() {
            this.value = null;
            this.hasValue = false;
        }
    }
}
