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
import static org.apache.calcite.rel.type.RelDataType.PRECISION_NOT_SPECIFIED;
import static org.apache.calcite.rel.type.RelDataType.SCALE_NOT_SPECIFIED;
import static org.apache.calcite.sql.type.SqlTypeUtil.isNull;
import static org.apache.calcite.util.Static.RESOURCE;
import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;
import static org.apache.ignite.lang.ErrorGroups.Sql.STMT_VALIDATION_ERR;

import it.unimi.dsi.fastutil.ints.Int2ObjectArrayMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.IntArraySet;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import java.math.BigDecimal;
import java.util.AbstractList;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.runtime.CalciteContextException;
import org.apache.calcite.runtime.PairList;
import org.apache.calcite.runtime.Resources;
import org.apache.calcite.schema.impl.ModifiableViewTable;
import org.apache.calcite.sql.JoinConditionType;
import org.apache.calcite.sql.JoinType;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlBasicTypeNameSpec;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlCharStringLiteral;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlDelete;
import org.apache.calcite.sql.SqlDynamicParam;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlIntervalLiteral;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlMerge;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlTypeNameSpec;
import org.apache.calcite.sql.SqlUnknownLiteral;
import org.apache.calcite.sql.SqlUpdate;
import org.apache.calcite.sql.SqlUtil;
import org.apache.calcite.sql.SqlWithItem;
import org.apache.calcite.sql.dialect.CalciteSqlDialect;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeMappingRule;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.sql.util.SqlShuttle;
import org.apache.calcite.sql.validate.AliasNamespace;
import org.apache.calcite.sql.validate.SelectScope;
import org.apache.calcite.sql.validate.SqlNonNullableAccessors;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorException;
import org.apache.calcite.sql.validate.SqlValidatorImpl;
import org.apache.calcite.sql.validate.SqlValidatorNamespace;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.sql.validate.SqlValidatorTable;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.util.TimestampString;
import org.apache.ignite.internal.sql.engine.exec.exp.IgniteSqlFunctions;
import org.apache.ignite.internal.sql.engine.schema.IgniteDataSource;
import org.apache.ignite.internal.sql.engine.schema.IgniteSystemView;
import org.apache.ignite.internal.sql.engine.schema.IgniteTable;
import org.apache.ignite.internal.sql.engine.sql.IgniteSqlExplain;
import org.apache.ignite.internal.sql.engine.type.IgniteTypeFactory;
import org.apache.ignite.internal.sql.engine.util.Commons;
import org.apache.ignite.internal.sql.engine.util.IgniteCustomAssignmentsRules;
import org.apache.ignite.internal.sql.engine.util.IgniteResource;
import org.apache.ignite.internal.sql.engine.util.IgniteSqlDateTimeUtils;
import org.apache.ignite.internal.sql.engine.util.TypeUtils;
import org.apache.ignite.sql.ColumnType;
import org.apache.ignite.sql.SqlException;
import org.jetbrains.annotations.Nullable;

/** Validator. */
public class IgniteSqlValidator extends SqlValidatorImpl {
    /** Decimal of Long.MAX_VALUE for fetch/offset bounding. */
    public static final BigDecimal LIMIT_UPPER = BigDecimal.valueOf(Long.MAX_VALUE);

    public static final int MAX_LENGTH_OF_ALIASES = 256;
    public static final int DECIMAL_DYNAMIC_PARAM_PRECISION = 28;
    public static final int DECIMAL_DYNAMIC_PARAM_SCALE = 6;
    public static final int TEMPORAL_DYNAMIC_PARAM_PRECISION = 9;

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

    /** Dynamic parameters state. */
    private final Int2ObjectMap<DynamicParamState> dynamicParameters;

    /**
     * The same dynamic parameter can be used in the same SQL tree multiple types after a rewrite.
     * (E.g. COALESCE(?0, ?1) is rewritten into CASE WHEN ?0 IS NOT NULL THEN ?0 ELSE ?1 END)
     * We store them to check that every i-th parameter has the same type.
     */
    private final IdentityHashMap<SqlDynamicParam, SqlDynamicParam> dynamicParamNodes = new IdentityHashMap<>();

    /**
     * Creates a validator.
     *
     * @param opTab           Operator table
     * @param catalogReader   Catalog reader
     * @param typeFactory     Type factory
     * @param config          Config
     * @param parametersTypes Dynamic parameters types
     */
    public IgniteSqlValidator(SqlOperatorTable opTab, CalciteCatalogReader catalogReader,
            IgniteTypeFactory typeFactory, SqlValidator.Config config, Int2ObjectMap<ColumnType> parametersTypes) {
        super(opTab, catalogReader, typeFactory, config);

        this.dynamicParameters = new Int2ObjectArrayMap<>(parametersTypes.size());
        for (Int2ObjectMap.Entry<ColumnType> param : parametersTypes.int2ObjectEntrySet()) {
            ColumnType colType = param.getValue();
            dynamicParameters.put(param.getIntKey(), new DynamicParamState(colType));
        }
    }

    /** {@inheritDoc} */
    @Override
    public SqlNode validate(SqlNode topNode) {
        SqlNode result;

        // Calcite fails to validate a query when its top node is EXPLAIN PLAN FOR
        // java.lang.NullPointerException: namespace for <query>
        // at org.apache.calcite.sql.validate.SqlValidatorImpl.getNamespaceOrThrow(SqlValidatorImpl.java:1280)
        if (topNode instanceof IgniteSqlExplain) {
            IgniteSqlExplain explainNode = (IgniteSqlExplain) topNode;
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

    @Override
    protected void registerNamespace(
            @Nullable SqlValidatorScope usingScope,
            @Nullable String alias,
            SqlValidatorNamespace ns,
            boolean forceNullable
    ) {
        if (ns instanceof AliasNamespace) {
            SqlNode call = ns.getNode();
            SqlNode enclosingNode = ns.getEnclosingNode();

            assert call instanceof SqlCall;
            assert enclosingNode != null;

            // Calcite's implementation lacks notion of system/hidden columns,
            // which is required to properly derive table type for column
            // renaming in FROM clause.
            ns = new IgniteAliasNamespace(
                    (SqlValidatorImpl) ns.getValidator(),
                    (SqlCall) call,
                    enclosingNode
            );
        }

        super.registerNamespace(usingScope, alias, ns, forceNullable);
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
    }

    @Override
    public void validateWithItem(SqlWithItem withItem) {
        if (withItem.recursive.booleanValue()) {
            // pass withItem.recursive instead of withItem, so exception message
            // will point to keyword RECURSIVE rather than name of the CTE
            throw newValidationError(withItem.recursive,
                    IgniteResource.INSTANCE.recursiveQueryIsNotSupported());
        }

        super.validateWithItem(withItem);
    }

    /** {@inheritDoc} */
    @Override
    public void validateMerge(SqlMerge call) {
        super.validateMerge(call);

        syncSelectList(call);
    }

    @Override
    public CalciteContextException newValidationError(
            SqlNode node,
            Resources.ExInst<SqlValidatorException> e
    ) {
        CalciteContextException ex = super.newValidationError(node, e);

        String newMessage = IgniteSqlValidatorErrorMessages.resolveErrorMessage(ex.getMessage());

        CalciteContextException newEx;
        if (newMessage != null) {
            newEx = new IgniteContextException(newMessage, ex.getCause());
            newEx.setPosition(ex.getPosLine(), ex.getPosColumn(), ex.getEndPosLine(), ex.getEndPosColumn());
            newEx.setOriginalStatement(ex.getOriginalStatement());
        } else {
            newEx = ex;
        }

        return newEx;
    }

    @Override
    protected void checkTypeAssignment(
            @Nullable SqlValidatorScope sourceScope,
            SqlValidatorTable table,
            RelDataType sourceRowType,
            RelDataType targetRowType,
            SqlNode query
    ) {
        // This method is copy-paste from parent SqlValidatorImpl with to key difference
        // (both in "Fall back to default behavior" part of the method):
        // 1) Current implementation doesn't ignore DynamicParams
        // 2) If SqlTypeUtil.canAssignFrom returns `true`, we do double check to account for
        // custom types
        boolean isUpdateModifiableViewTable = false;
        if (query instanceof SqlUpdate) {
            SqlNodeList targetColumnList = requireNonNull(((SqlUpdate) query).getTargetColumnList());
            int targetColumnCount = targetColumnList.size();
            targetRowType = SqlTypeUtil.extractLastNFields(typeFactory, targetRowType, targetColumnCount);
            sourceRowType = SqlTypeUtil.extractLastNFields(typeFactory, sourceRowType, targetColumnCount);
            isUpdateModifiableViewTable = table.unwrap(ModifiableViewTable.class) != null;
        }

        if (SqlTypeUtil.equalAsStructSansNullability(
                typeFactory, sourceRowType, targetRowType, null
        )) {
            // Returns early if source and target row type equals sans nullability.
            return;
        }

        if (config().typeCoercionEnabled() && !isUpdateModifiableViewTable) {
            // Try type coercion first if implicit type coercion is allowed.
            boolean coerced = getTypeCoercion().querySourceCoercion(
                    sourceScope, sourceRowType, targetRowType, query
            );

            if (coerced) {
                return;
            }
        }

        // Fall back to default behavior: compare the type families.
        List<RelDataTypeField> sourceFields = sourceRowType.getFieldList();
        List<RelDataTypeField> targetFields = targetRowType.getFieldList();
        int sourceCount = sourceFields.size();
        for (int i = 0; i < sourceCount; ++i) {
            RelDataType sourceType = sourceFields.get(i).getType();
            RelDataType targetType = targetFields.get(i).getType();

            boolean canAssign = SqlTypeUtil.canAssignFrom(targetType, sourceType);

            if (!canAssign) {
                // Throws proper exception if assignment is not possible due to
                // problem with dynamic parameter type inference.
                validateInferredDynamicParameters();

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

                SqlNode node = getNthExpr(query, i, sourceCount);

                throw newValidationError(node,
                        RESOURCE.typeNotAssignable(
                                targetFields.get(i).getName(), targetTypeString,
                                sourceFields.get(i).getName(), sourceTypeString));
            }
        }
    }

    /**
     * Locates the n'th expression in an INSERT or UPDATE query.
     *
     * @param query       Query
     * @param ordinal     Ordinal of expression
     * @param sourceCount Number of expressions
     * @return Ordinal'th expression, never null
     */
    private static SqlNode getNthExpr(SqlNode query, int ordinal, int sourceCount) {
        if (query instanceof SqlInsert) {
            SqlInsert insert = (SqlInsert) query;
            if (insert.getTargetColumnList() != null) {
                return insert.getTargetColumnList().get(ordinal);
            } else {
                return getNthExpr(
                        insert.getSource(),
                        ordinal,
                        sourceCount);
            }
        } else if (query instanceof SqlUpdate) {
            SqlUpdate update = (SqlUpdate) query;
            if (update.getSourceExpressionList() != null) {
                return update.getSourceExpressionList().get(ordinal);
            } else {
                return getNthExpr(SqlNonNullableAccessors.getSourceSelect(update),
                        ordinal, sourceCount);
            }
        } else if (query instanceof SqlSelect) {
            SqlSelect select = (SqlSelect) query;
            SqlNodeList selectList = SqlNonNullableAccessors.getSelectList(select);
            if (selectList.size() == sourceCount) {
                return selectList.get(ordinal);
            } else {
                return query; // give up
            }
        } else {
            return query; // give up
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

    private static void syncSelectList(SqlMerge call) {
        // Merge creates a source expression list which is not updated after type coercion adds CASTs
        // to source expressions in Update.

        SqlUpdate update = call.getUpdateCall();

        if (update == null) {
            return;
        }

        SqlSelect selectFromUpdate = update.getSourceSelect();

        assert selectFromUpdate != null;

        SqlSelect selectFromMerge = call.getSourceSelect();

        assert selectFromMerge != null : "Merge: SourceSelect has not been set";

        //
        // If a table has N columns and update::TargetColumnList has size = M
        // then select::SelectList has size = N + M:
        // col1, ... colN, value_expr1, ..., value_exprM
        //

        int selectListSize = selectFromUpdate.getSelectList().size();
        int columnsToUpdateSize = update.getTargetColumnList().size(); 
        List<SqlNode> sourceExpressionList = selectFromUpdate.getSelectList().subList(selectListSize - columnsToUpdateSize, selectListSize);
        SqlNodeList selectList = selectFromMerge.getSelectList();
        int sourceExprListSize = sourceExpressionList.size();
        int startPosition = selectList.size() - sourceExprListSize;

        for (var i = 0; i < sourceExprListSize; i++) {
            SqlNode replacement = sourceExpressionList.get(i);
            int position = startPosition + i;

            // This method was introduced to replace an expression with an expression that has the
            // required type cast. Therefore, this only applies when the replacement contains SqlBasicCall.
            // For example a call with SCALAR_QUERY is only present in sourceSelect, keeping original
            // SqlSelect in sourceExpressionList, and we should not make a replacement in this case.
            if (replacement instanceof SqlBasicCall) {
                selectList.set(position, replacement);
            }
        }
    }

    /** {@inheritDoc} */
    @Override
    public void validateLiteral(SqlLiteral literal) {
        SqlTypeName typeName = literal.getTypeName();

        if (typeName != SqlTypeName.DECIMAL) {
            super.validateLiteral(literal);
        }

        // SqlLiteral createSqlType can not be called on
        // SqlUnknownLiteral because SELECT TIMESTAMP 'valid-ts' is a SqlUnknownLiteral later converted to timestamp literal
        if (literal instanceof SqlUnknownLiteral) {
            return;
        } else if (literal.getClass() == SqlLiteral.class
                && !SqlTypeName.CHAR_TYPES.contains(typeName)
                && !SqlTypeName.INTERVAL_TYPES.contains(typeName)
                && !SqlTypeName.BINARY_TYPES.contains(typeName)) {
            // createSqlType can not be called in this case as well.
            return;
        }

        RelDataType dataType = literal.createSqlType(typeFactory);
        validatePrecisionScale(literal, dataType, dataType.getPrecision(), dataType.getScale());
    }

    @Override
    public SqlLiteral resolveLiteral(SqlLiteral literal) {
        SqlLiteral resolved = super.resolveLiteral(literal);
        SqlTypeName typeName = resolved.getTypeName();

        if (literal.getTypeName() != SqlTypeName.UNKNOWN) {
            return resolved;
        }

        if (TypeUtils.isTimestamp(typeName)) {
            String value = literal.getValueAs(String.class);

            if (IgniteSqlDateTimeUtils.isYearOutOfRange(value)) {
                throw newValidationError(literal, IgniteResource.INSTANCE.timestampLiteralOutOfRange(literal.toString()));
            }

            if (typeName == SqlTypeName.TIMESTAMP) {
                long ts = resolved.getValueAs(TimestampString.class).getMillisSinceEpoch();

                if (ts < IgniteSqlFunctions.TIMESTAMP_MIN_INTERNAL || ts > IgniteSqlFunctions.TIMESTAMP_MAX_INTERNAL) {
                    throw newValidationError(literal, IgniteResource.INSTANCE.timestampLiteralOutOfRange(literal.toString()));
                }
            }
        }

        return resolved;
    }

    @Override
    protected RelDataType createTargetRowType(
            SqlValidatorTable table,
            SqlNodeList targetColumnList,
            boolean append,
            SqlIdentifier targetTableAlias
    ) {
        RelDataType baseRowType = table.unwrap(IgniteTable.class).rowTypeForInsert((IgniteTypeFactory) typeFactory);
        if (targetColumnList == null) {
            return baseRowType;
        }
        List<RelDataTypeField> targetFields = baseRowType.getFieldList();
        final PairList<String, RelDataType> fields = PairList.of();
        if (append) {
            for (RelDataTypeField targetField : targetFields) {
                fields.add(SqlUtil.deriveAliasFromOrdinal(fields.size()),
                        targetField.getType());
            }
        }
        final IntSet assignedFields = new IntOpenHashSet(targetColumnList.size());
        final RelOptTable relOptTable = table instanceof RelOptTable
                ? ((RelOptTable) table) : null;
        for (SqlNode node : targetColumnList) {
            SqlIdentifier id = (SqlIdentifier) node;
            RelDataTypeField targetField =
                    SqlValidatorUtil.getTargetField(
                            baseRowType, typeFactory, id, getCatalogReader(), relOptTable);
            if (targetField == null) {
                throw newValidationError(id,
                        RESOURCE.unknownTargetColumn(id.toString()));
            }
            if (!assignedFields.add(targetField.getIndex())) {
                throw newValidationError(id,
                        RESOURCE.duplicateTargetColumn(targetField.getName()));
            }
            fields.add(targetField);
        }
        return typeFactory.createStructType(fields);
    }

    /** {@inheritDoc} */
    @Override
    protected SqlSelect createSourceSelectForUpdate(SqlUpdate call) {
        final SqlNodeList selectList = new SqlNodeList(SqlParserPos.ZERO);
        final SqlIdentifier targetTable = (SqlIdentifier) call.getTargetTable();

        IgniteTable igniteTable = getTableForModification(targetTable);

        SqlIdentifier alias = call.getAlias() != null ? call.getAlias() :
                new SqlIdentifier(deriveAlias(targetTable, 0), SqlParserPos.ZERO);

        igniteTable.rowTypeForUpdate((IgniteTypeFactory) typeFactory)
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
        if (includeSystemVars || exp.getKind() != SqlKind.IDENTIFIER || !isSystemColumnName(deriveAlias(exp, 0))) {
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

        invalidateFetchOffset(select.getFetch(), "fetch / limit");
        invalidateFetchOffset(select.getOffset(), "offset");
    }

    /**
     * Invalidate fetch/offset params restrictions.
     *
     * @param n        Node to check limit.
     * @param nodeName Node name.
     */
    private void invalidateFetchOffset(@Nullable SqlNode n, String nodeName) {
        if (n == null) {
            return;
        }

        if (n instanceof SqlLiteral) {
            BigDecimal offsetFetchLimit = ((SqlLiteral) n).bigDecimalValue();

            checkLimitOffset(offsetFetchLimit, n, nodeName);
        } else if (n instanceof SqlDynamicParam) {
            SqlDynamicParam dynamicParam = (SqlDynamicParam) n;
            SqlTypeName expectType = SqlTypeName.BIGINT;
            RelDataType dynParamType = typeFactory.createSqlType(expectType);

            // Validate value, if present.
            if (!isUnspecified(dynamicParam)) {
                ColumnType paramType = getDynamicParamType(dynamicParam);

                if (paramType == null) {
                    throw newValidationError(n, IgniteResource.INSTANCE.illegalFetchLimit(nodeName));
                }

                dynParamType = deriveDynamicParamType(dynamicParam);

                if (!SqlTypeUtil.isNumeric(dynParamType)) {
                    var err = IgniteResource.INSTANCE.incorrectDynamicParameterType(expectType.toString(),
                            dynParamType.getSqlTypeName().toString());
                    throw newValidationError(n, err);
                }
            }

            // Dynamic parameters are nullable.
            setDynamicParamType(dynamicParam, typeFactory.createTypeWithNullability(dynParamType, true));
        }
    }

    private void checkLimitOffset(BigDecimal offsetFetchLimit, @Nullable SqlNode n, String nodeName) {
        if (offsetFetchLimit.compareTo(LIMIT_UPPER) > 0 || offsetFetchLimit.signum() == -1) {
            throw newValidationError(n, IgniteResource.INSTANCE.illegalFetchLimit(nodeName));
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
        // if we already know the type, no need to re-derive
        RelDataType type = getValidatedNodeTypeIfKnown(expr);
        if (type != null) {
            if (expr instanceof SqlDynamicParam) {
                // Validated node type may be reassigned by the operators return type inference strategy and
                // operands checker. Both of them are ot aware about DynamicParamState we use for validation,
                // thus we need to update return type even if it was already assigned.
                setDynamicParamType((SqlDynamicParam) expr, type);
            }

            return type;
        }

        if (expr instanceof SqlDynamicParam) {
            return deriveDynamicParamType((SqlDynamicParam) expr);
        }

        validateCast(scope, expr);

        RelDataType dataType = super.deriveType(scope, expr);

        validateIn(scope, expr);

        SqlKind sqlKind = expr.getKind();

        // TODO https://issues.apache.org/jira/browse/IGNITE-20163 Remove this exception after this issue is fixed
        if (sqlKind == SqlKind.JSON_VALUE_EXPRESSION) {
            String name = SqlStdOperatorTable.JSON_VALUE_EXPRESSION.getName();
            throw newValidationError(expr, IgniteResource.INSTANCE.unsupportedExpression(name));
        } else if (!SqlKind.BINARY_COMPARISON.contains(sqlKind)) {
            return dataType;
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
        RelDataType any = typeFactory.createSqlType(SqlTypeName.ANY);
        sqlQuery.accept(
                new SqlShuttle() {
                    @Override public SqlNode visit(SqlDynamicParam param) {
                        if (alreadyVisited.add(param.getIndex())) {
                            RelDataType type = getValidatedNodeType(param);

                            int index = param.getIndex();
                            while (types.size() <= index) {
                                // Pad type list with type representing ANY value.
                                types.add(any);
                            }

                            types.set(index, type);
                        }
                        return param;
                    }
                });
        return typeFactory.createStructType(
                types,
                new AbstractList<>() {
                    @Override
                    public String get(int index) {
                        return "?" + index;
                    }

                    @Override
                    public int size() {
                        return types.size();
                    }
                });
    }

    private void validateIn(SqlValidatorScope scope, SqlNode expr) {
        if (expr.getKind() != SqlKind.IN && expr.getKind() != SqlKind.NOT_IN) {
            return;
        }

        // An operand checker of IN operator uses more relaxed rules (see
        // org.apache.calcite.sql.fun.SqlInOperator.deriveType, there
        // OperandTypes.COMPARABLE_UNORDERED_COMPARABLE_UNORDERED is used),
        // allowing comparison of types of different families. Here we add
        // post-validation to make sure comparison is possible only between
        // operands of the same type family.

        SqlCallBinding callBinding = new SqlCallBinding(this, scope, (SqlCall) expr);

        RelDataType leftHandType = callBinding.getOperandType(0);
        RelDataType rightHandType = callBinding.getOperandType(1);

        RelDataType leftRowType = SqlTypeUtil.promoteToRowType(
                typeFactory, leftHandType, null
        );
        RelDataType rightRowType = SqlTypeUtil.promoteToRowType(
                typeFactory, rightHandType, null
        );

        if (!TypeUtils.typeFamiliesAreCompatible(typeFactory, leftRowType, rightRowType)) {
            throw newValidationError(
                    expr,
                    RESOURCE.incompatibleValueType(SqlStdOperatorTable.IN.getName())
            );
        }
    }

    /** Check appropriate type cast availability. */
    private void validateCast(SqlValidatorScope scope, SqlNode expr) {
        if (expr.getKind() != SqlKind.CAST) {
            return;
        }

        // An operand checker of CAST operator uses SqlTypeUtil.canCastFrom method to ensure
        // cast is allowed from given operand to a target type. This utility methods allows
        // every type to be casted to type ANY. Unfortunately, custom types, like UUID, uses the
        // same type name (ANY), which makes possible to cast any operand to UUID. This is not desired
        // behaviour, so we introduced explicit operand type checking for CAST operator to properly
        // handle custom types.

        SqlBasicCall expr0 = (SqlBasicCall) expr;
        SqlNode castOperand = expr0.getOperandList().get(0);
        SqlNode toType = expr0.getOperandList().get(1);

        RelDataType returnType = super.deriveType(scope, toType);

        if (returnType.isStruct()) {
            throw newValidationError(expr, IgniteResource.INSTANCE.dataTypeIsNotSupported(returnType.getSqlTypeName().getName()));
        }

        RelDataType operandType = deriveType(scope, castOperand);

        boolean nullType = isNull(returnType) || isNull(operandType);

        // propagate null type validation
        if (nullType) {
            return;
        }

        returnType = typeFactory().createTypeWithNullability(returnType, operandType.isNullable());

        setValidatedNodeType(expr, returnType);

        if (castOperand instanceof SqlDynamicParam
                && (hasSameTypeName(operandType, returnType, SqlTypeName.DECIMAL)
                || hasSameTypeName(operandType, returnType, SqlTypeName.TIME)
                || hasSameTypeName(operandType, returnType, SqlTypeName.TIMESTAMP)
                || hasSameTypeName(operandType, returnType, SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE))
        ) {
            // By default type of dyn param of type DECIMAL is DECIMAL(28, 6) (see DECIMAL_DYNAMIC_PARAM_PRECISION and
            // DECIMAL_DYNAMIC_PARAM_SCALE at the beginning of the class declaration). Although this default seems
            // reasonable, it may not satisfy all of users. For those who need better control over type of dyn params
            // there is an ability to specify more precise type by CAST operation. Therefore if type of the dyn param
            // is decimal, and it immediately casted to DECIMAL as well, we need override type of the dyn param to desired
            // one.
            // The same is required for temporal types to downcast the value according to the specified precision before
            // going to the index.
            setDynamicParamType((SqlDynamicParam) castOperand, returnType);

            return;
        }

        boolean check = IgniteCustomAssignmentsRules.instance().canApplyFrom(
                returnType.getSqlTypeName(), operandType.getSqlTypeName()
        );

        if (!check) {
            throw newValidationError(expr,
                    RESOURCE.cannotCastValue(operandType.toString(), returnType.toString()));
        }
    }

    /** Returns a flag indicating whether both specified types have the specified type name. */
    private static boolean hasSameTypeName(RelDataType type1, RelDataType type2, SqlTypeName expected) {
        return type1.getSqlTypeName() == expected && type2.getSqlTypeName() == expected;
    }

    @Override
    public void validateDataType(SqlDataTypeSpec dataType) {
        RelDataType type = dataType.deriveType(this);

        SqlTypeNameSpec sqlTypeNameSpec = dataType.getTypeNameSpec();
        if (sqlTypeNameSpec instanceof SqlBasicTypeNameSpec) {
            SqlBasicTypeNameSpec typeNameSpec = (SqlBasicTypeNameSpec) sqlTypeNameSpec;

            validatePrecisionScale(dataType, type, typeNameSpec.getPrecision(), typeNameSpec.getScale());
        }

        super.validateDataType(dataType);
    }

    private void validatePrecisionScale(
            SqlNode typeNode,
            RelDataType type,
            int precision,
            int scale
    ) {
        // TypeFactory sets type's precision to maxPrecision if it exceeds type's maxPrecision.
        // Use precision/scale from type name spec to correct this issue.
        // Negative values are rejected by the parser so we need to check only max values.

        SqlTypeName typeName = type.getSqlTypeName();
        ColumnType columnType;
        // Report a standard validation error when the given SQL type is not supported.
        try {
            columnType = TypeUtils.columnType(type);
        } catch (Exception ignore) {
            throw newValidationError(typeNode, IgniteResource.INSTANCE.dataTypeIsNotSupported(typeName.getName()));
        }
        boolean allowsLength = columnType.lengthAllowed();
        boolean allowsScale = columnType.scaleAllowed();
        boolean allowsPrecision = columnType.precisionAllowed();

        RelDataTypeSystem typeSystem = typeFactory.getTypeSystem();

        if (precision != PRECISION_NOT_SPECIFIED && (allowsPrecision || allowsLength)) {
            int minPrecision = typeSystem.getMinPrecision(typeName);
            int maxPrecision = typeSystem.getMaxPrecision(typeName);

            // Empty varchar/bytestring literals have zero precision
            if (typeNode instanceof SqlLiteral && SqlTypeFamily.STRING.contains(type)) {
                minPrecision = 0;
            }

            if (precision < minPrecision || precision > maxPrecision) {
                String spaceName = typeName.getSpaceName();
                if (allowsLength) {
                    throw newValidationError(typeNode,
                            IgniteResource.INSTANCE.invalidLengthForType(spaceName, precision, minPrecision, maxPrecision));
                } else {
                    throw newValidationError(typeNode,
                            IgniteResource.INSTANCE.invalidPrecisionForType(spaceName, precision, minPrecision, maxPrecision));
                }
            }
        }

        if (scale != SCALE_NOT_SPECIFIED && allowsScale) {
            int minScale = typeSystem.getMinScale(typeName);
            int maxScale = typeSystem.getMaxScale(typeName);

            if (scale < minScale || scale > maxScale) {
                throw newValidationError(typeNode,
                        IgniteResource.INSTANCE.invalidScaleForType(typeName.getSpaceName(), scale, minScale, maxScale));
            }
        }
    }

    @Override
    protected void validateJoin(SqlJoin join, SqlValidatorScope scope) {
        if (join.getJoinType() == JoinType.ASOF || join.getJoinType() == JoinType.LEFT_ASOF) {
            throw new SqlException(STMT_VALIDATION_ERR, "Unsupported join type: " + join.getJoinType().toString().replace("_", " "));
        }

        super.validateJoin(join, scope);

        if (join.isNatural() || join.getConditionType() == JoinConditionType.USING) {
            // TODO Remove this method after https://issues.apache.org/jira/browse/IGNITE-22295
            validateJoinCondition(join);
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

        node = super.performUnconditionalRewrites(node, underFrom);

        if (node instanceof SqlUpdate) {
            SqlUpdate update = (SqlUpdate) node;

            for (int i = 0; i < update.getTargetColumnList().size(); i++) {
                // All expressions from SourceExpressionList were inlined into source select
                // during UnconditionalRewrite phase, therefore we don't care about the former list.
                // To avoid problems with this list (for instance, subqueries are not processed well:
                // they are neither transformed to joins nor converted to physical relations causing
                // json serializer to throw an assertion) we rewrite it with some dummy values.
                update.getSourceExpressionList().set(i, update.getTargetColumnList().get(i));
            }
        }

        return node;
    }

    @Override
    public SqlTypeMappingRule getTypeMappingRule() {
        return IgniteCustomAssignmentsRules.instance();
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

    /** Rewrite NATURAL join condition into a predicate. */
    private void validateJoinCondition(SqlJoin join) {
        SqlValidatorNamespace leftNs = getNamespace(join.getLeft());
        requireNonNull(leftNs, "leftNs");

        SqlValidatorNamespace rightNs = getNamespace(join.getRight());
        requireNonNull(leftNs, "rightNs");

        List<String> joinColumnList = SqlValidatorUtil.deriveNaturalJoinColumnList(
                getCatalogReader().nameMatcher(),
                leftNs.getRowType(),
                rightNs.getRowType());

        // Natural join between relations with a disjoint set of common columns
        if (joinColumnList.isEmpty()) {
            return;
        }

        for (int i = 0; i < joinColumnList.size(); i++) {
            String col = joinColumnList.get(i);
            RelDataTypeField leftField = leftNs.getRowType().getField(col, true, false);
            RelDataTypeField rightField = rightNs.getRowType().getField(col, true, false);

            assert leftField != null;
            assert rightField != null;

            RelDataType leftType = leftField.getType();
            RelDataType rightType = rightField.getType();

            if (!TypeUtils.typesRepresentTheSameColumnTypes(leftType, rightType)) {
                throw newValidationError(join, IgniteResource.INSTANCE.naturalOrUsingColumnNotCompatible(
                        i, leftType.toString(), rightType.toString())
                );
            }
        }
    }

    private void validateAggregateFunction(SqlCall call, SqlAggFunction aggFunction) {
        if (!aggFunction.isAggregator()) {
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
            case GROUPING:
                if (call.operandCount() > 63) {
                    // Function result of BIGINT can fit only bitmask of length less than 64;
                    throw newValidationError(call, IgniteResource.INSTANCE.invalidArgCount(aggFunction.getName(), 1, 63));
                }
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

    /** Returns {@code true} if the given alias is a system column name, {@code false} otherwise. */
    public static boolean isSystemColumnName(String alias) {
        return (Commons.implicitPkEnabled() && Commons.IMPLICIT_PK_COL_NAME.equals(alias))
                || alias.equals(Commons.PART_COL_NAME)
                || alias.equals(Commons.PART_COL_NAME_LEGACY);
    }

    // We use these scopes to filter out valid usages of a ROW operator.
    private final ArrayDeque<CallScope> callScopes = new ArrayDeque<>();

    /** {@inheritDoc} */
    @Override
    protected void validateValues(SqlCall node, RelDataType targetRowType, SqlValidatorScope scope) {
        // TODO: https://issues.apache.org/jira/browse/IGNITE-22084: Sql. Add support for row data type.
        // ROW operator is used in VALUES (row), (row1)
        callScopes.push(CallScope.VALUES);
        try {
            // Special handling of DEFAULT operator.
            //
            // By default its return type is ANY, and default implementation of validator
            // consider ANY as being assignable to any type. We made rules more strict,
            // therefore return type of DEFAULT operator must be derived as type of the
            // corresponding column.
            for (SqlNode rowConstructorNode : node.getOperandList()) {
                SqlCall rowConstructorCall = (SqlCall) rowConstructorNode;

                for (int i = 0; i < rowConstructorCall.operandCount(); i++) {
                    SqlNode operand = rowConstructorCall.operand(i);

                    if (operand.getKind() == SqlKind.DEFAULT) {
                        setValidatedNodeType(operand, targetRowType.getFieldList().get(i).getType());
                    }
                }
            }

            super.validateValues(node, targetRowType, scope);
        } finally {
            callScopes.pop();
        }
    }

    /** {@inheritDoc} */
    @Override
    protected void validateGroupClause(SqlSelect select) {
        // Calcite uses the ROW operator in the GROUP BY clause in the following cases:
        // - GROUP BY GROUPING SET ((a, b), (c, d))
        // - GROUP BY (a, b) (but GROUP BY a, b does not use the ROW operator)
        //
        // We need to make sure that the validator won't reject such clauses.
        SqlNodeList group = select.getGroup() == null ? SqlNodeList.EMPTY : select.getGroup();
        boolean rowInGroupScope = false;

        for (SqlNode node : group) {
            if (node.getKind() == SqlKind.GROUPING_SETS || node.getKind() == SqlKind.ROW) {
                rowInGroupScope = true;
                break;
            }
        }

        if (!rowInGroupScope) {
            super.validateGroupClause(select);
        } else {
            callScopes.push(CallScope.GROUP);

            try {
                super.validateGroupClause(select);
            } finally {
                callScopes.pop();
            }
        }
    }

    /** {@inheritDoc} */
    @Override
    public void validateCall(SqlCall call, SqlValidatorScope scope) {
        if (call.getKind() == SqlKind.AS) {
            String alias = deriveAlias(call, 0);

            if (isSystemColumnName(alias)) {
                throw newValidationError(call, IgniteResource.INSTANCE.illegalAlias(alias));
            }
        }

        CallScope callScope = callScopes.peek();
        boolean validatingRowOperator = call.getOperator() == SqlStdOperatorTable.ROW;
        boolean insideValues = callScope == CallScope.VALUES;
        boolean insideGroupClause = callScope == CallScope.GROUP;
        boolean valuesCall = call.getOperator() == SqlStdOperatorTable.VALUES;

        if (validatingRowOperator && !(insideValues || insideGroupClause)) {
            throw newValidationError(call, IgniteResource.INSTANCE.dataTypeIsNotSupported(call.getOperator().getName()));
        }

        if (valuesCall) {
            // VALUES in the WHERE clause in VALUES operator, which is not validated via validateValues method.
            callScopes.push(CallScope.VALUES);
        } else if (insideGroupClause) {
            // Allow GROUPING SET ( (a,b), (c, d) ) and GROUP BY (a, b)
            callScopes.push(CallScope.GROUP);
        } else {
            callScopes.push(CallScope.OTHER);
        }

        try {
            if (call.getKind() == SqlKind.INTERVAL) {
                assert !(call.operand(0) instanceof SqlCharStringLiteral)
                        && !(call.operand(0) instanceof SqlIntervalLiteral)
                        : "Should never got here in case of interval literal";

                throw newValidationError(call, IgniteResource.INSTANCE.unsupportedExpression("String literal expected"));
            }
            super.validateCall(call, scope);
        } finally {
            callScopes.pop();
        }
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
        RelDataType type = deriveDynamicParamTypeIfNotKnown(dynamicParam);

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

    private RelDataType deriveDynamicParamTypeIfNotKnown(SqlDynamicParam dynamicParam) {
        RelDataType validatedNodeType = getValidatedNodeTypeIfKnown(dynamicParam);

        if (validatedNodeType != null) {
            return validatedNodeType;
        }

        return deriveDynamicParamType(dynamicParam);
    }

    /** Derives the type of the given dynamic parameter. */
    private RelDataType deriveDynamicParamType(SqlDynamicParam dynamicParam) {
        dynamicParamNodes.put(dynamicParam, dynamicParam);

        if (isUnspecified(dynamicParam)) {
            setDynamicParamType(dynamicParam, unknownType);

            return unknownType;
        } else {
            ColumnType parameterType = getDynamicParamType(dynamicParam);

            // Dynamic parameters are always nullable.
            // Otherwise it seem to cause "Conversion to relational algebra failed to preserve datatypes" errors
            // in some cases.
            RelDataType nullableType = typeFactory.createTypeWithNullability(relTypeFromDynamicParamType(parameterType), true);

            setDynamicParamType(dynamicParam, nullableType);

            return nullableType;
        }
    }

    /** if dynamic parameter is not specified, set its type to the provided type, otherwise return the type of its value. */
    RelDataType resolveDynamicParameterType(SqlDynamicParam dynamicParam, RelDataType contextType) {
        if (isUnspecified(dynamicParam)) {
            RelDataType nullableContextType = typeFactory.createTypeWithNullability(contextType, true);

            setDynamicParamType(dynamicParam, nullableContextType);

            return nullableContextType;
        } else {
            return deriveDynamicParamTypeIfNotKnown(dynamicParam);
        }
    }

    private void setDynamicParamType(SqlDynamicParam dynamicParam, RelDataType dataType) {
        setValidatedNodeType(dynamicParam, dataType);

        setDynamicParamResolvedType(dynamicParam, dataType);
    }

    /**
     * Returns the type of the given dynamic parameter. If the type is not specified,
     * this method throws {@link IllegalArgumentException}.
     */
    @Nullable
    private ColumnType getDynamicParamType(SqlDynamicParam dynamicParam) {
        int paramIndex = dynamicParam.getIndex();

        if (isUnspecified(dynamicParam)) {
            throw new IllegalArgumentException(format("Value of dynamic parameter#{} is not specified", paramIndex));
        } else {
            return dynamicParameters.get(paramIndex).colType;
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

            if (!state.hasType) {
                continue;
            }

            RelDataType derivedType = getValidatedNodeTypeIfKnown(node);
            RelDataType paramType = state.resolvedType;

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
    boolean isUnspecified(SqlDynamicParam param) {
        int index = param.getIndex();
        DynamicParamState state = dynamicParameters.computeIfAbsent(index, (i) -> new DynamicParamState());

        return !state.hasType;
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

        final ColumnType colType;

        final boolean hasType;

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

        private DynamicParamState(@Nullable ColumnType colType) {
            this.colType = colType;
            this.hasType = true;
        }

        private DynamicParamState() {
            this.colType = null;
            this.hasType = false;
        }
    }

    /**
     * Scope to distinguish between different usages of the ROW operator.
     */
    // TODO: https://issues.apache.org/jira/browse/IGNITE-22084: Sql. Add support for row data type. Remove after row type is supported.
    private enum CallScope {
        VALUES,
        GROUP,
        OTHER
    }

    private RelDataType relTypeFromDynamicParamType(@Nullable ColumnType type) {
        if (type == null) {
            return typeFactory.createSqlType(SqlTypeName.NULL);
        }

        switch (type) {
            case NULL:
                return typeFactory.createSqlType(SqlTypeName.NULL);
            case BOOLEAN:
                return typeFactory.createSqlType(SqlTypeName.BOOLEAN);
            case INT8:
                return typeFactory.createSqlType(SqlTypeName.TINYINT);
            case INT16:
                return typeFactory.createSqlType(SqlTypeName.SMALLINT);
            case INT32:
                return typeFactory.createSqlType(SqlTypeName.INTEGER);
            case INT64:
                return typeFactory.createSqlType(SqlTypeName.BIGINT);
            case FLOAT:
                return typeFactory.createSqlType(SqlTypeName.REAL);
            case DOUBLE:
                return typeFactory.createSqlType(SqlTypeName.DOUBLE);
            case DATE:
                return typeFactory.createSqlType(SqlTypeName.DATE);
            case TIME:
                return typeFactory.createSqlType(SqlTypeName.TIME, TEMPORAL_DYNAMIC_PARAM_PRECISION);
            case DATETIME:
                return typeFactory.createSqlType(SqlTypeName.TIMESTAMP, TEMPORAL_DYNAMIC_PARAM_PRECISION);
            case TIMESTAMP:
                return typeFactory.createSqlType(SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE, TEMPORAL_DYNAMIC_PARAM_PRECISION);
            case BYTE_ARRAY:
                return typeFactory.createSqlType(SqlTypeName.VARBINARY, PRECISION_NOT_SPECIFIED);
            case STRING:
                return typeFactory.createSqlType(SqlTypeName.VARCHAR, PRECISION_NOT_SPECIFIED);
            case UUID:
                return typeFactory.createSqlType(SqlTypeName.UUID);
            case DECIMAL:
                return typeFactory.createSqlType(
                        SqlTypeName.DECIMAL, DECIMAL_DYNAMIC_PARAM_PRECISION, DECIMAL_DYNAMIC_PARAM_SCALE
                );
            default:
                throw new AssertionError("Unknown type " + type);
        }
    }
}
