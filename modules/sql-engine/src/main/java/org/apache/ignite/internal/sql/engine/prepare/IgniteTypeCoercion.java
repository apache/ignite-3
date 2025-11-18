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
import static org.apache.calcite.linq4j.Nullness.castNonNull;
import static org.apache.calcite.sql.type.NonNullableAccessors.getCollation;
import static org.apache.calcite.sql.validate.SqlNonNullableAccessors.getScope;
import static org.apache.calcite.util.Static.RESOURCE;
import static org.apache.ignite.internal.sql.engine.util.TypeUtils.typeFamiliesAreCompatible;

import java.nio.charset.Charset;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.rel.type.DynamicRecordType;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.SqlBasicTypeNameSpec;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlCollation;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlDynamicParam;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlUpdate;
import org.apache.calcite.sql.SqlUtil;
import org.apache.calcite.sql.fun.SqlCase;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeMappingRule;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.sql.validate.implicit.TypeCoercionImpl;
import org.apache.calcite.util.Util;
import org.apache.ignite.internal.sql.engine.type.IgniteTypeFactory;
import org.apache.ignite.internal.sql.engine.util.IgniteCustomAssignmentsRules;
import org.apache.ignite.internal.sql.engine.util.IgniteResource;
import org.jetbrains.annotations.Nullable;

/** Implicit type cast implementation. */
public class IgniteTypeCoercion extends TypeCoercionImpl {
    private final IgniteTypeFactory typeFactory;

    public IgniteTypeCoercion(RelDataTypeFactory typeFactory, SqlValidator validator) {
        super(typeFactory, validator);
        this.typeFactory = (IgniteTypeFactory) typeFactory;
    }

    /** {@inheritDoc} **/
    @Override
    public boolean binaryComparisonCoercion(SqlCallBinding binding) {
        SqlCall call = binding.getCall();
        if (binding.getOperandCount() != 2 || !SqlKind.BINARY_COMPARISON.contains(call.getKind())) {
            return super.binaryComparisonCoercion(binding);
        }

        RelDataType leftType = binding.getOperandType(0);
        RelDataType rightType = binding.getOperandType(1);

        if (!typeFamiliesAreCompatible(typeFactory, leftType, rightType)) {
            return false;
        }

        validateBinaryComparisonCoercion(binding, leftType, rightType, (IgniteSqlValidator) validator);

        // If types are equal, no need in coercion.
        if (leftType.equals(rightType)) {
            return false;
        }

        // Otherwise find the least restrictive type among the operand types
        // and coerce the operands to that type if such type exists.
        //
        // An example of a least restrictive type from the javadoc for RelDataTypeFactory::leastRestrictive:
        // leastRestrictive(INT, NUMERIC(3, 2)) could be NUMERIC(12, 2)
        //
        // A least restrictive type between types of different type families does not exist -
        // the method returns null (See SqlTypeFactoryImpl::leastRestrictive).
        //
        RelDataType targetType = factory.leastRestrictive(Arrays.asList(leftType, rightType));

        if (targetType == null) {
            return false;
        }

        boolean coerced = false;
        SqlValidatorScope scope = binding.getScope();

        if (!leftType.equals(targetType)) {
            coerced = coerceOperandType(scope, call, 0, targetType);
        }

        if (!rightType.equals(targetType)) {
            boolean rightCoerced = coerceOperandType(scope, call, 1, targetType);
            coerced = coerced || rightCoerced;
        }

        return coerced;
    }

    /** {@inheritDoc} */
    @Override
    public boolean binaryArithmeticCoercion(SqlCallBinding binding) {
        if (binding.getOperandCount() != 2) {
            return super.binaryComparisonCoercion(binding);
        }

        RelDataType leftType = binding.getOperandType(0);
        RelDataType rightType = binding.getOperandType(1);

        //noinspection SimplifiableIfStatement
        if (!typeFamiliesAreCompatible(typeFactory, leftType, rightType)) {
            return false;
        }

        return super.binaryArithmeticCoercion(binding);
    }

    /** {@inheritDoc} */
    @Override
    public boolean builtinFunctionCoercion(SqlCallBinding binding, List<RelDataType> operandTypes, List<SqlTypeFamily> expectedFamilies) {
        // if there is any unspecified dynamic params, let's throw a refined exception
        validateFunctionOperands(binding, operandTypes, expectedFamilies);

        boolean typesAreCompatible = true;
        if (binding.getOperandCount() == 2 && SqlKind.BINARY_ARITHMETIC.contains(binding.getCall().getKind())) {
            RelDataType leftType = binding.getOperandType(0);
            RelDataType rightType = binding.getOperandType(1);

            if (!typeFamiliesAreCompatible(typeFactory, leftType, rightType)) {
                typesAreCompatible = false;
            }
        } else {
            assert operandTypes.size() == expectedFamilies.size();

            for (int i = 0; i < operandTypes.size(); i++) {
                RelDataType typeFromExpectedFamily = expectedFamilies.get(i).getDefaultConcreteType(typeFactory);

                if (typeFromExpectedFamily == null) {
                    // function may accept literally ANY value
                    continue;
                }

                if (!typeFamiliesAreCompatible(typeFactory, typeFromExpectedFamily, operandTypes.get(i))) {
                    typesAreCompatible = false;

                    break;
                }
            }
        }

        //noinspection SimplifiableIfStatement
        if (!typesAreCompatible) {
            return false;
        }

        return super.builtinFunctionCoercion(binding, operandTypes, expectedFamilies);
    }

    /** {@inheritDoc} */
    @Override
    public boolean inOperationCoercion(SqlCallBinding binding) {
        SqlOperator operator = binding.getOperator();
        if (!operatorIsInOrQuantify(operator)) {
            return false;
        }

        assert binding.getOperandCount() == 2;

        RelDataType type1 = binding.getOperandType(0);
        RelDataType type2 = binding.getOperandType(1);

        RelDataType leftRowType = SqlTypeUtil.promoteToRowType(
                typeFactory, type1, null
        );
        RelDataType rightRowType = SqlTypeUtil.promoteToRowType(
                typeFactory, type2, null
        );

        if (!typeFamiliesAreCompatible(typeFactory, leftRowType, rightRowType)) {
            return false;
        }

        if (operatorIsQuantify(operator)) {
            return quantifyOperationCoercion(binding);
        }

        return super.inOperationCoercion(binding);
    }

    private static boolean operatorIsIn(SqlOperator operator) {
        return operator.getKind().belongsTo(Set.of(SqlKind.IN, SqlKind.NOT_IN));
    }

    private static boolean operatorIsQuantify(SqlOperator operator) {
        return operator.getKind().belongsTo(Set.of(SqlKind.SOME, SqlKind.ALL));
    }

    private static boolean operatorIsInOrQuantify(SqlOperator operator) {
        return operatorIsIn(operator) || operatorIsQuantify(operator);
    }

    private boolean quantifyOperationCoercion(SqlCallBinding binding) {
        // This method is a copy-paste of org.apache.calcite.sql.validate.implicit.TypeCoercionImpl.inOperationCoercion
        // with stripped validation of operation kind.
        assert operatorIsQuantify(binding.getOperator());
        assert binding.getOperandCount() == 2;

        RelDataType type1 = binding.getOperandType(0);
        RelDataType type2 = binding.getOperandType(1);
        SqlNode node1 = binding.operand(0);
        SqlNode node2 = binding.operand(1);
        SqlValidatorScope scope = binding.getScope();

        if (type1.isStruct() && type2.isStruct() && type1.getFieldCount() != type2.getFieldCount()) {
            return false;
        }

        int colCount = type1.isStruct() ? type1.getFieldCount() : 1;
        RelDataType[] argTypes = new RelDataType[2];
        argTypes[0] = type1;
        argTypes[1] = type2;
        boolean coerced = false;
        List<RelDataType> widenTypes = new ArrayList<>();
        for (int i = 0; i < colCount; i++) {
            int i2 = i;
            List<RelDataType> columnIthTypes = new AbstractList<>() {
                @Override
                public RelDataType get(int index) {
                    return argTypes[index].isStruct() ? argTypes[index].getFieldList().get(i2).getType() : argTypes[index];
                }

                @Override
                public int size() {
                    return argTypes.length;
                }
            };

            RelDataType widenType = commonTypeForBinaryComparison(columnIthTypes.get(0), columnIthTypes.get(1));
            if (widenType == null) {
                widenType = getTightestCommonType(columnIthTypes.get(0), columnIthTypes.get(1));
            }
            if (widenType == null) {
                // Can not find any common type, just return early.
                return false;
            }
            widenTypes.add(widenType);
        }
        // Find all the common type for RSH and LSH columns.
        assert widenTypes.size() == colCount;
        for (int i = 0; i < widenTypes.size(); i++) {
            RelDataType desired = widenTypes.get(i);
            // LSH maybe a row values or single node.
            if (node1.getKind() == SqlKind.ROW) {
                assert node1 instanceof SqlCall;
                if (coerceOperandType(scope, (SqlCall) node1, i, desired)) {
                    updateInferredColumnType(requireNonNull(scope, "scope"), node1, i, widenTypes.get(i));
                    coerced = true;
                }
            } else {
                coerced = coerceOperandType(scope, binding.getCall(), 0, desired) || coerced;
            }
            // RHS may be a row values expression or sub-query.
            if (node2 instanceof SqlNodeList) {
                final SqlNodeList node3 = (SqlNodeList) node2;
                boolean listCoerced = false;
                if (type2.isStruct()) {
                    for (SqlNode node : (SqlNodeList) node2) {
                        assert node instanceof SqlCall;
                        listCoerced = coerceOperandType(scope, (SqlCall) node, i, desired) || listCoerced;
                    }
                    if (listCoerced) {
                        updateInferredColumnType(requireNonNull(scope, "scope"), node2, i, desired);
                    }
                } else {
                    for (int j = 0; j < ((SqlNodeList) node2).size(); j++) {
                        listCoerced = coerceColumnType(scope, node3, j, desired) || listCoerced;
                    }
                    if (listCoerced) {
                        updateInferredType(node2, desired);
                    }
                }
                coerced = coerced || listCoerced;
            } else {
                // Another sub-query.
                SqlValidatorScope scope1 = node2 instanceof SqlSelect ? validator.getSelectScope((SqlSelect) node2) : scope;
                coerced = rowTypeCoercion(scope1, node2, i, desired) || coerced;
            }
        }
        return coerced;
    }

    /** {@inheritDoc} */
    @Override
    public boolean caseWhenCoercion(SqlCallBinding callBinding) {
        SqlValidatorScope scope = getScope(callBinding);
        SqlCase caseCall = (SqlCase) callBinding.getCall();
        SqlNodeList thenList = caseCall.getThenOperands();
        List<RelDataType> types = new ArrayList<>();
        for (SqlNode node : thenList) {
            RelDataType type = validator.deriveType(scope, node);

            types.add(type);
        }

        SqlNode elseOp = requireNonNull(
                caseCall.getElseOperand(),
                () -> "getElseOperand() is null for " + caseCall
        );

        RelDataType elseOpType = validator.deriveType(scope, elseOp);

        types.add(elseOpType);

        //noinspection SimplifiableIfStatement
        if (!typeFamiliesAreCompatible(typeFactory, types)) {
            return false;
        }

        return super.caseWhenCoercion(callBinding);
    }

    /** {@inheritDoc} */
    @Override
    public boolean querySourceCoercion(
            @Nullable SqlValidatorScope scope,
            RelDataType sourceRowType,
            RelDataType targetRowType,
            SqlNode query
    ) {
        if (sourceRowType.getFieldCount() != targetRowType.getFieldCount()) {
            return false;
        }

        for (int i = 0; i < targetRowType.getFieldCount(); i++) {
            RelDataType sourceField = sourceRowType.getFieldList().get(i).getType();
            RelDataType targetField = targetRowType.getFieldList().get(i).getType();

            if (!typeFamiliesAreCompatible(typeFactory, sourceField, targetField)) {
                return false;
            }
        }

        validateDynamicParametersInModify(scope, targetRowType, query);

        List<RelDataTypeField> sourceFields = sourceRowType.getFieldList();
        List<RelDataTypeField> targetFields = targetRowType.getFieldList();
        int sourceCount = sourceFields.size();
        SqlTypeMappingRule mappingRule = validator.getTypeMappingRule();
        for (int i = 0; i < sourceCount; i++) {
            RelDataType sourceType = sourceFields.get(i).getType();
            RelDataType targetType = targetFields.get(i).getType();
            if (!SqlTypeUtil.equalSansNullability(validator.getTypeFactory(), sourceType, targetType)
                    && !SqlTypeUtil.canCastFrom(targetType, sourceType, mappingRule)) {
                // Returns early if types not equals and can not do type coercion.
                return false;
            }
        }
        boolean coerced = false;
        for (int i = 0; i < sourceFields.size(); i++) {
            RelDataType targetType = targetFields.get(i).getType();
            coerced = coerceSourceRowType(scope, query, i, targetType) || coerced;
        }
        return coerced;
    }

    @SuppressWarnings("MethodOverridesInaccessibleMethodOfSuper")
    private boolean coerceSourceRowType(
            @Nullable SqlValidatorScope sourceScope,
            SqlNode query,
            int columnIndex,
            RelDataType targetType) {
        switch (query.getKind()) {
            case INSERT:
                SqlInsert insert = (SqlInsert) query;
                return coerceSourceRowType(sourceScope,
                        insert.getSource(),
                        columnIndex,
                        targetType);
            case UPDATE:
                SqlUpdate update = (SqlUpdate) query;
                SqlSelect select = castNonNull(update.getSourceSelect());
                columnIndex += select.getSelectList().size() - update.getTargetColumnList().size();
                return coerceSourceRowType(sourceScope,
                        select,
                        columnIndex,
                        targetType);
            default:
                return rowTypeCoercion(sourceScope, query, columnIndex, targetType);
        }
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable RelDataType getWiderTypeFor(
            List<RelDataType> typeList,
            boolean stringPromotion
    ) {
        if (!typeFamiliesAreCompatible(typeFactory, typeList)) {
            return null;
        }

        return super.getWiderTypeFor(typeList, stringPromotion);
    }

    /** {@inheritDoc} */
    @Override
    protected boolean needToCast(SqlValidatorScope scope, SqlNode node, RelDataType toType) {
        RelDataType fromType = validator.deriveType(scope, node);

        // We should not insert additional implicit casts between char and varchar because casts between these types
        // perform data silent truncation as defined by the SQL standard.
        // Do not add implicit casts to prevent data truncation from sneaking in.
        if (SqlTypeUtil.isCharacter(toType) && SqlTypeUtil.isCharacter(fromType)) {
            return false;
        }

        if (SqlTypeUtil.equalSansNullability(typeFactory, fromType, toType)) {
            // Implicit type coercion does not handle nullability.
            return false;
        } else if (SqlTypeUtil.isBinary(toType) && SqlTypeUtil.isBinary(fromType)) {
            // No need to cast between binary types.
            return false;
        } else if (SqlTypeUtil.isInterval(toType)) {
            if (SqlTypeUtil.isInterval(fromType)) {
                // Two different families of intervals: INTERVAL_DAY_TIME and INTERVAL_YEAR_MONTH.
                return fromType.getSqlTypeName().getFamily() != toType.getSqlTypeName().getFamily();
            }
        } else if (SqlTypeUtil.isIntType(toType)) {
            // The following checks ensure that there no ClassCastException when casting from one
            // integer type to another (e.g. int to smallint, int to bigint)
            if (SqlTypeUtil.isIntType(fromType) && fromType.getSqlTypeName() != toType.getSqlTypeName()) {
                return true;
            }
        } else if (SqlTypeUtil.isNull(fromType)) {
            // Need to cast NULL literal because type-checkers of built-in function cannot properly handle explicit NULL
            // since it belongs to a particular type family.
            return true;
        }

        return IgniteCustomAssignmentsRules.instance().canApplyFrom(toType.getSqlTypeName(), fromType.getSqlTypeName());
    }

    /** {@inheritDoc} */
    @Override
    protected boolean coerceOperandType(
            @Nullable SqlValidatorScope scope,
            SqlCall call,
            int index,
            RelDataType targetType) {
        // The method is fully copy from parent class with modified handling of dynamic parameters and [var]char types coercion.

        // Transform the JavaType to SQL type because the SqlDataTypeSpec
        // does not support deriving JavaType yet.
        if (RelDataTypeFactoryImpl.isJavaType(targetType)) {
            targetType = ((JavaTypeFactory) factory).toSql(targetType);
        }

        SqlNode operand = call.getOperandList().get(index);
        if (operand instanceof SqlDynamicParam) {
            validateOperand((SqlDynamicParam) operand, targetType, call.getOperator(), (IgniteSqlValidator) validator);
        }

        // We should never coerce DEFAULT, since it is going to break
        // SqlToRelConverter::convertColumnList and DEFAULTs are not going to be replaced with
        // values, produced by initializerFactory.newColumnDefaultValue.
        if (operand.getKind() == SqlKind.DEFAULT) {
            // DEFAULT is also of type ANY
            return false;
        }

        requireNonNull(scope, "scope");
        RelDataType operandType = validator.deriveType(scope, operand);
        if (coerceStringToArray(call, operand, index, operandType, targetType)) {
            return true;
        }
        // Check it early.
        if (!needToCast(scope, operand, targetType)) {
            return false;
        }
        // Fix up nullable attr.
        RelDataType targetType1 = syncAttributes(operandType, targetType);
        SqlNode desired = castTo(operand, targetType1);
        call.setOperand(index, desired);
        updateInferredType(desired, targetType1);
        return true;
    }

    /** {@inheritDoc} */
    @Override
    protected boolean coerceColumnType(
            @Nullable SqlValidatorScope scope,
            SqlNodeList nodeList,
            int index,
            RelDataType targetType) {
        // The method is fully copy from parent class with modified handling of dynamic parameters.

        // Transform the JavaType to SQL type because the SqlDataTypeSpec
        // does not support deriving JavaType yet.
        if (RelDataTypeFactoryImpl.isJavaType(targetType)) {
            targetType = ((JavaTypeFactory) factory).toSql(targetType);
        }

        // This will happen when there is a star/dynamic-star column in the select list,
        // and the source is values expression, i.e. `select * from (values(1, 2, 3))`.
        // There is no need to coerce the column type, only remark
        // the inferred row type has changed, we will then add in type coercion
        // when expanding star/dynamic-star.

        // See SqlToRelConverter#convertSelectList for details.
        if (index >= nodeList.size()) {
            // Can only happen when there is a star(*) in the column,
            // just return true.
            return true;
        }

        SqlNode node = nodeList.get(index);

        if (node instanceof SqlIdentifier) {
            // Do not expand a star/dynamic table col.
            SqlIdentifier node1 = (SqlIdentifier) node;
            if (node1.isStar()) {
                return true;
            } else if (DynamicRecordType.isDynamicStarColName(Util.last(node1.names))) {
                // Should support implicit cast for dynamic table.
                return false;
            }
        }

        requireNonNull(scope, "scope is needed for needToCast(scope, operand, targetType)");
        if (node instanceof SqlCall) {
            SqlCall node2 = (SqlCall) node;
            if (node2.getOperator().kind == SqlKind.AS) {
                final SqlNode operand = node2.operand(0);
                if (!needToCast(scope, operand, targetType)) {
                    return false;
                }
                RelDataType targetType2 = syncAttributes(validator.deriveType(scope, operand), targetType);
                final SqlNode casted = castTo(operand, targetType2);
                node2.setOperand(0, casted);
                updateInferredType(casted, targetType2);
                return true;
            }
        }
        if (!needToCast(scope, node, targetType)) {
            return false;
        }
        RelDataType targetType3 = syncAttributes(validator.deriveType(scope, node), targetType);
        final SqlNode node3 = castTo(node, targetType3);
        nodeList.set(index, node3);
        updateInferredType(node3, targetType3);
        return true;
    }

    /**
     * Sync the data type additional attributes before casting,
     * i.e. nullability, charset, collation.
     */
    @SuppressWarnings("PMD.MissingOverride")
    RelDataType syncAttributes(
            RelDataType fromType,
            RelDataType toType) {
        RelDataType syncedType = toType;
        if (fromType != null) {
            syncedType = factory.createTypeWithNullability(syncedType, fromType.isNullable());
            if (SqlTypeUtil.inCharOrBinaryFamilies(fromType)
                    && SqlTypeUtil.inCharOrBinaryFamilies(toType)) {
                Charset charset = fromType.getCharset();
                if (charset != null && SqlTypeUtil.inCharFamily(syncedType)) {
                    SqlCollation collation = getCollation(fromType);
                    syncedType =
                            factory.createTypeWithCharsetAndCollation(syncedType, charset,
                                    collation);
                }
            }
        }
        return syncedType;
    }

    /** {@inheritDoc} **/
    @Override
    public @Nullable RelDataType commonTypeForBinaryComparison(@Nullable RelDataType type1, @Nullable RelDataType type2) {
        if (type1 == null || type2 == null) {
            return null;
        }

        SqlTypeName t1 = type1.getSqlTypeName();
        SqlTypeName t2 = type2.getSqlTypeName();

        if (t1 == SqlTypeName.TIMESTAMP && t2 == SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE
                || t1 == SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE && t2 == SqlTypeName.TIMESTAMP) {
            return typeFactory.leastRestrictive(List.of(type1, type2));
        } else {
            return super.commonTypeForBinaryComparison(type1, type2);
        }
    }

    private static SqlNode castTo(SqlNode node, RelDataType type) {
        SqlDataTypeSpec targetDataType;
        if (type.getSqlTypeName() == SqlTypeName.UUID) {
            targetDataType = new SqlDataTypeSpec(
                    new SqlBasicTypeNameSpec(SqlTypeName.UUID, SqlParserPos.ZERO), null, type.isNullable(), SqlParserPos.ZERO
            );
        } else {
            targetDataType = SqlTypeUtil.convertTypeToSpec(type).withNullable(type.isNullable());
        }

        return SqlStdOperatorTable.CAST.createCall(SqlParserPos.ZERO, node, targetDataType);
    }

    /**
     * Validates dynamic parameters passed as function arguments.
     */
    private void validateFunctionOperands(SqlCallBinding binding, List<RelDataType> operandTypes, List<SqlTypeFamily> expectedFamilies) {
        // This method is also called from SqlBinaryOperator's CompositeOperandTypeChecker
        // in case fo binary operators.
        IgniteSqlValidator validator = (IgniteSqlValidator) binding.getValidator();

        for (int i = 0; i < binding.getOperandCount(); i++) {
            SqlNode operand = binding.getCall().operand(i);

            if (validator.isUnspecifiedDynamicParam(operand)) {
                SqlTypeFamily expectedTypeFamily = expectedFamilies.get(i);

                if (expectedTypeFamily.getTypeNames().size() > 1) {
                    String signature = IgniteResource.makeSignature(binding, operandTypes);
                    String allowedSignatures = binding.getOperator().getAllowedSignatures();

                    throw binding.newValidationError(IgniteResource.INSTANCE.ambiguousOperator2(signature, allowedSignatures));
                }
            }
        }
    }

    /**
     * Validates parameters in INSERT/UPDATE.
     */
    private void validateDynamicParametersInModify(@Nullable SqlValidatorScope scope, RelDataType targetRowType, SqlNode query) {
        List<List<SqlNode>> sourceLists;

        if (query instanceof SqlInsert) {
            SqlInsert insert = (SqlInsert) query;

            if (insert.getSource() instanceof SqlSelect) {
                // NOT MATCHED THEN arm of a MERGE statement.
                SqlSelect select = (SqlSelect) insert.getSource();
                sourceLists = List.of(select.getSelectList());
            } else {
                // Basic INSERT INTO ... VALUES (...).
                SqlCall values = (SqlCall) insert.getSource();
                assert values.getKind() == SqlKind.VALUES : "Unexpected source node for INSERT " + values;
                List<List<SqlNode>> rows = new ArrayList<>(values.getOperandList().size());

                for (SqlNode rowNode : values.getOperandList()) {
                    SqlCall row = (SqlCall) rowNode;
                    rows.add(row.getOperandList());
                }

                sourceLists = rows;
            }
        } else if (query instanceof SqlUpdate) {
            SqlUpdate update = (SqlUpdate) query;
            sourceLists = List.of(update.getSourceExpressionList());
        } else {
            throw new AssertionError("Encountered unexpected SQL node during dynamic parameter validation: " + query);
        }

        for (List<SqlNode> sourceList : sourceLists) {
            for (int i = 0; i < sourceList.size(); i++) {
                SqlNode node = sourceList.get(i);

                if (node.getKind() == SqlKind.DYNAMIC_PARAM) {
                    boolean insertOp = query instanceof SqlInsert;

                    SqlDynamicParam dynamicParam = (SqlDynamicParam) node;
                    RelDataType targetType = targetRowType.getFieldList().get(i).getType();
                    IgniteSqlValidator validator1 = (IgniteSqlValidator) scope.getValidator();

                    validateAssignment(dynamicParam, targetType, insertOp, validator1);
                }
            }
        }
    }

    /**
     * Validates dynamic parameters in binary comparison operation.
     */
    private void validateBinaryComparisonCoercion(SqlCallBinding binding, RelDataType leftType,
            RelDataType rightType, IgniteSqlValidator validator) {

        SqlNode lhs = binding.operand(0);
        SqlNode rhs = binding.operand(1);

        boolean lhsUnknown = validator.isUnspecifiedDynamicParam(lhs);
        boolean rhsUnknown = validator.isUnspecifiedDynamicParam(rhs);

        if (lhsUnknown && rhsUnknown) {
            String signature = IgniteResource.makeSignature(binding, leftType, rightType);
            throw binding.newValidationError(IgniteResource.INSTANCE.ambiguousOperator1(signature));
        }

        if (lhs instanceof SqlDynamicParam) {
            if (rhsUnknown) {
                RelDataType nullableType = typeFactory.createTypeWithNullability(rightType, true);
                validator.setValidatedNodeType(binding.operand(0), nullableType);
            } else {
                validateOperand((SqlDynamicParam) lhs, rightType, binding.getOperator(), validator);
            }
        }

        if (rhs instanceof SqlDynamicParam) {
            if (lhsUnknown) {
                RelDataType nullableType = typeFactory.createTypeWithNullability(leftType, true);
                validator.setValidatedNodeType(binding.operand(1), nullableType);
            } else {
                validateOperand((SqlDynamicParam) rhs, leftType, binding.getOperator(), validator);
            }
        }
    }

    // TODO: https://issues.apache.org/jira/browse/IGNITE-19721 - move this check to SqlValidator (if possible).
    private void validateAssignment(SqlDynamicParam node, RelDataType targetType, boolean insertOp, IgniteSqlValidator validator) {
        RelDataType paramType = validator.resolveDynamicParameterType(node, targetType);

        boolean compatible = typeFamiliesAreCompatible(typeFactory, targetType, paramType);

        if (compatible) {
            return;
        }

        if (insertOp) {
            // Throw the same error if T1 and T2 are not compatible:
            //
            // 1) INSERT INTO (t1_col) VALUES (<T2>)
            // 2) INSERT in not match arm of MERGE statement.
            //
            var ex = RESOURCE.incompatibleValueType(SqlStdOperatorTable.VALUES.getName());
            throw SqlUtil.newContextException(node.getParserPosition(), ex);
        } else {
            // Error for UPDATE in both standalone UPDATE and WHEN MATCHED arm of MERGE statements.

            String paramTypeString = paramType.toString();
            String targetTypeString = targetType.toString();

            var ex = IgniteResource.INSTANCE.assignmentRequiresExplicitCast(paramTypeString, targetTypeString);
            throw SqlUtil.newContextException(node.getParserPosition(), ex);
        }
    }

    // TODO: https://issues.apache.org/jira/browse/IGNITE-19721 - move this check to SqlValidator (if possible).
    private void validateOperand(SqlDynamicParam node, RelDataType targetType, SqlOperator operator, IgniteSqlValidator validator) {

        RelDataType paramType = validator.resolveDynamicParameterType(node, targetType);

        boolean compatible = typeFamiliesAreCompatible(typeFactory, targetType, paramType);

        if (compatible) {
            return;
        }

        if (validator.isUnspecified(node)) {
            var ex = IgniteResource.INSTANCE.ambiguousOperator1(operator.getName());
            throw SqlUtil.newContextException(node.getParserPosition(), ex);
        } else {
            var ex = IgniteResource.INSTANCE.operationRequiresExplicitCast(operator.getName());
            throw SqlUtil.newContextException(node.getParserPosition(), ex);
        }
    }
}
