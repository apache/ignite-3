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

package org.apache.ignite.internal.sql.engine.sql.fun;

import com.google.common.collect.ImmutableList;
import java.util.List;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlBasicFunction;
import org.apache.calcite.sql.SqlBinaryOperator;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.fun.SqlAbstractTimeFunction;
import org.apache.calcite.sql.fun.SqlInternalOperators;
import org.apache.calcite.sql.fun.SqlLibraryOperators;
import org.apache.calcite.sql.fun.SqlMinMaxAggFunction;
import org.apache.calcite.sql.fun.SqlMonotonicBinaryOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.fun.SqlSubstringFunction;
import org.apache.calcite.sql.type.InferTypes;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlOperandTypeChecker;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlSingleOperandTypeChecker;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeTransforms;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.sql.util.ReflectiveSqlOperatorTable;
import org.apache.calcite.util.Optionality;
import org.apache.ignite.internal.sql.engine.type.IgniteTypeFactory;
import org.apache.ignite.internal.sql.engine.util.Commons;
import org.apache.ignite.internal.sql.engine.util.TypeUtils;
import org.jetbrains.annotations.Nullable;

/**
 * Operator table that contains only Ignite-specific functions and operators.
 */
public class IgniteSqlOperatorTable extends ReflectiveSqlOperatorTable {
    private static final SqlSingleOperandTypeChecker SAME_SAME =
            new SameFamilyOperandTypeChecker(2);

    private static final SqlOperandTypeChecker DATETIME_MATCHING_INTERVAL =
            new SqlDateTimeIntervalTypeChecker(true);

    private static final SqlOperandTypeChecker MATCHING_INTERVAL_DATETIME =
            new SqlDateTimeIntervalTypeChecker(false);

    private static final SqlOperandTypeChecker PLUS_OPERATOR_TYPES_CHECKER =
            OperandTypes.NUMERIC_NUMERIC.and(SAME_SAME)
                    .or(OperandTypes.INTERVAL_SAME_SAME)
                    .or(DATETIME_MATCHING_INTERVAL)
                    .or(MATCHING_INTERVAL_DATETIME);

    private static final SqlOperandTypeChecker MINUS_OPERATOR_TYPES_CHECKER =
            OperandTypes.NUMERIC_NUMERIC.and(SAME_SAME)
                    .or(OperandTypes.INTERVAL_SAME_SAME)
                    .or(OperandTypes.DATETIME_INTERVAL.and(DATETIME_MATCHING_INTERVAL));

    private static final SqlSingleOperandTypeChecker DIVISION_OPERATOR_TYPES_CHECKER =
            OperandTypes.NUMERIC_NUMERIC.and(SAME_SAME)
                    .or(OperandTypes.INTERVAL_NUMERIC);

    public static final SqlSingleOperandTypeChecker MULTIPLY_OPERATOR_TYPES_CHECKER =
            OperandTypes.NUMERIC_NUMERIC.and(SAME_SAME)
                    .or(OperandTypes.INTERVAL_NUMERIC)
                    .or(OperandTypes.NUMERIC_INTERVAL);

    public static final SqlFunction LENGTH =
            new SqlFunction(
                    "LENGTH",
                    SqlKind.OTHER_FUNCTION,
                    ReturnTypes.INTEGER_NULLABLE,
                    null,
                    OperandTypes.CHARACTER.or(OperandTypes.BINARY),
                    SqlFunctionCategory.NUMERIC);

    public static final SqlFunction SYSTEM_RANGE = new SqlSystemRangeFunction();

    public static final SqlFunction TYPEOF =
            new SqlFunction(
                    "TYPEOF",
                    SqlKind.OTHER_FUNCTION,
                    ReturnTypes.VARCHAR_2000,
                    null,
                    OperandTypes.ANY,
                    SqlFunctionCategory.SYSTEM);

    /**
     * Least of two arguments. Unlike LEAST, which is converted to CASE WHEN THEN END clause, this function
     * is natively implemented.
     *
     * <p>Note: System function, cannot be used by user.
     */
    public static final SqlFunction LEAST2 =
            new SqlFunction(
                    "$LEAST2",
                    SqlKind.OTHER_FUNCTION,
                    ReturnTypes.LEAST_RESTRICTIVE.andThen(SqlTypeTransforms.TO_NULLABLE),
                    null,
                    SAME_SAME,
                    SqlFunctionCategory.SYSTEM);

    /**
     * Greatest of two arguments. Unlike GREATEST, which is converted to CASE WHEN THEN END clause, this function
     * is natively implemented.
     *
     * <p>Note: System function, cannot be used by user.
     */
    public static final SqlFunction GREATEST2 =
            new SqlFunction(
                    "$GREATEST2",
                    SqlKind.OTHER_FUNCTION,
                    ReturnTypes.LEAST_RESTRICTIVE.andThen(SqlTypeTransforms.TO_NULLABLE),
                    null,
                    SAME_SAME,
                    SqlFunctionCategory.SYSTEM);

    /**
     * Generic {@code SUBSTR(string, position [, length]} function.
     * This function works exactly the same as {@link SqlSubstringFunction SUSBSTRING(string, position [, length])}.
     */
    public static final SqlFunction SUBSTR =
            new SqlFunction(
                    "SUBSTR",
                    SqlKind.OTHER_FUNCTION,
                    ReturnTypes.ARG0_NULLABLE_VARYING,
                    null,
                    OperandTypes.STRING_INTEGER.or(OperandTypes.STRING_INTEGER_INTEGER),
                    SqlFunctionCategory.STRING);

    /**
     * The {@code RAND_UUID()} function, which yields a random UUID.
     */
    public static final SqlFunction RAND_UUID =
            new SqlFunction(
                    "RAND_UUID",
                    SqlKind.OTHER_FUNCTION,
                    ReturnTypes.explicit(SqlTypeName.UUID),
                    null,
                    OperandTypes.NILADIC,
                    SqlFunctionCategory.SYSTEM
            ) {
                @Override
                public boolean isDynamicFunction() {
                    return true;
                }

                @Override
                public boolean isDeterministic() {
                    return false;
                }
            };

    /** The {@code ROUND(numeric [, numeric])} function. */
    public static final SqlFunction ROUND = SqlBasicFunction.create("ROUND",
            new SetScaleToZeroIfSingleArgument(),
            OperandTypes.NUMERIC.or(OperandTypes.NUMERIC_INTEGER),
            SqlFunctionCategory.NUMERIC);

    /** The {@code TRUNCATE(numeric [, numeric])} function. */
    public static final SqlFunction TRUNCATE = SqlBasicFunction.create("TRUNCATE",
            new SetScaleToZeroIfSingleArgument(),
            OperandTypes.NUMERIC.or(OperandTypes.NUMERIC_INTEGER),
            SqlFunctionCategory.NUMERIC);

    /** The {@code OCTET_LENGTH(string|binary)} function. */
    public static final SqlFunction OCTET_LENGTH = SqlBasicFunction.create("OCTET_LENGTH",
            ReturnTypes.INTEGER_NULLABLE,
            OperandTypes.CHARACTER.or(OperandTypes.BINARY),
            SqlFunctionCategory.NUMERIC);

    /**
     * Division operator for decimal type. Uses provided values of {@code scale} and {@code precision} to return inferred type.
     */
    public static final SqlFunction DECIMAL_DIVIDE = SqlBasicFunction.create("DECIMAL_DIVIDE",
            new SqlReturnTypeInference() {
                @Override
                public @Nullable RelDataType inferReturnType(SqlOperatorBinding opBinding) {
                    RelDataType arg0 = opBinding.getOperandType(0);
                    Integer precision = opBinding.getOperandLiteralValue(2, Integer.class);
                    Integer scale = opBinding.getOperandLiteralValue(3, Integer.class);

                    assert precision != null : "precision is not specified: " + opBinding.getOperator();
                    assert scale != null : "scale is not specified: " + opBinding.getOperator();

                    boolean nullable = arg0.isNullable();
                    RelDataTypeFactory typeFactory = opBinding.getTypeFactory();

                    RelDataType returnType = typeFactory.createSqlType(SqlTypeName.DECIMAL, precision, scale);
                    return typeFactory.createTypeWithNullability(returnType, nullable);
                }
            },
            OperandTypes.DIVISION_OPERATOR,
            SqlFunctionCategory.NUMERIC);

    /**
     * Logical less-than operator, '{@code <}'.
     */
    public static final SqlBinaryOperator LESS_THAN =
            new SqlBinaryOperator(
                    "<",
                    SqlKind.LESS_THAN,
                    30,
                    true,
                    ReturnTypes.BOOLEAN_NULLABLE,
                    InferTypes.FIRST_KNOWN,
                    SAME_SAME);

    /**
     * Logical less-than-or-equal operator, '{@code <=}'.
     */
    public static final SqlBinaryOperator LESS_THAN_OR_EQUAL =
            new SqlBinaryOperator(
                    "<=",
                    SqlKind.LESS_THAN_OR_EQUAL,
                    30,
                    true,
                    ReturnTypes.BOOLEAN_NULLABLE,
                    InferTypes.FIRST_KNOWN,
                    SAME_SAME);

    /**
     * Logical equals operator, '{@code =}'.
     */
    public static final SqlBinaryOperator EQUALS =
            new SqlBinaryOperator(
                    "=",
                    SqlKind.EQUALS,
                    30,
                    true,
                    ReturnTypes.BOOLEAN_NULLABLE,
                    InferTypes.FIRST_KNOWN,
                    SAME_SAME);

    /**
     * Logical greater-than operator, '{@code >}'.
     */
    public static final SqlBinaryOperator GREATER_THAN =
            new SqlBinaryOperator(
                    ">",
                    SqlKind.GREATER_THAN,
                    30,
                    true,
                    ReturnTypes.BOOLEAN_NULLABLE,
                    InferTypes.FIRST_KNOWN,
                    SAME_SAME);

    /**
     * {@code IS DISTINCT FROM} operator.
     */
    public static final SqlBinaryOperator IS_DISTINCT_FROM =
            new SqlBinaryOperator(
                    "IS DISTINCT FROM",
                    SqlKind.IS_DISTINCT_FROM,
                    30,
                    true,
                    ReturnTypes.BOOLEAN,
                    InferTypes.FIRST_KNOWN,
                    SAME_SAME);

    /**
     * {@code IS NOT DISTINCT FROM} operator. Is equivalent to {@code NOT(x IS DISTINCT FROM y)}.
     */
    public static final SqlBinaryOperator IS_NOT_DISTINCT_FROM =
            new SqlBinaryOperator(
                    "IS NOT DISTINCT FROM",
                    SqlKind.IS_NOT_DISTINCT_FROM,
                    30,
                    true,
                    ReturnTypes.BOOLEAN,
                    InferTypes.FIRST_KNOWN,
                    SAME_SAME);

    /**
     * Logical greater-than-or-equal operator, '{@code >=}'.
     */
    public static final SqlBinaryOperator GREATER_THAN_OR_EQUAL =
            new SqlBinaryOperator(
                    ">=",
                    SqlKind.GREATER_THAN_OR_EQUAL,
                    30,
                    true,
                    ReturnTypes.BOOLEAN_NULLABLE,
                    InferTypes.FIRST_KNOWN,
                    SAME_SAME);

    /**
     * Logical not-equals operator, '{@code <>}'.
     */
    public static final SqlBinaryOperator NOT_EQUALS =
            new SqlBinaryOperator(
                    "<>",
                    SqlKind.NOT_EQUALS,
                    30,
                    true,
                    ReturnTypes.BOOLEAN_NULLABLE,
                    InferTypes.FIRST_KNOWN,
                    SAME_SAME);

    /**
     * Infix arithmetic plus operator, '{@code +}'.
     */
    public static final SqlBinaryOperator PLUS =
            new SqlMonotonicBinaryOperator(
                    "+",
                    SqlKind.PLUS,
                    40,
                    true,
                    new SqlReturnTypeInference() {
                        @Override
                        public @Nullable RelDataType inferReturnType(SqlOperatorBinding opBinding) {
                            RelDataType type1 = opBinding.getOperandType(0);
                            RelDataType type2 = opBinding.getOperandType(1);
                            boolean nullable = type1.isNullable() || type2.isNullable();

                            RelDataTypeFactory typeFactory = opBinding.getTypeFactory();
                            if (TypeUtils.typeFamiliesAreCompatible(typeFactory, type1, type2)) {
                                RelDataType resultType = typeFactory.getTypeSystem()
                                        .deriveDecimalPlusType(typeFactory, type1, type2);

                                if (resultType == null) {
                                    resultType = typeFactory.leastRestrictive(List.of(type1, type2));
                                }

                                if (resultType != null) {
                                    return typeFactory.createTypeWithNullability(resultType, nullable);
                                }
                            } else if (SqlTypeUtil.isDatetime(type1) && SqlTypeUtil.isInterval(type2)) {
                                return deriveDatetimePlusMinusIntervalType(typeFactory, type1, type2, nullable);
                            } else if (SqlTypeUtil.isDatetime(type2) && SqlTypeUtil.isInterval(type1)) {
                                return deriveDatetimePlusMinusIntervalType(typeFactory, type2, type1, nullable);
                            }

                            return null;
                        }
                    },
                    InferTypes.FIRST_KNOWN,
                    PLUS_OPERATOR_TYPES_CHECKER);

    /**
     * Infix arithmetic minus operator, '{@code -}'.
     */
    public static final SqlBinaryOperator MINUS =
            new SqlMonotonicBinaryOperator(
                    "-",
                    SqlKind.MINUS,
                    40,
                    true,
                    new SqlReturnTypeInference() {
                        @Override
                        public @Nullable RelDataType inferReturnType(SqlOperatorBinding opBinding) {
                            RelDataType type1 = opBinding.getOperandType(0);
                            RelDataType type2 = opBinding.getOperandType(1);
                            boolean nullable = type1.isNullable() || type2.isNullable();

                            RelDataTypeFactory typeFactory = opBinding.getTypeFactory();
                            if (TypeUtils.typeFamiliesAreCompatible(typeFactory, type1, type2)) {
                                RelDataType resultType = typeFactory.getTypeSystem()
                                        .deriveDecimalPlusType(typeFactory, type1, type2);

                                if (resultType == null) {
                                    resultType = typeFactory.leastRestrictive(List.of(type1, type2));
                                }

                                if (resultType != null) {
                                    return typeFactory.createTypeWithNullability(resultType, nullable);
                                }
                            } else if (SqlTypeUtil.isDatetime(type1) && SqlTypeUtil.isInterval(type2)) {
                                return deriveDatetimePlusMinusIntervalType(typeFactory, type1, type2, nullable);
                            }

                            return null;
                        }
                    },
                    InferTypes.FIRST_KNOWN,
                    MINUS_OPERATOR_TYPES_CHECKER);

    private static RelDataType deriveDatetimePlusMinusIntervalType(
            RelDataTypeFactory typeFactory,
            RelDataType datetimeType,
            RelDataType intervalType,
            boolean nullable
    ) {
        assert SqlTypeUtil.isDatetime(datetimeType) : "not datetime: " + datetimeType;
        assert SqlTypeUtil.isInterval(intervalType) : "not interval: " + intervalType;

        if (datetimeType.getSqlTypeName().allowsPrecScale(true, false)
                && intervalType.getScale() > datetimeType.getPrecision()) {
            // Using a fraction of a second from an interval as the precision of the expression.
            datetimeType = typeFactory.createSqlType(datetimeType.getSqlTypeName(), intervalType.getScale());
        }

        return typeFactory.createTypeWithNullability(datetimeType, nullable);
    }

    /**
     * Arithmetic division operator, '{@code /}'.
     */
    public static final SqlBinaryOperator DIVIDE =
            new SqlBinaryOperator(
                    "/",
                    SqlKind.DIVIDE,
                    60,
                    true,
                    ReturnTypes.QUOTIENT_NULLABLE,
                    InferTypes.FIRST_KNOWN,
                    DIVISION_OPERATOR_TYPES_CHECKER);

    /**
     * Arithmetic multiplication operator, '{@code *}'.
     */
    public static final SqlBinaryOperator MULTIPLY =
            new SqlMonotonicBinaryOperator(
                    "*",
                    SqlKind.TIMES,
                    60,
                    true,
                    ReturnTypes.PRODUCT_NULLABLE,
                    InferTypes.FIRST_KNOWN,
                    MULTIPLY_OPERATOR_TYPES_CHECKER);

    /**
     * Arithmetic remainder operator, '{@code %}'.
     */
    public static final SqlBinaryOperator PERCENT_REMAINDER =
            new SqlBinaryOperator(
                    "%",
                    SqlKind.MOD,
                    60,
                    true,
                    ReturnTypes.NULLABLE_MOD,
                    null,
                    OperandTypes.EXACT_NUMERIC_EXACT_NUMERIC.and(SAME_SAME));

    /**
     * {@code EVERY} aggregate function.
     */
    public static final SqlAggFunction EVERY =
            new SqlMinMaxAggFunction("EVERY", SqlKind.MIN, OperandTypes.BOOLEAN);

    /**
     * {@code SOME} aggregate function.
     */
    public static final SqlAggFunction SOME =
            new SqlMinMaxAggFunction("SOME", SqlKind.MAX, OperandTypes.BOOLEAN);

    /**
     * The <code>CURRENT_TIMESTAMP [(<i>precision</i>)]</code> function.
     */
    public static final SqlFunction CURRENT_TIMESTAMP =
            new SqlAbstractTimeFunction("CURRENT_TIMESTAMP", SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE) {};

    /**
     * The {@code SAME_VALUE(val) } aggregate function.
     */
    public static final SqlAggFunction SAME_VALUE = new SqlAggFunction("SAME_VALUE", null,
            SqlKind.OTHER_FUNCTION,
            ReturnTypes.ARG0_NULLABLE_IF_EMPTY,
            null, OperandTypes.ANY,
            SqlFunctionCategory.SYSTEM, false,
            false,
            Optionality.FORBIDDEN
    ) {
        @Override
        public boolean allowsFilter() {
            return false;
        }

        @Deprecated
        @Override
        @SuppressWarnings("deprecation")
        public List<RelDataType> getParameterTypes(RelDataTypeFactory typeFactory) {
            throw new UnsupportedOperationException("getParameterTypes should not be called");
        }

        @Deprecated
        @Override
        @SuppressWarnings("deprecation")
        public RelDataType getReturnType(RelDataTypeFactory typeFactory) {
            throw new UnsupportedOperationException("getReturnType should not be called");
        }

        @Override
        public Optionality getDistinctOptionality() {
            return Optionality.IGNORED;
        }

        @Override
        public SqlAggFunction getRollup() {
            return this;
        }
    };

    /** System function. Used to extract prefix from given patter from LIKE operator. */
    public static final SqlFunction FIND_PREFIX =
            new SqlFunction(
                    "$FIND_PREFIX",
                    SqlKind.OTHER_FUNCTION,
                    ReturnTypes.ARG0_NULLABLE,
                    null,
                    OperandTypes.STRING_STRING,
                    SqlFunctionCategory.SYSTEM
            );

    /** System function. Used to compute smallest string at most
     *  of the same length as input which is lexicographically greater than input. */
    public static final SqlFunction NEXT_GREATER_PREFIX =
            new SqlFunction(
                    "$NEXT_GREATER_PREFIX",
                    SqlKind.OTHER_FUNCTION,
                    ReturnTypes.ARG0_NULLABLE,
                    null,
                    OperandTypes.STRING,
                    SqlFunctionCategory.SYSTEM
            );

    /** Singleton instance. */
    public static final IgniteSqlOperatorTable INSTANCE = new IgniteSqlOperatorTable();

    /**
     * Default constructor.
     */
    public IgniteSqlOperatorTable() {
        init0();
    }

    private void init0() {
        ImmutableList.Builder<SqlOperator> definedOperatorsBuilder =
                ImmutableList.builder();
        // Set operators.
        definedOperatorsBuilder.add(SqlStdOperatorTable.UNION);
        definedOperatorsBuilder.add(SqlStdOperatorTable.UNION_ALL);
        definedOperatorsBuilder.add(SqlStdOperatorTable.EXCEPT);
        definedOperatorsBuilder.add(SqlStdOperatorTable.EXCEPT_ALL);
        definedOperatorsBuilder.add(SqlStdOperatorTable.INTERSECT);
        definedOperatorsBuilder.add(SqlStdOperatorTable.INTERSECT_ALL);

        // Logical.
        definedOperatorsBuilder.add(SqlStdOperatorTable.AND);
        definedOperatorsBuilder.add(SqlStdOperatorTable.OR);
        definedOperatorsBuilder.add(SqlStdOperatorTable.NOT);

        // Comparisons.
        definedOperatorsBuilder.add(LESS_THAN);
        definedOperatorsBuilder.add(LESS_THAN_OR_EQUAL);
        definedOperatorsBuilder.add(GREATER_THAN);
        definedOperatorsBuilder.add(GREATER_THAN_OR_EQUAL);
        definedOperatorsBuilder.add(EQUALS);
        definedOperatorsBuilder.add(NOT_EQUALS);
        definedOperatorsBuilder.add(SqlStdOperatorTable.BETWEEN);
        definedOperatorsBuilder.add(SqlStdOperatorTable.NOT_BETWEEN);

        // Arithmetic.
        definedOperatorsBuilder.add(PLUS);
        definedOperatorsBuilder.add(MINUS);
        definedOperatorsBuilder.add(MULTIPLY);
        definedOperatorsBuilder.add(DIVIDE);
        definedOperatorsBuilder.add(SqlStdOperatorTable.DIVIDE_INTEGER); // Used internally.
        definedOperatorsBuilder.add(PERCENT_REMAINDER);
        definedOperatorsBuilder.add(SqlStdOperatorTable.UNARY_MINUS);
        definedOperatorsBuilder.add(SqlStdOperatorTable.UNARY_PLUS);

        // Aggregates.
        definedOperatorsBuilder.add(SqlStdOperatorTable.COUNT);
        definedOperatorsBuilder.add(SqlStdOperatorTable.SUM);
        definedOperatorsBuilder.add(SqlStdOperatorTable.SUM0);
        definedOperatorsBuilder.add(SqlStdOperatorTable.AVG);
        definedOperatorsBuilder.add(DECIMAL_DIVIDE);
        definedOperatorsBuilder.add(SqlStdOperatorTable.MIN);
        definedOperatorsBuilder.add(SqlStdOperatorTable.MAX);
        definedOperatorsBuilder.add(SqlStdOperatorTable.ANY_VALUE);
        definedOperatorsBuilder.add(SqlStdOperatorTable.SINGLE_VALUE);
        definedOperatorsBuilder.add(SqlStdOperatorTable.FILTER);
        definedOperatorsBuilder.add(SqlStdOperatorTable.GROUPING);

        definedOperatorsBuilder.add(EVERY);
        definedOperatorsBuilder.add(SOME);
        definedOperatorsBuilder.add(SAME_VALUE);

        // IS ... operator.
        definedOperatorsBuilder.add(SqlStdOperatorTable.IS_NULL);
        definedOperatorsBuilder.add(SqlStdOperatorTable.IS_NOT_NULL);
        definedOperatorsBuilder.add(SqlStdOperatorTable.IS_TRUE);
        definedOperatorsBuilder.add(SqlStdOperatorTable.IS_NOT_TRUE);
        definedOperatorsBuilder.add(SqlStdOperatorTable.IS_FALSE);
        definedOperatorsBuilder.add(SqlStdOperatorTable.IS_NOT_FALSE);
        definedOperatorsBuilder.add(IS_DISTINCT_FROM);
        definedOperatorsBuilder.add(IS_NOT_DISTINCT_FROM);

        // LIKE and SIMILAR.
        definedOperatorsBuilder.add(SqlStdOperatorTable.LIKE);
        definedOperatorsBuilder.add(SqlStdOperatorTable.NOT_LIKE);
        definedOperatorsBuilder.add(SqlStdOperatorTable.SIMILAR_TO);
        definedOperatorsBuilder.add(SqlStdOperatorTable.NOT_SIMILAR_TO);

        // NULLS ordering.
        definedOperatorsBuilder.add(SqlStdOperatorTable.NULLS_FIRST);
        definedOperatorsBuilder.add(SqlStdOperatorTable.NULLS_LAST);
        definedOperatorsBuilder.add(SqlStdOperatorTable.DESC);

        // Exists.
        definedOperatorsBuilder.add(SqlStdOperatorTable.EXISTS);

        // String functions.
        definedOperatorsBuilder.add(SqlStdOperatorTable.UPPER);
        definedOperatorsBuilder.add(SqlStdOperatorTable.LOWER);
        definedOperatorsBuilder.add(SqlStdOperatorTable.INITCAP);
        definedOperatorsBuilder.add(SqlLibraryOperators.TO_BASE64);
        definedOperatorsBuilder.add(SqlLibraryOperators.FROM_BASE64);
        definedOperatorsBuilder.add(SqlLibraryOperators.MD5);
        definedOperatorsBuilder.add(SqlLibraryOperators.SHA1);
        definedOperatorsBuilder.add(SqlStdOperatorTable.SUBSTRING);
        definedOperatorsBuilder.add(SqlLibraryOperators.LEFT);
        definedOperatorsBuilder.add(SqlLibraryOperators.RIGHT);
        definedOperatorsBuilder.add(SqlStdOperatorTable.REPLACE);
        definedOperatorsBuilder.add(SqlLibraryOperators.TRANSLATE3);
        definedOperatorsBuilder.add(SqlLibraryOperators.CHR);
        definedOperatorsBuilder.add(SqlStdOperatorTable.CHAR_LENGTH);
        definedOperatorsBuilder.add(SqlStdOperatorTable.CHARACTER_LENGTH);
        definedOperatorsBuilder.add(SqlStdOperatorTable.CONCAT);
        definedOperatorsBuilder.add(SqlLibraryOperators.CONCAT_FUNCTION);
        definedOperatorsBuilder.add(SqlStdOperatorTable.OVERLAY);
        definedOperatorsBuilder.add(SqlStdOperatorTable.POSITION);
        definedOperatorsBuilder.add(SqlStdOperatorTable.ASCII);
        definedOperatorsBuilder.add(SqlLibraryOperators.REPEAT);
        definedOperatorsBuilder.add(SqlLibraryOperators.SPACE);
        definedOperatorsBuilder.add(SqlLibraryOperators.STRCMP);
        definedOperatorsBuilder.add(SqlLibraryOperators.SOUNDEX);
        definedOperatorsBuilder.add(SqlLibraryOperators.DIFFERENCE);
        definedOperatorsBuilder.add(SqlLibraryOperators.REVERSE);
        definedOperatorsBuilder.add(SqlStdOperatorTable.TRIM);
        definedOperatorsBuilder.add(SqlLibraryOperators.LTRIM);
        definedOperatorsBuilder.add(SqlLibraryOperators.RTRIM);
        definedOperatorsBuilder.add(SUBSTR);

        // Math functions.
        definedOperatorsBuilder.add(SqlStdOperatorTable.MOD); // Arithmetic remainder.
        definedOperatorsBuilder.add(SqlStdOperatorTable.EXP); // Euler's number e raised to the power of a value.
        definedOperatorsBuilder.add(SqlStdOperatorTable.POWER);
        definedOperatorsBuilder.add(SqlStdOperatorTable.LN); // Natural logarithm.
        definedOperatorsBuilder.add(SqlStdOperatorTable.LOG10); // The base 10 logarithm.
        definedOperatorsBuilder.add(SqlStdOperatorTable.ABS); // Absolute value.
        definedOperatorsBuilder.add(SqlStdOperatorTable.RAND); // Random.
        definedOperatorsBuilder.add(SqlStdOperatorTable.RAND_INTEGER); // Integer random.
        definedOperatorsBuilder.add(SqlStdOperatorTable.ACOS); // Arc cosine.
        definedOperatorsBuilder.add(SqlStdOperatorTable.ASIN); // Arc sine.
        definedOperatorsBuilder.add(SqlStdOperatorTable.ATAN); // Arc tangent.
        definedOperatorsBuilder.add(SqlStdOperatorTable.ATAN2); // Angle from coordinates.
        definedOperatorsBuilder.add(SqlStdOperatorTable.SQRT); // Square root.
        definedOperatorsBuilder.add(SqlStdOperatorTable.CBRT); // Cube root.
        definedOperatorsBuilder.add(SqlStdOperatorTable.COS); // Cosine
        definedOperatorsBuilder.add(SqlLibraryOperators.COSH); // Hyperbolic cosine.
        definedOperatorsBuilder.add(SqlStdOperatorTable.COT); // Cotangent.
        definedOperatorsBuilder.add(SqlStdOperatorTable.DEGREES); // Radians to degrees.
        definedOperatorsBuilder.add(SqlStdOperatorTable.RADIANS); // Degrees to radians.
        definedOperatorsBuilder.add(ROUND); // Fixes return type scale.
        definedOperatorsBuilder.add(SqlStdOperatorTable.SIGN);
        definedOperatorsBuilder.add(SqlStdOperatorTable.SIN); // Sine.
        definedOperatorsBuilder.add(SqlLibraryOperators.SINH); // Hyperbolic sine.
        definedOperatorsBuilder.add(SqlStdOperatorTable.TAN); // Tangent.
        definedOperatorsBuilder.add(SqlLibraryOperators.TANH); // Hyperbolic tangent.
        definedOperatorsBuilder.add(TRUNCATE); // Fixes return type scale.
        definedOperatorsBuilder.add(SqlStdOperatorTable.PI);

        // Date and time.
        definedOperatorsBuilder.add(SqlStdOperatorTable.DATETIME_PLUS);
        definedOperatorsBuilder.add(SqlStdOperatorTable.MINUS_DATE);
        definedOperatorsBuilder.add(SqlStdOperatorTable.EXTRACT);
        definedOperatorsBuilder.add(SqlStdOperatorTable.FLOOR);
        definedOperatorsBuilder.add(SqlStdOperatorTable.CEIL);
        definedOperatorsBuilder.add(SqlStdOperatorTable.TIMESTAMP_ADD);
        definedOperatorsBuilder.add(SqlStdOperatorTable.TIMESTAMP_DIFF);
        definedOperatorsBuilder.add(SqlStdOperatorTable.LAST_DAY);
        definedOperatorsBuilder.add(SqlLibraryOperators.DAYNAME);
        definedOperatorsBuilder.add(SqlLibraryOperators.MONTHNAME);
        definedOperatorsBuilder.add(SqlStdOperatorTable.DAYOFMONTH);
        definedOperatorsBuilder.add(SqlStdOperatorTable.DAYOFWEEK);
        definedOperatorsBuilder.add(SqlStdOperatorTable.DAYOFYEAR);
        definedOperatorsBuilder.add(SqlStdOperatorTable.YEAR);
        definedOperatorsBuilder.add(SqlStdOperatorTable.QUARTER);
        definedOperatorsBuilder.add(SqlStdOperatorTable.MONTH);
        definedOperatorsBuilder.add(SqlStdOperatorTable.WEEK);
        definedOperatorsBuilder.add(SqlStdOperatorTable.HOUR);
        definedOperatorsBuilder.add(SqlStdOperatorTable.MINUTE);
        definedOperatorsBuilder.add(SqlStdOperatorTable.SECOND);
        definedOperatorsBuilder.add(SqlLibraryOperators.TIMESTAMP_SECONDS); // Seconds since 1970-01-01 to timestamp.
        definedOperatorsBuilder.add(SqlLibraryOperators.TIMESTAMP_MILLIS); // Milliseconds since 1970-01-01 to timestamp.
        definedOperatorsBuilder.add(SqlLibraryOperators.TIMESTAMP_MICROS); // Microseconds since 1970-01-01 to timestamp.
        definedOperatorsBuilder.add(SqlLibraryOperators.UNIX_SECONDS); // Timestamp to seconds since 1970-01-01.
        definedOperatorsBuilder.add(SqlLibraryOperators.UNIX_MILLIS); // Timestamp to milliseconds since 1970-01-01.
        definedOperatorsBuilder.add(SqlLibraryOperators.UNIX_MICROS); // Timestamp to microseconds since 1970-01-01.
        definedOperatorsBuilder.add(SqlLibraryOperators.UNIX_DATE); // Date to days since 1970-01-01.
        definedOperatorsBuilder.add(SqlLibraryOperators.DATE_FROM_UNIX_DATE); // Days since 1970-01-01 to date.
        definedOperatorsBuilder.add(SqlLibraryOperators.DATE); // String to date.

        // POSIX REGEX.
        definedOperatorsBuilder.add(SqlStdOperatorTable.POSIX_REGEX_CASE_INSENSITIVE);
        definedOperatorsBuilder.add(SqlStdOperatorTable.POSIX_REGEX_CASE_SENSITIVE);
        definedOperatorsBuilder.add(SqlStdOperatorTable.NEGATED_POSIX_REGEX_CASE_INSENSITIVE);
        definedOperatorsBuilder.add(SqlStdOperatorTable.NEGATED_POSIX_REGEX_CASE_SENSITIVE);
        definedOperatorsBuilder.add(SqlLibraryOperators.REGEXP_REPLACE_2);
        definedOperatorsBuilder.add(SqlLibraryOperators.REGEXP_REPLACE_3);
        definedOperatorsBuilder.add(SqlLibraryOperators.REGEXP_REPLACE_4);
        definedOperatorsBuilder.add(SqlLibraryOperators.REGEXP_REPLACE_5);
        definedOperatorsBuilder.add(SqlLibraryOperators.REGEXP_REPLACE_6);

        // Collections.
        definedOperatorsBuilder.add(SqlStdOperatorTable.MAP_VALUE_CONSTRUCTOR);
        definedOperatorsBuilder.add(SqlStdOperatorTable.ARRAY_VALUE_CONSTRUCTOR);
        definedOperatorsBuilder.add(SqlStdOperatorTable.ITEM);
        definedOperatorsBuilder.add(SqlStdOperatorTable.CARDINALITY);
        definedOperatorsBuilder.add(SqlStdOperatorTable.IS_EMPTY);
        definedOperatorsBuilder.add(SqlStdOperatorTable.IS_NOT_EMPTY);

        // TODO https://issues.apache.org/jira/browse/IGNITE-19332
        // definedOperatorsBuilder.add(SqlStdOperatorTable.MAP_QUERY);
        // definedOperatorsBuilder.add(SqlStdOperatorTable.ARRAY_QUERY);

        // Multiset.
        // TODO https://issues.apache.org/jira/browse/IGNITE-15551
        // definedOperatorsBuilder.add(SqlStdOperatorTable.MULTISET_VALUE);
        // definedOperatorsBuilder.add(SqlStdOperatorTable.MULTISET_QUERY);
        // definedOperatorsBuilder.add(SqlStdOperatorTable.SLICE);
        // definedOperatorsBuilder.add(SqlStdOperatorTable.ELEMENT);
        // definedOperatorsBuilder.add(SqlStdOperatorTable.STRUCT_ACCESS);
        // definedOperatorsBuilder.add(SqlStdOperatorTable.MEMBER_OF);
        // definedOperatorsBuilder.add(SqlStdOperatorTable.IS_A_SET);
        // definedOperatorsBuilder.add(SqlStdOperatorTable.IS_NOT_A_SET);
        // definedOperatorsBuilder.add(SqlStdOperatorTable.MULTISET_INTERSECT_DISTINCT);
        // definedOperatorsBuilder.add(SqlStdOperatorTable.MULTISET_INTERSECT);
        // definedOperatorsBuilder.add(SqlStdOperatorTable.MULTISET_EXCEPT_DISTINCT);
        // definedOperatorsBuilder.add(SqlStdOperatorTable.MULTISET_EXCEPT);
        // definedOperatorsBuilder.add(SqlStdOperatorTable.MULTISET_UNION_DISTINCT);
        // definedOperatorsBuilder.add(SqlStdOperatorTable.MULTISET_UNION);
        // definedOperatorsBuilder.add(SqlStdOperatorTable.SUBMULTISET_OF);
        // definedOperatorsBuilder.add(SqlStdOperatorTable.NOT_SUBMULTISET_OF);

        // Other functions and operators.
        definedOperatorsBuilder.add(SqlStdOperatorTable.ROW);
        definedOperatorsBuilder.add(SqlStdOperatorTable.CAST);
        definedOperatorsBuilder.add(SqlLibraryOperators.INFIX_CAST);
        definedOperatorsBuilder.add(SqlStdOperatorTable.COALESCE);
        definedOperatorsBuilder.add(SqlLibraryOperators.NVL);
        definedOperatorsBuilder.add(SqlStdOperatorTable.NULLIF);
        definedOperatorsBuilder.add(SqlStdOperatorTable.CASE);
        definedOperatorsBuilder.add(SqlLibraryOperators.DECODE);
        definedOperatorsBuilder.add(SqlLibraryOperators.LEAST);
        definedOperatorsBuilder.add(SqlLibraryOperators.GREATEST);
        definedOperatorsBuilder.add(SqlLibraryOperators.COMPRESS);
        definedOperatorsBuilder.add(OCTET_LENGTH);
        definedOperatorsBuilder.add(SqlStdOperatorTable.DEFAULT);
        definedOperatorsBuilder.add(SqlStdOperatorTable.REINTERPRET);

        // XML Operators.
        definedOperatorsBuilder.add(SqlLibraryOperators.EXTRACT_VALUE);
        definedOperatorsBuilder.add(SqlLibraryOperators.XML_TRANSFORM);
        definedOperatorsBuilder.add(SqlLibraryOperators.EXTRACT_XML);
        definedOperatorsBuilder.add(SqlLibraryOperators.EXISTS_NODE);

        // JSON Operators
        definedOperatorsBuilder.add(SqlStdOperatorTable.JSON_TYPE_OPERATOR);
        definedOperatorsBuilder.add(SqlStdOperatorTable.JSON_VALUE_EXPRESSION);
        definedOperatorsBuilder.add(SqlStdOperatorTable.JSON_VALUE);
        definedOperatorsBuilder.add(SqlStdOperatorTable.JSON_QUERY);
        definedOperatorsBuilder.add(SqlLibraryOperators.JSON_TYPE);
        definedOperatorsBuilder.add(SqlStdOperatorTable.JSON_EXISTS);
        definedOperatorsBuilder.add(SqlLibraryOperators.JSON_DEPTH);
        definedOperatorsBuilder.add(SqlLibraryOperators.JSON_KEYS);
        definedOperatorsBuilder.add(SqlLibraryOperators.JSON_PRETTY);
        definedOperatorsBuilder.add(SqlLibraryOperators.JSON_LENGTH);
        definedOperatorsBuilder.add(SqlLibraryOperators.JSON_REMOVE);
        definedOperatorsBuilder.add(SqlLibraryOperators.JSON_STORAGE_SIZE);
        definedOperatorsBuilder.add(SqlStdOperatorTable.JSON_OBJECT);
        definedOperatorsBuilder.add(SqlStdOperatorTable.JSON_ARRAY);
        definedOperatorsBuilder.add(SqlStdOperatorTable.IS_JSON_VALUE);
        definedOperatorsBuilder.add(SqlStdOperatorTable.IS_JSON_OBJECT);
        definedOperatorsBuilder.add(SqlStdOperatorTable.IS_JSON_ARRAY);
        definedOperatorsBuilder.add(SqlStdOperatorTable.IS_JSON_SCALAR);
        definedOperatorsBuilder.add(SqlStdOperatorTable.IS_NOT_JSON_VALUE);
        definedOperatorsBuilder.add(SqlStdOperatorTable.IS_NOT_JSON_OBJECT);
        definedOperatorsBuilder.add(SqlStdOperatorTable.IS_NOT_JSON_ARRAY);
        definedOperatorsBuilder.add(SqlStdOperatorTable.IS_NOT_JSON_SCALAR);

        // Aggregate functions.
        definedOperatorsBuilder.add(SqlInternalOperators.LITERAL_AGG);

        // Current time functions.
        definedOperatorsBuilder.add(CURRENT_TIMESTAMP);
        definedOperatorsBuilder.add(SqlStdOperatorTable.CURRENT_DATE);
        definedOperatorsBuilder.add(SqlStdOperatorTable.LOCALTIME);
        definedOperatorsBuilder.add(SqlStdOperatorTable.LOCALTIMESTAMP);

        // Context variable functions
        definedOperatorsBuilder.add(SqlStdOperatorTable.CURRENT_USER);

        // Ignite specific operators
        definedOperatorsBuilder.add(LENGTH);
        definedOperatorsBuilder.add(SYSTEM_RANGE);
        definedOperatorsBuilder.add(TYPEOF);
        definedOperatorsBuilder.add(LEAST2);
        definedOperatorsBuilder.add(GREATEST2);
        definedOperatorsBuilder.add(RAND_UUID);
        definedOperatorsBuilder.add(FIND_PREFIX);
        definedOperatorsBuilder.add(NEXT_GREATER_PREFIX);

        setOperators(buildIndex(definedOperatorsBuilder.build()));
    }

    /** Sets scale to {@code 0} for single argument variants of ROUND/TRUNCATE operators. */
    private static class SetScaleToZeroIfSingleArgument implements SqlReturnTypeInference {
        @Override
        public @Nullable RelDataType inferReturnType(SqlOperatorBinding opBinding) {
            RelDataType operandType = opBinding.getOperandType(0);

            // If there is only one argument and it supports precision and scale, set scale 0.
            if (opBinding.getOperandCount() == 1 && operandType.getSqlTypeName().allowsPrecScale(true, true)) {
                int precision = operandType.getPrecision();
                IgniteTypeFactory typeFactory = Commons.typeFactory();

                RelDataType returnType = typeFactory.createSqlType(operandType.getSqlTypeName(), precision, 0);
                // Preserve nullability
                boolean nullable = operandType.isNullable();

                return typeFactory.createTypeWithNullability(returnType, nullable);
            } else {
                return operandType;
            }
        }
    }
}
