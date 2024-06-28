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

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlBasicFunction;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.fun.SqlInternalOperators;
import org.apache.calcite.sql.fun.SqlLibraryOperators;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.fun.SqlSubstringFunction;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeTransforms;
import org.apache.calcite.sql.util.ReflectiveSqlOperatorTable;
import org.apache.ignite.internal.sql.engine.type.IgniteTypeFactory;
import org.apache.ignite.internal.sql.engine.type.UuidType;
import org.apache.ignite.internal.sql.engine.util.Commons;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Operator table that contains only Ignite-specific functions and operators.
 */
public class IgniteSqlOperatorTable extends ReflectiveSqlOperatorTable {
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
                    OperandTypes.SAME_SAME,
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
                    OperandTypes.SAME_SAME,
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
                    OperandTypes.STRING_INTEGER_OPTIONAL_INTEGER,
                    SqlFunctionCategory.STRING);

    /**
     * The {@code RAND_UUID()} function, which yields a random UUID.
     */
    public static final SqlFunction RAND_UUID =
            new SqlFunction(
                    "RAND_UUID",
                    SqlKind.OTHER_FUNCTION,
                    ReturnTypes.explicit(new UuidType(false)),
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
            OperandTypes.NUMERIC_OPTIONAL_INTEGER,
            SqlFunctionCategory.NUMERIC);

    /** The {@code TRUNCATE(numeric [, numeric])} function. */
    public static final SqlFunction TRUNCATE = SqlBasicFunction.create("TRUNCATE",
            new SetScaleToZeroIfSingleArgument(),
            OperandTypes.NUMERIC_OPTIONAL_INTEGER,
            SqlFunctionCategory.NUMERIC);

    /** The {@code OCTET_LENGTH(string|binary)} function. */
    public static final SqlFunction OCTET_LENGTH = SqlBasicFunction.create("OCTET_LENGTH",
            ReturnTypes.INTEGER_NULLABLE,
            OperandTypes.CHARACTER.or(OperandTypes.BINARY),
            SqlFunctionCategory.NUMERIC);

    /**
     * Division operator used by REDUCE phase of AVG aggregate.
     * Uses provided values of {@code scale} and {@code precision} to return inferred type.
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

    /** Singleton instance. */
    public static final IgniteSqlOperatorTable INSTANCE = new IgniteSqlOperatorTable();

    /**
     * Default constructor.
     */
    public IgniteSqlOperatorTable() {
        // Set operators.
        register(SqlStdOperatorTable.UNION);
        register(SqlStdOperatorTable.UNION_ALL);
        register(SqlStdOperatorTable.EXCEPT);
        register(SqlStdOperatorTable.EXCEPT_ALL);
        register(SqlStdOperatorTable.INTERSECT);
        register(SqlStdOperatorTable.INTERSECT_ALL);

        // Logical.
        register(SqlStdOperatorTable.AND);
        register(SqlStdOperatorTable.OR);
        register(SqlStdOperatorTable.NOT);

        // Comparisons.
        register(SqlStdOperatorTable.LESS_THAN);
        register(SqlStdOperatorTable.LESS_THAN_OR_EQUAL);
        register(SqlStdOperatorTable.GREATER_THAN);
        register(SqlStdOperatorTable.GREATER_THAN_OR_EQUAL);
        register(SqlStdOperatorTable.EQUALS);
        register(SqlStdOperatorTable.NOT_EQUALS);
        register(SqlStdOperatorTable.BETWEEN);
        register(SqlStdOperatorTable.NOT_BETWEEN);

        // Arithmetic.
        register(SqlStdOperatorTable.PLUS);
        register(SqlStdOperatorTable.MINUS);
        register(SqlStdOperatorTable.MULTIPLY);
        register(SqlStdOperatorTable.DIVIDE);
        register(SqlStdOperatorTable.DIVIDE_INTEGER); // Used internally.
        register(SqlStdOperatorTable.PERCENT_REMAINDER);
        register(SqlStdOperatorTable.UNARY_MINUS);
        register(SqlStdOperatorTable.UNARY_PLUS);

        // Aggregates.
        register(SqlStdOperatorTable.COUNT);
        register(SqlStdOperatorTable.SUM);
        register(SqlStdOperatorTable.SUM0);
        register(SqlStdOperatorTable.AVG);
        register(DECIMAL_DIVIDE);
        register(SqlStdOperatorTable.MIN);
        register(SqlStdOperatorTable.MAX);
        register(SqlStdOperatorTable.ANY_VALUE);
        register(SqlStdOperatorTable.SINGLE_VALUE);
        register(SqlStdOperatorTable.FILTER);

        register(SqlStdOperatorTable.EVERY);
        register(SqlStdOperatorTable.SOME);

        // IS ... operator.
        register(SqlStdOperatorTable.IS_NULL);
        register(SqlStdOperatorTable.IS_NOT_NULL);
        register(SqlStdOperatorTable.IS_TRUE);
        register(SqlStdOperatorTable.IS_NOT_TRUE);
        register(SqlStdOperatorTable.IS_FALSE);
        register(SqlStdOperatorTable.IS_NOT_FALSE);
        register(SqlStdOperatorTable.IS_DISTINCT_FROM);
        register(SqlStdOperatorTable.IS_NOT_DISTINCT_FROM);

        // LIKE and SIMILAR.
        register(SqlStdOperatorTable.LIKE);
        register(SqlStdOperatorTable.NOT_LIKE);
        register(SqlStdOperatorTable.SIMILAR_TO);
        register(SqlStdOperatorTable.NOT_SIMILAR_TO);

        // NULLS ordering.
        register(SqlStdOperatorTable.NULLS_FIRST);
        register(SqlStdOperatorTable.NULLS_LAST);
        register(SqlStdOperatorTable.DESC);

        // Exists.
        register(SqlStdOperatorTable.EXISTS);

        // String functions.
        register(SqlStdOperatorTable.UPPER);
        register(SqlStdOperatorTable.LOWER);
        register(SqlStdOperatorTable.INITCAP);
        register(SqlLibraryOperators.TO_BASE64);
        register(SqlLibraryOperators.FROM_BASE64);
        register(SqlLibraryOperators.MD5);
        register(SqlLibraryOperators.SHA1);
        register(SqlStdOperatorTable.SUBSTRING);
        register(SqlLibraryOperators.LEFT);
        register(SqlLibraryOperators.RIGHT);
        register(SqlStdOperatorTable.REPLACE);
        register(SqlLibraryOperators.TRANSLATE3);
        register(SqlLibraryOperators.CHR);
        register(SqlStdOperatorTable.CHAR_LENGTH);
        register(SqlStdOperatorTable.CHARACTER_LENGTH);
        register(SqlStdOperatorTable.CONCAT);
        register(SqlLibraryOperators.CONCAT_FUNCTION);
        register(SqlStdOperatorTable.OVERLAY);
        register(SqlStdOperatorTable.POSITION);
        register(SqlStdOperatorTable.ASCII);
        register(SqlLibraryOperators.REPEAT);
        register(SqlLibraryOperators.SPACE);
        register(SqlLibraryOperators.STRCMP);
        register(SqlLibraryOperators.SOUNDEX);
        register(SqlLibraryOperators.DIFFERENCE);
        register(SqlLibraryOperators.REVERSE);
        register(SqlStdOperatorTable.TRIM);
        register(SqlLibraryOperators.LTRIM);
        register(SqlLibraryOperators.RTRIM);
        register(SUBSTR);

        // Math functions.
        register(SqlStdOperatorTable.MOD); // Arithmetic remainder.
        register(SqlStdOperatorTable.EXP); // Euler's number e raised to the power of a value.
        register(SqlStdOperatorTable.POWER);
        register(SqlStdOperatorTable.LN); // Natural logarithm.
        register(SqlStdOperatorTable.LOG10); // The base 10 logarithm.
        register(SqlStdOperatorTable.ABS); // Absolute value.
        register(SqlStdOperatorTable.RAND); // Random.
        register(SqlStdOperatorTable.RAND_INTEGER); // Integer random.
        register(SqlStdOperatorTable.ACOS); // Arc cosine.
        register(SqlStdOperatorTable.ASIN); // Arc sine.
        register(SqlStdOperatorTable.ATAN); // Arc tangent.
        register(SqlStdOperatorTable.ATAN2); // Angle from coordinates.
        register(SqlStdOperatorTable.SQRT); // Square root.
        register(SqlStdOperatorTable.CBRT); // Cube root.
        register(SqlStdOperatorTable.COS); // Cosine
        register(SqlLibraryOperators.COSH); // Hyperbolic cosine.
        register(SqlStdOperatorTable.COT); // Cotangent.
        register(SqlStdOperatorTable.DEGREES); // Radians to degrees.
        register(SqlStdOperatorTable.RADIANS); // Degrees to radians.
        register(ROUND); // Fixes return type scale.
        register(SqlStdOperatorTable.SIGN);
        register(SqlStdOperatorTable.SIN); // Sine.
        register(SqlLibraryOperators.SINH); // Hyperbolic sine.
        register(SqlStdOperatorTable.TAN); // Tangent.
        register(SqlLibraryOperators.TANH); // Hyperbolic tangent.
        register(TRUNCATE); // Fixes return type scale.
        register(SqlStdOperatorTable.PI);

        // Date and time.
        register(SqlStdOperatorTable.DATETIME_PLUS);
        register(SqlStdOperatorTable.MINUS_DATE);
        register(SqlStdOperatorTable.EXTRACT);
        register(SqlStdOperatorTable.FLOOR);
        register(SqlStdOperatorTable.CEIL);
        register(SqlStdOperatorTable.TIMESTAMP_ADD);
        register(SqlStdOperatorTable.TIMESTAMP_DIFF);
        register(SqlStdOperatorTable.LAST_DAY);
        register(SqlLibraryOperators.DAYNAME);
        register(SqlLibraryOperators.MONTHNAME);
        register(SqlStdOperatorTable.DAYOFMONTH);
        register(SqlStdOperatorTable.DAYOFWEEK);
        register(SqlStdOperatorTable.DAYOFYEAR);
        register(SqlStdOperatorTable.YEAR);
        register(SqlStdOperatorTable.QUARTER);
        register(SqlStdOperatorTable.MONTH);
        register(SqlStdOperatorTable.WEEK);
        register(SqlStdOperatorTable.HOUR);
        register(SqlStdOperatorTable.MINUTE);
        register(SqlStdOperatorTable.SECOND);
        register(SqlLibraryOperators.TIMESTAMP_SECONDS); // Seconds since 1970-01-01 to timestamp.
        register(SqlLibraryOperators.TIMESTAMP_MILLIS); // Milliseconds since 1970-01-01 to timestamp.
        register(SqlLibraryOperators.TIMESTAMP_MICROS); // Microseconds since 1970-01-01 to timestamp.
        register(SqlLibraryOperators.UNIX_SECONDS); // Timestamp to seconds since 1970-01-01.
        register(SqlLibraryOperators.UNIX_MILLIS); // Timestamp to milliseconds since 1970-01-01.
        register(SqlLibraryOperators.UNIX_MICROS); // Timestamp to microseconds since 1970-01-01.
        register(SqlLibraryOperators.UNIX_DATE); // Date to days since 1970-01-01.
        register(SqlLibraryOperators.DATE_FROM_UNIX_DATE); // Days since 1970-01-01 to date.
        register(SqlLibraryOperators.DATE); // String to date.

        // POSIX REGEX.
        register(SqlStdOperatorTable.POSIX_REGEX_CASE_INSENSITIVE);
        register(SqlStdOperatorTable.POSIX_REGEX_CASE_SENSITIVE);
        register(SqlStdOperatorTable.NEGATED_POSIX_REGEX_CASE_INSENSITIVE);
        register(SqlStdOperatorTable.NEGATED_POSIX_REGEX_CASE_SENSITIVE);
        register(SqlLibraryOperators.REGEXP_REPLACE);

        // Collections.
        register(SqlStdOperatorTable.MAP_VALUE_CONSTRUCTOR);
        register(SqlStdOperatorTable.ARRAY_VALUE_CONSTRUCTOR);
        register(SqlStdOperatorTable.ITEM);
        register(SqlStdOperatorTable.CARDINALITY);
        register(SqlStdOperatorTable.IS_EMPTY);
        register(SqlStdOperatorTable.IS_NOT_EMPTY);

        // TODO https://issues.apache.org/jira/browse/IGNITE-19332
        // register(SqlStdOperatorTable.MAP_QUERY);
        // register(SqlStdOperatorTable.ARRAY_QUERY);

        // Multiset.
        // TODO https://issues.apache.org/jira/browse/IGNITE-15551
        // register(SqlStdOperatorTable.MULTISET_VALUE);
        // register(SqlStdOperatorTable.MULTISET_QUERY);
        // register(SqlStdOperatorTable.SLICE);
        // register(SqlStdOperatorTable.ELEMENT);
        // register(SqlStdOperatorTable.STRUCT_ACCESS);
        // register(SqlStdOperatorTable.MEMBER_OF);
        // register(SqlStdOperatorTable.IS_A_SET);
        // register(SqlStdOperatorTable.IS_NOT_A_SET);
        // register(SqlStdOperatorTable.MULTISET_INTERSECT_DISTINCT);
        // register(SqlStdOperatorTable.MULTISET_INTERSECT);
        // register(SqlStdOperatorTable.MULTISET_EXCEPT_DISTINCT);
        // register(SqlStdOperatorTable.MULTISET_EXCEPT);
        // register(SqlStdOperatorTable.MULTISET_UNION_DISTINCT);
        // register(SqlStdOperatorTable.MULTISET_UNION);
        // register(SqlStdOperatorTable.SUBMULTISET_OF);
        // register(SqlStdOperatorTable.NOT_SUBMULTISET_OF);

        // Other functions and operators.
        //register(SqlStdOperatorTable.ROW);
        register(SqlStdOperatorTable.CAST);
        register(SqlLibraryOperators.INFIX_CAST);
        register(SqlStdOperatorTable.COALESCE);
        register(SqlLibraryOperators.NVL);
        register(SqlStdOperatorTable.NULLIF);
        register(SqlStdOperatorTable.CASE);
        register(SqlLibraryOperators.DECODE);
        register(SqlLibraryOperators.LEAST);
        register(SqlLibraryOperators.GREATEST);
        register(SqlLibraryOperators.COMPRESS);
        register(OCTET_LENGTH);
        register(SqlStdOperatorTable.DEFAULT);
        register(SqlStdOperatorTable.REINTERPRET);

        // XML Operators.
        register(SqlLibraryOperators.EXTRACT_VALUE);
        register(SqlLibraryOperators.XML_TRANSFORM);
        register(SqlLibraryOperators.EXTRACT_XML);
        register(SqlLibraryOperators.EXISTS_NODE);

        // JSON Operators
        register(SqlStdOperatorTable.JSON_TYPE_OPERATOR);
        register(SqlStdOperatorTable.JSON_VALUE_EXPRESSION);
        register(SqlStdOperatorTable.JSON_VALUE);
        register(SqlStdOperatorTable.JSON_QUERY);
        register(SqlLibraryOperators.JSON_TYPE);
        register(SqlStdOperatorTable.JSON_EXISTS);
        register(SqlLibraryOperators.JSON_DEPTH);
        register(SqlLibraryOperators.JSON_KEYS);
        register(SqlLibraryOperators.JSON_PRETTY);
        register(SqlLibraryOperators.JSON_LENGTH);
        register(SqlLibraryOperators.JSON_REMOVE);
        register(SqlLibraryOperators.JSON_STORAGE_SIZE);
        register(SqlStdOperatorTable.JSON_OBJECT);
        register(SqlStdOperatorTable.JSON_ARRAY);
        register(SqlStdOperatorTable.IS_JSON_VALUE);
        register(SqlStdOperatorTable.IS_JSON_OBJECT);
        register(SqlStdOperatorTable.IS_JSON_ARRAY);
        register(SqlStdOperatorTable.IS_JSON_SCALAR);
        register(SqlStdOperatorTable.IS_NOT_JSON_VALUE);
        register(SqlStdOperatorTable.IS_NOT_JSON_OBJECT);
        register(SqlStdOperatorTable.IS_NOT_JSON_ARRAY);
        register(SqlStdOperatorTable.IS_NOT_JSON_SCALAR);

        // Aggregate functions.
        register(SqlInternalOperators.LITERAL_AGG);

        // Current time functions.
        register(SqlStdOperatorTable.CURRENT_TIME);
        register(SqlStdOperatorTable.CURRENT_TIMESTAMP);
        register(SqlStdOperatorTable.CURRENT_DATE);
        register(SqlStdOperatorTable.LOCALTIME);
        register(SqlStdOperatorTable.LOCALTIMESTAMP);

        // Ignite specific operators
        register(LENGTH);
        register(SYSTEM_RANGE);
        register(TYPEOF);
        register(LEAST2);
        register(GREATEST2);
        register(RAND_UUID);
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
