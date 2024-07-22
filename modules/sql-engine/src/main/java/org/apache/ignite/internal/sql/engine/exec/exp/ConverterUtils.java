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

package org.apache.ignite.internal.sql.engine.exec.exp;

import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.adapter.enumerable.EnumUtils;
import org.apache.calcite.adapter.enumerable.RexImpTable;
import org.apache.calcite.linq4j.tree.ConstantUntypedNull;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.Primitive;
import org.apache.calcite.linq4j.tree.Types;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.runtime.SqlFunctions;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.BuiltInMethod;
import org.apache.calcite.util.Util;
import org.apache.ignite.internal.sql.engine.util.IgniteMath;

/**
 * ConverterUtils.
 * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
 */
public class ConverterUtils {
    private ConverterUtils() {
    }

    /**
     * In Calcite, {@code java.sql.Date} and {@code java.sql.Time} are stored as {@code Integer} type,
     * {@code java.sql.Timestamp} is stored as {@code Long} type.
     *
     * @param operand    Operand that should be converted.
     * @param targetType Required type.
     * @return New expression of required type.
     */
    static Expression toInternal(Expression operand, Type targetType) {
        return toInternal(operand, operand.getType(), targetType);
    }

    private static Type toInternal(RelDataType type) {
        return toInternal(type, false);
    }

    private static Expression toInternal(Expression operand,
            Type fromType, Type targetType) {
        if (fromType == java.sql.Date.class) {
            if (targetType == int.class) {
                return Expressions.call(BuiltInMethod.DATE_TO_INT.method, operand);
            } else if (targetType == Integer.class) {
                return Expressions.call(BuiltInMethod.DATE_TO_INT_OPTIONAL.method, operand);
            }
        } else if (fromType == java.sql.Time.class) {
            if (targetType == int.class) {
                return Expressions.call(BuiltInMethod.TIME_TO_INT.method, operand);
            } else if (targetType == Integer.class) {
                return Expressions.call(BuiltInMethod.TIME_TO_INT_OPTIONAL.method, operand);
            }
        } else if (fromType == java.sql.Timestamp.class) {
            if (targetType == long.class) {
                return Expressions.call(BuiltInMethod.TIMESTAMP_TO_LONG.method, operand);
            } else if (targetType == Long.class) {
                return Expressions.call(BuiltInMethod.TIMESTAMP_TO_LONG_OPTIONAL.method, operand);
            }
        }
        return operand;
    }

    static Type toInternal(RelDataType type, boolean forceNotNull) {
        switch (type.getSqlTypeName()) {
            case DATE:
            case TIME:
                return type.isNullable() && !forceNotNull ? Integer.class : int.class;
            case TIMESTAMP:
                return type.isNullable() && !forceNotNull ? Long.class : long.class;
            default:
                return null; // we don't care; use the default storage type
        }
    }

    /**
     * Converts from internal representation to JDBC representation used by arguments of user-defined functions.
     * For example, converts date values from {@code int} to {@link java.sql.Date}.
     */
    private static Expression fromInternal(Expression operand, Type targetType) {
        return fromInternal(operand, operand.getType(), targetType);
    }

    private static Expression fromInternal(Expression operand,
            Type fromType, Type targetType) {
        if (operand == ConstantUntypedNull.INSTANCE) {
            return operand;
        }
        if (!(operand.getType() instanceof Class)) {
            return operand;
        }
        if (Types.isAssignableFrom(targetType, fromType)) {
            return operand;
        }
        if (targetType == java.sql.Date.class) {
            // E.g. from "int" or "Integer" to "java.sql.Date",
            // generate "SqlFunctions.internalToDate".
            if (isA(fromType, Primitive.INT)) {
                return Expressions.call(BuiltInMethod.INTERNAL_TO_DATE.method, operand);
            }
        } else if (targetType == java.sql.Time.class) {
            // E.g. from "int" or "Integer" to "java.sql.Time",
            // generate "SqlFunctions.internalToTime".
            if (isA(fromType, Primitive.INT)) {
                return Expressions.call(BuiltInMethod.INTERNAL_TO_TIME.method, operand);
            }
        } else if (targetType == java.sql.Timestamp.class) {
            // E.g. from "long" or "Long" to "java.sql.Timestamp",
            // generate "SqlFunctions.internalToTimestamp".
            if (isA(fromType, Primitive.LONG)) {
                return Expressions.call(BuiltInMethod.INTERNAL_TO_TIMESTAMP.method, operand);
            }
        }
        if (Primitive.is(operand.type)
                && Primitive.isBox(targetType)) {
            // E.g. operand is "int", target is "Long", generate "(long) operand".
            return Expressions.convert_(operand,
                    Primitive.ofBox(targetType).primitiveClass);
        }
        return operand;
    }

    static List<Expression> fromInternal(Class<?>[] targetTypes,
            List<Expression> expressions) {
        final List<Expression> list = new ArrayList<>();
        if (targetTypes.length == expressions.size()) {
            for (int i = 0; i < expressions.size(); i++) {
                list.add(fromInternal(expressions.get(i), targetTypes[i]));
            }
        } else {
            int j = 0;
            for (int i = 0; i < expressions.size(); i++) {
                Class<?> type;
                if (!targetTypes[j].isArray()) {
                    type = targetTypes[j];
                    j++;
                } else {
                    type = targetTypes[j].getComponentType();
                }

                list.add(fromInternal(expressions.get(i), type));
            }
        }
        return list;
    }

    static List<Type> internalTypes(List<? extends RexNode> operandList) {
        return Util.transform(operandList, node -> toInternal(node.getType()));
    }

    /**
     * Convert {@code operand} from {@code fromType} to {@code targetType} which is BigDecimal type.
     *
     * @param operand The expression to convert
     * @param targetType Target type
     * @return An expression with BidDecimal type, which calls IgniteSqlFunctions.toBigDecimal function.
     */
    public static Expression convertToDecimal(Expression operand, RelDataType targetType) {
        assert targetType.getSqlTypeName() == SqlTypeName.DECIMAL;
        return Expressions.call(
                IgniteSqlFunctions.class,
                "toBigDecimal",
                operand,
                Expressions.constant(targetType.getPrecision()),
                Expressions.constant(targetType.getScale()));
    }

    /**
     * Convert {@code operand} to target type {@code toType}.
     *
     * @param operand The expression to convert.
     * @param toType  Target type.
     * @return A new expression with type {@code toType} or original if there is no need to convert.
     */
    public static Expression convert(Expression operand, Type toType) {
        final Type fromType = operand.getType();
        return convert(operand, fromType, toType);
    }

    /**
     * Convert {@code operand} to target type {@code toType}.
     *
     * @param operand  The expression to convert.
     * @param fromType Field type.
     * @param toType   Target type.
     * @return A new expression with type {@code toType} or original if there is no need to convert.
     */
    public static Expression convert(Expression operand, Type fromType, Type toType) {
        if (!Types.needTypeCast(fromType, toType)) {
            return operand;
        }

        if (toType == Void.class) {
            return RexImpTable.NULL_EXPR;
        }

        if (toType == BigDecimal.class) {
            throw new AssertionError("For conversion to decimal, ConverterUtils#convertToDecimal method should be used instead.");
        }

        Primitive toPrimitive = Primitive.of(toType);
        Primitive fromPrimitive = Primitive.of(fromType);

        boolean fromNumber = fromType instanceof Class
                && Number.class.isAssignableFrom((Class<?>) fromType);
        Primitive fromBox = Primitive.ofBox(fromType);

        if (toPrimitive != null) {
            if ((toPrimitive == Primitive.LONG || toPrimitive == Primitive.INT || toPrimitive == Primitive.SHORT
                    || toPrimitive == Primitive.BYTE) && fromType == String.class) {
                return Expressions.call(IgniteMath.class, "convertTo"
                        + SqlFunctions.initcap(toPrimitive.primitiveName) + "Exact", operand);
            }

            if (fromPrimitive != null) {
                // E.g. from "float" to "double"
                return IgniteExpressions.convertChecked(operand, fromPrimitive, toPrimitive);
            }

            if (fromNumber) {
                // Generate "x.shortValue()".
                return IgniteExpressions.unboxChecked(operand, fromBox, toPrimitive);
            } else {
                // E.g. from "Object" to "short".
                // Generate "SqlFunctions.toShort(x)"
                return Expressions.call(
                        SqlFunctions.class,
                        "to" + SqlFunctions.initcap(toPrimitive.primitiveName),
                        operand);
            }
        }

        // SELECT '0.1'::DECIMAL::VARCHAR case, looks like a stub
        if (toType == String.class) {
            if (fromType == BigDecimal.class) {
                // E.g. from "BigDecimal" to "String"
                // Generate "SqlFunctions.toString(x)"
                return Expressions.condition(
                        Expressions.equal(operand, RexImpTable.NULL_EXPR),
                        RexImpTable.NULL_EXPR,
                        Expressions.call(
                                IgniteSqlFunctions.class,
                                "toString",
                                operand));
            }
        }

        var toCustomType = CustomTypesConversion.INSTANCE.tryConvert(operand, toType);
        return toCustomType != null ? toCustomType : EnumUtils.convert(operand, toType);
    }

    private static boolean isA(Type fromType, Primitive primitive) {
        return Primitive.of(fromType) == primitive
                || Primitive.ofBox(fromType) == primitive;
    }

    /**
     * In {@link org.apache.calcite.sql.type.SqlTypeAssignmentRule}, some rules decide whether one type can be
     * assignable to another type.
     * Based on these rules, a function can accept arguments with assignable types.
     *
     * <p>For example, a function with Long type operand can accept Integer as input.
     * See {@code org.apache.calcite.sql.SqlUtil#filterRoutinesByParameterType()} for details.
     *
     * <p>During query execution, some of the assignable types need explicit conversion
     * to the target types. i.e., Decimal expression should be converted to Integer before it is assigned to the Integer
     * type Lvalue(In Java, Decimal can not be assigned to Integer directly).
     *
     * @param targetTypes Formal operand types declared for the function arguments.
     * @param arguments   Input expressions to the function.
     * @return Input expressions with probable type conversion.
     */
    static List<Expression> convertAssignableTypes(Class<?>[] targetTypes,
            List<Expression> arguments) {
        final List<Expression> list = new ArrayList<>();
        if (targetTypes.length == arguments.size()) {
            for (int i = 0; i < arguments.size(); i++) {
                list.add(convertAssignableType(arguments.get(i), targetTypes[i]));
            }
        } else {
            int j = 0;
            for (Expression argument : arguments) {
                Class<?> type;
                if (!targetTypes[j].isArray()) {
                    type = targetTypes[j];
                    j++;
                } else {
                    type = targetTypes[j].getComponentType();
                }

                list.add(convertAssignableType(argument, type));
            }
        }
        return list;
    }

    /**
     * Handles decimal type specifically with explicit type conversion.
     */
    private static Expression convertAssignableType(Expression argument, Type targetType) {
        if (targetType != BigDecimal.class) {
            return argument;
        }

        return convert(argument, targetType);
    }
}
