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

package org.apache.ignite.internal.util;

import org.apache.ignite.sql.ColumnType;
import org.apache.ignite.table.Tuple;

/**
 * Helper methods that perform conversions between numeric types. These methods
 * are used when writing the primitive numeric values to a {@link Tuple tuple}.
 *
 * <p>The following conversions are supported:
 * <ul>
 *     <li>Any integer type to any other integer type with range checks (e.g. {@link ColumnType#INT64} to {@link ColumnType#INT8}
 *     may throw {@link ArithmeticException} if the value doesn't fit into {@link ColumnType#INT8}).</li>
 *     <li>{@link ColumnType#FLOAT} to {@link ColumnType#DOUBLE} are always allowed.</li>
 *     <li>{@link ColumnType#DOUBLE} to {@link ColumnType#FLOAT} with precision checks (may throw {@link ArithmeticException}
 *     if the value cannot be represented as FLOAT without precision loss).</li>
 * </ul>
 */
public class TupleTypeCastUtils {
    /** Integer column types bitmask. */
    private static final int INT_COLUMN_TYPES_BITMASK = buildIntegerTypesBitMask();

    /** Casts an object to {@code byte} if possible. */
    public static byte castToByte(Object number) {
        if (number instanceof Byte) {
            return (byte) number;
        }

        if (number instanceof Long || number instanceof Integer || number instanceof Short) {
            long longVal = ((Number) number).longValue();
            byte byteVal = ((Number) number).byteValue();

            if (longVal == byteVal) {
                return byteVal;
            }

            throw new ArithmeticException("Byte value overflow: " + number);
        }

        throw new ClassCastException(number.getClass() + " cannot be cast to " + byte.class);
    }

    /** Casts an object to {@code short} if possible. */
    public static short castToShort(Object number) {
        if (number instanceof Short) {
            return (short) number;
        }

        if (number instanceof Long || number instanceof Integer || number instanceof Byte) {
            long longVal = ((Number) number).longValue();
            short shortVal = ((Number) number).shortValue();

            if (longVal == shortVal) {
                return shortVal;
            }

            throw new ArithmeticException("Short value overflow: " + number);
        }

        throw new ClassCastException(number.getClass() + " cannot be cast to " + short.class);
    }

    /** Casts an object to {@code int} if possible. */
    public static int castToInt(Object number) {
        if (number instanceof Integer) {
            return (int) number;
        }

        if (number instanceof Long || number instanceof Short || number instanceof Byte) {
            long longVal = ((Number) number).longValue();
            int intVal = ((Number) number).intValue();

            if (longVal == intVal) {
                return intVal;
            }

            throw new ArithmeticException("Int value overflow: " + number);
        }

        throw new ClassCastException(number.getClass() + " cannot be cast to " + int.class);
    }

    /** Casts an object to {@code long} if possible. */
    public static long castToLong(Object number) {
        if (number instanceof Long || number instanceof Integer || number instanceof Short || number instanceof Byte) {
            return ((Number) number).longValue();
        }

        throw new ClassCastException(number.getClass() + " cannot be cast to " + long.class);
    }

    /** Casts an object to {@code float} if possible. */
    public static float castToFloat(Object number) {
        if (number instanceof Float) {
            return (float) number;
        }

        if (number instanceof Double) {
            double doubleVal = ((Number) number).doubleValue();
            float floatVal = ((Number) number).floatValue();

            //noinspection FloatingPointEquality
            if (doubleVal == floatVal || Double.isNaN(doubleVal)) {
                return floatVal;
            }

            throw new ArithmeticException("Float value overflow: " + number);
        }

        throw new ClassCastException(number.getClass() + " cannot be cast to " + float.class);
    }

    /** Casts an object to {@code double} if possible. */
    public static double castToDouble(Object number) {
        if (number instanceof Double || number instanceof Float) {
            return ((Number) number).doubleValue();
        }

        throw new ClassCastException(number.getClass() + " cannot be cast to " + double.class);
    }

    /**
     * Checks whether a cast is possible between two types for the given value.
     *
     * <p>Widening casts between integer types and between floating-point types are always allowed.
     *
     * <p>Narrowing casts between integer types and between floating-point types are allowed only
     * when the provided value can be represented in the target type.
     *
     * @param from Source column type
     * @param to Target column type
     * @param val The value to be cast
     * @return {@code True} if the cast is possible without data loss, {@code false} otherwise.
     */
    public static boolean isCastAllowed(ColumnType from, ColumnType to, Object val) {
        if (!(val instanceof Number)) {
            return false;
        }

        Number number = (Number) val;

        switch (to) {
            case INT8:
                return integerType(from) && number.byteValue() == number.longValue();
            case INT16:
                return integerType(from) && number.shortValue() == number.longValue();
            case INT32:
                return integerType(from) && number.intValue() == number.longValue();
            case INT64:
                return integerType(from);
            case FLOAT:
                if (from == ColumnType.DOUBLE) {
                    double doubleValue = number.doubleValue();
                    return number.floatValue() == doubleValue || Double.isNaN(doubleValue);
                }
                return false;
            case DOUBLE:
                return from == ColumnType.FLOAT;

            default:
                return false;
        }
    }

    private static boolean integerType(ColumnType type) {
        return (INT_COLUMN_TYPES_BITMASK & (1 << type.ordinal())) != 0;
    }

    private static int buildIntegerTypesBitMask() {
        assert ColumnType.values().length < Integer.SIZE : "Too many column types to fit in an integer bitmask";

        ColumnType[] intTypes = {
                ColumnType.INT8,
                ColumnType.INT16,
                ColumnType.INT32,
                ColumnType.INT64
        };

        int elements = 0;

        for (ColumnType e : intTypes) {
            elements |= (1 << e.ordinal());
        }

        return elements;
    }
}
