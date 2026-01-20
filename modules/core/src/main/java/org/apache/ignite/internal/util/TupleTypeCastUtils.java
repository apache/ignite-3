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

import org.apache.ignite.internal.lang.IgniteStringFormatter;
import org.apache.ignite.internal.lang.InternalTuple;
import org.apache.ignite.sql.ColumnType;
import org.apache.ignite.table.Tuple;

/**
 * Helper methods that perform conversions between numeric types. These methods are used when reading
 * and writing the primitive numeric values of a {@link Tuple tuple}.
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
    private static final String TYPE_CAST_ERROR_COLUMN_NAME = "Column with name '{}' has type {} but {} was requested";

    private static final String TYPE_CAST_ERROR_COLUMN_INDEX = "Column with index {} has type {} but {} was requested";

    private static final int INT_COLUMN_TYPES_BITMASK = buildIntegerTypesBitMask();

    /** Reads a value from the tuple and converts it to a byte if possible. */
    public static byte readByteValue(InternalTuple binaryTuple, int binaryTupleIndex, ColumnType actualType, int columnIndex) {
        if (!integerType(actualType)) {
            throwClassCastException(ColumnType.INT8, actualType, columnIndex);
        }

        IgniteUtils.ensureNotNull(binaryTuple, binaryTupleIndex, columnIndex);

        return toByte(binaryTuple, binaryTupleIndex, actualType);
    }

    /** Reads a value from the tuple and converts it to a byte if possible. */
    public static byte readByteValue(InternalTuple binaryTuple, int binaryTupleIndex, ColumnType actualType, String columnName) {
        if (!integerType(actualType)) {
            throwClassCastException(ColumnType.INT8, actualType, columnName);
        }

        IgniteUtils.ensureNotNull(binaryTuple, binaryTupleIndex, columnName);

        return toByte(binaryTuple, binaryTupleIndex, actualType);
    }

    /** Reads a value from the tuple and converts it to a short if possible. */
    public static short readShortValue(InternalTuple binaryTuple, int binaryTupleIndex, ColumnType actualType, int columnIndex) {
        if (!integerType(actualType)) {
            throwClassCastException(ColumnType.INT16, actualType, columnIndex);
        }

        IgniteUtils.ensureNotNull(binaryTuple, binaryTupleIndex, columnIndex);

        return toShort(binaryTuple, binaryTupleIndex, actualType);
    }

    /** Reads a value from the tuple and converts it to a short if possible. */
    public static short readShortValue(InternalTuple binaryTuple, int binaryTupleIndex, ColumnType actualType, String columnName) {
        if (!integerType(actualType)) {
            throwClassCastException(ColumnType.INT16, actualType, columnName);
        }

        IgniteUtils.ensureNotNull(binaryTuple, binaryTupleIndex, columnName);

        return toShort(binaryTuple, binaryTupleIndex, actualType);
    }

    /** Reads a value from the tuple and converts it to an int if possible. */
    public static int readIntValue(InternalTuple binaryTuple, int binaryTupleIndex, ColumnType actualType, int columnIndex) {
        if (!integerType(actualType)) {
            throwClassCastException(ColumnType.INT32, actualType, columnIndex);
        }

        IgniteUtils.ensureNotNull(binaryTuple, binaryTupleIndex, columnIndex);

        return toInt(binaryTuple, binaryTupleIndex, actualType);
    }

    /** Reads a value from the tuple and converts it to an int if possible. */
    public static int readIntValue(InternalTuple binaryTuple, int binaryTupleIndex, ColumnType actualType, String columnName) {
        if (!integerType(actualType)) {
            throwClassCastException(ColumnType.INT32, actualType, columnName);
        }

        IgniteUtils.ensureNotNull(binaryTuple, binaryTupleIndex, columnName);

        return toInt(binaryTuple, binaryTupleIndex, actualType);
    }

    /** Reads a value from the tuple and returns it as a long. Only integer column types are allowed. */
    public static long readLongValue(InternalTuple binaryTuple, int binaryTupleIndex, ColumnType actualType, int columnIndex) {
        if (!integerType(actualType)) {
            throwClassCastException(ColumnType.INT64, actualType, columnIndex);
        }

        IgniteUtils.ensureNotNull(binaryTuple, binaryTupleIndex, columnIndex);

        return binaryTuple.longValue(binaryTupleIndex);
    }

    /** Reads a value from the tuple and returns it as a long. Only integer column types are allowed. */
    public static long readLongValue(InternalTuple binaryTuple, int binaryTupleIndex, ColumnType actualType, String columnName) {
        if (!integerType(actualType)) {
            throwClassCastException(ColumnType.INT64, actualType, columnName);
        }

        IgniteUtils.ensureNotNull(binaryTuple, binaryTupleIndex, columnName);

        return binaryTuple.longValue(binaryTupleIndex);
    }

    /** Reads a value from the tuple and converts it to a float if possible. */
    public static float readFloatValue(InternalTuple binaryTuple, int binaryTupleIndex, ColumnType actualType, int columnIndex) {
        if (actualType == ColumnType.FLOAT || actualType == ColumnType.DOUBLE) {
            IgniteUtils.ensureNotNull(binaryTuple, binaryTupleIndex, columnIndex);

            return toFloat(binaryTuple, binaryTupleIndex, actualType);
        }

        throw newClassCastException(ColumnType.FLOAT, actualType, columnIndex);
    }

    /** Reads a value from the tuple and converts it to a float if possible. */
    public static float readFloatValue(InternalTuple binaryTuple, int binaryTupleIndex, ColumnType actualType, String columnName) {
        if (actualType == ColumnType.FLOAT || actualType == ColumnType.DOUBLE) {
            IgniteUtils.ensureNotNull(binaryTuple, binaryTupleIndex, columnName);

            return toFloat(binaryTuple, binaryTupleIndex, actualType);
        }

        throw newClassCastException(ColumnType.FLOAT, actualType, columnName);
    }

    /** Reads a value from the tuple and returns it as a double. */
    public static double readDoubleValue(InternalTuple binaryTuple, int binaryTupleIndex, ColumnType actualType, int columnIndex) {
        if (actualType == ColumnType.DOUBLE || actualType == ColumnType.FLOAT) {
            IgniteUtils.ensureNotNull(binaryTuple, binaryTupleIndex, columnIndex);

            return binaryTuple.doubleValue(binaryTupleIndex);
        }

        throw newClassCastException(ColumnType.DOUBLE, actualType, columnIndex);
    }

    /** Reads a value from the tuple and returns it as a double. */
    public static double readDoubleValue(InternalTuple binaryTuple, int binaryTupleIndex, ColumnType actualType, String columnName) {
        if (actualType == ColumnType.DOUBLE || actualType == ColumnType.FLOAT) {
            IgniteUtils.ensureNotNull(binaryTuple, binaryTupleIndex, columnName);

            return binaryTuple.doubleValue(binaryTupleIndex);
        }

        throw newClassCastException(ColumnType.DOUBLE, actualType, columnName);
    }

    /**
     * Validates that the requested column type matches the actual type and throws {@link ClassCastException}
     * otherwise.
     */
    public static void validateColumnType(ColumnType requestedType, ColumnType actualType, int columnIndex) {
        if (requestedType != actualType) {
            throwClassCastException(requestedType, actualType, columnIndex);
        }
    }

    /**
     * Validates that the requested column type matches the actual type and throws {@link ClassCastException}
     * otherwise.
     */
    public static void validateColumnType(ColumnType requestedType, ColumnType actualType, String columnName) {
        if (requestedType != actualType) {
            throwClassCastException(requestedType, actualType, columnName);
        }
    }

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

    /** Casts an integer value from the tuple to {@code byte} performing range checks. */
    @SuppressWarnings("NumericCastThatLosesPrecision")
    private static byte toByte(InternalTuple binaryTuple, int binaryTupleIndex, ColumnType valueType) {
        switch (valueType) {
            case INT8:
                return binaryTuple.byteValue(binaryTupleIndex);

            case INT16: {
                short value = binaryTuple.shortValue(binaryTupleIndex);
                byte byteValue = (byte) value;

                if (byteValue == value) {
                    return byteValue;
                }

                throw new ArithmeticException("Byte value overflow: " + value);
            }
            case INT32: {
                int value = binaryTuple.intValue(binaryTupleIndex);
                byte byteValue = (byte) value;

                if (byteValue == value) {
                    return byteValue;
                }

                throw new ArithmeticException("Byte value overflow: " + value);
            }
            case INT64: {
                long value = binaryTuple.longValue(binaryTupleIndex);
                byte byteValue = (byte) value;

                if (byteValue == value) {
                    return byteValue;
                }

                throw new ArithmeticException("Byte value overflow: " + value);
            }

            default:
        }

        throw new IllegalArgumentException("invalid type: " + valueType);
    }

    /** Casts an integer value from the tuple to {@code short} performing range checks. */
    @SuppressWarnings("NumericCastThatLosesPrecision")
    private static short toShort(InternalTuple binaryTuple, int binaryTupleIndex, ColumnType valueType) {
        switch (valueType) {
            case INT16:
            case INT8:
                return binaryTuple.shortValue(binaryTupleIndex);

            case INT32: {
                int value = binaryTuple.intValue(binaryTupleIndex);
                short shortValue = (short) value;

                if (shortValue == value) {
                    return shortValue;
                }

                throw new ArithmeticException("Short value overflow: " + value);
            }
            case INT64: {
                long value = binaryTuple.longValue(binaryTupleIndex);
                short shortValue = (short) value;

                if (shortValue == value) {
                    return shortValue;
                }

                throw new ArithmeticException("Short value overflow: " + value);
            }

            default:
        }

        throw new IllegalArgumentException("invalid type: " + valueType);
    }

    /** Casts an integer value from the tuple to {@code int} performing range checks. */
    @SuppressWarnings("NumericCastThatLosesPrecision")
    private static int toInt(InternalTuple binaryTuple, int binaryTupleIndex, ColumnType valueType) {
        switch (valueType) {
            case INT32:
            case INT16:
            case INT8:
                return binaryTuple.intValue(binaryTupleIndex);

            case INT64: {
                long value = binaryTuple.longValue(binaryTupleIndex);
                int intValue = (int) value;

                if (intValue == value) {
                    return intValue;
                }

                throw new ArithmeticException("Int value overflow: " + value);
            }

            default:
                assert false : valueType;
        }

        throw new IllegalArgumentException("invalid type: " + valueType);
    }

    /** Casts a floating-point value from the tuple to {@code float} performing precision checks. */
    @SuppressWarnings({"NumericCastThatLosesPrecision", "FloatingPointEquality"})
    private static float toFloat(InternalTuple binaryTuple, int binaryTupleIndex, ColumnType actualType) {
        if (actualType == ColumnType.FLOAT) {
            return binaryTuple.floatValue(binaryTupleIndex);
        }

        double doubleValue = binaryTuple.doubleValue(binaryTupleIndex);
        float floatValue = (float) doubleValue;

        if (doubleValue == floatValue || Double.isNaN(doubleValue)) {
            return floatValue;
        }

        throw new ArithmeticException("Float value overflow: " + doubleValue);
    }

    private static void throwClassCastException(ColumnType requestedType, ColumnType actualType, int index) {
        throw newClassCastException(requestedType, actualType, index);
    }

    private static void throwClassCastException(ColumnType requestedType, ColumnType actualType, String columnName) {
        throw newClassCastException(requestedType, actualType, columnName);
    }

    private static RuntimeException newClassCastException(ColumnType requestedType, ColumnType actualType, int index) {
        return new ClassCastException(IgniteStringFormatter.format(TYPE_CAST_ERROR_COLUMN_INDEX, index, actualType, requestedType));
    }

    private static RuntimeException newClassCastException(ColumnType requestedType, ColumnType actualType, String columnName) {
        return new ClassCastException(IgniteStringFormatter.format(TYPE_CAST_ERROR_COLUMN_NAME, columnName, actualType, requestedType));
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
