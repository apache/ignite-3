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

/**
 * Helper methods for reading values from {@link InternalTuple} with allowed numeric type conversions.
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

        return castToByte(binaryTuple, binaryTupleIndex, actualType);
    }

    /** Reads a value from the tuple and converts it to a byte if possible. */
    public static byte readByteValue(InternalTuple binaryTuple, int binaryTupleIndex, ColumnType actualType, String columnName) {
        if (!integerType(actualType)) {
            throwClassCastException(ColumnType.INT8, actualType, columnName);
        }

        IgniteUtils.ensureNotNull(binaryTuple, binaryTupleIndex, columnName);

        return castToByte(binaryTuple, binaryTupleIndex, actualType);
    }

    /** Reads a value from the tuple and converts it to a short if possible. */
    public static short readShortValue(InternalTuple binaryTuple, int binaryTupleIndex, ColumnType actualType, int columnIndex) {
        if (!integerType(actualType)) {
            throwClassCastException(ColumnType.INT16, actualType, columnIndex);
        }

        IgniteUtils.ensureNotNull(binaryTuple, binaryTupleIndex, columnIndex);

        return castToShort(binaryTuple, binaryTupleIndex, actualType);
    }

    /** Reads a value from the tuple and converts it to a short if possible. */
    public static short readShortValue(InternalTuple binaryTuple, int binaryTupleIndex, ColumnType actualType, String columnName) {
        if (!integerType(actualType)) {
            throwClassCastException(ColumnType.INT16, actualType, columnName);
        }

        IgniteUtils.ensureNotNull(binaryTuple, binaryTupleIndex, columnName);

        return castToShort(binaryTuple, binaryTupleIndex, actualType);
    }

    /** Reads a value from the tuple and converts it to an int if possible. */
    public static int readIntValue(InternalTuple binaryTuple, int binaryTupleIndex, ColumnType actualType, int columnIndex) {
        if (!integerType(actualType)) {
            throwClassCastException(ColumnType.INT32, actualType, columnIndex);
        }

        IgniteUtils.ensureNotNull(binaryTuple, binaryTupleIndex, columnIndex);

        return castToInt(binaryTuple, binaryTupleIndex, actualType);
    }

    /** Reads a value from the tuple and converts it to an int if possible. */
    public static int readIntValue(InternalTuple binaryTuple, int binaryTupleIndex, ColumnType actualType, String columnName) {
        if (!integerType(actualType)) {
            throwClassCastException(ColumnType.INT32, actualType, columnName);
        }

        IgniteUtils.ensureNotNull(binaryTuple, binaryTupleIndex, columnName);

        return castToInt(binaryTuple, binaryTupleIndex, actualType);
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

            return castToFloat(binaryTuple, binaryTupleIndex, actualType);
        }

        throw newClassCastException(ColumnType.FLOAT, actualType, columnIndex);
    }

    /** Reads a value from the tuple and converts it to a float if possible. */
    public static float readFloatValue(InternalTuple binaryTuple, int binaryTupleIndex, ColumnType actualType, String columnName) {
        if (actualType == ColumnType.FLOAT || actualType == ColumnType.DOUBLE) {
            IgniteUtils.ensureNotNull(binaryTuple, binaryTupleIndex, columnName);

            return castToFloat(binaryTuple, binaryTupleIndex, actualType);
        }

        throw newClassCastException(ColumnType.FLOAT, actualType, columnName);
    }

    /** Reads a value from the tuple and returns it as a double. */
    public static double readDoubleValue(InternalTuple binaryTuple, int binaryTupleIndex, ColumnType actualType, int columnIndex) {
        if (actualType == ColumnType.DOUBLE || actualType == ColumnType.FLOAT) {
            IgniteUtils.ensureNotNull(binaryTuple, binaryTupleIndex, columnIndex);

            return binaryTuple.doubleValue(binaryTupleIndex);
        }

        throw newClassCastException(ColumnType.FLOAT, actualType, columnIndex);
    }

    /** Reads a value from the tuple and returns it as a double. */
    public static double readDoubleValue(InternalTuple binaryTuple, int binaryTupleIndex, ColumnType actualType, String columnName) {
        if (actualType == ColumnType.DOUBLE || actualType == ColumnType.FLOAT) {
            IgniteUtils.ensureNotNull(binaryTuple, binaryTupleIndex, columnName);

            return binaryTuple.doubleValue(binaryTupleIndex);
        }

        throw newClassCastException(ColumnType.FLOAT, actualType, columnName);
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

    /** Casts an integer value from the tuple to {@code byte} performing range checks. */
    private static byte castToByte(InternalTuple binaryTuple, int binaryTupleIndex, ColumnType valueType) {
        switch (valueType) {
            case INT8:
                return binaryTuple.byteValue(binaryTupleIndex);

            case INT16: {
                short value = binaryTuple.shortValue(binaryTupleIndex);
                byte byteValue = (byte) value;

                if (byteValue == value) {
                    return byteValue;
                }

                break;
            }
            case INT32: {
                int value = binaryTuple.intValue(binaryTupleIndex);
                byte byteValue = (byte) value;

                if (byteValue == value) {
                    return byteValue;
                }

                break;
            }
            case INT64: {
                long value = binaryTuple.longValue(binaryTupleIndex);
                byte byteValue = (byte) value;

                if (byteValue == value) {
                    return byteValue;
                }

                break;
            }

            default:
                assert false : valueType;
        }

        throw new ArithmeticException("Byte value overflow");
    }

    /** Casts an integer value from the tuple to {@code short} performing range checks. */
    private static short castToShort(InternalTuple binaryTuple, int binaryTupleIndex, ColumnType valueType) {
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

                break;
            }
            case INT64: {
                long value = binaryTuple.longValue(binaryTupleIndex);
                short shortValue = (short) value;

                if (shortValue == value) {
                    return shortValue;
                }

                break;
            }

            default:
                assert false : valueType;
        }

        throw new ArithmeticException("Short value overflow");
    }

    /** Casts an integer value from the tuple to {@code int} performing range checks. */
    private static int castToInt(InternalTuple binaryTuple, int binaryTupleIndex, ColumnType valueType) {
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

                break;
            }

            default:
                assert false : valueType;
        }

        throw new ArithmeticException("Int value overflow");
    }

    /** Casts a floating-point value from the tuple to {@code float} performing precision checks. */
    private static float castToFloat(InternalTuple binaryTuple, int binaryTupleIndex, ColumnType actualType) {
        if (actualType == ColumnType.FLOAT) {
            return binaryTuple.floatValue(binaryTupleIndex);
        }

        double doubleValue = binaryTuple.doubleValue(binaryTupleIndex);
        float floatValue = (float) doubleValue;

        if (doubleValue == floatValue || Double.isNaN(doubleValue)) {
            return floatValue;
        }

        throw new ArithmeticException("Float value overflow");
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
