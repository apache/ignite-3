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

import java.util.EnumSet;
import java.util.Set;
import org.apache.ignite.internal.lang.IgniteStringFormatter;
import org.apache.ignite.internal.lang.InternalTuple;
import org.apache.ignite.sql.ColumnType;

/**
 * Utility helpers for reading and casting values from {@link InternalTuple} instances.
 *
 * <p>Provides typed readers (byte, short, int, long, float, double) that validate the actual column
 * type and perform safe implicit conversions where allowed. Methods throw {@link ClassCastException}
 * when the requested type is incompatible with the actual column type. Narrowing conversions that
 * would lose information throw {@link ArithmeticException}.</p>
 */
public class TupleTypeCastUtils {
    private static final String TYPE_CAST_ERROR_COLUMN_NAME = "Column with name '{}' has type {} but {} was requested";

    private static final String TYPE_CAST_ERROR_COLUMN_INDEX = "Column with index {} has type {} but {} was requested";

    private static final Set<ColumnType> INT_TYPES =
            EnumSet.of(ColumnType.INT8, ColumnType.INT16, ColumnType.INT32, ColumnType.INT64);

    /**
     * Reads a value from the tuple and converts it to a byte if possible.
     *
     * @param binaryTuple the tuple instance
     * @param binaryTupleIndex index of the column in the tuple
     * @param actualType actual column type stored in the tuple
     * @param columnIndex column index used for error reporting
     * @return the column value as a {@code byte}
     * @throws ClassCastException if the actual column type is not an integer type
     * @throws NullPointerException if {@code binaryTuple} is null (enforced by {@code IgniteUtils.ensureNotNull})
     * @throws ArithmeticException if the value cannot be represented as {@code byte} without overflow
     */
    public static byte readByteValue(InternalTuple binaryTuple, int binaryTupleIndex, ColumnType actualType, int columnIndex) {
        if (!integerType(actualType)) {
            throwClassCastException(ColumnType.INT8, actualType, columnIndex);
        }

        IgniteUtils.ensureNotNull(binaryTuple, binaryTupleIndex, columnIndex);

        return castToByte(binaryTuple, binaryTupleIndex, actualType);
    }

    /**
     * Reads a value from the tuple and converts it to a byte if possible.
     *
     * @param binaryTuple the tuple instance
     * @param binaryTupleIndex index of the column in the tuple
     * @param actualType actual column type stored in the tuple
     * @param columnName column name used for error reporting
     * @return the column value as a {@code byte}
     * @throws ClassCastException if the actual column type is not an integer type
     * @throws NullPointerException if {@code binaryTuple} is null (enforced by {@code IgniteUtils.ensureNotNull})
     * @throws ArithmeticException if the value cannot be represented as {@code byte} without overflow
     */
    public static byte readByteValue(InternalTuple binaryTuple, int binaryTupleIndex, ColumnType actualType, String columnName) {
        if (!integerType(actualType)) {
            throwClassCastException(ColumnType.INT8, actualType, columnName);
        }

        IgniteUtils.ensureNotNull(binaryTuple, binaryTupleIndex, columnName);

        return castToByte(binaryTuple, binaryTupleIndex, actualType);
    }

    /**
     * Reads a value from the tuple and converts it to a short if possible.
     *
     * @param binaryTuple the tuple instance
     * @param binaryTupleIndex index of the column in the tuple
     * @param actualType actual column type stored in the tuple
     * @param columnIndex column index used for error reporting
     * @return the column value as a {@code short}
     * @throws ClassCastException if the actual column type is not an integer type
     * @throws NullPointerException if {@code binaryTuple} is null
     * @throws ArithmeticException if the value cannot be represented as {@code short} without overflow
     */
    public static short readShortValue(InternalTuple binaryTuple, int binaryTupleIndex, ColumnType actualType, int columnIndex) {
        if (!integerType(actualType)) {
            throwClassCastException(ColumnType.INT16, actualType, columnIndex);
        }

        IgniteUtils.ensureNotNull(binaryTuple, binaryTupleIndex, columnIndex);

        return castToShort(binaryTuple, binaryTupleIndex, actualType);
    }

    /**
     * Reads a value from the tuple and converts it to a short if possible.
     *
     * @param binaryTuple the tuple instance
     * @param binaryTupleIndex index of the column in the tuple
     * @param actualType actual column type stored in the tuple
     * @param columnName column name used for error reporting
     * @return the column value as a {@code short}
     * @throws ClassCastException if the actual column type is not an integer type
     * @throws NullPointerException if {@code binaryTuple} is null
     * @throws ArithmeticException if the value cannot be represented as {@code short} without overflow
     */
    public static short readShortValue(InternalTuple binaryTuple, int binaryTupleIndex, ColumnType actualType, String columnName) {
        if (!integerType(actualType)) {
            throwClassCastException(ColumnType.INT16, actualType, columnName);
        }

        IgniteUtils.ensureNotNull(binaryTuple, binaryTupleIndex, columnName);

        return castToShort(binaryTuple, binaryTupleIndex, actualType);
    }

    /**
     * Reads a value from the tuple and converts it to an int if possible.
     *
     * @param binaryTuple the tuple instance
     * @param binaryTupleIndex index of the column in the tuple
     * @param actualType actual column type stored in the tuple
     * @param columnIndex column index used for error reporting
     * @return the column value as an {@code int}
     * @throws ClassCastException if the actual column type is not an integer type
     * @throws NullPointerException if {@code binaryTuple} is null
     * @throws ArithmeticException if the value cannot be represented as {@code int} without overflow
     */
    public static int readIntValue(InternalTuple binaryTuple, int binaryTupleIndex, ColumnType actualType, int columnIndex) {
        if (!integerType(actualType)) {
            throwClassCastException(ColumnType.INT32, actualType, columnIndex);
        }

        IgniteUtils.ensureNotNull(binaryTuple, binaryTupleIndex, columnIndex);

        return castToInt(binaryTuple, binaryTupleIndex, actualType);
    }

    /**
     * Reads a value from the tuple and converts it to an int if possible.
     *
     * @param binaryTuple the tuple instance
     * @param binaryTupleIndex index of the column in the tuple
     * @param actualType actual column type stored in the tuple
     * @param columnName column name used for error reporting
     * @return the column value as an {@code int}
     * @throws ClassCastException if the actual column type is not an integer type
     * @throws NullPointerException if {@code binaryTuple} is null
     * @throws ArithmeticException if the value cannot be represented as {@code int} without overflow
     */
    public static int readIntValue(InternalTuple binaryTuple, int binaryTupleIndex, ColumnType actualType, String columnName) {
        if (!integerType(actualType)) {
            throwClassCastException(ColumnType.INT32, actualType, columnName);
        }

        IgniteUtils.ensureNotNull(binaryTuple, binaryTupleIndex, columnName);

        return castToInt(binaryTuple, binaryTupleIndex, actualType);
    }

    /**
     * Reads a value from the tuple and returns it as a long. Only integer column types are allowed.
     *
     * @param binaryTuple the tuple instance
     * @param binaryTupleIndex index of the column in the tuple
     * @param actualType actual column type stored in the tuple
     * @param columnIndex column index used for error reporting
     * @return the column value as a {@code long}
     * @throws ClassCastException if the actual column type is not an integer type
     * @throws NullPointerException if {@code binaryTuple} is null
     */
    public static long readLongValue(InternalTuple binaryTuple, int binaryTupleIndex, ColumnType actualType, int columnIndex) {
        if (!integerType(actualType)) {
            throwClassCastException(ColumnType.INT64, actualType, columnIndex);
        }

        IgniteUtils.ensureNotNull(binaryTuple, binaryTupleIndex, columnIndex);

        return binaryTuple.longValue(binaryTupleIndex);
    }

    /**
     * Reads a value from the tuple and returns it as a long. Only integer column types are allowed.
     *
     * @param binaryTuple the tuple instance
     * @param binaryTupleIndex index of the column in the tuple
     * @param actualType actual column type stored in the tuple
     * @param columnName column name used for error reporting
     * @return the column value as a {@code long}
     * @throws ClassCastException if the actual column type is not an integer type
     * @throws NullPointerException if {@code binaryTuple} is null
     */
    public static long readLongValue(InternalTuple binaryTuple, int binaryTupleIndex, ColumnType actualType, String columnName) {
        if (!integerType(actualType)) {
            throwClassCastException(ColumnType.INT64, actualType, columnName);
        }

        IgniteUtils.ensureNotNull(binaryTuple, binaryTupleIndex, columnName);

        return binaryTuple.longValue(binaryTupleIndex);
    }

    /**
     * Reads a value from the tuple and converts it to a float if possible. Accepts FLOAT and DOUBLE actual types.
     *
     * @param binaryTuple the tuple instance
     * @param binaryTupleIndex index of the column in the tuple
     * @param actualType actual column type stored in the tuple
     * @param columnIndex column index used for error reporting
     * @return the column value as a {@code float}
     * @throws ClassCastException if the actual column type is neither FLOAT nor DOUBLE
     * @throws NullPointerException if {@code binaryTuple} is null
     * @throws ArithmeticException if a double value cannot be represented as float without precision loss
     */
    public static float readFloatValue(InternalTuple binaryTuple, int binaryTupleIndex, ColumnType actualType, int columnIndex) {
        if (actualType == ColumnType.FLOAT || actualType == ColumnType.DOUBLE) {
            IgniteUtils.ensureNotNull(binaryTuple, binaryTupleIndex, columnIndex);

            return castToFloat(binaryTuple, binaryTupleIndex, actualType);
        }

        throw newClassCastException(ColumnType.FLOAT, actualType, columnIndex);
    }

    /**
     * Reads a value from the tuple and converts it to a float if possible. Accepts FLOAT and DOUBLE actual types.
     *
     * @param binaryTuple the tuple instance
     * @param binaryTupleIndex index of the column in the tuple
     * @param actualType actual column type stored in the tuple
     * @param columnName column name used for error reporting
     * @return the column value as a {@code float}
     * @throws ClassCastException if the actual column type is neither FLOAT nor DOUBLE
     * @throws NullPointerException if {@code binaryTuple} is null
     * @throws ArithmeticException if a double value cannot be represented as float without precision loss
     */
    public static float readFloatValue(InternalTuple binaryTuple, int binaryTupleIndex, ColumnType actualType, String columnName) {
        if (actualType == ColumnType.FLOAT || actualType == ColumnType.DOUBLE) {
            IgniteUtils.ensureNotNull(binaryTuple, binaryTupleIndex, columnName);

            return castToFloat(binaryTuple, binaryTupleIndex, actualType);
        }

        throw newClassCastException(ColumnType.FLOAT, actualType, columnName);
    }

    /**
     * Reads a value from the tuple and returns it as a double. Accepts DOUBLE and FLOAT actual types.
     *
     * @param binaryTuple the tuple instance
     * @param binaryTupleIndex index of the column in the tuple
     * @param actualType actual column type stored in the tuple
     * @param columnIndex column index used for error reporting
     * @return the column value as a {@code double}
     * @throws ClassCastException if the actual column type is neither DOUBLE nor FLOAT
     * @throws NullPointerException if {@code binaryTuple} is null
     */
    public static double readDoubleValue(InternalTuple binaryTuple, int binaryTupleIndex, ColumnType actualType, int columnIndex) {
        if (actualType == ColumnType.DOUBLE || actualType == ColumnType.FLOAT) {
            IgniteUtils.ensureNotNull(binaryTuple, binaryTupleIndex, columnIndex);

            return binaryTuple.doubleValue(binaryTupleIndex);
        }

        throw newClassCastException(ColumnType.FLOAT, actualType, columnIndex);
    }

    /**
     * Reads a value from the tuple and returns it as a double. Accepts DOUBLE and FLOAT actual types.
     *
     * @param binaryTuple the tuple instance
     * @param binaryTupleIndex index of the column in the tuple
     * @param actualType actual column type stored in the tuple
     * @param columnName column name used for error reporting
     * @return the column value as a {@code double}
     * @throws ClassCastException if the actual column type is neither DOUBLE nor FLOAT
     * @throws NullPointerException if {@code binaryTuple} is null
     */
    public static double readDoubleValue(InternalTuple binaryTuple, int binaryTupleIndex, ColumnType actualType, String columnName) {
        if (actualType == ColumnType.DOUBLE || actualType == ColumnType.FLOAT) {
            IgniteUtils.ensureNotNull(binaryTuple, binaryTupleIndex, columnName);

            return binaryTuple.doubleValue(binaryTupleIndex);
        }

        throw newClassCastException(ColumnType.FLOAT, actualType, columnName);
    }

    /**
     * Casts an integer value from the tuple to {@code byte} performing range checks.
     *
     * @param binaryTuple the tuple instance
     * @param binaryTupleIndex index of the column in the tuple
     * @param valueType actual integer column type
     * @return the value as a {@code byte}
     * @throws ArithmeticException if value doesn't fit into a {@code byte}
     */
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

    /**
     * Casts an integer value from the tuple to {@code short} performing range checks.
     *
     * @param binaryTuple the tuple instance
     * @param binaryTupleIndex index of the column in the tuple
     * @param valueType actual integer column type
     * @return the value as a {@code short}
     * @throws ArithmeticException if value doesn't fit into a {@code short}
     */
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

    /**
     * Casts an integer value from the tuple to {@code int} performing range checks.
     *
     * @param binaryTuple the tuple instance
     * @param binaryTupleIndex index of the column in the tuple
     * @param valueType actual integer column type
     * @return the value as an {@code int}
     * @throws ArithmeticException if value doesn't fit into an {@code int}
     */
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

    /**
     * Casts a floating-point value from the tuple to {@code float} performing precision checks.
     *
     * @param binaryTuple the tuple instance
     * @param binaryTupleIndex index of the column in the tuple
     * @param actualType actual floating-point column type
     * @return the value as a {@code float}
     * @throws ArithmeticException if a double value cannot be represented as a float without precision loss
     */
    private static float castToFloat(InternalTuple binaryTuple, int binaryTupleIndex, ColumnType actualType) {
        if (actualType == ColumnType.FLOAT) {
            return binaryTuple.floatValue(binaryTupleIndex);
        }

        double doubleValue = binaryTuple.doubleValue(binaryTupleIndex);
        float floatValue = (float) doubleValue;

        if (doubleValue == floatValue) {
            return floatValue;
        }

        throw new ArithmeticException("Float value overflow");
    }

    /**
     * Validates that the requested column type matches the actual type and throws {@link ClassCastException}
     * otherwise.
     *
     * @param requestedType the requested column type
     * @param actualType the actual column type
     * @param columnIndex column index used for error reporting
     * @throws ClassCastException when types do not match
     */
    public static void validateColumnType(ColumnType requestedType, ColumnType actualType, int columnIndex) {
        if (requestedType != actualType) {
            throwClassCastException(requestedType, actualType, columnIndex);
        }
    }

    /**
     * Validates that the requested column type matches the actual type and throws {@link ClassCastException}
     * otherwise.
     *
     * @param requestedType the requested column type
     * @param actualType the actual column type
     * @param columnName column name used for error reporting
     * @throws ClassCastException when types do not match
     */
    public static void validateColumnType(ColumnType requestedType, ColumnType actualType, String columnName) {
        if (requestedType != actualType) {
            throwClassCastException(requestedType, actualType, columnName);
        }
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

    /**
     * Returns {@code true} when the provided {@link ColumnType} is an integer type.
     *
     * @param actualType column type to check
     * @return {@code true} if {@code actualType} is one of INT8, INT16, INT32 or INT64
     */
    private static boolean integerType(ColumnType actualType) {
        return INT_TYPES.contains(actualType);
    }
}
