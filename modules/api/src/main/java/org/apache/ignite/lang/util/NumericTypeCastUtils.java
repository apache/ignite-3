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

package org.apache.ignite.lang.util;

/**
 * Utility methods for converting {@link Number} instances to Java primitive numeric types.
 *
 * <p>The following conversions are supported:
 * <ul>
 *     <li>Any integer type to any other integer type with range checks (e.g. {@link Long} to {@link Byte}
 *     may throw {@link ArithmeticException} if the value doesn't fit into {@link Byte} range).</li>
 *     <li>{@link Float} to {@link Double} are always allowed.</li>
 *     <li>{@link Double} to {@link Float} with precision checks. Throws {@link ArithmeticException}
 *     if the value cannot be represented as FLOAT without precision loss.</li>
 * </ul>
 */
public class NumericTypeCastUtils {
    /**
     * Casts a {@link Number} to {@code byte}.
     *
     * @param number Number to cast.
     * @return Byte primitive value.
     * @throws ClassCastException If the provided {@code number} is of unsupported type.
     * @throws ArithmeticException if value overflows {@code byte} range.
     */
    public static byte castToByte(Number number) {
        if (number instanceof Byte) {
            return (byte) number;
        }

        if (number instanceof Long || number instanceof Integer || number instanceof Short) {
            long longVal = number.longValue();
            byte byteVal = number.byteValue();

            if (longVal == byteVal) {
                return byteVal;
            }

            throw new ArithmeticException("Byte value overflow: " + number);
        }

        throw new ClassCastException(number.getClass() + " cannot be cast to " + byte.class);
    }

    /**
     * Casts a {@link Number} to {@code short}.
     *
     * @param number Number to cast.
     * @return Short primitive value.
     * @throws ClassCastException If the provided {@code number} is of unsupported type.
     * @throws ArithmeticException if value overflows {@code short} range.
     */
    public static short castToShort(Number number) {
        if (number instanceof Short) {
            return (short) number;
        }

        if (number instanceof Long || number instanceof Integer || number instanceof Byte) {
            long longVal = number.longValue();
            short shortVal = number.shortValue();

            if (longVal == shortVal) {
                return shortVal;
            }

            throw new ArithmeticException("Short value overflow: " + number);
        }

        throw new ClassCastException(number.getClass() + " cannot be cast to " + short.class);
    }

    /**
     * Casts a {@link Number} to {@code int}.
     *
     * @param number Number to cast.
     * @return Int primitive value.
     * @throws ClassCastException If the provided {@code number} is of unsupported type.
     * @throws ArithmeticException If the value overflows {@code int} range.
     */
    public static int castToInt(Number number) {
        if (number instanceof Integer) {
            return (int) number;
        }

        if (number instanceof Long || number instanceof Short || number instanceof Byte) {
            long longVal = number.longValue();
            int intVal = number.intValue();

            if (longVal == intVal) {
                return intVal;
            }

            throw new ArithmeticException("Int value overflow: " + number);
        }

        throw new ClassCastException(number.getClass() + " cannot be cast to " + int.class);
    }

    /**
     * Casts a {@link Number} to {@code long}.
     *
     * @param number Number to cast.
     * @return Long primitive value.
     * @throws ClassCastException If the provided {@code number} is of unsupported type.
     */
    public static long castToLong(Number number) {
        if (number instanceof Long || number instanceof Integer || number instanceof Short || number instanceof Byte) {
            return number.longValue();
        }

        throw new ClassCastException(number.getClass() + " cannot be cast to " + long.class);
    }

    /**
     * Casts a {@link Number} to {@code float}.
     *
     * @param number Number to cast.
     * @return Float primitive value.
     * @throws ClassCastException If the provided {@code number} is of unsupported type.
     * @throws ArithmeticException If the value overflows {@code float} range.
     */
    public static float castToFloat(Number number) {
        if (number instanceof Float) {
            return (float) number;
        }

        if (number instanceof Double) {
            double doubleVal = number.doubleValue();
            float floatVal = number.floatValue();

            //noinspection FloatingPointEquality
            if (doubleVal == floatVal || Double.isNaN(doubleVal)) {
                return floatVal;
            }

            throw new ArithmeticException("Float value overflow: " + number);
        }

        throw new ClassCastException(number.getClass() + " cannot be cast to " + float.class);
    }

    /**
     * Casts a {@link Number} to {@code double}.
     *
     * @param number Number to cast.
     * @return Double primitive value.
     * @throws ClassCastException If the provided {@code number} is of unsupported type.
     */
    public static double castToDouble(Number number) {
        if (number instanceof Double || number instanceof Float) {
            return number.doubleValue();
        }

        throw new ClassCastException(number.getClass() + " cannot be cast to " + double.class);
    }
}
