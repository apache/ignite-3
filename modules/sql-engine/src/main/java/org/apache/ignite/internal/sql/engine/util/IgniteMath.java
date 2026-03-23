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

package org.apache.ignite.internal.sql.engine.util;

import static org.apache.calcite.sql.type.SqlTypeName.BIGINT;
import static org.apache.calcite.sql.type.SqlTypeName.INTEGER;
import static org.apache.calcite.sql.type.SqlTypeName.SMALLINT;
import static org.apache.calcite.sql.type.SqlTypeName.TINYINT;
import static org.apache.ignite.lang.ErrorGroups.Sql.RUNTIME_ERR;

import java.math.BigDecimal;
import java.math.RoundingMode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.ignite.internal.sql.engine.sql.fun.IgniteSqlOperatorTable;
import org.apache.ignite.internal.sql.engine.type.IgniteTypeSystem;
import org.apache.ignite.sql.SqlException;
import org.jetbrains.annotations.Nullable;

/** Math operations with overflow checking. */
public class IgniteMath {
    private static final BigDecimal UPPER_LONG_BIG_DECIMAL = BigDecimal.valueOf(Long.MAX_VALUE).add(BigDecimal.ONE);
    private static final BigDecimal LOWER_LONG_BIG_DECIMAL = BigDecimal.valueOf(Long.MIN_VALUE).subtract(BigDecimal.ONE);
    private static final Double UPPER_LONG_DOUBLE = (double) Long.MAX_VALUE + 1;
    private static final Double LOWER_LONG_DOUBLE = (double) Long.MIN_VALUE - 1;
    private static final Float UPPER_LONG_FLOAT = (float) Long.MAX_VALUE + 1;
    private static final Float LOWER_LONG_FLOAT = (float) Long.MIN_VALUE - 1;

    private static final BigDecimal UPPER_DOUBLE_BIG_DECIMAL = new BigDecimal(String.valueOf(Double.MAX_VALUE));
    private static final BigDecimal LOWER_DOUBLE_BIG_DECIMAL = UPPER_DOUBLE_BIG_DECIMAL.negate();
    private static final BigDecimal UPPER_FLOAT_BIG_DECIMAL = new BigDecimal(String.valueOf(Float.MAX_VALUE));
    private static final BigDecimal LOWER_FLOAT_BIG_DECIMAL = UPPER_FLOAT_BIG_DECIMAL.negate();

    private static final double UPPER_FLOAT_DOUBLE = Double.parseDouble("" + Float.MAX_VALUE);
    private static final double LOWER_FLOAT_DOUBLE = Double.parseDouble("" + (-Float.MAX_VALUE));

    /** Decimal rounding mode. */
    public static final RoundingMode ROUNDING_MODE = IgniteTypeSystem.INSTANCE.roundingMode();

    /** Returns the sum of its arguments, throwing an exception if the result overflows an {@code long}. */
    public static long addExact(long x, long y) {
        long r = x + y;

        if (((x ^ r) & (y ^ r)) < 0) {
            throw outOfRangeForTypeException(BIGINT);
        }

        return r;
    }

    /** Returns the sum of its arguments, throwing an exception if the result overflows an {@code int}. */
    public static int addExact(int x, int y) {
        int r = x + y;

        if (((x ^ r) & (y ^ r)) < 0) {
            throw outOfRangeForTypeException(INTEGER);
        }

        return r;
    }

    /** Returns the sum of its arguments, throwing an exception if the result overflows an {@code short}. */
    public static short addExact(short x, short y) {
        int r = x + y;

        if (r != (short) r) {
            throw outOfRangeForTypeException(SMALLINT);
        }

        return (short) r;
    }

    /** Returns the sum of its arguments, throwing an exception if the result overflows an {@code byte}. */
    public static byte addExact(byte x, byte y) {
        int r = x + y;

        if (r != (byte) r) {
            throw outOfRangeForTypeException(TINYINT);
        }

        return (byte) r;
    }

    /** Returns the negation of the argument, throwing an exception if the result overflows an {@code long}. */
    public static long negateExact(long x) {
        long res = -x;

        if (x != 0 && x == res) {
            throw outOfRangeForTypeException(BIGINT);
        }

        return res;
    }

    /** Returns the negation of the argument, throwing an exception if the result overflows an {@code int}. */
    public static int negateExact(int x) {
        int res = -x;

        if (x != 0 && x == res) {
            throw outOfRangeForTypeException(INTEGER);
        }

        return res;
    }

    /** Returns the negation of the argument, throwing an exception if the result overflows an {@code short}. */
    public static short negateExact(short x) {
        int res = -x;

        if (res > Short.MAX_VALUE) {
            throw outOfRangeForTypeException(SMALLINT);
        }

        return (short) res;
    }

    /** Returns the negation of the argument, throwing an exception if the result overflows an {@code byte}. */
    public static byte negateExact(byte x) {
        int res = -x;

        if (res > Byte.MAX_VALUE) {
            throw outOfRangeForTypeException(TINYINT);
        }

        return (byte) res;
    }

    /** Returns the difference of the arguments, throwing an exception if the result overflows an {@code long}.*/
    public static long subtractExact(long x, long y) {
        long r = x - y;

        if (((x ^ y) & (x ^ r)) < 0) {
            throw outOfRangeForTypeException(BIGINT);
        }

        return r;
    }

    /** Returns the difference of the arguments, throwing an exception if the result overflows an {@code int}.*/
    public static int subtractExact(int x, int y) {
        int r = x - y;

        if (((x ^ y) & (x ^ r)) < 0) {
            throw outOfRangeForTypeException(INTEGER);
        }

        return r;
    }

    /** Returns the difference of the arguments, throwing an exception if the result overflows an {@code short}.*/
    public static short subtractExact(short x, short y) {
        int r = x - y;

        if (r != (short) r) {
            throw outOfRangeForTypeException(SMALLINT);
        }

        return (short) r;
    }

    /** Returns the difference of the arguments, throwing an exception if the result overflows an {@code byte}.*/
    public static byte subtractExact(byte x, byte y) {
        int r = x - y;

        if (r != (byte) r) {
            throw outOfRangeForTypeException(TINYINT);
        }

        return (byte) r;
    }

    /** Returns the product of the arguments, throwing an exception if the result overflows an {@code long}. */
    public static long multiplyExact(long x, long y) {
        long r = x * y;
        long ax = Math.abs(x);
        long ay = Math.abs(y);

        if ((ax | ay) >>> 31 != 0 && ((y != 0 && r / y != x) || (x == Long.MIN_VALUE && y == -1))) {
            throw outOfRangeForTypeException(BIGINT);
        }

        return r;
    }

    /** Returns the product of the arguments, throwing an exception if the result overflows an {@code int}. */
    public static int multiplyExact(int x, int y) {
        long r = (long) x * y;

        if ((int) r != r) {
            throw outOfRangeForTypeException(INTEGER);
        }

        return (int) r;
    }

    /** Returns the product of the arguments, throwing an exception if the result overflows an {@code short}. */
    public static short multiplyExact(short x, short y) {
        int r = x * y;

        if (r != (short) r) {
            throw outOfRangeForTypeException(SMALLINT);
        }

        return (short) r;
    }

    /** Returns the product of the arguments, throwing an exception if the result overflows an {@code byte}. */
    public static byte multiplyExact(byte x, byte y) {
        int r = x * y;

        if (r != (byte) r) {
            throw outOfRangeForTypeException(TINYINT);
        }

        return (byte) r;
    }

    /** Returns the quotient of the arguments, throwing an exception if the result overflows an {@code long}. */
    public static long divideExact(long x, long y) {
        if (y == -1) {
            return negateExact(x);
        }
        if (y == 0) {
            throwDivisionByZero();
        }

        return x / y;
    }

    /** Returns the quotient of the arguments, throwing an exception if the result overflows an {@code int}. */
    public static int divideExact(int x, int y) {
        if (y == -1) {
            return negateExact(x);
        }
        if (y == 0) {
            throwDivisionByZero();
        }

        return x / y;
    }

    /** Returns the quotient of the arguments, throwing an exception if the result overflows an {@code short}. */
    public static short divideExact(short x, short y) {
        if (y == -1) {
            return negateExact(x);
        }
        if (y == 0) {
            throwDivisionByZero();
        }

        return (short) (x / y);
    }

    /** Returns the quotient of the arguments, throwing an exception if the result overflows an {@code byte}. */
    public static byte divideExact(byte x, byte y) {
        if (y == -1) {
            return negateExact(x);
        }
        if (y == 0) {
            throwDivisionByZero();
        }

        return (byte) (x / y);
    }

    /**
     * Decimal division. Precision is only used by type inferenc, its value is ignored at runtime.
     * See {@link IgniteSqlOperatorTable#DECIMAL_DIVIDE}.
     */
    @SuppressWarnings("PMD.UnusedFormalParameter")
    public static @Nullable BigDecimal decimalDivide(@Nullable BigDecimal x, @Nullable BigDecimal y, int p, int s) {
        if (x == null || y == null) {
            return null;
        }
        return x.divide(y, s, ROUNDING_MODE);
    }

    private static void throwDivisionByZero() {
        throw new SqlException(RUNTIME_ERR, "Division by zero");
    }

    /** Cast value to {@code long}, throwing an exception if the result overflows an {@code long}. */
    public static long convertToLongExact(Number x) {
        checkNumberLongBounds(BIGINT, x);

        return x.longValue();
    }

    /** Cast value to {@code long}, throwing an exception if the result overflows an {@code long}. */
    public static long convertToLongExact(double x) {
        if (x >= UPPER_LONG_DOUBLE || x <= LOWER_LONG_DOUBLE) {
            throw outOfRangeForTypeException(BIGINT);
        }

        return (long) x;
    }

    /** Cast value to {@code long}, throwing an exception if the result overflows an {@code long}. */
    public static long convertToLongExact(String x) {
        try {
            BigDecimal decimal = new BigDecimal(x.strip());
            return convertToLongExact(decimal);
        } catch (NumberFormatException ex1) {
            throw invalidInputStringForTypeException(BIGINT, x);
        }
    }

    /** Cast value to {@code int}, throwing an exception if the result overflows an {@code int}. */
    public static int convertToIntExact(long x) {
        if ((int) x != x) {
            throw outOfRangeForTypeException(INTEGER);
        }

        return (int) x;
    }

    /** Cast value to {@code int}, throwing an exception if the result overflows an {@code int}. */
    public static int convertToIntExact(double x) {
        if (x >= (long) Integer.MAX_VALUE + 1 || x <= (long) Integer.MIN_VALUE - 1) {
            throw outOfRangeForTypeException(INTEGER);
        }

        return (int) x;
    }

    /** Cast value to {@code int}, throwing an exception if the result overflows an {@code int}. */
    public static int convertToIntExact(float x) {
        if (x >= (long) Integer.MAX_VALUE + 1 || x <= (long) Integer.MIN_VALUE - 1) {
            throw outOfRangeForTypeException(INTEGER);
        }

        return (int) x;
    }

    /** Cast value to {@code int}, throwing an exception if the result overflows an {@code int}. */
    public static int convertToIntExact(Number x) {
        checkNumberLongBounds(INTEGER, x);
        return convertToIntExact(x.longValue());
    }

    /** Cast value to {@code int}, throwing an exception if the result overflows an {@code int}. */
    public static int convertToIntExact(String x) {
        try {
            BigDecimal decimal = new BigDecimal(x.strip());
            return convertToIntExact(decimal);
        } catch (NumberFormatException ex1) {
            throw invalidInputStringForTypeException(INTEGER, x);
        }
    }

    /** Cast value to {@code short}, throwing an exception if the result overflows an {@code short}. */
    public static short convertToShortExact(long x) {
        if ((short) x != x) {
            throw outOfRangeForTypeException(SMALLINT);
        }

        return (short) x;
    }

    /** Cast value to {@code short}, throwing an exception if the result overflows an {@code short}. */
    public static short convertToShortExact(Number x) {
        checkNumberLongBounds(SMALLINT, x);
        return convertToShortExact(x.longValue());
    }

    /** Cast value to {@code short}, throwing an exception if the result overflows an {@code short}. */
    public static short convertToShortExact(double x) {
        if (x >= Short.MAX_VALUE + 1 || x <= Short.MIN_VALUE - 1) {
            throw outOfRangeForTypeException(SMALLINT);
        }

        return (short) x;
    }

    /** Cast value to {@code short}, throwing an exception if the result overflows an {@code short}. */
    public static short convertToShortExact(String x) {
        try {
            BigDecimal decimal = new BigDecimal(x.strip());
            return convertToShortExact(decimal);
        } catch (NumberFormatException ex1) {
            throw invalidInputStringForTypeException(SMALLINT, x);
        }
    }

    /** Cast value to {@code byte}, throwing an exception if the result overflows an {@code byte}. */
    public static byte convertToByteExact(double x) {
        if (x >= Byte.MAX_VALUE + 1 || x <= Byte.MIN_VALUE - 1) {
            throw outOfRangeForTypeException(TINYINT);
        }

        return (byte) x;
    }

    /** Cast value to {@code byte}, throwing an exception if the result overflows an {@code byte}. */
    public static byte convertToByteExact(Number x) {
        checkNumberLongBounds(TINYINT, x);
        return convertToByteExact(x.longValue());
    }

    /** Cast value to {@code byte}, throwing an exception if the result overflows an {@code byte}. */
    public static byte convertToByteExact(long x) {
        if ((byte) x != x) {
            throw outOfRangeForTypeException(TINYINT);
        }

        return (byte) x;
    }

    /** Cast value to {@code byte}, throwing an exception if the result overflows an {@code byte}. */
    public static byte convertToByteExact(String x) {
        try {
            BigDecimal decimal = new BigDecimal(x.strip());
            return convertToByteExact(decimal);
        } catch (NumberFormatException ex1) {
            throw invalidInputStringForTypeException(TINYINT, x);
        }
    }

    /** Cast value to {@code float}, throwing an exception if the result overflows. */
    public static float convertToFloatExact(Number x) {
        if (x instanceof BigDecimal) {
            BigDecimal value = (BigDecimal) x;

            if (value.compareTo(UPPER_FLOAT_BIG_DECIMAL) > 0) {
                throw outOfRangeForTypeException(SqlTypeName.REAL);
            }
            if (value.compareTo(LOWER_FLOAT_BIG_DECIMAL) < 0) {
                throw outOfRangeForTypeException(SqlTypeName.REAL);
            }

            return value.floatValue();
        } else {
            double v = x.doubleValue();

            if (!Double.isFinite(v)) {
                return x.floatValue();
            }

            if (v > UPPER_FLOAT_DOUBLE || v < LOWER_FLOAT_DOUBLE) {
                throw outOfRangeForTypeException(SqlTypeName.REAL);
            }

            return x.floatValue();
        }
    }

    /** Cast value to {@code double}, throwing an exception if the result overflows. */
    public static double convertToDoubleExact(Number x) {
        if (x instanceof BigDecimal) {
            BigDecimal value = (BigDecimal) x;
            if (value.compareTo(UPPER_DOUBLE_BIG_DECIMAL) > 0) {
                throw outOfRangeForTypeException(SqlTypeName.DOUBLE);
            }

            if (value.compareTo(LOWER_DOUBLE_BIG_DECIMAL) < 0) {
                throw outOfRangeForTypeException(SqlTypeName.DOUBLE);
            }

            return value.doubleValue();
        } else {
            return x.doubleValue();
        }
    }

    private static void checkNumberLongBounds(SqlTypeName type, Number x) {
        if (x instanceof BigDecimal) {
            if ((((BigDecimal) x).compareTo(UPPER_LONG_BIG_DECIMAL) < 0 && ((BigDecimal) x).compareTo(LOWER_LONG_BIG_DECIMAL) > 0)) {
                return;
            }
        } else if (x instanceof Double) {
            if ((((Double) x).compareTo(UPPER_LONG_DOUBLE) < 0 && ((Double) x).compareTo(LOWER_LONG_DOUBLE) > 0)) {
                return;
            }
        } else if (x instanceof Float) {
            if ((((Float) x).compareTo(UPPER_LONG_FLOAT) < 0 && ((Float) x).compareTo(LOWER_LONG_FLOAT) > 0)) {
                return;
            }
        } else {
            return;
        }

        throw outOfRangeForTypeException(type);
    }

    private static RuntimeException outOfRangeForTypeException(SqlTypeName type) {
        return new SqlException(RUNTIME_ERR, type.getName() + " out of range");
    }

    private static RuntimeException invalidInputStringForTypeException(SqlTypeName type, String value) {
        return new SqlException(RUNTIME_ERR, "Invalid input string for type " + type.getName() + ": \"" + value + "\"");
    }
}
