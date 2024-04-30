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

import static java.time.format.DateTimeFormatter.ISO_LOCAL_DATE;
import static java.time.format.DateTimeFormatter.ISO_LOCAL_TIME;
import static org.apache.calcite.runtime.SqlFunctions.charLength;
import static org.apache.ignite.internal.sql.engine.prepare.IgniteSqlValidator.NUMERIC_FIELD_OVERFLOW_ERROR;
import static org.apache.ignite.lang.ErrorGroups.Sql.RUNTIME_ERR;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.MathContext;
import java.math.RoundingMode;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.ResolverStyle;
import java.util.TimeZone;
import java.util.UUID;
import org.apache.calcite.DataContext;
import org.apache.calcite.avatica.util.ByteString;
import org.apache.calcite.avatica.util.DateTimeUtils;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.linq4j.function.NonDeterministic;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.runtime.SqlFunctions;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.ignite.internal.lang.IgniteStringBuilder;
import org.apache.ignite.internal.sql.engine.sql.fun.IgniteSqlOperatorTable;
import org.apache.ignite.internal.sql.engine.type.IgniteTypeSystem;
import org.apache.ignite.internal.sql.engine.util.Commons;
import org.apache.ignite.internal.sql.engine.util.TypeUtils;
import org.apache.ignite.sql.SqlException;
import org.jetbrains.annotations.Nullable;

/**
 * Ignite SQL functions.
 */
public class IgniteSqlFunctions {
    private static final DateTimeFormatter ISO_LOCAL_DATE_TIME_EX;
    private static final RoundingMode roundingMode = RoundingMode.HALF_UP;

    private static final BigDecimal MAX_INT = new BigDecimal(Integer.MAX_VALUE);
    private static final BigDecimal MIN_INT = new BigDecimal(Integer.MIN_VALUE);

    static {
        ISO_LOCAL_DATE_TIME_EX = new DateTimeFormatterBuilder()
                .parseCaseInsensitive()
                .append(ISO_LOCAL_DATE)
                .appendLiteral(' ')
                .append(ISO_LOCAL_TIME)
                .toFormatter();
    }

    /**
     * Default constructor.
     */
    private IgniteSqlFunctions() {
        // No-op.
    }

    /** Just a stub. Validates Date\Time literal, still use calcite implementation for numeric representation.
     * Otherwise need to fix {@code DateTimeUtils#unixTimestampToString} usage additionally.
     */
    public static long timestampStringToNumeric(String dtStr) {
        dtStr = dtStr.trim();
        // "YYYY-MM-dd HH:mm:ss.ninenanos"
        if (dtStr.length() > 29) {
            dtStr = dtStr.substring(0, 29);
        }

        LocalDateTime.parse(dtStr, ISO_LOCAL_DATE_TIME_EX.withResolverStyle(ResolverStyle.STRICT));

        return DateTimeUtils.timestampStringToUnixDate(dtStr);
    }

    /** CAST(DECIMAL AS VARCHAR). */
    public static String toString(BigDecimal x) {
        return x == null ? null : x.toPlainString();
    }

    /** CAST(VARBINARY AS VARCHAR). */
    public static String toString(ByteString b) {
        return b == null ? null : new String(b.getBytes(), Commons.typeFactory().getDefaultCharset());
    }

    /** LENGTH(VARBINARY|VARCHAR). */
    public static int length(Object b) {
        return b instanceof ByteString ? SqlFunctions.octetLength((ByteString) b) : charLength((String) b);
    }

    /** OCTET_LENGTH(VARBINARY). */
    public static int octetLength(ByteString s) {
        return s.length();
    }

    /** OCTET_LENGTH(VARCHAR). */
    public static int octetLength(String s) {
        return s.getBytes().length;
    }

    // SQL ROUND function

    /** SQL {@code ROUND} operator applied to byte values. */
    public static byte sround(byte b0) {
        return (byte) sround(b0, 0);
    }

    /** SQL {@code ROUND} operator applied to byte values. */
    public static byte sround(byte b0, int b1) {
        return (byte) sround((int) b0, b1);
    }

    /** SQL {@code ROUND} operator applied to short values. */
    public static byte sround(short b0) {
        return (byte) sround(b0, 0);
    }

    /** SQL {@code ROUND} operator applied to short values. */
    public static short sround(short b0, int b1) {
        return (short) sround((int) b0, b1);
    }

    /** SQL {@code ROUND} operator applied to int values. */
    public static int sround(int b0) {
        return sround(b0, 0);
    }

    /** SQL {@code ROUND} operator applied to int values. */
    public static int sround(int b0, int b1) {
        if (b1 == 0) {
            return b0;
        } else if (b1 > 0) {
            return b0;
        } else {
            return (int) sround((long) b0, b1);
        }
    }

    /** SQL {@code ROUND} operator applied to long values. */
    public static long sround(long b0) {
        return sround(b0, 0);
    }

    /** SQL {@code ROUND} operator applied to long values. */
    public static long sround(long b0, int b1) {
        if (b1 == 0) {
            return b0;
        } else if (b1 > 0) {
            return b0;
        } else {
            long abs = (long) Math.pow(10, Math.abs(b1));
            return divide(b0, abs, RoundingMode.HALF_UP) * abs;
        }
    }

    /** SQL {@code ROUND} operator applied to double values. */
    public static double sround(double b0) {
        return sround(BigDecimal.valueOf(b0)).doubleValue();
    }

    /** SQL {@code ROUND} operator applied to double values. */
    public static double sround(double b0, int b1) {
        return sround(BigDecimal.valueOf(b0), b1).doubleValue();
    }

    /** SQL {@code ROUND} operator applied to float values. */
    public static float sround(float b0) {
        return sround(BigDecimal.valueOf(b0)).floatValue();
    }

    /** SQL {@code ROUND} operator applied to float values. */
    public static float sround(float b0, int b1) {
        return sround(BigDecimal.valueOf(b0), b1).floatValue();
    }

    /** SQL {@code ROUND} operator applied to BigDecimal values. */
    public static BigDecimal sround(BigDecimal b0) {
        return b0.setScale(0, RoundingMode.HALF_UP);
    }

    /** SQL {@code ROUND} operator applied to BigDecimal values. */
    public static BigDecimal sround(BigDecimal b0, int b1) {
        int originalScale = b0.scale();

        if (b1 >= originalScale) {
            return b0;
        }

        BigDecimal roundedValue = b0.setScale(b1, RoundingMode.HALF_UP);
        // Pad with zeros to match the original scale
        return roundedValue.setScale(originalScale, RoundingMode.UNNECESSARY);
    }

    /** Returns {@link Integer} bounded value. */
    private static int normalizeRegardingInt(BigDecimal num) {
        int res;

        if (num.compareTo(MAX_INT) >= 0) {
            res = Integer.MAX_VALUE;
        } else if (num.compareTo(MIN_INT) <= 0) {
            res = Integer.MIN_VALUE;
        } else {
            res = num.intValue();
        }

        return res;
    }

    /** SQL SUBSTRING(string FROM ...) function. */
    public static String substring(String c, int s) {
        if (s <= 1) {
            return c;
        }

        return SqlFunctions.substring(c, s);
    }

    /** SQL SUBSTRING(string FROM ...) function. */
    public static String substring(String c, BigDecimal s) {
        if (s.compareTo(BigDecimal.ONE) <= 0) {
            return c;
        }

        int s0 = normalizeRegardingInt(s);
        return SqlFunctions.substring(c, s0);
    }

    /** SQL SUBSTRING(string FROM ...) function. */
    public static String substring(String c, int s, int l) {
        return SqlFunctions.substring(c, s, l);
    }

    /** SQL SUBSTRING(string FROM ...) function. */
    public static String substring(String c, int s, BigDecimal l) {
        if (s < 0) {
            if (l.signum() > 0) {
                l = l.add(BigDecimal.valueOf(s));
                return substring(c, 0, l);
            }
        }
        int l0 = normalizeRegardingInt(l);
        return SqlFunctions.substring(c, s, l0);
    }

    /** SQL SUBSTRING(string FROM ...) function. */
    public static String substring(String c, BigDecimal s, BigDecimal l) {
        if (s.signum() < 0) {
            if (l.signum() > 0) {
                l = l.add(s);
                return substring(c, 0, l);
            }
        }
        int s0 = normalizeRegardingInt(s);
        int l0 = normalizeRegardingInt(l);
        return SqlFunctions.substring(c, s0, l0);
    }

    // TRUNCATE function

    /** SQL {@code TRUNCATE} operator applied to byte values. */
    public static byte struncate(byte b0) {
        return (byte) struncate(b0, 0);
    }

    /** SQL {@code TRUNCATE} operator applied to byte values. */
    public static byte struncate(byte b0, int b1) {
        return (byte) struncate((int) b0, b1);
    }

    /** SQL {@code TRUNCATE} operator applied to short values. */
    public static byte struncate(short b0) {
        return (byte) struncate(b0, 0);
    }

    /** SQL {@code TRUNCATE} operator applied to short values. */
    public static short struncate(short b0, int b1) {
        return (short) struncate((int) b0, b1);
    }

    /** SQL {@code TRUNCATE} operator applied to int values. */
    public static int struncate(int b0) {
        return sround(b0, 0);
    }

    /** SQL {@code TRUNCATE} operator applied to int values. */
    public static int struncate(int b0, int b1) {
        if (b1 == 0) {
            return b0;
        } else if (b1 > 0) {
            return b0;
        } else {
            return (int) struncate((long) b0, b1);
        }
    }

    /** SQL {@code TRUNCATE} operator applied to long values. */
    public static long struncate(long b0) {
        return sround(b0, 0);
    }

    /** SQL {@code TRUNCATE} operator applied to long values. */
    public static long struncate(long b0, int b1) {
        if (b1 == 0) {
            return b0;
        } else if (b1 > 0) {
            return b0;
        } else {
            long abs = (long) Math.pow(10, Math.abs(b1));
            return divide(b0, abs, RoundingMode.DOWN) * abs;
        }
    }

    /** SQL {@code TRUNCATE} operator applied to double values. */
    public static double struncate(double b0) {
        return struncate(BigDecimal.valueOf(b0)).doubleValue();
    }

    /** SQL {@code TRUNCATE} operator applied to double values. */
    public static double struncate(double b0, int b1) {
        return struncate(BigDecimal.valueOf(b0), b1).doubleValue();
    }

    /** SQL {@code TRUNCATE} operator applied to float values. */
    public static float struncate(float b0) {
        return struncate(BigDecimal.valueOf(b0)).floatValue();
    }

    /** SQL {@code TRUNCATE} operator applied to float values. */
    public static float struncate(float b0, int b1) {
        return struncate(BigDecimal.valueOf(b0), b1).floatValue();
    }

    /** SQL {@code TRUNCATE} operator applied to BigDecimal values. */
    public static BigDecimal struncate(BigDecimal b0) {
        return b0.setScale(0, RoundingMode.DOWN);
    }

    /** SQL {@code TRUNCATE} operator applied to BigDecimal values. */
    public static BigDecimal struncate(BigDecimal b0, int b1) {
        int originalScale = b0.scale();

        if (b1 >= originalScale) {
            return b0;
        }

        BigDecimal roundedValue = b0.setScale(b1, RoundingMode.DOWN);
        // Pad with zeros to match the original scale
        return roundedValue.setScale(originalScale, RoundingMode.UNNECESSARY);
    }

    /** CAST(DOUBLE AS DECIMAL). */
    public static BigDecimal toBigDecimal(double val, int precision, int scale) {
        return toBigDecimal((Double) val, precision, scale);
    }

    /** CAST(FLOAT AS DECIMAL). */
    public static BigDecimal toBigDecimal(float val, int precision, int scale) {
        return toBigDecimal((Float) val, precision, scale);
    }

    /** CAST(java long AS DECIMAL). */
    public static BigDecimal toBigDecimal(long val, int precision, int scale) {
        BigDecimal decimal = BigDecimal.valueOf(val);
        return toBigDecimal(decimal, precision, scale);
    }

    /** CAST(INT AS DECIMAL). */
    public static BigDecimal toBigDecimal(int val, int precision, int scale) {
        BigDecimal decimal = new BigDecimal(val);
        return toBigDecimal(decimal, precision, scale);
    }

    /** CAST(java short AS DECIMAL). */
    public static BigDecimal toBigDecimal(short val, int precision, int scale) {
        BigDecimal decimal = new BigDecimal(val);
        return toBigDecimal(decimal, precision, scale);
    }

    /** CAST(java byte AS DECIMAL). */
    public static BigDecimal toBigDecimal(byte val, int precision, int scale) {
        BigDecimal decimal = new BigDecimal(val);
        return toBigDecimal(decimal, precision, scale);
    }

    /** CAST(BOOL AS DECIMAL). */
    public static BigDecimal toBigDecimal(boolean val, int precision, int scale) {
        throw new UnsupportedOperationException();
    }

    /** CAST(VARCHAR AS DECIMAL). */
    public static BigDecimal toBigDecimal(String s, int precision, int scale) {
        if (s == null) {
            return null;
        }
        BigDecimal decimal = new BigDecimal(s.trim());
        return toBigDecimal(decimal, precision, scale);
    }

    /** Cast object depending on type to DECIMAL. */
    public static BigDecimal toBigDecimal(Object o, int precision, int scale) {
        if (o == null) {
            return null;
        }

        if (o instanceof Boolean) {
            throw new UnsupportedOperationException();
        }

        return o instanceof Number ? toBigDecimal((Number) o, precision, scale)
                : toBigDecimal(o.toString(), precision, scale);
    }

    /**
     * Converts the given {@code Number} to a decimal with the given {@code precision} and {@code scale}
     * according to SQL spec for CAST specification: General Rules, 8.
     */
    public static BigDecimal toBigDecimal(Number value, int precision, int scale) {
        assert precision > 0 : "Invalid precision: " + precision;

        if (value == null) {
            return null;
        }

        int defaultPrecision = IgniteTypeSystem.INSTANCE.getDefaultPrecision(SqlTypeName.DECIMAL);

        if (precision == defaultPrecision) {
            BigDecimal dec = convertToBigDecimal(value);
            // This branch covers at least one known case: access to dynamic parameter from context.
            // In this scenario precision = DefaultTypePrecision, because types for dynamic params
            // are created by toSql(createType(param.class)).
            return dec;
        }

        if (value.longValue() == 0) {
            return processFractionData(value, precision, scale);
        } else {
            return processValueWithIntegralPart(value, precision, scale);
        }
    }

    /**
     * Division function for REDUCE phase of AVG aggregate. Precision and scale is only used by type inference
     * (see {@link IgniteSqlOperatorTable#DECIMAL_DIVIDE}, their values are ignored at runtime.
     */
    public static BigDecimal decimalDivide(BigDecimal sum, BigDecimal cnt, int p, int s) {
        return sum.divide(cnt, MathContext.DECIMAL64);
    }

    private static BigDecimal processValueWithIntegralPart(Number value, int precision, int scale) {
        BigDecimal dec = convertToBigDecimal(value);

        if (scale > precision) {
            throw new SqlException(RUNTIME_ERR, NUMERIC_FIELD_OVERFLOW_ERROR);
        } else {
            int currentSignificantDigits = dec.precision() - dec.scale();
            int expectedSignificantDigits = precision - scale;

            if (currentSignificantDigits > expectedSignificantDigits) {
                throw new SqlException(RUNTIME_ERR, NUMERIC_FIELD_OVERFLOW_ERROR);
            }
        }

        return dec.setScale(scale, roundingMode);
    }

    private static BigDecimal processFractionData(Number value, int precision, int scale) {
        BigDecimal num = convertToBigDecimal(value);

        if (num.unscaledValue().equals(BigInteger.ZERO)) {
            return num.setScale(scale, RoundingMode.UNNECESSARY);
        }

        // skip all fractional part after scale
        BigDecimal num0 = num.movePointRight(scale).setScale(0, RoundingMode.DOWN);

        int numPrecision = Math.min(num0.precision(), scale);

        if (numPrecision > precision) {
            throw new SqlException(RUNTIME_ERR, NUMERIC_FIELD_OVERFLOW_ERROR);
        }

        return num.setScale(scale, roundingMode);
    }

    private static BigDecimal convertToBigDecimal(Number value) {
        BigDecimal dec;
        if (value instanceof Float) {
            dec = new BigDecimal(value.floatValue());
        } else if (value instanceof Double) {
            dec = new BigDecimal(value.doubleValue());
        } else if (value instanceof BigDecimal) {
            dec = (BigDecimal) value;
        } else if (value instanceof BigInteger) {
            dec = new BigDecimal((BigInteger) value);
        } else {
            dec = new BigDecimal(value.longValue());
        }

        return dec;
    }

    /** CAST(VARCHAR AS VARBINARY). */
    public static ByteString toByteString(String s) {
        return s == null ? null : new ByteString(s.getBytes(Commons.typeFactory().getDefaultCharset()));
    }

    public static int currentTime(DataContext ctx) {
        return (int) TypeUtils.toInternal(LocalTime.now(), LocalTime.class);
    }

    /** LEAST2. */
    public static @Nullable Object least2(Object arg0, Object arg1) {
        return leastOrGreatest(true, arg0, arg1);
    }

    /** GREATEST2. */
    public static @Nullable Object greatest2(Object arg0, Object arg1) {
        return leastOrGreatest(false, arg0, arg1);
    }

    /** Generates a random UUID and converts it to string. **/
    @NonDeterministic
    public static String genRandomUuid() {
        return UUID.randomUUID().toString();
    }

    /** Returns the second argument and ignores the first. */
    public static Object consumeFirstArgument(Object args0, Object args1) {
        return args1;
    }

    /** Returns the timestamp value minus the offset of the specified timezone. */
    public static Long subtractTimeZoneOffset(long timestamp, TimeZone timeZone) {
        // A second offset calculation is required to handle DST transition period correctly.
        int offset = timeZone.getOffset(timestamp - timeZone.getOffset(timestamp));

        return timestamp - offset;
    }

    /**
     * Helper for CAST({timestamp} AS VARCHAR(n)).
     *
     * <p>Note: this method is a copy of the avatica {@link DateTimeUtils#unixTimestampToString(long, int)} method,
     *          with the only difference being that it does not add trailing zeros.
     */
    public static String unixTimestampToString(long timestamp, int precision) {
        IgniteStringBuilder buf = new IgniteStringBuilder(17);
        int date = (int) (timestamp / DateTimeUtils.MILLIS_PER_DAY);
        int time = (int) (timestamp % DateTimeUtils.MILLIS_PER_DAY);

        if (time < 0) {
            --date;

            time += (int) DateTimeUtils.MILLIS_PER_DAY;
        }

        buf.app(DateTimeUtils.unixDateToString(date)).app(' ');

        unixTimeToString(buf, time, precision);

        return buf.toString();
    }

    /**
     * Helper for CAST({time} AS VARCHAR(n)).
     *
     * <p>Note: this method is a copy of the avatica {@link DateTimeUtils#unixTimestampToString(long, int)} method,
     *          with the only difference being that it does not add trailing zeros.
     */
    public static String unixTimeToString(int time, int precision) {
        IgniteStringBuilder buf = new IgniteStringBuilder(8 + (precision > 0 ? 1 + precision : 0));

        unixTimeToString(buf, time, precision);

        return buf.toString();
    }

    private static void unixTimeToString(IgniteStringBuilder buf, int time, int precision) {
        int h = time / 3600000;
        int time2 = time % 3600000;
        int m = time2 / 60000;
        int time3 = time2 % 60000;
        int s = time3 / 1000;
        int ms = time3 % 1000;

        buf.app((char) ('0' + (h / 10) % 10))
                .app((char) ('0' + h % 10))
                .app(':')
                .app((char) ('0' + (m / 10) % 10))
                .app((char) ('0' + m % 10))
                .app(':')
                .app((char) ('0' + (s / 10) % 10))
                .app((char) ('0' + s % 10));

        if (precision == 0 || ms == 0) {
            return;
        }

        buf.app('.');
        do {
            buf.app((char) ('0' + (ms / 100)));

            ms = ms % 100;
            ms = ms * 10;
            --precision;
        } while (ms > 0 && precision > 0);
    }

    private static @Nullable Object leastOrGreatest(boolean least, Object arg0, Object arg1) {
        if (arg0 == null || arg1 == null) {
            return null;
        }

        assert arg0 instanceof Comparable && arg1 instanceof Comparable :
                "Unexpected class [arg0=" + arg0.getClass().getName() + ", arg1=" + arg1.getClass().getName() + ']';

        if (((Comparable<Object>) arg0).compareTo(arg1) < 0) {
            return least ? arg0 : arg1;
        } else {
            return least ? arg1 : arg0;
        }
    }

    /**
     * Dummy table to implement the SYSTEM_RANGE function.
     */
    private static class RangeTable implements ScannableTable {
        /** Start of the range. */
        private final Object rangeStart;

        /** End of the range. */
        private final Object rangeEnd;

        /** Increment. */
        private final Object increment;

        /**
         * Note: {@code Object} arguments required here due to: 1. {@code NULL} arguments need to be supported, so we
         * can't use {@code long} arguments type. 2. {@code Integer} and other numeric classes can be converted to
         * {@code long} type by java, but can't be converted to {@code Long} type, so we can't use {@code Long}
         * arguments type either. Instead, we accept {@code Object} arguments type and try to convert valid types to
         * {@code long}.
         */
        RangeTable(Object rangeStart, Object rangeEnd, Object increment) {
            this.rangeStart = rangeStart;
            this.rangeEnd = rangeEnd;
            this.increment = increment;
        }

        /** {@inheritDoc} */
        @Override
        public RelDataType getRowType(RelDataTypeFactory typeFactory) {
            return typeFactory.builder().add("X", SqlTypeName.BIGINT).build();
        }

        /** {@inheritDoc} */
        @Override
        public Enumerable<@Nullable Object[]> scan(DataContext root) {
            if (rangeStart == null || rangeEnd == null || increment == null) {
                return Linq4j.emptyEnumerable();
            }

            long rangeStart = convertToLongArg(this.rangeStart, "rangeStart");
            long rangeEnd = convertToLongArg(this.rangeEnd, "rangeEnd");
            long increment = convertToLongArg(this.increment, "increment");

            if (increment == 0L) {
                throw new IllegalArgumentException("Increment can't be 0");
            }

            return new AbstractEnumerable<>() {
                @Override
                public Enumerator<@Nullable Object[]> enumerator() {
                    return new Enumerator<>() {
                        long cur = rangeStart - increment;

                        @Override
                        public Object[] current() {
                            return new Object[]{cur};
                        }

                        @Override
                        public boolean moveNext() {
                            cur += increment;

                            return increment > 0L ? cur <= rangeEnd : cur >= rangeEnd;
                        }

                        @Override
                        public void reset() {
                            cur = rangeStart - increment;
                        }

                        @Override
                        public void close() {
                            // No-op.
                        }
                    };
                }
            };
        }

        private long convertToLongArg(Object val, String name) {
            if (val instanceof Byte || val instanceof Short || val instanceof Integer || val instanceof Long) {
                return ((Number) val).longValue();
            }

            throw new IllegalArgumentException("Unsupported argument type [arg=" + name
                    + ", type=" + val.getClass().getSimpleName() + ']');
        }

        /** {@inheritDoc} */
        @Override
        public Statistic getStatistic() {
            throw new UnsupportedOperationException();
        }

        /** {@inheritDoc} */
        @Override
        public Schema.TableType getJdbcTableType() {
            return Schema.TableType.TABLE;
        }

        /** {@inheritDoc} */
        @Override
        public boolean isRolledUp(String column) {
            return false;
        }

        /** {@inheritDoc} */
        @Override
        public boolean rolledUpColumnValidInsideAgg(String column, SqlCall call,
                SqlNode parent, CalciteConnectionConfig cfg) {
            return true;
        }
    }

    private static long divide(long p, long q, RoundingMode mode) {
        // Stripped down version of guava's LongMath::divide.

        long div = p / q; // throws if q == 0
        long rem = p - q * div; // equals p % q

        int signum = 1 | (int) ((p ^ q) >> (Long.SIZE - 1));
        boolean increment;
        switch (mode) {
            case HALF_DOWN:
            case HALF_UP:
                long absRem = Math.abs(rem);
                long cmpRemToHalfDivisor = absRem - (Math.abs(q) - absRem);

                if (cmpRemToHalfDivisor == 0) { // exactly on the half mark
                    increment = mode == RoundingMode.HALF_UP;
                } else {
                    increment = cmpRemToHalfDivisor > 0; // closer to the UP value
                }
                break;
            case DOWN:
                increment = false;
                break;
            default:
                throw new AssertionError();
        }

        return increment ? div + signum : div;
    }
}
