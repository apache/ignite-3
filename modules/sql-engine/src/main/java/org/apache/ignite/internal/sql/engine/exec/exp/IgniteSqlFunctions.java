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

import static org.apache.calcite.avatica.util.DateTimeUtils.dateStringToUnixDate;
import static org.apache.calcite.runtime.SqlFunctions.charLength;
import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;
import static org.apache.ignite.internal.sql.engine.prepare.IgniteSqlValidator.NUMERIC_FIELD_OVERFLOW_ERROR;
import static org.apache.ignite.lang.ErrorGroups.Sql.RUNTIME_ERR;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.time.LocalTime;
import java.time.Year;
import java.util.TimeZone;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.calcite.DataContext;
import org.apache.calcite.DataContext.Variable;
import org.apache.calcite.avatica.util.ByteString;
import org.apache.calcite.avatica.util.DateTimeUtils;
import org.apache.calcite.linq4j.function.NonDeterministic;
import org.apache.calcite.runtime.SqlFunctions;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.ignite.internal.lang.IgniteStringBuilder;
import org.apache.ignite.internal.schema.SchemaUtils;
import org.apache.ignite.internal.sql.engine.util.Commons;
import org.apache.ignite.internal.sql.engine.util.IgniteMath;
import org.apache.ignite.internal.sql.engine.util.TypeUtils;
import org.apache.ignite.internal.type.NativeTypeSpec;
import org.apache.ignite.sql.SqlException;
import org.jetbrains.annotations.Nullable;

/**
 * Ignite SQL functions.
 */
public class IgniteSqlFunctions {
    public static final long TIMESTAMP_MIN_INTERNAL = (long) TypeUtils.toInternal(SchemaUtils.DATETIME_MIN, NativeTypeSpec.DATETIME);
    public static final long TIMESTAMP_MAX_INTERNAL = (long) TypeUtils.toInternal(SchemaUtils.DATETIME_MAX, NativeTypeSpec.DATETIME);

    private static final long TIMESTAMP_LTZ_MIN_INTERNAL = (long) TypeUtils.toInternal(SchemaUtils.TIMESTAMP_MIN, NativeTypeSpec.TIMESTAMP);
    private static final long TIMESTAMP_LTZ_MAX_INTERNAL = (long) TypeUtils.toInternal(SchemaUtils.TIMESTAMP_MAX, NativeTypeSpec.TIMESTAMP);

    private static final int DATE_MIN_INTERNAL = (int) TypeUtils.toInternal(SchemaUtils.DATE_MIN, NativeTypeSpec.DATE);
    private static final int DATE_MAX_INTERNAL = (int) TypeUtils.toInternal(SchemaUtils.DATE_MAX, NativeTypeSpec.DATE);

    /** Regex for time, HH:MM:SS. */
    private static final Pattern ISO_TIME_PATTERN =
            Pattern.compile("^([0-2]\\d):[0-5]\\d:[0-5]\\d(\\.\\d*)*$");

    /** Regex for date, YYYY-MM-DD. */
    private static final Pattern ISO_DATE_PATTERN =
            Pattern.compile("^(\\d{4})-([0]\\d|1[0-2])-([0-2]\\d|3[01])$");

    /**
     * Default constructor.
     */
    private IgniteSqlFunctions() {
        // No-op.
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

        if (value.longValue() == 0) {
            return processFractionData(value, precision, scale);
        } else {
            return processValueWithIntegralPart(value, precision, scale);
        }
    }

    /** Checks the boundaries of {@link SqlTypeName#DATE} value. */
    public static Integer toDateExact(Object object) {
        if (object == null) {
            return null;
        }

        assert object instanceof Integer : object.getClass();

        return toDateExact((int) object);
    }

    /** Checks the boundaries of {@link SqlTypeName#DATE} value. */
    public static Integer toDateExact(long longDate) {
        return toDateExact(Math.toIntExact(longDate));
    }

    /** Checks the boundaries of {@link SqlTypeName#DATE} value. */
    public static Integer toDateExact(int intDate) {
        if (intDate < DATE_MIN_INTERNAL || intDate > DATE_MAX_INTERNAL) {
            throw new SqlException(RUNTIME_ERR, SqlTypeName.DATE + " out of range");
        }

        return intDate;
    }

    /** Checks the boundaries of {@link SqlTypeName#TIMESTAMP} value. */
    public static Long toTimestampExact(Object object) {
        if (object == null) {
            return null;
        }

        assert object instanceof Long : object.getClass();

        return toTimestampExact((long) object);
    }

    /** Checks the boundaries of {@link SqlTypeName#TIMESTAMP} value. */
    public static long toTimestampExact(long ts) {
        if (ts < TIMESTAMP_MIN_INTERNAL || ts > TIMESTAMP_MAX_INTERNAL) {
            throw new SqlException(RUNTIME_ERR, SqlTypeName.TIMESTAMP + " out of range");
        }

        return ts;
    }

    /** Checks the boundaries of {@link SqlTypeName#TIMESTAMP_WITH_LOCAL_TIME_ZONE} value. */
    public static Long toTimestampLtzExact(Object object) {
        if (object == null) {
            return null;
        }

        assert object instanceof Long : object.getClass();

        return toTimestampLtzExact((long) object);
    }

    /** Checks the boundaries of {@link SqlTypeName#TIMESTAMP_WITH_LOCAL_TIME_ZONE} value. */
    public static long toTimestampLtzExact(long ts) {
        if (ts < TIMESTAMP_LTZ_MIN_INTERNAL || ts > TIMESTAMP_LTZ_MAX_INTERNAL) {
            throw new SqlException(RUNTIME_ERR, SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE + " out of range");
        }

        return ts;
    }

    // LN, LOG, LOG10, LOG2

    /** SQL {@code LOG(number, number2)} function applied to double values. */
    public static double log(double d0, double d1) {
        return Math.log(d0) / Math.log(d1);
    }

    /** SQL {@code LOG(number, number2)} function applied to
     * double and BigDecimal values. */
    public static double log(double d0, BigDecimal d1) {
        return Math.log(d0) / Math.log(d1.doubleValue());
    }

    /** SQL {@code LOG(number, number2)} function applied to
     * BigDecimal and double values. */
    public static double log(BigDecimal d0, double d1) {
        return Math.log(d0.doubleValue()) / Math.log(d1);
    }

    /** SQL {@code LOG(number, number2)} function applied to double values. */
    public static double log(BigDecimal d0, BigDecimal d1) {
        return Math.log(d0.doubleValue()) / Math.log(d1.doubleValue());
    }

    /** SQL {@code LOG10(number)} function applied to double values. */
    public static double log10(double d0) {
        return Math.log10(d0);
    }

    /** SQL {@code LOG10(number)} function applied to BigDecimal values. */
    public static double log10(BigDecimal d0) {
        return Math.log10(d0.doubleValue());
    }

    private static BigDecimal processValueWithIntegralPart(Number value, int precision, int scale) {
        BigDecimal dec = convertToBigDecimal(value);

        if (scale > precision) {
            throw numericOverflowError(precision, scale);
        } else {
            int currentSignificantDigits = dec.precision() - dec.scale();
            int expectedSignificantDigits = precision - scale;

            if (currentSignificantDigits > expectedSignificantDigits) {
                throw numericOverflowError(precision, scale);
            }
        }

        return dec.setScale(scale, IgniteMath.ROUNDING_MODE);
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
            throw numericOverflowError(precision, scale);
        }

        return num.setScale(scale, IgniteMath.ROUNDING_MODE);
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

    private static SqlException numericOverflowError(int precision, int scale) {
        String maxVal;

        if (precision == scale) {
            maxVal = "1";
        } else {
            maxVal = format("10^{}", precision - scale);
        }

        String detail = format("A field with precision {}, scale {} must round to an absolute value less than {}.",
                precision, scale, maxVal
        );

        throw new SqlException(RUNTIME_ERR, NUMERIC_FIELD_OVERFLOW_ERROR + ". " + detail);
    }

    /** CAST(VARCHAR AS VARBINARY). */
    public static ByteString toByteString(String s) {
        return s == null ? null : new ByteString(s.getBytes(Commons.typeFactory().getDefaultCharset()));
    }

    public static int currentTime(DataContext ctx) {
        return (int) TypeUtils.toInternal(LocalTime.now(), NativeTypeSpec.TIME);
    }

    /** CURRENT_TIMESTAMP. */
    public static long currentTimeStamp(DataContext ctx) {
        return Variable.CURRENT_TIMESTAMP.get(ctx);
    }

    /** LEAST2. */
    public static @Nullable Object least2(Object arg0, Object arg1) {
        return leastOrGreatest(true, arg0, arg1);
    }

    /** GREATEST2. */
    public static @Nullable Object greatest2(Object arg0, Object arg1) {
        return leastOrGreatest(false, arg0, arg1);
    }

    /** Generates a random UUID. **/
    @NonDeterministic
    public static UUID randUuid() {
        return UUID.randomUUID();
    }

    /** Returns the second argument and ignores the first. */
    public static Object consumeFirstArgument(Object args0, Object args1) {
        return args1;
    }

    /** Returns the timestamp value minus the offset of the specified timezone. */
    public static Long subtractTimeZoneOffset(long timestamp, TimeZone timeZone) {
        // A second offset calculation is required to handle DST transition period correctly.
        int offset = timeZone.getOffset(timestamp - timeZone.getOffset(timestamp));

        // After adjusting to UTC, you need to make sure that the value matches the allowed values.
        return toTimestampLtzExact(timestamp - offset);
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
     * Converts timestamp string into unix timestamp.
     *
     * <p>Note: this method is a copy of the avatica {@link DateTimeUtils#timestampStringToUnixDate(String)} method,
     *          with the only difference that result is truncated to milliseconds without rounding.
     *
     * @param s Timestamp string.
     * @return Timestamp.
     */
    public static long timestampStringToUnixDate(String s) {
        try {
            final long d;
            final long t;
            s = s.trim();
            int space = s.indexOf(' ');
            if (space >= 0) {
                String datePart = s.substring(0, space);
                validateDate(datePart, s);
                d = dateStringToUnixDate(datePart);

                String timePart = s.substring(space + 1);
                validateTime(timePart, s);
                t = timeStringToUnixDate(timePart);
            } else {
                validateDate(s, s);
                d = dateStringToUnixDate(s);
                t = 0;
            }
            return d * DateTimeUtils.MILLIS_PER_DAY + t;
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(e.getMessage());
        }
    }

    /**
     * Converts time string into unix time.
     *
     * <p>Note: this method is a copy of the avatica {@link DateTimeUtils#timeStringToUnixDate(String)} method,
     *          with the only difference that result is truncated to milliseconds without rounding.
     *
     * @param v Time string.
     * @return Timestamp.
     */
    public static int timeStringToUnixDate(String v) {
        final int colon1 = v.indexOf(':');
        int hour;
        int minute;
        int second;
        int milli = 0;
        if (colon1 < 0) {
            hour = Integer.parseInt(v.trim());
            minute = 0;
            second = 0;
        } else {
            hour = Integer.parseInt(v.substring(0, colon1).trim());
            final int colon2 = v.indexOf(':', colon1 + 1);
            if (colon2 < 0) {
                minute = Integer.parseInt(v.substring(colon1 + 1).trim());
                second = 0;
            } else {
                minute = Integer.parseInt(v.substring(colon1 + 1, colon2).trim());
                int dot = v.indexOf('.', colon2);
                if (dot < 0) {
                    second = Integer.parseInt(v.substring(colon2 + 1).trim());
                } else {
                    second = Integer.parseInt(v.substring(colon2 + 1, dot).trim());
                    String fraction = v.substring(dot + 1).trim();
                    int multiplier = 100;
                    for (int i = 0; i < Math.min(fraction.length(), 3); i++) {
                        char c = fraction.charAt(i);
                        int x = c < '0' || c > '9' ? 0 : (c - '0');
                        milli += multiplier * x;
                        multiplier /= 10;
                    }
                }
            }
        }
        return hour * (int) DateTimeUtils.MILLIS_PER_HOUR
                + minute * (int) DateTimeUtils.MILLIS_PER_MINUTE
                + second * (int) DateTimeUtils.MILLIS_PER_SECOND
                + milli;
    }

    private static void validateTime(String time, String full) {
        Matcher matcher = ISO_TIME_PATTERN.matcher(time);
        if (matcher.find()) {
            int hour = Integer.parseInt(matcher.group(1));
            if (hour > 23) {
                throw fieldOutOfRange("HOUR", full);
            }
        } else {
            throw invalidType("TIME", full);
        }
    }

    private static void validateDate(String s, String full) {
        Matcher matcher = ISO_DATE_PATTERN.matcher(s);
        if (matcher.find()) {
            int year = Integer.parseInt(matcher.group(1));
            int month = Integer.parseInt(matcher.group(2));
            int day = Integer.parseInt(matcher.group(3));
            checkLegalDate(year, month, day, full);
        } else {
            throw invalidType("DATE", full);
        }
    }

    /** Check that the combination year, month, date forms a legal date. */
    private static void checkLegalDate(int year, int month, int day, String full) {
        if (day > daysInMonth(year, month)) {
            throw fieldOutOfRange("DAY", full);
        }
        if (month < 1 || month > 12) {
            throw fieldOutOfRange("MONTH", full);
        }
        if (year <= 0) {
            // Year 0 is not really a legal value.
            throw fieldOutOfRange("YEAR", full);
        }
    }

    /** Returns the number of days in a month in the proleptic Gregorian calendar
     * used by ISO-8601.
     *
     * <p>"Proleptic" means that we apply the calendar to dates before the
     * Gregorian calendar was invented (in 1582). Thus, years 0 and 1200 are
     * considered leap years, and 1500 is not. */
    private static int daysInMonth(int year, int month) {
        switch (month) {
            case 9:
            case 4:
            case 6:
            case 11:
                // Thirty days hath September,
                // April, June, and November,
                return 30;

            default:
                // All the rest have thirty-one,
                return 31;

            case 2:
                // Except February, twenty-eight days clear,
                // And twenty-nine in each leap year.
                return Year.isLeap(year) ? 29 : 28;
        }
    }

    private static IllegalArgumentException fieldOutOfRange(String field,
            String full) {
        return new IllegalArgumentException("Value of " + field
                + " field is out of range in '" + full + "'");
    }

    private static IllegalArgumentException invalidType(String type,
            String full) {
        return new IllegalArgumentException("Invalid " + type + " value, '"
                + full + "'");
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
