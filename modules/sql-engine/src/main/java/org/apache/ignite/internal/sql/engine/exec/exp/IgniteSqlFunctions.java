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

import static org.apache.calcite.runtime.SqlFunctions.charLength;
import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;
import static org.apache.ignite.internal.sql.engine.prepare.IgniteSqlValidator.NUMERIC_FIELD_OVERFLOW_ERROR;
import static org.apache.ignite.lang.ErrorGroups.Sql.RUNTIME_ERR;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.sql.Date;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.time.zone.ZoneRules;
import java.util.Objects;
import java.util.TimeZone;
import java.util.UUID;
import org.apache.calcite.avatica.util.ByteString;
import org.apache.calcite.linq4j.function.NonDeterministic;
import org.apache.calcite.runtime.SqlFunctions;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.ignite.internal.schema.SchemaUtils;
import org.apache.ignite.internal.sql.engine.util.Commons;
import org.apache.ignite.internal.sql.engine.util.IgniteMath;
import org.apache.ignite.internal.sql.engine.util.IgniteSqlDateTimeUtils;
import org.apache.ignite.internal.sql.engine.util.TypeUtils;
import org.apache.ignite.internal.sql.engine.util.format.SqlDateTimeFormatter;
import org.apache.ignite.sql.ColumnType;
import org.apache.ignite.sql.SqlException;
import org.jetbrains.annotations.Nullable;

/**
 * Ignite SQL functions.
 */
public class IgniteSqlFunctions {
    public static final long TIMESTAMP_MIN_INTERNAL = (long) TypeUtils.toInternal(SchemaUtils.DATETIME_MIN, ColumnType.DATETIME);
    public static final long TIMESTAMP_MAX_INTERNAL = (long) TypeUtils.toInternal(SchemaUtils.DATETIME_MAX, ColumnType.DATETIME);

    private static final long TIMESTAMP_LTZ_MIN_INTERNAL = (long) TypeUtils.toInternal(SchemaUtils.TIMESTAMP_MIN, ColumnType.TIMESTAMP);
    private static final long TIMESTAMP_LTZ_MAX_INTERNAL = (long) TypeUtils.toInternal(SchemaUtils.TIMESTAMP_MAX, ColumnType.TIMESTAMP);

    private static final int DATE_MIN_INTERNAL = (int) TypeUtils.toInternal(SchemaUtils.DATE_MIN, ColumnType.DATE);
    private static final int DATE_MAX_INTERNAL = (int) TypeUtils.toInternal(SchemaUtils.DATE_MAX, ColumnType.DATE);
    /** java.sql.Time is stored as the number of milliseconds since 1970/01/01. */
    private static final LocalDate JAVA_SQL_TIME_DATE = LocalDate.of(1970, 1, 1);

    /**
     * Default constructor.
     */
    private IgniteSqlFunctions() {
        // No-op.
    }

    /** CAST(DECIMAL AS VARCHAR). */
    public static @Nullable String toString(@Nullable BigDecimal x) {
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
        return sround(b0, 0);
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
        return struncate(b0, 0);
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
    @SuppressWarnings("PMD.UnusedFormalParameter")
    public static BigDecimal toBigDecimal(boolean val, int precision, int scale) {
        throw new UnsupportedOperationException();
    }

    /** CAST(VARCHAR AS DECIMAL). */
    public static @Nullable BigDecimal toBigDecimal(@Nullable String s, int precision, int scale) {
        if (s == null) {
            return null;
        }
        BigDecimal decimal = new BigDecimal(s.trim());
        return toBigDecimal(decimal, precision, scale);
    }

    /** Cast object depending on type to DECIMAL. */
    public static @Nullable BigDecimal toBigDecimal(@Nullable Object o, int precision, int scale) {
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
    public static @Nullable BigDecimal toBigDecimal(@Nullable Number value, int precision, int scale) {
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

    /** Adjusts precision of {@link SqlTypeName#TIME} value. */
    public static @Nullable Integer toTimeExact(@Nullable Object object, int precision) {
        if (object == null) {
            return null;
        }

        assert object instanceof Integer : object.getClass();

        return IgniteSqlDateTimeUtils.adjustTimeMillis((Integer) object, precision);
    }

    /** Adjusts precision of {@link SqlTypeName#TIME} value. */
    public static int toTimeExact(long val, int precision) {
        assert precision >= 0 : "Invalid precision: " + precision;

        return IgniteSqlDateTimeUtils.adjustTimeMillis(Math.toIntExact(val), precision);
    }

    /** Checks the boundaries of {@link SqlTypeName#DATE} value. */
    public static @Nullable Integer toDateExact(@Nullable Object object) {
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

    /** Adjusts precision and validates the boundaries of {@link SqlTypeName#TIMESTAMP} value. */
    public static @Nullable Long toTimestampExact(@Nullable Object object, int precision) {
        if (object == null) {
            return null;
        }

        assert object instanceof Long : object.getClass();

        return toTimestampExact((long) object, precision);
    }

    /** Adjusts precision and validates the boundaries of {@link SqlTypeName#TIMESTAMP} value. */
    public static long toTimestampExact(long ts, int precision) {
        assert precision >= 0 : "Invalid precision: " + precision;

        long verified = toTimestampExact(ts);

        return IgniteSqlDateTimeUtils.adjustTimestampMillis(verified, precision);
    }

    /** Checks the boundaries of {@link SqlTypeName#TIMESTAMP} value. */
    public static @Nullable Long toTimestampExact(@Nullable Object object) {
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

    /** Adjusts precision and validates the boundaries of {@link SqlTypeName#TIMESTAMP_WITH_LOCAL_TIME_ZONE} value. */
    public static @Nullable Long toTimestampLtzExact(@Nullable Object object, int precision) {
        if (object == null) {
            return null;
        }

        assert object instanceof Long : object.getClass();

        return toTimestampLtzExact((long) object, precision);
    }

    /** Adjusts precision and validates the boundaries of {@link SqlTypeName#TIMESTAMP_WITH_LOCAL_TIME_ZONE} value. */
    public static long toTimestampLtzExact(long ts, int precision) {
        assert precision >= 0 : "Invalid precision: " + precision;

        long verified = toTimestampLtzExact(ts);

        return IgniteSqlDateTimeUtils.adjustTimestampMillis(verified, precision);
    }

    /** Checks the boundaries of {@link SqlTypeName#TIMESTAMP_WITH_LOCAL_TIME_ZONE} value. */
    public static @Nullable Long toTimestampLtzExact(@Nullable Object object) {
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
    @SuppressWarnings("PMD.UnusedFormalParameter")
    public static Object consumeFirstArgument(Object args0, Object args1) {
        return args1;
    }

    /** Converts timestamp string into a timestamp with local time zone. */
    public static @Nullable Long toTimestampWithLocalTimeZone(@Nullable String v, String format, TimeZone timeZone) {
        if (v == null) {
            return null;
        }

        Objects.requireNonNull(format, "format");
        Objects.requireNonNull(timeZone, "timeZone");

        // TODO https://issues.apache.org/jira/browse/IGNITE-25320 reuse to improve performance.
        LocalDateTime dateTime = SqlDateTimeFormatter.timestampFormatter(format).parseTimestamp(v);
        Instant instant = dateTime.toInstant(ZoneOffset.UTC);

        // Adjust instant millis
        ZoneRules rules = timeZone.toZoneId().getRules();
        ZoneOffset offset = rules.getOffset(instant);
        Instant adjusted = instant.minus(offset.getTotalSeconds(), ChronoUnit.SECONDS);
        return adjusted.toEpochMilli();
    }

    /** Converts a date string into a date value. */
    public static @Nullable Integer toDate(@Nullable String v, String format) {
        if (v == null) {
            return null;
        }

        // TODO https://issues.apache.org/jira/browse/IGNITE-25320 reuse to improve performance.
        LocalDate date = SqlDateTimeFormatter.dateFormatter(format).parseDate(v);
        return SqlFunctions.toInt(Date.valueOf(date));
    }

    /** Converts a time string into a time value. */
    public static @Nullable Integer toTime(@Nullable String v, String format) {
        if (v == null) {
            return null;
        }

        // TODO https://issues.apache.org/jira/browse/IGNITE-25320 reuse to improve performance.
        LocalTime time = SqlDateTimeFormatter.timeFormatter(format).parseTime(v);
        Instant instant = time.atDate(JAVA_SQL_TIME_DATE).toInstant(ZoneOffset.UTC);
        return (int) instant.toEpochMilli();
    }

    /** Converts a timestamp string into a timestamp value. */
    public static @Nullable Long toTimestamp(@Nullable String v, String format) {
        if (v == null) {
            return null;
        }

        // TODO https://issues.apache.org/jira/browse/IGNITE-25320 reuse to improve performance.
        LocalDateTime ts = SqlDateTimeFormatter.timestampFormatter(format).parseTimestamp(v);
        Instant instant = ts.toInstant(ZoneOffset.UTC);
        return instant.toEpochMilli();
    }

    /** Converts a date value into a date string. */
    public static @Nullable String formatDate(String format, @Nullable Integer v) {
        if (v == null) {
            return null;
        }

        LocalDate date = LocalDate.ofEpochDay(v.longValue());
        return SqlDateTimeFormatter.dateFormatter(format).formatDate(date);
    }

    /** Converts a time value into a time string. */
    public static @Nullable String formatTime(String format, @Nullable Integer v) {
        if (v == null) {
            return null;
        }

        LocalTime time = LocalTime.ofInstant(Instant.ofEpochMilli(v.longValue()), ZoneOffset.UTC);
        // TODO https://issues.apache.org/jira/browse/IGNITE-25320 reuse to improve performance.
        return SqlDateTimeFormatter.timeFormatter(format).formatTime(time);
    }

    /** Converts a timestamp value into a timestamp string. */
    public static @Nullable String formatTimestamp(String format, @Nullable Long v) {
        if (v == null) {
            return null;
        }

        LocalDateTime dateTime = LocalDateTime.ofInstant(Instant.ofEpochMilli(v), ZoneOffset.UTC);
        // TODO https://issues.apache.org/jira/browse/IGNITE-25320 reuse to improve performance.
        return SqlDateTimeFormatter.timestampFormatter(format).formatTimestamp(dateTime, ZoneOffset.UTC);
    }

    /** Converts a timestamp value with local time zone into a timestamp string. */
    public static @Nullable String formatTimestampWithLocalTimeZone(String format, @Nullable Long v, TimeZone timeZone) {
        if (v == null) {
            return null;
        }

        Instant instant = Instant.ofEpochMilli(v);
        ZoneRules rules = timeZone.toZoneId().getRules();
        ZoneOffset offset = rules.getOffset(instant);
        LocalDateTime value = LocalDateTime.ofEpochSecond(instant.getEpochSecond(), instant.getNano(), offset);

        // TODO https://issues.apache.org/jira/browse/IGNITE-25320 reuse to improve performance.
        return SqlDateTimeFormatter.timestampFormatter(format).formatTimestamp(value, offset);
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

    /**
     * Computes the next lexicographically greater string by incrementing the rightmost character that is not at its maximum value.
     *
     * <p>This method is primarily used in conjunction with {@link #findPrefix(String, String)} to create upper bounds for efficient
     * range scans. Given a prefix extracted from a LIKE pattern, this method produces the smallest string that is lexicographically greater
     * than the prefix, enabling queries like {@code WHERE key >= prefix AND key < nextGreaterPrefix}.
     *
     * <p>If all characters are at maximum value ({@value Character#MAX_VALUE}), the method returns {@code null} because no greater string
     * exists within the Unicode character space.
     *
     * <p>Basic examples:
     * <pre>
     * nextGreaterPrefix("abc")            → "abd"
     * nextGreaterPrefix("test")           → "tesu"
     * nextGreaterPrefix("user")           → "uses"
     * nextGreaterPrefix("a")              → "b"
     * nextGreaterPrefix("z")              → "{"
     * nextGreaterPrefix("9")              → ":"
     * </pre>
     *
     * <p>Examples with maximum values:
     * <pre>
     * nextGreaterPrefix("abc\uFFFF")      → "abd"        (skip max char, increment 'c')
     * nextGreaterPrefix("a\uFFFF\uFFFF")  → "b"          (skip two max chars, increment 'a')
     * nextGreaterPrefix("test\uFFFF")     → "tesu"       (skip max char, increment 't')
     * nextGreaterPrefix("\uFFFF")         → null         (cannot increment)
     * nextGreaterPrefix("\uFFFF\uFFFF")   → null         (all chars at max)
     * </pre>
     *
     * <p>Edge cases:
     * <pre>
     * nextGreaterPrefix(null)             → null         (null input)
     * nextGreaterPrefix("")               → null         (empty string, no chars to increment)
     * nextGreaterPrefix("abc\uFFFF\uFFFF") → "abd"       (truncates all trailing max values)
     * </pre>
     *
     * <p>Properties:
     * <ul>
     *   <li>The result is always the <em>smallest</em> string greater than the input</li>
     *   <li>For any valid input {@code s}, {@code nextGreaterPrefix(s).compareTo(s) > 0}</li>
     *   <li>The result may be shorter than the input (trailing max-value chars are removed)</li>
     *   <li>Trailing {@code \uFFFF} characters are effectively truncated</li>
     * </ul>
     *
     * @param prefix The string to increment. If {@code null}, returns {@code null}.
     * @return A next lexicographically greater string, or {@code null} if the input is {@code null}, empty, or consists entirely of
     *         maximum-value characters ({@code \uFFFF}). The returned string is guaranteed to be the smallest string greater than the
     *         input.
     */
    public static @Nullable String nextGreaterPrefix(@Nullable String prefix) {
        if (prefix == null) {
            return null;
        }

        // Try to increment characters from right to left.
        for (int i = prefix.length() - 1; i >= 0; i--) {
            char c = prefix.charAt(i);

            // Check if we can increment this character.
            if (c < Character.MAX_VALUE) {
                // Increment and return
                return prefix.substring(0, i) + ((char) (c + 1));
            }

            // This character is already max, continue to previous character.
        }

        // All characters are at maximum value, or prefix is empty.
        return null; // Given prefix is the greatest.
    }

    /**
     * Extracts the literal prefix from a SQL LIKE pattern by identifying all characters before the first unescaped wildcard.
     *
     * <p>This method processes SQL LIKE patterns containing wildcards ({@code %} for any sequence, {@code _} for single character) and
     * returns the constant prefix that can be used for optimized range scans.
     *
     * <p>When an escape character is provided, it allows wildcards to be treated as literal characters. The escape character itself can
     * also be escaped to include it literally in the prefix.
     *
     * <p>Examples without escape character:
     * <pre>
     * findPrefix("user%", null)           → "user"
     * findPrefix("admin_123", null)       → "admin"
     * findPrefix("admin%123", null)       → "admin"
     * findPrefix("test", null)            → "test"
     * findPrefix("%anything", null)       → ""
     * findPrefix("_anything", null)       → ""
     * </pre>
     *
     * <p>Examples with escape character:
     * <pre>
     * findPrefix("user\\%", "\\")         → "user%"      (% is literal)
     * findPrefix("admin\\_", "\\")        → "admin_"     (_ is literal)
     * findPrefix("path\\\\dir", "\\")     → "path\\dir"  (\\ escapes to \)
     * findPrefix("test\\%end%", "\\")     → "test%end"   (first % escaped, second is wildcard)
     * findPrefix("a\\%b\\%c", "\\")       → "a%b%c"      (both % are literal)
     * findPrefix("value^%", "^")          → "value%"     (different escape char)
     * </pre>
     *
     * <p>Edge cases:
     * <pre>
     * findPrefix(null, null)              → null         (null pattern)
     * findPrefix("test", "")              → throws       (invalid escape length)
     * findPrefix("test", "ab")            → throws       (invalid escape length)
     * findPrefix("", null)                → ""           (empty pattern)
     * findPrefix("test\\", "\\")          → "test\\"     (escape at end treated as literal)
     * </pre>
     *
     * @param pattern The SQL LIKE pattern to analyze; may contain wildcards {@code %} and {@code _}. If {@code null}, returns
     *         {@code null}.
     * @param escape The escape character used to treat wildcards as literals; must be exactly one character long if provided. Use
     *         {@code null} if no escape character is defined.
     * @return A literal prefix of the pattern before the first unescaped wildcard, with all escape sequences resolved to their literal
     *         characters. Returns an empty string if the pattern starts with a wildcard. Returns {@code null} if the pattern is
     *         {@code null}.
     * @throws IllegalArgumentException If {@code escape} is not null and not exactly one character long.
     */
    public static @Nullable String findPrefix(@Nullable String pattern, @Nullable String escape) {
        if (pattern == null) {
            return null;
        }

        if (escape != null && escape.length() != 1) {
            throw new IllegalArgumentException("Invalid escape character '" + escape + "'.");
        }

        if (escape == null) {
            int cutPoint = findCutPoint(pattern);

            if (cutPoint == 0) {
                return "";
            }

            return pattern.substring(0, cutPoint);
        }

        return findPrefix(pattern, escape.charAt(0));
    }

    private static String findPrefix(String pattern, char escape) {
        StringBuilder prefix = new StringBuilder(pattern.length());
        int lastAppendEnd = 0;

        for (int i = 0; i < pattern.length(); i++) {
            char current = pattern.charAt(i);

            if (current == escape) {
                int nextCharIdx = i + 1;
                if (nextCharIdx < pattern.length()) {
                    char nextChar = pattern.charAt(nextCharIdx);

                    if (nextChar == escape || nextChar == '%' || nextChar == '_') {
                        // Append everything from lastAppendEnd to current position (excluding escape char)
                        if (lastAppendEnd < i) {
                            prefix.append(pattern, lastAppendEnd, i);
                        }

                        // Append the escaped character
                        prefix.append(nextChar);

                        i++; // Skip the next character
                        lastAppendEnd = i + 1; // Update to position after escaped char
                    }
                }

                continue;
            }

            if (current == '%' || current == '_') {
                // Found wildcard - append any remaining prefix and return
                if (lastAppendEnd < i) {
                    prefix.append(pattern, lastAppendEnd, i);
                }
                return prefix.toString();
            }
        }

        // No wildcards found - append any remaining characters
        if (lastAppendEnd < pattern.length()) {
            prefix.append(pattern, lastAppendEnd, pattern.length());
        }

        return prefix.toString();
    }

    private static int findCutPoint(String pattern) {
        for (int i = 0; i < pattern.length(); i++) {
            char current = pattern.charAt(i);

            if (current == '%' || current == '_') {
                return i;
            }
        }

        return pattern.length();
    }
}
