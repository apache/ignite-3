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
import static org.apache.calcite.runtime.SqlFunctions.octetLength;
import static org.apache.ignite.lang.ErrorGroups.Sql.QUERY_INVALID_ERR;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.time.DateTimeException;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.ResolverStyle;
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
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.ignite.internal.sql.engine.type.IgniteTypeSystem;
import org.apache.ignite.internal.sql.engine.util.Commons;
import org.apache.ignite.internal.sql.engine.util.TypeUtils;
import org.apache.ignite.lang.IgniteInternalException;
import org.apache.ignite.sql.SqlException;
import org.jetbrains.annotations.Nullable;

/**
 * Ignite SQL functions.
 */
public class IgniteSqlFunctions {
    private static final DateTimeFormatter ISO_LOCAL_DATE_TIME_EX;
    private static final String NUMERIC_FIELD_OVERFLOW_ERROR = "Numeric field overflow";

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

    /** SQL SYSTEM_RANGE(start, end) table function. */
    public static ScannableTable systemRange(Object rangeStart, Object rangeEnd) {
        return new RangeTable(rangeStart, rangeEnd, 1L);
    }

    /** SQL SYSTEM_RANGE(start, end, increment) table function. */
    public static ScannableTable systemRange(Object rangeStart, Object rangeEnd, Object increment) {
        return new RangeTable(rangeStart, rangeEnd, increment);
    }

    /** Just a stub. Validates Date\Time literal, still use calcite implementation for numeric representation.
     * Otherwise need to fix {@code DateTimeUtils#unixTimestampToString} usage additionally.
     */
    public static long timestampStringToNumeric(String dtStr) {
        try {
            return timestampStringToNumeric0(dtStr);
        } catch (DateTimeException e) {
            throw new IgniteInternalException(QUERY_INVALID_ERR, e.getMessage());
        }
    }

    private static long timestampStringToNumeric0(String dtStr) {
        dtStr = dtStr.trim();
        //"YYYY-MM-dd HH:mm:ss.ninenanos"
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
        return b instanceof ByteString ? octetLength((ByteString) b) : charLength((String) b);
    }

    private static BigDecimal setScale(int precision, int scale, BigDecimal decimal) {
        return precision == IgniteTypeSystem.INSTANCE.getDefaultPrecision(SqlTypeName.DECIMAL)
            ? decimal : decimal.setScale(scale, RoundingMode.HALF_UP);
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
        return convertDecimal(decimal, precision, scale);
    }

    /** CAST(INT AS DECIMAL). */
    public static BigDecimal toBigDecimal(int val, int precision, int scale) {
        BigDecimal decimal = new BigDecimal(val);
        return convertDecimal(decimal, precision, scale);
    }

    /** CAST(java short AS DECIMAL). */
    public static BigDecimal toBigDecimal(short val, int precision, int scale) {
        BigDecimal decimal = new BigDecimal(val);
        return convertDecimal(decimal, precision, scale);
    }

    /** CAST(java byte AS DECIMAL). */
    public static BigDecimal toBigDecimal(byte val, int precision, int scale) {
        BigDecimal decimal = new BigDecimal(val);
        return convertDecimal(decimal, precision, scale);
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
        return convertDecimal(decimal, precision, scale);
    }

    /** CAST(REAL AS DECIMAL). */
    public static BigDecimal toBigDecimal(Number num, int precision, int scale) {
        if (num == null) {
            return null;
        }

        BigDecimal dec;
        if (num instanceof Float) {
            dec = new BigDecimal(num.floatValue());
        } else if (num instanceof Double) {
            dec = new BigDecimal(num.doubleValue());
        } else if (num instanceof BigDecimal) {
            dec = (BigDecimal) num;
        } else if (num instanceof BigInteger) {
            dec = new BigDecimal((BigInteger) num);
        } else {
            dec = new BigDecimal(num.longValue());
        }

        return convertDecimal(dec, precision, scale);
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
     * Converts the given {@code BigDecimal} to a decimal with the given {@code precision} and {@code scale}
     * according to SQL spec for CAST specification: General Rules, 8.
     */
    public static BigDecimal convertDecimal(BigDecimal value, int precision, int scale) {
        assert precision > 0 : "Invalid precision: " + precision;

        int defaultPrecision = IgniteTypeSystem.INSTANCE.getDefaultPrecision(SqlTypeName.DECIMAL);
        if (precision == defaultPrecision) {
            // This branch covers at least one known case: access to dynamic parameter from context.
            // In this scenario precision = DefaultTypePrecision, because types for dynamic params
            // are created by toSql(createType(param.class)).
            return value;
        }

        boolean nonZero = !value.unscaledValue().equals(BigInteger.ZERO);

        if (nonZero) {
            if (scale > precision) {
                throw new SqlException(QUERY_INVALID_ERR, NUMERIC_FIELD_OVERFLOW_ERROR);
            } else {
                int currentSignificantDigits = value.precision() - value.scale();
                int expectedSignificantDigits = precision - scale;

                if (currentSignificantDigits > expectedSignificantDigits) {
                    throw new SqlException(QUERY_INVALID_ERR, NUMERIC_FIELD_OVERFLOW_ERROR);
                }
            }
        }

        return value.setScale(scale, RoundingMode.HALF_UP);
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
}
