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

import static java.time.format.DateTimeFormatter.ISO_LOCAL_DATE;
import static java.time.format.DateTimeFormatter.ISO_LOCAL_TIME;
import static java.util.Objects.requireNonNull;
import static org.apache.ignite.internal.TestWrappers.unwrapIgniteImpl;
import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;
import static org.apache.ignite.internal.sql.engine.QueryCancelledException.CANCEL_MSG;
import static org.apache.ignite.internal.sql.engine.util.TypeUtils.columnType;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.StringContains.containsString;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.Period;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.time.temporal.Temporal;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.function.ToIntFunction;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.apache.calcite.avatica.util.ByteString;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.DateString;
import org.apache.calcite.util.TimeString;
import org.apache.calcite.util.TimestampString;
import org.apache.ignite.Ignite;
import org.apache.ignite.internal.Cluster;
import org.apache.ignite.internal.sql.engine.InternalSqlRow;
import org.apache.ignite.internal.sql.engine.QueryCancelledException;
import org.apache.ignite.internal.sql.engine.SqlQueryProcessor;
import org.apache.ignite.internal.sql.engine.exec.fsm.ExecutionPhase;
import org.apache.ignite.internal.sql.engine.type.IgniteTypeFactory;
import org.apache.ignite.internal.sql.engine.type.IgniteTypeSystem;
import org.apache.ignite.internal.testframework.IgniteTestUtils;
import org.apache.ignite.internal.type.DecimalNativeType;
import org.apache.ignite.internal.type.NativeType;
import org.apache.ignite.internal.type.TemporalNativeType;
import org.apache.ignite.internal.type.VarlenNativeType;
import org.apache.ignite.internal.util.StringUtils;
import org.apache.ignite.lang.ErrorGroup;
import org.apache.ignite.lang.ErrorGroups;
import org.apache.ignite.lang.ErrorGroups.Sql;
import org.apache.ignite.sql.ColumnType;
import org.apache.ignite.sql.IgniteSql;
import org.apache.ignite.sql.ResultSet;
import org.apache.ignite.sql.SqlException;
import org.apache.ignite.tx.Transaction;
import org.awaitility.Awaitility;
import org.hamcrest.CoreMatchers;
import org.hamcrest.Matcher;
import org.hamcrest.StringDescription;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.function.Executable;

/**
 * Test utils for SQL.
 */
public class SqlTestUtils {
    private static final ThreadLocalRandom RND = ThreadLocalRandom.current();

    private static final EnumMap<ColumnType, SqlTypeName> COLUMN_TYPE_TO_SQL_TYPE_NAME_MAP = new EnumMap<>(ColumnType.class);

    private static final Map<Integer, DateTimeFormatter> SQL_TIME_FORMATTERS = new HashMap<>();

    public static final DateTimeFormatter SQL_CONFORMANT_DATETIME_FORMATTER = new DateTimeFormatterBuilder()
                .parseCaseInsensitive()
                .append(ISO_LOCAL_DATE)
                .appendLiteral(' ')
                .append(ISO_LOCAL_TIME)
                .toFormatter();

    static {
        COLUMN_TYPE_TO_SQL_TYPE_NAME_MAP.put(ColumnType.BOOLEAN, SqlTypeName.BOOLEAN);
        COLUMN_TYPE_TO_SQL_TYPE_NAME_MAP.put(ColumnType.INT8, SqlTypeName.TINYINT);
        COLUMN_TYPE_TO_SQL_TYPE_NAME_MAP.put(ColumnType.INT16, SqlTypeName.SMALLINT);
        COLUMN_TYPE_TO_SQL_TYPE_NAME_MAP.put(ColumnType.INT32, SqlTypeName.INTEGER);
        COLUMN_TYPE_TO_SQL_TYPE_NAME_MAP.put(ColumnType.INT64, SqlTypeName.BIGINT);
        COLUMN_TYPE_TO_SQL_TYPE_NAME_MAP.put(ColumnType.FLOAT, SqlTypeName.REAL);
        COLUMN_TYPE_TO_SQL_TYPE_NAME_MAP.put(ColumnType.DOUBLE, SqlTypeName.DOUBLE);
        COLUMN_TYPE_TO_SQL_TYPE_NAME_MAP.put(ColumnType.DECIMAL, SqlTypeName.DECIMAL);
        COLUMN_TYPE_TO_SQL_TYPE_NAME_MAP.put(ColumnType.DATE, SqlTypeName.DATE);
        COLUMN_TYPE_TO_SQL_TYPE_NAME_MAP.put(ColumnType.TIME, SqlTypeName.TIME);
        COLUMN_TYPE_TO_SQL_TYPE_NAME_MAP.put(ColumnType.DATETIME, SqlTypeName.TIMESTAMP);
        COLUMN_TYPE_TO_SQL_TYPE_NAME_MAP.put(ColumnType.TIMESTAMP, SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE);
        COLUMN_TYPE_TO_SQL_TYPE_NAME_MAP.put(ColumnType.STRING, SqlTypeName.VARCHAR);
        COLUMN_TYPE_TO_SQL_TYPE_NAME_MAP.put(ColumnType.BYTE_ARRAY, SqlTypeName.VARBINARY);
        COLUMN_TYPE_TO_SQL_TYPE_NAME_MAP.put(ColumnType.NULL, SqlTypeName.NULL);
        COLUMN_TYPE_TO_SQL_TYPE_NAME_MAP.put(ColumnType.UUID, SqlTypeName.UUID);
        COLUMN_TYPE_TO_SQL_TYPE_NAME_MAP.put(ColumnType.PERIOD, null);
        COLUMN_TYPE_TO_SQL_TYPE_NAME_MAP.put(ColumnType.DURATION, null);
        COLUMN_TYPE_TO_SQL_TYPE_NAME_MAP.put(ColumnType.STRUCT, null);

        for (ColumnType value : ColumnType.values()) {
            assert COLUMN_TYPE_TO_SQL_TYPE_NAME_MAP.containsKey(value) : "absent type is " + value;
        }

        for (int i = 0; i <= 9; i++) {
            if (i == 0) {
                SQL_TIME_FORMATTERS.put(i, DateTimeFormatter.ofPattern("HH:mm:ss"));
            } else {
                SQL_TIME_FORMATTERS.put(i, DateTimeFormatter.ofPattern("HH:mm:ss." + "S".repeat(i)));
            }
        }
    }

    /**
     * <em>Assert</em> that execution of the supplied {@code executable} throws
     * an {@link SqlException} with expected error code and message.
     *
     * @param expectedCode Expected error code of {@link SqlException}.
     * @param expectedMessage Expected error message of {@link SqlException}.
     * @param executable Supplier to execute and check thrown exception.
     */
    public static void assertThrowsSqlException(int expectedCode, String expectedMessage, Executable executable) {
        assertThrowsSqlException(SqlException.class, expectedCode, expectedMessage, executable);
    }

    /**
     * <em>Assert</em> that execution of the supplied {@code executable} throws
     * an expected {@link SqlException} with expected error code and message.
     *
     * @param expectedType Expected exception type.
     * @param expectedCode Expected error code of {@link SqlException}.
     * @param expectedMessage Expected error message of {@link SqlException}.
     * @param executable Supplier to execute and check thrown exception.
     * @return Thrown the {@link SqlException}.
     */
    public static <T extends SqlException> T assertThrowsSqlException(
            Class<T> expectedType,
            int expectedCode,
            String expectedMessage,
            Executable executable) {
        T ex = assertThrows(expectedType, executable);

        int expectedErrorCode = ErrorGroup.extractErrorCode(expectedCode);
        ErrorGroup expectedErrorGroup = ErrorGroups.errorGroupByCode(expectedCode);
        String expectedError = format("{}-{}", expectedErrorGroup.name(), expectedErrorCode);
        String actualError = format("{}-{}", ex.groupName(), ex.errorCode());

        boolean errorMatches = expectedError.equals(actualError);
        boolean errorMessageMatches = CoreMatchers.containsString(expectedMessage).matches(ex.getMessage());

        if (!errorMatches || !errorMessageMatches) {
            StringWriter sw = new StringWriter();

            try (PrintWriter pw = new PrintWriter(sw)) {
                StringDescription description = new StringDescription();

                if (!errorMatches) {
                    description.appendText("Error code does not match. Expected: ");
                    description.appendValue(expectedError);
                    description.appendText(" actual: ");
                    description.appendValue(actualError);
                } else {
                    description.appendText("Error message does not match. Expected to include: ");
                    description.appendValue(expectedMessage);
                    description.appendText(" actual: ");
                    description.appendValue(ex.getMessage());
                }

                pw.println(description);
                ex.printStackTrace(pw);
            }

            fail(sw.toString());
        }

        assertEquals(expectedError, actualError, "Error does not match. " + ex);
        assertThat("Error message", ex.getMessage(), containsString(expectedMessage));

        return ex;
    }

    public static <T> Stream<T> asStream(Iterator<T> sourceIterator) {
        Iterable<T> iterable = () -> sourceIterator;
        return StreamSupport.stream(iterable.spliterator(), false);
    }

    /**
     * Convert {@link ColumnType} to string representation of SQL type.
     *
     * @param columnType Ignite type column.
     * @return String representation of SQL type.
     */
    public static String toSqlType(ColumnType columnType) {
        SqlTypeName type = COLUMN_TYPE_TO_SQL_TYPE_NAME_MAP.get(columnType);

        if (type == null) {
            throw new IllegalArgumentException("Unsupported type " + columnType);
        }

        return type.getSpaceName();
    }

    /**
     * Generate random value for given type.
     *
     * @param type {@link NativeType} type to generate value.
     * @return Generated value for given type.
     */
    public static @Nullable Object generateValueByType(NativeType type) {
        int scale = 0;
        int precision = 0;
        if (type instanceof DecimalNativeType) {
            scale = ((DecimalNativeType) type).scale();
            precision = ((DecimalNativeType) type).precision();
        } else if (type instanceof TemporalNativeType) {
            precision = ((TemporalNativeType) type).precision();
        } else if (type instanceof VarlenNativeType) {
            precision = ((VarlenNativeType) type).length();
        }

        return generateValueByType(type.spec(), precision, scale);
    }

    /**
     * Generate random value for given type.
     *
     * @param type {@link RelDataType} type to generate value.
     * @return Generated value for given type.
     */
    public static @Nullable Object generateValueByType(RelDataType type) {
        return generateValueByType(columnType(type), type.getPrecision(), type.getScale());
    }

    /**
     * Generate random value for given SQL type.
     *
     * @param type SQL type to generate value related to the type.
     * @return Generated value for given SQL type.
     */
    public static @Nullable Object generateValueByType(ColumnType type, int precision, int scale) {
        int negativeOrPositive = RND.nextBoolean() ? -1 : 1;
        switch (type) {
            case BOOLEAN:
                return RND.nextBoolean();
            case INT8:
                return (byte) (RND.nextInt(Byte.MAX_VALUE + 1) * negativeOrPositive);
            case INT16:
                return (short) (RND.nextInt(Byte.MAX_VALUE + 1, Short.MAX_VALUE + 1) * negativeOrPositive);
            case INT32:
                return (RND.nextInt(Short.MAX_VALUE + 1, Integer.MAX_VALUE) + 1) * negativeOrPositive;
            case INT64:
                return (RND.nextLong(Integer.MAX_VALUE + 1L, Long.MAX_VALUE) + 1) * negativeOrPositive;
            case FLOAT:
                // copy-paste from JDK 21 jdk.internal.util.random.RandomSupport.boundedNextFloat(java.util.random.RandomGenerator, float)
                float bound = Float.MAX_VALUE;
                float r = RND.nextFloat();
                r = r * bound;
                if (r >= bound) {
                    r = Math.nextDown(bound);
                }
                return r * negativeOrPositive;
            case DOUBLE:
                return RND.nextDouble(Float.MAX_VALUE + Double.MIN_NORMAL, Double.MAX_VALUE) * negativeOrPositive;
            case STRING:
                return IgniteTestUtils.randomString(RND, precision);
            case BYTE_ARRAY:
                return IgniteTestUtils.randomBytes(RND, precision);
            case NULL:
                return null;
            case DECIMAL:
                assert precision >= scale : "Scale of BigDecimal for SQL shouldn't be more than precision";
                assert precision > 0 : "Precision of BigDecimal for SQL should be positive";

                return IgniteTestUtils.randomBigDecimal(RND, precision, scale);
            case UUID:
                return new UUID(RND.nextLong(), RND.nextLong());
            case DURATION:
                return Duration.ofNanos(RND.nextLong());
            case DATETIME:
                return LocalDateTime.of(
                        (LocalDate) generateValueByType(ColumnType.DATE, precision, scale),
                        (LocalTime) generateValueByType(ColumnType.TIME, precision, scale)
                );
            case TIMESTAMP:
                return Instant.from(
                        ZonedDateTime.of((LocalDateTime) generateValueByType(ColumnType.DATETIME, precision, scale),
                                ZoneId.systemDefault()));
            case DATE:
                return LocalDate.of(1900 + RND.nextInt(1000), 1 + RND.nextInt(12), 1 + RND.nextInt(28));
            case TIME:
                return IgniteTestUtils.randomTime(RND, precision);
            case PERIOD:
                return Period.of(RND.nextInt(200), RND.nextInt(200), RND.nextInt(200));
            default:
                throw new IllegalArgumentException("unsupported type " + type);
        }
    }

    /**
     * Generate random value for given {@link ColumnType}. For precision and scale will be used maximums precisions and scale in SQL type
     * system, except for strings, byte arrays and decimals, to decrease generated values.
     *
     * @param type SQL type to generate value related to the type.
     * @return Generated value for given SQL type.
     */
    public static @Nullable Object generateValueByTypeWithMaxScalePrecisionForSql(ColumnType type) {
        SqlTypeName sqlTypeName = columnType2SqlTypeName(type);
        int precision = IgniteTypeSystem.INSTANCE.getMaxPrecision(sqlTypeName);
        int scale = IgniteTypeSystem.INSTANCE.getMaxScale(sqlTypeName);

        // To prevent generate too big values.
        if (type == ColumnType.STRING || type == ColumnType.BYTE_ARRAY || type == ColumnType.DECIMAL) {
            precision = 7_000;
            scale = precision / 2;
        }

        return generateValueByType(type, precision, scale);
    }

    /**
     * Generate value for given {@link ColumnType} based on given base number. Result of invocation always will be the same
     * for the same pair of arguments.
     *
     * @param base Base value to generate result value.
     * @param type Type to generate value.
     * @return Generated value for given type.
     */
    public static Object generateStableValueByType(int base, ColumnType type) {
        switch (type) {
            case BOOLEAN:
                return base % 2 == 0;
            case INT8:
                return (byte) base;
            case INT16:
                return (short) base;
            case INT32:
                return base;
            case INT64:
                return (long) base;
            case FLOAT:
                return (float) base + ((float) base / 1000);
            case DOUBLE:
                return (double) base + ((double) base / 1000);
            case DECIMAL:
                return BigDecimal.valueOf((double) base + ((double) base / 1000));
            case UUID:
                return new UUID(base, base);
            case STRING:
                return "str_" + base;
            case BYTE_ARRAY:
                return new byte[]{(byte) base, (byte) (base + 1), (byte) (base + 2)};
            case DATE:
                return LocalDate.of(2022, 01, 01).plusDays(base);
            case TIME:
                return LocalTime.of(0, 00, 00).plusSeconds(base);
            case DATETIME:
                return LocalDateTime.of(
                        (LocalDate) generateStableValueByType(base, ColumnType.DATE),
                        (LocalTime) generateStableValueByType(base, ColumnType.TIME)
                );
            case TIMESTAMP:
                return ((LocalDateTime) generateStableValueByType(base, ColumnType.DATETIME))
                        .atZone(ZoneId.systemDefault())
                        .toInstant();
            default:
                throw new IllegalStateException("Unexpected type: " + type);
        }
    }

    /**
     * Makes SQL literal for given value with a given type.
     *
     * @param value Value to present as SQL literal.
     * @param type Type of value to generate literal.
     * @return String representation of value as a SQL literal.
     */
    public static String makeLiteral(Object value, NativeType type) {
        if (value == null) {
            return "NULL";
        }

        switch (type.spec()) {
            case DECIMAL:
                return "DECIMAL '" + value + "'";
            case TIME:
                LocalTime localTime = (LocalTime) value;
                TemporalNativeType timeType = (TemporalNativeType) type;
                return "TIME '" + SQL_TIME_FORMATTERS.get(timeType.precision()).format(localTime) + "'";
            case DATE:
                return "DATE '" + value + "'";
            case TIMESTAMP:
                return "TIMESTAMP WITH LOCAL TIME ZONE '" + LocalDateTime.ofInstant(
                        ((Instant) value),
                        ZoneId.systemDefault()
                ).format(SQL_CONFORMANT_DATETIME_FORMATTER) + "'";
            case STRING:
                return "'" + value.toString().replace("'", "''") + "'";
            case DATETIME:
                return "TIMESTAMP '" + ((LocalDateTime) value).format(SQL_CONFORMANT_DATETIME_FORMATTER) + "'";
            case BYTE_ARRAY:
                assert value instanceof byte[];
                return "X'" + StringUtils.toHexString((byte[]) value) + "'";
            case UUID:
                return "UUID '" + value + "'";
            case NULL:
            case PERIOD:
            case DURATION:
                throw new IllegalArgumentException("The type " + type + " isn't supported right now");
            default:
                return value.toString();
        }
    }

    /**
     * Converts list of {@link InternalSqlRow} to list of list of objects, where internal list represent a row with fields.
     *
     * @param rows List of rows to be converted.
     * @return List of converted rows.
     */
    public static List<List<Object>> convertSqlRows(List<InternalSqlRow> rows) {
        return rows.stream().map(SqlTestUtils::convertSqlRowToObjects).collect(Collectors.toList());
    }

    /**
     * Converts {@link InternalSqlRow} to list of objects, where each of list elements represents a field.
     *
     * @param row {@link InternalSqlRow} need to be converted.
     * @return Converted value.
     */
    public static List<Object> convertSqlRowToObjects(InternalSqlRow row) {
        List<Object> result = new ArrayList<>(row.fieldCount());
        for (int i = 0; i < row.fieldCount(); i++) {
            result.add(row.get(i));
        }
        return result;
    }

    /** Generates literal using specified column type and value. */
    public static RexNode generateLiteral(ColumnType type, @Nullable Object value) {
        RexBuilder rexBuilder = Commons.rexBuilder();
        IgniteTypeFactory typeFactory = Commons.typeFactory();

        switch (type) {
            case NULL:
                return rexBuilder.makeNullLiteral(typeFactory.createSqlType(SqlTypeName.NULL));
            case BOOLEAN:
                return rexBuilder.makeLiteral(value, typeFactory.createSqlType(SqlTypeName.BOOLEAN));
            case INT8:
                return rexBuilder.makeLiteral(value, typeFactory.createSqlType(SqlTypeName.TINYINT));
            case INT16:
                return rexBuilder.makeLiteral(value, typeFactory.createSqlType(SqlTypeName.SMALLINT));
            case INT32:
                return rexBuilder.makeLiteral(value, typeFactory.createSqlType(SqlTypeName.INTEGER));
            case INT64:
                return rexBuilder.makeLiteral(value, typeFactory.createSqlType(SqlTypeName.BIGINT));
            case FLOAT:
                BigDecimal valueValue = new BigDecimal(requireNonNull(value).toString());
                return rexBuilder.makeLiteral(valueValue, typeFactory.createSqlType(SqlTypeName.REAL));
            case DOUBLE:
                BigDecimal doubleValue = new BigDecimal(requireNonNull(value).toString());
                return rexBuilder.makeLiteral(doubleValue, typeFactory.createSqlType(SqlTypeName.DOUBLE));
            case DECIMAL: {
                int precision = typeFactory.getTypeSystem().getMaxPrecision(SqlTypeName.DECIMAL);
                int scale = ((BigDecimal) value).scale();

                return rexBuilder.makeLiteral(value, typeFactory.createSqlType(SqlTypeName.DECIMAL, precision, scale));
            }
            case DATE:
                LocalDate localDate = (LocalDate) value;
                int epochDay = (int) requireNonNull(localDate).toEpochDay();

                return rexBuilder.makeDateLiteral(DateString.fromDaysSinceEpoch(epochDay));
            case TIME:
                LocalTime time = (LocalTime) value;
                int millisOfDay = (int) TimeUnit.NANOSECONDS.toMillis(requireNonNull(time).toNanoOfDay());

                return rexBuilder.makeTimeLiteral(TimeString.fromMillisOfDay(millisOfDay), 6);
            case DATETIME:
                LocalDateTime localDateTime = (LocalDateTime) value;
                Instant instant1 = requireNonNull(localDateTime).toInstant(ZoneOffset.UTC);
                TimestampString timestampString = TimestampString.fromMillisSinceEpoch(instant1.toEpochMilli());

                return rexBuilder.makeTimestampWithLocalTimeZoneLiteral(timestampString, 6);
            case TIMESTAMP:
                Instant instant = (Instant) value;

                return rexBuilder.makeTimestampLiteral(TimestampString.fromMillisSinceEpoch(requireNonNull(instant).toEpochMilli()), 6);
            case UUID:
                return rexBuilder.makeUuidLiteral((UUID) value);
            case STRING:
                return rexBuilder.makeLiteral(value, typeFactory.createSqlType(SqlTypeName.VARCHAR));
            case BYTE_ARRAY:
                byte[] bytes = requireNonNull((byte[]) value);
                ByteString byteStr = new ByteString(bytes);
                return rexBuilder.makeLiteral(byteStr, typeFactory.createSqlType(SqlTypeName.VARBINARY));
            case PERIOD:
            case DURATION:
            default:
                throw new IllegalArgumentException("Unexpected type: " + type);
        }
    }

    /** Convert {@link ColumnType} to {@link SqlTypeName}. */
    public static SqlTypeName columnType2SqlTypeName(ColumnType columnType) {
        SqlTypeName type = COLUMN_TYPE_TO_SQL_TYPE_NAME_MAP.get(columnType);

        if (type == null) {
            throw new IllegalArgumentException("Unknown type " + columnType);
        }

        return type;
    }

    /**
     * Executes an update on a sql, possibly in a transaction.
     *
     * @param query SQL query to execute.
     * @param sql Session on which to execute.
     * @param transaction Transaction in which to execute the update, or {@code null} if the update should be executed in an
     *         implicit transaction.
     */
    public static void executeUpdate(String query, IgniteSql sql, @Nullable Transaction transaction) {
        try (ResultSet<?> ignored = sql.execute(transaction, query)) {
            // Do nothing, just adhere to the syntactic ceremony...
        }
    }

    /**
     * Executes an update on a sql in an implicit transaction.
     *
     * @param query SQL query to execute.
     * @param sql Session on which to execute.
     */
    public static void executeUpdate(String query, IgniteSql sql) {
        executeUpdate(query, sql, null);
    }

    /** The action is expected to throw a {@link SqlException} with code {@link Sql#EXECUTION_CANCELLED_ERR}. */
    public static void expectQueryCancelled(Executable action) {
        assertThrowsSqlException(
                Sql.EXECUTION_CANCELLED_ERR,
                CANCEL_MSG,
                action
        );
    }

    /** The action is expected to throw a {@link QueryCancelledException}. */
    public static void expectQueryCancelledInternalException(Executable action) {
        //noinspection ThrowableNotThrown
        IgniteTestUtils.assertThrows(
                QueryCancelledException.class,
                action,
                CANCEL_MSG
        );
    }

    /**
     * Waits until the number of running queries matches the specified matcher.
     *
     * @param cluster Cluster.
     * @param matcher Matcher to check the number of running queries.
     * @throws AssertionError If after waiting the number of running queries still does not match the specified matcher.
     */
    public static void waitUntilRunningQueriesCount(Cluster cluster, Matcher<Integer> matcher) {
        ToIntFunction<Ignite> queriesCountPerNode = node ->
                ((SqlQueryProcessor) unwrapIgniteImpl(node).queryEngine()).runningQueries().size();

        Awaitility.await().timeout(5, TimeUnit.SECONDS)
                .until(() -> cluster.runningNodes().mapToInt(queriesCountPerNode).sum(), matcher);
    }

    /**
     * Waits until the number of queries in given phase matches the specified matcher.
     *
     * @param queryProcessor The query processor to derive list of running queries.
     * @param phase The {@link ExecutionPhase} of the interest.
     * @param matcher THe matcher to check the number of running queries.
     * @throws AssertionError If after waiting the number of running queries still does not match the specified matcher.
     */
    public static void waitUntilQueriesInPhaseCount(SqlQueryProcessor queryProcessor, ExecutionPhase phase, Matcher<Integer> matcher) {
        Awaitility.await().until(
                () -> (int) queryProcessor.runningQueries().stream()
                        .filter(queryInfo -> queryInfo.phase() == phase)
                        .count(),
                matcher
        );
    }

    /**
     * Trims milliseconds of a source temporal-type object to the target precision.
     *
     * @param type Source type.
     * @param source Source temporal object.
     * @param precision Target precision.
     * @return Temporal object with the adjusted number of nanoseconds.
     */
    public static Temporal adjustTemporalPrecision(ColumnType type, Temporal source, int precision) {
        switch (type) {
            case TIME: {
                LocalTime time = (LocalTime) source;

                return time.withNano(adjustNanos(time.getNano(), precision));
            }

            case DATETIME: {
                LocalDateTime dt = (LocalDateTime) source;

                return dt.withNano(adjustNanos(dt.getNano(), precision));
            }

            case TIMESTAMP: {
                Instant dt = (Instant) source;

                return dt.with(ChronoField.NANO_OF_SECOND, adjustNanos(dt.getNano(), precision));
            }

            default:
                throw new IllegalStateException("Unexpected type: " + type);
        }
    }

    /**
     * Trims number of nanoseconds according to the specified precision.
     *
     * <p>Note: the maximum supported precision is 3.
     *
     * @param nanos Number of nanoseconds.
     * @param precision Desired precision.
     * @return Adjusted number of nanoseconds.
     */
    @SuppressWarnings("NumericCastThatLosesPrecision")
    public static int adjustNanos(int nanos, int precision) {
        long millis = TimeUnit.NANOSECONDS.toMillis(nanos);

        int d = 3 - Math.min(3, precision);
        long adjustedMillis = (millis / (long) Math.pow(10, d)) * (long) Math.pow(10, d);

        return (int) TimeUnit.MILLISECONDS.toNanos(adjustedMillis);
    }
}
