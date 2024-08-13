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

import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;
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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.apache.calcite.avatica.util.ByteString;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.DateString;
import org.apache.calcite.util.TimeString;
import org.apache.calcite.util.TimestampString;
import org.apache.ignite.internal.sql.engine.InternalSqlRow;
import org.apache.ignite.internal.sql.engine.type.IgniteCustomType;
import org.apache.ignite.internal.sql.engine.type.IgniteTypeFactory;
import org.apache.ignite.internal.sql.engine.type.UuidType;
import org.apache.ignite.lang.ErrorGroup;
import org.apache.ignite.lang.ErrorGroups;
import org.apache.ignite.sql.ColumnType;
import org.apache.ignite.sql.IgniteSql;
import org.apache.ignite.sql.ResultSet;
import org.apache.ignite.sql.SqlException;
import org.apache.ignite.tx.Transaction;
import org.hamcrest.CoreMatchers;
import org.hamcrest.StringDescription;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.function.Executable;

/**
 * Test utils for SQL.
 */
public class SqlTestUtils {
    private static final ThreadLocalRandom RND = ThreadLocalRandom.current();

    /**
     * <em>Assert</em> that execution of the supplied {@code executable} throws
     * an {@link SqlException} with expected error code and message.
     *
     * @param expectedCode Expected error code of {@link SqlException}.
     * @param expectedMessage Expected error message of {@link SqlException}.
     * @param executable Supplier to execute and check thrown exception.
     * @return Thrown the {@link SqlException}.
     */
    public static SqlException assertThrowsSqlException(int expectedCode, String expectedMessage, Executable executable) {
        return assertThrowsSqlException(SqlException.class, expectedCode, expectedMessage, executable);
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
        switch (columnType) {
            case BOOLEAN:
                return SqlTypeName.BOOLEAN.getName();
            case INT8:
                return SqlTypeName.TINYINT.getName();
            case INT16:
                return SqlTypeName.SMALLINT.getName();
            case INT32:
                return SqlTypeName.INTEGER.getName();
            case INT64:
                return SqlTypeName.BIGINT.getName();
            case FLOAT:
                return SqlTypeName.REAL.getName();
            case DOUBLE:
                return SqlTypeName.DOUBLE.getName();
            case DECIMAL:
                return SqlTypeName.DECIMAL.getName();
            case DATE:
                return SqlTypeName.DATE.getName();
            case TIME:
                return SqlTypeName.TIME.getName();
            case DATETIME:
                return SqlTypeName.TIMESTAMP.getName();
            case TIMESTAMP:
                return SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE.getName();
            case UUID:
                return UuidType.NAME;
            case STRING:
                return SqlTypeName.VARCHAR.getName();
            case BYTE_ARRAY:
                return SqlTypeName.VARBINARY.getName();
            case NULL:
                return SqlTypeName.NULL.getName();
            default:
                throw new IllegalArgumentException("Unsupported type " + columnType);
        }
    }

    /**
     * Generate random value for given SQL type.
     *
     * @param type SQL type to generate value related to the type.
     * @return Generated value for given SQL type.
     */
    public static Object generateValueByType(ColumnType type) {
        return generateValueByType(RND.nextInt(Short.MAX_VALUE), type);
    }

    /**
     * Generate value for given SQL type.
     *
     * @param base Base value to generate value.
     * @param type SQL type to generate value related to the type.
     * @return Generated value for given SQL type.
     */
    public static Object generateValueByType(int base, ColumnType type) {
        base = Math.abs(base);

        switch (type) {
            case BOOLEAN:
                return base % 2 == 0;
            case INT8:
                return (byte) base;
            case INT16:
                return (short) (Short.MAX_VALUE - (byte) base);
            case INT32:
                return Integer.MAX_VALUE - (short) base;
            case INT64:
                return Long.MAX_VALUE - base;
            case FLOAT:
                return Float.MAX_VALUE - (short) base + ((float) base / 1000);
            case DOUBLE:
                return Double.MAX_VALUE - base + ((double) base / 1000);
            case STRING:
                return "str_" + base;
            case BYTE_ARRAY:
                return new byte[]{(byte) base, (byte) (base + 1), (byte) (base + 2)};
            case NULL:
                return null;
            case DECIMAL:
                return BigDecimal.valueOf(base + ((double) base / 1000)).add(BigDecimal.valueOf(Long.MAX_VALUE));
            case UUID:
                return new UUID(base, base);
            case DURATION:
                return Duration.ofNanos(base);
            case DATETIME:
                return LocalDateTime.of(
                        (LocalDate) generateValueByType(base, ColumnType.DATE),
                        (LocalTime) generateValueByType(base, ColumnType.TIME)
                );
            case TIMESTAMP:
                return Instant.from(
                        ZonedDateTime.of((LocalDateTime) generateValueByType(base, ColumnType.DATETIME), ZoneId.systemDefault()));
            case DATE:
                return LocalDate.of(2022, 01, 01).plusDays(base % 30);
            case TIME:
                return LocalTime.of(0, 00, 00).plusSeconds(base % 1000);
            case PERIOD:
                return Period.of(base % 2, base % 12, base % 29);
            default:
                throw new IllegalArgumentException("unsupported type " + type);
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

    /** Generates literal or value expression using specified column type and value. */
    public static RexNode generateLiteralOrValueExpr(ColumnType type, Object value) {
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
                BigDecimal valueValue = new BigDecimal(value.toString());
                return rexBuilder.makeLiteral(valueValue, typeFactory.createSqlType(SqlTypeName.REAL));
            case DOUBLE:
                BigDecimal doubleValue = new BigDecimal(value.toString());
                return rexBuilder.makeLiteral(doubleValue, typeFactory.createSqlType(SqlTypeName.DOUBLE));
            case DECIMAL:
                return rexBuilder.makeLiteral(value, typeFactory.createSqlType(SqlTypeName.DECIMAL));
            case DATE:
                LocalDate localDate = (LocalDate) value;
                int epochDay = (int) localDate.toEpochDay();

                return rexBuilder.makeDateLiteral(DateString.fromDaysSinceEpoch(epochDay));
            case TIME:
                LocalTime time = (LocalTime) value;
                int millisOfDay = (int) TimeUnit.NANOSECONDS.toMillis(time.toNanoOfDay());

                return rexBuilder.makeTimeLiteral(TimeString.fromMillisOfDay(millisOfDay), 6);
            case DATETIME:
                LocalDateTime localDateTime = (LocalDateTime) value;
                Instant instant1 = localDateTime.toInstant(ZoneOffset.UTC);
                TimestampString timestampString = TimestampString.fromMillisSinceEpoch(instant1.toEpochMilli());

                return rexBuilder.makeTimestampWithLocalTimeZoneLiteral(timestampString, 6);
            case TIMESTAMP:
                Instant instant = (Instant) value;

                return rexBuilder.makeTimestampLiteral(TimestampString.fromMillisSinceEpoch(instant.toEpochMilli()), 6);
            case UUID:
                RexLiteral uuidStr = rexBuilder.makeLiteral(value.toString(), typeFactory.createSqlType(SqlTypeName.VARCHAR));
                IgniteCustomType uuidType = typeFactory.createCustomType(UuidType.NAME);

                return rexBuilder.makeCast(uuidType, uuidStr);
            case STRING:
                return rexBuilder.makeLiteral(value, typeFactory.createSqlType(SqlTypeName.VARCHAR));
            case BYTE_ARRAY:
                byte[] bytes = (byte[]) value;
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
        switch (columnType) {
            case NULL:
                return SqlTypeName.NULL;
            case BOOLEAN:
                return SqlTypeName.BOOLEAN;
            case INT8:
                return SqlTypeName.TINYINT;
            case INT16:
                return SqlTypeName.SMALLINT;
            case INT32:
                return SqlTypeName.INTEGER;
            case INT64:
                return SqlTypeName.BIGINT;
            case FLOAT:
                return SqlTypeName.REAL;
            case DOUBLE:
                return SqlTypeName.DOUBLE;
            case DECIMAL:
                return SqlTypeName.DECIMAL;
            case DATE:
                return SqlTypeName.DATE;
            case TIME:
                return SqlTypeName.TIME;
            case DATETIME:
                return SqlTypeName.TIMESTAMP;
            case TIMESTAMP:
                return SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE;
            case UUID:
                return SqlTypeName.ANY;
            case STRING:
                return SqlTypeName.VARCHAR;
            case BYTE_ARRAY:
                return SqlTypeName.VARBINARY;
            case PERIOD:
                return SqlTypeName.INTERVAL_YEAR_MONTH;
            case DURATION:
                return SqlTypeName.INTERVAL_SECOND;
            default:
                throw new IllegalArgumentException("Unknown type " + columnType);
        }
    }

    /**
     * Executes an update on a sql, possibly in a transaction.
     *
     * @param query SQL query to execute.
     * @param sql Session on which to execute.
     * @param transaction Transaction in which to execute the update, or {@code null} if the update should
     *     be executed in an implicit transaction.
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
}
