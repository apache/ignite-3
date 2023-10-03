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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.StringContains.containsString;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.Period;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.ignite.Ignite;
import org.apache.ignite.internal.sql.engine.type.UuidType;
import org.apache.ignite.lang.IgniteExceptionMapperUtil;
import org.apache.ignite.sql.ColumnType;
import org.apache.ignite.sql.ResultSet;
import org.apache.ignite.sql.Session;
import org.apache.ignite.sql.SqlException;
import org.apache.ignite.sql.SqlRow;
import org.apache.ignite.tx.Transaction;
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
        assertEquals(expectedCode, ex.code());

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
            case NUMBER:
                return SqlTypeName.INTEGER.getName();
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
        return generateValueByType(RND.nextInt(), type);
    }

    /**
     * Generate value for given SQL type.
     *
     * @param base Base value to generate value.
     * @param type SQL type to generate value related to the type.
     * @return Generated value for given SQL type.
     */
    public static Object generateValueByType(int base, ColumnType type) {
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
                return base + ((float) base / 1000);
            case DOUBLE:
                return base + ((double) base / 1000);
            case STRING:
                return "str_" + base;
            case BYTE_ARRAY:
                return new byte[]{(byte) base, (byte) (base + 1), (byte) (base + 2)};
            case NULL:
                return null;
            case DECIMAL:
                return BigDecimal.valueOf(base + ((double) base / 1000));
            case NUMBER:
                return BigInteger.valueOf(base);
            case UUID:
                return new UUID(base, base);
            case BITMASK:
                return BitSet.valueOf(BigInteger.valueOf(base).toByteArray());
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
     * Run SQL on given Ignite instance with given transaction and parameters.
     *
     * @param ignite Ignite instance to run a query.
     * @param tx Transaction to run a given query. Can be {@code null} to run within implicit transaction.
     * @param sql Query to be run.
     * @param args Dynamic parameters for a given query.
     * @return List of lists, where outer list represents a rows, internal lists represents a columns.
     */
    public static List<List<Object>> sql(Ignite ignite, @Nullable Transaction tx, String sql, Object... args) {
        try (
                Session session = ignite.sql().createSession();
                ResultSet<SqlRow> rs = session.execute(tx, sql, args)
        ) {
            return getAllResultSet(rs);
        }
    }

    private static List<List<Object>> getAllResultSet(ResultSet<SqlRow> resultSet) {
        List<List<Object>> res = new ArrayList<>();

        while (resultSet.hasNext()) {
            SqlRow sqlRow = resultSet.next();

            ArrayList<Object> row = new ArrayList<>(sqlRow.columnCount());
            for (int i = 0; i < sqlRow.columnCount(); i++) {
                row.add(sqlRow.value(i));
            }

            res.add(row);
        }

        resultSet.close();

        return res;
    }
}
