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
import java.util.BitSet;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.ignite.internal.sql.engine.type.UuidType;
import org.apache.ignite.sql.ColumnType;
import org.apache.ignite.sql.SqlException;
import org.junit.jupiter.api.function.Executable;

/**
 * Test utils for SQL.
 */
public class SqlTestUtils {
    private static final ThreadLocalRandom RND = ThreadLocalRandom.current();


    /**
     * <em>Assert</em> that execution of the supplied {@code executable} throws
     * an {@link SqlException} with expected error code and return the exception.
     *
     * @param expectedCode Expected error code of {@link SqlException}
     * @param executable Supplier to execute and check thrown exception.
     * @return Thrown the {@link SqlException}.
     */
    public static SqlException assertThrowsSqlException(int expectedCode, Executable executable) {
        SqlException ex = assertThrows(SqlException.class, executable);
        assertEquals(expectedCode, ex.code());

        return ex;
    }

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
        SqlException ex = assertThrowsSqlException(expectedCode, executable);

        assertThat("Error message", ex.getMessage(), containsString(expectedMessage));

        return ex;
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
                return (float) base + ((float) base / 1000);
            case DOUBLE:
                return (double) base + ((double) base / 1000);
            case STRING:
                return "str_" + base;
            case BYTE_ARRAY:
                return new byte[]{(byte) base, (byte) (base + 1), (byte) (base + 2)};
            case NULL:
                return null;
            case DECIMAL:
                return BigDecimal.valueOf((double) base + ((double) base / 1000));
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
}
