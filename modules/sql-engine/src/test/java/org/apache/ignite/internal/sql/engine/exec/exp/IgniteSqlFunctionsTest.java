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

import static org.apache.ignite.internal.sql.engine.prepare.IgniteSqlValidator.NUMERIC_FIELD_OVERFLOW_ERROR;
import static org.apache.ignite.internal.sql.engine.util.SqlTestUtils.assertThrowsSqlException;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.TimeZone;
import java.util.function.Supplier;
import java.util.stream.Stream;
import org.apache.calcite.runtime.SqlFunctions;
import org.apache.calcite.runtime.SqlFunctions.DateParseFunction;
import org.apache.ignite.internal.sql.engine.util.IgniteMath;
import org.apache.ignite.lang.ErrorGroups.Sql;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Sql functions test.
 */
public class IgniteSqlFunctionsTest {
    @Test
    public void testBigDecimalToString() {
        assertNull(IgniteSqlFunctions.toString((BigDecimal) null));

        assertEquals(
                "10",
                IgniteSqlFunctions.toString(BigDecimal.valueOf(10))
        );

        assertEquals(
                "9223372036854775807",
                IgniteSqlFunctions.toString(BigDecimal.valueOf(Long.MAX_VALUE))
        );

        assertEquals(
                "340282350000000000000000000000000000000",
                IgniteSqlFunctions.toString(new BigDecimal(String.valueOf(Float.MAX_VALUE)))
        );

        assertEquals(
                "-340282346638528860000000000000000000000",
                IgniteSqlFunctions.toString(BigDecimal.valueOf(-Float.MAX_VALUE))
        );
    }

    @Test
    public void testBooleanPrimitiveToBigDecimal() {
        assertThrows(
                UnsupportedOperationException.class,
                () -> IgniteSqlFunctions.toBigDecimal(true, 10, 10));
    }

    @Test
    public void testBooleanObjectToBigDecimal() {
        assertThrows(
                UnsupportedOperationException.class,
                () -> IgniteSqlFunctions.toBigDecimal(Boolean.valueOf(true), 10, 10));
    }

    @Test
    public void testPrimitiveToDecimal() {
        assertEquals(
                BigDecimal.TEN,
                IgniteSqlFunctions.toBigDecimal((byte) 10, 10, 0)
        );

        assertEquals(
                BigDecimal.TEN,
                IgniteSqlFunctions.toBigDecimal((short) 10, 10, 0)
        );

        assertEquals(
                BigDecimal.TEN,
                IgniteSqlFunctions.toBigDecimal(10, 10, 0)
        );

        assertEquals(
                new BigDecimal("10.0"),
                IgniteSqlFunctions.toBigDecimal(10L, 10, 1)
        );

        assertEquals(
                new BigDecimal("10.100"),
                IgniteSqlFunctions.toBigDecimal(10.101f, 10, 3)
        );

        assertEquals(
                new BigDecimal("10.101"),
                IgniteSqlFunctions.toBigDecimal(10.101d, 10, 3)
        );
    }

    @Test
    public void testObjectToDecimal() {
        assertNull(IgniteSqlFunctions.toBigDecimal((Object) null, 10, 0));

        assertNull(IgniteSqlFunctions.toBigDecimal((Double) null, 10, 0));

        assertNull(IgniteSqlFunctions.toBigDecimal((String) null, 10, 0));

        assertEquals(
                BigDecimal.TEN,
                IgniteSqlFunctions.toBigDecimal(Byte.valueOf("10"), 10, 0)
        );

        assertEquals(
                BigDecimal.TEN,
                IgniteSqlFunctions.toBigDecimal(Short.valueOf("10"), 10, 0)
        );

        assertEquals(
                BigDecimal.TEN,
                IgniteSqlFunctions.toBigDecimal(Integer.valueOf(10), 10, 0)
        );

        assertEquals(
                new BigDecimal("10.0"),
                IgniteSqlFunctions.toBigDecimal(Long.valueOf(10L), 10, 1)
        );

        assertEquals(
                new BigDecimal("10.100"),
                IgniteSqlFunctions.toBigDecimal(Float.valueOf(10.101f), 10, 3)
        );

        assertEquals(
                new BigDecimal("10.101"),
                IgniteSqlFunctions.toBigDecimal(Double.valueOf(10.101d), 10, 3)
        );
    }

    @Test
    public void testFractionsToDecimal() {
        assertEquals(
                new BigDecimal("0.0100"),
                IgniteSqlFunctions.toBigDecimal(Float.valueOf(0.0101f), 3, 4)
        );

        assertEquals(
                new BigDecimal("0.0100"),
                IgniteSqlFunctions.toBigDecimal(Double.valueOf(0.0101d), 3, 4)
        );
    }

    /** Tests for decimal conversion function. */
    @ParameterizedTest
    @CsvSource({
            // input, precision, scale, result (number or error)
            "0, 1, 0, 0",
            "0, 1, 2, 0.00",
            "0, 2, 2, 0.00",
            "0, 2, 4, 0.0000",

            "1, 1, 0, 1",
            "1, 3, 2, 1.00",
            "1, 2, 2, overflow",

            "0.1, 1, 1, 0.1",

            "0.0101, 3, 4, 0.0101",
            "0.1234, 2, 1, 0.1",
            "0.1234, 5, 4, 0.1234",
            "0.123, 5, 4, 0.1230",

            "0.12, 2, 1, 0.1",
            "0.12, 2, 2, 0.12",
            "0.12, 2, 3, overflow",
            "0.12, 3, 3, 0.120",
            "0.12, 3, 2, 0.12",

            "0.123, 2, 2, 0.12",
            "0.123, 2, 1, 0.1",
            "0.123, 5, 5, 0.12300",
            "1.123, 4, 3, 1.123",
            "1.123, 4, 4, overflow",

            "0.0011, 1, 3, 0.001",
            "0.0016, 1, 3, 0.001",
            "-0.0011, 1, 3, -0.001",
            "-0.0011, 1, 4, overflow",
            "-0.0011, 2, 4, -0.0011",
            "-0.0011, 3, 4, -0.0011",
            "-0.0011, 2, 5, overflow",

            "10, 2, 0, 10",
            "10.0, 2, 0, 10",
            "10, 3, 0, 10",

            "10.01, 2, 0, 10",
            "10.1, 3, 0, 10",
            "10.11, 2, 1, overflow",
            "10.11, 3, 1, 10.1",
            "10.00, 3, 1, 10.0",

            "100.0, 3, 1, overflow",

            "1, 32767, 32767, overflow",

            "100.01, 4, 1, 100.0",
            "100.01, 4, 0, 100",
            "100.111, 3, 1, overflow",

            "11.1, 5, 3, 11.100",
            "11.100, 5, 3, 11.100",

            "-10.1, 3, 1, -10.1",
            "-10.1, 2, 0, -10",
            "-10.1, 2, 1, overflow",
    })
    public void testConvertDecimal(String input, int precision, int scale, String result) {
        Supplier<BigDecimal> convert = () -> IgniteSqlFunctions.toBigDecimal(new BigDecimal(input), precision, scale);

        if (!"overflow".equalsIgnoreCase(result)) {
            BigDecimal expected = convert.get();
            assertEquals(new BigDecimal(result), expected);
        } else {
            assertThrowsSqlException(Sql.RUNTIME_ERR, NUMERIC_FIELD_OVERFLOW_ERROR, convert::get);
        }
    }

    // ROUND

    /** Tests for ROUND(x) function. */
    @Test
    @SuppressWarnings("PMD.BigIntegerInstantiation")
    public void testRound() {
        assertEquals(BigDecimal.ONE, IgniteSqlFunctions.sround(new BigDecimal("1.000")));
        assertEquals(new BigDecimal("2"), IgniteSqlFunctions.sround(new BigDecimal("1.5")));
        assertEquals(1, IgniteSqlFunctions.sround(1), "int");
        assertEquals(1L, IgniteSqlFunctions.sround(1L), "long");
        assertEquals(2.0f, IgniteSqlFunctions.sround(1.5f), "float");
        assertEquals(2.0d, IgniteSqlFunctions.sround(1.5d), "double");
    }

    /** Tests for ROUND(x, s) function, where x is a BigDecimal value. */
    @ParameterizedTest
    @CsvSource({
            "1.123, -1, 0.000",
            "1.123, 0, 1.000",
            "1.123, 1, 1.100",
            "1.123, 2, 1.120",
            "1.125, 2, 1.130",
            "1.123, 3, 1.123",
            "1.123, 4, 1.123",
            "10.123, 0, 10.000",
            "10.500, 0, 11.000",
            "10.800, 0, 11.000",
            "10.123, -1, 10.000",
            "10.123, -2, 0.000",
            "10.123, 3, 10.123",
            "10.123, 4, 10.123",
    })
    public void testRound2Decimal(String input, int scale, String result) {
        assertEquals(new BigDecimal(result), IgniteSqlFunctions.sround(new BigDecimal(input), scale));
    }

    /** Tests for ROUND(x, s) function, where x is a double value. */
    @ParameterizedTest
    @CsvSource({
            "1.123, 3, 1.123",
            "1.123, 2, 1.12",
            "1.125, 2, 1.13",
            "1.245, 1, 1.2",
            "1.123, 0, 1.0",
            "1.500, 0, 2.0",
            "1.800, 0, 2.0",
            "1.123, -1, 0.0",
            "10.123, 0, 10.000",
            "10.123, -1, 10.000",
            "10.123, -2, 0.000",
            "10.123, 3, 10.123",
            "10.123, 4, 10.123",
    })
    public void testRound2Double(double input, int scale, double result) {
        assertEquals(result, IgniteSqlFunctions.sround(input, scale));
    }

    /** Tests for ROUND(x, s) function, where x is an byte. */
    @ParameterizedTest
    @CsvSource({
            "42, -2, 0",
            "42, -1, 40",
            "47, -1, 50",
            "42, 0, 42",
            "42, 1, 42",
            "42, 2, 42",
            "-42, -1, -40",
            "-47, -1, -50",
    })
    public void testRound2ByteType(byte input, int scale, byte result) {
        assertEquals(result, IgniteSqlFunctions.sround(input, scale));
    }

    /** Tests for ROUND(x, s) function, where x is an short. */
    @ParameterizedTest
    @CsvSource({
            "42, -2, 0",
            "42, -1, 40",
            "47, -1, 50",
            "42, 0, 42",
            "42, 1, 42",
            "42, 2, 42",
            "-42, -1, -40",
            "-47, -1, -50",
    })
    public void testRound2ShortType(short input, int scale, short result) {
        assertEquals(result, IgniteSqlFunctions.sround(input, scale));
    }

    /** Tests for ROUND(x, s) function, where x is an int. */
    @ParameterizedTest
    @CsvSource({
            "42, -2, 0",
            "42, -1, 40",
            "47, -1, 50",
            "42, 0, 42",
            "42, 1, 42",
            "42, 2, 42",
            "-42, -1, -40",
            "-47, -1, -50",
    })
    public void testRound2IntType(int input, int scale, int result) {
        assertEquals(result, IgniteSqlFunctions.sround(input, scale));
    }

    /** Tests for ROUND(x, s) function, where x is a long. */
    @ParameterizedTest
    @CsvSource({
            "42, -2, 0",
            "42, -1, 40",
            "47, -1, 50",
            "42, 0, 42",
            "42, 1, 42",
            "42, 2, 42",
            "-42, -1, -40",
            "-47, -1, -50",
    })
    public void testRound2LongType(long input, int scale, long result) {
        assertEquals(result, IgniteSqlFunctions.sround(input, scale));
    }

    /** Tests for TRUNCATE(x) function. */
    @Test
    public void testTruncate() {
        assertEquals(BigDecimal.ONE, IgniteSqlFunctions.struncate(new BigDecimal("1.000")));
        assertEquals(BigDecimal.ONE, IgniteSqlFunctions.struncate(new BigDecimal("1.5")));
        assertEquals(1, IgniteSqlFunctions.struncate(1), "int");
        assertEquals(1L, IgniteSqlFunctions.struncate(1L), "long");
        assertEquals(1.0d, IgniteSqlFunctions.struncate(1.5d), "double");
        assertEquals(1.0f, IgniteSqlFunctions.struncate(1.5f), "float");
    }

    /** Tests for TRUNCATE(x, s) function, where x is a BigDecimal value. */
    @ParameterizedTest
    @CsvSource({
            "1.123, -1, 0.000",
            "1.123, 0, 1.000",
            "1.123, 1, 1.100",
            "1.123, 2, 1.120",
            "1.125, 2, 1.120",
            "1.123, 3, 1.123",
            "1.123, 4, 1.123",
            "10.123, 0, 10.000",
            "10.500, 0, 10.000",
            "10.800, 0, 10.000",
            "10.123, -1, 10.000",
            "10.123, -2, 0.000",
            "10.123, 3, 10.123",
            "10.123, 4, 10.123",
    })
    public void testTruncate2Decimal(String input, int scale, String result) {
        assertEquals(new BigDecimal(result), IgniteSqlFunctions.struncate(new BigDecimal(input), scale));
    }

    /** Tests for TRUNCATE(x, s) function, where x is a double value. */
    @ParameterizedTest
    @CsvSource({
            "1.123, 3, 1.123",
            "1.123, 2, 1.12",
            "1.125, 2, 1.12",
            "1.245, 1, 1.2",
            "1.123, 0, 1.0",
            "1.500, 0, 1.0",
            "1.800, 0, 1.0",
            "1.123, -1, 0.0",
            "10.123, 0, 10.000",
            "10.123, -1, 10.000",
            "10.123, -2, 0.000",
            "10.123, 3, 10.123",
            "10.123, 4, 10.123",
    })
    public void testTruncate2Double(double input, int scale, double result) {
        assertEquals(result, IgniteSqlFunctions.struncate(input, scale));
    }

    /** Tests for TRUNCATE(x, s) function, where x is an byte. */
    @ParameterizedTest
    @CsvSource({
            "42, -2, 0",
            "42, -1, 40",
            "47, -1, 40",
            "42, 0, 42",
            "42, 1, 42",
            "42, 2, 42",
            "-42, -1, -40",
            "-47, -1, -40",
    })
    public void testTruncate2ByteType(byte input, int scale, byte result) {
        assertEquals(result, IgniteSqlFunctions.struncate(input, scale));
    }

    /** Tests for TRUNCATE(x, s) function, where x is an short. */
    @ParameterizedTest
    @CsvSource({
            "42, -2, 0",
            "42, -1, 40",
            "47, -1, 40",
            "42, 0, 42",
            "42, 1, 42",
            "42, 2, 42",
            "-42, -1, -40",
            "-47, -1, -40",
    })
    public void testTruncateShortType(short input, int scale, short result) {
        assertEquals(result, IgniteSqlFunctions.struncate(input, scale));
    }

    /** Tests for TRUNCATE(x, s) function, where x is an int. */
    @ParameterizedTest
    @CsvSource({
            "42, -2, 0",
            "42, -1, 40",
            "47, -1, 40",
            "42, 0, 42",
            "42, 1, 42",
            "42, 2, 42",
            "-42, -1, -40",
            "-47, -1, -40",
    })
    public void testTruncate2IntType(int input, int scale, int result) {
        assertEquals(result, IgniteSqlFunctions.struncate(input, scale));
    }

    /** Tests for TRUNCATE(x, s) function, where x is a long. */
    @ParameterizedTest
    @CsvSource({
            "42, -2, 0",
            "42, -1, 40",
            "47, -1, 40",
            "42, 0, 42",
            "42, 1, 42",
            "42, 2, 42",
            "-42, -1, -40",
            "-47, -1, -40",
    })
    public void testTruncate2LongType(long input, int scale, long result) {
        assertEquals(result, IgniteSqlFunctions.struncate(input, scale));
    }

    @ParameterizedTest
    @CsvSource(
            value = {
                    "1; 2; 0.50",
                    "1; 3; 0.33",
                    "6; 2; 3.00",
                    "1; 0;",
            },
            delimiterString = ";"
    )
    public void testAvgDivide(String a, String b, @Nullable String expected) {
        BigDecimal num = new BigDecimal(a);
        BigDecimal denum = new BigDecimal(b);

        if (expected != null) {
            BigDecimal actual = IgniteMath.decimalDivide(num, denum, 4, 2);
            assertEquals(new BigDecimal(expected), actual);
        } else {
            assertThrows(ArithmeticException.class, () -> IgniteMath.decimalDivide(num, denum, 4, 2));
        }
    }

    @ParameterizedTest
    @MethodSource("timeZoneTime")
    public void toTimestampWithLocalTimeZoneFormat(String zoneIdstr, LocalDateTime time) {
        ZoneId zoneId = ZoneId.of(zoneIdstr);

        String v = time.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS"));

        String format = "YYYY-MM-DD HH24:MI:SS.FF3";
        TimeZone timeZone = TimeZone.getTimeZone(zoneId);
        long calciteTsLtz = SqlFunctions.toTimestampWithLocalTimeZone(v, timeZone);
        long tsLtz = IgniteSqlFunctions.toTimestampWithLocalTimeZone(v, format, timeZone);

        assertEquals(Instant.ofEpochMilli(calciteTsLtz), Instant.ofEpochMilli(tsLtz));

        String formatted = IgniteSqlFunctions.formatTimestampWithLocalTimeZone(format + " TZHTZM", calciteTsLtz, timeZone);

        String tzOffsetStr = OffsetDateTime.ofInstant(Instant.ofEpochMilli(tsLtz), zoneId)
                .format(DateTimeFormatter.ofPattern("Z"));

        assertEquals(v + " " + tzOffsetStr, formatted);
    }

    private static Stream<Arguments> timeZoneTime() {
        List<String> zones = List.of("Europe/Paris", "Europe/Moscow", "Asia/Tokyo", "America/New_York");
        List<LocalDateTime> times = List.of(
                LocalDateTime.of(2012, 7, 19, 11, 13, 58, 1_000_000),
                LocalDateTime.of(2012, 7, 19, 11, 13, 58, 123_000_000),
                LocalDateTime.of(2012, 7, 19, 11, 13, 58, 500_000_000),
                LocalDateTime.of(2012, 7, 19, 11, 13, 58, 999_000_000),

                LocalDateTime.of(2025, 5, 7, 11, 13, 58, 1_000_000),
                LocalDateTime.of(2025, 5, 7, 11, 13, 58, 123_000_000),
                LocalDateTime.of(2025, 5, 7, 11, 13, 58, 500_000_000),
                LocalDateTime.of(2025, 5, 7, 11, 13, 58, 999_000_000),

                LocalDateTime.of(2023, 10, 29, 2, 1, 1, 111_000_000),
                LocalDateTime.of(2023, 10, 29, 3, 1, 1, 111_000_000),
                LocalDateTime.of(2023, 10, 29, 4, 1, 1, 111_000_000),
                LocalDateTime.of(2023, 10, 29, 5, 1, 1, 111_000_000),

                // TODO https://issues.apache.org/jira/browse/IGNITE-25342
                // This time (02:00 - 02:59) does not exist because France switches to the summer time schedule
                // LocalDateTime.of(2024, 3, 31, 2, 1, 1, 111_000_000),
                LocalDateTime.of(2024, 3, 31, 3, 1, 1, 111_000_000),
                LocalDateTime.of(2024, 3, 31, 4, 1, 1, 111_000_000),
                LocalDateTime.of(2024, 3, 31, 5, 1, 1, 111_000_000)
        );

        return zones.stream().flatMap(z -> times.stream().map(t -> Arguments.of(z, t)));
    }

    @ParameterizedTest
    @MethodSource("timeValues")
    public void testToTime(String timeStr, int expectedMillis) {
        String format = "HH24:MI:SS";

        DateParseFunction f = new DateParseFunction();
        int millis = f.parseTime(format, timeStr);
        int time2 = IgniteSqlFunctions.toTime(timeStr, format);

        assertEquals(expectedMillis, millis);
        assertEquals(millis, time2);

        String formatted = IgniteSqlFunctions.formatTime(format, time2);
        assertEquals(timeStr, formatted);
    }

    private static Stream<Arguments> timeValues() {
        return Stream.of(
                Arguments.of("00:00:00", 0),
                Arguments.of("01:01:43", 3703000),
                Arguments.of("07:37:59", 27479000),
                Arguments.of("19:01:32", 68492000),
                Arguments.of("23:59:59", 86399000)
        );
    }

    @ParameterizedTest
    @MethodSource("dateValues")
    public void testToDate(String timeStr, int expectedDays) {
        String format = "YYYY-MM-DD";

        DateParseFunction f = new DateParseFunction();
        int days = f.parseDate(format, timeStr);
        int days2 = IgniteSqlFunctions.toDate(timeStr, format);

        assertEquals(expectedDays, days);
        assertEquals(days, days2);

        String formatted = IgniteSqlFunctions.formatDate(format, days2);
        assertEquals(timeStr, formatted);
    }

    private static Stream<Arguments> dateValues() {
        return Stream.of(
                Arguments.of("1970-01-01", 0),
                Arguments.of("2025-01-01", 20089)
        );
    }

    @ParameterizedTest
    @MethodSource("timestampValues")
    public void testToTimestamp(String timeStr, long expectedTs) {
        String format = "YYYY-MM-DD HH24:MI:SS";

        DateParseFunction f = new DateParseFunction();
        long ts = f.parseTimestamp(format, timeStr);
        Long ts2 = IgniteSqlFunctions.toTimestamp(timeStr, format);

        assertEquals(expectedTs, ts);
        assertEquals(ts, ts2);

        String formatted = IgniteSqlFunctions.formatTimestamp(format, ts2);
        assertEquals(timeStr, formatted);
    }

    private static Stream<Arguments> timestampValues() {
        return Stream.of(
                Arguments.of("1970-01-01 00:00:00", 0),
                Arguments.of("1970-01-01 00:00:10", 10000),
                Arguments.of("2025-01-01 00:00:00", 1735689600000L),
                Arguments.of("2025-01-01 00:00:20", 1735689620000L)
        );
    }
}
