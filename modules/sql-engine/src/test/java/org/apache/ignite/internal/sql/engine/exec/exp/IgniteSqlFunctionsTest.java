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
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.math.BigDecimal;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.Locale;
import java.util.TimeZone;
import java.util.function.Supplier;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.ignite.internal.sql.engine.type.IgniteTypeSystem;
import org.apache.ignite.lang.ErrorGroups.Sql;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

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
                new BigDecimal(10),
                IgniteSqlFunctions.toBigDecimal((byte) 10, 10, 0)
        );

        assertEquals(
                new BigDecimal(10),
                IgniteSqlFunctions.toBigDecimal((short) 10, 10, 0)
        );

        assertEquals(
                new BigDecimal(10),
                IgniteSqlFunctions.toBigDecimal(10, 10, 0)
        );

        assertEquals(
                new BigDecimal("10.0"),
                IgniteSqlFunctions.toBigDecimal(10L, 10, 1)
        );

        assertEquals(
                new BigDecimal("10.101"),
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
                new BigDecimal(10),
                IgniteSqlFunctions.toBigDecimal(Byte.valueOf("10"), 10, 0)
        );

        assertEquals(
                new BigDecimal(10),
                IgniteSqlFunctions.toBigDecimal(Short.valueOf("10"), 10, 0)
        );

        assertEquals(
                new BigDecimal(10),
                IgniteSqlFunctions.toBigDecimal(Integer.valueOf(10), 10, 0)
        );

        assertEquals(
                new BigDecimal("10.0"),
                IgniteSqlFunctions.toBigDecimal(Long.valueOf(10L), 10, 1)
        );

        assertEquals(
                new BigDecimal("10.101"),
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
                new BigDecimal("0.0101"),
                IgniteSqlFunctions.toBigDecimal(Float.valueOf(0.0101f), 3, 4)
        );

        assertEquals(
                new BigDecimal("0.0101"),
                IgniteSqlFunctions.toBigDecimal(Double.valueOf(0.0101d), 3, 4)
        );
    }

    /** Access of dynamic parameter value - parameter is not transformed. */
    @Test
    public void testToBigDecimalFromObject() {
        Object value = new BigDecimal("100.1");
        int defaultPrecision = IgniteTypeSystem.INSTANCE.getDefaultPrecision(SqlTypeName.DECIMAL);

        assertSame(value, IgniteSqlFunctions.toBigDecimal(value, defaultPrecision, 0));
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
            "0.0016, 1, 3, 0.002",
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
    public void testRound() {
        assertEquals(new BigDecimal("1"), IgniteSqlFunctions.sround(new BigDecimal("1.000")));
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
        assertEquals(new BigDecimal("1"), IgniteSqlFunctions.struncate(new BigDecimal("1.000")));
        assertEquals(new BigDecimal("1"), IgniteSqlFunctions.struncate(new BigDecimal("1.5")));
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
    @ValueSource(strings = {
            "2023-10-29 02:01:01",
            "2023-10-29 03:01:01",
            "2023-10-29 04:01:01",
            "2023-10-29 05:01:01",
            "2024-03-31 02:01:01",
            "2024-03-31 03:01:01",
            "2024-03-31 04:01:01",
            "2024-03-31 05:01:01",
    })
    public void testSubtractTimeZoneOffset(String input) throws ParseException {
        TimeZone cyprusTz = TimeZone.getTimeZone("Asia/Nicosia");
        TimeZone utcTz = TimeZone.getTimeZone("UTC");

        SimpleDateFormat dateFormatTz = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss", Locale.getDefault());
        dateFormatTz.setTimeZone(cyprusTz);

        SimpleDateFormat dateFormatUtc = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss", Locale.getDefault());
        dateFormatUtc.setTimeZone(utcTz);

        long expMillis = dateFormatTz.parse(input).getTime();
        long utcMillis = dateFormatUtc.parse(input).getTime();

        long actualTs = IgniteSqlFunctions.subtractTimeZoneOffset(utcMillis, cyprusTz);

        assertEquals(Instant.ofEpochMilli(expMillis), Instant.ofEpochMilli(actualTs));
    }

    @ParameterizedTest
    @CsvSource(
            value = {
                    "1; 2; 0.5",
                    "1; 3; 0.3333333333333333",
                    "6; 2; 3",
                    "1; 0;",
            },
            delimiterString = ";"
    )
    public void testAvgDivide(String a, String b, @Nullable String expected) {
        BigDecimal num = new BigDecimal(a);
        BigDecimal denum = new BigDecimal(b);

        if (expected != null) {
            BigDecimal actual = IgniteSqlFunctions.decimalDivide(num, denum, 4, 2);
            assertEquals(new BigDecimal(expected), actual);
        } else {
            assertThrows(ArithmeticException.class, () -> IgniteSqlFunctions.decimalDivide(num, denum, 4, 2));
        }
    }

    @ParameterizedTest
    @CsvSource({
            "1970-01-01 00:00:00,     0, 0",
            "1970-01-01 00:00:00.12,  2, 123",
            "1970-01-01 00:00:00.123, 3, 123",
            "1970-01-01 00:00:00.123, 6, 123",
            "1970-02-01 23:59:59,     0, 2764799000",
            "1970-02-01 23:59:59.04,  2, 2764799040",
            "1969-12-31 23:59:59.999, 3, -1",
            "1969-12-31 23:59:59.98,  2, -11",
    })
    public void testTimestampToString(String expectedDate, int precision, long millis) {
        assertThat(IgniteSqlFunctions.unixTimestampToString(millis, precision), is(expectedDate));
    }
}
