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

import static java.util.UUID.randomUUID;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Clock;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.TimeZone;
import java.util.stream.Stream;
import org.apache.calcite.DataContext;
import org.apache.ignite.internal.lang.IgniteStringFormatter;
import org.apache.ignite.internal.network.ClusterNodeImpl;
import org.apache.ignite.internal.sql.engine.exec.ExecutionContext;
import org.apache.ignite.internal.sql.engine.exec.QueryTaskExecutor;
import org.apache.ignite.internal.sql.engine.exec.mapping.FragmentDescription;
import org.apache.ignite.internal.sql.engine.framework.TestBuilders;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.testframework.IgniteTestUtils;
import org.apache.ignite.network.NetworkAddress;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mockito;

/**
 * Test cases for {@link IgniteSqlDateTimeUtils}.
 */
public class IgniteSqlDateTimeUtilsTest extends BaseIgniteAbstractTest {
    /**
     * Ensures that {@link IgniteSqlDateTimeUtils#currentDate(DataContext)} takes into account the client's time zone.
     */
    @ParameterizedTest
    @CsvSource({
            "2025-01-01T23:00:00Z, 2025-01-01, GMT",
            "2025-01-01T23:00:00Z, 2025-01-02, GMT+1",

            // DST transition (GMT+2 => GMT+3).
            "2024-03-30T21:00:00Z, 2024-03-30, Asia/Nicosia",
            "2024-03-31T21:00:00Z, 2024-04-01, Asia/Nicosia",

            // DST transition (GMT+3 => GMT+2).
            "2023-10-28T20:00:00Z, 2023-10-28, Asia/Nicosia",
            "2023-10-29T21:00:00Z, 2023-10-29, Asia/Nicosia",

            // Negative values.
            "1901-01-01T23:00:00Z, 1901-01-01, GMT",
            "1901-01-01T23:00:00Z, 1901-01-02, GMT+1",
    })
    public void testCurrentDate(String currentUtcTime, String expectedDateString, String timeZone) {
        ZoneId zoneId = TimeZone.getTimeZone(timeZone).toZoneId();
        ClusterNodeImpl node = new ClusterNodeImpl(randomUUID(), "N1", new NetworkAddress("localhost", 1234));

        ExecutionContext<?> ctx = TestBuilders.executionContext()
                .fragment(Mockito.mock(FragmentDescription.class))
                .executor(Mockito.mock(QueryTaskExecutor.class))
                .localNode(node)
                .clock(Clock.fixed(Instant.parse(currentUtcTime), zoneId))
                .timeZone(zoneId)
                .build();

        int result = IgniteSqlDateTimeUtils.currentDate(ctx);

        LocalDate expected = LocalDate.parse(expectedDateString);
        LocalDate actual = LocalDate.ofEpochDay(result);

        assertThat(actual, equalTo(expected));
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

        long actualTs = IgniteSqlDateTimeUtils.subtractTimeZoneOffset(utcMillis, cyprusTz);

        assertEquals(Instant.ofEpochMilli(expMillis), Instant.ofEpochMilli(actualTs));
    }

    @ParameterizedTest
    @CsvSource({
            "00:00:00,     0, 0",
            "00:00:00,     0, 123",
            "00:00:00.1,   1, 123",
            "00:00:00.12,  2, 123",
            "00:00:00.123, 3, 123",
            "00:00:00.123, 4, 123",
            "00:00:00.123, 5, 123",
            "00:00:00.123, 6, 123",
            "23:59:59.999, 3, 86399999",
            "23:59:59.99,  2, 86399999",
            "23:59:59.9,   1, 86399999",
    })
    public void testTimeToString(String expectedTime, int precision, int millis) {
        assertThat(IgniteSqlDateTimeUtils.unixTimeToString(millis, precision), is(expectedTime));
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
        assertThat(IgniteSqlDateTimeUtils.unixTimestampToString(millis, precision), is(expectedDate));
    }

    @ParameterizedTest
    @MethodSource("validTimes")
    public void testTimeStringToUnixDate(String timeString, int expected) {
        assertThat(IgniteSqlDateTimeUtils.timeStringToUnixDate(timeString), is(expected));
    }

    private static Stream<Arguments> validTimes() {
        return Stream.of(
                Arguments.of("0:0:0", 0),
                // According to the SQL spec (6.13 <cast specification>)
                // leading and trailing spaces in a value must be trimmed.
                Arguments.of("  0:0:1  ", 1000),
                Arguments.of("  00:00:1  ", 1000),
                Arguments.of("  00:00:1.1  ", 1100),
                Arguments.of("  00:00:00.001  ", 1),
                Arguments.of("00:00:00", 0),
                Arguments.of("00:00:00.", 0),
                Arguments.of("00:00:00.1", 100),
                Arguments.of("00:00:00.12", 120),
                Arguments.of("00:00:00.123", 123),
                Arguments.of("00:00:00.1234", 123),
                Arguments.of("00:00:00.12345", 123),
                Arguments.of("00:00:00.123456", 123),
                Arguments.of("00:00:00.1234567", 123),
                Arguments.of("00:00:00.12345678", 123),
                Arguments.of("00:00:00.123456789", 123),
                Arguments.of("23:59:59.999", 86399999),
                Arguments.of("23:59:59.999999999", 86399999)
        );
    }

    @ParameterizedTest
    @MethodSource("invalidTimes")
    public void testInvalidTimeStringToUnixDate(String timeString) {
        IgniteTestUtils.assertThrows(
                IllegalArgumentException.class,
                () -> IgniteSqlDateTimeUtils.timeStringToUnixDate(timeString),
                IgniteStringFormatter.format("Invalid TIME value, '{}'", timeString)
        );
    }

    private static Stream<Arguments> invalidTimes() {
        String[] invalidTimeStrings = {
                "0",
                "00",
                "00:00",
                "00.0",
                "00.00",
                "00:00.1",
                "00: 00:00",
                "00 :00:00",
                "00:00: 00",
                "00:00 :00",
                "0a:00:00",
                "00:0a:00",
                "00:00:0a",
                "a0:00:00",
                "00:a0:00",
                "00:00:a0",
                "00:00:00.a",
                "-10:00:00",
                "-1:00:00",
                "00:-10:00",
                "00:-1:00",
                "00:00:-10",
                "00:00:-1",
                "+10:00:00",
                "+1:00:00",
                "00:+10:00",
                "00:+1:00",
                "00:00:+10",
                "00:00:+1",
                "00:00:00.-1",
                "00:00:00.+1"
        };

        return Arrays.stream(invalidTimeStrings).map(Arguments::of);
    }

    @ParameterizedTest
    @CsvSource({
            "25:00:00,HOUR",
            "125:00:00,HOUR",
            "9999999999999999:00:00,HOUR",
            "00:60:00,MINUTE",
            "00:125:00,MINUTE",
            "00:9999999999999999:00,MINUTE",
            "00:00:60,SECOND",
            "00:00:125,SECOND",
            "00:00:9999999999999999,SECOND",
    })
    public void testOutOfRangeTimeStringToUnixDate(String timeString, String expectedField) {
        IgniteTestUtils.assertThrows(
                IllegalArgumentException.class,
                () -> IgniteSqlDateTimeUtils.timeStringToUnixDate(timeString),
                IgniteStringFormatter.format("Value of {} field is out of range in '{}'", expectedField, timeString)
        );
    }

    @ParameterizedTest
    @CsvSource({
            "1970-01-01 00:00:00,            0",
            "1970-01-01 00:00:00.1,          100",
            "1970-01-01 00:00:00.12,         120",
            "1970-01-01 00:00:00.123,        123",
            "1970-01-01 00:00:00.1234,       123",
            "1970-01-01 00:00:00.12345,      123",
            "1970-01-01 00:00:00.123456,     123",
            "1970-01-01 00:00:00.1234567,    123",
            "1970-01-01 00:00:00.12345678,   123",
            "1970-01-01 00:00:00.123456789,  123",
            "1970-02-01 23:59:59,            2764799000",
            "1970-02-01 23:59:59.04,         2764799040",
            "1969-12-31 23:59:59.999,       -1",
            "1969-12-31 23:59:59.999999999, -1",
            "1969-12-31 23:59:59.98,        -20",
    })
    public void testTimestampStringToUnixDate(String timestampString, long expected) {
        assertThat(IgniteSqlDateTimeUtils.timestampStringToUnixDate(timestampString), is(expected));
    }

    @ParameterizedTest
    @MethodSource("invalidTimestamps")
    public void testInvalidTimestampStringToUnixDate(String timestampString, String expectedPart) {
        IgniteTestUtils.assertThrows(
                IllegalArgumentException.class,
                () -> IgniteSqlDateTimeUtils.timestampStringToUnixDate(timestampString),
                IgniteStringFormatter.format("Invalid {} value, '{}'", expectedPart, timestampString)
        );
    }

    private static List<Arguments> invalidTimestamps() {
        String[] invalidDateStrings = {
                "0001",
                "0001-01",
                "1-01-01",
                "01-01-01",
                "001-01-01",
                "0001-1-01",
                "0001-01-1",
                "00001-01-01",
                "0001-001-01",
                "0001-01-001",
                "000a-01-01",
                "0001-0a-01",
                "0001-01-0a",
                "0001--01-01",
                "0001-01--01",
                "0001- 01-01",
                "0001 -01-01",
                "0001-01 -01",
                "0001-01- 01",
                "0001.01.01",
        };

        List<Arguments> args = new ArrayList<>();

        for (String date : invalidDateStrings) {
            args.add(Arguments.of(date + " 00:00:00", "DATE"));
        }

        invalidTimes().map(arg -> (String) arg.get()[0]).forEach(time -> {
            args.add(Arguments.of("1970-01-01 " + time, "TIME"));
        });

        return args;
    }
}
