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

package org.apache.ignite.internal.sql.engine.util.format;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoField;
import java.time.temporal.TemporalAccessor;
import java.util.stream.Stream;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Single field tests for {@link Formatter}.
 */
public class FormatterSimpleFieldsTest extends BaseIgniteAbstractTest {

    @ParameterizedTest
    @MethodSource("yearValues")
    public void testFormatYear(String pattern, int year, String expected) {
        LocalDate date = LocalDate.now().withYear(year);

        formatSimpleField(pattern, expected, date);
    }

    private static Stream<Arguments> yearValues() {
        return Stream.of(
                Arguments.of("Y", 0, "0"),
                Arguments.of("Y", 1, "1"),
                Arguments.of("Y", 10, "0"),
                Arguments.of("Y", 29, "9"),

                Arguments.of("YY", 0, "00"),
                Arguments.of("YY", 1, "01"),
                Arguments.of("YY", 10, "10"),
                Arguments.of("YY", 29, "29"),

                Arguments.of("YYY", 0, "000"),
                Arguments.of("YYY", 1, "001"),
                Arguments.of("YYY", 29, "029"),
                Arguments.of("YYY", 290, "290"),
                Arguments.of("YYY", 223, "223"),
                Arguments.of("YYY", 2001, "001"),
                Arguments.of("YYY", 2023, "023"),

                Arguments.of("YYYY", 0, "0000"),
                Arguments.of("YYYY", 1, "0001"),
                Arguments.of("YYYY", 29, "0029"),
                Arguments.of("YYYY", 290, "0290"),
                Arguments.of("YYYY", 223, "0223"),
                Arguments.of("YYYY", 2001, "2001"),
                Arguments.of("YYYY", 2023, "2023")
        );
    }

    @ParameterizedTest
    @MethodSource("roundedYearValues")
    public void testFormatRoundedYear(String pattern, int year, String expected) {
        LocalDate date = LocalDate.now().withYear(year);

        formatSimpleField(pattern, expected, date);
    }

    private static Stream<Arguments> roundedYearValues() {
        return Stream.of(
                Arguments.of("RR", 0, "00"),
                Arguments.of("RR", 1, "01"),
                Arguments.of("RR", 10, "10"),
                Arguments.of("RR", 29, "29"),
                Arguments.of("RR", 290, "90"),
                Arguments.of("RR", 223, "23"),
                Arguments.of("RR", 1950, "50"),
                Arguments.of("RR", 2049, "49"),

                Arguments.of("RRRR", 290, "0290"),
                Arguments.of("RRRR", 223, "0223"),
                Arguments.of("RRRR", 1950, "1950"),
                Arguments.of("RRRR", 2000, "2000"),
                Arguments.of("RRRR", 2005, "2005"),
                Arguments.of("RRRR", 2023, "2023")
        );
    }

    @ParameterizedTest
    @MethodSource("monthValues")
    public void testFormatMonth(String pattern, int month, String expected) {
        LocalDate date = LocalDate.now().withMonth(month);

        formatSimpleField(pattern, expected, date);
    }

    private static Stream<Arguments> monthValues() {
        return Stream.of(
                Arguments.of("MM", 1, "01"),
                Arguments.of("MM", 2, "02"),
                Arguments.of("MM", 3, "03"),
                Arguments.of("MM", 4, "04"),
                Arguments.of("MM", 5, "05"),
                Arguments.of("MM", 6, "06"),
                Arguments.of("MM", 7, "07"),
                Arguments.of("MM", 8, "08"),
                Arguments.of("MM", 9, "09"),
                Arguments.of("MM", 10, "10"),
                Arguments.of("MM", 11, "11"),
                Arguments.of("MM", 12, "12")
        );
    }

    @ParameterizedTest
    @MethodSource("dayValues")
    public void testFormatDay(String pattern, int day, String expected) {
        LocalDate date = LocalDate.now().withMonth(12).withDayOfMonth(day);

        formatSimpleField(pattern, expected, date);
    }

    private static Stream<Arguments> dayValues() {
        return Stream.of(
                Arguments.of("DD", 1, "01"),
                Arguments.of("DD", 3, "03"),
                Arguments.of("DD", 10, "10"),
                Arguments.of("DD", 17, "17"),
                Arguments.of("DD", 20, "20"),
                Arguments.of("DD", 29, "29"),
                Arguments.of("DD", 30, "30"),
                Arguments.of("DD", 31, "31")
        );
    }

    @ParameterizedTest
    @MethodSource("dayOfYearValues")
    public void testFormatDayOfYear(String pattern, int day, String expected) {
        LocalDate date = LocalDate.ofYearDay(2001, day);

        formatSimpleField(pattern, expected, date);
    }

    private static Stream<Arguments> dayOfYearValues() {
        return Stream.of(
                Arguments.of("DDD", 1, "001"),
                Arguments.of("DDD", 13, "013"),
                Arguments.of("DDD", 100, "100"),
                Arguments.of("DDD", 257, "257"),
                Arguments.of("DDD", 365, "365")
        );
    }

    @ParameterizedTest
    @MethodSource("hoursValues")
    public void testFormatHours(String pattern, int hours, String expected) {
        LocalTime time = LocalTime.now().withHour(hours);

        formatSimpleField(pattern, expected, time);
    }

    @ParameterizedTest
    @MethodSource("hoursValues")
    public void testFormatHours12(String pattern, int hours, String expected) {
        LocalTime time = LocalTime.now().withHour(hours);

        formatSimpleField(pattern.replace("HH", "HH12"), expected, time);
    }

    private static Stream<Arguments> hoursValues() {
        return Stream.of(
                // AM
                Arguments.of("HHA.M.", 0, "12A.M."),
                Arguments.of("HHA.M.", 1, "01A.M."),
                Arguments.of("HHA.M.", 9, "09A.M."),
                Arguments.of("HHA.M.", 10, "10A.M."),
                Arguments.of("HHA.M.", 12, "12P.M."),
                Arguments.of("HHA.M.", 13, "01P.M."),
                Arguments.of("HHA.M.", 17, "05P.M."),
                Arguments.of("HHA.M.", 23, "11P.M."),

                // P.M.
                Arguments.of("HHP.M.", 0, "12A.M."),
                Arguments.of("HHP.M.", 1, "01A.M."),
                Arguments.of("HHP.M.", 9, "09A.M."),
                Arguments.of("HHP.M.", 10, "10A.M."),
                Arguments.of("HHP.M.", 12, "12P.M."),
                Arguments.of("HHP.M.", 13, "01P.M."),
                Arguments.of("HHP.M.", 17, "05P.M."),
                Arguments.of("HHP.M.", 23, "11P.M.")
        );
    }

    @ParameterizedTest
    @MethodSource("hours24Values")
    public void testFormatHours24(String pattern, int hours, String expected) {
        LocalTime time = LocalTime.now().withHour(hours);

        formatSimpleField(pattern, expected, time);
    }

    private static Stream<Arguments> hours24Values() {
        return Stream.of(
                Arguments.of("HH24", 0, "00"),
                Arguments.of("HH24", 1, "01"),
                Arguments.of("HH24", 9, "09"),
                Arguments.of("HH24", 10, "10"),
                Arguments.of("HH24", 12, "12"),
                Arguments.of("HH24", 17, "17"),
                Arguments.of("HH24", 23, "23")
        );
    }

    @ParameterizedTest
    @MethodSource("minuteValues")
    public void testFormatMinutes(String pattern, int minutes, String expected) {
        LocalTime time = LocalTime.now().withMinute(minutes);

        formatSimpleField(pattern, expected, time);
    }

    private static Stream<Arguments> minuteValues() {
        return Stream.of(
                Arguments.of("MI", 0, "00"),
                Arguments.of("MI", 1, "01"),
                Arguments.of("MI", 9, "09"),
                Arguments.of("MI", 10, "10"),
                Arguments.of("MI", 17, "17"),
                Arguments.of("MI", 59, "59")
        );
    }

    @ParameterizedTest
    @MethodSource("secondValues")
    public void testFormatSeconds(String pattern, int seconds, String expected) {
        LocalTime time = LocalTime.now().withSecond(seconds);

        formatSimpleField(pattern, expected, time);
    }

    private static Stream<Arguments> secondValues() {
        return Stream.of(
                Arguments.of("SS", 0, "00"),
                Arguments.of("SS", 1, "01"),
                Arguments.of("SS", 9, "09"),
                Arguments.of("SS", 10, "10"),
                Arguments.of("SS", 17, "17"),
                Arguments.of("SS", 59, "59")
        );
    }

    @ParameterizedTest
    @MethodSource("secondOfDayValues")
    public void testFormatSecondsOfDay(String pattern, int seconds, String expected) {
        LocalTime time = LocalTime.now().with(ChronoField.SECOND_OF_DAY, seconds);

        formatSimpleField(pattern, expected, time);
    }

    private static Stream<Arguments> secondOfDayValues() {
        return Stream.of(
                Arguments.of("SSSSS", 0, "00000"),
                Arguments.of("SSSSS", 1, "00001"),
                Arguments.of("SSSSS", 9, "00009"),
                Arguments.of("SSSSS", 10, "00010"),
                Arguments.of("SSSSS", 86, "00086"),
                Arguments.of("SSSSS", 863, "00863"),
                Arguments.of("SSSSS", 8639, "08639"),
                Arguments.of("SSSSS", 86399, "86399")
        );
    }

    @ParameterizedTest
    @MethodSource("fractionValues")
    public void testFormatFractions(String pattern, int nanos, String expected) {
        LocalTime time = LocalTime.of(0, 0, 0, 0).withNano(nanos);

        formatSimpleField(pattern, expected, time);
    }

    private static Stream<Arguments> fractionValues() {
        return Stream.of(
                Arguments.of("FF1", 0, "0"),
                Arguments.of("FF1", 100_000_000, "1"),
                Arguments.of("FF1", 260_000_000, "2"),
                Arguments.of("FF1", 999_000_000, "9"),

                Arguments.of("FF2", 0, "00"),
                Arguments.of("FF2", 100_000_000, "10"),
                Arguments.of("FF2", 260_000_000, "26"),
                Arguments.of("FF2", 999_000_000, "99"),

                Arguments.of("FF3", 0, "000"),
                Arguments.of("FF3", 100_000_000, "100"),
                Arguments.of("FF3", 120_000_000, "120"),
                Arguments.of("FF3", 123_000_000, "123"),
                Arguments.of("FF3", 123_500_000, "123"),
                Arguments.of("FF3", 999_990_000, "999"),

                Arguments.of("FF4", 0, "0000"),
                Arguments.of("FF4", 100_000_000, "1000"),
                Arguments.of("FF4", 120_000_000, "1200"),
                Arguments.of("FF4", 123_000_000, "1230"),
                Arguments.of("FF4", 123_400_000, "1234"),
                Arguments.of("FF4", 123_450_000, "1234"),
                Arguments.of("FF4", 999_990_000, "9999"),

                Arguments.of("FF5", 0, "00000"),
                Arguments.of("FF5", 100_000_000, "10000"),
                Arguments.of("FF5", 123_000_000, "12300"),
                Arguments.of("FF5", 123_400_000, "12340"),
                Arguments.of("FF5", 123_450_000, "12345"),
                Arguments.of("FF5", 123_456_000, "12345"),
                Arguments.of("FF5", 999_999_900, "99999"),

                Arguments.of("FF6", 0, "000000"),
                Arguments.of("FF6", 100_000_000, "100000"),
                Arguments.of("FF6", 123_000_000, "123000"),
                Arguments.of("FF6", 123_400_000, "123400"),
                Arguments.of("FF6", 123_450_000, "123450"),
                Arguments.of("FF6", 123_456_700, "123456"),
                Arguments.of("FF6", 999_999_900, "999999"),

                Arguments.of("FF7", 0, "0000000"),
                Arguments.of("FF7", 100_000_000, "1000000"),
                Arguments.of("FF7", 123_000_000, "1230000"),
                Arguments.of("FF7", 123_400_000, "1234000"),
                Arguments.of("FF7", 123_450_000, "1234500"),
                Arguments.of("FF7", 123_456_000, "1234560"),
                Arguments.of("FF7", 123_456_700, "1234567"),
                Arguments.of("FF7", 123_456_780, "1234567"),
                Arguments.of("FF7", 999_999_990, "9999999"),

                Arguments.of("FF8", 0, "00000000"),
                Arguments.of("FF8", 100_000_000, "10000000"),
                Arguments.of("FF8", 123_000_000, "12300000"),
                Arguments.of("FF8", 123_400_000, "12340000"),
                Arguments.of("FF8", 123_450_000, "12345000"),
                Arguments.of("FF8", 123_456_000, "12345600"),
                Arguments.of("FF8", 123_456_700, "12345670"),
                Arguments.of("FF8", 123_456_780, "12345678"),
                Arguments.of("FF8", 123_456_789, "12345678"),
                Arguments.of("FF8", 999_999_999, "99999999"),

                Arguments.of("FF9", 0, "000000000"),
                Arguments.of("FF9", 100_000_000, "100000000"),
                Arguments.of("FF9", 123_000_000, "123000000"),
                Arguments.of("FF9", 123_400_000, "123400000"),
                Arguments.of("FF9", 123_450_000, "123450000"),
                Arguments.of("FF9", 123_456_000, "123456000"),
                Arguments.of("FF9", 123_456_700, "123456700"),
                Arguments.of("FF9", 123_456_780, "123456780"),
                Arguments.of("FF9", 123_456_789, "123456789"),
                Arguments.of("FF9", 999_999_999, "999999999")
        );
    }

    @ParameterizedTest
    @MethodSource("timeZoneValues")
    public void testFormatTimeZone(String pattern, ZoneOffset offset, String expected) {
        LocalTime time = LocalTime.now();

        formatSimpleField(pattern, expected, time, offset);
    }

    private static Stream<Arguments> timeZoneValues() {
        return Stream.of(
                Arguments.of("TZH:TZM", ZoneOffset.ofHoursMinutes(0, 0), "+00:00"),
                Arguments.of("TZH:TZM", ZoneOffset.ofHoursMinutes(1, 15), "+01:15"),
                Arguments.of("TZH:TZM", ZoneOffset.ofHoursMinutes(16, 59), "+16:59"),

                Arguments.of("TZH:TZM", ZoneOffset.ofHoursMinutes(-1, 0), "-01:00"),
                Arguments.of("TZH:TZM", ZoneOffset.ofHoursMinutes(-1, -15), "-01:15"),
                Arguments.of("TZH:TZM", ZoneOffset.ofHoursMinutes(-16, -59), "-16:59")
        );
    }

    private void formatSimpleField(String pattern, String expected, TemporalAccessor value) {
        Formatter formatter = new Formatter(new Scanner(pattern).scan());

        log.info("Pattern: {}", pattern);
        log.info("Result: {}", expected);

        assertEquals(expected, formatter.format(value, ZoneOffset.UTC));
    }

    private void formatSimpleField(String pattern, String expected, TemporalAccessor value, ZoneOffset offset) {
        Formatter formatter = new Formatter(new Scanner(pattern).scan());

        log.info("Pattern: {}", pattern);
        log.info("Result: {}", expected);

        assertEquals(expected, formatter.format(value, offset));
    }
}
