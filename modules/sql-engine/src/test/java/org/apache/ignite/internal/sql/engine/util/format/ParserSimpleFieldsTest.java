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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import java.time.Clock;
import java.time.DateTimeException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.ignite.internal.sql.engine.util.format.DateTimeTemplateField.FieldKind;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Single field tests for {@link Parser}.
 */
class ParserSimpleFieldsTest extends BaseIgniteAbstractTest {

    // Fix the clock, because the results of parsing year fields are time dependent.
    private static final Clock FIXED_CLOCK = Clock.fixed(Instant.parse("2025-01-01T00:00:00.000Z"), ZoneId.of("UTC"));

    @ParameterizedTest
    @MethodSource("yearValues")
    public void testParseYear(String pattern, String text, Integer val, String error) {
        LocalDate date = val != null ? LocalDate.now(FIXED_CLOCK).withYear(val) : null;

        parseSingleField(pattern, text, ParsedFields::getDate, date, error);
    }

    @ParameterizedTest
    @MethodSource("yearValuesValid")
    public void testParseYearUnexpectedDelimiter(String pattern, String text) {
        parseSingleField(pattern, text + "/", null, unexpectedTrailingCharAfter(pattern));
        parseSingleField(pattern, "/" + text, null, expectedField(pattern));
    }

    private static Stream<Arguments> yearValuesValid() {
        return yearValues().filter(a -> {
            Object[] args = a.get();
            return args[2] != null;
        }).map(a -> {
            Object[] args = a.get();
            return Arguments.of(args[0], args[1]);
        });
    }

    private static Stream<Arguments> yearValues() {
        return Stream.of(
                Arguments.of("Y", "", null, noValuesForFields("Y")),
                Arguments.of("Y", "+1", null, expectedField("Y")),

                Arguments.of("Y", "0", 2020, null),
                Arguments.of("Y", "1", 2021, null),
                Arguments.of("Y", "9", 2029, null),
                Arguments.of("Y", "10", null, unexpectedTrailingCharAfter("Y")),

                Arguments.of("YY", "", null, noValuesForFields("YY")),
                Arguments.of("YY", "+1", null, expectedField("YY")),
                Arguments.of("YY", "0", 2000, null),
                Arguments.of("YY", "1", 2001, null),
                Arguments.of("YY", "9", 2009, null),
                Arguments.of("YY", "12", 2012, null),
                Arguments.of("YY", "99", 2099, null),
                Arguments.of("YY", "100", null, unexpectedTrailingCharAfter("YY")),

                Arguments.of("YYY", "", null, noValuesForFields("YYY")),
                Arguments.of("YYY", "+1", null, expectedField("YYY")),
                Arguments.of("YYY", "0", 2000, null),
                Arguments.of("YYY", "1", 2001, null),
                Arguments.of("YYY", "9", 2009, null),
                Arguments.of("YYY", "12", 2012, null),
                Arguments.of("YYY", "99", 2099, null),
                Arguments.of("YYY", "123", 2123, null),
                Arguments.of("YYY", "999", 2999, null),
                Arguments.of("YYY", "1000", null, unexpectedTrailingCharAfter("YYY")),

                Arguments.of("YYYY", "", null, noValuesForFields("YYYY")),
                Arguments.of("YYYY", "+1", null, expectedField("YYYY")),
                Arguments.of("YYYY", "0", null, valueOutOfRange("Year")),
                Arguments.of("YYYY", "1", 1, null),
                Arguments.of("YYYY", "9", 9, null),
                Arguments.of("YYYY", "12", 12, null),
                Arguments.of("YYYY", "99", 99, null),
                Arguments.of("YYYY", "123", 123, null),
                Arguments.of("YYYY", "999", 999, null),
                Arguments.of("YYYY", "1234", 1234, null),
                Arguments.of("YYYY", "9999", 9999, null),
                Arguments.of("YYYY", "10000", null, unexpectedTrailingCharAfter("YYYY"))
        );
    }

    @ParameterizedTest
    @MethodSource("yearValuesAnotherDecade")
    public void testParseYearAnotherDecade(String pattern, String text, Integer val) {
        Clock clock = Clock.fixed(Instant.parse("2030-01-01T00:00:00.000Z"), ZoneId.of("UTC"));
        LocalDate date = val != null ? LocalDate.now(clock).withYear(val) : null;

        parseSingleField(pattern, text, clock, ParsedFields::getDate, date, null);
    }

    private static Stream<Arguments> yearValuesAnotherDecade() {
        return Stream.of(
                Arguments.of("Y", "0", 2030, null),
                Arguments.of("Y", "1", 2031, null),

                Arguments.of("YY", "0", 2000, null),
                Arguments.of("YY", "1", 2001, null),
                Arguments.of("YY", "12", 2012, null),

                Arguments.of("YYY", "0", 2000, null),
                Arguments.of("YYY", "1", 2001, null),
                Arguments.of("YYY", "12", 2012, null),
                Arguments.of("YYY", "123", 2123, null),

                Arguments.of("YYYY", "1", 1, null),
                Arguments.of("YYYY", "2", 2, null),
                Arguments.of("YYYY", "12", 12, null),
                Arguments.of("YYYY", "123", 123, null),
                Arguments.of("YYYY", "2000", 2000, null),
                Arguments.of("YYYY", "2031", 2031, null)
        );
    }

    @ParameterizedTest
    @MethodSource("roundedYearValues")
    public void testParseRoundedYear(int currentYear, String pattern, String text, Integer val, String error) {
        Clock clock = Clock.fixed(Instant.parse(currentYear + "-01-01T00:00:00.0Z"), ZoneId.systemDefault());
        LocalDate date = val != null ? LocalDate.now(clock).withYear(val) : null;

        parseSingleField(pattern, text, clock, ParsedFields::getDate, date, error);
    }

    @ParameterizedTest
    @MethodSource("roundedYearValidValues")
    public void testParseRoundedYearUnexpectedDelimiter(int currentYear, String pattern, String text) {
        Clock clock = Clock.fixed(Instant.parse(currentYear + "-01-01T00:00:00.0Z"), ZoneId.systemDefault());

        parseSingleField(pattern, text + "/", clock, null, unexpectedTrailingCharAfter(pattern));
        parseSingleField(pattern, "/" + text, clock, null, expectedField(pattern));
    }

    private static Stream<Arguments> roundedYearValidValues() {
        return roundedYearValues().filter(a -> {
            Object[] args = a.get();
            return args[3] != null;
        }).map(a -> {
            Object[] args = a.get();
            return Arguments.of(args[0], args[1], args[2]);
        });
    }

    private static Stream<Arguments> roundedYearValues() {
        return Stream.of(
                // RR
                Arguments.of(2000, "RR", "", null, noValuesForFields("RR")),
                Arguments.of(2000, "RR", "+1", null, expectedField("RR")),

                Arguments.of(1999, "RR", "0", 2000, null),
                Arguments.of(1999, "RR", "1", 2001, null),
                Arguments.of(1999, "RR", "49", 2049, null),
                Arguments.of(1999, "RR", "50", 1950, null),
                Arguments.of(1999, "RR", "77", 1977, null),
                Arguments.of(1999, "RR", "99", 1999, null),
                Arguments.of(1999, "RR", "123", null, unexpectedTrailingCharAfter("RR")),

                Arguments.of(2025, "RR", "0", 2000, null),
                Arguments.of(2025, "RR", "1", 2001, null),
                Arguments.of(2025, "RR", "49", 2049, null),
                Arguments.of(2025, "RR", "50", 1950, null),
                Arguments.of(2025, "RR", "77", 1977, null),
                Arguments.of(2025, "RR", "99", 1999, null),
                Arguments.of(2025, "RR", "123", null, unexpectedTrailingCharAfter("RR")),

                Arguments.of(2101, "RR", "0", 2100, null),
                Arguments.of(2101, "RR", "1", 2101, null),
                Arguments.of(2101, "RR", "50", 2050, null),
                Arguments.of(2101, "RR", "77", 2077, null),
                Arguments.of(2101, "RR", "99", 2099, null),
                Arguments.of(2101, "RR", "123", null, unexpectedTrailingCharAfter("RR")),

                // RRRR

                Arguments.of(2000, "RRRR", "", null, noValuesForFields("RRRR")),
                Arguments.of(2000, "RRRR", "+1", null, expectedField("RRRR")),

                Arguments.of(1999, "RRRR", "0", 2000, null),
                Arguments.of(1999, "RRRR", "1", 2001, null),
                Arguments.of(1999, "RRRR", "49", 2049, null),
                Arguments.of(1999, "RRRR", "049", 2049, null),
                Arguments.of(1999, "RRRR", "50", 1950, null),
                Arguments.of(1999, "RRRR", "050", 1950, null),
                Arguments.of(1999, "RRRR", "77", 1977, null),
                Arguments.of(1999, "RRRR", "077", 1977, null),
                Arguments.of(1999, "RRRR", "99", 1999, null),
                Arguments.of(1999, "RRRR", "099", 1999, null),
                Arguments.of(1999, "RRRR", "123", 123, null),
                Arguments.of(1999, "RRRR", "9999", 9999, null),
                Arguments.of(1999, "RRRR", "12345", null, unexpectedTrailingCharAfter("RRRR")),

                Arguments.of(2025, "RRRR", "0", 2000, null),
                Arguments.of(2025, "RRRR", "1", 2001, null),
                Arguments.of(2025, "RRRR", "49", 2049, null),
                Arguments.of(2025, "RRRR", "049", 2049, null),
                Arguments.of(2025, "RRRR", "50", 1950, null),
                Arguments.of(2025, "RRRR", "050", 1950, null),
                Arguments.of(2025, "RRRR", "77", 1977, null),
                Arguments.of(2025, "RRRR", "077", 1977, null),
                Arguments.of(2025, "RRRR", "99", 1999, null),
                Arguments.of(2025, "RRRR", "099", 1999, null),
                Arguments.of(2025, "RRRR", "123", 123, null),
                Arguments.of(2025, "RRRR", "9999", 9999, null),
                Arguments.of(2025, "RRRR", "12345", null, unexpectedTrailingCharAfter("RRRR")),

                Arguments.of(2101, "RRRR", "0", 2100, null),
                Arguments.of(2101, "RRRR", "1", 2101, null),
                Arguments.of(2101, "RRRR", "49", 2149, null),
                Arguments.of(2101, "RRRR", "049", 2149, null),
                Arguments.of(2101, "RRRR", "50", 2050, null),
                Arguments.of(2101, "RRRR", "050", 2050, null),
                Arguments.of(2101, "RRRR", "77", 2077, null),
                Arguments.of(2101, "RRRR", "077", 2077, null),
                Arguments.of(2101, "RRRR", "99", 2099, null),
                Arguments.of(2101, "RRRR", "099", 2099, null),
                Arguments.of(2101, "RRRR", "9999", 9999, null),
                Arguments.of(2101, "RRRR", "12345", null, unexpectedTrailingCharAfter("RRRR"))
        );
    }

    @ParameterizedTest
    @MethodSource("monthValues")
    public void testParseMonth(String text, Integer val, String error) {
        LocalDate date = val != null ? LocalDate.now(FIXED_CLOCK).withMonth(val) : null;

        parseSingleField("MM", text, ParsedFields::getDate, date, error);
    }

    @ParameterizedTest
    @MethodSource("monthValuesValid")
    public void testParseMonthUnexpectedDelimiter(String text) {
        parseSingleField("MM", text + "/", null, unexpectedTrailingCharAfter("MM"));
        parseSingleField("MM", "/" + text, null, expectedField("MM"));
    }

    private static Stream<Arguments> monthValuesValid() {
        return monthValues().filter(a -> {
            Object[] args = a.get();
            return args[1] != null;
        }).map(a -> {
            Object[] args = a.get();
            return Arguments.of(args[0]);
        });
    }

    private static Stream<Arguments> monthValues() {
        return Stream.of(
                Arguments.of("", null, noValuesForFields("MM")),
                Arguments.of("+1", null, expectedField("MM")),
                Arguments.of("0", null, valueOutOfRange("MonthOfYear")),
                Arguments.of("1", 1, null),
                Arguments.of("2", 2, null),
                Arguments.of("3", 3, null),
                Arguments.of("4", 4, null),
                Arguments.of("5", 5, null),
                Arguments.of("6", 6, null),
                Arguments.of("7", 7, null),
                Arguments.of("8", 8, null),
                Arguments.of("9", 9, null),
                Arguments.of("10", 10, null),
                Arguments.of("11", 11, null),
                Arguments.of("12", 12, null),
                Arguments.of("13", null, valueOutOfRange("MonthOfYear")),
                Arguments.of("012", null, unexpectedTrailingCharAfter("MM"))
        );
    }

    @ParameterizedTest
    @MethodSource("dayValues")
    public void testParseDay(String text, Integer val, String error) {
        LocalDate date = val != null ? LocalDate.now(FIXED_CLOCK).withDayOfMonth(val) : null;

        parseSingleField("DD", text, ParsedFields::getDate, date, error);
    }

    @ParameterizedTest
    @MethodSource("dayValidValues")
    public void testParseDayUnexpectedDelimiter(String text) {
        parseSingleField("DD", text + "/", null, unexpectedTrailingCharAfter("DD"));
        parseSingleField("DD", "/" + text, null, expectedField("DD"));
    }

    private static Stream<Arguments> dayValidValues() {
        return dayValues().filter(a -> {
            Object[] args = a.get();
            return args[1] != null;
        }).map(a -> {
            Object[] args = a.get();
            return Arguments.of(args[0]);
        });
    }

    private static Stream<Arguments> dayValues() {
        return Stream.of(
                Arguments.of("", null, noValuesForFields("DD")),
                Arguments.of("+1", null, expectedField("DD")),
                Arguments.of("0", null, valueOutOfRange("DayOfMonth")),
                Arguments.of("1", 1, null),
                Arguments.of("13", 13, null),
                Arguments.of("17", 17, null),
                Arguments.of("27", 27, null),
                Arguments.of("28", 28, null),
                Arguments.of("31", 31, null),
                Arguments.of("32", null, valueOutOfRange("DayOfMonth")),
                Arguments.of("030", null, unexpectedTrailingCharAfter("DD"))
        );
    }

    @ParameterizedTest
    @MethodSource("dayOfYearValues")
    public void testParseDayOfYear(String text, Integer val, String error) {
        LocalDate date = val != null ? LocalDate.now(FIXED_CLOCK).withDayOfYear(val) : null;

        parseSingleField("DDD", text, ParsedFields::getDate, date, error);
    }

    @ParameterizedTest
    @MethodSource("dayOfYearValidValues")
    public void testParseDayOfYearUnexpectedDelimiter(String text) {
        parseSingleField("DDD", text + "/", null, unexpectedTrailingCharAfter("DDD"));
        parseSingleField("DDD", "/" + text, null, expectedField("DDD"));
    }

    private static Stream<Arguments> dayOfYearValidValues() {
        return dayOfYearValues().filter(a -> {
            Object[] args = a.get();
            return args[1] != null;
        }).map(a -> {
            Object[] args = a.get();
            return Arguments.of(args[0]);
        });
    }

    private static Stream<Arguments> dayOfYearValues() {
        return Stream.of(
                Arguments.of("", null, noValuesForFields("DDD")),
                Arguments.of("+1", null, expectedField("DDD")),
                Arguments.of("0", null, valueOutOfRange("DayOfYear")),
                Arguments.of("1", 1, null),
                Arguments.of("13", 13, null),
                Arguments.of("37", 37, null),
                Arguments.of("100", 100, null),
                Arguments.of("200", 200, null),
                Arguments.of("365", 365, null),
                Arguments.of("366", null, "Invalid date 'DayOfYear 366' as '2025' is not a leap year"),
                Arguments.of("0123", null, unexpectedTrailingCharAfter("DDD"))
        );
    }

    @ParameterizedTest
    @MethodSource("hours12Values")
    public void testParseHours12(String pattern, String text, Integer val, String error) {
        LocalTime time = val != null ? LocalTime.of(val, 0, 0) : null;

        parseSingleField(pattern, text, (parsedFields, ignore) -> parsedFields.getTime(), time, error);
    }

    @ParameterizedTest
    @MethodSource("hours12ValidValues")
    public void testParseHours12UnexpectedDelimiter(String pattern, String text) {
        // Error depend on position of AM
        parseSingleField(pattern, text + "/", null, " ");
        parseSingleField(pattern, "/" + text, null, " ");
    }

    @ParameterizedTest
    @MethodSource("hours12Values")
    public void testParseHours12pmAm(String pattern, String text, Integer val, String error) {
        // Pattern HH A.M. accepts both 11 A.M. (11 hours) and 11 P.M. (converts it to 23 hours)
        pattern = pattern.replace("A.M.", "P.M.");
        error = error != null ? error.replace("AM", "PM") : error;

        LocalTime time = val != null ? LocalTime.of(val, 0, 0) : null;

        parseSingleField(pattern, text, (parsedFields, ignore) -> parsedFields.getTime(), time, error);
    }

    @ParameterizedTest
    @MethodSource("hours12ValidValues")
    public void testParseHours12pmAmUnexpectedDelimiter(String pattern, String text) {
        // Pattern HH A.M. accepts both 11 A.M.
        pattern = pattern.replace("A.M.", "P.M.");

        // Error depend on position of AM
        parseSingleField(pattern, text + "/", null, " ");
        parseSingleField(pattern, "/" + text, null, " ");
    }

    @ParameterizedTest
    @MethodSource("hours12Values")
    public void testParseHours12h12(String pattern, String text, Integer val, String error) {
        LocalTime time = val != null ? LocalTime.of(val, 0, 0) : null;

        pattern = pattern.replace("HH", "HH12");
        error = error != null ? error.replace("HH", "HH12") : null;

        parseSingleField(pattern, text, (parsedFields, ignore) -> parsedFields.getTime(), time, error);
    }

    @ParameterizedTest
    @MethodSource("hours12ValidValues")
    public void testParseHours12h12UnexpectedDelimiter(String pattern, String text) {
        // Error depend on position of AM
        pattern = pattern.replace("HH", "HH12");
        parseSingleField(pattern, text + "/", null, " ");
        parseSingleField(pattern, "/" + text, null, " ");
    }

    @ParameterizedTest
    @MethodSource("hours12Values")
    public void testParseHours12h12pmAm(String pattern, String text, Integer val, String error) {
        LocalTime time = val != null ? LocalTime.of(val, 0, 0) : null;

        pattern = pattern.replace("HH", "HH12");
        error = error != null ? error.replace("HH", "HH12") : null;

        // Pattern HH A.M. accepts both 11 A.M.
        pattern = pattern.replace("A.M.", "P.M.");
        error = error != null ? error.replace("AM", "PM") : error;

        parseSingleField(pattern, text, (parsedFields, ignore) -> parsedFields.getTime(), time, error);
    }

    @ParameterizedTest
    @MethodSource("hours12ValidValues")
    public void testParseHours12h12pmAmUnexpectedDelimiter(String pattern, String text) {
        // Error depend on position of AM
        pattern = pattern.replace("HH", "HH12");
        // Pattern HH A.M. accepts both 11 A.M.
        pattern = pattern.replace("A.M.", "P.M.");

        parseSingleField(pattern, text + "/", null, " ");
        parseSingleField(pattern, "/" + text, null, " ");
    }

    private static Stream<Arguments> hours12ValidValues() {
        return hours12Values().filter(a -> {
            Object[] args = a.get();
            return args[1] != null;
        }).map(a -> {
            Object[] args = a.get();
            return Arguments.of(args[0], args[1]);
        });
    }

    private static Stream<Arguments> hours12Values() {
        return Stream.of(
                Arguments.of("HHA.M.", "", null, noValuesForFields("HH", "AM")),

                // A.M.

                Arguments.of("HHA.M.", "+1A.M.", null, expectedField("HH")),

                Arguments.of("HHA.M.", "0A.M.", null, valueOutOfRange("HourAmPm")),
                Arguments.of("HHA.M.", "1A.M.", 1, null),
                Arguments.of("HHA.M.", "7A.M.", 7, null),
                Arguments.of("HHA.M.", "10A.M.", 10, null),
                Arguments.of("HHA.M.", "11A.M.", 11, null),
                Arguments.of("HHA.M.", "12A.M.", 0, null),
                Arguments.of("HHA.M.", "13A.M.", null, valueOutOfRange("HourAmPm")),

                Arguments.of("HHA.M.", "10AM", null, expectedField("AM")),
                Arguments.of("HHA.M.", "A.M.", null, expectedField("HH")),
                Arguments.of("HHA.M.", "10A.", null, expectedField("AM")),
                Arguments.of("HHA.M.", "10A.M", null, expectedField("AM")),

                // P.M.

                Arguments.of("HHA.M.", "+1P.M.", null, expectedField("HH")),

                Arguments.of("HHA.M.", "0P.M.", null, valueOutOfRange("HourAmPm")),
                Arguments.of("HHA.M.", "1P.M.", 13, null),
                Arguments.of("HHA.M.", "7P.M.", 19, null),
                Arguments.of("HHA.M.", "10P.M.", 22, null),
                Arguments.of("HHA.M.", "11P.M.", 23, null),
                Arguments.of("HHA.M.", "12P.M.", 12, null),
                Arguments.of("HHA.M.", "13P.M.", null, valueOutOfRange("HourAmPm")),

                // Incorrect
                Arguments.of("HH A.M.", "10 M.M.", null, expectedField("AM")),
                Arguments.of("HH A.M.", "10 A.T.", null, expectedField("AM")),
                Arguments.of("HH A.M.", "10 A.T.", null, expectedField("AM"))
        );
    }

    @ParameterizedTest
    @CsvSource(value = {
            "7 A.m., 7",
            "7 a.M., 7",
            "7 a.m., 7",
            "7 P.m., 19",
            "7 p.M., 19",
            "7 p.m., 19"
    })
    public void testParseHour12AmPmCaseInsensitivity(String text, int hours24) {
        Scanner scanner = new Scanner("HH12 A.M.");
        Parser parser = new Parser(scanner.scan());
        ParsedFields fields = parser.parse(text);

        assertEquals(LocalTime.of(hours24, 0), fields.getTime());
    }

    @ParameterizedTest
    @MethodSource("hour24Values")
    public void testParseHours24(String text, Integer val, String error) {
        LocalTime time = val != null ? LocalTime.of(val, 0, 0) : null;

        parseSingleField("HH24", text, (parsedFields, ignore) -> parsedFields.getTime(), time, error);
    }

    @ParameterizedTest
    @MethodSource("hours24Valid")
    public void testParseHours24UnexpectedDelimiter(String text) {
        parseSingleField("HH24", text + "/", null, unexpectedTrailingCharAfter("HH24"));
        parseSingleField("HH24", "/" + text, null, expectedField("HH24"));
    }

    private static Stream<Arguments> hours24Valid() {
        return hour24Values().filter(a -> {
            Object[] args = a.get();
            return args[1] != null;
        }).map(a -> {
            Object[] args = a.get();
            return Arguments.of(args[0], args[1]);
        });
    }

    private static Stream<Arguments> hour24Values() {
        return Stream.of(
                Arguments.of("", null, noValuesForFields("HH24")),
                Arguments.of("+1", null, expectedField("HH24")),
                Arguments.of("0", 0, null),
                Arguments.of("1", 1, null),
                Arguments.of("2", 2, null),
                Arguments.of("3", 3, null),
                Arguments.of("4", 4, null),
                Arguments.of("5", 5, null),
                Arguments.of("6", 6, null),
                Arguments.of("7", 7, null),
                Arguments.of("8", 8, null),
                Arguments.of("9", 9, null),
                Arguments.of("10", 10, null),
                Arguments.of("11", 11, null),
                Arguments.of("12", 12, null),
                Arguments.of("13", 13, null),
                Arguments.of("14", 14, null),
                Arguments.of("15", 15, null),
                Arguments.of("16", 16, null),
                Arguments.of("17", 17, null),
                Arguments.of("18", 18, null),
                Arguments.of("19", 19, null),
                Arguments.of("20", 20, null),
                Arguments.of("21", 21, null),
                Arguments.of("22", 22, null),
                Arguments.of("23", 23, null),
                Arguments.of("24", null, valueOutOfRange("HourOfDay")),
                Arguments.of("25", null, valueOutOfRange("HourOfDay")),
                Arguments.of("000", null, unexpectedTrailingCharAfter("HH24"))
        );
    }

    @ParameterizedTest
    @MethodSource("minutesValues")
    public void testParseMinutes(String text, Integer val, String error) {
        LocalTime time = val != null ? LocalTime.of(0, val, 0) : null;

        parseSingleField("MI", text, (parsedFields, ignore) -> parsedFields.getTime(), time, error);
    }

    @ParameterizedTest
    @MethodSource("minutesValidValues")
    public void testParseMinutesUnexpectedDelimiter(String text) {
        parseSingleField("MI", text + "/", null, unexpectedTrailingCharAfter("MI"));
        parseSingleField("MI", "/" + text, null, expectedField("MI"));
    }

    private static Stream<Arguments> minutesValidValues() {
        return minutesValues().filter(a -> {
            Object[] args = a.get();
            return args[1] != null;
        }).map(a -> {
            Object[] args = a.get();
            return Arguments.of(args[0], args[1]);
        });
    }

    private static Stream<Arguments> minutesValues() {
        return Stream.of(
                Arguments.of("", null, noValuesForFields("MI")),
                Arguments.of("+1", null, expectedField("MI")),
                Arguments.of("-1", null, expectedField("MI")),
                Arguments.of("0", 0, null),
                Arguments.of("00", 0, null),
                Arguments.of("1", 1, null),
                Arguments.of("01", 1, null),
                Arguments.of("09", 9, null),
                Arguments.of("10", 10, null),
                Arguments.of("37", 37, null),
                Arguments.of("59", 59, null),
                Arguments.of("60", null, valueOutOfRange("MinuteOfHour")),
                Arguments.of("000", null, unexpectedTrailingCharAfter("MI"))
        );
    }

    @ParameterizedTest
    @MethodSource("secondsValues")
    public void testParseSeconds(String text, Integer val, String error) {
        LocalTime time = val != null ? LocalTime.of(0, 0, val) : null;

        parseSingleField("SS", text, (parsedFields, ignore) -> parsedFields.getTime(), time, error);
    }

    @ParameterizedTest
    @MethodSource("secondValuesValid")
    public void testParseSecondsUnexpectedDelimiter(String text) {
        parseSingleField("SS", text + "/", null, unexpectedTrailingCharAfter("SS"));
        parseSingleField("SS", "/" + text, null, expectedField("SS"));
    }

    private static Stream<Arguments> secondValuesValid() {
        return secondsValues().filter(a -> {
            Object[] args = a.get();
            return args[1] != null;
        }).map(a -> {
            Object[] args = a.get();
            return Arguments.of(args[0], args[1]);
        });
    }

    private static Stream<Arguments> secondsValues() {
        return Stream.of(
                Arguments.of("", null, noValuesForFields("SS")),
                Arguments.of("+1", null, expectedField("SS")),
                Arguments.of("-1", null, expectedField("SS")),
                Arguments.of("0", 0, null),
                Arguments.of("00", 0, null),
                Arguments.of("1", 1, null),
                Arguments.of("01", 1, null),
                Arguments.of("09", 9, null),
                Arguments.of("10", 10, null),
                Arguments.of("37", 37, null),
                Arguments.of("59", 59, null),
                Arguments.of("60", null, valueOutOfRange("SecondOfMinute")),
                Arguments.of("000", null, unexpectedTrailingCharAfter("SS"))
        );
    }

    @ParameterizedTest
    @MethodSource("secondsOfDayValues")
    public void testParseSecondsOfDay(String text, Integer val, String error) {
        LocalTime time = val != null ? LocalTime.ofSecondOfDay(val) : null;

        parseSingleField("SSSSS", text, (parsedFields, ignore) -> parsedFields.getTime(), time, error);
    }

    @ParameterizedTest
    @MethodSource("secondsOfDayValuesValid")
    public void testParseSecondsOfDayUnexpectedDelimiter(String text) {
        parseSingleField("SSSSS", text + "/", null, unexpectedTrailingCharAfter("SSSSS"));
        parseSingleField("SSSSS", "/" + text, null, expectedField("SSSSS"));
    }

    private static Stream<Arguments> secondsOfDayValuesValid() {
        return secondsValues().filter(a -> {
            Object[] args = a.get();
            return args[1] != null;
        }).map(a -> {
            Object[] args = a.get();
            return Arguments.of(args[0], args[1]);
        });
    }

    private static Stream<Arguments> secondsOfDayValues() {
        return Stream.of(
                Arguments.of("", null, noValuesForFields("SSSSS")),
                Arguments.of("+1", null, expectedField("SSSSS")),
                Arguments.of("-1", null, expectedField("SSSSS")),
                Arguments.of("0", 0, null),
                Arguments.of("00000", 0, null),
                Arguments.of("1", 1, null),
                Arguments.of("01", 1, null),
                Arguments.of("09", 9, null),
                Arguments.of("370", 370, null),
                Arguments.of("059", 59, null),
                Arguments.of("00008", 8, null),
                Arguments.of("00086", 86, null),
                Arguments.of("00864", 864, null),
                Arguments.of("08640", 8640, null),
                Arguments.of("86399", 86399, null),
                Arguments.of("86401", null, valueOutOfRange("SecondOfDay")),
                Arguments.of("90000", null, valueOutOfRange("SecondOfDay")),
                Arguments.of("010000", null, unexpectedTrailingCharAfter("SSSSS"))
        );
    }

    @ParameterizedTest
    @MethodSource("fractionValues")
    public void testParseFractions(String pattern, String text, Integer val, String error) {
        LocalTime time = val != null ? LocalTime.ofNanoOfDay(val) : null;

        parseSingleField(pattern, text, (parsedFields, ignore) -> parsedFields.getTime(), time, error);
    }

    @ParameterizedTest
    @MethodSource("fractionValuesValid")
    public void testParseFractionsUnexpectedDelimiter(String pattern, String text) {
        parseSingleField(pattern, text + "/", null, unexpectedTrailingCharAfter(pattern));
        parseSingleField(pattern, "/" + text, null, "Expected field " + pattern);
    }

    private static Stream<Arguments> fractionValuesValid() {
        return fractionValues().filter(a -> {
            Object[] args = a.get();
            return args[2] != null;
        }).map(a -> {
            Object[] args = a.get();
            return Arguments.of(args[0], args[1]);
        });
    }

    private static Stream<Arguments> fractionValues() {
        return Stream.of(
                // Millis
                Arguments.of("FF1", "1", 100_000_000, null),
                Arguments.of("FF1", "10", null, unexpectedTrailingCharAfter("FF1")),

                Arguments.of("FF2", "1", 100_000_000, null),
                Arguments.of("FF2", "12", 120_000_000, null),
                Arguments.of("FF2", "100", null, unexpectedTrailingCharAfter("FF2")),

                Arguments.of("FF3", "1", 100_000_000, null),
                Arguments.of("FF3", "12", 120_000_000, null),
                Arguments.of("FF3", "123", 123_000_000, null),
                Arguments.of("FF3", "1000", null, unexpectedTrailingCharAfter("FF3")),

                // Micros
                Arguments.of("FF4", "1", 100_000_000, null),
                Arguments.of("FF4", "12", 120_000_000, null),
                Arguments.of("FF4", "123", 123_000_000, null),
                Arguments.of("FF4", "1234", 123_400_000, null),
                Arguments.of("FF4", "10000", null, unexpectedTrailingCharAfter("FF4")),

                Arguments.of("FF5", "1", 100_000_000, null),
                Arguments.of("FF5", "12", 120_000_000, null),
                Arguments.of("FF5", "123", 123_000_000, null),
                Arguments.of("FF5", "1234", 123_400_000, null),
                Arguments.of("FF5", "12345", 123_450_000, null),
                Arguments.of("FF5", "100000", null, unexpectedTrailingCharAfter("FF5")),

                Arguments.of("FF6", "1", 100_000_000, null),
                Arguments.of("FF6", "12", 120_000_000, null),
                Arguments.of("FF6", "123", 123_000_000, null),
                Arguments.of("FF6", "1234", 123_400_000, null),
                Arguments.of("FF6", "12345", 123_450_000, null),
                Arguments.of("FF6", "123456", 123_456_000, null),
                Arguments.of("FF6", "1000000", null, unexpectedTrailingCharAfter("FF6")),

                // Nanos
                Arguments.of("FF7", "1", 100_000_000, null),
                Arguments.of("FF7", "12", 120_000_000, null),
                Arguments.of("FF7", "123", 123_000_000, null),
                Arguments.of("FF7", "1234", 123_400_000, null),
                Arguments.of("FF7", "12345", 123_450_000, null),
                Arguments.of("FF7", "123456", 123_456_000, null),
                Arguments.of("FF7", "1234567", 123_456_700, null),
                Arguments.of("FF7", "10000000", null, unexpectedTrailingCharAfter("FF7")),

                Arguments.of("FF8", "1", 100_000_000, null),
                Arguments.of("FF8", "12", 120_000_000, null),
                Arguments.of("FF8", "123", 123_000_000, null),
                Arguments.of("FF8", "1234", 123_400_000, null),
                Arguments.of("FF8", "12345", 123_450_000, null),
                Arguments.of("FF8", "123456", 123_456_000, null),
                Arguments.of("FF8", "1234567", 123_456_700, null),
                Arguments.of("FF8", "12345678", 123_456_780, null),
                Arguments.of("FF8", "100000000", null, unexpectedTrailingCharAfter("FF8")),

                Arguments.of("FF9", "1", 100_000_000, null),
                Arguments.of("FF9", "12", 120_000_000, null),
                Arguments.of("FF9", "123", 123_000_000, null),
                Arguments.of("FF9", "1234", 123_400_000, null),
                Arguments.of("FF9", "12345", 123_450_000, null),
                Arguments.of("FF9", "123456", 123_456_000, null),
                Arguments.of("FF9", "1234567", 123_456_700, null),
                Arguments.of("FF9", "12345678", 123_456_780, null),
                Arguments.of("FF9", "123456789", 123_456_789, null),
                Arguments.of("FF9", "1000000000", null, unexpectedTrailingCharAfter("FF9"))
        );
    }

    @ParameterizedTest
    @MethodSource("timeZoneValues")
    public void testParseTimeZone(String format, String text, ZoneOffset offset, String error) {
        parseSingleField(format, text, (fs, c) -> fs.toZoneOffset(), offset, error);
    }

    @ParameterizedTest
    @MethodSource("timeZoneValues")
    public void testParseTimeZoneSwapped(String format, String text, ZoneOffset offset, String error) {
        String[] elements = format.split(":");
        String[] values = text.split(":");
        String swappedFormat = elements[1] + ":" + elements[0];
        String swappedText = values[1] + ":" + values[0];

        parseSingleField(swappedFormat, swappedText, (fs, c) -> fs.toZoneOffset(), offset, error);
    }

    private static Stream<Arguments> timeZoneValues() {
        return Stream.of(
                // Positive
                Arguments.of("TZH:TZM", "+0:0", ZoneOffset.ofHoursMinutes(0, 0), null),
                Arguments.of("TZH:TZM", "+0:1", ZoneOffset.ofHoursMinutes(0, 1), null),
                Arguments.of("TZH:TZM", "+0:10", ZoneOffset.ofHoursMinutes(0, 10), null),
                Arguments.of("TZH:TZM", "+5:0", ZoneOffset.ofHoursMinutes(5, 0), null),
                Arguments.of("TZH:TZM", "+5:3", ZoneOffset.ofHoursMinutes(5, 3), null),
                Arguments.of("TZH:TZM", "+5:45", ZoneOffset.ofHoursMinutes(5, 45), null),
                Arguments.of("TZH:TZM", "+13:60", null, valueOutOfRange("TimeZone MinuteOfHour")),
                Arguments.of("TZH:TZM", "+10:0", ZoneOffset.ofHoursMinutes(10, 0), null),
                Arguments.of("TZH:TZM", "+10:12", ZoneOffset.ofHoursMinutes(10, 12), null),
                Arguments.of("TZH:TZM", "+17:43", ZoneOffset.ofHoursMinutes(17, 43), null),
                Arguments.of("TZH:TZM", "+18:0", ZoneOffset.ofHoursMinutes(18, 0), null),
                Arguments.of("TZH:TZM", "+18:1", null, invalidTimeZoneValue()),
                Arguments.of("TZH:TZM", "+19:0", null, invalidTimeZoneValue()),
                Arguments.of("TZH:TZM", "+19:59", null, invalidTimeZoneValue()),
                Arguments.of("TZH:TZM", "+19:60", null, valueOutOfRange("TimeZone MinuteOfHour")),

                // Positive
                Arguments.of("TZH:TZM", "-0:0", ZoneOffset.ofHoursMinutes(0, 0), null),
                Arguments.of("TZH:TZM", "-0:1", ZoneOffset.ofHoursMinutes(0, -1), null),
                Arguments.of("TZH:TZM", "-0:10", ZoneOffset.ofHoursMinutes(0, -10), null),
                Arguments.of("TZH:TZM", "-5:0", ZoneOffset.ofHoursMinutes(-5, 0), null),
                Arguments.of("TZH:TZM", "-5:3", ZoneOffset.ofHoursMinutes(-5, -3), null),
                Arguments.of("TZH:TZM", "-5:45", ZoneOffset.ofHoursMinutes(-5, -45), null),
                Arguments.of("TZH:TZM", "-13:60", null, valueOutOfRange("TimeZone MinuteOfHour")),
                Arguments.of("TZH:TZM", "-10:0", ZoneOffset.ofHoursMinutes(-10, 0), null),
                Arguments.of("TZH:TZM", "-10:12", ZoneOffset.ofHoursMinutes(-10, -12), null),
                Arguments.of("TZH:TZM", "-17:43", ZoneOffset.ofHoursMinutes(-17, -43), null),
                Arguments.of("TZH:TZM", "-18:0", ZoneOffset.ofHoursMinutes(-18, 0), null),
                Arguments.of("TZH:TZM", "-18:1", null, invalidTimeZoneValue()),
                Arguments.of("TZH:TZM", "-19:0", null, invalidTimeZoneValue()),
                Arguments.of("TZH:TZM", "-19:59", null, invalidTimeZoneValue()),
                Arguments.of("TZH:TZM", "-19:60", null, valueOutOfRange("TimeZone MinuteOfHour")),

                // Error depends on field ordering:
                // Invalid format. Expected literal <:> but got <1>
                //  Unexpected trailing characters after TZH when swapped
                Arguments.of("TZH:TZM", "+001:20", null, " "),
                Arguments.of("TZH:TZM", "+000:20", null, " "),

                // Invalid format. Expected literal <:> but got <0>
                // Unexpected trailing characters after TZH when swapped
                Arguments.of("TZH:TZM", "+1:000", null, " "),
                Arguments.of("TZH:TZM", "+1:001", null, " "),

                Arguments.of("TZH:TZM", "1:20", null, expectedField("TZH")),
                Arguments.of("TZM:TZH", "10:1", null, expectedField("TZH")),

                // Error depends on field ordering
                // Invalid value for field TZM when swapped
                // Expected +/- but got: 1
                Arguments.of("TZH:TZM", "1:-20", null, " "),

                Arguments.of("TZH:TZM", "+1:+20", null, expectedField("TZM")),
                Arguments.of("TZH:TZM", "+1:-20", null, expectedField("TZM")),
                Arguments.of("TZM:TZH", "+10:-1", null, expectedField("TZM"))
        );
    }

    private static String valueOutOfRange(String f) {
        return "Invalid value for " + f;
    }

    private static String expectedField(String f) {
        return "Expected field " + f;
    }

    private static String invalidTimeZoneValue() {
        return "Zone offset";
    }

    private static String unexpectedTrailingCharAfter(String f) {
        return "Unexpected trailing characters after field " + f;
    }

    private static String noValuesForFields(String... f) {
        return "No values for elements " + Arrays.stream(f).map(f0 -> "field " + f0).collect(Collectors.joining(", "));
    }

    private void parseSingleField(
            String pattern,
            String text,
            @Nullable Map<FieldKind, Object> fields,
            @Nullable String error
    ) {
        parseSingleField(pattern, text, FIXED_CLOCK, (fs, c) -> fs, fields, error);
    }

    private void parseSingleField(
            String pattern,
            String text,
            Clock clock,
            @Nullable Object expected,
            @Nullable String error
    ) {
        parseSingleField(pattern, text, clock, (fs, c) -> fs, expected, error);
    }

    private void parseSingleField(
            String pattern,
            String text,
            @Nullable BiFunction<ParsedFields, Clock, Object> func,
            @Nullable Object expected,
            @Nullable String error
    ) {
        parseSingleField(pattern, text, FIXED_CLOCK, func, expected, error);
    }

    private void parseSingleField(
            String pattern,
            String text,
            Clock clock,
            @Nullable BiFunction<ParsedFields, Clock, Object> func,
            @Nullable Object expected,
            @Nullable String error
    ) {
        List<DateTimeFormatElement> elements = new Scanner(pattern).scan();
        Parser parser = new Parser(elements, clock);

        log.info("Pattern: {}", pattern);
        log.info("Result: {}", expected);
        log.info("Error: {}", error);

        if (expected != null) {
            ParsedFields parsedText = parser.parse(text);
            Object actual = func.apply(parsedText, clock);

            assertEquals(expected, actual);
        } else {
            try {
                ParsedFields parsedText = parser.parse(text);
                Object unexpectedVal = func.apply(parsedText, clock);
                fail("Expected an error but got " + unexpectedVal);
            } catch (DateTimeException e) {
                assertThat("Error message: ", e.getMessage(), containsString(error));
            }
        }
    }
}
