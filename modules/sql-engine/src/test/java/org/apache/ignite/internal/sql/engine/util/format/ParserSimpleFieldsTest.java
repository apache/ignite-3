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

import static org.apache.ignite.internal.sql.engine.util.format.DateTimeTemplateField.MI;
import static org.apache.ignite.internal.sql.engine.util.format.DateTimeTemplateField.SS;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.ignite.internal.sql.engine.util.format.DateTimeTemplateField.DateTimeField;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
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
        Map<DateTimeField, Object> fields = val != null ? Map.of(DateTimeField.YEAR, val) : null;

        parseSingleField(pattern, text, fields, error);
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
                Arguments.of("YYYY", "0", null, fieldOutOfRange("year")),
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

        parseSingleField(pattern, text, clock, Map.of(DateTimeField.YEAR, val), null);
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
    public void testParseRoundedYear(String pattern, String text, Integer val, String error) {
        Map<DateTimeField, Object> fields = val != null ? Map.of(DateTimeField.ROUNDED_YEAR, val) : null;

        parseSingleField(pattern, text, fields, error);
    }

    @ParameterizedTest
    @MethodSource("roundedYearValidValues")
    public void testParseRoundedYearUnexpectedDelimiter(String pattern, String text) {
        parseSingleField(pattern, text + "/", null, unexpectedTrailingCharAfter(pattern));
        parseSingleField(pattern, "/" + text, null, expectedField(pattern));
    }

    private static Stream<Arguments> roundedYearValidValues() {
        return yearValues().filter(a -> {
            Object[] args = a.get();
            return args[2] != null;
        }).map(a -> {
            Object[] args = a.get();
            return Arguments.of(args[0], args[1]);
        });
    }

    private static Stream<Arguments> roundedYearValues() {
        return Stream.of(
                Arguments.of("RR", "", null, noValuesForFields("RR")),
                Arguments.of("RR", "+1", null, expectedField("RR")),
                Arguments.of("RR", "0", 2000, null),
                Arguments.of("RR", "1", 2001, null),
                Arguments.of("RR", "50", 1950, null),
                Arguments.of("RR", "51", 1951, null),
                Arguments.of("RR", "77", 1977, null),
                Arguments.of("RR", "99", 1999, null),
                Arguments.of("RR", "123", null, unexpectedTrailingCharAfter("RR")),

                Arguments.of("RRRR", "0", 2000, null),
                Arguments.of("RRRR", "1", 2001, null),
                Arguments.of("RRRR", "10", 2010, null),
                Arguments.of("RRRR", "49", 2049, null),
                Arguments.of("RRRR", "50", 1950, null),
                Arguments.of("RRRR", "51", 1951, null),
                Arguments.of("RRRR", "77", 1977, null),
                Arguments.of("RRRR", "99", 1999, null),
                Arguments.of("RRRR", "199", 199, null),
                Arguments.of("RRRR", "999", 999, null),
                Arguments.of("RRRR", "9999", 9999, null),
                Arguments.of("RRRR", "12345", null, unexpectedTrailingCharAfter("RRRR"))
        );
    }

    @ParameterizedTest
    @MethodSource("monthValues")
    public void testParseMonth(String text, Integer val, String error) {
        Map<DateTimeField, Object> fields = val != null ? Map.of(DateTimeField.MONTH, val) : null;

        parseSingleField("MM", text, fields, error);
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
                Arguments.of("0", null, fieldOutOfRange("month")),
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
                Arguments.of("13", null, fieldOutOfRange("month"))
        );
    }

    @ParameterizedTest
    @MethodSource("dayValues")
    public void testParseDay(String text, Integer val, String error) {
        Map<DateTimeField, Object> fields = val != null ? Map.of(DateTimeField.DAY_OF_MONTH, val) : null;

        parseSingleField("DD", text, fields, error);
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
                Arguments.of("0", null, fieldOutOfRange("day of month")),
                Arguments.of("1", 1, null),
                Arguments.of("13", 13, null),
                Arguments.of("17", 17, null),
                Arguments.of("27", 27, null),
                Arguments.of("28", 28, null),
                Arguments.of("31", 31, null),
                Arguments.of("32", null, fieldOutOfRange("day of month"))
        );
    }

    @ParameterizedTest
    @MethodSource("dayOfYearValues")
    public void testParseDayOfYear(String text, Integer val, String error) {
        Map<DateTimeField, Object> fields = val != null ? Map.of(DateTimeField.DAY_OF_YEAR, val) : null;

        parseSingleField("DDD", text, fields, error);
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
                Arguments.of("0", null, fieldOutOfRange("day of year")),
                Arguments.of("1", 1, null),
                Arguments.of("13", 13, null),
                Arguments.of("37", 37, null),
                Arguments.of("100", 100, null),
                Arguments.of("200", 200, null),
                Arguments.of("365", 365, null),
                Arguments.of("366", null, fieldOutOfRange("day of year"))
        );
    }

    @ParameterizedTest
    @MethodSource("hours12Values")
    public void testParseHours12(String pattern, String text, Integer val, String error) {
        Map<DateTimeField, Object> fields = val != null ? Map.of(DateTimeField.HOUR_12, val) : null;

        parseSingleField(pattern, text, fields, error);
    }

    @ParameterizedTest
    @MethodSource("hours12ValidValues")
    public void testParseHours12UnexpectedDelimiter(String pattern, String text) {
        // Error depend on position of AM
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
                Arguments.of("HHA.M.", "+1A.M.", null, expectedField("HH")),
                Arguments.of("HHA.M.", "0A.M.", 0, null),
                Arguments.of("HHA.M.", "1A.M.", 1, null),
                Arguments.of("HHA.M.", "2A.M.", 2, null),
                Arguments.of("HHA.M.", "7A.M.", 7, null),
                Arguments.of("HHA.M.", "10A.M.", 10, null),
                Arguments.of("HHA.M.", "12A.M.", 12, null),
                Arguments.of("HHA.M.", "13A.M.", null, fieldOutOfRange("12-hour")),
                Arguments.of("HHA.M.", "A.M.", null, expectedField("HH")),

                Arguments.of("A.M.HH", "A.M.0", 0, null),
                Arguments.of("A.M.HH", "A.M.1", 1, null),
                Arguments.of("A.M.HH", "A.M.2", 2, null),
                Arguments.of("A.M.HH", "A.M.7", 7, null),
                Arguments.of("A.M.HH", "A.M.10", 10, null),
                Arguments.of("A.M.HH", "A.M.12", 12, null),
                Arguments.of("A.M.HH", "A.M.13", null, fieldOutOfRange("12-hour")),
                Arguments.of("A.M.HH", "A.M.", null, noValuesForFields("HH")),

                Arguments.of("HH A.M.", "0 A.M.", 0, null),
                Arguments.of("HH A.M.", "9 A.M.", 9, null),
                Arguments.of("HH A.M.", "10 A.M.", 10, null),
                Arguments.of("HH A.M.", "11 A.M.", 11, null),
                Arguments.of("HH A.M.", "12 A.M.", 12, null),
                Arguments.of("HH A.M.", "13 A.M.", null, fieldOutOfRange("12-hour")),
                Arguments.of("HH A.M.", " A.M.", null, expectedField("HH")),

                Arguments.of("HHP.M.", "", null, noValuesForFields("HH", "PM")),

                // AM as PM
                Arguments.of("HHA.M.", "0P.M.", 0, null),
                Arguments.of("HHA.M.", "1P.M.", 1, null),
                Arguments.of("HHA.M.", "10P.M.", 10, null),
                Arguments.of("HH A.M.", "10 P.M.", 10, null),

                Arguments.of("A.M.HH", "P.M.0", 0, null),
                Arguments.of("A.M.HH", "P.M.1", 1, null),
                Arguments.of("A.M.HH", "P.M.10", 10, null),
                Arguments.of("A.M.HH", "P.M.10", 10, null),

                // PM as AM
                Arguments.of("HHP.M.", "0A.M.", 0, null),
                Arguments.of("HHP.M.", "1A.M.", 1, null),
                Arguments.of("HHP.M.", "10A.M.", 10, null),
                Arguments.of("HH P.M.", "10 A.M.", 10, null),

                Arguments.of("P.M.HH", "A.M.0", 0, null),
                Arguments.of("P.M.HH", "A.M.1", 1, null),
                Arguments.of("P.M.HH", "A.M.10", 10, null),
                Arguments.of("P.M. HH", "A.M. 10", 10, null),

                // Incorrect AM/PM
                Arguments.of("HH A.M.", "10 M.M.", null, "Expected A.M./P.M. but got M.M."),
                Arguments.of("HH A.M.", "10 A.T.", null, "Expected A.M./P.M. but got A.T."),
                Arguments.of("HH P.M.", "10 A.T.", null, "Expected A.M./P.M. but got A.T.")
        );
    }

    @ParameterizedTest
    @MethodSource("hour24Values")
    public void testParseHours24(String text, Integer val, String error) {
        Map<DateTimeField, Object> fields = val != null ? Map.of(DateTimeField.HOUR_24, val) : null;

        parseSingleField("HH24", text, fields, error);
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
                Arguments.of("24", null, fieldOutOfRange("24-hour"))
        );
    }

    @ParameterizedTest
    @MethodSource("minutesValues")
    public void testParseMinutes(String text, Integer val, String error) {
        Map<DateTimeField, Object> fields = val != null ? Map.of(DateTimeField.MINUTE, val) : null;

        parseSingleField("MI", text, fields, error);
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
        return values(MI, 59);
    }

    @ParameterizedTest
    @MethodSource("secondsValues")
    public void testParseSeconds(String text, Integer val, String error) {
        Map<DateTimeField, Object> fields = val != null ? Map.of(DateTimeField.SECOND_OF_MINUTE, val) : null;

        parseSingleField("SS", text, fields, error);
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
        return values(SS, 59);
    }

    private static Stream<Arguments> values(DateTimeTemplateField field, int max) {
        Stream<Arguments> s1 = IntStream.rangeClosed(0, max)
                .mapToObj(i -> Arguments.of(i + "", i, null));

        // 0-9 -> pad with 0
        Stream<Arguments> s2 = IntStream.range(0, 10)
                .mapToObj(i -> Arguments.of("0" + i, i, null));

        Stream<Arguments> valid = Stream.concat(
                s1,
                s2
        );
        return Stream.concat(
                valid,
                Stream.of(
                        Arguments.of("-1", null, expectedField(field.asPattern())),
                        Arguments.of(Integer.toString(max + 1), null, fieldOutOfRange(field.displayName())),
                        Arguments.of(Integer.toString(max + 2), null, fieldOutOfRange(field.displayName()))
                )
        );
    }

    @ParameterizedTest
    @MethodSource("fractionValues")
    public void testParseFractions(String pattern, String text, Integer val, String error) {
        Map<DateTimeField, Object> fields = val != null ? Map.of(DateTimeField.FRACTION, val) : null;

        parseSingleField(pattern, text, fields, error);
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
        if (offset != null) {
            parseSingleField(format, text, Map.of(DateTimeField.TIMEZONE, offset), error);
        } else {
            parseSingleField(format, text, null, error);
        }
    }

    @ParameterizedTest
    @MethodSource("timeZoneValues")
    public void testParseTimeZoneSwapped(String format, String text, ZoneOffset offset, String error) {
        String[] elements = format.split(":");
        String[] values = text.split(":");
        String swappedFormat = elements[1] + ":" + elements[0];
        String swappedText = values[1] + ":" + values[0];

        if (offset != null) {
            parseSingleField(swappedFormat, swappedText, Map.of(DateTimeField.TIMEZONE, offset), error);
        } else {
            parseSingleField(swappedFormat, swappedText, null, error);
        }
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
                Arguments.of("TZH:TZM", "+13:60", null, fieldOutOfRange("time zone minutes")),
                Arguments.of("TZH:TZM", "+10:0", ZoneOffset.ofHoursMinutes(10, 0), null),
                Arguments.of("TZH:TZM", "+10:12", ZoneOffset.ofHoursMinutes(10, 12), null),
                Arguments.of("TZH:TZM", "+17:43", ZoneOffset.ofHoursMinutes(17, 43), null),
                Arguments.of("TZH:TZM", "+18:0", ZoneOffset.ofHoursMinutes(18, 0), null),
                Arguments.of("TZH:TZM", "+18:1", null, invalidValue("time zone")),
                Arguments.of("TZH:TZM", "+19:0", null, invalidValue("time zone")),
                Arguments.of("TZH:TZM", "+19:59", null, invalidValue("time zone")),
                Arguments.of("TZH:TZM", "+19:60", null, fieldOutOfRange("time zone minutes")),

                // Positive
                Arguments.of("TZH:TZM", "-0:0", ZoneOffset.ofHoursMinutes(0, 0), null),
                Arguments.of("TZH:TZM", "-0:1", ZoneOffset.ofHoursMinutes(0, -1), null),
                Arguments.of("TZH:TZM", "-0:10", ZoneOffset.ofHoursMinutes(0, -10), null),
                Arguments.of("TZH:TZM", "-5:0", ZoneOffset.ofHoursMinutes(-5, 0), null),
                Arguments.of("TZH:TZM", "-5:3", ZoneOffset.ofHoursMinutes(-5, -3), null),
                Arguments.of("TZH:TZM", "-5:45", ZoneOffset.ofHoursMinutes(-5, -45), null),
                Arguments.of("TZH:TZM", "-13:60", null, fieldOutOfRange("time zone minutes")),
                Arguments.of("TZH:TZM", "-10:0", ZoneOffset.ofHoursMinutes(-10, 0), null),
                Arguments.of("TZH:TZM", "-10:12", ZoneOffset.ofHoursMinutes(-10, -12), null),
                Arguments.of("TZH:TZM", "-17:43", ZoneOffset.ofHoursMinutes(-17, -43), null),
                Arguments.of("TZH:TZM", "-18:0", ZoneOffset.ofHoursMinutes(-18, 0), null),
                Arguments.of("TZH:TZM", "-18:1", null, invalidValue("time zone")),
                Arguments.of("TZH:TZM", "-19:0", null, invalidValue("time zone")),
                Arguments.of("TZH:TZM", "-19:59", null, invalidValue("time zone")),
                Arguments.of("TZH:TZM", "-19:60", null, fieldOutOfRange("time zone minutes")),

                // Error depends on field ordering:
                // Invalid format. Expected literal <:> but got <1>
                //  Unexpected trailing characters after TZH when swapped
                Arguments.of("TZH:TZM", "+001:20", null, " "),
                Arguments.of("TZH:TZM", "+000:20", null, " "),

                // Invalid format. Expected literal <:> but got <0>
                // Unexpected trailing characters after TZH when swapped
                Arguments.of("TZH:TZM", "+1:000", null, " "),
                Arguments.of("TZH:TZM", "+1:001", null, " "),


                Arguments.of("TZH:TZM", "1:20", null, "Expected +/-"),
                Arguments.of("TZM:TZH", "10:1", null, "Expected +/-"),

                // Error depends on field ordering
                // Invalid value for field time zone minutes when swapped
                // Expected +/- but got: 1
                Arguments.of("TZH:TZM", "1:-20", null, " "),

                Arguments.of("TZH:TZM", "+1:+20", null, expectedField("TZM")),
                Arguments.of("TZH:TZM", "+1:-20", null, expectedField("TZM")),
                Arguments.of("TZM:TZH", "+10:-1", null, expectedField("TZM"))
        );
    }

    private static String fieldOutOfRange(String f) {
        return "Field value is out of range " + f;
    }

    private static String expectedField(String f) {
        return "Expected field " + f;
    }

    private static String invalidValue(String f) {
        return "Invalid value for field " + f;
    }

    private static String unexpectedTrailingCharAfter(String f) {
        return "Unexpected trailing characters after " + f;
    }

    private static String noValuesForFields(String... f) {
        return "No values for fields: " + Arrays.asList(f);
    }

    private static void parseSingleField(
            String pattern,
            String text,
            @Nullable Map<DateTimeField, Object> fields,
            @Nullable String error
    ) {
        parseSingleField(pattern, text, FIXED_CLOCK, fields, error);
    }

    private static void parseSingleField(
            String pattern,
            String text,
            Clock clock,
            @Nullable Map<DateTimeField, Object> fields,
            @Nullable String error
    ) {
        List<DateTimeFormatElement> elements = new Scanner(pattern).parse();
        Parser parser = new Parser(elements);

        if (fields != null) {
            ParsedFields parsedText = parser.parse(text, clock);

            assertEquals(fields, parsedText.fields());
        } else {
            try {
                ParsedFields parsedText = parser.parse(text);
                fail("Expected an error but got " + parsedText.fields());
            } catch (DateTimeFormatException e) {
                e.printStackTrace(System.err);
                assertThat("Error message: ", e.getMessage(), containsString(error));
            }
        }
    }
}
