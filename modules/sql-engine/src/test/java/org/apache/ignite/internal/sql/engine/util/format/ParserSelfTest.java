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

import static org.apache.ignite.internal.sql.engine.util.format.DateTimeTemplateField.AM;
import static org.apache.ignite.internal.sql.engine.util.format.DateTimeTemplateField.DD;
import static org.apache.ignite.internal.sql.engine.util.format.DateTimeTemplateField.FF4;
import static org.apache.ignite.internal.sql.engine.util.format.DateTimeTemplateField.HH;
import static org.apache.ignite.internal.sql.engine.util.format.DateTimeTemplateField.MI;
import static org.apache.ignite.internal.sql.engine.util.format.DateTimeTemplateField.MM;
import static org.apache.ignite.internal.sql.engine.util.format.DateTimeTemplateField.SS;
import static org.apache.ignite.internal.sql.engine.util.format.DateTimeTemplateField.TZH;
import static org.apache.ignite.internal.sql.engine.util.format.DateTimeTemplateField.TZM;
import static org.apache.ignite.internal.sql.engine.util.format.DateTimeTemplateField.YYYY;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;

import java.time.Clock;
import java.time.DateTimeException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Basic tests for {@link Parser}.
 */
class ParserSelfTest extends BaseIgniteAbstractTest {

    // Fix the clock, because the results of parsing year fields are time dependent.
    private static final Clock FIXED_CLOCK = Clock.fixed(Instant.parse("2025-01-01T00:00:00.000Z"), ZoneId.of("UTC"));

    @ParameterizedTest
    @MethodSource("basicPatterns")
    public void testBasicPatterns(String pattern, String value, LocalDateTime dateTime, ZoneOffset offset) {
        Scanner scanner = new Scanner(pattern);
        Parser parser = new Parser(scanner.scan());
        ParsedFields parsedFields = parser.parse(value);

        assertEquals(dateTime, parsedFields.getDateTime(FIXED_CLOCK));
        assertEquals(offset, parsedFields.toZoneOffset());
    }

    private static Stream<Arguments> basicPatterns() {
        return Stream.of(
                Arguments.of("YYYY MM DD", "2024 10 01", LocalDateTime.of(LocalDate.of(2024, 10, 1), LocalTime.ofSecondOfDay(0)), null),
                Arguments.of("YYYY-MM-DD", "2024-10-01", LocalDateTime.of(LocalDate.of(2024, 10, 1), LocalTime.ofSecondOfDay(0)), null),
                Arguments.of("YYYY DDD", "2024 1", LocalDateTime.of(LocalDate.of(2024, 1, 1), LocalTime.ofSecondOfDay(0)), null),
                Arguments.of("YYYY DDD", "2024 50", LocalDateTime.of(LocalDate.of(2024, 2, 19), LocalTime.ofSecondOfDay(0)), null),
                Arguments.of("YYYY:DDD", "2024:100", LocalDateTime.of(LocalDate.of(2024, 4, 9), LocalTime.ofSecondOfDay(0)), null),
                Arguments.of("YYYY/DDD", "2024/365", LocalDateTime.of(LocalDate.of(2024, 12, 30), LocalTime.ofSecondOfDay(0)), null),
                Arguments.of("YYYYDDD", "20243", LocalDateTime.of(LocalDate.of(2024, 1, 3), LocalTime.ofSecondOfDay(0)), null),
                Arguments.of("YYYYDDD", "202436", LocalDateTime.of(LocalDate.of(2024, 2, 5), LocalTime.ofSecondOfDay(0)), null),

                // TIME
                Arguments.of("HH24:MI:SS.FF3", "3:7:9.12",
                        LocalDateTime.of(LocalDate.of(2025, 1, 1), LocalTime.of(3, 7, 9, 120_000_000)), null),

                Arguments.of("HH24:MI:SS.FF3 TZH:TZM", "3:7:9.12 +3:30",
                        LocalDateTime.of(LocalDate.of(2025, 1, 1), LocalTime.of(3, 7, 9, 120_000_000)),
                        ZoneOffset.ofHoursMinutes(3, 30)),

                Arguments.of("HH24:MI:SS.FF5 TZH:TZM", "3:7:9.9995 -10:50",
                        LocalDateTime.of(LocalDate.of(2025, 1, 1), LocalTime.of(3, 7, 9, 999_500_000)),
                        ZoneOffset.ofHoursMinutes(-10, -50)),

                // YEAR + TIME
                Arguments.of("YYYY-MM-DD HH24:MI:SS.FF3", "2024-10-01 3:7:9.12",
                        LocalDateTime.of(LocalDate.of(2024, 10, 1), LocalTime.of(3, 7, 9, 120_000_000)), null),

                Arguments.of("YYYY-MM-DD HH24:MI:SS.FF3 TZH:TZM", "2024-10-01 3:7:9.12 +3:30",
                        LocalDateTime.of(LocalDate.of(2024, 10, 1), LocalTime.of(3, 7, 9, 120_000_000)),
                        ZoneOffset.ofHoursMinutes(3, 30)),

                Arguments.of("YYYY-MM-DD/HH12:MI:SS.FF3 A.M.", "2024-10-01/3:7:9.12 A.M.",
                        LocalDateTime.of(LocalDate.of(2024, 10, 1), LocalTime.of(3, 7, 9, 120_000_000)), null)

        );
    }

    @ParameterizedTest
    @MethodSource("basicPatterns")
    public void testBasicPatternsCaseInsensitivity(String pattern, String value, LocalDateTime fields, ZoneOffset offset) {
        Scanner scanner = new Scanner(pattern);
        Parser parser = new Parser(scanner.scan());
        ParsedFields parsedFields = parser.parse(value.toLowerCase(Locale.US));

        assertEquals(fields, parsedFields.getDateTime(FIXED_CLOCK));
        assertEquals(offset, parsedFields.toZoneOffset());
    }

    @ParameterizedTest
    @MethodSource("hour12Patterns")
    public void testHour12Patterns(String pattern, String value, LocalTime expected) {
        Scanner scanner = new Scanner(pattern);
        Parser parser = new Parser(scanner.scan());

        if (expected != null) {
            ParsedFields parsedFields = parser.parse(value);

            assertEquals(expected, parsedFields.getTime());
        } else {
            try {
                ParsedFields parsedFields = parser.parse(value);
                LocalTime time = parsedFields.getTime();
                fail("Unexpected fields: " + time + " offset: " + time);
            } catch (DateTimeException e) {
                assertThat(e.getMessage(), containsString("Invalid value for"));
            }
        }
    }

    private static Stream<Arguments> hour12Patterns() {
        return Stream.of(
                // Out of range
                Arguments.of("HH12:MI:SS.FF3 A.M.", "0:7:9.12 A.M.", null),

                // A.M.
                Arguments.of("HH12:MI:SS.FF3 A.M.", "3:7:9.12 A.M.", LocalTime.of(3, 7, 9, 120_000_000)),
                Arguments.of("HH12:MI:SS.FF3 A.M.", "11:59:9.12 A.M.", LocalTime.of(11, 59, 9, 120_000_000)),

                // P.M.

                // Out of range
                Arguments.of("HH12:MI:SS.FF3 P.M.", "0:7:9.12 A.M.", null),
                Arguments.of("HH12:MI:SS.FF3 P.M.", "12:59:9.12 P.M.", LocalTime.of(12, 59, 9, 120_000_000)),
                Arguments.of("HH12:MI:SS.FF3 P.M.", "11:59:9.12 P.M.", LocalTime.of(23, 59, 9, 120_000_000))
        );
    }

    @ParameterizedTest
    @MethodSource("basicInvalidPatterns")
    public void testBasicInvalidPatterns(String pattern, String text, String error) {
        DateTimeException err = parseAndThrow(pattern, text);
        assertThat(err.getMessage(), containsString(error));
    }

    private DateTimeException parseAndThrow(String pattern, String text) {
        Scanner scanner = new Scanner(pattern);
        Parser parser = new Parser(scanner.scan());

        return assertThrows(DateTimeException.class, () -> {
            ParsedFields fields = parser.parse(text);
            fields.getDateTime(FIXED_CLOCK);
            fields.toZoneOffset();
        });
    }

    private static Stream<Arguments> basicInvalidPatterns() {
        return Stream.of(
                // Leading space
                Arguments.of(" YYY", "100", "Expected literal < > but got <1>"),
                // Trailing space
                Arguments.of("YYY ", "100", "No values for elements delimiter < >"),

                Arguments.of("YYYY", "100g", "Unexpected trailing characters after field YYYY"),
                Arguments.of("YYYY/MM", "200020", "Invalid format. Expected literal </> but got <2>"),
                Arguments.of("YYYYMM", "2000XX", "Expected field MM but got <X>"),
                Arguments.of("YYYY/MM", "g2000/20", "Expected field YYYY but got <g>"),
                Arguments.of("YYYY/MM", "2000[20", "Invalid format. Expected literal </> but got <[>"),

                Arguments.of("YYYYMM", "200013", "Invalid value for MonthOfYear"),
                Arguments.of("HH24:MI", "25:50", "Invalid value for HourOfDay"),
                Arguments.of("HH24:MI", "22:60", "Invalid value for MinuteOfHour"),

                Arguments.of("HH24:MI TZH:TZM", "22:40 +:0", "Expected field TZH but got <+>"),
                Arguments.of("HH24:MI TZMTZH", "22:40 0+", "Expected field TZH but got <+>"),
                Arguments.of("HH24:MI TZMTZH", "22:40 0-", "Expected field TZH but got <->")
        );
    }

    @ParameterizedTest
    @MethodSource("simpleValuesValid")
    public void testUnexpectedLeadingDelimiters(String pattern, String value) {
        Scanner scanner = new Scanner(pattern);
        Parser parser = new Parser(scanner.scan());
        DateTimeException e = assertThrows(DateTimeException.class, () -> parser.parse("/" + value));
        assertThat(e.getMessage(), containsString("Expected field " + pattern + " but got </>"));
    }

    @ParameterizedTest
    @MethodSource("simpleValuesValid")
    public void testUnexpectedTrailingDelimiters(String pattern, String value) {
        Scanner scanner = new Scanner(pattern);
        Parser parser = new Parser(scanner.scan());
        DateTimeException e = assertThrows(DateTimeException.class, () -> parser.parse(value + "/"));
        assertThat(e.getMessage(), containsString("Unexpected trailing characters after field"));
    }

    private static Stream<Arguments> simpleValuesValid() {
        return Stream.of(
                Arguments.of("YY", "1"),
                Arguments.of("YY", "10"),
                Arguments.of("DD", "1"),
                Arguments.of("DD", "10"),
                Arguments.of("MM", "1"),
                Arguments.of("MM", "12"),
                Arguments.of("HH24", "1"),
                Arguments.of("HH24", "10"),
                Arguments.of("FF2", "1"),
                Arguments.of("FF2", "12"),
                Arguments.of("FF6", "123"),
                Arguments.of("FF6", "123456")
        );
    }

    @ParameterizedTest
    @MethodSource("shuffledPatterns")
    public void testFixedLengthPatterNoDelimitersShuffled(
            List<DateTimeFormatElement> elements,
            String text,
            LocalDate expectedDate,
            LocalTime expectedTime,
            ZoneOffset expectedOffset
    ) {

        log.info("Elements: {}", elements);
        log.info("Text: {}", text);
        log.info("Values: {}", Arrays.asList(expectedDate, expectedTime, expectedOffset));

        Parser parser = new Parser(elements);
        ParsedFields parsedFields = parser.parse(text);

        LocalDate date = parsedFields.getDate(Clock.systemDefaultZone());
        LocalTime time = parsedFields.getTime();
        ZoneOffset offset = parsedFields.toZoneOffset();

        assertEquals(expectedDate, date);
        assertEquals(expectedTime, time);
        assertEquals(expectedOffset, offset);
    }

    private static Stream<Arguments> shuffledPatterns() {
        return getShuffledPatterns(false);
    }

    @ParameterizedTest
    @MethodSource("shuffledPatternsWithDelimiters")
    public void fixedLengthPatterWithDelimitersShuffled(
            List<DateTimeFormatElement> elements,
            String text,
            LocalDate expectedDate,
            LocalTime expectedTime,
            ZoneOffset expectedOffset
    ) {

        log.info("Elements: {}", elements);
        log.info("Text: {}", text);
        log.info("Values: {}", Arrays.asList(expectedDate, expectedTime, expectedOffset));

        Parser parser = new Parser(elements);
        ParsedFields parsedFields = parser.parse(text);

        LocalDate date = parsedFields.getDate(Clock.systemDefaultZone());
        LocalTime time = parsedFields.getTime();
        ZoneOffset offset = parsedFields.toZoneOffset();

        assertEquals(expectedDate, date);
        assertEquals(expectedTime, time);
        assertEquals(expectedOffset, offset);
    }

    private static Stream<Arguments> shuffledPatternsWithDelimiters() {
        return getShuffledPatterns(true);
    }

    private static Stream<Arguments> getShuffledPatterns(boolean addDelimiters) {
        List<DateTimeTemplateField> fields = List.of(
                YYYY, MM, DD, HH, MI, SS, FF4, AM, TZH, TZM
        );
        List<String> textValues = List.of(
                "2025", "05", "30", "04", "23", "59", "1234", "P.M.", "+03", "30"
        );

        LocalDate date = LocalDate.of(2025, 5, 30);
        LocalTime time = LocalTime.of(16, 23, 59, 123_400_000);
        ZoneOffset offset = ZoneOffset.ofHoursMinutes(3, 30);

        Random random = new Random();
        long seed = System.nanoTime();
        random.setSeed(seed);

        System.err.println("Seed " + seed);

        return IntStream.range(0, 100).mapToObj(v -> {

            List<Integer> ints = IntStream.range(0, fields.size())
                    .boxed().collect(Collectors.toList());
            Collections.shuffle(ints, random);

            StringBuilder text = new StringBuilder();
            List<DateTimeFormatElement> elements = new ArrayList<>();

            for (int i : ints) {
                DateTimeTemplateField f = fields.get(i);
                if (addDelimiters) {
                    char c = '/';
                    elements.add(new DateTimeFormatElement(c));
                    text.append(c);
                }
                elements.add(new DateTimeFormatElement(f));
                text.append(textValues.get(i));
            }

            return Arguments.of(elements, text.toString(), date, time, offset);
        });
    }
}
