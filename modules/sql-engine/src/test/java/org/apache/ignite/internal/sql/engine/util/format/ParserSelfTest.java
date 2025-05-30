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
import static org.apache.ignite.internal.sql.engine.util.format.DateTimeTemplateField.DateTimeField.DAY_OF_MONTH;
import static org.apache.ignite.internal.sql.engine.util.format.DateTimeTemplateField.DateTimeField.DAY_OF_YEAR;
import static org.apache.ignite.internal.sql.engine.util.format.DateTimeTemplateField.DateTimeField.FRACTION;
import static org.apache.ignite.internal.sql.engine.util.format.DateTimeTemplateField.DateTimeField.HOUR_12;
import static org.apache.ignite.internal.sql.engine.util.format.DateTimeTemplateField.DateTimeField.HOUR_24;
import static org.apache.ignite.internal.sql.engine.util.format.DateTimeTemplateField.DateTimeField.MINUTE;
import static org.apache.ignite.internal.sql.engine.util.format.DateTimeTemplateField.DateTimeField.MONTH;
import static org.apache.ignite.internal.sql.engine.util.format.DateTimeTemplateField.DateTimeField.SECOND_OF_MINUTE;
import static org.apache.ignite.internal.sql.engine.util.format.DateTimeTemplateField.DateTimeField.TIMEZONE;
import static org.apache.ignite.internal.sql.engine.util.format.DateTimeTemplateField.DateTimeField.YEAR;
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

import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.ignite.internal.sql.engine.util.format.DateTimeTemplateField.DateTimeField;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Basic tests for {@link Parser}.
 */
class ParserSelfTest extends BaseIgniteAbstractTest {

    @ParameterizedTest
    @MethodSource("basicPatterns")
    public void testBasicPatterns(String pattern, String value, Map<DateTimeField, Object> fields) {
        Scanner scanner = new Scanner(pattern);
        Parser parser = new Parser(scanner.parse());
        ParsedFields parsedFields = parser.parse(value);
        assertEquals(fields, parsedFields.fields());
    }

    private static Stream<Arguments> basicPatterns() {
        return Stream.of(
                Arguments.of("YYYY MM DD", "2024 10 01", Map.of(YEAR, 2024, MONTH, 10, DAY_OF_MONTH, 1)),
                Arguments.of("YYYY-MM-DD", "2024-10-01", Map.of(YEAR, 2024, MONTH, 10, DAY_OF_MONTH, 1)),
                Arguments.of("YYYY DDD", "2024 1", Map.of(YEAR, 2024, DAY_OF_YEAR, 1)),
                Arguments.of("YYYY DDD", "2024 50", Map.of(YEAR, 2024, DAY_OF_YEAR, 50)),
                Arguments.of("YYYY:DDD", "2024:100", Map.of(YEAR, 2024, DAY_OF_YEAR, 100)),
                Arguments.of("YYYY/DDD", "2024/365", Map.of(YEAR, 2024, DAY_OF_YEAR, 365)),
                Arguments.of("YYYYDDD", "20243", Map.of(YEAR, 2024, DAY_OF_YEAR, 3)),
                Arguments.of("YYYYDDD", "202436", Map.of(YEAR, 2024, DAY_OF_YEAR, 36)),

                // TIME
                Arguments.of("HH24:MI:SS.FF3", "3:7:9.12",
                        Map.of(HOUR_24, 3, MINUTE, 7, SECOND_OF_MINUTE, 9, FRACTION, 120_000_000)),

                Arguments.of("HH24:MI:SS.FF3 TZH:TZM", "3:7:9.12 +3:30",
                        Map.of(HOUR_24, 3, MINUTE, 7, SECOND_OF_MINUTE, 9, FRACTION, 120_000_000,
                                TIMEZONE, ZoneOffset.ofHoursMinutes(3, 30))),

                Arguments.of("HH24:MI:SS.FF5 TZH:TZM", "3:7:9.9995 -10:50",
                        Map.of(HOUR_24, 3, MINUTE, 7, SECOND_OF_MINUTE, 9, FRACTION, 999_500_000,
                                TIMEZONE, ZoneOffset.ofHoursMinutes(-10, -50))),

                Arguments.of("HH12:MI:SS.FF3 P.M.", "3:7:9.12 P.M.",
                        Map.of(HOUR_12, 3, MINUTE, 7, SECOND_OF_MINUTE, 9, FRACTION, 120_000_000)),


                // YEAR + TIME
                Arguments.of("YYYY-MM-DD HH24:MI:SS.FF3", "2024-10-01 3:7:9.12",
                        Map.of(YEAR, 2024, MONTH, 10, DAY_OF_MONTH, 1,
                                HOUR_24, 3, MINUTE, 7, SECOND_OF_MINUTE, 9, FRACTION, 120_000_000)),

                Arguments.of("YYYY-MM-DD HH24:MI:SS.FF3 TZH:TZM", "2024-10-01 3:7:9.12 +3:30",
                        Map.of(YEAR, 2024, MONTH, 10, DAY_OF_MONTH, 1,
                                HOUR_24, 3, MINUTE, 7, SECOND_OF_MINUTE, 9, FRACTION, 120_000_000,
                                TIMEZONE, ZoneOffset.ofHoursMinutes(3, 30))),

                Arguments.of("YYYY-MM-DD/HH12:MI:SS.FF3 P.M.", "2024-10-01/3:7:9.12 P.M.",
                        Map.of(YEAR, 2024, MONTH, 10, DAY_OF_MONTH, 1,
                                HOUR_12, 3, MINUTE, 7, SECOND_OF_MINUTE, 9, FRACTION, 120_000_000))

        );
    }

    @ParameterizedTest
    @MethodSource("basicInvalidPatterns")
    public void testBasicInvalidPatterns(String pattern, String text, String error) {
        Scanner scanner = new Scanner(pattern);
        Parser parser = new Parser(scanner.parse());
        DateTimeFormatException err = assertThrows(DateTimeFormatException.class, () -> parser.parse(text));
        assertThat(err.getMessage(), containsString(error));
    }

    private static Stream<Arguments> basicInvalidPatterns() {
        return Stream.of(
                // Leading space
                Arguments.of(" YYY", "100", "Expected literal < > but got <1>"),
                // Trailing space
                Arguments.of("YYY ", "100", "No values for fields: [< >]"),

                Arguments.of("YYYY", "100g", "Unexpected trailing characters after YYYY"),
                Arguments.of("YYYY/MM", "200020", "Invalid format. Expected literal </> but got <2>"),
                Arguments.of("YYYYMM", "2000XX", "Expected field MM but got <X>"),
                Arguments.of("YYYY/MM", "g2000/20", "Expected field YYYY but got <g>"),
                Arguments.of("YYYY/MM", "2000[20", "Invalid format. Expected literal </> but got <[>"),

                Arguments.of("YYYYMM", "200013", "Field value is out of range month"),
                Arguments.of("HH24:MI", "25:50", "Field value is out of range 24-hour"),
                Arguments.of("HH24:MI", "22:60", "Field value is out of range minute"),

                Arguments.of("HH24:MI TZH:TZM", "22:40 +:0", "Expected field TZH but got <:>")
        );
    }

    @ParameterizedTest
    @MethodSource("simpleValuesValid")
    public void testUnexpectedLeadingDelimiters(String pattern, String value) {
        Scanner scanner = new Scanner(pattern);
        Parser parser = new Parser(scanner.parse());
        DateTimeFormatException e = assertThrows(DateTimeFormatException.class, () -> parser.parse("/" + value));
        assertThat(e.getMessage(), containsString("Expected field " + pattern + " but got </>"));
    }

    @ParameterizedTest
    @MethodSource("simpleValuesValid")
    public void testUnexpectedTrailingDelimiters(String pattern, String value) {
        Scanner scanner = new Scanner(pattern);
        Parser parser = new Parser(scanner.parse());
        DateTimeFormatException e = assertThrows(DateTimeFormatException.class, () -> parser.parse(value + "/"));
        assertThat(e.getMessage(), containsString("Unexpected trailing characters after"));
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
            Map<Object, Object> fields
    ) {

        log.info("Elements: {}", elements);
        log.info("Text: {}", text);
        log.info("Values: {}", fields);

        Parser parser = new Parser(elements);
        ParsedFields parsedText = parser.parse(text);

        assertEquals(fields, parsedText.fields());
    }

    private static Stream<Arguments> shuffledPatterns() {
        return getShuffledPatterns(false);
    }

    @ParameterizedTest
    @MethodSource("shuffledPatternsWithDelimiters")
    public void fixedLengthPatterWithDelimitersShuffled(
            List<DateTimeFormatElement> elements,
            String text,
            Map<Object, Object> fields
    ) {

        log.info("Elements: {}", elements);
        log.info("Text: {}", text);
        log.info("Values: {}", fields);

        Parser parser = new Parser(elements);
        ParsedFields parsedText = parser.parse(text);

        assertEquals(fields, parsedText.fields());
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

        Map<Object, Object> values = Map.of(
                YEAR, 2025,
                MONTH, 5,
                DAY_OF_MONTH, 30,
                HOUR_12, 4,
                MINUTE, 23,
                SECOND_OF_MINUTE, 59,
                FRACTION, 123_400_000,
                TIMEZONE, ZoneOffset.ofHoursMinutes(3, 30)
        );

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

            return Arguments.of(elements, text.toString(), values);
        });
    }
}
