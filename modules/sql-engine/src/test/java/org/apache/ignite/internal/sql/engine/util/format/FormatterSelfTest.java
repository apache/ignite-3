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
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Basic tests for {@link Formatter}.
 */
public class FormatterSelfTest extends BaseIgniteAbstractTest {

    @ParameterizedTest
    @MethodSource("basicPatterns")
    public void testBasicPatterns(String pattern, String value, LocalDateTime dateTime, ZoneOffset offset) {
        Scanner scanner = new Scanner(pattern);
        Formatter parser = new Formatter(scanner.scan());

        String formatted = parser.format(dateTime, offset != null ? offset : ZoneOffset.UTC);
        assertEquals(value, formatted);
    }

    private static Stream<Arguments> basicPatterns() {
        return Stream.of(
                Arguments.of("YYYY MM DD", "2024 10 01", LocalDateTime.of(LocalDate.of(2024, 10, 1), LocalTime.ofSecondOfDay(0)), null),
                Arguments.of("YYYY-MM-DD", "2024-10-01", LocalDateTime.of(LocalDate.of(2024, 10, 1), LocalTime.ofSecondOfDay(0)), null),
                Arguments.of("YYYY DDD", "2024 001", LocalDateTime.of(LocalDate.of(2024, 1, 1), LocalTime.ofSecondOfDay(0)), null),
                Arguments.of("YYYY DDD", "2024 050", LocalDateTime.of(LocalDate.of(2024, 2, 19), LocalTime.ofSecondOfDay(0)), null),
                Arguments.of("YYYY:DDD", "2024:100", LocalDateTime.of(LocalDate.of(2024, 4, 9), LocalTime.ofSecondOfDay(0)), null),
                Arguments.of("YYYY/DDD", "2024/365", LocalDateTime.of(LocalDate.of(2024, 12, 30), LocalTime.ofSecondOfDay(0)), null),
                Arguments.of("YYYYDDD", "2024003", LocalDateTime.of(LocalDate.of(2024, 1, 3), LocalTime.ofSecondOfDay(0)), null),
                Arguments.of("YYYYDDD", "2024036", LocalDateTime.of(LocalDate.of(2024, 2, 5), LocalTime.ofSecondOfDay(0)), null),

                // TIME
                Arguments.of("HH24:MI:SS.FF3", "03:07:09.120",
                        LocalDateTime.of(LocalDate.of(2025, 1, 1), LocalTime.of(3, 7, 9, 120_000_000)), null),

                Arguments.of("HH24:MI:SS.FF3 TZH:TZM", "03:07:09.120 +03:30",
                        LocalDateTime.of(LocalDate.of(2025, 1, 1), LocalTime.of(3, 7, 9, 120_000_000)),
                        ZoneOffset.ofHoursMinutes(3, 30)),

                Arguments.of("HH24:MI:SS.FF5 TZH:TZM", "03:07:09.99950 -10:50",
                        LocalDateTime.of(LocalDate.of(2025, 1, 1), LocalTime.of(3, 7, 9, 999_500_000)),
                        ZoneOffset.ofHoursMinutes(-10, -50)),

                // YEAR + TIME
                Arguments.of("YYYY-MM-DD HH24:MI:SS.FF3", "2024-10-01 03:07:09.120",
                        LocalDateTime.of(LocalDate.of(2024, 10, 1), LocalTime.of(3, 7, 9, 120_000_000)), null),

                Arguments.of("YYYY-MM-DD HH24:MI:SS.FF3 TZH:TZM", "2024-10-01 03:07:09.120 +03:30",
                        LocalDateTime.of(LocalDate.of(2024, 10, 1), LocalTime.of(3, 7, 9, 120_000_000)),
                        ZoneOffset.ofHoursMinutes(3, 30)),

                Arguments.of("YYYY-MM-DD/HH12:MI:SS.FF3 A.M.", "2024-10-01/03:07:09.120 A.M.",
                        LocalDateTime.of(LocalDate.of(2024, 10, 1), LocalTime.of(3, 7, 9, 120_000_000)), null)

        );
    }

    @ParameterizedTest
    @MethodSource("hour12Patterns")
    public void testHour12Patterns(String pattern, String value, LocalTime timeValue) {
        Scanner scanner = new Scanner(pattern);
        Formatter parser = new Formatter(scanner.scan());

        String formatted = parser.format(timeValue, ZoneOffset.UTC);
        assertEquals(value, formatted);
    }

    private static Stream<Arguments> hour12Patterns() {
        return Stream.of(
                // A.M.
                Arguments.of("HH12:MI:SS.FF3 A.M.", "12:07:09.120 A.M.", LocalTime.of(0, 7, 9, 120_000_000)),
                Arguments.of("HH12:MI:SS.FF3 A.M.", "03:07:09.120 A.M.", LocalTime.of(3, 7, 9, 120_000_000)),
                Arguments.of("HH12:MI:SS.FF3 A.M.", "11:59:09.120 A.M.", LocalTime.of(11, 59, 9, 120_000_000)),

                // P.M.
                Arguments.of("HH12:MI:SS.FF3 P.M.", "12:59:09.120 P.M.", LocalTime.of(12, 59, 9, 120_000_000)),
                Arguments.of("HH12:MI:SS.FF3 P.M.", "11:59:09.120 P.M.", LocalTime.of(23, 59, 9, 120_000_000))
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

        Formatter formatter = new Formatter(elements);

        String formatted = formatter.format(LocalDateTime.of(expectedDate, expectedTime), expectedOffset);
        assertEquals(text, formatted);
    }

    private static Stream<Arguments> shuffledPatterns() {
        return getShuffledPatterns(false);
    }

    @ParameterizedTest
    @MethodSource("shuffledPatternsWithDelimiters")
    public void testFixedLengthPatterWithDelimitersShuffled(
            List<DateTimeFormatElement> elements,
            String text,
            LocalDate expectedDate,
            LocalTime expectedTime,
            ZoneOffset expectedOffset
    ) {

        log.info("Elements: {}", elements);
        log.info("Text: {}", text);
        log.info("Values: {}", Arrays.asList(expectedDate, expectedTime, expectedOffset));

        Formatter formatter = new Formatter(elements);

        String formatted = formatter.format(LocalDateTime.of(expectedDate, expectedTime), expectedOffset);
        assertEquals(text, formatted);
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
