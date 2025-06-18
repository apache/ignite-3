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
import static org.apache.ignite.internal.sql.engine.util.format.DateTimeTemplateField.DDD;
import static org.apache.ignite.internal.sql.engine.util.format.DateTimeTemplateField.FF3;
import static org.apache.ignite.internal.sql.engine.util.format.DateTimeTemplateField.HH;
import static org.apache.ignite.internal.sql.engine.util.format.DateTimeTemplateField.HH12;
import static org.apache.ignite.internal.sql.engine.util.format.DateTimeTemplateField.HH24;
import static org.apache.ignite.internal.sql.engine.util.format.DateTimeTemplateField.MI;
import static org.apache.ignite.internal.sql.engine.util.format.DateTimeTemplateField.MM;
import static org.apache.ignite.internal.sql.engine.util.format.DateTimeTemplateField.PM;
import static org.apache.ignite.internal.sql.engine.util.format.DateTimeTemplateField.RR;
import static org.apache.ignite.internal.sql.engine.util.format.DateTimeTemplateField.RRRR;
import static org.apache.ignite.internal.sql.engine.util.format.DateTimeTemplateField.SS;
import static org.apache.ignite.internal.sql.engine.util.format.DateTimeTemplateField.SSSSS;
import static org.apache.ignite.internal.sql.engine.util.format.DateTimeTemplateField.TZH;
import static org.apache.ignite.internal.sql.engine.util.format.DateTimeTemplateField.TZM;
import static org.apache.ignite.internal.sql.engine.util.format.DateTimeTemplateField.Y;
import static org.apache.ignite.internal.sql.engine.util.format.DateTimeTemplateField.YY;
import static org.apache.ignite.internal.sql.engine.util.format.DateTimeTemplateField.YYY;
import static org.apache.ignite.internal.sql.engine.util.format.DateTimeTemplateField.YYYY;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.time.DateTimeException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.ignite.internal.sql.engine.util.format.DateTimeTemplateField.FieldKind;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Tests for {@link Scanner}.
 */
class ScannerSelfTest extends BaseIgniteAbstractTest {
    private static final List<Character> DELIMITERS = List.of('-', '.', '/', ',', '\'', ';', ':', ' ');

    private final Random random = new Random();

    @BeforeEach
    public void before() {
        long seed = System.nanoTime();
        random.setSeed(seed);
        log.info("Seed: {}", seed);
    }

    @ParameterizedTest
    @MethodSource("singleElements")
    public void testSingleElement(String pattern, List<DateTimeTemplateField> expected) {
        List<DateTimeFormatElement> elements = expected.stream()
                .map(DateTimeFormatElement::new)
                .collect(Collectors.toList());
        expectParsed(pattern, elements);
    }

    @ParameterizedTest
    @MethodSource("singleElements")
    public void testPreserveWhitespace(String pattern, List<DateTimeTemplateField> expected) {
        {
            // Leading
            List<DateTimeFormatElement> elements = new ArrayList<>();
            elements.add(new DateTimeFormatElement(' '));
            expected.forEach(f -> elements.add(new DateTimeFormatElement(f)));

            expectParsed(" " + pattern, elements);
        }

        {
            // Trailing
            List<DateTimeFormatElement> elements = new ArrayList<>();
            expected.forEach(f -> elements.add(new DateTimeFormatElement(f)));
            elements.add(new DateTimeFormatElement(' '));

            expectParsed(pattern + " ", elements);
        }
    }

    private static Stream<Arguments> singleElements() {
        return Stream.of(
                Arguments.of("Y", List.of(Y)),
                Arguments.of("YY", List.of(YY)),
                Arguments.of("YYY", List.of(YYY)),
                Arguments.of("YYYY", List.of(YYYY)),
                Arguments.of("RR", List.of(RR)),
                Arguments.of("RRRR", List.of(RRRR)),
                Arguments.of("MM", List.of(MM)),
                Arguments.of("DDD", List.of(DDD)),
                Arguments.of("HHA.M.", List.of(HH, AM)),
                Arguments.of("HHP.M.", List.of(HH, PM)),
                Arguments.of("HH12A.M.", List.of(HH12, AM)),
                Arguments.of("HH12P.M.", List.of(HH12, PM)),
                Arguments.of("HH24", List.of(HH24)),
                Arguments.of("MI", List.of(MI)),
                Arguments.of("SS", List.of(SS)),
                Arguments.of("SSSSS", List.of(SSSSS))
        );
    }

    @ParameterizedTest
    @MethodSource("singleElements")
    public void testPatternCaseInsensitivity(String pattern, List<DateTimeTemplateField> expected) {

        StringBuilder modifiedPattern = new StringBuilder();
        for (int i = 0; i < pattern.length(); i++) {
            if (random.nextBoolean()) {
                modifiedPattern.append(Character.toLowerCase(pattern.charAt(i)));
            } else {
                modifiedPattern.append(Character.toUpperCase(pattern.charAt(i)));
            }
        }
        String newPattern = modifiedPattern.toString();

        log.info("Random case pattern: {}", newPattern);

        List<DateTimeFormatElement> elements = expected.stream()
                .map(DateTimeFormatElement::new)
                .collect(Collectors.toList());
        expectParsed(newPattern, elements);
    }

    @ParameterizedTest
    @MethodSource("singleElements")
    public void testSingleElementWithDelimiters(String ignore, List<DateTimeTemplateField> fields) {
        StringBuilder pattern = new StringBuilder();
        List<DateTimeFormatElement> expected = new ArrayList<>();

        List<DateTimeTemplateField> shuffledFields = new ArrayList<>(fields);
        Collections.shuffle(shuffledFields, random);

        for (DateTimeTemplateField f : shuffledFields) {
            // Conditionally add a delimiter between fields
            if (random.nextBoolean()) {
                char delimiter = DELIMITERS.get(random.nextInt(DELIMITERS.size()));
                pattern.append(delimiter);
                expected.add(new DateTimeFormatElement(delimiter));
            }
            pattern.append(f.pattern());
            expected.add(new DateTimeFormatElement(f));
        }

        // Conditionally add a delimiter after the last field
        if (random.nextBoolean()) {
            char delimiter = DELIMITERS.get(random.nextInt(DELIMITERS.size()));
            pattern.append(delimiter);
            expected.add(new DateTimeFormatElement(delimiter));
        }

        log.info("Fields: {}", fields);
        log.info("Pattern: {}", pattern);
        log.info("Expected: {}", expected);

        expectParsed(pattern.toString(), expected);
    }

    @ParameterizedTest
    @MethodSource("invalidPatterns")
    public void testRejectInvalidPattern(String pattern, String error) {
        Scanner scanner = new Scanner(pattern);
        DateTimeException err = assertThrows(DateTimeException.class, scanner::scan);
        assertThat(err.getMessage(), containsString(error));
    }

    private static Stream<Arguments> invalidPatterns() {
        return Stream.of(
                Arguments.of("gY", "Unexpected element <GY> in pattern"),
                Arguments.of("Yx", "Unexpected element <X> in pattern"),
                Arguments.of("Yxyz", "Unexpected element <XYZ> in pattern"),
                Arguments.of("Y gogogo", "Unexpected element <GOGOGO> in pattern"),
                Arguments.of("Y gogo!abc", "Unexpected element <GOGO> in pattern"),
                Arguments.of("Y=", "Unexpected character <=> in pattern"),
                Arguments.of("MM[Y", "Unexpected character <[> in pattern"),
                Arguments.of("MM[Y", "Unexpected character <[> in pattern"),
                Arguments.of("YYYY<DD>", "Unexpected character <<> in pattern")
        );
    }

    @ParameterizedTest
    @EnumSource(DateTimeTemplateField.class)
    public void testElementAppearsAtMostOnce(DateTimeTemplateField field) {
        // Ignore Y as Y + Y = YY which is a valid pattern, same applies to YY and RR.
        // Ignore DD, since DDDD is parsed as <DDD>D and we get D unexpected is patter here.
        boolean checkConsecutive = field != Y && field != YY && field != RR && field != DD;

        if (checkConsecutive) {
            String pattern = field.pattern() + field.pattern();

            DateTimeException err = assertThrows(DateTimeException.class,
                    () -> new Scanner(pattern).scan());
            assertThat(err.getMessage(), containsString("Element is already present: " + field.kind()));
        }

        {
            String pattern = field.pattern() + " " + field.pattern();

            DateTimeException err = assertThrows(DateTimeException.class,
                    () -> new Scanner(pattern).scan());
            assertThat(err.getMessage(), containsString("Element is already present: " + field.kind()));
        }
    }

    @ParameterizedTest
    @MethodSource("validFields")
    public void testValidFieldsShuffledNoDelimiters(List<DateTimeTemplateField> fields) {
        StringBuilder pattern = new StringBuilder();
        List<DateTimeFormatElement> expected = new ArrayList<>();

        List<DateTimeTemplateField> shuffledFields = new ArrayList<>(fields);
        Collections.shuffle(shuffledFields, random);

        for (DateTimeTemplateField f : shuffledFields) {
            pattern.append(f.pattern());
            expected.add(new DateTimeFormatElement(f));
        }

        log.info("Fields: {}", shuffledFields);
        log.info("Pattern: {}", pattern);

        expectParsed(pattern.toString(), expected);
    }

    @ParameterizedTest
    @MethodSource("validFields")
    public void testValidFieldsShuffled(List<DateTimeTemplateField> fields) {
        StringBuilder pattern = new StringBuilder();
        List<DateTimeFormatElement> expected = new ArrayList<>();

        List<DateTimeTemplateField> shuffledFields = new ArrayList<>(fields);
        Collections.shuffle(shuffledFields, random);

        for (DateTimeTemplateField f : shuffledFields) {
            if (random.nextBoolean()) {
                char delimiter = DELIMITERS.get(random.nextInt(DELIMITERS.size()));
                if (pattern.length() > 0) {
                    pattern.append(delimiter);
                    expected.add(new DateTimeFormatElement(delimiter));
                }
            }
            pattern.append(f.pattern());
            expected.add(new DateTimeFormatElement(f));
        }

        log.info("Fields: {}", shuffledFields);
        log.info("Pattern: {}", pattern);

        expectParsed(pattern.toString(), expected);
    }

    private static Stream<List<DateTimeTemplateField>> validFields() {
        return Stream.of(
                List.of(YY, DD, MM),
                List.of(YYY, DD, MM),
                List.of(YYYY, DD, MM),

                List.of(RR, DD, MM),
                List.of(RRRR, DD, MM),

                List.of(HH, MI, SS, AM),
                List.of(HH, MI, SS, FF3, AM),

                List.of(HH, MI, SS, PM),
                List.of(HH, MI, SS, FF3, PM),

                List.of(HH12, MI, SS, AM),
                List.of(HH12, MI, SS, FF3, AM),

                List.of(HH12, MI, SS, AM),
                List.of(HH12, MI, SS, FF3, AM),
                List.of(HH12, MI, SS, FF3, AM, TZH, TZM),

                List.of(HH24, MI, SS),
                List.of(HH24, MI, SS, FF3),
                List.of(HH24, MI, SS, FF3, TZH, TZM),

                List.of(YY, DDD, HH, MI, SS, FF3, AM),
                List.of(YY, DDD, HH, MI, SS, FF3, AM, TZH, TZM),
                List.of(YY, DDD, HH, MI, SS, FF3, PM, TZH, TZM),
                List.of(YY, DDD, HH24, MI, SS, FF3, TZH, TZM),
                List.of(YY, DDD, HH24, MI, SS, FF3, TZH, TZM),

                List.of(YYY, DDD, HH, MI, SS, FF3, AM),
                List.of(YYY, DDD, HH, MI, SS, FF3, AM, TZH, TZM),
                List.of(YYY, DDD, HH, MI, SS, FF3, PM, TZH, TZM),
                List.of(YYY, DDD, HH24, MI, SS, FF3, TZH, TZM),
                List.of(YYY, DDD, HH24, MI, SS, FF3, TZH, TZM),

                List.of(YYYY, DD, MM, HH, MI, SS, FF3, AM),
                List.of(YYYY, DD, MM, HH, MI, SS, FF3, AM, TZH, TZM),
                List.of(YYYY, DD, MM, HH, MI, SS, FF3, PM, TZH, TZM),
                List.of(YYYY, DD, MM, HH24, MI, SS, FF3),
                List.of(YYYY, DD, MM, HH24, MI, SS, FF3, TZH, TZM),

                List.of(DDD),
                List.of(Y, DDD),
                List.of(YY, DDD),
                List.of(YYY, DDD),
                List.of(YYYY, DDD),

                List.of(DDD),
                List.of(RR, DDD),
                List.of(RRRR, DDD),

                List.of(SSSSS)
        );
    }

    private static void expectParsed(String pattern, List<DateTimeFormatElement> expected) {
        Scanner scanner = new Scanner(pattern);
        List<DateTimeFormatElement> actual = scanner.scan();
        assertEquals(expected, actual);
    }

    @ParameterizedTest
    @MethodSource("singleElements")
    public void testWrongDelimiters(String pattern) {
        boolean before = random.nextBoolean();

        char[] wrongDelimiters = {'[', ']', '!', '?', '<', '>', '\\'};

        char c = wrongDelimiters[random.nextInt(wrongDelimiters.length)];

        String updated;
        if (before) {
            updated = c + pattern;
        } else {
            updated = pattern + c;
        }

        log.info("Updated pattern: {}", updated);

        DateTimeException err = assertThrows(DateTimeException.class,
                () -> new Scanner(updated).scan());
        assertThat(err.getMessage(), containsString("Unexpected character "));
    }

    @ParameterizedTest
    @MethodSource("consecutiveDelimitersPatterns")
    public void testConsecutiveDelimiters(String pattern) {
        String updated = pattern;

        for (int i = 0; i < 9; i++) {
            char d = DELIMITERS.get(random.nextInt(DELIMITERS.size()));
            updated = updated.replace(Integer.toString(i), Character.toString(d));
        }

        log.info("Updated pattern: {}", updated);

        checkSyntaxRule(updated, "Consecutive delimiters are not allowed");
    }

    private static Stream<Arguments> consecutiveDelimitersPatterns() {
        String f1 = "MM";
        String f2 = "DD";

        List<String> patterns = new ArrayList<>();

        patterns.add(f1 + "12" + f2);
        patterns.add("12" + f1 + f2);
        patterns.add(f1 + f2 + "12");
        patterns.add("1" + f1 + f2 + "33");
        patterns.add("1" + f1 + "23" + f2 + "4");

        return patterns.stream().map(Arguments::of);
    }

    @ParameterizedTest
    @MethodSource("yearPatterns")
    public void testYearRule(String pattern, String error) {
        checkSyntaxRule(pattern, error);
    }

    private static Stream<Arguments> yearPatterns() {
        String error = "Only one field must be present: year / rounded year";
        return Stream.of(
                Arguments.of("Y RR", error),
                Arguments.of("YY RR", error),
                Arguments.of("YYY RR", error),
                Arguments.of("YYYY RR", error),

                Arguments.of("Y RRRR", error),
                Arguments.of("YY RRRR", error),
                Arguments.of("YYY RRRR", error),
                Arguments.of("YYYY RRRR", error)
        );
    }

    @ParameterizedTest
    @MethodSource("dayPatterns")
    public void testDayRule(String pattern, String error) {
        checkSyntaxRule(pattern, error);
    }

    private static Stream<Arguments> dayPatterns() {
        return Stream.of(
                Arguments.of("MM DDD", "day of year / month"),
                Arguments.of("DD DDD", "day of year / day of month")
        );
    }

    @ParameterizedTest
    @MethodSource("hourPatterns")
    public void testHourRule(String pattern, String error) {
        checkSyntaxRule(pattern, error);
    }

    private static Stream<Arguments> hourPatterns() {
        return Stream.of(
                Arguments.of("HH24 HH12", "24-hour / 12-hour"),
                Arguments.of("HH24 HH", "24-hour / 12-hour")
        );
    }

    @ParameterizedTest
    @MethodSource("amPmPatterns")
    public void testAmPmRule(String pattern, String error) {
        checkSyntaxRule(pattern, error);
    }

    private static Stream<Arguments> amPmPatterns() {
        return Stream.of(
                Arguments.of("HH12", "12-hour / am pm"),
                // Handled by the previous rule
                Arguments.of("HH24 A.M.", "/ am pm"),
                Arguments.of("HH24 P.M.", "/ am pm")
        );
    }

    @ParameterizedTest
    @MethodSource("secondPatterns")
    public void testSecondRule(String pattern, String error) {
        checkSyntaxRule(pattern, error);
    }

    private static Stream<Arguments> secondPatterns() {
        return Stream.of(
                Arguments.of("SSSSS HH A.M.", "second of day / 12-hour"),
                Arguments.of("SSSSS HH24", "second of day / 24-hour"),
                Arguments.of("SSSSS MI", "second of day / minute"),
                Arguments.of("SSSSS SS", "second of day / second of minute")
        );
    }

    @ParameterizedTest
    @MethodSource("timeZonePatterns")
    public void testTimeZoneRule(String pattern, String error) {
        checkSyntaxRule(pattern, error);
    }

    private static Stream<Arguments> timeZonePatterns() {
        return Stream.of(
                Arguments.of("TZH", "Expected both fields: time zone hour / time zone minute"),
                Arguments.of("TZM", "Expected both fields: time zone hour / time zone minute")
        );
    }

    @Test
    public void testRestrictFieldSet() {
        {
            Scanner scanner = new Scanner("YY", "XYZ", Set.of(FieldKind.MONTH));
            DateTimeException err = assertThrows(DateTimeException.class, scanner::scan);
            assertThat(err.getMessage(), containsString("Illegal field <YY> for format XYZ"));
        }
        {
            Scanner scanner = new Scanner("DD", "XYZ", Set.of(FieldKind.MONTH, FieldKind.YEAR));
            DateTimeException err = assertThrows(DateTimeException.class, scanner::scan);
            assertThat(err.getMessage(), containsString("Illegal field <DD> for format XYZ"));
        }
    }

    private void checkSyntaxRule(String pattern, String error) {
        String[] split = pattern.split(" ");

        List<String> shuffled = Arrays.asList(split);
        Collections.shuffle(shuffled, random);

        log.info("Pattern: {}", shuffled);

        Scanner scanner = new Scanner(pattern);
        DateTimeException err = assertThrows(DateTimeException.class, scanner::scan);
        assertThat(err.getMessage(), containsString(error));
    }
}
