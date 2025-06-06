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

import java.time.Clock;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Tests for {@link SqlDateTimeParser}.
 */
public class SqlDateTimeParserSelfTest extends BaseIgniteAbstractTest {

    private static final Clock CLOCK = Clock.fixed(Instant.parse("2017-05-13T00:00:00.0Z"), ZoneOffset.UTC);

    @ParameterizedTest
    @MethodSource("timeValues")
    public void testTime(String format, String str, LocalTime value) {
        LocalTime actual = SqlDateTimeParser.timeFormatter(format).parseTime(str);
        assertEquals(actual, value);
    }

    private static Stream<Arguments> timeValues() {
        return Stream.of(
                Arguments.of("HH24:MI:SS", "15:37:43", LocalTime.of(15, 37, 43)),
                Arguments.of("HH24:MI:SS.FF3", "15:37:43.871", LocalTime.of(15, 37, 43, 871_000_000)),
                Arguments.of("HH24:MI", "15:37", LocalTime.of(15, 37, 0)),
                Arguments.of("HH24.FF3", "15.871", LocalTime.of(15, 0, 0, 871_000_000)),
                Arguments.of("HH24", "15", LocalTime.of(15, 0, 0)),
                Arguments.of("MI", "37", LocalTime.of(0, 37, 0)),
                Arguments.of("MI:SS", "37:43", LocalTime.of(0, 37, 43)),
                Arguments.of("SS", "43", LocalTime.of(0, 0, 43)),
                Arguments.of("FF3", "871", LocalTime.of(0, 0, 0, 871_000_000)),
                Arguments.of("HH:MI:SS P.M.", "03:37:43 P.M.", LocalTime.of(15, 37, 43)),
                Arguments.of("HH P.M.", "3 P.M.", LocalTime.of(15, 0, 0))
        );
    }

    @ParameterizedTest
    @MethodSource("dateValues")
    public void testDate(String format, String str, LocalDate value) {
        LocalDate actual = SqlDateTimeParser.dateFormatter(format).parseDate(str, CLOCK);
        assertEquals(value, actual);
    }

    private static Stream<Arguments> dateValues() {
        LocalDate date = LocalDate.of(2017, 5, 13);

        return Stream.of(
                Arguments.of("YYYY-MM-DD", "2017-05-13", date),
                Arguments.of("YYYY-MM", "2017-05", date),
                Arguments.of("YYYY-DD", "2017-13", date),
                Arguments.of("YYYY-DDD", "2017-133", date),
                Arguments.of("YYYY", "2017", date),
                Arguments.of("MM-DD", "05-13", date),
                Arguments.of("MM", "05", date),
                Arguments.of("DD", "13", date),
                Arguments.of("DDD", "133", date)
        );
    }

    @ParameterizedTest
    @MethodSource("timestampValues")
    public void testTimestamp(String format, String str, LocalDateTime value) {
        LocalDateTime actual = SqlDateTimeParser.timestampFormatter(format).parseTimestamp(str, CLOCK);
        assertEquals(value, actual);
    }

    private static Stream<Arguments> timestampValues() {
        List<Arguments> dates = dateValues().collect(Collectors.toList());
        List<Arguments> times = timeValues().collect(Collectors.toList());
        List<Arguments> out = new ArrayList<>();

        for (Arguments date : dates) {
            Object[] dateArgs = date.get();
            String dateFormat = (String) dateArgs[0];
            String strDateVal = (String) dateArgs[1];
            LocalDate dateVal = (LocalDate) dateArgs[2];

            for (Arguments time : times) {
                Object[] timeArgs = time.get();
                String timeFormat = (String) timeArgs[0];
                String timeStr = (String) timeArgs[1];
                LocalTime timeVal = (LocalTime) timeArgs[2];

                String tsFormat = dateFormat + " " + timeFormat;
                String tsStr = strDateVal + " " + timeStr;
                LocalDateTime tsVal = LocalDateTime.of(dateVal, timeVal);

                out.add(Arguments.of(tsFormat, tsStr, tsVal));
            }
        }

        return out.stream();
    }
}
