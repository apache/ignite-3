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

package org.apache.ignite.internal.sql.engine.datatypes;

import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.fail;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoField;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nullable;
import org.apache.ignite.Ignite;
import org.apache.ignite.internal.sql.BaseSqlIntegrationTest;
import org.apache.ignite.internal.sql.SqlCommon;
import org.apache.ignite.sql.SqlException;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/** Test cases for cast to datetime type with specified format {@code (CAST x AS DATETIME TYPE FORMAT 'FMT')}. */
public class ItDateTimeCastFormatTest extends BaseSqlIntegrationTest {

    private static final ZoneId TIME_ZONE_ID = ZoneId.of("Europe/Paris");

    @BeforeAll
    public void createDateTimeColumnsTable() {
        sql("CREATE TABLE datetime_cols (id INT PRIMARY KEY, "
                + "fmt_col VARCHAR,"
                + "date0_col DATE, "

                + "time0_col TIME(0), "
                + "time1_col TIME(1), "
                + "time2_col TIME(2), "
                + "time3_col TIME(3), "

                + "timestamp0_col TIMESTAMP(0), "
                + "timestamp1_col TIMESTAMP(1), "
                + "timestamp2_col TIMESTAMP(2), "
                + "timestamp3_col TIMESTAMP(3), "

                + "timestamp_with_local_time_zone0_col TIMESTAMP(0) WITH LOCAL TIME ZONE, "
                + "timestamp_with_local_time_zone1_col TIMESTAMP(1) WITH LOCAL TIME ZONE, "
                + "timestamp_with_local_time_zone2_col TIMESTAMP(2) WITH LOCAL TIME ZONE, "
                + "timestamp_with_local_time_zone3_col TIMESTAMP(3) WITH LOCAL TIME ZONE "
                + ")");
        sql("INSERT INTO datetime_cols (id) VALUES (1)");
    }

    @ParameterizedTest
    @MethodSource("date")
    public void dateLiterals(DateTimeArgs<LocalDate> args) {
        String sqlCast = format("SELECT CAST('{}' AS DATE FORMAT '{}')", args.str, args.format);

        checkQuery(sqlCast, args.value, args.error);
    }

    @ParameterizedTest
    @MethodSource("date")
    public void dateDynamicParams(DateTimeArgs<LocalDate> args) {
        String sqlCast = format("SELECT CAST(? AS DATE FORMAT '{}')", args.format);

        checkQuery(sqlCast, args.value, args.error, args.str);
    }

    @ParameterizedTest
    @MethodSource("date")
    public void dateUpdateFromLiteral(DateTimeArgs<LocalDate> args) {
        String sqlCast = format(
                "UPDATE datetime_cols SET date0_col=CAST(? AS DATE FORMAT '{}') WHERE id = 1",
                args.format
        );

        checkDml(sqlCast, args.error, args.str);

        if (args.value != null) {
            assertQuery("SELECT date0_col FROM datetime_cols WHERE id = 1")
                    .returns(args.value)
                    .check();
        }
    }

    @ParameterizedTest
    @MethodSource("date")
    public void dateUpdateFromDynamicParam(DateTimeArgs<LocalDate> args) {
        String sqlCast = format(
                "UPDATE datetime_cols SET date0_col=CAST('{}' AS DATE FORMAT '{}') WHERE id = 1",
                args.str, args.format
        );

        checkDml(sqlCast, args.error);

        if (args.value != null) {
            assertQuery("SELECT date0_col FROM datetime_cols WHERE id = 1")
                    .returns(args.value)
                    .check();
        }
    }

    private static Stream<DateTimeArgs<LocalDate>> date() {
        return Stream.of(
                dateTime("2000-01-01", "yyyy-MM-dd", LocalDate.of(2000, 1, 1), null),
                dateTime("2-01-01", "y-MM-dd", LocalDate.of(2022, 1, 1), null),
                dateTime("02-01-01", "y-MM-dd", null, "Invalid format. Expected literal <-> but got"),
                dateTime("20-01-01", "yy-MM-dd", LocalDate.of(2020, 1, 1), null),
                dateTime("020-01-01", "yyy-MM-dd", LocalDate.of(2020, 1, 1), null),
                dateTime("002-01-01", "yyy-MM-dd", LocalDate.of(2002, 1, 1), null),
                dateTime("200-01-01", "yyy-MM-dd", LocalDate.of(2200, 1, 1), null),
                dateTime("20-01-01", "yyyy-MM-dd", LocalDate.of(20, 1, 1), null),
                dateTime("9999-01-01", "yyyy-MM-dd", LocalDate.of(9999, 1, 1), null),

                dateTime("2000/01-01", "yyyy/MM-dd", LocalDate.of(2000, 1, 1), null),

                dateTime("10000-01-01", "yyyy-MM-dd", null, "Invalid format. Expected literal <-> but got"),
                dateTime("10000000-01-01", "yyyy-MM-dd", null, "Invalid format. Expected literal <-> but got"),

                dateTime("01-01-01", "RR-MM-dd", LocalDate.of(2001, 1, 1), null),
                dateTime("33-01-01", "RR-MM-dd", LocalDate.of(2033, 1, 1), null),
                dateTime("49-01-01", "RR-MM-dd", LocalDate.of(2049, 1, 1), null),

                dateTime("51-01-01", "RR-MM-dd", LocalDate.of(1951, 1, 1), null),
                dateTime("77-01-01", "RR-MM-dd", LocalDate.of(1977, 1, 1), null),

                dateTime("01-01-01", "RRRR-MM-dd", LocalDate.of(2001, 1, 1), null),
                dateTime("33-01-01", "RRRR-MM-dd", LocalDate.of(2033, 1, 1), null),
                dateTime("49-01-01", "RRRR-MM-dd", LocalDate.of(2049, 1, 1), null),
                dateTime("51-01-01", "RRRR-MM-dd", LocalDate.of(1951, 1, 1), null),
                dateTime("77-01-01", "RRRR-MM-dd", LocalDate.of(1977, 1, 1), null),

                dateTime("2001-01-01", "RRRR-MM-dd", LocalDate.of(2001, 1, 1), null),
                dateTime("2033-01-01", "RRRR-MM-dd", LocalDate.of(2033, 1, 1), null),
                dateTime("2049-01-01", "RRRR-MM-dd", LocalDate.of(2049, 1, 1), null),
                dateTime("2051-01-01", "RRRR-MM-dd", LocalDate.of(2051, 1, 1), null),
                dateTime("1951-01-01", "RRRR-MM-dd", LocalDate.of(1951, 1, 1), null),

                dateTime("001-01-01", "RRRR-MM-dd", LocalDate.of(2001, 1, 1), null),
                dateTime("1-01-01", "RRRR-MM-dd", LocalDate.of(2001, 1, 1), null),
                dateTime("33-01-01", "RRRR-MM-dd", LocalDate.of(2033, 1, 1), null),
                dateTime("033-01-01", "RRRR-MM-dd", LocalDate.of(2033, 1, 1), null),
                dateTime("51-01-01", "RRRR-MM-dd", LocalDate.of(1951, 1, 1), null),
                dateTime("051-01-01", "RRRR-MM-dd", LocalDate.of(1951, 1, 1), null),
                dateTime("077-01-01", "RRRR-MM-dd", LocalDate.of(1977, 1, 1), null),

                dateTime("151-01-01", "RRRR-MM-dd", LocalDate.of(151, 1, 1), null),
                dateTime("177-01-01", "RRRR-MM-dd", LocalDate.of(177, 1, 1), null),

                dateTime("2000-05-07", "yyyy-MM-dddd", null, "Unexpected element <D> in pattern"),
                dateTime("2000-05-07", "yyyy-MM-dddd", null, "Unexpected element <D> in pattern"),
                dateTime("2000-5-07", "yyyyy-M-dd", null, "Element is already present: YEAR"),
                dateTime("2000-005-07", "yyyyy-MMM-dddd", null, "Element is already present: YEAR"),

                dateTime("100-05-07", "R-MM-dd", null, "Unexpected element <R> in pattern"),
                dateTime("100-05-07", "RRR-MM-dd", null, "Unexpected element <R> in pattern"),
                dateTime("100-05-07", "RRRRR-MM-dd", null, "Unexpected element <R> in pattern "),
                dateTime("201-01-01", "RR-MM-dd", null, "Invalid format. Expected literal <-> but got"),

                // Different error combination with ff/ff0 TIMESTAMP/ TIMESTAMP LTZ: Illegal pattern character 'f'
                dateTime("2000-005-07", "yyyy-MMM-dd", null, "Unexpected element <M> in pattern"),

                dateTime("0-0-0", "yyyy-MM-dd", null, "Invalid value for Year"),
                dateTime("0000-01-01", "yyyy-MM-dd", null, "Invalid value for Year"),
                dateTime("-01-01-01", "yyyy-MM-dd", null, "Expected field YYYY but got")
        );
    }

    // TIME

    @ParameterizedTest
    @MethodSource("time")
    public void timeLiterals(DateTimeArgs<LocalTime> args) {
        String sqlCast = format("SELECT CAST('{}' AS TIME(3) FORMAT '{}')", args.str, args.format);
        checkQuery(sqlCast, args.value, args.error);
    }

    @ParameterizedTest
    @MethodSource("time")
    public void timeDynamicParams(DateTimeArgs<LocalTime> args) {
        String sqlCast = format("SELECT CAST(? AS TIME(3) FORMAT '{}')", args.format);
        checkQuery(sqlCast, args.value, args.error, args.str);
    }

    @ParameterizedTest
    @MethodSource("timeNoMillis")
    public void timeUpdateFromDynamicParam(DateTimeArgs<LocalTime> args) {
        String sqlCast = format(
                "UPDATE datetime_cols SET time0_col=CAST(? AS TIME(4) FORMAT '{}') WHERE id = 1",
                args.format
        );

        checkDml(sqlCast, args.error, args.str);

        if (args.value != null) {
            assertQuery("SELECT time0_col FROM datetime_cols WHERE id = 1")
                    .returns(args.value)
                    .check();
        }
    }

    @ParameterizedTest
    @MethodSource("timeNoMillis")
    public void timeUpdateFromLiteral(DateTimeArgs<LocalTime> args) {
        String sqlCast = format(
                "UPDATE datetime_cols SET time0_col=CAST('{}' AS TIME(3) FORMAT '{}') WHERE id = 1",
                args.str,
                args.format
        );

        checkDml(sqlCast, args.error);

        if (args.value != null) {
            assertQuery("SELECT time0_col FROM datetime_cols WHERE id = 1")
                    .returns(args.value)
                    .check();
        }
    }

    private static Stream<DateTimeArgs<LocalTime>> timeNoMillis() {
        // The expected result should be TIME(0) (without milliseconds).
        return time().map(v ->
                dateTime(v.str, v.format, v.value == null ? null : v.value.withNano(0), v.error));
    }

    private static Stream<DateTimeArgs<LocalTime>> time() {
        return Stream.of(
                dateTime("05:02 a.m.", "hh12:mi a.m.", LocalTime.of(5, 2), null),
                dateTime("11:02 a.m.", "hh12:mi a.m.", LocalTime.of(11, 2), null),
                dateTime("12:02 a.m.", "hh12:mi a.m.", LocalTime.of(0, 2), null),
                dateTime("13:02 a.m.", "hh12:mi a.m.", null, "Invalid value for HourAmPm"),

                dateTime("05:02 p.m.", "hh12:mi p.m.", LocalTime.of(17, 2), null),
                dateTime("11:02 p.m.", "hh12:mi p.m.", LocalTime.of(23, 2), null),
                dateTime("12:02 p.m.", "hh12:mi p.m.", LocalTime.of(12, 2), null),
                dateTime("13:02 p.m.", "hh12:mi p.m.", null, "Invalid value for HourAmPm"),

                // hh24
                dateTime("12:02:03", "hh24:mi:ss", LocalTime.of(12, 2, 3), null),
                dateTime("23:02:03", "hh24:mi:ss", LocalTime.of(23, 2, 3), null),
                dateTime("23/02:03", "hh24/mi:ss", LocalTime.of(23, 2, 3), null),

                // fractional
                dateTime("23:02:03.99", "hh24:mi:ss.ff2", LocalTime.of(23, 2, 3, 990_000_000), null),
                dateTime("23:02:03.999", "hh24:mi:ss.ff3", LocalTime.of(23, 2, 3, 999_000_000), null),
                dateTime("23:02:03.123", "hh24:mi:ss.ff3", LocalTime.of(23, 2, 3, 123_000_000), null),

                dateTime("23:02:03.1234", "hh24:mi:ss.ff3", null, "Unexpected trailing characters after field FF3"),
                dateTime("23:02:03.1234", "hh24:mi:ss.ff4", LocalTime.of(23, 2, 3, 123_000_000), null),

                dateTime("24:02:03", "hh24:mi:ss", null, "Invalid value for HourOfDay"),
                dateTime("123:02:03", "hh24:mi:ss", null, "Invalid format. Expected literal <:> but got <3>"),
                dateTime("23:60:03", "hh24:mi:ss", null, "Invalid value for MinuteOfHour"),
                dateTime("23:123:03", "hh24:mi:ss", null, "Invalid format. Expected literal <:> but got <3>"),
                dateTime("23:02:60", "hh24:mi:ss", null, "Invalid value for SecondOfMinute"),
                dateTime("23:02:123", "hh24:mi:ss", null, "Unexpected trailing characters after field SS"),

                dateTime("22:02:03", "hX:mi:ss", null, "Unexpected element <HX> in pattern"),
                dateTime("22:02:03", "hh:mX:ss", null, "Unexpected element <MX> in pattern"),
                dateTime("22:02:03", "hh:mi:sX", null, "Unexpected element <SX> in pattern"),
                dateTime("22:02:03", "hh:mm:ss", null, "Illegal field <MM> for format TIME"),

                dateTime("22:02:03", "hh:mi:ss.ff", null, "Unexpected element <FF> in pattern"),
                dateTime("22:02:03", "hh:mi:ss.ff0", null, "Unexpected element <FF0> in pattern"),
                dateTime("22:02:03", "hh:mi:ss.ff10", null, "Unexpected character <0> in pattern"),

                dateTime("23:02:03.123", "hh24:mi:ss", null, "Unexpected trailing characters after field SS"),
                dateTime("23:02:03.12", "hh24:mi:ss.ff3", LocalTime.of(23, 2, 3, 120_000_000), null),
                dateTime("23:02:03.1234", "hh24:mi:ss.ff3", null, "Unexpected trailing characters after field FF3")
        );
    }

    @ParameterizedTest
    @MethodSource("timeWithPrecision")
    public void timeWithPrecisionLiterals(int precision, DateTimeArgs<LocalTime> args) {
        String sqlCast = format("SELECT CAST('{}' AS TIME({}) FORMAT '{}')", args.str, precision, args.format);

        checkQuery(sqlCast, args.value, args.error);
    }

    @ParameterizedTest
    @MethodSource("timeWithPrecision")
    public void timeWithPrecisionDynamicParams(int precision, DateTimeArgs<LocalTime> args) {
        String sqlCast = format("SELECT CAST(? AS TIME({}) FORMAT '{}')", precision, args.format);

        checkQuery(sqlCast, args.value, args.error, args.str);
    }

    @ParameterizedTest
    @MethodSource("timeWithPrecision")
    public void timeWithPrecisionUpdateFromDynamicParam(int precision, DateTimeArgs<LocalTime> args) {
        String col = format("time{}_col", precision);

        String sqlCast = format(
                "UPDATE datetime_cols SET {}=CAST(? AS TIME(3) FORMAT '{}') WHERE id = 1",
                col, args.format
        );

        checkDml(sqlCast, args.error, args.str);

        if (args.value != null) {
            assertQuery(format("SELECT {} FROM datetime_cols WHERE id = 1", col))
                    .returns(args.value)
                    .check();
        }
    }

    @ParameterizedTest
    @MethodSource("timeWithPrecision")
    public void timeWithPrecisionUpdateFromLiteral(int precision, DateTimeArgs<LocalTime> args) {
        String col = format("time{}_col", precision);

        // Use TIME(3) to preserve fractional part
        String sqlCast = format(
                "UPDATE datetime_cols SET {}=CAST('{}' AS TIME(3) FORMAT '{}') WHERE id = 1",
                col,
                args.str,
                args.format
        );

        checkDml(sqlCast, args.error);

        if (args.value != null) {
            assertQuery(format("SELECT {} FROM datetime_cols WHERE id = 1", col))
                    .returns(args.value)
                    .check();
        }
    }

    private static Stream<Arguments> timeWithPrecision() {
        return Stream.of(
                // FF1

                Arguments.of(0,
                        dateTime("15:32:17.1", "hh24:mi:ss.ff1", LocalTime.of(15, 32, 17).withNano(0), null)),
                Arguments.of(1,
                        dateTime("15:32:17.1", "hh24:mi:ss.ff1", LocalTime.of(15, 32, 17).withNano(100_000_000), null)),
                Arguments.of(2,
                        dateTime("15:32:17.1", "hh24:mi:ss.ff1", LocalTime.of(15, 32, 17).withNano(100_000_000), null)),
                Arguments.of(3,
                        dateTime("15:32:17.1", "hh24:mi:ss.ff1", LocalTime.of(15, 32, 17).withNano(100_000_000), null)),

                // FF2

                Arguments.of(0,
                        dateTime("15:32:17.12", "hh24:mi:ss.ff2", LocalTime.of(15, 32, 17).withNano(0), null)),
                Arguments.of(1,
                        dateTime("15:32:17.12", "hh24:mi:ss.ff2", LocalTime.of(15, 32, 17).withNano(100_000_000), null)),
                Arguments.of(2,
                        dateTime("15:32:17.12", "hh24:mi:ss.ff2", LocalTime.of(15, 32, 17).withNano(120_000_000), null)),
                Arguments.of(3,
                        dateTime("15:32:17.12", "hh24:mi:ss.ff2", LocalTime.of(15, 32, 17).withNano(120_000_000), null)),

                // FF3
                Arguments.of(0,
                        dateTime("15:32:17.123", "hh24:mi:ss.ff3", LocalTime.of(15, 32, 17).withNano(0), null)),
                Arguments.of(0,
                        dateTime("15:32:17.500", "hh24:mi:ss.ff3", LocalTime.of(15, 32, 17).withNano(0), null)),
                Arguments.of(0,
                        dateTime("15:32:17.999", "hh24:mi:ss.ff3", LocalTime.of(15, 32, 17).withNano(0), null)),

                Arguments.of(1,
                        dateTime("15:32:17.123", "hh24:mi:ss.ff3", LocalTime.of(15, 32, 17).withNano(100_000_000), null)),
                Arguments.of(1,
                        dateTime("15:32:17.500", "hh24:mi:ss.ff3", LocalTime.of(15, 32, 17).withNano(500_000_000), null)),
                Arguments.of(1,
                        dateTime("15:32:17.999", "hh24:mi:ss.ff3", LocalTime.of(15, 32, 17).withNano(900_000_000), null)),

                Arguments.of(2,
                        dateTime("15:32:17.123", "hh24:mi:ss.ff3", LocalTime.of(15, 32, 17).withNano(120_000_000), null)),
                Arguments.of(2,
                        dateTime("15:32:17.500", "hh24:mi:ss.ff3", LocalTime.of(15, 32, 17).withNano(500_000_000), null)),
                Arguments.of(2,
                        dateTime("15:32:17.999", "hh24:mi:ss.ff3", LocalTime.of(15, 32, 17).withNano(990_000_000), null)),

                Arguments.of(3,
                        dateTime("15:32:17.123", "hh24:mi:ss.ff3", LocalTime.of(15, 32, 17).withNano(123_000_000), null)),
                Arguments.of(3,
                        dateTime("15:32:17.500", "hh24:mi:ss.ff3", LocalTime.of(15, 32, 17).withNano(500_000_000), null)),
                Arguments.of(3,
                        dateTime("15:32:17.999", "hh24:mi:ss.ff3", LocalTime.of(15, 32, 17).withNano(999_000_000), null))
        );
    }

    // TIMESTAMP

    @ParameterizedTest
    @MethodSource("timestamp")
    public void timestampLiterals(DateTimeArgs<LocalDateTime> args) {
        String sqlCast = format("SELECT CAST('{}' AS TIMESTAMP FORMAT '{}')", args.str, args.format);

        checkQuery(sqlCast, args.value, args.error);
    }

    private static Stream<DateTimeArgs<LocalDateTime>> timestampNoMillis() {
        // The expected result should be TIMESTAMP(0) (without milliseconds).
        return timestamp().map(dt ->
                new DateTimeArgs<>(dt.str, dt.format, dt.value == null ? null : dt.value.withNano(0), dt.error));
    }

    private static Stream<DateTimeArgs<LocalDateTime>> timestamp() {
        List<DateTimeArgs<LocalDate>> date = date().collect(Collectors.toList());
        List<DateTimeArgs<LocalTime>> time = time().collect(Collectors.toList());

        List<DateTimeArgs<LocalDateTime>> result = new ArrayList<>();

        for (DateTimeArgs<LocalDate> d : date) {
            for (DateTimeArgs<LocalTime> t : time) {
                String tsStr = d.str + " " + t.str;
                String tsFmt = d.format + " " + t.format;

                if (d.value != null && t.value != null) {
                    LocalDateTime tsExpected = LocalDateTime.of(d.value, t.value);
                    result.add(dateTime(tsStr, tsFmt, tsExpected, null));
                } else {
                    result.add(dateTime(tsStr, tsFmt, null, " "));
                }
            }
        }

        result.add(new DateTimeArgs<>("2025-10-02 22:15 +02:30", "YYYY-MM-DD HH24:MI TZH:TZM",
                LocalDateTime.of(2025, 10, 2, 19, 45), null));

        result.add(new DateTimeArgs<>("2025-10-02 22:15 -02:30", "YYYY-MM-DD HH24:MI TZH:TZM",
                LocalDateTime.of(2025, 10, 3, 0, 45), null));

        return result.stream();
    }

    @ParameterizedTest
    @MethodSource("timestampNoMillis")
    public void timestampUpdateFromDynamicParam(DateTimeArgs<LocalDateTime> args) {
        String sqlCast = format(
                "UPDATE datetime_cols SET timestamp0_col=CAST(? AS TIMESTAMP FORMAT '{}') WHERE id = 1",
                args.format
        );

        checkDml(sqlCast, args.error, args.str);

        if (args.value != null) {
            assertQuery("SELECT timestamp0_col FROM datetime_cols WHERE id = 1")
                    .returns(args.value)
                    .check();
        }
    }

    @ParameterizedTest
    @MethodSource("timestampNoMillis")
    public void timestampUpdateFromLiteral(DateTimeArgs<LocalDateTime> args) {
        String sqlCast = format(
                "UPDATE datetime_cols SET timestamp0_col=CAST('{}' AS TIMESTAMP FORMAT '{}') WHERE id = 1",
                args.str, args.format
        );

        checkDml(sqlCast, args.error);

        if (args.value != null) {
            assertQuery("SELECT timestamp0_col FROM datetime_cols WHERE id = 1")
                    .returns(args.value)
                    .check();
        }
    }

    @ParameterizedTest
    @MethodSource("timestampWithPrecision")
    public void timestampWithPrecisionLiterals(int precision, DateTimeArgs<LocalDateTime> args) {
        String sqlCast = format("SELECT CAST('{}' AS TIMESTAMP({}) FORMAT '{}')", args.str, precision, args.format);

        checkQuery(sqlCast, args.value, args.error);
    }

    @ParameterizedTest
    @MethodSource("timestampWithPrecision")
    public void timestampWithPrecisionUpdateFromLiteral(int precision, DateTimeArgs<LocalDateTime> args) {
        String col = format("timestamp{}_col", precision);

        String sqlCast = format(
                "UPDATE datetime_cols SET {}=CAST(? AS TIMESTAMP FORMAT '{}') WHERE id = 1",
                col, args.format
        );

        checkDml(sqlCast, args.error, args.str);

        if (args.value != null) {
            assertQuery(format("SELECT {} FROM datetime_cols WHERE id = 1", col))
                    .returns(args.value)
                    .check();
        }
    }

    @ParameterizedTest
    @MethodSource("timestampWithPrecision")
    public void timestampWithPrecisionUpdateFromDynamicParam(int precision, DateTimeArgs<LocalDateTime> args) {
        String col = format("timestamp{}_col", precision);

        String sqlCast = format(
                "UPDATE datetime_cols SET {}=CAST('{}' AS TIMESTAMP FORMAT '{}') WHERE id = 1",
                col, args.str, args.format
        );

        checkDml(sqlCast, args.error);

        if (args.value != null) {
            assertQuery(format("SELECT {} FROM datetime_cols WHERE id = 1", col))
                    .returns(args.value)
                    .check();
        }
    }

    private static Stream<Arguments> timestampWithPrecision() {
        LocalDate date = LocalDate.of(2020, 2, 5);
        return Stream.of(
                // FF1
                Arguments.of(0,
                        dateTime("2020-02-05 15:32:17.1", "yyyy-MM-dd hh24:mi:ss.ff1",
                                LocalDateTime.of(date, LocalTime.of(15, 32, 17).withNano(0)), null)),
                Arguments.of(1,
                        dateTime("2020-02-05 15:32:17.1", "yyyy-MM-dd hh24:mi:ss.ff1",
                                LocalDateTime.of(date, LocalTime.of(15, 32, 17).withNano(100_000_000)), null)),
                Arguments.of(2,
                        dateTime("2020-02-05 15:32:17.1", "yyyy-MM-dd hh24:mi:ss.ff1",
                                LocalDateTime.of(date, LocalTime.of(15, 32, 17).withNano(100_000_000)), null)),
                Arguments.of(3,
                        dateTime("2020-02-05 15:32:17.1", "yyyy-MM-dd hh24:mi:ss.ff1",
                                LocalDateTime.of(date, LocalTime.of(15, 32, 17).withNano(100_000_000)), null)),

                // FF2

                Arguments.of(0,
                        dateTime("2020-02-05 15:32:17.12", "yyyy-MM-dd hh24:mi:ss.ff2",
                                LocalDateTime.of(date, LocalTime.of(15, 32, 17).withNano(0)), null)),
                Arguments.of(1,
                        dateTime("2020-02-05 15:32:17.12", "yyyy-MM-dd hh24:mi:ss.ff2",
                                LocalDateTime.of(date, LocalTime.of(15, 32, 17).withNano(100_000_000)), null)),
                Arguments.of(2,
                        dateTime("2020-02-05 15:32:17.12", "yyyy-MM-dd hh24:mi:ss.ff2",
                                LocalDateTime.of(date, LocalTime.of(15, 32, 17).withNano(120_000_000)), null)),
                Arguments.of(3,
                        dateTime("2020-02-05 15:32:17.12", "yyyy-MM-dd hh24:mi:ss.ff2",
                                LocalDateTime.of(date, LocalTime.of(15, 32, 17).withNano(120_000_000)), null)),

                // FF3

                Arguments.of(0,
                        dateTime("2020-02-05 15:32:17.123", "yyyy-MM-dd hh24:mi:ss.ff3",
                                LocalDateTime.of(date, LocalTime.of(15, 32, 17).withNano(0)), null)),
                Arguments.of(0,
                        dateTime("2020-02-05 15:32:17.500", "yyyy-MM-dd hh24:mi:ss.ff3",
                                LocalDateTime.of(date, LocalTime.of(15, 32, 17).withNano(0)), null)),
                Arguments.of(0,
                        dateTime("2020-02-05 15:32:17.999", "yyyy-MM-dd hh24:mi:ss.ff3",
                                LocalDateTime.of(date, LocalTime.of(15, 32, 17).withNano(0)), null)),

                Arguments.of(1,
                        dateTime("2020-02-05 15:32:17.123", "yyyy-MM-dd hh24:mi:ss.ff3",
                                LocalDateTime.of(date, LocalTime.of(15, 32, 17).withNano(100_000_000)), null)),
                Arguments.of(1,
                        dateTime("2020-02-05 15:32:17.500", "yyyy-MM-dd hh24:mi:ss.ff3",
                                LocalDateTime.of(date, LocalTime.of(15, 32, 17).withNano(500_000_000)), null)),
                Arguments.of(1,
                        dateTime("2020-02-05 15:32:17.999", "yyyy-MM-dd hh24:mi:ss.ff3",
                                LocalDateTime.of(date, LocalTime.of(15, 32, 17).withNano(900_000_000)), null)),

                Arguments.of(2,
                        dateTime("2020-02-05 15:32:17.123", "yyyy-MM-dd hh24:mi:ss.ff3",
                                LocalDateTime.of(date, LocalTime.of(15, 32, 17).withNano(120_000_000)), null)),
                Arguments.of(2,
                        dateTime("2020-02-05 15:32:17.500", "yyyy-MM-dd hh24:mi:ss.ff3",
                                LocalDateTime.of(date, LocalTime.of(15, 32, 17).withNano(500_000_000)), null)),
                Arguments.of(2,
                        dateTime("2020-02-05 15:32:17.999", "yyyy-MM-dd hh24:mi:ss.ff3",
                                LocalDateTime.of(date, LocalTime.of(15, 32, 17).withNano(990_000_000)), null)),

                Arguments.of(3,
                        dateTime("2020-02-05 15:32:17.123", "yyyy-MM-dd hh24:mi:ss.ff3",
                                LocalDateTime.of(date, LocalTime.of(15, 32, 17).withNano(123_000_000)), null)),
                Arguments.of(3,
                        dateTime("2020-02-05 15:32:17.500", "yyyy-MM-dd hh24:mi:ss.ff3",
                                LocalDateTime.of(date, LocalTime.of(15, 32, 17).withNano(500_000_000)), null)),
                Arguments.of(3,
                        dateTime("2020-02-05 15:32:17.999", "yyyy-MM-dd hh24:mi:ss.ff3",
                                LocalDateTime.of(date, LocalTime.of(15, 32, 17).withNano(999_000_000)), null))
        );
    }

    // TIMESTAMP LTZ

    @ParameterizedTest
    @MethodSource("timestampLtz")
    public void timestampLtzLiterals(DateTimeArgs<Instant> args) {
        String sqlCast = format(
                "SELECT CAST('{}' AS TIMESTAMP WITH LOCAL TIME ZONE FORMAT '{}')",
                args.str, args.format
        );

        checkQuery(sqlCast, args.value, args.error);
    }

    @ParameterizedTest
    @MethodSource("timestampLtzNoMillis")
    public void timestampLtzUpdateFromDynamicParam(DateTimeArgs<Instant> args) {
        String sqlCast = format(""
                        + "UPDATE datetime_cols "
                        + "SET timestamp_with_local_time_zone0_col=CAST(? AS TIMESTAMP WITH LOCAL TIME ZONE FORMAT '{}') WHERE id = 1",
                args.format
        );

        checkDml(sqlCast, args.error, args.str);

        if (args.value != null) {
            assertQuery("SELECT timestamp_with_local_time_zone0_col FROM datetime_cols WHERE id = 1")
                    .returns(args.value)
                    .check();
        }
    }

    @ParameterizedTest
    @MethodSource("timestampLtzNoMillis")
    public void timestampLtzUpdateFromLiteral(DateTimeArgs<Instant> args) {
        String sqlCast = format(""
                        + "UPDATE datetime_cols "
                        + "SET timestamp_with_local_time_zone0_col=CAST('{}' AS TIMESTAMP WITH LOCAL TIME ZONE FORMAT '{}') WHERE id = 1",
                args.str, args.format
        );

        checkDml(sqlCast, args.error);

        if (args.value != null) {
            assertQuery("SELECT timestamp_with_local_time_zone0_col FROM datetime_cols WHERE id = 1")
                    .returns(args.value)
                    .check();
        }
    }

    private static Stream<DateTimeArgs<Instant>> timestampLtzNoMillis() {
        // The expected result should be without milliseconds.
        return timestampLtz().map(dt ->
                dateTime(dt.str, dt.format, dt.value == null ? null : dt.value.with(ChronoField.NANO_OF_SECOND, 0), dt.error));
    }

    private static Stream<DateTimeArgs<Instant>> timestampLtz() {
        return timestamp().map(dt -> {
            if (dt.value != null) {
                Instant expectedInstant = ZonedDateTime.of(dt.value, TIME_ZONE_ID).toInstant();
                return dateTime(dt.str, dt.format, expectedInstant, null);
            } else {
                return dateTime(dt.str, dt.format, null, " ");
            }
        });
    }

    @ParameterizedTest
    @MethodSource("timestampLtzWithPrecision")
    public void timestampLtzWithPrecisionLiterals(int precision, DateTimeArgs<Instant> args) {
        String sqlCast = format(
                "SELECT CAST('{}' AS TIMESTAMP({}) WITH LOCAL TIME ZONE FORMAT '{}')",
                args.str, precision, args.format
        );

        checkQuery(sqlCast, args.value, args.error);
    }

    @ParameterizedTest
    @MethodSource("timestampLtzWithPrecision")
    public void timestampLtzWithPrecisionUpdateFromLiteral(int precision, DateTimeArgs<Instant> args) {
        String col = format("timestamp_with_local_time_zone{}_col", precision);

        String sqlCast = format(""
                        + "UPDATE datetime_cols "
                        + "SET {}=CAST(? AS TIMESTAMP WITH LOCAL TIME ZONE FORMAT '{}') WHERE id = 1",
                col, args.format
        );

        checkDml(sqlCast, args.error, args.str);

        if (args.value != null) {
            assertQuery(format("SELECT {} FROM datetime_cols WHERE id = 1", col))
                    .returns(args.value)
                    .check();
        }
    }

    @ParameterizedTest
    @MethodSource("timestampLtzWithPrecision")
    public void timestampLtzWithPrecisionUpdateFromDynamicParam(int precision, DateTimeArgs<Instant> args) {
        String col = format("timestamp_with_local_time_zone{}_col", precision);

        String sqlCast = format(""
                        + "UPDATE datetime_cols "
                        + "SET {}=CAST('{}' AS TIMESTAMP WITH LOCAL TIME ZONE FORMAT '{}') WHERE id = 1",
                col, args.str, args.format
        );

        checkDml(sqlCast, args.error);

        if (args.value != null) {
            assertQuery(format("SELECT {} FROM datetime_cols WHERE id = 1", col))
                    .returns(args.value)
                    .check();
        }
    }

    private static Stream<Arguments> timestampLtzWithPrecision() {
        return timestampWithPrecision().map(a -> {
            Object[] array = a.get();
            int precision = (int) array[0];
            DateTimeArgs<LocalDateTime> args = (DateTimeArgs<LocalDateTime>) array[1];
            assert args.value != null;

            Instant expectedInstant = ZonedDateTime.of(args.value, TIME_ZONE_ID).toInstant();

            return Arguments.of(precision, dateTime(args.str, args.format, expectedInstant, null));
        });
    }

    @ParameterizedTest
    @MethodSource("dateFormat")
    public void dateFormatLiterals(DateTimeArgs<LocalDate> args) {
        String lit = DateTimeFormatter.ISO_DATE.format(args.value);
        String sqlCast = format("SELECT CAST(DATE '{}' AS VARCHAR FORMAT '{}')", lit, args.format);

        checkQuery(sqlCast, args.str, args.error);
    }

    @ParameterizedTest
    @MethodSource("dateFormat")
    public void dateFormatDynamicParams(DateTimeArgs<LocalDate> args) {
        String sqlCast = format("SELECT CAST(? AS VARCHAR FORMAT '{}')", args.format);

        checkQuery(sqlCast, args.str, args.error, args.value);
    }

    private static Stream<DateTimeArgs<LocalDate>> dateFormat() {
        return Stream.of(
                dateTime("2000-01-01", "yyyy-MM-dd", LocalDate.of(2000, 1, 1), null),
                dateTime("2-01-01", "y-MM-dd", LocalDate.of(2022, 1, 1), null),
                dateTime("20-01-01", "yy-MM-dd", LocalDate.of(2020, 1, 1), null),
                dateTime("020-01-01", "yyy-MM-dd", LocalDate.of(2020, 1, 1), null),
                dateTime("002-01-01", "yyy-MM-dd", LocalDate.of(2002, 1, 1), null),
                dateTime("200-01-01", "yyy-MM-dd", LocalDate.of(2200, 1, 1), null),
                dateTime("0020-01-01", "yyyy-MM-dd", LocalDate.of(20, 1, 1), null),
                dateTime("9999-01-01", "yyyy-MM-dd", LocalDate.of(9999, 1, 1), null),
                dateTime("2000/01-01", "yyyy/MM-dd", LocalDate.of(2000, 1, 1), null),

                dateTime("01-01-01", "RR-MM-dd", LocalDate.of(2001, 1, 1), null),
                dateTime("33-01-01", "RR-MM-dd", LocalDate.of(2033, 1, 1), null),
                dateTime("49-01-01", "RR-MM-dd", LocalDate.of(2049, 1, 1), null),
                dateTime("51-01-01", "RR-MM-dd", LocalDate.of(1951, 1, 1), null),
                dateTime("77-01-01", "RR-MM-dd", LocalDate.of(1977, 1, 1), null),

                dateTime("2001-01-01", "RRRR-MM-dd", LocalDate.of(2001, 1, 1), null),
                dateTime("2001-01-01", "RRRR-MM-dd", LocalDate.of(2001, 1, 1), null),
                dateTime("2033-01-01", "RRRR-MM-dd", LocalDate.of(2033, 1, 1), null),
                dateTime("2033-01-01", "RRRR-MM-dd", LocalDate.of(2033, 1, 1), null),
                dateTime("1951-01-01", "RRRR-MM-dd", LocalDate.of(1951, 1, 1), null),
                dateTime("1951-01-01", "RRRR-MM-dd", LocalDate.of(1951, 1, 1), null),
                dateTime("1977-01-01", "RRRR-MM-dd", LocalDate.of(1977, 1, 1), null),
                dateTime("0151-01-01", "RRRR-MM-dd", LocalDate.of(151, 1, 1), null),
                dateTime("0177-01-01", "RRRR-MM-dd", LocalDate.of(177, 1, 1), null)
        );
    }

    @ParameterizedTest
    @MethodSource("timeFormat")
    public void timeFormatLiterals(DateTimeArgs<LocalTime> args) {
        String lit = DateTimeFormatter.ofPattern("HH:mm:ss.SSS").format(args.value);
        String sqlCast = format("SELECT CAST(TIME '{}' AS VARCHAR FORMAT '{}')", lit, args.format);

        checkQuery(sqlCast, args.str, args.error);
    }

    @ParameterizedTest
    @MethodSource("timeFormat")
    public void timeFormatDynamicParams(DateTimeArgs<LocalTime> args) {
        String sqlCast = format("SELECT CAST(? AS VARCHAR FORMAT '{}')", args.format);

        checkQuery(sqlCast, args.str, args.error, args.value);
    }

    private static Stream<DateTimeArgs<LocalTime>> timeFormat() {
        return Stream.of(
                dateTime("05:02 A.M.", "hh12:mi A.M.", LocalTime.of(5, 2), null),
                dateTime("11:02 A.M.", "hh12:mi A.M.", LocalTime.of(11, 2), null),
                dateTime("12:02 A.M.", "hh12:mi A.M.", LocalTime.of(0, 2), null),

                dateTime("05:02 P.M.", "hh12:mi P.M.", LocalTime.of(17, 2), null),
                dateTime("11:02 P.M.", "hh12:mi P.M.", LocalTime.of(23, 2), null),
                dateTime("12:02 P.M.", "hh12:mi P.M.", LocalTime.of(12, 2), null),

                // hh24
                dateTime("12:02:03", "hh24:mi:ss", LocalTime.of(12, 2, 3), null),
                dateTime("23:02:03", "hh24:mi:ss", LocalTime.of(23, 2, 3), null),
                dateTime("23/02:03", "hh24/mi:ss", LocalTime.of(23, 2, 3), null),

                // fractional
                dateTime("23:02:03.99", "hh24:mi:ss.ff2", LocalTime.of(23, 2, 3, 990_000_000), null),
                dateTime("23:02:03.999", "hh24:mi:ss.ff3", LocalTime.of(23, 2, 3, 999_000_000), null),
                dateTime("23:02:03.123", "hh24:mi:ss.ff3", LocalTime.of(23, 2, 3, 123_000_000), null),
                dateTime("23:02:03.1230", "hh24:mi:ss.ff4", LocalTime.of(23, 2, 3, 123_000_000), null),
                dateTime("23:02:03.120", "hh24:mi:ss.ff3", LocalTime.of(23, 2, 3, 120_000_000), null)
        );
    }

    @ParameterizedTest
    @MethodSource("timestampFormat")
    public void timestampFormatLiterals(DateTimeArgs<LocalDateTime> args) {
        String lit = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS").format(args.value);
        String sqlCast = format("SELECT CAST(TIMESTAMP '{}' AS VARCHAR FORMAT '{}')", lit, args.format);

        checkQuery(sqlCast, args.str, args.error);
    }

    @ParameterizedTest
    @MethodSource("timestampFormat")
    public void timestampFormatDynamicParams(DateTimeArgs<LocalDateTime> args) {
        String sqlCast = format("SELECT CAST(? AS VARCHAR FORMAT '{}')", args.format);

        checkQuery(sqlCast, args.str, args.error, args.value);
    }

    private static Stream<DateTimeArgs<LocalDateTime>> timestampFormat() {
        return Stream.concat(
                timestampFormatBasic(),
                Stream.of(
                        new DateTimeArgs<>("2025-10-02 19:45 +00:00", "YYYY-MM-DD HH24:MI TZH:TZM",
                                LocalDateTime.of(2025, 10, 2, 19, 45), null),

                        new DateTimeArgs<>("2025-10-03 00:45 +00:00", "YYYY-MM-DD HH24:MI TZH:TZM",
                                LocalDateTime.of(2025, 10, 3, 0, 45), null)
                )
        );
    }

    private static Stream<DateTimeArgs<LocalDateTime>> timestampFormatBasic() {
        List<DateTimeArgs<LocalDate>> date = dateFormat().collect(Collectors.toList());
        List<DateTimeArgs<LocalTime>> time = timeFormat().collect(Collectors.toList());

        List<DateTimeArgs<LocalDateTime>> result = new ArrayList<>();

        for (DateTimeArgs<LocalDate> d : date) {
            for (DateTimeArgs<LocalTime> t : time) {
                String tsStr = d.str + " " + t.str;
                String tsFmt = d.format + " " + t.format;

                LocalDateTime tsExpected = LocalDateTime.of(d.value, t.value);
                result.add(dateTime(tsStr, tsFmt, tsExpected, null));
            }
        }

        return result.stream();
    }

    @ParameterizedTest
    @MethodSource("timestampFormatLtz")
    public void timestampLtzFormatLiterals(DateTimeArgs<ZonedDateTime> args) {
        String lit = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS").format(args.value);
        String sqlCast = format("SELECT CAST(TIMESTAMP WITH LOCAL TIME ZONE '{}' AS VARCHAR FORMAT '{}')", lit, args.format);

        checkQuery(sqlCast, args.str, args.error);
    }

    @ParameterizedTest
    @MethodSource("timestampFormatLtz")
    public void timestampLtzFormatDynamicParams(DateTimeArgs<ZonedDateTime> args) {
        String sqlCast = format("SELECT CAST(? AS VARCHAR FORMAT '{}')", args.format);

        checkQuery(sqlCast, args.str, args.error, args.value.toInstant());
    }

    private static Stream<DateTimeArgs<ZonedDateTime>> timestampFormatLtz() {
        return timestampFormatBasic()
                .filter(dt -> dt.value == null || dt.value.getYear() >= 1900)
                .map(dt -> {
                    if (dt.value != null) {
                        ZonedDateTime dateTime = ZonedDateTime.of(dt.value, TIME_ZONE_ID);

                        return dateTime(dt.str, dt.format, dateTime, null);
                    } else {
                        return dateTime(dt.str, dt.format, null, " ");
                    }
                });
    }

    private static <T> DateTimeArgs<T> dateTime(String str, String format, @Nullable T value, @Nullable String error) {
        return new DateTimeArgs<>(str, format, value, error);
    }

    static class DateTimeArgs<T> {

        final String str;

        final String format;

        @Nullable
        private final String error;

        @Nullable
        private final T value;

        DateTimeArgs(String str, String format, @Nullable T value, @Nullable String error) {
            this.str = str;
            this.format = format;
            this.error = error;
            this.value = value;
        }

        public String toString() {
            return str + " " + format + " " + (error != null ? "ERROR: " + error : "") + (value != null ? "= " + value : "");
        }
    }

    private static void checkQuery(String query, @Nullable Object expected, @Nullable String error, Object... params) {
        if (error != null) {
            try {
                Ignite node = CLUSTER.node(0);
                List<List<Object>> rows = sql(node, null, SqlCommon.DEFAULT_SCHEMA_NAME, TIME_ZONE_ID, query, params);
                fail("Expected error: " + error + ". But got rows: " + rows);
            } catch (SqlException e) {
                e.printStackTrace(System.err);
                assertThat("error message", e.getMessage(), containsString(error));
            }
        } else {
            assertQuery(query)
                    .withTimeZoneId(TIME_ZONE_ID)
                    .withParams(params)
                    .returns(expected)
                    .check();
        }
    }

    private static void checkDml(String query, @Nullable String error, Object... params) {
        if (error != null) {
            try {
                Ignite node = CLUSTER.node(0);
                sql(node, null, SqlCommon.DEFAULT_SCHEMA_NAME, TIME_ZONE_ID, query, params);

                List<?> rows = sql("SELECT * FROM datetime_cols");

                fail("Expected error: " + error + ". But got rows: " + rows);
            } catch (SqlException e) {
                e.printStackTrace(System.err);
                assertThat("error message", e.getMessage(), containsString(error));
            }
        } else {
            assertQuery(query)
                    .withTimeZoneId(TIME_ZONE_ID)
                    .withParams(params)
                    .returns(1L)
                    .check();
        }
    }
}
