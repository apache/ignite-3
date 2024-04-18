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

package org.apache.ignite.internal.sql.engine;

import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;
import static org.apache.ignite.internal.sql.engine.util.SqlTestUtils.assertThrowsSqlException;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.Period;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoField;
import java.time.temporal.ChronoUnit;
import java.time.temporal.Temporal;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.BiConsumer;
import java.util.function.Function;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.internal.sql.BaseSqlIntegrationTest;
import org.apache.ignite.lang.ErrorGroups.Sql;
import org.apache.ignite.lang.IgniteException;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;

/** Interval coverage tests. */
public class ItIntervalTest extends BaseSqlIntegrationTest {
    @Override
    protected int initialNodes() {
        return 1;
    }

    /**
     * Test returned result for interval data types.
     */
    @Test
    public void testIntervalResult() {
        assertEquals(Duration.ofDays(4), eval("INTERVAL 4 DAYS"));
        assertEquals(Duration.ofSeconds(1), eval("INTERVAL 1 SECONDS"));
        assertEquals(Duration.ofSeconds(-1), eval("INTERVAL -1 SECONDS"));
        assertEquals(Duration.ofSeconds(123), eval("INTERVAL 123 SECONDS"));
        assertEquals(Duration.ofSeconds(123), eval("INTERVAL '123' SECONDS(3)"));
        assertEquals(Duration.ofMinutes(2), eval("INTERVAL 2 MINUTES"));
        assertEquals(Duration.ofHours(3), eval("INTERVAL 3 HOURS"));
        assertEquals(Duration.ofDays(4), eval("INTERVAL 4 DAYS"));
        assertEquals(Period.ofMonths(5), eval("INTERVAL 5 MONTHS"));
        assertEquals(Period.ofMonths(-5), eval("INTERVAL -5 MONTHS"));
        assertEquals(Period.ofYears(6), eval("INTERVAL 6 YEARS"));
        assertEquals(Period.of(1, 2, 0), eval("INTERVAL '1-2' YEAR TO MONTH"));
        assertEquals(Duration.ofHours(25), eval("INTERVAL '1 1' DAY TO HOUR"));
        assertEquals(Duration.ofMinutes(62), eval("INTERVAL '1:2' HOUR TO MINUTE"));
        assertEquals(Duration.ofSeconds(63), eval("INTERVAL '1:3' MINUTE TO SECOND"));
        assertEquals(Duration.ofSeconds(3723), eval("INTERVAL '1:2:3' HOUR TO SECOND"));
        assertEquals(Duration.ofMillis(3723456), eval("INTERVAL '0 1:2:3.456' DAY TO SECOND"));

        assertThrowsEx("SELECT INTERVAL '123' SECONDS", IgniteException.class, "exceeds precision");

        // Interval range overflow
        assertThrowsSqlException(Sql.RUNTIME_ERR, "INTEGER out of range", () -> sql("SELECT INTERVAL 5000000 MONTHS * 1000"));
        assertThrowsSqlException(Sql.RUNTIME_ERR, "BIGINT out of range", () -> sql("SELECT DATE '2021-01-01' + INTERVAL 999999999999 DAY"));
        assertThrowsSqlException(Sql.RUNTIME_ERR, "INTEGER out of range", () -> sql("SELECT DATE '2021-01-01' + INTERVAL -999999999 YEAR"));
    }

    /**
     * Test cast interval types to integer and integer to interval.
     */
    @Test
    public void testIntervalIntCast() {
        assertNull(eval("CAST(NULL::INTERVAL SECONDS AS INT)"));
        assertNull(eval("CAST(NULL::INTERVAL MONTHS AS INT)"));
        assertEquals(1, eval("CAST(INTERVAL 1 SECONDS AS INT)"));
        assertEquals(2, eval("CAST(INTERVAL 2 MINUTES AS INT)"));
        assertEquals(3, eval("CAST(INTERVAL 3 HOURS AS INT)"));
        assertEquals(4, eval("CAST(INTERVAL 4 DAYS AS INT)"));
        assertEquals(-4, eval("CAST(INTERVAL -4 DAYS AS INT)"));
        assertEquals(5, eval("CAST(INTERVAL 5 MONTHS AS INT)"));
        assertEquals(6, eval("CAST(INTERVAL 6 YEARS AS INT)"));
        assertEquals(-6, eval("CAST(INTERVAL -6 YEARS AS INT)"));

        assertEquals("+6", eval("CAST(INTERVAL 6 YEARS AS VARCHAR)"));
        assertEquals("+1", eval("CAST(INTERVAL 1 HOUR AS VARCHAR)"));
        assertEquals("+7.000000", eval("CAST(INTERVAL 7 SECONDS AS VARCHAR)"));

        assertNull(eval("CAST(NULL::INT AS INTERVAL SECONDS)"));
        assertNull(eval("CAST(NULL::INT AS INTERVAL MONTHS)"));
        assertEquals(Duration.ofSeconds(1), eval("CAST(1 AS INTERVAL SECONDS)"));
        assertEquals(Duration.ofMinutes(2), eval("CAST(2 AS INTERVAL MINUTES)"));
        assertEquals(Duration.ofHours(3), eval("CAST(3 AS INTERVAL HOURS)"));
        assertEquals(Duration.ofDays(4), eval("CAST(4 AS INTERVAL DAYS)"));
        assertEquals(Period.ofMonths(5), eval("CAST(5 AS INTERVAL MONTHS)"));
        assertEquals(Period.ofYears(6), eval("CAST(6 AS INTERVAL YEARS)"));

        // Compound interval types cannot be cast.
        assertThrowsEx("SELECT CAST(INTERVAL '1-2' YEAR TO MONTH AS INT)", IgniteException.class, "cannot convert");
        assertThrowsEx("SELECT CAST(INTERVAL '1 2' DAY TO HOUR AS INT)", IgniteException.class, "cannot convert");

        assertThrowsEx("SELECT CAST(1 AS INTERVAL YEAR TO MONTH)", IgniteException.class, "cannot convert");
        assertThrowsEx("SELECT CAST(1 AS INTERVAL DAY TO HOUR)", IgniteException.class, "cannot convert");
    }

    /**
     * Test cast interval types to string and string to interval.
     */
    @Test
    public void testIntervalStringCast() {
        assertNull(eval("CAST(NULL::INTERVAL SECONDS AS VARCHAR)"));
        assertNull(eval("CAST(NULL::INTERVAL MONTHS AS VARCHAR)"));
        assertEquals("+1.234", eval("CAST(INTERVAL '1.234' SECONDS (1,3) AS VARCHAR)"));
        assertEquals("+1.000000", eval("CAST(INTERVAL 1 SECONDS AS VARCHAR)"));
        assertEquals("+2", eval("CAST(INTERVAL 2 MINUTES AS VARCHAR)"));
        assertEquals("+3", eval("CAST(INTERVAL 3 HOURS AS VARCHAR)"));
        assertEquals("+4", eval("CAST(INTERVAL 4 DAYS AS VARCHAR)"));
        assertEquals("+5", eval("CAST(INTERVAL 5 MONTHS AS VARCHAR)"));
        assertEquals("+6", eval("CAST(INTERVAL 6 YEARS AS VARCHAR)"));
        assertEquals("+1-02", eval("CAST(INTERVAL '1-2' YEAR TO MONTH AS VARCHAR)"));
        assertEquals("+1 02", eval("CAST(INTERVAL '1 2' DAY TO HOUR AS VARCHAR)"));
        assertEquals("-1 02:03:04.000000", eval("CAST(INTERVAL '-1 2:3:4' DAY TO SECOND AS VARCHAR)"));

        assertNull(eval("CAST(NULL::VARCHAR AS INTERVAL SECONDS)"));
        assertNull(eval("CAST(NULL::VARCHAR AS INTERVAL MONTHS)"));
        assertEquals(Duration.ofSeconds(1), eval("CAST('1' AS INTERVAL SECONDS)"));
        assertEquals(Duration.ofMinutes(2), eval("CAST('2' AS INTERVAL MINUTES)"));
        assertEquals(Duration.ofHours(3), eval("CAST('3' AS INTERVAL HOURS)"));
        assertEquals(Duration.ofDays(4), eval("CAST('4' AS INTERVAL DAYS)"));
        assertEquals(Duration.ofHours(26), eval("CAST('1 2' AS INTERVAL DAY TO HOUR)"));
        assertEquals(Duration.ofMinutes(62), eval("CAST('1:2' AS INTERVAL HOUR TO MINUTE)"));
        assertEquals(Duration.ofMillis(3723456), eval("CAST('0 1:2:3.456' AS INTERVAL DAY TO SECOND)"));
        assertEquals(Duration.ofMillis(-3723456), eval("CAST('-0 1:2:3.456' AS INTERVAL DAY TO SECOND)"));
        assertEquals(Period.ofMonths(5), eval("CAST('5' AS INTERVAL MONTHS)"));
        assertEquals(Period.ofYears(6), eval("CAST('6' AS INTERVAL YEARS)"));
        assertEquals(Period.of(1, 2, 0), eval("CAST('1-2' AS INTERVAL YEAR TO MONTH)"));
    }

    /**
     * Test cast between interval types.
     */
    @Test
    public void testIntervalToIntervalCast() {
        assertNull(eval("CAST(NULL::INTERVAL MINUTE AS INTERVAL SECONDS)"));
        assertNull(eval("CAST(NULL::INTERVAL YEAR AS INTERVAL MONTHS)"));
        assertEquals(Duration.ofMinutes(1), eval("CAST(INTERVAL 60 SECONDS AS INTERVAL MINUTE)"));
        assertEquals(Duration.ofHours(1), eval("CAST(INTERVAL 60 MINUTES AS INTERVAL HOUR)"));
        assertEquals(Duration.ofDays(1), eval("CAST(INTERVAL 24 HOURS AS INTERVAL DAY)"));
        assertEquals(Period.ofYears(1), eval("CAST(INTERVAL 1 YEAR AS INTERVAL MONTHS)"));
        assertEquals(Period.ofYears(1), eval("CAST(INTERVAL 12 MONTHS AS INTERVAL YEARS)"));

        // Cannot convert between month-year and day-time interval types.
        assertThrowsEx("SELECT CAST(INTERVAL 1 MONTHS AS INTERVAL DAYS)", IgniteException.class, "cannot convert");
        assertThrowsEx("SELECT CAST(INTERVAL 1 DAYS AS INTERVAL MONTHS)", IgniteException.class, "cannot convert");
    }

    /**
     * Test DML statements with interval data type.
     */
    @Disabled("https://issues.apache.org/jira/browse/IGNITE-16637")
    @Test
    public void testDml() {
        sql("CREATE TABLE test(id int PRIMARY KEY, ym INTERVAL YEAR, dt INTERVAL DAYS)");
        sql("INSERT INTO test VALUES (1, INTERVAL 1 MONTH, INTERVAL 2 DAYS)");
        sql("INSERT INTO test VALUES (2, INTERVAL 3 YEARS, INTERVAL 4 HOURS)");
        sql("INSERT INTO test VALUES (3, INTERVAL '4-5' YEARS TO MONTHS, INTERVAL '6:7' HOURS TO MINUTES)");
        sql("INSERT INTO test VALUES (4, NULL, NULL)");

        assertThrowsEx("INSERT INTO test VALUES (5, INTERVAL 1 DAYS, INTERVAL 1 HOURS)", IgniteInternalException.class,
                "cannot assign");

        assertThrowsEx("INSERT INTO test VALUES (6, INTERVAL 1 YEARS, INTERVAL 1 MONTHS)", IgniteInternalException.class,
                "cannot assign");

        assertQuery("SELECT ym, dt FROM test")
                .returns(Period.ofMonths(1), Duration.ofDays(2))
                .returns(Period.ofYears(3), Duration.ofHours(4))
                .returns(Period.of(4, 5, 0), Duration.ofMinutes(367))
                .returns(null, null)
                .check();

        assertThrowsEx("SELECT * FROM test WHERE ym = INTERVAL 6 DAYS", IgniteInternalException.class, "Cannot apply");
        assertThrowsEx("SELECT * FROM test WHERE dt = INTERVAL 6 YEARS", IgniteInternalException.class, "Cannot apply");

        sql("UPDATE test SET dt = INTERVAL 3 DAYS WHERE ym = INTERVAL 1 MONTH");
        sql("UPDATE test SET ym = INTERVAL 5 YEARS WHERE dt = INTERVAL 4 HOURS");
        sql("UPDATE test SET ym = INTERVAL '6-7' YEARS TO MONTHS, dt = INTERVAL '8 9' DAYS TO HOURS "
                + "WHERE ym = INTERVAL '4-5' YEARS TO MONTHS AND dt = INTERVAL '6:7' HOURS TO MINUTES");

        assertThrowsEx("UPDATE test SET dt = INTERVAL 5 YEARS WHERE ym = INTERVAL 1 MONTH", IgniteInternalException.class,
                "Cannot assign");

        assertThrowsEx("UPDATE test SET ym = INTERVAL 8 YEARS WHERE dt = INTERVAL 1 MONTH", IgniteInternalException.class,
                "Cannot apply");

        assertQuery("SELECT * FROM test")
                .returns(Period.ofMonths(1), Duration.ofDays(3))
                .returns(Period.ofYears(5), Duration.ofHours(4))
                .returns(Period.of(6, 7, 0), Duration.ofHours(201))
                .returns(null, null)
                .check();

        assertThrowsEx("DELETE FROM test WHERE ym = INTERVAL 6 DAYS", IgniteInternalException.class, "cannot apply");
        assertThrowsEx("DELETE FROM test WHERE dt = INTERVAL 6 YEARS", IgniteInternalException.class, "cannot apply");

        sql("DELETE FROM test WHERE ym = INTERVAL 1 MONTH");
        sql("DELETE FROM test WHERE dt = INTERVAL 4 HOURS");
        sql("DELETE FROM test WHERE ym = INTERVAL '6-7' YEARS TO MONTHS AND dt = INTERVAL '8 9' DAYS TO HOURS");
        sql("DELETE FROM test WHERE ym IS NULL AND dt IS NULL");

        assertEquals(0, sql("SELECT * FROM test").size());

        sql("ALTER TABLE test ADD (ym2 INTERVAL MONTH, dt2 INTERVAL HOURS)");

        sql("INSERT INTO test(id, ym, ym2, dt, dt2) VALUES (7, INTERVAL 1 YEAR, INTERVAL 2 YEARS, "
                + "INTERVAL 1 SECOND, INTERVAL 2 MINUTES)");

        assertQuery("SELECT ym, ym2, dt, dt2 FROM test")
                .returns(Period.ofYears(1), Period.ofYears(2), Duration.ofSeconds(1), Duration.ofMinutes(2))
                .check();
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("dateTimeIntervalTestCases")
    public void testBasicDateTimeIntervalArithmetic(DateTimeIntervalBasicTestCase testCase) {
        assertQuery(testCase.query())
                .withTimeZoneId(DateTimeIntervalBasicTestCase.TIME_ZONE_ID)
                .returns(testCase.expected())
                .check();
    }

    /**
     * Test date and time interval arithmetic.
     */
    @Test
    public void testDateTimeIntervalArithmetic() {
        // Date +/- interval.
        assertEquals(LocalDate.parse("2020-12-31"), eval("DATE '2021-01-01' + INTERVAL -1 DAY"));
        assertEquals(LocalDate.parse("2022-02-01"), eval("DATE '2021-01-01' + INTERVAL '1-1' YEAR TO MONTH"));

        // Date - date as interval.
        assertEquals(Duration.ofDays(1), eval("(DATE '2021-01-02' - DATE '2021-01-01') DAYS"));
        assertEquals(Duration.ofDays(-1), eval("(DATE '2021-01-01' - DATE '2021-01-02') DAYS"));
        assertEquals(Duration.ofDays(1), eval("(DATE '2021-01-02' - DATE '2021-01-01') HOURS"));
        assertEquals(Period.ofYears(1), eval("(DATE '2022-01-01' - DATE '2021-01-01') YEARS"));
        assertEquals(Period.ofMonths(1), eval("(DATE '2021-02-01' - DATE '2021-01-01') MONTHS"));
        assertEquals(Period.ofMonths(-1), eval("(DATE '2021-01-01' - DATE '2021-02-01') MONTHS"));
        assertEquals(Period.ofMonths(0), eval("(DATE '2021-01-20' - DATE '2021-01-01') MONTHS"));

        // Time - time as interval.
        assertEquals(Duration.ofHours(1), eval("(TIME '02:00:00' - TIME '01:00:00') HOURS"));
        assertEquals(Duration.ofMinutes(1), eval("(TIME '00:02:00' - TIME '00:01:00') HOURS"));
        assertEquals(Duration.ofMinutes(1), eval("(TIME '00:02:00' - TIME '00:01:00') MINUTES"));
        assertEquals(Duration.ofSeconds(1), eval("(TIME '00:00:02' - TIME '00:00:01') SECONDS"));
        assertEquals(Duration.ofMillis(123), eval("(TIME '00:00:01.123' - TIME '00:00:01') SECONDS"));
    }

    /** Timestamp [with local time zone] interval arithmetic. */
    @ParameterizedTest
    @EnumSource(value = SqlTypeName.class, names = {"TIMESTAMP", "TIMESTAMP_WITH_LOCAL_TIME_ZONE"})
    public void testTimestampIntervalArithmetic(SqlTypeName sqlTypeName) {
        String typeName = sqlTypeName.getSpaceName();

        // Timestamp - timestamp as interval.
        assertEquals(Duration.ofDays(1),
                eval(format("({} '2021-01-02 00:00:00' - {} '2021-01-01 00:00:00') DAYS", typeName, typeName)));
        assertEquals(Duration.ofDays(-1),
                eval(format("({} '2021-01-01 00:00:00' - {} '2021-01-02 00:00:00') DAYS", typeName, typeName)));
        assertEquals(Duration.ofHours(1),
                eval(format("({} '2021-01-01 01:00:00' - {} '2021-01-01 00:00:00') HOURS", typeName, typeName)));
        assertEquals(Duration.ofMinutes(1),
                eval(format("({} '2021-01-01 00:01:00' - {} '2021-01-01 00:00:00') MINUTES", typeName, typeName)));
        assertEquals(Duration.ofSeconds(1),
                eval(format("({} '2021-01-01 00:00:01' - {} '2021-01-01 00:00:00') SECONDS", typeName, typeName)));
        assertEquals(Duration.ofMillis(123),
                eval(format("({} '2021-01-01 00:00:00.123' - {} '2021-01-01 00:00:00') SECONDS", typeName, typeName)));
        assertEquals(Period.ofYears(1),
                eval(format("({} '2022-01-01 00:00:00' - {} '2021-01-01 00:00:00') YEARS", typeName, typeName)));
        assertEquals(Period.ofMonths(1),
                eval(format("({} '2021-02-01 00:00:00' - {} '2021-01-01 00:00:00') MONTHS", typeName, typeName)));
        assertEquals(Period.ofMonths(-1),
                eval(format("({} '2021-01-01 00:00:00' - {} '2021-02-01 00:00:00') MONTHS", typeName, typeName)));
        assertEquals(Period.ofMonths(0),
                eval(format("({} '2021-01-20 00:00:00' - {} '2021-01-01 00:00:00') MONTHS", typeName, typeName)));

        // Check string representation here, since after timestamp calculation we have '2021-11-07T01:30:00.000-0800'
        // but Timestamp.valueOf method converts '2021-11-07 01:30:00' in 'America/Los_Angeles' time zone to
        // '2021-11-07T01:30:00.000-0700' (we pass through '2021-11-07 01:30:00' twice after DST ended).
        ZoneId zoneId = ZoneId.systemDefault();
        String tzSuffix = sqlTypeName == SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE ? ' ' + zoneId.getId() : "";

        assertQuery(format("SELECT ({} '2021-11-06 02:30:00' + interval (23) hours)::varchar", typeName))
                .withTimeZoneId(zoneId)
                .returns("2021-11-07 01:30:00" + tzSuffix).check();

        assertQuery(format("SELECT ({} '2021-11-06 01:30:00' + interval (24) hours)::varchar", typeName))
                .withTimeZoneId(zoneId)
                .returns("2021-11-07 01:30:00" + tzSuffix).check();

        // Timestamp - interval.
        BiConsumer<String, String> timestampChecker = (expression, expected) -> {
            ZoneId timeZoneId = ZoneId.systemDefault();

            Function<String, Object> validator = sqlTypeName == SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE
                    ? ts -> LocalDateTime.parse(ts).atZone(timeZoneId).toInstant()
                    : LocalDateTime::parse;

            assertQuery(format(expression, sqlTypeName.getSpaceName()))
                    .withTimeZoneId(timeZoneId)
                    .returns(validator.apply(expected))
                    .check();
        };

        timestampChecker.accept("SELECT {} '2021-01-01 00:00:00' + INTERVAL '1.123' SECOND", "2021-01-01T00:00:01.123");
        timestampChecker.accept("SELECT {} '2021-01-01 00:00:00.123' + INTERVAL '1.123' SECOND", "2021-01-01T00:00:01.246");
        timestampChecker.accept("SELECT {} '2021-01-01 00:00:00' + INTERVAL '1 1:1:1.123' DAY TO SECOND", "2021-01-02T01:01:01.123");

        // TODO Enable this case after https://issues.apache.org/jira/browse/IGNITE-21557
        if (sqlTypeName != SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE) {
            timestampChecker.accept("SELECT {} '2021-01-01 01:01:01.123' + INTERVAL '1-1' YEAR TO MONTH", "2022-02-01T01:01:01.123");
        }
    }

    @Test
    public void testIntervalArithmetic() {
        // Interval +/- interval.
        assertEquals(Duration.ofSeconds(2), eval("INTERVAL 1 SECONDS + INTERVAL 1 SECONDS"));
        assertEquals(Duration.ofSeconds(1), eval("INTERVAL 2 SECONDS - INTERVAL 1 SECONDS"));
        assertEquals(Duration.ofSeconds(61), eval("INTERVAL 1 MINUTE + INTERVAL 1 SECONDS"));
        assertEquals(Duration.ofSeconds(59), eval("INTERVAL 1 MINUTE - INTERVAL 1 SECONDS"));
        assertEquals(Duration.ofSeconds(59), eval("INTERVAL 1 MINUTE + INTERVAL -1 SECONDS"));
        assertEquals(Duration.ofSeconds(3723), eval("INTERVAL 1 HOUR + INTERVAL '2:3' MINUTE TO SECONDS"));
        assertEquals(Duration.ofSeconds(3477), eval("INTERVAL 1 HOUR - INTERVAL '2:3' MINUTE TO SECONDS"));
        assertEquals(Duration.ofHours(25), eval("INTERVAL 1 DAY + INTERVAL 1 HOUR"));
        assertEquals(Period.ofMonths(2), eval("INTERVAL 1 MONTH + INTERVAL 1 MONTH"));
        assertEquals(Period.ofYears(2), eval("INTERVAL 1 YEAR + INTERVAL 1 YEAR"));
        assertEquals(Period.of(1, 1, 0), eval("INTERVAL 1 YEAR + INTERVAL 1 MONTH"));
        assertEquals(Period.ofMonths(11), eval("INTERVAL 1 YEAR - INTERVAL 1 MONTH"));
        assertEquals(Period.ofMonths(11), eval("INTERVAL 1 YEAR + INTERVAL -1 MONTH"));
        assertThrowsEx("SELECT INTERVAL 1 DAY + INTERVAL 1 MONTH", IgniteException.class, "Cannot apply");

        // Interval * scalar.
        assertEquals(Duration.ofSeconds(2), eval("INTERVAL 1 SECONDS * 2"));
        assertEquals(Duration.ofSeconds(-2), eval("INTERVAL -1 SECONDS * 2"));
        assertEquals(Duration.ofMinutes(4), eval("INTERVAL 2 MINUTES * 2"));
        assertEquals(Duration.ofHours(6), eval("INTERVAL 3 HOURS * 2"));
        assertEquals(Duration.ofDays(8), eval("INTERVAL 4 DAYS * 2"));
        assertEquals(Period.ofMonths(10), eval("INTERVAL 5 MONTHS * 2"));
        assertEquals(Period.ofMonths(-10), eval("INTERVAL -5 MONTHS * 2"));
        assertEquals(Period.ofYears(12), eval("INTERVAL 6 YEARS * 2"));
        assertEquals(Period.of(2, 4, 0), eval("INTERVAL '1-2' YEAR TO MONTH * 2"));
        assertEquals(Duration.ofHours(50), eval("INTERVAL '1 1' DAY TO HOUR * 2"));
        assertEquals(Duration.ofMinutes(124), eval("INTERVAL '1:2' HOUR TO MINUTE * 2"));
        assertEquals(Duration.ofSeconds(126), eval("INTERVAL '1:3' MINUTE TO SECOND * 2"));
        assertEquals(Duration.ofSeconds(7446), eval("INTERVAL '1:2:3' HOUR TO SECOND * 2"));
        assertEquals(Duration.ofMillis(7446912), eval("INTERVAL '0 1:2:3.456' DAY TO SECOND * 2"));

        // Interval / scalar
        assertEquals(Duration.ofSeconds(1), eval("INTERVAL 2 SECONDS / 2"));
        assertEquals(Duration.ofSeconds(-1), eval("INTERVAL -2 SECONDS / 2"));
        assertEquals(Duration.ofSeconds(30), eval("INTERVAL 1 MINUTES / 2"));
        assertEquals(Duration.ofMinutes(90), eval("INTERVAL 3 HOURS / 2"));
        assertEquals(Duration.ofDays(2), eval("INTERVAL 4 DAYS / 2"));
        assertEquals(Period.ofMonths(2), eval("INTERVAL 5 MONTHS / 2"));
        assertEquals(Period.ofMonths(-2), eval("INTERVAL -5 MONTHS / 2"));
        assertEquals(Period.of(3, 6, 0), eval("INTERVAL 7 YEARS / 2"));
        assertEquals(Period.ofMonths(7), eval("INTERVAL '1-2' YEAR TO MONTH / 2"));
        assertEquals(Duration.ofHours(13), eval("INTERVAL '1 2' DAY TO HOUR / 2"));
        assertEquals(Duration.ofMinutes(31), eval("INTERVAL '1:2' HOUR TO MINUTE / 2"));
        assertEquals(Duration.ofSeconds(31), eval("INTERVAL '1:2' MINUTE TO SECOND / 2"));
        assertEquals(Duration.ofSeconds(1862), eval("INTERVAL '1:2:4' HOUR TO SECOND / 2"));
        assertEquals(Duration.ofMillis(1862228), eval("INTERVAL '0 1:2:4.456' DAY TO SECOND / 2"));
    }

    /**
     * Test EXTRACT function with interval data types.
     */
    @Test
    public void testExtract() {
        assertEquals(2L, eval("EXTRACT(MONTH FROM INTERVAL 14 MONTHS)"));
        assertEquals(0L, eval("EXTRACT(MONTH FROM INTERVAL 1 YEAR)"));
        assertEquals(2L, eval("EXTRACT(MONTH FROM INTERVAL '1-2' YEAR TO MONTH)"));
        assertEquals(1L, eval("EXTRACT(YEAR FROM INTERVAL '1-2' YEAR TO MONTH)"));
        assertEquals(-1L, eval("EXTRACT(MONTH FROM INTERVAL -1 MONTHS)"));
        assertEquals(-1L, eval("EXTRACT(YEAR FROM INTERVAL -14 MONTHS)"));
        assertEquals(-2L, eval("EXTRACT(MONTH FROM INTERVAL -14 MONTHS)"));
        assertEquals(-20L, eval("EXTRACT(MINUTE FROM INTERVAL '-10:20' HOURS TO MINUTES)"));
        assertEquals(1L, eval("EXTRACT(DAY FROM INTERVAL '1 2:3:4.567' DAY TO SECOND)"));
        assertEquals(2L, eval("EXTRACT(HOUR FROM INTERVAL '1 2:3:4.567' DAY TO SECOND)"));
        assertEquals(3L, eval("EXTRACT(MINUTE FROM INTERVAL '1 2:3:4.567' DAY TO SECOND)"));
        assertEquals(4L, eval("EXTRACT(SECOND FROM INTERVAL '1 2:3:4.567' DAY TO SECOND)"));
        assertEquals(4567L, eval("EXTRACT(MILLISECOND FROM INTERVAL '1 2:3:4.567' DAY TO SECOND)"));
        assertEquals(-1L, eval("EXTRACT(DAY FROM INTERVAL '-1 2:3:4.567' DAY TO SECOND)"));
        assertEquals(-2L, eval("EXTRACT(HOUR FROM INTERVAL '-1 2:3:4.567' DAY TO SECOND)"));
        assertEquals(-3L, eval("EXTRACT(MINUTE FROM INTERVAL '-1 2:3:4.567' DAY TO SECOND)"));
        assertEquals(-4L, eval("EXTRACT(SECOND FROM INTERVAL '-1 2:3:4.567' DAY TO SECOND)"));
        assertEquals(-4567L, eval("EXTRACT(MILLISECOND FROM INTERVAL '-1 2:3:4.567' DAY TO SECOND)"));

        assertThrowsEx("SELECT EXTRACT(DAY FROM INTERVAL 1 MONTH)", IgniteException.class, "Cannot apply");
        assertThrowsEx("SELECT EXTRACT(MONTH FROM INTERVAL 1 DAY)", IgniteException.class, "Cannot apply");
    }

    /**
     * Test caching of expressions by digest.
     */
    @Test
    public void testScalarCache() {
        // These expressions differs only in return data type, so digest should include also data type correctly
        // compile scalar for second expression (should not get compiled scalar from the cache).
        assertEquals(Duration.ofDays(1), eval("(DATE '2021-01-02' - DATE '2021-01-01') DAYS"));
        assertEquals(Period.ofMonths(0), eval("(DATE '2021-01-02' - DATE '2021-01-01') MONTHS"));
    }

    public Object eval(String exp) {
        return sql("SELECT " + exp).get(0).get(0);
    }

    private void assertThrowsEx(String sql, Class<? extends Exception> cls, String errMsg) {
        Exception ex = assertThrows(cls, () -> sql(sql));

        assertTrue(ex.getMessage().toLowerCase().contains(errMsg.toLowerCase()));
    }

    private static List<DateTimeIntervalBasicTestCase> dateTimeIntervalTestCases() {
        SqlTypeName[] types = {
                SqlTypeName.TIME,
                SqlTypeName.DATE,
                SqlTypeName.TIMESTAMP,
                SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE
        };

        Map<ChronoUnit, int[]> timeUnitData = new LinkedHashMap<>();

        timeUnitData.put(ChronoUnit.SECONDS, new int[]{1, 59, 60, 61, 100, 1_000, 10_000});
        timeUnitData.put(ChronoUnit.MINUTES, new int[]{1, 59, 60, 61, 100, 1_000, 10_000});
        timeUnitData.put(ChronoUnit.HOURS, new int[]{1, 23, 24, 25, 48, 96, 1_000, 10_000});
        timeUnitData.put(ChronoUnit.DAYS, new int[]{1, 29, 30, 31, 100, 1_000, 10_000});
        timeUnitData.put(ChronoUnit.MONTHS, new int[]{1, 11, 12, 13, 100, 1_000});
        timeUnitData.put(ChronoUnit.YEARS, new int[]{1, 10, 100, 1_000});

        List<DateTimeIntervalBasicTestCase> testCases = new ArrayList<>();

        for (SqlTypeName typeName : types) {
            for (Entry<ChronoUnit, int[]> entry : timeUnitData.entrySet()) {
                ChronoUnit unit = entry.getKey();
                // TODO Remove after https://issues.apache.org/jira/browse/IGNITE-21557
                if (typeName == SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE
                        && (unit == ChronoUnit.MONTHS || unit == ChronoUnit.YEARS)) {
                    continue;
                }

                // TODO Remove after https://issues.apache.org/jira/browse/IGNITE-21589
                if (typeName == SqlTypeName.TIME && (unit == ChronoUnit.HOURS || unit == ChronoUnit.DAYS)) {
                    continue;
                }

                for (int amount : entry.getValue()) {
                    testCases.add(DateTimeIntervalBasicTestCase.newTestCase(typeName, unit, amount));
                    testCases.add(DateTimeIntervalBasicTestCase.newTestCase(typeName, unit, -amount));
                }
            }
        }

        return testCases;
    }

    abstract static class DateTimeIntervalBasicTestCase {
        private static final ZoneId TIME_ZONE_ID = ZoneId.of("Asia/Nicosia");
        private static final DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");
        private static final String dateString = "1992-01-19 00:00:00.123";
        private static final LocalDateTime testLocalDate = LocalDateTime.parse(dateString, dateTimeFormatter);

        final SqlTypeName type;
        final ChronoUnit unit;
        final int amount;

        private String query;

        private DateTimeIntervalBasicTestCase(SqlTypeName type, ChronoUnit unit, int amount) {
            this.type = type;
            this.unit = unit;
            this.amount = amount;
        }

        public abstract Temporal expected();

        public String query() {
            if (query == null) {
                String intervalSubstring = (amount > 0 ? '+' : '-') + " interval (" + Math.abs(amount) + ") " + unit;
                query = format("SELECT {} '{}'{}", type.getSpaceName(), sqlDateLiteral(), intervalSubstring);
            }

            return query;
        }

        String sqlDateLiteral() {
            return dateString;
        }

        static DateTimeIntervalBasicTestCase newTestCase(SqlTypeName type, ChronoUnit unit, Integer amount) {
            switch (type) {
                case TIMESTAMP:
                    return new SqlTimestampIntervalIntervalTestCase(unit, amount);

                case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                    return new SqlTimestampTzIntervalIntervalTestCase(unit, amount);

                case DATE:
                    return new SqlDateIntervalIntervalTestCase(unit, amount);

                case TIME:
                    return new SqlTimeIntervalIntervalTestCase(unit, amount);

                default:
                    throw new UnsupportedOperationException("Not implemented: " + type);
            }
        }

        @Override
        public String toString() {
            return query();
        }

        private static class SqlTimestampIntervalIntervalTestCase extends DateTimeIntervalBasicTestCase {
            private SqlTimestampIntervalIntervalTestCase(ChronoUnit unit, Integer amount) {
                super(SqlTypeName.TIMESTAMP, unit, amount);
            }

            @Override
            public LocalDateTime expected() {
                return testLocalDate.plus(amount, unit);
            }
        }

        private static class SqlTimestampTzIntervalIntervalTestCase extends DateTimeIntervalBasicTestCase {
            private SqlTimestampTzIntervalIntervalTestCase(ChronoUnit unit, Integer amount) {
                super(SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE, unit, amount);
            }

            @Override
            public Temporal expected() {
                return testLocalDate.atZone(TIME_ZONE_ID).toInstant().plus(amount, unit);
            }
        }

        private static class SqlDateIntervalIntervalTestCase extends DateTimeIntervalBasicTestCase {
            final LocalDateTime testLocalDateEndOfDay;

            SqlDateIntervalIntervalTestCase(ChronoUnit unit, Integer amount) {
                super(SqlTypeName.DATE, unit, amount);

                // DateTime + 23:59:59.999
                testLocalDateEndOfDay = testLocalDate
                        .plus(1, ChronoUnit.DAYS)
                        .minus(1 + testLocalDate.get(ChronoField.MILLI_OF_SECOND), ChronoUnit.MILLIS);
            }

            @Override
            String sqlDateLiteral() {
                return dateString.substring(0, 10);
            }

            @Override
            public Temporal expected() {
                return (amount < 0 ? testLocalDateEndOfDay : testLocalDate).plus(amount, unit).toLocalDate();
            }
        }

        private static class SqlTimeIntervalIntervalTestCase extends DateTimeIntervalBasicTestCase {
            private final LocalTime initTime;

            private SqlTimeIntervalIntervalTestCase(ChronoUnit unit, Integer amount) {
                super(SqlTypeName.TIME, unit, amount);

                initTime = testLocalDate.toLocalTime();
            }

            @Override
            String sqlDateLiteral() {
                return dateString.substring(11);
            }

            @Override
            public LocalTime expected() {
                if (initTime.isSupported(unit)) {
                    return initTime.plus(amount, unit);
                }

                return initTime;
            }
        }
    }
}
