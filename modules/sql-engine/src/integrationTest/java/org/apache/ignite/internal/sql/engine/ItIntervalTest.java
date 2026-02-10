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
import static org.apache.ignite.internal.sql.engine.ItIntervalTest.Parser.dateTime;
import static org.apache.ignite.internal.sql.engine.ItIntervalTest.Parser.instant;
import static org.apache.ignite.internal.sql.engine.ItIntervalTest.Parser.time;
import static org.apache.ignite.internal.sql.engine.util.SqlTestUtils.assertThrowsSqlException;
import static org.apache.ignite.internal.sql.engine.util.SqlTestUtils.toSqlType;
import static org.apache.ignite.sql.ColumnType.DATETIME;
import static org.apache.ignite.sql.ColumnType.TIME;
import static org.apache.ignite.sql.ColumnType.TIMESTAMP;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.Period;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoField;
import java.time.temporal.ChronoUnit;
import java.time.temporal.Temporal;
import java.time.temporal.TemporalAmount;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Stream;
import org.apache.calcite.avatica.util.TimeUnit;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlIntervalLiteral;
import org.apache.calcite.sql.SqlIntervalLiteral.IntervalValue;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.parser.SqlParserUtil;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.internal.sql.BaseSqlIntegrationTest;
import org.apache.ignite.internal.sql.engine.prepare.IgniteSqlValidator;
import org.apache.ignite.internal.sql.engine.sql.IgniteSqlParser;
import org.apache.ignite.internal.sql.engine.util.Commons;
import org.apache.ignite.internal.sql.engine.util.MetadataMatcher;
import org.apache.ignite.internal.sql.engine.util.SqlTestUtils;
import org.apache.ignite.lang.ErrorGroups.Sql;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.sql.ColumnType;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;

/** Interval coverage tests. */
public class ItIntervalTest extends BaseSqlIntegrationTest {

    private static final Set<SqlTypeName> DATETIME_TYPES = Set.of(
            SqlTypeName.TIME,
            SqlTypeName.DATE,
            SqlTypeName.TIMESTAMP,
            SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE
    );

    private static final Map<SqlTypeName, Set<SqlTypeName>> DATETIME_INTERVALS = Map.of(
            SqlTypeName.TIME, Set.of(
                    SqlTypeName.INTERVAL_DAY,
                    SqlTypeName.INTERVAL_DAY_HOUR,
                    SqlTypeName.INTERVAL_DAY_MINUTE,
                    SqlTypeName.INTERVAL_DAY_SECOND,
                    SqlTypeName.INTERVAL_HOUR,
                    SqlTypeName.INTERVAL_HOUR_MINUTE,
                    SqlTypeName.INTERVAL_HOUR_SECOND,
                    SqlTypeName.INTERVAL_MINUTE,
                    SqlTypeName.INTERVAL_MINUTE_SECOND,
                    SqlTypeName.INTERVAL_SECOND
            ),
            SqlTypeName.DATE, SqlTypeName.INTERVAL_TYPES,
            SqlTypeName.TIMESTAMP, SqlTypeName.INTERVAL_TYPES,
            SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE, SqlTypeName.INTERVAL_TYPES
    );

    @Override
    protected int initialNodes() {
        return 1;
    }

    @BeforeAll
    public void createDateTimeColumnsTable() {
        sql("CREATE TABLE datetime_cols (id INT PRIMARY KEY, "
                + "date0_col DATE, "

                + "time0_col TIME(0), "
                + "time1_col TIME(1), "
                + "time2_col TIME(2), "
                + "time3_col TIME(3), "
                + "time_nullable TIME(0) NULL, "
                + "time_not_nullable TIME(0) NOT NULL, "

                + "timestamp0_col TIMESTAMP(0), "
                + "timestamp1_col TIMESTAMP(1), "
                + "timestamp2_col TIMESTAMP(2), "
                + "timestamp3_col TIMESTAMP(3), "
                + "timestamp_nullable TIMESTAMP(0) NULL, "
                + "timestamp_not_nullable TIMESTAMP(0) NOT NULL, "

                + "timestamp_with_local_time_zone0_col TIMESTAMP(0) WITH LOCAL TIME ZONE, "
                + "timestamp_with_local_time_zone1_col TIMESTAMP(1) WITH LOCAL TIME ZONE, "
                + "timestamp_with_local_time_zone2_col TIMESTAMP(2) WITH LOCAL TIME ZONE, "
                + "timestamp_with_local_time_zone3_col TIMESTAMP(3) WITH LOCAL TIME ZONE, "
                + "timestamp_with_local_time_zone_nullable TIMESTAMP(0) WITH LOCAL TIME ZONE NULL, "
                + "timestamp_with_local_time_zone_not_nullable TIMESTAMP(0) WITH LOCAL TIME ZONE NOT NULL "
                + ")");
        sql("INSERT INTO datetime_cols (id, time_not_nullable, timestamp_not_nullable, timestamp_with_local_time_zone_not_nullable)"
                + " VALUES (1, TIME '00:00:00', TIMESTAMP '1970-01-01 00:00:00', TIMESTAMP WITH LOCAL TIME ZONE '1970-01-01 00:00:00')");
    }

    /**
     * Test returned result for interval data types.
     */
    @Test
    public void testIntervalResult() {
        assertEquals(Duration.ofDays(4), eval("INTERVAL '4' DAYS"));
        assertEquals(Duration.ofSeconds(1), eval("INTERVAL '1' SECONDS"));
        assertEquals(Duration.ofSeconds(1), eval("INTERVAL - '-1' SECONDS"));
        assertEquals(Duration.ofSeconds(-1), eval("INTERVAL - '1' SECONDS"));
        assertEquals(Duration.ofSeconds(-1), eval("INTERVAL '-1' SECONDS"));
        assertEquals(Duration.ofSeconds(123), eval("INTERVAL '123' SECONDS"));
        assertEquals(Duration.ofSeconds(123), eval("INTERVAL '123' SECONDS(3)"));
        assertEquals(Duration.ofMinutes(2), eval("INTERVAL '2' MINUTES"));
        assertEquals(Duration.ofHours(3), eval("INTERVAL '3' HOURS"));
        assertEquals(Duration.ofDays(4), eval("INTERVAL '4' DAYS"));
        assertEquals(Period.ofMonths(5), eval("INTERVAL '5' MONTHS"));
        assertEquals(Period.ofMonths(5), eval("INTERVAL - '-5' MONTHS"));
        assertEquals(Period.ofMonths(-5), eval("INTERVAL - '5' MONTHS"));
        assertEquals(Period.ofMonths(-5), eval("INTERVAL '-5' MONTHS"));
        assertEquals(Period.ofYears(6), eval("INTERVAL '6' YEARS"));
        assertEquals(Period.of(1, 2, 0), eval("INTERVAL '1-2' YEAR TO MONTH"));
        assertEquals(Duration.ofHours(25), eval("INTERVAL '1 1' DAY TO HOUR"));
        assertEquals(Duration.ofMinutes(62), eval("INTERVAL '1:2' HOUR TO MINUTE"));
        assertEquals(Duration.ofSeconds(63), eval("INTERVAL '1:3' MINUTE TO SECOND"));
        assertEquals(Duration.ofSeconds(3723), eval("INTERVAL '1:2:3' HOUR TO SECOND"));
        assertEquals(Duration.ofMillis(3723456), eval("INTERVAL '0 1:2:3.456' DAY TO SECOND"));

        assertEquals(Duration.ofSeconds(123), eval("INTERVAL '123' SECONDS"));
        assertEquals(Duration.ofMillis(123987), eval("INTERVAL '123.987' SECONDS"));
        // TODO: uncomment after IGNITE-19162
        // assertEquals(Duration.ofMillis(123987654), eval("INTERVAL '123.987654' SECONDS"));

        // Interval range overflow
        assertThrowsSqlException(Sql.RUNTIME_ERR, "INTEGER out of range",
                () -> sql("SELECT INTERVAL '5000000' MONTHS * 1000"));
        assertThrowsSqlException(Sql.STMT_VALIDATION_ERR, "Failed to validate query.",
                () -> sql("SELECT DATE '2021-01-01' + INTERVAL '999999999999' DAY"));
        assertThrowsSqlException(Sql.RUNTIME_ERR, "DATE out of range",
                () -> sql("SELECT DATE '2021-01-01' + INTERVAL - '999999999' YEAR"));

        // Invalid interval literals format
        assertThrowsSqlException(Sql.STMT_VALIDATION_ERR, "String literal expected", () -> sql("SELECT INTERVAL 1 HOUR"));
        assertThrowsSqlException(Sql.STMT_VALIDATION_ERR, "String literal expected", () -> sql("SELECT INTERVAL 1 YEAR"));
        assertThrowsSqlException(Sql.STMT_VALIDATION_ERR, "String literal expected", () -> sql("SELECT INTERVAL -1 HOUR"));
        assertThrowsSqlException(Sql.STMT_VALIDATION_ERR, "String literal expected", () -> sql("SELECT INTERVAL -1 YEAR"));
        assertThrowsSqlException(Sql.STMT_VALIDATION_ERR, "String literal expected", () -> sql("SELECT INTERVAL 5000000 MONTHS * 1000"));
        assertThrowsSqlException(Sql.STMT_VALIDATION_ERR, "String literal expected",
                () -> sql("SELECT DATE '2021-01-01' + INTERVAL 999999999999 DAY"));
        assertThrowsSqlException(Sql.STMT_VALIDATION_ERR, "String literal expected",
                () -> sql("SELECT DATE '2021-01-01' + INTERVAL -999999999 YEAR"));
    }

    /**
     * Test cast interval types to integer and integer to interval.
     */
    @Test
    public void testIntervalIntCast() {
        assertNull(eval("CAST(NULL::INTERVAL SECONDS AS INT)"));
        assertNull(eval("CAST(NULL::INTERVAL MONTHS AS INT)"));
        assertEquals(1, eval("CAST(INTERVAL '1' SECONDS AS INT)"));
        assertEquals(2, eval("CAST(INTERVAL '2' MINUTES AS INT)"));
        assertEquals(3, eval("CAST(INTERVAL '3' HOURS AS INT)"));
        assertEquals(4, eval("CAST(INTERVAL '4' DAYS AS INT)"));
        assertEquals(-4, eval("CAST(INTERVAL '-4' DAYS AS INT)"));
        assertEquals(5, eval("CAST(INTERVAL '5' MONTHS AS INT)"));
        assertEquals(6, eval("CAST(INTERVAL '6' YEARS AS INT)"));
        assertEquals(-6, eval("CAST(INTERVAL - '6' YEARS AS INT)"));

        assertEquals("+6", eval("CAST(INTERVAL '6' YEARS AS VARCHAR)"));
        assertEquals("+1", eval("CAST(INTERVAL '1' HOUR AS VARCHAR)"));
        assertEquals("+7.000000", eval("CAST(INTERVAL '7' SECONDS AS VARCHAR)"));

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
        assertEquals("+1.000000", eval("CAST(INTERVAL '1' SECONDS AS VARCHAR)"));
        assertEquals("+2", eval("CAST(INTERVAL '2' MINUTES AS VARCHAR)"));
        assertEquals("+3", eval("CAST(INTERVAL '3' HOURS AS VARCHAR)"));
        assertEquals("+4", eval("CAST(INTERVAL '4' DAYS AS VARCHAR)"));
        assertEquals("+5", eval("CAST(INTERVAL '5' MONTHS AS VARCHAR)"));
        assertEquals("+6", eval("CAST(INTERVAL '6' YEARS AS VARCHAR)"));
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
        assertEquals(Duration.ofMinutes(1), eval("CAST(INTERVAL '60' SECONDS AS INTERVAL MINUTE)"));
        assertEquals(Duration.ofHours(1), eval("CAST(INTERVAL '60' MINUTES AS INTERVAL HOUR)"));
        assertEquals(Duration.ofDays(1), eval("CAST(INTERVAL '24' HOURS AS INTERVAL DAY)"));
        assertEquals(Period.ofYears(1), eval("CAST(INTERVAL '1' YEAR AS INTERVAL MONTHS)"));
        assertEquals(Period.ofYears(1), eval("CAST(INTERVAL '12' MONTHS AS INTERVAL YEARS)"));

        // Cannot convert between month-year and day-time interval types.
        assertThrowsEx("SELECT CAST(INTERVAL '1' MONTHS AS INTERVAL DAYS)", IgniteException.class, "cannot convert");
        assertThrowsEx("SELECT CAST(INTERVAL '1' DAYS AS INTERVAL MONTHS)", IgniteException.class, "cannot convert");
    }

    /**
     * Test DML statements with interval data type.
     */
    @Disabled("https://issues.apache.org/jira/browse/IGNITE-16637")
    @Test
    public void testDml() {
        sql("CREATE TABLE test(id int PRIMARY KEY, ym INTERVAL YEAR, dt INTERVAL DAYS)");
        sql("INSERT INTO test VALUES (1, INTERVAL '1' MONTH, INTERVAL 2 DAYS)");
        sql("INSERT INTO test VALUES (2, INTERVAL '3' YEARS, INTERVAL 4 HOURS)");
        sql("INSERT INTO test VALUES (3, INTERVAL '4-5' YEARS TO MONTHS, INTERVAL '6:7' HOURS TO MINUTES)");
        sql("INSERT INTO test VALUES (4, NULL, NULL)");

        assertThrowsEx("INSERT INTO test VALUES (5, INTERVAL '1' DAYS, INTERVAL '1' HOURS)", IgniteInternalException.class,
                "cannot assign");

        assertThrowsEx("INSERT INTO test VALUES (6, INTERVAL '1' YEARS, INTERVAL '1' MONTHS)", IgniteInternalException.class,
                "cannot assign");

        assertQuery("SELECT ym, dt FROM test")
                .returns(Period.ofMonths(1), Duration.ofDays(2))
                .returns(Period.ofYears(3), Duration.ofHours(4))
                .returns(Period.of(4, 5, 0), Duration.ofMinutes(367))
                .returns(null, null)
                .check();

        assertThrowsEx("SELECT * FROM test WHERE ym = INTERVAL '6' DAYS", IgniteInternalException.class, "Cannot apply");
        assertThrowsEx("SELECT * FROM test WHERE dt = INTERVAL '6' YEARS", IgniteInternalException.class, "Cannot apply");

        sql("UPDATE test SET dt = INTERVAL '3' DAYS WHERE ym = INTERVAL '1' MONTH");
        sql("UPDATE test SET ym = INTERVAL '5' YEARS WHERE dt = INTERVAL '4' HOURS");
        sql("UPDATE test SET ym = INTERVAL '6-7' YEARS TO MONTHS, dt = INTERVAL '8 9' DAYS TO HOURS "
                + "WHERE ym = INTERVAL '4-5' YEARS TO MONTHS AND dt = INTERVAL '6:7' HOURS TO MINUTES");

        assertThrowsEx("UPDATE test SET dt = INTERVAL '5' YEARS WHERE ym = INTERVAL '1' MONTH", IgniteInternalException.class,
                "Cannot assign");

        assertThrowsEx("UPDATE test SET ym = INTERVAL '8' YEARS WHERE dt = INTERVAL '1' MONTH", IgniteInternalException.class,
                "Cannot apply");

        assertQuery("SELECT * FROM test")
                .returns(Period.ofMonths(1), Duration.ofDays(3))
                .returns(Period.ofYears(5), Duration.ofHours(4))
                .returns(Period.of(6, 7, 0), Duration.ofHours(201))
                .returns(null, null)
                .check();

        assertThrowsEx("DELETE FROM test WHERE ym = INTERVAL 6 DAYS", IgniteInternalException.class, "cannot apply");
        assertThrowsEx("DELETE FROM test WHERE dt = INTERVAL 6 YEARS", IgniteInternalException.class, "cannot apply");

        sql("DELETE FROM test WHERE ym = INTERVAL '1' MONTH");
        sql("DELETE FROM test WHERE dt = INTERVAL '4' HOURS");
        sql("DELETE FROM test WHERE ym = INTERVAL '6-7' YEARS TO MONTHS AND dt = INTERVAL '8 9' DAYS TO HOURS");
        sql("DELETE FROM test WHERE ym IS NULL AND dt IS NULL");

        assertEquals(0, sql("SELECT * FROM test").size());

        sql("ALTER TABLE test ADD (ym2 INTERVAL MONTH, dt2 INTERVAL HOURS)");

        sql("INSERT INTO test(id, ym, ym2, dt, dt2) VALUES (7, INTERVAL '1' YEAR, INTERVAL 2 YEARS, "
                + "INTERVAL '1' SECOND, INTERVAL 2 MINUTES)");

        assertQuery("SELECT ym, ym2, dt, dt2 FROM test")
                .returns(Period.ofYears(1), Period.ofYears(2), Duration.ofSeconds(1), Duration.ofMinutes(2))
                .check();
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("dateTimeIntervalTestCases")
    public void testDateTimeLiteralIntervalArithmetic(DateTimeIntervalBasicTestCase testCase) {
        Interval intervalVal = testCase.intervalVal;
        SqlTypeName typeName = testCase.type.getSqlTypeName();

        String query;
        if (intervalVal.sign() > 0) {
            query = format("SELECT {} '{}' + {}", typeName.getSpaceName(), testCase.sqlDateLiteral(), intervalVal.toLiteral());
        } else {
            query = format("SELECT {} '{}' - {}", typeName.getSpaceName(), testCase.sqlDateLiteral(), intervalVal.toLiteral());
        }

        assertQuery(query)
                .withTimeZoneId(DateTimeIntervalBasicTestCase.TIME_ZONE_ID)
                .returns(testCase.expected())
                .check();
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("dateTimeIntervalTestCases")
    public void testDateTimeDynamicParamIntervalArithmetic(DateTimeIntervalBasicTestCase testCase) {
        Interval intervalVal = testCase.intervalVal;

        String query;
        if (intervalVal.sign() > 0) {
            query = format("SELECT ? + {}", intervalVal.toLiteral());
        } else {
            query = format("SELECT ? - {}", intervalVal.toLiteral());
        }

        assertQuery(query)
                .withParams(testCase.dateTimeValue())
                .withTimeZoneId(DateTimeIntervalBasicTestCase.TIME_ZONE_ID)
                .returns(testCase.expected())
                .check();
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("colDateTimeIntervalTestCases")
    public void testDateTimeColumnIntervalArithmetic(DateTimeIntervalBasicTestCase testCase) {
        Interval intervalVal = testCase.intervalVal;
        SqlTypeName typeName = testCase.type.getSqlTypeName();

        String colName = typeName.getName() + testCase.type.getPrecision() + "_col";

        String dml = format("UPDATE datetime_cols SET {} = {} '{}' WHERE id = 1",
                colName,
                typeName.getSpaceName(),
                testCase.sqlDateLiteral()
        );
        assertQuery(dml)
                .withTimeZoneId(DateTimeIntervalBasicTestCase.TIME_ZONE_ID)
                .returnSomething()
                .check();

        String query;
        if (intervalVal.sign() > 0) {
            query = format("SELECT {} + {} FROM datetime_cols", colName, intervalVal.toLiteral());
        } else {
            query = format("SELECT {} - {} FROM datetime_cols", colName, intervalVal.toLiteral());
        }

        assertQuery(query)
                .withTimeZoneId(DateTimeIntervalBasicTestCase.TIME_ZONE_ID)
                .returns(testCase.expected())
                .check();
    }

    private static List<DateTimeIntervalBasicTestCase> colDateTimeIntervalTestCases() {
        List<DateTimeIntervalBasicTestCase> testCases = new ArrayList<>();

        BiConsumer<SqlTypeName, Integer> addVariants = (typeName, precision) -> {
            Map<SqlTypeName, List<Interval>> intervalValues = createIntervalValues(typeName);

            for (Map.Entry<SqlTypeName, List<Interval>> entry : intervalValues.entrySet()) {
                for (Interval value : entry.getValue()) {
                    if (precision != null) {
                        testCases.add(DateTimeIntervalBasicTestCase.newTestCase(typeName, precision, value));
                        testCases.add(DateTimeIntervalBasicTestCase.newTestCase(typeName, precision, value.negated()));
                    } else {
                        testCases.add(DateTimeIntervalBasicTestCase.newTestCase(typeName, value));
                        testCases.add(DateTimeIntervalBasicTestCase.newTestCase(typeName, value.negated()));
                    }
                }
            }
        };

        for (SqlTypeName typeName : DATETIME_TYPES) {
            if (!typeName.allowsPrec()) {
                addVariants.accept(typeName, null);
            } else {
                for (int p : new int[]{0, 1, 2, 3}) {
                    addVariants.accept(typeName, p);
                }
            }
        }

        return testCases;
    }

    @Test
    public void testRejectNotAllowedIntervalPlusMinusOperations() {
        // TIME + INTERVAL / INTERVAL + TIME
        BiConsumer<String, String> timePlus = (interval, type) -> {
            assertThrowsSqlException(Sql.STMT_VALIDATION_ERR,
                    "Cannot apply '+' to arguments of type '<TIME(0)> + " + type + "'",
                    () -> sql("SELECT TIME '00:00:00' + " + interval));

            assertThrowsSqlException(Sql.STMT_VALIDATION_ERR,
                    "Cannot apply '+' to arguments of type '" + type + " + <TIME(0)>'",
                    () -> sql("SELECT " + interval + " + TIME '00:00:00'"));
        };

        timePlus.accept("INTERVAL '1' YEAR", "<INTERVAL YEAR>");
        timePlus.accept("INTERVAL '1' MONTH", "<INTERVAL MONTH>");
        timePlus.accept("INTERVAL '1-1' YEAR TO MONTH", "<INTERVAL YEAR TO MONTH>");

        // TIME - INTERVAL
        BiConsumer<String, String> timeMinus = (interval, type) -> assertThrowsSqlException(Sql.STMT_VALIDATION_ERR,
                "Cannot apply '-' to arguments of type '<TIME(0)> - " + type + "'",
                () -> sql("SELECT TIME '00:00:00' - " + interval));

        timeMinus.accept("INTERVAL '1' YEAR", "<INTERVAL YEAR>");
        timeMinus.accept("INTERVAL '1' MONTH", "<INTERVAL MONTH>");
        timeMinus.accept("INTERVAL '1-1' YEAR TO MONTH", "<INTERVAL YEAR TO MONTH>");
    }

    /**
     * Test date and time interval arithmetic.
     */
    @Test
    public void testDateTimeIntervalArithmetic() {
        // Date +/- interval.
        assertEquals(LocalDate.parse("2020-12-31"), eval("DATE '2021-01-01' + INTERVAL '-1' DAY"));
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

        assertQuery(format("SELECT ({} '2021-11-06 02:30:00' + interval '23' hours)::varchar", typeName))
                .withTimeZoneId(zoneId)
                .returns("2021-11-07 01:30:00.000000" + tzSuffix).check();

        assertQuery(format("SELECT ({} '2021-11-06 01:30:00' + interval '24' hours)::varchar", typeName))
                .withTimeZoneId(zoneId)
                .returns("2021-11-07 01:30:00.000000" + tzSuffix).check();

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
        timestampChecker.accept("SELECT {} '2021-01-01 01:01:01.123' + INTERVAL '1-1' YEAR TO MONTH", "2022-02-01T01:01:01.123");
    }

    @ParameterizedTest
    @MethodSource("testDatetimePlusMinusIntervalWithFractionOfSecondLiteralArgs")
    public void testDatetimePlusMinusIntervalWithFractionOfSecond(String expr, Temporal expected, ColumnType expType, int expPrecision) {
        SqlTypeName sqlType = SqlTestUtils.columnType2SqlTypeName(expType);

        assertQuery("SELECT " + expr)
                .withTimeZoneId(ZoneOffset.UTC)
                .columnMetadata(new MetadataMatcher().type(expType).precision(expPrecision))
                .returns(expected)
                .check();

        Consumer<Integer> updateValidator = columnPrecision -> {
            String columnPrefix = sqlType.getName();

            assertQuery(format("UPDATE datetime_cols SET {}{}_col={}", columnPrefix, columnPrecision, expr))
                    .withTimeZoneId(ZoneOffset.UTC)
                    .check();

            assertQuery(format("SELECT {}{}_col FROM datetime_cols", sqlType.getName(), columnPrecision))
                    .withTimeZoneId(ZoneOffset.UTC)
                    .columnMetadata(new MetadataMatcher().type(expType).precision(columnPrecision))
                    .returns(SqlTestUtils.adjustTemporalPrecision(expType, expected, Math.min(expPrecision, columnPrecision)))
                    .check();
        };

        // UPDATE column with precision 0-3.
        updateValidator.accept(0);
        updateValidator.accept(1);
        updateValidator.accept(2);
        updateValidator.accept(3);
    }

    private static Stream<Arguments> testDatetimePlusMinusIntervalWithFractionOfSecondLiteralArgs() {
        return Stream.concat(
                testDatetimePlusIntervalWithFractionOfSecondArgs()
                        .flatMap(arg -> {
                            Object[] args = arg.get();
                            String literal = format("{} '{}'{}", toSqlType((ColumnType) args[4]), args[0], args[1]);

                            return Stream.of(
                                    // datetime + interval
                                    Arguments.of(literal + " + " + args[2], args[3], args[4], args[5]),
                                    // interval + datetime
                                    Arguments.of(args[2] + " + " + literal, args[3], args[4], args[5]));
                        }),
                testDatetimeMinusIntervalWithFrartionOfSecondArgs()
                        .map(arg -> {
                            Object[] args = arg.get();
                            String expression = format("{} '{}'{} - {}", toSqlType((ColumnType) args[4]), args[0], args[1], args[2]);

                            // datetime - interval
                            return Arguments.of(expression, args[3], args[4], args[5]);
                        })
        );
    }

    @ParameterizedTest
    @MethodSource("testDatetimePlusMinusIntervalWithFractionOfSecondDynParamArgs")
    public void testDatetimePlusMinusIntervalWithFractionOfSecondDynParam(String expr, Temporal param, Temporal expected,
            ColumnType expType, int expPrecision) {
        SqlTypeName sqlType = SqlTestUtils.columnType2SqlTypeName(expType);

        assertQuery("SELECT " + expr)
                .withParam(param)
                .withTimeZoneId(ZoneOffset.UTC)
                .columnMetadata(new MetadataMatcher().type(expType).precision(expPrecision))
                .returns(expected)
                .check();

        Consumer<Integer> updateValidator = columnPrecision -> {
            String columnPrefix = sqlType.getName();

            assertQuery(format("UPDATE datetime_cols SET {}{}_col={}", columnPrefix, columnPrecision, expr))
                    .withParam(param)
                    .withTimeZoneId(ZoneOffset.UTC)
                    .check();

            assertQuery(format("SELECT {}{}_col FROM datetime_cols", sqlType.getName(), columnPrecision))
                    .withTimeZoneId(ZoneOffset.UTC)
                    .columnMetadata(new MetadataMatcher().type(expType).precision(columnPrecision))
                    .returns(SqlTestUtils.adjustTemporalPrecision(expType, expected, Math.min(expPrecision, columnPrecision)))
                    .check();
        };

        // UPDATE column with precision 0-3.
        updateValidator.accept(0);
        updateValidator.accept(1);
        updateValidator.accept(2);
        updateValidator.accept(3);
    }

    private static Stream<Arguments> testDatetimePlusMinusIntervalWithFractionOfSecondDynParamArgs() {
        BiFunction<String, ColumnType, Temporal> parser = (value, type) -> {
            switch (type) {
                case TIME:
                    return time(value);

                case DATETIME:
                    return dateTime(value);

                case TIMESTAMP:
                    return instant(value);

                default:
                    throw new IllegalArgumentException("Unexpected type " + type);
            }
        };

        return Stream.concat(
                testDatetimePlusIntervalWithFractionOfSecondArgs()
                        .flatMap(arg -> {
                            Object[] args = arg.get();
                            Temporal input = parser.apply((String) args[0], (ColumnType) args[4]);
                            String paramExpr = format("?{}", args[1]);

                            // The default precision of a dynamic parameter is different from a literal,
                            // we need to override it if there was no explicit type cast.
                            if (((String) args[1]).isEmpty()) {
                                args[5] = IgniteSqlValidator.TEMPORAL_DYNAMIC_PARAM_PRECISION;
                            }

                            return Stream.of(
                                    // datetime + interval
                                    Arguments.of(paramExpr + " + " + args[2], input, args[3], args[4], args[5]),
                                    // interval + datetime
                                    Arguments.of(args[2] + " + " + paramExpr, input, args[3], args[4], args[5]));
                        }),
                testDatetimeMinusIntervalWithFrartionOfSecondArgs()
                        .map(arg -> {
                            Object[] args = arg.get();
                            Temporal input = parser.apply((String) args[0], (ColumnType) args[4]);
                            String expression = format("?{} - {}", args[1], args[2]);

                            // The default precision of a dynamic parameter is different from a literal,
                            // we need to override it if there was no explicit type cast.
                            if (((String) args[1]).isEmpty()) {
                                args[5] = IgniteSqlValidator.TEMPORAL_DYNAMIC_PARAM_PRECISION;
                            }

                            // datetime - interval
                            return Arguments.of(expression, input, args[3], args[4], args[5]);
                        })
        );
    }

    private static Stream<Arguments> testDatetimePlusIntervalWithFractionOfSecondArgs() {
        // DATETIME + INTERVAL
        return Stream.of(
                // SQL 2016 10.1 syntax rule 6 "interval fractional seconds precision" should be 6 by default
                Arguments.of("00:00:00", "", "INTERVAL '1' SECOND", time("00:00:01"), TIME, 6),
                Arguments.of("00:00:00", "", "INTERVAL '1' SECOND(1, 0)", time("00:00:01"), TIME, 0),
                Arguments.of("00:00:00", "", "INTERVAL '1:1' MINUTE TO SECOND", time("00:01:01"), TIME, 6),
                Arguments.of("00:00:00", "::TIME(9)", "INTERVAL '1:1' MINUTE TO SECOND", time("00:01:01"), TIME, 9),

                // INTERVAL precision greater than the TIME precision.
                Arguments.of("00:00:00", "", "INTERVAL '0.001' SECOND", time("00:00:00.001"), TIME, 6),
                Arguments.of("00:00:00.1", "", "INTERVAL '0.1' SECOND(1, 1)", time("00:00:00.2"), TIME, 1),
                Arguments.of("00:00:00.1", "", "INTERVAL '0.01' SECOND(1, 2)", time("00:00:00.11"), TIME, 2),
                Arguments.of("00:00:00.1", "", "INTERVAL '0.001' SECOND(1, 3)", time("00:00:00.101"), TIME, 3),

                Arguments.of("00:00:00", "", "INTERVAL '1:1.1' MINUTE TO SECOND(1)", time("00:01:01.1"), TIME, 1),
                Arguments.of("00:00:00.1", "", "INTERVAL '1:11.01' MINUTE TO SECOND(2)", time("00:01:11.11"), TIME, 2),
                Arguments.of("00:00:00.1", "", "INTERVAL '1:11.011' MINUTE TO SECOND(3)", time("00:01:11.111"), TIME, 3),

                // TIME precision greater than the INTERVAL precision.
                Arguments.of("00:00:00.1", "", "INTERVAL '1' SECOND(1, 0)", time("00:00:01.1"), TIME, 1),
                Arguments.of("00:00:00.01", "", "INTERVAL '1' SECOND(1, 1)", time("00:00:01.01"), TIME, 2),
                Arguments.of("00:00:00.001", "", "INTERVAL '1' SECOND(1, 2)", time("00:00:01.001"), TIME, 3),
                Arguments.of("00:00:00.1", "", "INTERVAL '1:1' MINUTE TO SECOND(1)", time("00:01:01.1"), TIME, 1),
                Arguments.of("00:00:00.01", "", "INTERVAL '1:1' MINUTE TO SECOND(1)", time("00:01:01.01"), TIME, 2),
                Arguments.of("00:00:00.001", "", "INTERVAL '1:1' MINUTE TO SECOND(1)", time("00:01:01.001"), TIME, 3),

                // TIME literal and dyn param with explicit cast.
                Arguments.of("00:00:00", "::TIME(0)", "INTERVAL '1' SECOND(1, 0)", time("00:00:01"), TIME, 0),
                Arguments.of("00:00:00", "::TIME(3)", "INTERVAL '1' SECOND(1, 0)", time("00:00:01"), TIME, 3),
                Arguments.of("00:00:00", "::TIME(6)", "INTERVAL '1' SECOND(1, 0)", time("00:00:01"), TIME, 6),
                Arguments.of("00:00:00", "::TIME(9)", "INTERVAL '1' SECOND(1, 0)", time("00:00:01"), TIME, 9),
                Arguments.of("00:00:00.111", "::TIME(0)", "INTERVAL '1' SECOND(1, 0)", time("00:00:01"), TIME, 0),
                Arguments.of("00:00:00.111", "::TIME(1)", "INTERVAL '1' SECOND(1, 0)", time("00:00:01.1"), TIME, 1),
                Arguments.of("00:00:00.111", "::TIME(2)", "INTERVAL '1' SECOND(1, 0)", time("00:00:01.11"), TIME, 2),
                Arguments.of("00:00:00.111", "::TIME(3)", "INTERVAL '1' SECOND(1, 0)", time("00:00:01.111"), TIME, 3),

                // TIMESTAMP
                Arguments.of("1970-01-01 00:00:00", "", "INTERVAL '1' SECOND",
                        dateTime("1970-01-01 00:00:01"), DATETIME, 6),
                Arguments.of("1970-01-01 00:00:00", "", "INTERVAL '1' SECOND(1, 0)",
                        dateTime("1970-01-01 00:00:01"), DATETIME, 0),
                Arguments.of("1970-01-01 00:00:00", "", "INTERVAL '1:1' MINUTE TO SECOND",
                        dateTime("1970-01-01 00:01:01"), DATETIME, 6),

                // INTERVAL precision greater than the TIMESTAMP precision.
                Arguments.of("1970-01-01 00:00:00", "", "INTERVAL '0.001' SECOND",
                        dateTime("1970-01-01 00:00:00.001"), DATETIME, 6),
                Arguments.of("1970-01-01 00:00:00.1", "", "INTERVAL '0.1' SECOND(1, 1)",
                        dateTime("1970-01-01 00:00:00.2"), DATETIME, 1),
                Arguments.of("1970-01-01 00:00:00.1", "", "INTERVAL '0.01' SECOND(1, 2)",
                        dateTime("1970-01-01 00:00:00.11"), DATETIME, 2),
                Arguments.of("1970-01-01 00:00:00.1", "", "INTERVAL '0.001' SECOND(1, 3)",
                        dateTime("1970-01-01 00:00:00.101"), DATETIME, 3),

                Arguments.of("1970-01-01 00:00:00", "", "INTERVAL '1:1.1' MINUTE TO SECOND(1)",
                        dateTime("1970-01-01 00:01:01.1"), DATETIME, 1),
                Arguments.of("1970-01-01 00:00:00.1", "", "INTERVAL '1:11.01' MINUTE TO SECOND(2)",
                        dateTime("1970-01-01 00:01:11.11"), DATETIME, 2),
                Arguments.of("1970-01-01 00:00:00.1", "", "INTERVAL '1:11.011' MINUTE TO SECOND(3)",
                        dateTime("1970-01-01 00:01:11.111"), DATETIME, 3),

                // TIMESTAMP precision greater than the INTERVAL precision.
                Arguments.of("1970-01-01 00:00:00.1", "", "INTERVAL '1' SECOND(1, 0)",
                        dateTime("1970-01-01 00:00:01.1"), DATETIME, 1),
                Arguments.of("1970-01-01 00:00:00.01", "", "INTERVAL '1' SECOND(1, 1)",
                        dateTime("1970-01-01 00:00:01.01"), DATETIME, 2),
                Arguments.of("1970-01-01 00:00:00.001", "", "INTERVAL '1' SECOND(1, 2)",
                        dateTime("1970-01-01 00:00:01.001"), DATETIME, 3),
                Arguments.of("1970-01-01 00:00:00.1", "", "INTERVAL '1:1' MINUTE TO SECOND(1)",
                        dateTime("1970-01-01 00:01:01.1"), DATETIME, 1),
                Arguments.of("1970-01-01 00:00:00.01", "", "INTERVAL '1:1' MINUTE TO SECOND(1)",
                        dateTime("1970-01-01 00:01:01.01"), DATETIME, 2),
                Arguments.of("1970-01-01 00:00:00.001", "", "INTERVAL '1:1' MINUTE TO SECOND(1)",
                        dateTime("1970-01-01 00:01:01.001"), DATETIME, 3),

                // TIMESTAMP literal and dyn param with explicit cast
                Arguments.of("1970-01-01 00:00:00", "::TIMESTAMP(0)", "INTERVAL '1' SECOND(1, 0)",
                        dateTime("1970-01-01 00:00:01"), DATETIME, 0),
                Arguments.of("1970-01-01 00:00:00", "::TIMESTAMP(3)", "INTERVAL '1' SECOND(1, 0)",
                        dateTime("1970-01-01 00:00:01"), DATETIME, 3),
                Arguments.of("1970-01-01 00:00:00", "::TIMESTAMP(6)", "INTERVAL '1' SECOND(1, 0)",
                        dateTime("1970-01-01 00:00:01"), DATETIME, 6),
                Arguments.of("1970-01-01 00:00:00", "::TIMESTAMP(9)", "INTERVAL '1' SECOND(1, 0)",
                        dateTime("1970-01-01 00:00:01"), DATETIME, 9),

                Arguments.of("1970-01-01 00:00:00.111", "::TIMESTAMP(0)", "INTERVAL '1' SECOND(1, 0)",
                        dateTime("1970-01-01 00:00:01"), DATETIME, 0),
                Arguments.of("1970-01-01 00:00:00.111", "::TIMESTAMP(1)", "INTERVAL '1' SECOND(1, 0)",
                        dateTime("1970-01-01 00:00:01.1"), DATETIME, 1),
                Arguments.of("1970-01-01 00:00:00.111", "::TIMESTAMP(2)", "INTERVAL '1' SECOND(1, 0)",
                        dateTime("1970-01-01 00:00:01.11"), DATETIME, 2),
                Arguments.of("1970-01-01 00:00:00.111", "::TIMESTAMP(3)", "INTERVAL '1' SECOND(1, 0)",
                        dateTime("1970-01-01 00:00:01.111"), DATETIME, 3),

                // TIMESTAMP WITH LOCAL TIME ZONE
                Arguments.of("1970-01-01 00:00:00", "", "INTERVAL '1' SECOND",
                        instant("1970-01-01 00:00:01"), TIMESTAMP, 6),
                Arguments.of("1970-01-01 00:00:00", "", "INTERVAL '1' SECOND(1, 0)",
                        instant("1970-01-01 00:00:01"), TIMESTAMP, 0),
                Arguments.of("1970-01-01 00:00:00", "", "INTERVAL '1:1' MINUTE TO SECOND",
                        instant("1970-01-01 00:01:01"), TIMESTAMP, 6),

                // INTERVAL precision greater than the TIMESTAMP_LTZ precision.
                Arguments.of("1970-01-01 00:00:00", "", "INTERVAL '0.001' SECOND",
                        instant("1970-01-01 00:00:00.001"), TIMESTAMP, 6),
                Arguments.of("1970-01-01 00:00:00.1", "", "INTERVAL '0.1' SECOND(1, 1)",
                        instant("1970-01-01 00:00:00.2"), TIMESTAMP, 1),
                Arguments.of("1970-01-01 00:00:00.1", "", "INTERVAL '0.01' SECOND(1, 2)",
                        instant("1970-01-01 00:00:00.11"), TIMESTAMP, 2),
                Arguments.of("1970-01-01 00:00:00.1", "", "INTERVAL '0.001' SECOND(1, 3)",
                        instant("1970-01-01 00:00:00.101"), TIMESTAMP, 3),

                Arguments.of("1970-01-01 00:00:00", "", "INTERVAL '1:1.1' MINUTE TO SECOND(1)",
                        instant("1970-01-01 00:01:01.1"), TIMESTAMP, 1),
                Arguments.of("1970-01-01 00:00:00.1", "", "INTERVAL '1:11.01' MINUTE TO SECOND(2)",
                        instant("1970-01-01 00:01:11.11"), TIMESTAMP, 2),
                Arguments.of("1970-01-01 00:00:00.1", "", "INTERVAL '1:11.011' MINUTE TO SECOND(3)",
                        instant("1970-01-01 00:01:11.111"), TIMESTAMP, 3),

                // TIMESTAMP_LTZ precision greater than the INTERVAL precision.
                Arguments.of("1970-01-01 00:00:00.1", "", "INTERVAL '1' SECOND(1, 0)",
                        instant("1970-01-01 00:00:01.1"), TIMESTAMP, 1),
                Arguments.of("1970-01-01 00:00:00.01", "", "INTERVAL '1' SECOND(1, 1)",
                        instant("1970-01-01 00:00:01.01"), TIMESTAMP, 2),
                Arguments.of("1970-01-01 00:00:00.001", "", "INTERVAL '1' SECOND(1, 2)",
                        instant("1970-01-01 00:00:01.001"), TIMESTAMP, 3),
                Arguments.of("1970-01-01 00:00:00.1", "", "INTERVAL '1:1' MINUTE TO SECOND(1)",
                        instant("1970-01-01 00:01:01.1"), TIMESTAMP, 1),
                Arguments.of("1970-01-01 00:00:00.01", "", "INTERVAL '1:1' MINUTE TO SECOND(1)",
                        instant("1970-01-01 00:01:01.01"), TIMESTAMP, 2),
                Arguments.of("1970-01-01 00:00:00.001", "", "INTERVAL '1:1' MINUTE TO SECOND(1)",
                        instant("1970-01-01 00:01:01.001"), TIMESTAMP, 3),

                // TIMESTAMP_LTZ literal and dyn param with explicit cast
                Arguments.of("1970-01-01 00:00:00", "::TIMESTAMP(0) WITH LOCAL TIME ZONE", "INTERVAL '1' SECOND(1, 0)",
                        instant("1970-01-01 00:00:01"), TIMESTAMP, 0),
                Arguments.of("1970-01-01 00:00:00", "::TIMESTAMP(3) WITH LOCAL TIME ZONE", "INTERVAL '1' SECOND(1, 0)",
                        instant("1970-01-01 00:00:01"), TIMESTAMP, 3),
                Arguments.of("1970-01-01 00:00:00", "::TIMESTAMP(6) WITH LOCAL TIME ZONE", "INTERVAL '1' SECOND(1, 0)",
                        instant("1970-01-01 00:00:01"), TIMESTAMP, 6),
                Arguments.of("1970-01-01 00:00:00", "::TIMESTAMP(9) WITH LOCAL TIME ZONE", "INTERVAL '1' SECOND(1, 0)",
                        instant("1970-01-01 00:00:01"), TIMESTAMP, 9),

                Arguments.of("1970-01-01 00:00:00.111", "::TIMESTAMP(0) WITH LOCAL TIME ZONE", "INTERVAL '1' SECOND(1, 0)",
                        instant("1970-01-01 00:00:01"), TIMESTAMP, 0),
                Arguments.of("1970-01-01 00:00:00.111", "::TIMESTAMP(1) WITH LOCAL TIME ZONE", "INTERVAL '1' SECOND(1, 0)",
                        instant("1970-01-01 00:00:01.1"), TIMESTAMP, 1),
                Arguments.of("1970-01-01 00:00:00.111", "::TIMESTAMP(2) WITH LOCAL TIME ZONE", "INTERVAL '1' SECOND(1, 0)",
                        instant("1970-01-01 00:00:01.11"), TIMESTAMP, 2),
                Arguments.of("1970-01-01 00:00:00.111", "::TIMESTAMP(3) WITH LOCAL TIME ZONE", "INTERVAL '1' SECOND(1, 0)",
                        instant("1970-01-01 00:00:01.111"), TIMESTAMP, 3)
        );
    }

    private static Stream<Arguments> testDatetimeMinusIntervalWithFrartionOfSecondArgs() {
        // DATETIME - INTERVAL
        return Stream.of(
                // TIME
                Arguments.of("00:00:01", "", "INTERVAL '1' SECOND", time("00:00:00"), TIME, 6),
                Arguments.of("00:00:01", "", "INTERVAL '1' SECOND(1, 0)", time("00:00:00"), TIME, 0),
                Arguments.of("00:01:01", "", "INTERVAL '1:1' MINUTE TO SECOND", time("00:00:00"), TIME, 6),

                // INTERVAL precision greater than the TIME precision.
                Arguments.of("00:00:00", "", "INTERVAL '0.001' SECOND", time("23:59:59.999"), TIME, 6),
                Arguments.of("00:00:00.9", "", "INTERVAL '0.1' SECOND(1, 1)", time("00:00:00.8"), TIME, 1),
                Arguments.of("00:00:00.9", "", "INTERVAL '0.01' SECOND(1, 2)", time("00:00:00.89"), TIME, 2),
                Arguments.of("00:00:00.9", "", "INTERVAL '0.001' SECOND(1, 3)", time("00:00:00.899"), TIME, 3),

                Arguments.of("00:00:00", "", "INTERVAL '1:1.1' MINUTE TO SECOND(1)", time("23:58:58.9"), TIME, 1),
                Arguments.of("00:00:00.1", "", "INTERVAL '1:11.01' MINUTE TO SECOND(2)", time("23:58:49.090"), TIME, 2),
                Arguments.of("00:00:00.1", "", "INTERVAL '1:11.001' MINUTE TO SECOND(3)", time("23:58:49.099"), TIME, 3),

                // TIME precision greater than the INTERVAL precision.
                Arguments.of("00:00:00.1", "", "INTERVAL '1' SECOND(1, 0)", time("23:59:59.1"), TIME, 1),
                Arguments.of("00:00:00.01", "", "INTERVAL '1' SECOND(1, 1)", time("23:59:59.01"), TIME, 2),
                Arguments.of("00:00:00.001", "", "INTERVAL '1' SECOND(1, 2)", time("23:59:59.001"), TIME, 3),
                Arguments.of("00:00:00.1", "", "INTERVAL '1:1' MINUTE TO SECOND(1)", time("23:58:59.1"), TIME, 1),
                Arguments.of("00:00:00.01", "", "INTERVAL '1:1' MINUTE TO SECOND(1)", time("23:58:59.01"), TIME, 2),
                Arguments.of("00:00:00.001", "", "INTERVAL '1:1' MINUTE TO SECOND(1)", time("23:58:59.001"), TIME, 3),

                // TIMESTAMP
                Arguments.of("1970-01-01 00:00:01", "", "INTERVAL '1' SECOND",
                        dateTime("1970-01-01 00:00:00"), DATETIME, 6),
                Arguments.of("1970-01-01 00:00:01", "", "INTERVAL '1' SECOND(1, 0)",
                        dateTime("1970-01-01 00:00:00"), DATETIME, 0),
                Arguments.of("1970-01-01 00:01:01", "", "INTERVAL '1:1' MINUTE TO SECOND",
                        dateTime("1970-01-01 00:00:00"), DATETIME, 6),

                // INTERVAL precision greater than the TIMESTAMP precision.
                Arguments.of("1970-01-01 00:00:00", "", "INTERVAL '0.001' SECOND",
                        dateTime("1969-12-31 23:59:59.999"), DATETIME, 6),
                Arguments.of("1970-01-01 00:00:00.9", "", "INTERVAL '0.1' SECOND(1, 1)",
                        dateTime("1970-01-01 00:00:00.8"), DATETIME, 1),
                Arguments.of("1970-01-01 00:00:00.9", "", "INTERVAL '0.01' SECOND(1, 2)",
                        dateTime("1970-01-01 00:00:00.89"), DATETIME, 2),
                Arguments.of("1970-01-01 00:00:00.9", "", "INTERVAL '0.001' SECOND(1, 3)",
                        dateTime("1970-01-01 00:00:00.899"), DATETIME, 3),

                Arguments.of("1970-01-01 00:00:00", "", "INTERVAL '1:1.1' MINUTE TO SECOND(1)",
                        dateTime("1969-12-31 23:58:58.9"), DATETIME, 1),
                Arguments.of("1970-01-01 00:00:00.1", "", "INTERVAL '1:11.01' MINUTE TO SECOND(2)",
                        dateTime("1969-12-31 23:58:49.090"), DATETIME, 2),
                Arguments.of("1970-01-01 00:00:00.1", "", "INTERVAL '1:11.001' MINUTE TO SECOND(3)",
                        dateTime("1969-12-31 23:58:49.099"), DATETIME, 3),

                // TIMESTAMP precision greater than the INTERVAL precision.
                Arguments.of("1970-01-01 00:00:00.1", "", "INTERVAL '1' SECOND(1, 0)",
                        dateTime("1969-12-31 23:59:59.1"), DATETIME, 1),
                Arguments.of("1970-01-01 00:00:00.01", "", "INTERVAL '1' SECOND(1, 1)",
                        dateTime("1969-12-31 23:59:59.01"), DATETIME, 2),
                Arguments.of("1970-01-01 00:00:00.001", "", "INTERVAL '1' SECOND(1, 2)",
                        dateTime("1969-12-31 23:59:59.001"), DATETIME, 3),
                Arguments.of("1970-01-01 00:00:00.1", "", "INTERVAL '1:1' MINUTE TO SECOND(1)",
                        dateTime("1969-12-31 23:58:59.1"), DATETIME, 1),
                Arguments.of("1970-01-01 00:00:00.01", "", "INTERVAL '1:1' MINUTE TO SECOND(1)",
                        dateTime("1969-12-31 23:58:59.01"), DATETIME, 2),
                Arguments.of("1970-01-01 00:00:00.001", "", "INTERVAL '1:1' MINUTE TO SECOND(1)",
                        dateTime("1969-12-31 23:58:59.001"), DATETIME, 3),

                // TIMESTAMP_LTZ
                Arguments.of("1970-01-01 00:00:01", "", "INTERVAL '1' SECOND",
                        instant("1970-01-01 00:00:00"), TIMESTAMP, 6),
                Arguments.of("1970-01-01 00:00:01", "", "INTERVAL '1' SECOND(1, 0)",
                        instant("1970-01-01 00:00:00"), TIMESTAMP, 0),
                Arguments.of("1970-01-01 00:01:01", "", "INTERVAL '1:1' MINUTE TO SECOND",
                        instant("1970-01-01 00:00:00"), TIMESTAMP, 6),

                // INTERVAL precision greater than the TIMESTAMP_LTZ precision.
                Arguments.of("1970-01-01 00:00:00", "", "INTERVAL '0.001' SECOND",
                        instant("1969-12-31 23:59:59.999"), TIMESTAMP, 6),
                Arguments.of("1970-01-01 00:00:00.9", "", "INTERVAL '0.1' SECOND(1, 1)",
                        instant("1970-01-01 00:00:00.8"), TIMESTAMP, 1),
                Arguments.of("1970-01-01 00:00:00.9", "", "INTERVAL '0.01' SECOND(1, 2)",
                        instant("1970-01-01 00:00:00.89"), TIMESTAMP, 2),
                Arguments.of("1970-01-01 00:00:00.9", "", "INTERVAL '0.001' SECOND(1, 3)",
                        instant("1970-01-01 00:00:00.899"), TIMESTAMP, 3),

                Arguments.of("1970-01-01 00:00:00", "", "INTERVAL '1:1.1' MINUTE TO SECOND(1)",
                        instant("1969-12-31 23:58:58.9"), TIMESTAMP, 1),
                Arguments.of("1970-01-01 00:00:00.1", "", "INTERVAL '1:11.01' MINUTE TO SECOND(2)",
                        instant("1969-12-31 23:58:49.090"), TIMESTAMP, 2),
                Arguments.of("1970-01-01 00:00:00.1", "", "INTERVAL '1:11.001' MINUTE TO SECOND(3)",
                        instant("1969-12-31 23:58:49.099"), TIMESTAMP, 3),

                // TIMESTAMP_LTZ precision greater than the INTERVAL precision.
                Arguments.of("1970-01-01 00:00:00.1", "", "INTERVAL '1' SECOND(1, 0)",
                        instant("1969-12-31 23:59:59.1"), TIMESTAMP, 1),
                Arguments.of("1970-01-01 00:00:00.01", "", "INTERVAL '1' SECOND(1, 1)",
                        instant("1969-12-31 23:59:59.01"), TIMESTAMP, 2),
                Arguments.of("1970-01-01 00:00:00.001", "", "INTERVAL '1' SECOND(1, 2)",
                        instant("1969-12-31 23:59:59.001"), TIMESTAMP, 3),
                Arguments.of("1970-01-01 00:00:00.1", "", "INTERVAL '1:1' MINUTE TO SECOND(1)",
                        instant("1969-12-31 23:58:59.1"), TIMESTAMP, 1),
                Arguments.of("1970-01-01 00:00:00.01", "", "INTERVAL '1:1' MINUTE TO SECOND(1)",
                        instant("1969-12-31 23:58:59.01"), TIMESTAMP, 2),
                Arguments.of("1970-01-01 00:00:00.001", "", "INTERVAL '1:1' MINUTE TO SECOND(1)",
                        instant("1969-12-31 23:58:59.001"), TIMESTAMP, 3)
        );
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("testDatetimePlusMinusIntervalNullabilityArgs")
    public void testDatetimePlusMinusIntervalNullability(String colNamePrefix, Temporal inputValue) {
        String nullableColName = format("{}_nullable", colNamePrefix);
        String notNullableColName = format("{}_not_nullable", colNamePrefix);

        assertQuery(format("UPDATE datetime_cols SET {}=?, {}=?", nullableColName, notNullableColName))
                .withParams(inputValue, inputValue)
                .withTimeZoneId(ZoneOffset.UTC)
                .check();

        // NULLABLE
        {
            assertQuery(format("SELECT {} + INTERVAL '0.1' SECOND(1, 1) from datetime_cols", nullableColName))
                    .columnMetadata(new MetadataMatcher().precision(1).nullable(true))
                    .returns(inputValue.plus(100, ChronoUnit.MILLIS))
                    .check();

            assertQuery(format("SELECT INTERVAL '0.1' SECOND(1, 1) + {} from datetime_cols", nullableColName))
                    .columnMetadata(new MetadataMatcher().precision(1).nullable(true))
                    .returns(inputValue.plus(100, ChronoUnit.MILLIS))
                    .check();

            assertQuery(format("SELECT {} - INTERVAL '0.1' SECOND(1, 1) from datetime_cols", nullableColName))
                    .columnMetadata(new MetadataMatcher().precision(1).nullable(true))
                    .returns(inputValue.minus(100, ChronoUnit.MILLIS))
                    .check();
        }

        // NOT NULLABLE
        {
            assertQuery(format("SELECT {} + INTERVAL '0.1' SECOND(1, 1) from datetime_cols", notNullableColName))
                    .columnMetadata(new MetadataMatcher().precision(1).nullable(false))
                    .returns(inputValue.plus(100, ChronoUnit.MILLIS))
                    .check();

            assertQuery(format("SELECT INTERVAL '0.1' SECOND(1, 1) + {} from datetime_cols", notNullableColName))
                    .columnMetadata(new MetadataMatcher().precision(1).nullable(false))
                    .returns(inputValue.plus(100, ChronoUnit.MILLIS))
                    .check();

            assertQuery(format("SELECT {} - INTERVAL '0.1' SECOND(1, 1) from datetime_cols", notNullableColName))
                    .columnMetadata(new MetadataMatcher().precision(1).nullable(false))
                    .returns(inputValue.minus(100, ChronoUnit.MILLIS))
                    .check();
        }
    }

    private static Stream<Arguments> testDatetimePlusMinusIntervalNullabilityArgs() {
        return Stream.of(
                Arguments.of("TIME", time("00:00:00")),
                Arguments.of("TIMESTAMP", dateTime("1970-01-01 00:00:00")),
                Arguments.of("TIMESTAMP_WITH_LOCAL_TIME_ZONE", instant("1970-01-01 00:00:00"))
        );
    }

    @Test
    public void testIntervalArithmetic() {
        // Interval +/- interval.
        assertEquals(Duration.ofSeconds(2), eval("INTERVAL '1' SECONDS + INTERVAL '1' SECONDS"));
        assertEquals(Duration.ofSeconds(1), eval("INTERVAL '2' SECONDS - INTERVAL '1' SECONDS"));
        assertEquals(Duration.ofSeconds(61), eval("INTERVAL '1' MINUTE + INTERVAL '1' SECONDS"));
        assertEquals(Duration.ofSeconds(59), eval("INTERVAL '1' MINUTE - INTERVAL '1' SECONDS"));
        assertEquals(Duration.ofSeconds(59), eval("INTERVAL '1' MINUTE + INTERVAL '-1' SECONDS"));
        assertEquals(Duration.ofSeconds(3723), eval("INTERVAL '1' HOUR + INTERVAL '2:3' MINUTE TO SECONDS"));
        assertEquals(Duration.ofSeconds(3477), eval("INTERVAL '1' HOUR - INTERVAL '2:3' MINUTE TO SECONDS"));
        assertEquals(Duration.ofHours(25), eval("INTERVAL '1' DAY + INTERVAL '1 'HOUR"));
        assertEquals(Period.ofMonths(2), eval("INTERVAL '1' MONTH + INTERVAL '1' MONTH"));
        assertEquals(Period.ofYears(2), eval("INTERVAL '1' YEAR + INTERVAL '1' YEAR"));
        assertEquals(Period.of(1, 1, 0), eval("INTERVAL '1' YEAR + INTERVAL '1' MONTH"));
        assertEquals(Period.ofMonths(11), eval("INTERVAL '1' YEAR - INTERVAL '1' MONTH"));
        assertEquals(Period.ofMonths(11), eval("INTERVAL '1' YEAR + INTERVAL '-1' MONTH"));
        assertThrowsEx("SELECT INTERVAL '1' DAY + INTERVAL '1' MONTH", IgniteException.class, "Cannot apply");

        // Interval * scalar.
        assertEquals(Duration.ofSeconds(2), eval("INTERVAL '1' SECONDS * 2"));
        assertEquals(Duration.ofSeconds(-2), eval("INTERVAL '-1' SECONDS * 2"));
        assertEquals(Duration.ofMinutes(4), eval("INTERVAL '2' MINUTES * 2"));
        assertEquals(Duration.ofHours(6), eval("INTERVAL '3' HOURS * 2"));
        assertEquals(Duration.ofDays(8), eval("INTERVAL '4' DAYS * 2"));
        assertEquals(Period.ofMonths(10), eval("INTERVAL '5' MONTHS * 2"));
        assertEquals(Period.ofMonths(-10), eval("INTERVAL '-5' MONTHS * 2"));
        assertEquals(Period.ofYears(12), eval("INTERVAL '6' YEARS * 2"));
        assertEquals(Period.of(2, 4, 0), eval("INTERVAL '1-2' YEAR TO MONTH * 2"));
        assertEquals(Duration.ofHours(50), eval("INTERVAL '1 1' DAY TO HOUR * 2"));
        assertEquals(Duration.ofMinutes(124), eval("INTERVAL '1:2' HOUR TO MINUTE * 2"));
        assertEquals(Duration.ofSeconds(126), eval("INTERVAL '1:3' MINUTE TO SECOND * 2"));
        assertEquals(Duration.ofSeconds(7446), eval("INTERVAL '1:2:3' HOUR TO SECOND * 2"));
        assertEquals(Duration.ofMillis(7446912), eval("INTERVAL '0 1:2:3.456' DAY TO SECOND * 2"));

        // Interval / scalar
        assertEquals(Duration.ofSeconds(1), eval("INTERVAL '2' SECONDS / 2"));
        assertEquals(Duration.ofSeconds(-1), eval("INTERVAL '-2' SECONDS / 2"));
        assertEquals(Duration.ofSeconds(30), eval("INTERVAL '1' MINUTES / 2"));
        assertEquals(Duration.ofMinutes(90), eval("INTERVAL '3' HOURS / 2"));
        assertEquals(Duration.ofDays(2), eval("INTERVAL '4' DAYS / 2"));
        assertEquals(Period.ofMonths(2), eval("INTERVAL '5' MONTHS / 2"));
        assertEquals(Period.ofMonths(-2), eval("INTERVAL '-5' MONTHS / 2"));
        assertEquals(Period.of(3, 6, 0), eval("INTERVAL '7' YEARS / 2"));
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
        assertEquals(2L, eval("EXTRACT(MONTH FROM INTERVAL '14' MONTHS)"));
        assertEquals(0L, eval("EXTRACT(MONTH FROM INTERVAL '1' YEAR)"));
        assertEquals(2L, eval("EXTRACT(MONTH FROM INTERVAL '1-2' YEAR TO MONTH)"));
        assertEquals(1L, eval("EXTRACT(YEAR FROM INTERVAL '1-2' YEAR TO MONTH)"));
        assertEquals(-1L, eval("EXTRACT(MONTH FROM INTERVAL '-1' MONTHS)"));
        assertEquals(-1L, eval("EXTRACT(YEAR FROM INTERVAL '-14' MONTHS)"));
        assertEquals(-2L, eval("EXTRACT(MONTH FROM INTERVAL '-14' MONTHS)"));
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
        assertEquals(0L, eval("EXTRACT(DAY FROM INTERVAL '1' MONTH)"));

        assertThrowsEx("SELECT EXTRACT(MONTH FROM INTERVAL '1' DAY)", IgniteException.class, "Cannot apply");
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
        List<DateTimeIntervalBasicTestCase> testCases = new ArrayList<>();

        for (SqlTypeName typeName : DATETIME_TYPES) {
            Map<SqlTypeName, List<Interval>> intervalValues = createIntervalValues(typeName);
            for (Map.Entry<SqlTypeName, List<Interval>> entry : intervalValues.entrySet()) {
                for (Interval value : entry.getValue()) {
                    testCases.add(DateTimeIntervalBasicTestCase.newTestCase(typeName, value));
                    testCases.add(DateTimeIntervalBasicTestCase.newTestCase(typeName, value.negated()));
                }
            }
        }

        return testCases;
    }

    private static Map<SqlTypeName, List<Interval>> createIntervalValues(SqlTypeName dateTypeType) {
        Map<SqlTypeName, int[]> singleUnitData = new LinkedHashMap<>();

        singleUnitData.put(SqlTypeName.INTERVAL_SECOND, new int[]{1, 59, 60, 61, 100, 1_000, 10_000});
        singleUnitData.put(SqlTypeName.INTERVAL_MINUTE, new int[]{1, 59, 60, 61, 100, 1_000, 10_000});
        singleUnitData.put(SqlTypeName.INTERVAL_HOUR, new int[]{1, 23, 24, 25, 48, 96, 1_000, 10_000});
        singleUnitData.put(SqlTypeName.INTERVAL_DAY, new int[]{1, 29, 30, 31, 100, 1_000, 10_000});
        singleUnitData.put(SqlTypeName.INTERVAL_MONTH, new int[]{1, 11, 12, 13, 100, 1_000});
        singleUnitData.put(SqlTypeName.INTERVAL_YEAR, new int[]{1, 10, 100, 1_000});

        Map<SqlTypeName, List<Interval>> intervalValues = new LinkedHashMap<>();

        Consumer<SqlTypeName> convert = (type) -> {
            if (!DATETIME_INTERVALS.get(dateTypeType).contains(type)) {
                return;
            }
            List<Interval> vals = intervalValues.computeIfAbsent(type, (k) -> new ArrayList<>());
            for (int val : singleUnitData.get(type)) {
                String literal = format("INTERVAL '{}' {}", val, type.getName().replace("INTERVAL_", ""));
                vals.add(Interval.parse(literal));
            }
        };

        // YEAR MONTH intervals

        convert.accept(SqlTypeName.INTERVAL_YEAR);
        convert.accept(SqlTypeName.INTERVAL_MONTH);

        if (DATETIME_INTERVALS.get(dateTypeType).contains(SqlTypeName.INTERVAL_YEAR_MONTH)) {
            Interval[] yearMonth = {
                    Interval.parse("INTERVAL '1-0' YEAR TO MONTH"),
                    Interval.parse("INTERVAL '7-1' YEAR TO MONTH"),
                    Interval.parse("INTERVAL '17-11' YEAR TO MONTH"),
            };
            for (Interval interval : yearMonth) {
                intervalValues.computeIfAbsent(SqlTypeName.INTERVAL_YEAR_MONTH, (k) -> new ArrayList<>()).add(interval);
            }
        }

        // DAY SECOND intervals

        convert.accept(SqlTypeName.INTERVAL_DAY);
        convert.accept(SqlTypeName.INTERVAL_HOUR);
        convert.accept(SqlTypeName.INTERVAL_MINUTE);
        convert.accept(SqlTypeName.INTERVAL_SECOND);

        if (DATETIME_INTERVALS.get(dateTypeType).contains(SqlTypeName.INTERVAL_DAY_HOUR)) {
            Interval[] dayHour = {
                    Interval.parse("INTERVAL '0 0' DAY TO HOUR"),
                    Interval.parse("INTERVAL '3 0' DAY TO HOUR"),
                    Interval.parse("INTERVAL '3 17' DAY TO HOUR"),
                    Interval.parse("INTERVAL '99 17' DAY TO HOUR"),
            };
            for (Interval interval : dayHour) {
                intervalValues.computeIfAbsent(SqlTypeName.INTERVAL_DAY_HOUR, (k) -> new ArrayList<>()).add(interval);
            }
        }

        if (DATETIME_INTERVALS.get(dateTypeType).contains(SqlTypeName.INTERVAL_DAY_MINUTE)) {
            Interval[] dayMinute = {
                    Interval.parse("INTERVAL '0 00:00' DAY TO MINUTE"),
                    Interval.parse("INTERVAL '0 03:58' DAY TO MINUTE"),
                    Interval.parse("INTERVAL '3 17:58' DAY TO MINUTE"),
                    Interval.parse("INTERVAL '99 17:58' DAY TO MINUTE"),
            };
            for (Interval interval : dayMinute) {
                intervalValues.computeIfAbsent(SqlTypeName.INTERVAL_DAY_MINUTE, (k) -> new ArrayList<>()).add(interval);
            }
        }

        if (DATETIME_INTERVALS.get(dateTypeType).contains(SqlTypeName.INTERVAL_DAY_SECOND)) {
            Interval[] daySecond = {
                    Interval.parse("INTERVAL '0 00:00:00' DAY TO SECOND"),
                    Interval.parse("INTERVAL '3 17:58:59' DAY TO SECOND"),
                    Interval.parse("INTERVAL '3 17:58:59.891' DAY TO SECOND"),
                    Interval.parse("INTERVAL '99 17:58:59.891' DAY TO SECOND"),
            };
            for (Interval interval : daySecond) {
                intervalValues.computeIfAbsent(SqlTypeName.INTERVAL_DAY_SECOND, (k) -> new ArrayList<>()).add(interval);
            }
        }

        if (DATETIME_INTERVALS.get(dateTypeType).contains(SqlTypeName.INTERVAL_HOUR_MINUTE)) {
            Interval[] hourMinute = {
                    Interval.parse("INTERVAL '00:00' HOUR TO MINUTE"),
                    Interval.parse("INTERVAL '00:59' HOUR TO MINUTE"),
                    Interval.parse("INTERVAL '05:00' HOUR TO MINUTE"),
                    Interval.parse("INTERVAL '15:37' HOUR TO MINUTE"),
                    Interval.parse("INTERVAL '23:59' HOUR TO MINUTE")
            };
            for (Interval interval : hourMinute) {
                intervalValues.computeIfAbsent(SqlTypeName.INTERVAL_HOUR_MINUTE, (k) -> new ArrayList<>()).add(interval);
            }
        }

        if (DATETIME_INTERVALS.get(dateTypeType).contains(SqlTypeName.INTERVAL_HOUR_SECOND)) {
            Interval[] hourSecond = {
                    Interval.parse("INTERVAL '00:00:00' HOUR TO SECOND"),
                    Interval.parse("INTERVAL '00:58:59' HOUR TO SECOND"),
                    Interval.parse("INTERVAL '05:37:19' HOUR TO SECOND"),
                    Interval.parse("INTERVAL '17:58:59' HOUR TO SECOND"),
                    Interval.parse("INTERVAL '17:58:59.891' HOUR TO SECOND"),
            };
            for (Interval interval : hourSecond) {
                intervalValues.computeIfAbsent(SqlTypeName.INTERVAL_HOUR_SECOND, (k) -> new ArrayList<>()).add(interval);
            }
        }

        if (DATETIME_INTERVALS.get(dateTypeType).contains(SqlTypeName.INTERVAL_MINUTE_SECOND)) {
            Interval[] minuteSecond = {
                    Interval.parse("INTERVAL '00:00' MINUTE TO SECOND"),
                    Interval.parse("INTERVAL '00:37' MINUTE TO SECOND"),
                    Interval.parse("INTERVAL '17:59' MINUTE TO SECOND"),
                    Interval.parse("INTERVAL '43:29' MINUTE TO SECOND"),
                    Interval.parse("INTERVAL '58:59' MINUTE TO SECOND"),
                    Interval.parse("INTERVAL '58:59.891' MINUTE TO SECOND"),
            };

            for (Interval interval : minuteSecond) {
                intervalValues.computeIfAbsent(SqlTypeName.INTERVAL_MINUTE_SECOND, (k) -> new ArrayList<>()).add(interval);
            }
        }

        return intervalValues;
    }

    abstract static class DateTimeIntervalBasicTestCase {
        private static final ZoneId TIME_ZONE_ID = ZoneId.of("Asia/Nicosia");
        private static final DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");
        private static final String dateString = "1992-01-19 00:00:00.123";
        private static final LocalDateTime testLocalDate = LocalDateTime.parse(dateString, dateTimeFormatter);
        private static final int DEFAULT_DATETIME_LITERAL_PRECISION = 3;

        final RelDataType type;
        final Interval intervalVal;

        private DateTimeIntervalBasicTestCase(RelDataType type, Interval intervalVal) {
            this.type = type;
            this.intervalVal = intervalVal;
        }

        public abstract Temporal expected();

        public abstract Temporal dateTimeValue();

        String sqlDateLiteral() {
            return dateString;
        }

        static DateTimeIntervalBasicTestCase newTestCase(SqlTypeName type, int precision, Interval value) {
            switch (type) {
                case TIMESTAMP:
                    return new SqlTimestampIntervalIntervalTestCase(Commons.typeFactory().createSqlType(type, precision), value);

                case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                    return new SqlTimestampLtzIntervalIntervalTestCase(Commons.typeFactory().createSqlType(type, precision), value);

                case DATE:
                    return new SqlDateIntervalIntervalTestCase(value);

                case TIME:
                    return new SqlTimeIntervalIntervalTestCase(Commons.typeFactory().createSqlType(type, precision), value);

                default:
                    throw new UnsupportedOperationException("Not implemented: " + type);
            }
        }

        static DateTimeIntervalBasicTestCase newTestCase(SqlTypeName type, Interval value) {
            switch (type) {
                case TIMESTAMP:
                    return new SqlTimestampIntervalIntervalTestCase(
                            Commons.typeFactory().createSqlType(type, DEFAULT_DATETIME_LITERAL_PRECISION),
                            value
                    );

                case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                    return new SqlTimestampLtzIntervalIntervalTestCase(
                            Commons.typeFactory().createSqlType(type, DEFAULT_DATETIME_LITERAL_PRECISION),
                            value
                    );

                case DATE:
                    return new SqlDateIntervalIntervalTestCase(value);

                case TIME:
                    return new SqlTimeIntervalIntervalTestCase(
                            Commons.typeFactory().createSqlType(type, DEFAULT_DATETIME_LITERAL_PRECISION),
                            value
                    );

                default:
                    throw new UnsupportedOperationException("Not implemented: " + type);
            }
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append(type.getSqlTypeName().getName());
            if (type.getSqlTypeName().allowsPrec()) {
                sb.append("(");
                sb.append(type.getPrecision());
                sb.append(")");
            }
            sb.append(" '");
            sb.append(sqlDateLiteral());
            sb.append("' ");
            if (intervalVal.sign() > 0) {
                sb.append("+ ");
            } else {
                sb.append("- ");
            }
            sb.append(intervalVal.toLiteral());
            sb.append(" = ");
            sb.append(expected());
            return sb.toString();
        }

        private static class SqlTimestampIntervalIntervalTestCase extends DateTimeIntervalBasicTestCase {

            final LocalDateTime timestamp;

            private SqlTimestampIntervalIntervalTestCase(RelDataType type, Interval intervalVal) {
                super(type, intervalVal);

                int precision = type.getPrecision();
                long nanos = SqlTestUtils.adjustNanos(testLocalDate.getNano(), precision);
                timestamp = testLocalDate.with(ChronoField.NANO_OF_SECOND, nanos);
            }

            @Override
            public LocalDateTime expected() {
                return timestamp.plus(intervalVal.toTemporalAmount());
            }

            @Override
            public Temporal dateTimeValue() {
                return timestamp;
            }
        }

        private static class SqlTimestampLtzIntervalIntervalTestCase extends DateTimeIntervalBasicTestCase {
            final Instant initial;
            final Instant result;

            private SqlTimestampLtzIntervalIntervalTestCase(RelDataType type, Interval value) {
                super(type, value);

                int precision = type.getPrecision();
                long nanos = SqlTestUtils.adjustNanos(testLocalDate.getNano(), precision);

                // Instant only supports Month in jdk21+, so we do all calculations using
                // LocalDateTime and adjust the result according to the required time zone.
                LocalDateTime timestamp = testLocalDate.with(ChronoField.NANO_OF_SECOND, nanos);

                initial = timestamp.atZone(TIME_ZONE_ID).toInstant();
                result = LocalDateTime.ofInstant(initial, ZoneOffset.UTC)
                        .plus(intervalVal.toTemporalAmount()).toInstant(ZoneOffset.UTC);
            }

            @Override
            public Temporal expected() {
                return result;
            }

            @Override
            public Temporal dateTimeValue() {
                return initial;
            }
        }

        private static class SqlDateIntervalIntervalTestCase extends DateTimeIntervalBasicTestCase {
            final LocalDateTime initDateTime;

            SqlDateIntervalIntervalTestCase(Interval value) {
                super(Commons.typeFactory().createSqlType(SqlTypeName.DATE), value);

                // Reset a time component of LocalDateTime type for arithmetic operation to produce correct results
                initDateTime = testLocalDate.truncatedTo(ChronoUnit.DAYS);
            }

            @Override
            String sqlDateLiteral() {
                return dateString.substring(0, 10);
            }

            @Override
            public Temporal dateTimeValue() {
                return initDateTime.toLocalDate();
            }

            @Override
            public Temporal expected() {
                TemporalAmount amount = intervalVal.toTemporalAmount();

                if (amount instanceof Period) {
                    Period period = (Period) amount;
                    return initDateTime.plus(period).toLocalDate();
                } else {
                    // DATE + INTERVAL = DATE + (INTERVAL_TYPE)::(INTERVAL IN DAYS)
                    Duration duration = (Duration) amount;
                    long daysNum = java.util.concurrent.TimeUnit.NANOSECONDS.toDays(duration.toNanos());
                    Duration days = Duration.ofDays(daysNum);
                    return initDateTime.plus(days).toLocalDate();
                }
            }
        }

        private static class SqlTimeIntervalIntervalTestCase extends DateTimeIntervalBasicTestCase {
            private final LocalTime initTime;

            private SqlTimeIntervalIntervalTestCase(RelDataType type, Interval value) {
                super(type, value);

                LocalTime localTime = testLocalDate.toLocalTime();

                int precision = type.getPrecision();
                long nanos = SqlTestUtils.adjustNanos(localTime.getNano(), precision);

                initTime = localTime.withNano((int) nanos);
            }

            @Override
            String sqlDateLiteral() {
                return dateString.substring(11);
            }

            @Override
            public Temporal dateTimeValue() {
                return initTime;
            }

            @Override
            public LocalTime expected() {
                TemporalAmount temporalAmount = intervalVal.toTemporalAmount();
                return initTime.plus(temporalAmount);
            }
        }
    }

    private static class Interval {

        final SqlIntervalLiteral.IntervalValue value;

        final int sign;

        private Interval(IntervalValue value, int sign) {
            this.value = value;
            this.sign = sign;
        }

        static Interval parse(String literal) {
            SqlParser sqlParser = SqlParser.create(literal, IgniteSqlParser.PARSER_CONFIG);
            SqlIntervalLiteral.IntervalValue intervalValue;

            try {
                SqlIntervalLiteral expr = (SqlIntervalLiteral) sqlParser.parseExpression();
                intervalValue = (SqlIntervalLiteral.IntervalValue) expr.getValue();
                Objects.requireNonNull(intervalValue, "intervalValue");
            } catch (Exception e) {
                throw new IllegalArgumentException("Invalid interval literal: " + e.getMessage());
            }

            if (intervalValue.getSign() == -1) {
                String error = format("Interval literal sign should positive. Interval {} ", literal);
                throw new IllegalArgumentException(error);
            }

            return new Interval(intervalValue, 1);
        }

        boolean hasType(TimeUnit unit) {
            return value.getIntervalQualifier().getStartUnit() == unit;
        }

        int sign() {
            return sign;
        }

        Interval negated() {
            return new Interval(value, -sign);
        }

        String toLiteral() {
            // Literal is always positive
            return SqlIntervalLiteral.createInterval(1,
                    value.getIntervalLiteral(),
                    value.getIntervalQualifier(),
                    SqlParserPos.ZERO).toString();
        }

        TemporalAmount toTemporalAmount() {
            TimeUnit intervalUnit = value.getIntervalQualifier().getStartUnit();
            switch (intervalUnit) {
                case YEAR:
                case MONTH: {
                    long val = SqlParserUtil.intervalToMonths(value);
                    return Period.ofMonths((int) val).multipliedBy(sign);
                }
                case DAY:
                case HOUR:
                case MINUTE:
                case SECOND: {
                    long val = SqlParserUtil.intervalToMillis(value);
                    return Duration.ofMillis(val).multipliedBy(sign);
                }
                default:
                    throw new IllegalArgumentException("Unsupported interval qualifier: " + intervalUnit);
            }
        }
    }

    static class Parser {
        static LocalDateTime dateTime(String s) {
            return LocalDateTime.parse(s.replace(' ', 'T'));
        }

        static Instant instant(String s) {
            return dateTime(s).toInstant(ZoneOffset.UTC);
        }

        static LocalTime time(String s) {
            return LocalTime.parse(s);
        }
    }
}
