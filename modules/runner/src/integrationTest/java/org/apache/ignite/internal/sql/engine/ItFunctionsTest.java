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

import static org.apache.calcite.util.Static.RESOURCE;
import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;
import static org.apache.ignite.internal.sql.engine.util.SqlTestUtils.assertThrowsSqlException;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.assertThrowsWithCause;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.temporal.Temporal;
import org.apache.calcite.sql.validate.SqlValidatorException;
import org.apache.ignite.lang.ErrorGroups.Sql;
import org.apache.ignite.lang.IgniteException;
import org.junit.jupiter.api.Test;

/**
 * Test Ignite SQL functions.
 */
public class ItFunctionsTest extends ClusterPerClassIntegrationTest {
    private static final Object[] NULL_RESULT = { null };

    @Test
    public void testTimestampDiffWithFractionsOfSecond() {
        assertQuery("SELECT TIMESTAMPDIFF(MICROSECOND, TIMESTAMP '2022-02-01 10:30:28.000', "
                + "TIMESTAMP '2022-02-01 10:30:28.128')").returns(128000).check();

        assertQuery("SELECT TIMESTAMPDIFF(NANOSECOND, TIMESTAMP '2022-02-01 10:30:28.000', "
                + "TIMESTAMP '2022-02-01 10:30:28.128')").returns(128000000L).check();
    }

    @Test
    public void testLength() {
        assertQuery("SELECT LENGTH('TEST')").returns(4).check();
        assertQuery("SELECT LENGTH(NULL)").returns(NULL_RESULT).check();
    }

    @Test
    public void testCurrentDateTimeTimeStamp() {
        checkDateTimeQuery("SELECT CURRENT_DATE", Clock.DATE_CLOCK, LocalDate.class);
        checkDateTimeQuery("SELECT CURRENT_TIME", Clock.TIME_CLOCK, LocalTime.class);
        checkDateTimeQuery("SELECT CURRENT_TIMESTAMP", Clock.DATE_TIME_CLOCK, LocalDateTime.class);
        checkDateTimeQuery("SELECT LOCALTIME", Clock.TIME_CLOCK, LocalTime.class);
        checkDateTimeQuery("SELECT LOCALTIMESTAMP", Clock.DATE_TIME_CLOCK, LocalDateTime.class);
        checkDateTimeQuery("SELECT {fn CURDATE()}", Clock.DATE_CLOCK, LocalDate.class);
        checkDateTimeQuery("SELECT {fn CURTIME()}", Clock.TIME_CLOCK, LocalTime.class);
        checkDateTimeQuery("SELECT {fn NOW()}", Clock.DATE_TIME_CLOCK, LocalDateTime.class);
    }

    private static <T extends Temporal & Comparable<? super T>> void checkDateTimeQuery(String sql, Clock<T> clock, Class<T> cls) {
        while (true) {
            T tsBeg = clock.now();

            var res = sql(sql);

            T tsEnd = clock.now();

            // Date changed, time comparison may return wrong result.
            if (tsBeg.compareTo(tsEnd) > 0) {
                continue;
            }

            assertEquals(1, res.size());
            assertEquals(1, res.get(0).size());

            Object time = res.get(0).get(0);

            assertThat(time, instanceOf(cls));

            var castedTime = cls.cast(time);

            assertTrue(tsBeg.compareTo(castedTime) <= 0, format("exp ts:{}, act ts:{}", tsBeg, castedTime));
            assertTrue(tsEnd.compareTo(castedTime) >= 0, format("exp ts:{}, act ts:{}", tsEnd, castedTime));

            return;
        }
    }

    @Test
    public void testRange() {
        assertQuery("SELECT * FROM table(system_range(1, 4))")
                .returns(1L)
                .returns(2L)
                .returns(3L)
                .returns(4L)
                .check();

        assertQuery("SELECT * FROM table(system_range(1, 4, 2))")
                .returns(1L)
                .returns(3L)
                .check();

        assertQuery("SELECT * FROM table(system_range(4, 1, -1))")
                .returns(4L)
                .returns(3L)
                .returns(2L)
                .returns(1L)
                .check();

        assertQuery("SELECT * FROM table(system_range(4, 1, -2))")
                .returns(4L)
                .returns(2L)
                .check();

        assertEquals(0, sql("SELECT * FROM table(system_range(4, 1))").size());

        assertEquals(0, sql("SELECT * FROM table(system_range(null, 1))").size());

        IgniteException ex = assertThrowsSqlException(
                Sql.RUNTIME_ERR,
                "Increment can't be 0",
                () -> sql("SELECT * FROM table(system_range(1, 1, 0))"));

        assertTrue(
                ex.getCause() instanceof IllegalArgumentException,
                format(
                        "Expected cause is {}, but was {}",
                        IllegalArgumentException.class.getSimpleName(),
                        ex.getCause() == null ? null : ex.getCause().getClass().getSimpleName()
                )
        );

        assertEquals("Increment can't be 0", ex.getCause().getMessage());
    }

    @Test
    public void testRangeWithCache() {
        sql("CREATE TABLE test(id INT PRIMARY KEY, val INT)");

        try {
            for (int i = 0; i < 100; i++) {
                sql("INSERT INTO test VALUES (?, ?)", i, i);
            }

            // Correlated INNER join.
            assertQuery("SELECT t.val FROM test t WHERE t.val < 5 AND "
                    + "t.id in (SELECT x FROM table(system_range(t.val, t.val))) ")
                    .returns(0)
                    .returns(1)
                    .returns(2)
                    .returns(3)
                    .returns(4)
                    .check();

            // Correlated LEFT joins.
            assertQuery("SELECT t.val FROM test t WHERE t.val < 5 AND "
                    + "EXISTS (SELECT x FROM table(system_range(t.val, t.val)) WHERE mod(x, 2) = 0) ")
                    .returns(0)
                    .returns(2)
                    .returns(4)
                    .check();

            assertQuery("SELECT t.val FROM test t WHERE t.val < 5 AND "
                    + "NOT EXISTS (SELECT x FROM table(system_range(t.val, t.val)) WHERE mod(x, 2) = 0) ")
                    .returns(1)
                    .returns(3)
                    .check();

            assertQuery("SELECT t.val FROM test t WHERE "
                    + "EXISTS (SELECT x FROM table(system_range(t.val, null))) ")
                    .check();

            // Non-correlated join.
            assertQuery("SELECT t.val FROM test t JOIN table(system_range(1, 50)) as r ON t.id = r.x "
                    + "WHERE mod(r.x, 10) = 0")
                    .returns(10)
                    .returns(20)
                    .returns(30)
                    .returns(40)
                    .returns(50)
                    .check();
        } finally {
            sql("DROP TABLE IF EXISTS test");
        }
    }

    @Test
    public void testPercentRemainder() {
        assertQuery("SELECT 3 % 2").returns(1).check();
        assertQuery("SELECT 4 % 2").returns(0).check();
        assertQuery("SELECT NULL % 2").returns(NULL_RESULT).check();
        assertQuery("SELECT 3 % NULL::int").returns(NULL_RESULT).check();
        assertQuery("SELECT 3 % NULL").returns(NULL_RESULT).check();
    }

    @Test
    public void testNullFunctionArguments() {
        // Don't infer result data type from arguments (result is always INTEGER_NULLABLE).
        assertQuery("SELECT ASCII(NULL)").returns(NULL_RESULT).check();
        // Inferring result data type from first STRING argument.
        assertQuery("SELECT REPLACE(NULL, '1', '2')").returns(NULL_RESULT).check();
        // Inferring result data type from both arguments.
        assertQuery("SELECT MOD(1, null)").returns(NULL_RESULT).check();
        // Inferring result data type from first NUMERIC argument.
        assertQuery("SELECT TRUNCATE(NULL, 0)").returns(NULL_RESULT).check();
        // Inferring arguments data types and then inferring result data type from all arguments.
        assertQuery("SELECT FALSE AND NULL").returns(false).check();
    }

    @Test
    public void testReplace() {
        assertQuery("SELECT REPLACE('12341234', '1', '55')").returns("5523455234").check();
        assertQuery("SELECT REPLACE(NULL, '1', '5')").returns(NULL_RESULT).check();
        assertQuery("SELECT REPLACE('1', NULL, '5')").returns(NULL_RESULT).check();
        assertQuery("SELECT REPLACE('11', '1', NULL)").returns(NULL_RESULT).check();
        assertQuery("SELECT REPLACE('11', '1', '')").returns("").check();
    }

    @Test
    public void testMonthnameDayname() {
        assertQuery("SELECT MONTHNAME(DATE '2021-01-01')").returns("January").check();
        assertQuery("SELECT DAYNAME(DATE '2021-01-01')").returns("Friday").check();
    }

    @Test
    public void testRegex() {
        assertQuery("SELECT 'abcd' ~ 'ab[cd]'").returns(true).check();
        assertQuery("SELECT 'abcd' ~ 'ab[cd]$'").returns(false).check();
        assertQuery("SELECT 'abcd' ~ 'ab[CD]'").returns(false).check();
        assertQuery("SELECT 'abcd' ~* 'ab[cd]'").returns(true).check();
        assertQuery("SELECT 'abcd' ~* 'ab[cd]$'").returns(false).check();
        assertQuery("SELECT 'abcd' ~* 'ab[CD]'").returns(true).check();
        assertQuery("SELECT 'abcd' !~ 'ab[cd]'").returns(false).check();
        assertQuery("SELECT 'abcd' !~ 'ab[cd]$'").returns(true).check();
        assertQuery("SELECT 'abcd' !~ 'ab[CD]'").returns(true).check();
        assertQuery("SELECT 'abcd' !~* 'ab[cd]'").returns(false).check();
        assertQuery("SELECT 'abcd' !~* 'ab[cd]$'").returns(true).check();
        assertQuery("SELECT 'abcd' !~* 'ab[CD]'").returns(false).check();
        assertQuery("SELECT null ~ 'ab[cd]'").returns(NULL_RESULT).check();
        assertQuery("SELECT 'abcd' ~ null").returns(NULL_RESULT).check();
        assertQuery("SELECT null ~ null").returns(NULL_RESULT).check();
        assertQuery("SELECT null ~* 'ab[cd]'").returns(NULL_RESULT).check();
        assertQuery("SELECT 'abcd' ~* null").returns(NULL_RESULT).check();
        assertQuery("SELECT null ~* null").returns(NULL_RESULT).check();
        assertQuery("SELECT null !~ 'ab[cd]'").returns(NULL_RESULT).check();
        assertQuery("SELECT 'abcd' !~ null").returns(NULL_RESULT).check();
        assertQuery("SELECT null !~ null").returns(NULL_RESULT).check();
        assertQuery("SELECT null !~* 'ab[cd]'").returns(NULL_RESULT).check();
        assertQuery("SELECT 'abcd' !~* null").returns(NULL_RESULT).check();
        assertQuery("SELECT null !~* null").returns(NULL_RESULT).check();
        assertThrows(IgniteException.class, () -> sql("SELECT 'abcd' ~ '[a-z'"));
    }

    @Test
    public void testCastToBoolean() {
        assertQuery("SELECT 'true'::BOOLEAN").returns(true).check();
        assertQuery("SELECT 'TruE'::BOOLEAN").returns(true).check();
        assertQuery("SELECT 'false'::BOOLEAN").returns(false).check();
        assertQuery("SELECT 'FalsE'::BOOLEAN").returns(false).check();
        assertQuery("SELECT NULL::CHAR::BOOLEAN").returns(NULL_RESULT).check();
        assertQuery("SELECT ?::CHAR::BOOLEAN").withParams(NULL_RESULT).returns(NULL_RESULT).check();

        assertThrowsSqlException(Sql.STMT_VALIDATION_ERR, deriveCannotCastMessage("INTEGER", "BOOLEAN"), () -> sql("SELECT 1::BOOLEAN"));
        assertThrowsSqlException(
                Sql.STMT_VALIDATION_ERR,
                deriveCannotCastMessage("INTEGER", "BOOLEAN"),
                () -> sql("SELECT ?::BOOLEAN", 1));
        assertThrowsSqlException(
                Sql.STMT_VALIDATION_ERR,
                deriveCannotCastMessage("DECIMAL(2, 1)", "BOOLEAN"),
                () -> sql("SELECT 1.0::BOOLEAN"));
        assertThrowsSqlException(
                Sql.STMT_VALIDATION_ERR,
                deriveCannotCastMessage("DOUBLE", "BOOLEAN"),
                () -> sql("SELECT ?::BOOLEAN", 1.0));
        assertThrowsSqlException(Sql.RUNTIME_ERR, "Invalid character for cast", () -> sql("SELECT '1'::BOOLEAN"));
        assertThrowsSqlException(Sql.RUNTIME_ERR, "Invalid character for cast", () -> sql("SELECT ?::BOOLEAN", "1"));
    }

    private String deriveCannotCastMessage(String fromType, String toType) {
        return RESOURCE.cannotCastValue(fromType, toType).ex().getMessage();
    }

    @Test
    public void testTypeOf() {
        assertQuery("SELECT TYPEOF(1)").returns("INTEGER").check();
        assertQuery("SELECT TYPEOF(1.1::DOUBLE)").returns("DOUBLE").check();
        assertQuery("SELECT TYPEOF(1.1::DECIMAL(3, 2))").returns("DECIMAL(3, 2)").check();
        assertQuery("SELECT TYPEOF('a')").returns("CHAR(1)").check();
        assertQuery("SELECT TYPEOF('a'::varchar(1))").returns("VARCHAR(1)").check();
        assertQuery("SELECT TYPEOF(NULL)").returns("NULL").check();
        assertQuery("SELECT TYPEOF(NULL::VARCHAR(100))").returns("VARCHAR(100)").check();
        // A compound expression
        assertQuery("SELECT TYPEOF('abcd' || COALESCE('efg', ?))").withParams("2").returns("VARCHAR").check();

        // An expression that produces an error
        IgniteException failed = assertThrows(IgniteException.class, () -> assertQuery("SELECT typeof(CAST('NONE' as INTEGER))").check());
        assertSame(NumberFormatException.class, failed.getCause().getClass(), "cause");
        assertThat(failed.getCause().getMessage(), containsString("For input string: \"NONE\""));

        assertThrowsWithCause(() -> sql("SELECT TYPEOF()"), SqlValidatorException.class, "Invalid number of arguments");

        assertThrowsWithCause(() -> sql("SELECT TYPEOF(1, 2)"), SqlValidatorException.class, "Invalid number of arguments");

        assertThrowsWithCause(() -> sql("SELECT TYPEOF(SELECT 1, 2)"), IgniteException.class);
    }

    /**
     * Tests for {@code SUBSTRING(str, start[, length])} function.
     */
    @Test
    public void testSubstring() {
        assertQuery("SELECT SUBSTRING('1234567', 1, 3)").returns("123").check();
        assertQuery("SELECT SUBSTRING('1234567', 2)").returns("234567").check();
        assertQuery("SELECT SUBSTRING('1234567', -1)").returns("1234567").check();
        assertQuery("SELECT SUBSTRING(1000, 1, 3)").returns("100").check();

        assertQuery("SELECT SUBSTRING(NULL FROM 1 FOR 2)").returns(null).check();
        assertQuery("SELECT SUBSTRING('text' FROM 1 FOR null)").returns(null).check();
        assertQuery("SELECT SUBSTRING('test' FROM null FOR 2)").returns(null).check();

        // uncomment after https://issues.apache.org/jira/browse/IGNITE-19686 was implemented.
        //assertQuery("select SUBSTRING(s from i for l) from (values ('abc', null, 2)) as t (s, i, l);").returns(null).check();

        assertThrowsWithCause(() -> sql("SELECT SUBSTRING('abcdefg', 1, -3)"), IgniteException.class,
                "negative substring length");
        assertThrowsWithCause(() -> sql("SELECT SUBSTRING('abcdefg' FROM 1 FOR -1)"), IgniteException.class,
                "negative substring length");
    }

    /**
     * Tests for {@code SUBSTR(str, start[, length])} function.
     */
    @Test
    public void testSubstr() {
        assertQuery("SELECT SUBSTR('1234567', 1, 3)").returns("123").check();
        assertQuery("SELECT SUBSTR('1234567', 2)").returns("234567").check();
        assertQuery("SELECT SUBSTR('1234567', -1)").returns("1234567").check();
        assertQuery("SELECT SUBSTR(1000, 1, 3)").returns("100").check();

        assertThrowsWithCause(() -> sql("SELECT SUBSTR('1234567', 1, -3)"), IgniteException.class, "negative substring length");
    }

    /**
     * An interface describing a clock reporting time in a specified temporal value.
     *
     * @param <T> A type of the temporal value returned by the clock.
     */
    private interface Clock<T extends Temporal & Comparable<? super T>> {
        /**
         * A clock reporting a local time.
         */
        Clock<LocalTime> TIME_CLOCK = LocalTime::now;

        /**
         * A clock reporting a local date.
         */
        Clock<LocalDate> DATE_CLOCK = LocalDate::now;

        /**
         * A clock reporting a local datetime.
         */
        Clock<LocalDateTime> DATE_TIME_CLOCK = LocalDateTime::now;

        /**
         * Returns a temporal value representing the current moment.
         *
         * @return Current moment representing by a temporal value.
         */
        T now();
    }
}
