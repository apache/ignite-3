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
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.time.temporal.Temporal;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Stream;
import org.apache.calcite.sql.validate.SqlValidatorException;
import org.apache.ignite.internal.sql.BaseSqlIntegrationTest;
import org.apache.ignite.internal.sql.engine.util.MetadataMatcher;
import org.apache.ignite.internal.util.ArrayUtils;
import org.apache.ignite.lang.ErrorGroups.Sql;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.sql.ColumnType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Test Ignite SQL functions.
 */
@SuppressWarnings("ThrowableNotThrown")
public class ItFunctionsTest extends BaseSqlIntegrationTest {
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
    public void testOctetLength() {
        assertQuery("SELECT OCTET_LENGTH('TEST')").returns(4).check();
        assertQuery("SELECT OCTET_LENGTH('我愛Java')").returns(10).check();
        assertQuery("SELECT OCTET_LENGTH(x'012F')").returns(2).check();
        assertQuery("SELECT OCTET_LENGTH(NULL)").returns(NULL_RESULT).check();
    }

    /**
     * SQL F051-08 feature. Basic date and time. LOCALTIMESTAMP.
     */
    @ParameterizedTest(name = "use default time zone: {0}")
    @ValueSource(booleans = {true, false})
    public void testCurrentDateTimeTimeStamp(boolean useDefaultTimeZone) {
        ZoneId zoneId = ZoneId.systemDefault();

        if (!useDefaultTimeZone) {
            ZoneId utcZone = ZoneId.ofOffset("GMT", ZoneOffset.UTC);
            Instant now = Instant.now();

            if (now.atZone(zoneId).toLocalDateTime().equals(now.atZone(utcZone).toLocalDateTime())) {
                zoneId = ZoneId.ofOffset("GMT", ZoneOffset.of("+01:00"));
            } else {
                zoneId = utcZone;
            }
        }

        checkDateTimeQuery("SELECT CURRENT_DATE", Clock.DATE_CLOCK, LocalDate.class, zoneId);
        checkDateTimeQuery("SELECT CURRENT_TIME", Clock.TIME_CLOCK, LocalTime.class, zoneId);
        checkDateTimeQuery("SELECT CURRENT_TIMESTAMP", Clock.DATE_TIME_CLOCK, LocalDateTime.class, zoneId);
        checkDateTimeQuery("SELECT LOCALTIME", Clock.TIME_CLOCK, LocalTime.class, zoneId);
        checkDateTimeQuery("SELECT LOCALTIMESTAMP", Clock.DATE_TIME_CLOCK, LocalDateTime.class, zoneId);
        checkDateTimeQuery("SELECT {fn CURDATE()}", Clock.DATE_CLOCK, LocalDate.class, zoneId);
        checkDateTimeQuery("SELECT {fn CURTIME()}", Clock.TIME_CLOCK, LocalTime.class, zoneId);
        checkDateTimeQuery("SELECT {fn NOW()}", Clock.DATE_TIME_CLOCK, LocalDateTime.class, zoneId);
    }

    private static <T extends Temporal & Comparable<? super T>> void checkDateTimeQuery(
            String sql, Clock<T> clock, Class<T> cls, ZoneId timeZone
    ) {
        while (true) {
            T tsBeg = clock.now(timeZone);

            List<List<Object>> res = sql(0, null, timeZone, sql, ArrayUtils.OBJECT_EMPTY_ARRAY);

            T tsEnd = clock.now(timeZone);

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

        assertEquals(0, sql("SELECT * FROM table(system_range(1, null))").size());

        assertThrowsSqlException(
                Sql.RUNTIME_ERR,
                "Increment can't be 0",
                () -> sql("SELECT * FROM table(system_range(1, 1, 0))"));

        assertQuery("SELECT (SELECT * FROM table(system_range(4, 1)))")
                .returns(null)
                .check();

        assertQuery("SELECT (SELECT * FROM table(system_range(1, 1)))")
                .returns(1L)
                .check();

        assertThrowsSqlException(
                Sql.RUNTIME_ERR,
                "Subquery returned more than 1 value",
                () -> sql("SELECT (SELECT * FROM table(system_range(1, 10)))"));
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
        assertThrowsSqlException(Sql.STMT_PARSE_ERR, "", () -> sql("SELECT typeof(CAST('NONE' as INTEGER))"));

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

        assertQuery("SELECT SUBSTRING(s from i for l) from (values ('abc', null, 2)) as t (s, i, l);").returns(null).check();

        assertQuery("SELECT SUBSTRING('1234567', 2.1, 3.1);").returns("234").check();
        assertQuery("SELECT SUBSTRING('1234567', 2.1, 3);").returns("234").check();
        assertQuery("SELECT SUBSTRING('1234567', 2, 3.1);").returns("234").check();
        assertQuery("SELECT SUBSTRING('1234567', 2.1);").returns("234567").check();

        // type coercion
        assertQuery("SELECT SUBSTRING('1234567', 2, '1');").returns("2").check();
        assertQuery("SELECT SUBSTRING('1234567', '2', 1);").returns("2").check();

        assertQuery(String.format("SELECT SUBSTRING('1234567', 1, %d)", Long.MAX_VALUE)).returns("1234567").check();
        assertQuery(String.format("SELECT SUBSTRING('1234567', %d)", Long.MAX_VALUE)).returns("").check();
        assertQuery(String.format("SELECT SUBSTRING('1234567', %d::BIGINT)", 1)).returns("1234567").check();
        assertQuery(String.format("SELECT SUBSTRING('1234567', %d)", Long.MIN_VALUE)).returns("1234567").check();
        assertQuery(String.format("SELECT SUBSTRING('1234567', %d)", Integer.MIN_VALUE)).returns("1234567").check();
        assertQuery(String.format("SELECT SUBSTRING('1234567', %d, %d)", Integer.MIN_VALUE, 10L + Integer.MAX_VALUE))
                .returns("1234567").check();
        assertQuery(String.format("SELECT SUBSTRING('1234567', %d, %d)", -1, 5)).returns("123").check();

        assertThrowsSqlException(Sql.RUNTIME_ERR, "negative substring length", () -> sql("SELECT SUBSTRING('1234567', 1, -1)"));
        assertThrowsSqlException(Sql.RUNTIME_ERR, "negative substring length", () ->
                sql(String.format("SELECT SUBSTRING('1234567', %d, %d)", Long.MIN_VALUE, Long.MIN_VALUE)));
        assertThrowsSqlException(Sql.RUNTIME_ERR, "negative substring length", () -> sql("SELECT SUBSTRING('abcdefg', 1, -3)"));
        assertThrowsSqlException(Sql.RUNTIME_ERR, "negative substring length", () -> sql("SELECT SUBSTRING('abcdefg' FROM 1 FOR -1)"));
    }

    /** Tests LOWER, UPPER functions. */
    @Test
    public void testLowerUpper() {
        assertQuery("SELECT LOWER(NULL)").returns(null).check();
        assertQuery("SELECT LOWER('NULL')").returns("null").check();
        assertQuery("SELECT UPPER(NULL)").returns(null).check();
        assertQuery("SELECT UPPER('NULL')").returns("NULL").check();
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

    @ParameterizedTest(name = "{0}")
    @MethodSource("integralTypes")
    public void testRoundIntTypes(ParseNum parse, MetadataMatcher matcher) {
        String v1 = parse.value("42");
        String v2 = parse.value("45");
        String v3 = parse.value("47");

        assertQuery(format("SELECT ROUND({}), ROUND({}, 0)", v1, v1))
                .returns(parse.apply("42"), parse.apply("42"))
                .columnMetadata(matcher, matcher)
                .check();

        String query = format(
                "SELECT ROUND({}, -2), ROUND({}, -1), ROUND({}, -1), ROUND({}, -1)",
                v1, v1, v2, v3);

        assertQuery(query)
                .returns(parse.apply("0"), parse.apply("40"), parse.apply("50"), parse.apply("50"))
                .columnMetadata(matcher, matcher, matcher, matcher)
                .check();
    }

    private static Stream<Arguments> integralTypes() {
        return Stream.of(
                Arguments.of(new ParseNum("TINYINT", Byte::parseByte), new MetadataMatcher().type(ColumnType.INT8)),
                Arguments.of(new ParseNum("SMALLINT", Short::parseShort), new MetadataMatcher().type(ColumnType.INT16)),
                Arguments.of(new ParseNum("INTEGER", Integer::parseInt), new MetadataMatcher().type(ColumnType.INT32)),
                Arguments.of(new ParseNum("BIGINT", Long::parseLong), new MetadataMatcher().type(ColumnType.INT64)),
                Arguments.of(new ParseNum("DECIMAL(4)", BigDecimal::new), new MetadataMatcher().type(ColumnType.DECIMAL).precision(4))
        );
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("nonIntegerTypes")
    public void testRoundNonIntegralTypes(ParseNum parse, MetadataMatcher round1, MetadataMatcher round2) {
        String v1 = parse.value("42.123");
        String v2 = parse.value("45.000");
        String v3 = parse.value("47.123");

        assertQuery(format("SELECT ROUND(1.5::{})", parse.typeName))
                .returns(parse.apply("2"))
                .check();

        assertQuery(format("SELECT ROUND({}), ROUND({}, 0)", v1, v1))
                .returns(parse.apply("42"), parse.apply("42.000"))
                .columnMetadata(round1, round2)
                .check();

        assertQuery(format("SELECT ROUND({}, -2), ROUND({}, -1), ROUND({}, -1),  ROUND({}, -1)", v1, v1, v2, v3))
                .returns(parse.apply("0.000"), parse.apply("40.000"), parse.apply("50.000"), parse.apply("50.000"))
                .columnMetadata(round2, round2, round2, round2)
                .check();

        String v4 = parse.value("1.123");

        assertQuery(format("SELECT ROUND({}, s) FROM (VALUES (-2), (-1), (0), (1), (2), (3), (4), (100) ) t(s)", v4))
                .returns(parse.apply("0.000"))
                .returns(parse.apply("0.000"))
                .returns(parse.apply("1.000"))
                .returns(parse.apply("1.100"))
                .returns(parse.apply("1.120"))
                .returns(parse.apply("1.123"))
                .returns(parse.apply("1.123"))
                .returns(parse.apply("1.123"))
                .columnMetadata(round2)
                .check();
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("integralTypes")
    public void testTruncateIntTypes(ParseNum parse, MetadataMatcher matcher) {
        String v1 = parse.value("42");
        String v2 = parse.value("45");
        String v3 = parse.value("47");

        assertQuery(format("SELECT TRUNCATE({}), TRUNCATE({}, 0)", v1, v1))
                .returns(parse.apply("42"), parse.apply("42"))
                .columnMetadata(matcher, matcher)
                .check();

        String query = format(
                "SELECT TRUNCATE({}, -2), TRUNCATE({}, -1), TRUNCATE({}, -1), TRUNCATE({}, -1)",
                v1, v1, v2, v3);

        assertQuery(query)
                .returns(parse.apply("0"), parse.apply("40"), parse.apply("40"), parse.apply("40"))
                .columnMetadata(matcher, matcher, matcher, matcher)
                .check();
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("nonIntegerTypes")
    public void testTruncateNonIntegralTypes(ParseNum parse, MetadataMatcher round1, MetadataMatcher round2) {
        String v1 = parse.value("42.123");
        String v2 = parse.value("45.000");
        String v3 = parse.value("47.123");

        assertQuery(format("SELECT TRUNCATE(1.6::{})", parse.typeName))
                .returns(parse.apply("1"))
                .check();

        assertQuery(format("SELECT TRUNCATE({}), TRUNCATE({}, 0)", v1, v1))
                .returns(parse.apply("42"), parse.apply("42.000"))
                .columnMetadata(round1, round2)
                .check();

        assertQuery(format("SELECT TRUNCATE({}, -2), TRUNCATE({}, -1), TRUNCATE({}, -1),  TRUNCATE({}, -1)", v1, v1, v2, v3))
                .returns(parse.apply("0.000"), parse.apply("40.000"), parse.apply("40.000"), parse.apply("40.000"))
                .columnMetadata(round2, round2, round2, round2)
                .check();

        String v4 = parse.value("1.123");

        assertQuery(format("SELECT TRUNCATE({}, s) FROM (VALUES (-2), (-1), (0), (1), (2), (3), (4), (100) ) t(s)", v4))
                .returns(parse.apply("0.000"))
                .returns(parse.apply("0.000"))
                .returns(parse.apply("1.000"))
                .returns(parse.apply("1.100"))
                .returns(parse.apply("1.120"))
                .returns(parse.apply("1.123"))
                .returns(parse.apply("1.123"))
                .returns(parse.apply("1.123"))
                .columnMetadata(round2)
                .check();
    }

    private static Stream<Arguments> nonIntegerTypes() {
        MetadataMatcher matchFloat = new MetadataMatcher().type(ColumnType.FLOAT);
        MetadataMatcher matchDouble = new MetadataMatcher().type(ColumnType.DOUBLE);

        MetadataMatcher matchDecimal1 = new MetadataMatcher().type(ColumnType.DECIMAL).precision(5).scale(0);
        MetadataMatcher matchDecimal2 = new MetadataMatcher().type(ColumnType.DECIMAL).precision(5).scale(3);

        return Stream.of(
                Arguments.of(new ParseNum("REAL", Float::parseFloat), matchFloat, matchFloat),
                Arguments.of(new ParseNum("DOUBLE", Double::parseDouble), matchDouble, matchDouble),
                Arguments.of(new ParseNum("DECIMAL(5, 3)", BigDecimal::new), matchDecimal1, matchDecimal2)
        );
    }

    /** Numeric type parser. */
    public static final class ParseNum {

        private final String typeName;

        private final Function<String, Object> func;

        ParseNum(String typeName, Function<String, Object> func) {
            this.typeName = typeName;
            this.func = func;
        }

        public Object apply(String val) {
            return func.apply(val);
        }

        public String value(String val) {
            return val + "::" + typeName;
        }

        @Override
        public String toString() {
            return typeName;
        }
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
        Clock<LocalTime> TIME_CLOCK = zoneId -> LocalTime.now(zoneId).truncatedTo(ChronoUnit.MILLIS);

        /**
         * A clock reporting a local date.
         */
        Clock<LocalDate> DATE_CLOCK = LocalDate::now;

        /**
         * A clock reporting a local datetime.
         */
        Clock<LocalDateTime> DATE_TIME_CLOCK = zoneId -> LocalDateTime.now(zoneId).truncatedTo(ChronoUnit.MILLIS);

        /**
         * Returns a temporal value representing the current moment.
         *
         * @return Current moment representing by a temporal value.
         */
        T now(ZoneId zoneId);
    }
}
