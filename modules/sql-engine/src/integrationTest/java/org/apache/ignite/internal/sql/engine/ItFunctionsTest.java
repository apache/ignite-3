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
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.jupiter.api.Assertions.assertEquals;
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
import org.apache.ignite.internal.sql.BaseSqlIntegrationTest;
import org.apache.ignite.internal.sql.engine.util.MetadataMatcher;
import org.apache.ignite.internal.util.ArrayUtils;
import org.apache.ignite.sql.ColumnType;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Test Ignite SQL functions.
 */
public class ItFunctionsTest extends BaseSqlIntegrationTest {

    /**
     * SQL F051-06 feature. Basic date and time. CURRENT_DATE.
     * SQL F051-07 feature. Basic date and time. LOCALTIME.
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
