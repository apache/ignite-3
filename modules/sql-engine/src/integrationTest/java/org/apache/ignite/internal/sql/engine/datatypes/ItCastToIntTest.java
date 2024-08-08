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
import static org.apache.ignite.internal.sql.engine.util.QueryChecker.NULL_AS_VARARG;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.ignite.internal.sql.BaseSqlIntegrationTest;
import org.apache.ignite.internal.sql.engine.util.QueryChecker;
import org.apache.ignite.internal.sql.engine.util.SqlTestUtils;
import org.apache.ignite.internal.testframework.WithSystemProperty;
import org.apache.ignite.lang.ErrorGroups.Sql;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Set of tests to ensure correctness of CAST expression to INTEGER for
 * type pairs supported by cast specification.
 */
@WithSystemProperty(key = "IMPLICIT_PK_ENABLED", value = "true")
public class ItCastToIntTest extends BaseSqlIntegrationTest {
    @BeforeAll
    static void createTable() {
        sql("CREATE TABLE test (val INTEGER)");
        sql("CREATE TABLE src (id INT PRIMARY KEY, ti TINYINT, si SMALLINT, bi BIGINT," 
                + " dec DECIMAL(16, 2), r REAL, d DOUBLE, s VARCHAR(100))");
    }

    @AfterAll
    static void dropTable() {
        sql("DROP TABLE IF EXISTS test");
        sql("DROP TABLE IF EXISTS src");
    }

    @AfterEach
    void clearTable() {
        sql("DELETE FROM test");
        sql("DELETE FROM src");
    }

    @ParameterizedTest
    @MethodSource("literalsWithExpectedResult")
    void implicitCastOfLiteralsOnInsert(String literal, Object expectedResult) {
        assertQuery(format("INSERT INTO test VALUES ({})", literal)).check();

        assertQuery("SELECT * FROM test")
                .returns(expectedResult)
                .check();
    }

    @SuppressWarnings("ThrowableNotThrown")
    @ParameterizedTest
    @MethodSource("literalsWithOverflow")
    void implicitCastOfLiteralsOnInsertWithOverflow(String literal) {
        SqlTestUtils.assertThrowsSqlException(
                Sql.RUNTIME_ERR,
                // TODO IGNITE-22932 The message must be "INTEGER out of range"
                "out of range",
                () -> sql(format("INSERT INTO test VALUES ({})", literal))
        );
    }

    @ParameterizedTest
    @MethodSource("literalsWithExpectedResult")
    void explicitCastOfLiteralsOnInsert(String literal, Object expectedResult) {
        assertQuery(format("INSERT INTO test VALUES (CAST({} as INTEGER))", literal)).check();

        assertQuery("SELECT * FROM test")
                .returns(expectedResult)
                .check();
    }

    @SuppressWarnings("ThrowableNotThrown")
    @ParameterizedTest
    @MethodSource("literalsWithOverflow")
    void explicitCastOfLiteralsOnInsertWithOverflow(String literal) {
        SqlTestUtils.assertThrowsSqlException(
                Sql.RUNTIME_ERR,
                // TODO IGNITE-22932 The message must be "INTEGER out of range"
                "out of range",
                () -> sql(format("INSERT INTO test VALUES (CAST({} as INTEGER))", literal))
        );
    }

    @ParameterizedTest
    @MethodSource("literalsWithExpectedResult")
    void explicitCastOfLiteralsOnSelect(String literal, Object expectedResult) {
        assertQuery(format("SELECT CAST({} as INTEGER)", literal))
                .returns(expectedResult)
                .check();
    }

    @SuppressWarnings("ThrowableNotThrown")
    @ParameterizedTest
    @MethodSource("literalsWithOverflow")
    void explicitCastOfLiteralsOnSelectWithOverflow(String literal) {
        SqlTestUtils.assertThrowsSqlException(
                Sql.RUNTIME_ERR,
                // TODO IGNITE-22932 The message must be "INTEGER out of range"
                "out of range",
                () -> sql(format("SELECT CAST({} as INTEGER)", literal))
        );
    }

    @Test
    void explicitCastOfLiteralsOnMultiInsert() {
        List<Object> expectedResults = new ArrayList<>();

        StringBuilder builder = new StringBuilder("INSERT INTO test VALUES");
        boolean appendComma = false;
        for (Arguments args : literalsWithExpectedResult().collect(Collectors.toList())) {
            if (appendComma) {
                builder.append(",");
            }

            appendComma = true;

            String literal = (String) args.get()[0];
            Object expectedResult = args.get()[1];

            builder.append("(CAST(").append(literal).append(" as INTEGER))");

            expectedResults.add(expectedResult);
        }

        assertQuery(builder.toString()).check();

        QueryChecker checker = assertQuery("SELECT * FROM test");

        expectedResults.forEach(checker::returns);

        checker.check();
    }

    @ParameterizedTest
    @MethodSource("valuesWithExpectedResult")
    @Disabled("https://issues.apache.org/jira/browse/IGNITE-22779")
    void implicitCastOfDynParamsOnInsert(Object param, Object expectedResult) {
        assertQuery("INSERT INTO test VALUES (?)")
                .withParam(param)
                .check();

        assertQuery("SELECT * FROM test")
                .returns(expectedResult)
                .check();
    }

    @SuppressWarnings("ThrowableNotThrown")
    @ParameterizedTest
    @MethodSource("valuesWithOverflow")
    @Disabled("https://issues.apache.org/jira/browse/IGNITE-22779")
    void implicitCastOfDynParamsOnInsertWithOverflow(Object param) {
        SqlTestUtils.assertThrowsSqlException(
                Sql.RUNTIME_ERR,
                "INTEGER out of range",
                () -> sql("INSERT INTO test VALUES (?)", param)
        );
    }

    @ParameterizedTest
    @MethodSource("valuesWithExpectedResult")
    void explicitCastOfDynParamsOnInsert(Object param, Object expectedResult) {
        assertQuery("INSERT INTO test VALUES (CAST(? as INTEGER))")
                .withParam(param)
                .check();

        assertQuery("SELECT * FROM test")
                .returns(expectedResult)
                .check();
    }

    @SuppressWarnings("ThrowableNotThrown")
    @ParameterizedTest
    @MethodSource("valuesWithOverflow")
    void explicitCastOfDynParamsOnInsertWithOverflow(Object param) {
        SqlTestUtils.assertThrowsSqlException(
                Sql.RUNTIME_ERR,
                "INTEGER out of range",
                () -> sql("INSERT INTO test VALUES (CAST(? as INTEGER))", param)
        );
    }

    @ParameterizedTest
    @MethodSource("valuesWithExpectedResult")
    void explicitCastOfDynParamsOnSelect(Object param, Object expectedResult) {
        assertQuery("SELECT CAST(? as INTEGER)")
                .withParam(param)
                .returns(expectedResult)
                .check();
    }

    @SuppressWarnings("ThrowableNotThrown")
    @ParameterizedTest
    @MethodSource("valuesWithOverflow")
    void explicitCastOfDynParamsOnSelectWithOverflow(Object param) {
        SqlTestUtils.assertThrowsSqlException(
                Sql.RUNTIME_ERR,
                "INTEGER out of range",
                () -> sql("SELECT CAST(? as INTEGER)", param)
        );
    }

    @Test
    void explicitCastOfDynParamsOnMultiInsert() {
        List<Object> expectedResults = new ArrayList<>();

        StringBuilder builder = null;
        boolean appendComma = false;
        int count = 0;
        List<Object> params = null;
        for (Arguments args : valuesWithExpectedResult().collect(Collectors.toList())) {
            if (builder == null) {
                builder = new StringBuilder("INSERT INTO test VALUES");
                appendComma = false;
                params = new ArrayList<>();
            }

            if (appendComma) {
                builder.append(",");
            }

            appendComma = true;

            Object param = args.get()[0];
            Object expectedResult = args.get()[1];

            builder.append("(CAST(? as INTEGER))");

            expectedResults.add(expectedResult);
            params.add(param);

            if (count++ % 10 == 0) {
                assertQuery(builder.toString())
                        .withParams(params.toArray())
                        .check();

                builder = null;
            }

        }

        if (builder != null) {
            assertQuery(builder.toString())
                    .withParams(params.toArray())
                    .check();
        }

        QueryChecker checker = assertQuery("SELECT * FROM test");

        expectedResults.forEach(checker::returns);

        checker.check();
    }

    @Test
    void implicitCastOfSourceTableOnInsert() {
        sql("INSERT INTO src VALUES "
                + "(0, NULL, NULL, NULL, NULL, NULL, NULL, NULL),"
                + "(1, 42, 42, 42, 42, 42, 42, '42'),"
                + "(2, 127, 32767, 2147483647, 2147483647, 2.14748358E9, 2147483647, '2147483647'),"
                + "(3, 127.1, 32767.1, 2147483647.1, 2147483647.1, 2.14748358E9, 2147483647.1, '2147483647.1'),"
                + "(4, 127.9, 32767.9, 2147483647.9, 2147483647.9, 2.14748358E9, 2147483647.9, '2147483647.9'),"
                + "(5, -128, -32768, -2147483648, -2147483648, -2.14748358E9, -2147483648, '-2147483648'),"
                + "(6, -128.1, -32768.1, -2147483648.1, -2147483648.1, -2.14748358E9, -2147483648.1, '-2147483648.1'),"
                + "(7, -128.9, -32768.9, -2147483648.9, -2147483648.9, -2.14748358E9, -2147483648.9, '-2147483648.9')"
        );

        assertQuery("INSERT INTO test SELECT ti FROM src").check();
        assertQuery("INSERT INTO test SELECT si FROM src").check();
        assertQuery("INSERT INTO test SELECT bi FROM src").check();
        assertQuery("INSERT INTO test SELECT dec FROM src").check();
        assertQuery("INSERT INTO test SELECT r FROM src").check();
        assertQuery("INSERT INTO test SELECT d FROM src").check();
        assertQuery("INSERT INTO test SELECT s FROM src").check();

        assertQuery("SELECT * FROM test")
                .returns(NULL_AS_VARARG)
                .returns(42)
                .returns(127)
                .returns(127)
                .returns(127)
                .returns(-128)
                .returns(-128)
                .returns(-128)
                .returns(NULL_AS_VARARG)
                .returns(42)
                .returns(32767)
                .returns(32767)
                .returns(32767)
                .returns(-32768)
                .returns(-32768)
                .returns(-32768)
                .returns(NULL_AS_VARARG)
                .returns(42)
                .returns(2147483647)
                .returns(2147483647)
                .returns(2147483647)
                .returns(-2147483648)
                .returns(-2147483648)
                .returns(-2147483648)
                .returns(NULL_AS_VARARG)
                .returns(42)
                .returns(2147483647)
                .returns(2147483647)
                .returns(2147483647)
                .returns(-2147483648)
                .returns(-2147483648)
                .returns(-2147483648)
                .returns(NULL_AS_VARARG)
                .returns(42)
                .returns(2147483520)
                .returns(2147483520)
                .returns(2147483520)
                .returns(-2147483520)
                .returns(-2147483520)
                .returns(-2147483520)
                .returns(NULL_AS_VARARG)
                .returns(42)
                .returns(2147483647)
                .returns(2147483647)
                .returns(2147483647)
                .returns(-2147483648)
                .returns(-2147483648)
                .returns(-2147483648)
                .returns(NULL_AS_VARARG)
                .returns(42)
                .returns(2147483647)
                .returns(2147483647)
                .returns(2147483647)
                .returns(-2147483648)
                .returns(-2147483648)
                .returns(-2147483648)
                .check();
    }

    @SuppressWarnings("ThrowableNotThrown")
    @Test
    void implicitCastOfSourceTableOnInsertWithOverflow() {
        sql("INSERT INTO src VALUES "
                + "(1, 127, 32767, 2147483648, 2147483648, 2.14748365E9, 2147483648, '2147483648'),"
                + "(2, 127.0, 32767.0, 2147483648.0, 2147483648.0, 2.14748365E9, 2147483648.0, '2147483648.0'),"
                + "(3, 127.1, 32767.1, 2147483648.1, 2147483648.1, 2.14748365E9, 2147483648.1, '2147483648.1'),"
                + "(4, -128, -32768, -2147483649, -2147483649, -2.14748378E9, -2147483649, '-2147483649'),"
                + "(5, -128.0, -32768.0, -2147483649.0, -2147483649.0, -2.14748378E9, -2147483649.0, '-2147483649.0'),"
                + "(6, -128.1, -32768.1, -2147483649.1, -2147483649.1, -2.14748378E9, -2147483649.1, '-2147483649.1')"
        );

        for (int idx : new int[] {1, 2, 3, 4, 5, 6}) {
            for (String column : new String[] {"bi", "dec", "r", "d", "s"}) {
                SqlTestUtils.assertThrowsSqlException(
                        Sql.RUNTIME_ERR,
                        "INTEGER out of range",
                        () -> sql(format("INSERT INTO test SELECT {} FROM src WHERE id = {}", column, idx))
                );
            }
        }
    }

    @Test
    void explicitCastOfSourceTableOnInsert() {
        sql("INSERT INTO src VALUES "
                + "(0, NULL, NULL, NULL, NULL, NULL, NULL, NULL),"
                + "(1, 42, 42, 42, 42, 42, 42, '42'),"
                + "(2, 127, 32767, 2147483647, 2147483647, 2.14748358E9, 2147483647, '2147483647'),"
                + "(3, 127.1, 32767.1, 2147483647.1, 2147483647.1, 2.14748358E9, 2147483647.1, '2147483647.1'),"
                + "(4, 127.9, 32767.9, 2147483647.9, 2147483647.9, 2.14748358E9, 2147483647.9, '2147483647.9'),"
                + "(5, -128, -32768, -2147483648, -2147483648, -2.14748358E9, -2147483648, '-2147483648'),"
                + "(6, -128.1, -32768.1, -2147483648.1, -2147483648.1, -2.14748358E9, -2147483648.1, '-2147483648.1'),"
                + "(7, -128.9, -32768.9, -2147483648.9, -2147483648.9, -2.14748358E9, -2147483648.9, '-2147483648.9')"
        );

        assertQuery("INSERT INTO test SELECT CAST(ti as INTEGER) FROM src").check();
        assertQuery("INSERT INTO test SELECT CAST(si as INTEGER) FROM src").check();
        assertQuery("INSERT INTO test SELECT CAST(bi as INTEGER) FROM src").check();
        assertQuery("INSERT INTO test SELECT CAST(dec as INTEGER) FROM src").check();
        assertQuery("INSERT INTO test SELECT CAST(r as INTEGER) FROM src").check();
        assertQuery("INSERT INTO test SELECT CAST(d as INTEGER) FROM src").check();
        assertQuery("INSERT INTO test SELECT CAST(s as INTEGER) FROM src").check();

        assertQuery("SELECT * FROM test")
                .returns(NULL_AS_VARARG)
                .returns(42)
                .returns(127)
                .returns(127)
                .returns(127)
                .returns(-128)
                .returns(-128)
                .returns(-128)
                .returns(NULL_AS_VARARG)
                .returns(42)
                .returns(32767)
                .returns(32767)
                .returns(32767)
                .returns(-32768)
                .returns(-32768)
                .returns(-32768)
                .returns(NULL_AS_VARARG)
                .returns(42)
                .returns(2147483647)
                .returns(2147483647)
                .returns(2147483647)
                .returns(-2147483648)
                .returns(-2147483648)
                .returns(-2147483648)
                .returns(NULL_AS_VARARG)
                .returns(42)
                .returns(2147483647)
                .returns(2147483647)
                .returns(2147483647)
                .returns(-2147483648)
                .returns(-2147483648)
                .returns(-2147483648)
                .returns(NULL_AS_VARARG)
                .returns(42)
                .returns(2147483520)
                .returns(2147483520)
                .returns(2147483520)
                .returns(-2147483520)
                .returns(-2147483520)
                .returns(-2147483520)
                .returns(NULL_AS_VARARG)
                .returns(42)
                .returns(2147483647)
                .returns(2147483647)
                .returns(2147483647)
                .returns(-2147483648)
                .returns(-2147483648)
                .returns(-2147483648)
                .returns(NULL_AS_VARARG)
                .returns(42)
                .returns(2147483647)
                .returns(2147483647)
                .returns(2147483647)
                .returns(-2147483648)
                .returns(-2147483648)
                .returns(-2147483648)
                .check();
    }

    @SuppressWarnings("ThrowableNotThrown")
    @Test
    void explicitCastOfSourceTableOnInsertWithOverflow() {
        sql("INSERT INTO src VALUES "
                + "(1, 127, 32767, 2147483648, 2147483648, 2.14748365E9, 2147483648, '2147483648'),"
                + "(2, 127.0, 32767.0, 2147483648.0, 2147483648.0, 2.14748365E9, 2147483648.0, '2147483648.0'),"
                + "(3, 127.1, 32767.1, 2147483648.1, 2147483648.1, 2.14748365E9, 2147483648.1, '2147483648.1'),"
                + "(4, -128, -32768, -2147483649, -2147483649, -2.14748378E9, -2147483649, '-2147483649'),"
                + "(5, -128.0, -32768.0, -2147483649.0, -2147483649.0, -2.14748378E9, -2147483649.0, '-2147483649.0'),"
                + "(6, -128.1, -32768.1, -2147483649.1, -2147483649.1, -2.14748378E9, -2147483649.1, '-2147483649.1')"
        );

        for (int idx : new int[] {1, 2, 3, 4, 5, 6}) {
            for (String column : new String[] {"bi", "dec", "r", "d", "s"}) {
                SqlTestUtils.assertThrowsSqlException(
                        Sql.RUNTIME_ERR,
                        "INTEGER out of range",
                        () -> sql(format("INSERT INTO test SELECT CAST({} as INTEGER) FROM src WHERE id = {}", column, idx))
                );
            }
        }
    }

    @Test
    void explicitCastOfSourceTableOnSelect() {
        sql("INSERT INTO src VALUES "
                + "(0, NULL, NULL, NULL, NULL, NULL, NULL, NULL),"
                + "(1, 42, 42, 42, 42, 42, 42, '42'),"
                + "(2, 127, 32767, 2147483647, 2147483647, 2.14748358E9, 2147483647, '2147483647'),"
                + "(3, 127.1, 32767.1, 2147483647.1, 2147483647.1, 2.14748358E9, 2147483647.1, '2147483647.1'),"
                + "(4, 127.9, 32767.9, 2147483647.9, 2147483647.9, 2.14748358E9, 2147483647.9, '2147483647.9'),"
                + "(5, -128, -32768, -2147483648, -2147483648, -2.14748358E9, -2147483648, '-2147483648'),"
                + "(6, -128.1, -32768.1, -2147483648.1, -2147483648.1, -2.14748358E9, -2147483648.1, '-2147483648.1'),"
                + "(7, -128.9, -32768.9, -2147483648.9, -2147483648.9, -2.14748358E9, -2147483648.9, '-2147483648.9')"
        );

        assertQuery("SELECT CAST(ti as INTEGER) FROM src")
                .returns(NULL_AS_VARARG)
                .returns(42)
                .returns(127)
                .returns(127)
                .returns(127)
                .returns(-128)
                .returns(-128)
                .returns(-128)
                .check();
        assertQuery("SELECT CAST(si as INTEGER) FROM src")
                .returns(NULL_AS_VARARG)
                .returns(42)
                .returns(32767)
                .returns(32767)
                .returns(32767)
                .returns(-32768)
                .returns(-32768)
                .returns(-32768)
                .check();
        assertQuery("SELECT CAST(bi as INTEGER) FROM src")
                .returns(NULL_AS_VARARG)
                .returns(42)
                .returns(2147483647)
                .returns(2147483647)
                .returns(2147483647)
                .returns(-2147483648)
                .returns(-2147483648)
                .returns(-2147483648)
                .check();
        assertQuery("SELECT CAST(dec as INTEGER) FROM src")
                .returns(NULL_AS_VARARG)
                .returns(42)
                .returns(2147483647)
                .returns(2147483647)
                .returns(2147483647)
                .returns(-2147483648)
                .returns(-2147483648)
                .returns(-2147483648)
                .check();
        assertQuery("SELECT CAST(r as INTEGER) FROM src")
                .returns(NULL_AS_VARARG)
                .returns(42)
                .returns(2147483520)
                .returns(2147483520)
                .returns(2147483520)
                .returns(-2147483520)
                .returns(-2147483520)
                .returns(-2147483520)
                .check();
        assertQuery("SELECT CAST(d as INTEGER) FROM src")
                .returns(NULL_AS_VARARG)
                .returns(42)
                .returns(2147483647)
                .returns(2147483647)
                .returns(2147483647)
                .returns(-2147483648)
                .returns(-2147483648)
                .returns(-2147483648)
                .check();
        assertQuery("SELECT CAST(s as INTEGER) FROM src")
                .returns(NULL_AS_VARARG)
                .returns(42)
                .returns(2147483647)
                .returns(2147483647)
                .returns(2147483647)
                .returns(-2147483648)
                .returns(-2147483648)
                .returns(-2147483648)
                .check();
    }

    @SuppressWarnings("ThrowableNotThrown")
    @Test
    void explicitCastOfSourceTableOnSelectWithOverflow() {
        sql("INSERT INTO src VALUES "
                + "(1, 127, 32767, 2147483648, 2147483648, 2.14748365E9, 2147483648, '2147483648'),"
                + "(2, 127.0, 32767.0, 2147483648.0, 2147483648.0, 2.14748365E9, 2147483648.0, '2147483648.0'),"
                + "(3, 127.1, 32767.1, 2147483648.1, 2147483648.1, 2.14748365E9, 2147483648.1, '2147483648.1'),"
                + "(4, -128, -32768, -2147483649, -2147483649, -2.14748378E9, -2147483649, '-2147483649'),"
                + "(5, -128.0, -32768.0, -2147483649.0, -2147483649.0, -2.14748378E9, -2147483649.0, '-2147483649.0'),"
                + "(6, -128.1, -32768.1, -2147483649.1, -2147483649.1, -2.14748378E9, -2147483649.1, '-2147483649.1')"
        );

        for (int idx : new int[] {1, 2, 3, 4, 5, 6}) {
            for (String column : new String[] {"bi", "dec", "r", "d", "s"}) {
                SqlTestUtils.assertThrowsSqlException(
                        Sql.RUNTIME_ERR,
                        "INTEGER out of range",
                        () -> sql(format("SELECT CAST({} as INTEGER) FROM src WHERE id = {}", column, idx))
                );
            }
        }
    }

    private static Stream<Arguments> literalsWithExpectedResult() {
        return Stream.of(
                Arguments.of("NULL", null),
                Arguments.of("42", 42),
                Arguments.of("2147483647", 2147483647),
                Arguments.of("-2147483648", -2147483648),
                Arguments.of("decimal '42'", 42),
                Arguments.of("decimal '2147483647'", 2147483647),
                Arguments.of("decimal '2147483647.1'", 2147483647),
                Arguments.of("decimal '2147483647.9'", 2147483647),
                Arguments.of("decimal '-2147483648'", -2147483648),
                Arguments.of("decimal '-2147483648.1'", -2147483648),
                Arguments.of("decimal '-2147483648.9'", -2147483648),
                Arguments.of("42.0", 42),
                Arguments.of("2147483647.0", 2147483647),
                Arguments.of("2147483647.1", 2147483647),
                Arguments.of("2147483647.9", 2147483647),
                Arguments.of("-2147483648.0", -2147483648),
                Arguments.of("-2147483648.1", -2147483648),
                Arguments.of("-2147483648.9", -2147483648),
                Arguments.of("'42'", 42),
                Arguments.of("'2147483647'", 2147483647),
                Arguments.of("'2147483647.1'", 2147483647),
                Arguments.of("'2147483647.9'", 2147483647),
                Arguments.of("'-2147483648'", -2147483648),
                Arguments.of("'-2147483648.1'", -2147483648),
                Arguments.of("'-2147483648.9'", -2147483648)
        );
    }

    private static Stream<Arguments> valuesWithExpectedResult() {
        return Stream.of(
                Arguments.of(null, null),
                Arguments.of((byte) 42, 42),
                Arguments.of((byte) 127, 127),
                Arguments.of((byte) -128, -128),
                Arguments.of((short) 42, 42),
                Arguments.of((short) 32767, 32767),
                Arguments.of((short) -32768, -32768),
                Arguments.of((long) 42, 42),
                Arguments.of((long) 2147483647, 2147483647),
                Arguments.of((long) -2147483648, -2147483648),
                Arguments.of(new BigDecimal("42"), 42),
                Arguments.of(new BigDecimal("2147483647"), 2147483647),
                Arguments.of(new BigDecimal("2147483647.1"), 2147483647),
                Arguments.of(new BigDecimal("2147483647.9"), 2147483647),
                Arguments.of(new BigDecimal("-2147483648"), -2147483648),
                Arguments.of(new BigDecimal("-2147483648.1"), -2147483648),
                Arguments.of(new BigDecimal("-2147483648.9"), -2147483648),
                Arguments.of(42.0D, 42),
                Arguments.of(2147483647.0D, 2147483647),
                Arguments.of(2147483647.1D, 2147483647),
                Arguments.of(2147483647.9D, 2147483647),
                Arguments.of(-2147483648.0D, -2147483648),
                Arguments.of(-2147483648.1D, -2147483648),
                Arguments.of(-2147483648.9D, -2147483648),
                Arguments.of(42.0F, 42),
                Arguments.of(2.14748358E9F, 2147483520),
                Arguments.of(-2.14748358E9F, -2147483520),
                Arguments.of("42", 42),
                Arguments.of("2147483647", 2147483647),
                Arguments.of("2147483647.1", 2147483647),
                Arguments.of("2147483647.9", 2147483647),
                Arguments.of("-2147483648", -2147483648),
                Arguments.of("-2147483648.1", -2147483648),
                Arguments.of("-2147483648.9", -2147483648)
        );
    }

    private static Stream<Arguments> literalsWithOverflow() {
        return Stream.of(
                "2147483648", "2147483648.0", "2147483648.1", "-2147483649", "-2147483649.0",
                "-2147483649.1", "decimal '2147483648'", "decimal '2147483648.1'", "decimal '-2147483649'",
                "decimal '-2147483649.1'", "'2147483648'", "'2147483648.0'", "'2147483648.1'", "'-2147483649'",
                "'-2147483649.1'",
                // Literals that don't fit into BIGINT
                "9223372036854775808", "9223372036854775808.0", "9223372036854775808.1",
                "-9223372036854775809", "-9223372036854775809.0", "-9223372036854775809.1",
                "decimal '9223372036854775808'", "decimal '9223372036854775808.1'",
                "decimal '-9223372036854775809'", "decimal '-9223372036854775809.1'", "'9223372036854775808'",
                "'9223372036854775808.0'", "'9223372036854775808.1'", "'-9223372036854775809'",
                "'-9223372036854775809.1'"
        ).map(Arguments::of);
    }

    private static Stream<Arguments> valuesWithOverflow() {
        return Stream.of(
                2147483648L, -2147483649L, 2.14748365E9F, -2.14748378E9F,
                2147483648.0D, 2147483648.1D, -2147483650.0D, -2147483649.1D,
                new BigDecimal("2147483648"), new BigDecimal("2147483648.1"),
                new BigDecimal("-2147483649"), new BigDecimal("-2147483649.1"), "2147483648",
                "2147483648.0", "2147483648.1", "-2147483649", "-2147483649.1"
        ).map(Arguments::of);
    }
}
