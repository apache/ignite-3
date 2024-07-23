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
 * Set of tests to ensure correctness of CAST expression to BIGINT for
 * type pairs supported by cast specification.
 */
@WithSystemProperty(key = "IMPLICIT_PK_ENABLED", value = "true")
public class ItCastToBigintTest extends BaseSqlIntegrationTest {
    @BeforeAll
    static void createTable() {
        sql("CREATE TABLE test (val BIGINT)");
        sql("CREATE TABLE src (id INT PRIMARY KEY, ti TINYINT, si SMALLINT, i INT," 
                + " dec DECIMAL(24, 2), r REAL, d DOUBLE, s VARCHAR(100))");
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
                "BIGINT out of range",
                () -> sql(format("INSERT INTO test VALUES ({})", literal))
        );
    }

    @ParameterizedTest
    @MethodSource("literalsWithExpectedResult")
    void explicitCastOfLiteralsOnInsert(String literal, Object expectedResult) {
        assertQuery(format("INSERT INTO test VALUES (CAST({} as BIGINT))", literal)).check();

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
                "BIGINT out of range",
                () -> sql(format("INSERT INTO test VALUES (CAST({} as BIGINT))", literal))
        );
    }

    @ParameterizedTest
    @MethodSource("literalsWithExpectedResult")
    void explicitCastOfLiteralsOnSelect(String literal, Object expectedResult) {
        assertQuery(format("SELECT CAST({} as BIGINT)", literal))
                .returns(expectedResult)
                .check();
    }

    @SuppressWarnings("ThrowableNotThrown")
    @ParameterizedTest
    @MethodSource("literalsWithOverflow")
    void explicitCastOfLiteralsOnSelectWithOverflow(String literal) {
        SqlTestUtils.assertThrowsSqlException(
                Sql.RUNTIME_ERR,
                "BIGINT out of range",
                () -> sql(format("SELECT CAST({} as BIGINT)", literal))
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

            builder.append("(CAST(").append(literal).append(" as BIGINT))");

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
                "BIGINT out of range",
                () -> sql("INSERT INTO test VALUES (?)", param)
        );
    }

    @ParameterizedTest
    @MethodSource("valuesWithExpectedResult")
    void explicitCastOfDynParamsOnInsert(Object param, Object expectedResult) {
        assertQuery("INSERT INTO test VALUES (CAST(? as BIGINT))")
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
                "BIGINT out of range",
                () -> sql("INSERT INTO test VALUES (CAST(? as BIGINT))", param)
        );
    }

    @ParameterizedTest
    @MethodSource("valuesWithExpectedResult")
    void explicitCastOfDynParamsOnSelect(Object param, Object expectedResult) {
        assertQuery("SELECT CAST(? as BIGINT)")
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
                "BIGINT out of range",
                () -> sql("SELECT CAST(? as BIGINT)", param)
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

            builder.append("(CAST(? as BIGINT))");

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
                + "(1, 42, 42, 42, 42, 42, 42, '42'),"
                + "(2, 127, 32767, 2147483647, 9223372036854775807, 9.223371E18, 9.223372036854775E18, '9223372036854775807'),"
                + "(3, 127.1, 32767.1, 2147483647.1, 9223372036854775807.1, 9.223371E18, 9.223372036854775E18, '9223372036854775807.1'),"
                + "(4, 127.9, 32767.9, 2147483647.9, 9223372036854775807.9, 9.223371E18, 9.223372036854775E18, '9223372036854775807.9'),"
                + "(5, -128, -32768, -2147483648, -9223372036854775808, -9.223371E18, -9.223372036854775E18, '-9223372036854775808'),"
                + "(6, -128.1, -32768.1, -2147483648.1, -9223372036854775808.1, -9.223371E18," 
                + " -9.223372036854775E18, '-9223372036854775808.1'),"
                + "(7, -128.9, -32768.9, -2147483648.9, -9223372036854775808.9, -9.223371E18," 
                + " -9.223372036854775E18, '-9223372036854775808.9')"
        );

        assertQuery("INSERT INTO test SELECT ti FROM src").check();
        assertQuery("INSERT INTO test SELECT si FROM src").check();
        assertQuery("INSERT INTO test SELECT i FROM src").check();
        assertQuery("INSERT INTO test SELECT dec FROM src").check();
        assertQuery("INSERT INTO test SELECT r FROM src").check();
        assertQuery("INSERT INTO test SELECT d FROM src").check();
        assertQuery("INSERT INTO test SELECT s FROM src").check();

        assertQuery("SELECT * FROM test")
                .returns(42L)
                .returns(127L)
                .returns(127L)
                .returns(127L)
                .returns(-128L)
                .returns(-128L)
                .returns(-128L)
                .returns(42L)
                .returns(32767L)
                .returns(32767L)
                .returns(32767L)
                .returns(-32768L)
                .returns(-32768L)
                .returns(-32768L)
                .returns(42L)
                .returns(2147483647L)
                .returns(2147483647L)
                .returns(2147483647L)
                .returns(-2147483648L)
                .returns(-2147483648L)
                .returns(-2147483648L)
                .returns(42L)
                .returns(9223372036854775807L)
                .returns(9223372036854775807L)
                .returns(9223372036854775807L)
                .returns(-9223372036854775808L)
                .returns(-9223372036854775808L)
                .returns(-9223372036854775808L)
                .returns(42L)
                .returns(9223370937343148032L)
                .returns(9223370937343148032L)
                .returns(9223370937343148032L)
                .returns(-9223370937343148032L)
                .returns(-9223370937343148032L)
                .returns(-9223370937343148032L)
                .returns(42L)
                .returns(9223372036854774784L)
                .returns(9223372036854774784L)
                .returns(9223372036854774784L)
                .returns(-9223372036854774784L)
                .returns(-9223372036854774784L)
                .returns(-9223372036854774784L)
                .returns(42L)
                .returns(9223372036854775807L)
                .returns(9223372036854775807L)
                .returns(9223372036854775807L)
                .returns(-9223372036854775808L)
                .returns(-9223372036854775808L)
                .returns(-9223372036854775808L)
                .check();
    }

    @SuppressWarnings("ThrowableNotThrown")
    @Test
    void implicitCastOfSourceTableOnInsertWithOverflow() {
        sql("INSERT INTO src VALUES "
                + "(1, 127, 32767, 2147483647, 9223372036854775808, 9.223372E18, 9.223372036854776E18, '9223372036854775808'),"
                + "(2, 127.0, 32767.0, 2147483647.0, 9223372036854775808.0, 9.223372E18, 9.223372036854776E18, '9223372036854775808.0'),"
                + "(3, 127.1, 32767.1, 2147483647.1, 9223372036854775808.1, 9.223372E18, 9.223372036854776E18, '9223372036854775808.1'),"
                + "(4, -128, -32768, -2147483648, -9223372036854775809, -9.223372E18, -9.223372036854776E18, '-9223372036854775809'),"
                + "(5, -128.0, -32768.0, -2147483648.0, -9223372036854775809.0, -9.223372E18," 
                + " -9.223372036854776E18, '-9223372036854775809.0'),"
                + "(6, -128.1, -32768.1, -2147483648.1, -9223372036854775809.1, -9.223372E18," 
                + " -9.223372036854776E18, '-9223372036854775809.1')"
        );

        for (int idx : new int[] {1, 2, 3, 4, 5, 6}) {
            for (String column : new String[] {"dec", "r", "d", "s"}) {
                SqlTestUtils.assertThrowsSqlException(
                        Sql.RUNTIME_ERR,
                        "BIGINT out of range",
                        () -> sql(format("INSERT INTO test SELECT {} FROM src WHERE id = {}", column, idx))
                );
            }
        }
    }

    @Test
    void explicitCastOfSourceTableOnInsert() {
        sql("INSERT INTO src VALUES "
                + "(1, 42, 42, 42, 42, 42, 42, '42'),"
                + "(2, 127, 32767, 2147483647, 9223372036854775807, 9.223371E18, 9.223372036854775E18, '9223372036854775807'),"
                + "(3, 127.1, 32767.1, 2147483647.1, 9223372036854775807.1, 9.223371E18, 9.223372036854775E18, '9223372036854775807.1'),"
                + "(4, 127.9, 32767.9, 2147483647.9, 9223372036854775807.9, 9.223371E18, 9.223372036854775E18, '9223372036854775807.9'),"
                + "(5, -128, -32768, -2147483648, -9223372036854775808, -9.223371E18, -9.223372036854775E18, '-9223372036854775808'),"
                + "(6, -128.1, -32768.1, -2147483648.1, -9223372036854775808.1, -9.223371E18," 
                + " -9.223372036854775E18, '-9223372036854775808.1'),"
                + "(7, -128.9, -32768.9, -2147483648.9, -9223372036854775808.9, -9.223371E18," 
                + " -9.223372036854775E18, '-9223372036854775808.9')"
        );

        assertQuery("INSERT INTO test SELECT CAST(ti as BIGINT) FROM src").check();
        assertQuery("INSERT INTO test SELECT CAST(si as BIGINT) FROM src").check();
        assertQuery("INSERT INTO test SELECT CAST(i as BIGINT) FROM src").check();
        assertQuery("INSERT INTO test SELECT CAST(dec as BIGINT) FROM src").check();
        assertQuery("INSERT INTO test SELECT CAST(r as BIGINT) FROM src").check();
        assertQuery("INSERT INTO test SELECT CAST(d as BIGINT) FROM src").check();
        assertQuery("INSERT INTO test SELECT CAST(s as BIGINT) FROM src").check();

        assertQuery("SELECT * FROM test")
                .returns(42L)
                .returns(127L)
                .returns(127L)
                .returns(127L)
                .returns(-128L)
                .returns(-128L)
                .returns(-128L)
                .returns(42L)
                .returns(32767L)
                .returns(32767L)
                .returns(32767L)
                .returns(-32768L)
                .returns(-32768L)
                .returns(-32768L)
                .returns(42L)
                .returns(2147483647L)
                .returns(2147483647L)
                .returns(2147483647L)
                .returns(-2147483648L)
                .returns(-2147483648L)
                .returns(-2147483648L)
                .returns(42L)
                .returns(9223372036854775807L)
                .returns(9223372036854775807L)
                .returns(9223372036854775807L)
                .returns(-9223372036854775808L)
                .returns(-9223372036854775808L)
                .returns(-9223372036854775808L)
                .returns(42L)
                .returns(9223370937343148032L)
                .returns(9223370937343148032L)
                .returns(9223370937343148032L)
                .returns(-9223370937343148032L)
                .returns(-9223370937343148032L)
                .returns(-9223370937343148032L)
                .returns(42L)
                .returns(9223372036854774784L)
                .returns(9223372036854774784L)
                .returns(9223372036854774784L)
                .returns(-9223372036854774784L)
                .returns(-9223372036854774784L)
                .returns(-9223372036854774784L)
                .returns(42L)
                .returns(9223372036854775807L)
                .returns(9223372036854775807L)
                .returns(9223372036854775807L)
                .returns(-9223372036854775808L)
                .returns(-9223372036854775808L)
                .returns(-9223372036854775808L)
                .check();
    }

    @SuppressWarnings("ThrowableNotThrown")
    @Test
    void explicitCastOfSourceTableOnInsertWithOverflow() {
        sql("INSERT INTO src VALUES "
                + "(1, 127, 32767, 2147483647, 9223372036854775808, 9.223372E18, 9.223372036854776E18, '9223372036854775808'),"
                + "(2, 127.0, 32767.0, 2147483647.0, 9223372036854775808.0, 9.223372E18, 9.223372036854776E18, '9223372036854775808.0'),"
                + "(3, 127.1, 32767.1, 2147483647.1, 9223372036854775808.1, 9.223372E18, 9.223372036854776E18, '9223372036854775808.1'),"
                + "(4, -128, -32768, -2147483648, -9223372036854775809, -9.223372E18, -9.223372036854776E18, '-9223372036854775809'),"
                + "(5, -128.0, -32768.0, -2147483648.0, -9223372036854775809.0, -9.223372E18," 
                + " -9.223372036854776E18, '-9223372036854775809.0'),"
                + "(6, -128.1, -32768.1, -2147483648.1, -9223372036854775809.1, -9.223372E18," 
                + " -9.223372036854776E18, '-9223372036854775809.1')"
        );

        for (int idx : new int[] {1, 2, 3, 4, 5, 6}) {
            for (String column : new String[] {"dec", "r", "d", "s"}) {
                SqlTestUtils.assertThrowsSqlException(
                        Sql.RUNTIME_ERR,
                        "BIGINT out of range",
                        () -> sql(format("INSERT INTO test SELECT CAST({} as BIGINT) FROM src WHERE id = {}", column, idx))
                );
            }
        }
    }

    @Test
    void explicitCastOfSourceTableOnSelect() {
        sql("INSERT INTO src VALUES "
                + "(1, 42, 42, 42, 42, 42, 42, '42'),"
                + "(2, 127, 32767, 2147483647, 9223372036854775807, 9.223371E18, 9.223372036854775E18, '9223372036854775807'),"
                + "(3, 127.1, 32767.1, 2147483647.1, 9223372036854775807.1, 9.223371E18, 9.223372036854775E18, '9223372036854775807.1'),"
                + "(4, 127.9, 32767.9, 2147483647.9, 9223372036854775807.9, 9.223371E18, 9.223372036854775E18, '9223372036854775807.9'),"
                + "(5, -128, -32768, -2147483648, -9223372036854775808, -9.223371E18, -9.223372036854775E18, '-9223372036854775808'),"
                + "(6, -128.1, -32768.1, -2147483648.1, -9223372036854775808.1, -9.223371E18," 
                + " -9.223372036854775E18, '-9223372036854775808.1'),"
                + "(7, -128.9, -32768.9, -2147483648.9, -9223372036854775808.9, -9.223371E18," 
                + " -9.223372036854775E18, '-9223372036854775808.9')"
        );

        assertQuery("SELECT CAST(ti as BIGINT) FROM src")
                .returns(42L)
                .returns(127L)
                .returns(127L)
                .returns(127L)
                .returns(-128L)
                .returns(-128L)
                .returns(-128L)
                .check();
        assertQuery("SELECT CAST(si as BIGINT) FROM src")
                .returns(42L)
                .returns(32767L)
                .returns(32767L)
                .returns(32767L)
                .returns(-32768L)
                .returns(-32768L)
                .returns(-32768L)
                .check();
        assertQuery("SELECT CAST(i as BIGINT) FROM src")
                .returns(42L)
                .returns(2147483647L)
                .returns(2147483647L)
                .returns(2147483647L)
                .returns(-2147483648L)
                .returns(-2147483648L)
                .returns(-2147483648L)
                .check();
        assertQuery("SELECT CAST(dec as BIGINT) FROM src")
                .returns(42L)
                .returns(9223372036854775807L)
                .returns(9223372036854775807L)
                .returns(9223372036854775807L)
                .returns(-9223372036854775808L)
                .returns(-9223372036854775808L)
                .returns(-9223372036854775808L)
                .check();
        assertQuery("SELECT CAST(r as BIGINT) FROM src")
                .returns(42L)
                .returns(9223370937343148032L)
                .returns(9223370937343148032L)
                .returns(9223370937343148032L)
                .returns(-9223370937343148032L)
                .returns(-9223370937343148032L)
                .returns(-9223370937343148032L)
                .check();
        assertQuery("SELECT CAST(d as BIGINT) FROM src")
                .returns(42L)
                .returns(9223372036854774784L)
                .returns(9223372036854774784L)
                .returns(9223372036854774784L)
                .returns(-9223372036854774784L)
                .returns(-9223372036854774784L)
                .returns(-9223372036854774784L)
                .check();
        assertQuery("SELECT CAST(s as BIGINT) FROM src")
                .returns(42L)
                .returns(9223372036854775807L)
                .returns(9223372036854775807L)
                .returns(9223372036854775807L)
                .returns(-9223372036854775808L)
                .returns(-9223372036854775808L)
                .returns(-9223372036854775808L)
                .check();
    }

    @SuppressWarnings("ThrowableNotThrown")
    @Test
    void explicitCastOfSourceTableOnSelectWithOverflow() {
        sql("INSERT INTO src VALUES "
                + "(1, 127, 32767, 2147483647, 9223372036854775808, 9.223372E18, 9.223372036854776E18, '9223372036854775808'),"
                + "(2, 127.0, 32767.0, 2147483647.0, 9223372036854775808.0, 9.223372E18, 9.223372036854776E18, '9223372036854775808.0'),"
                + "(3, 127.1, 32767.1, 2147483647.1, 9223372036854775808.1, 9.223372E18, 9.223372036854776E18, '9223372036854775808.1'),"
                + "(4, -128, -32768, -2147483648, -9223372036854775809, -9.223372E18, -9.223372036854776E18, '-9223372036854775809'),"
                + "(5, -128.0, -32768.0, -2147483648.0, -9223372036854775809.0, -9.223372E18," 
                + " -9.223372036854776E18, '-9223372036854775809.0'),"
                + "(6, -128.1, -32768.1, -2147483648.1, -9223372036854775809.1, -9.223372E18," 
                + " -9.223372036854776E18, '-9223372036854775809.1')"
        );

        for (int idx : new int[] {1, 2, 3, 4, 5, 6}) {
            for (String column : new String[] {"dec", "r", "d", "s"}) {
                SqlTestUtils.assertThrowsSqlException(
                        Sql.RUNTIME_ERR,
                        "BIGINT out of range",
                        () -> sql(format("SELECT CAST({} as BIGINT) FROM src WHERE id = {}", column, idx))
                );
            }
        }
    }

    private static Stream<Arguments> literalsWithExpectedResult() {
        return Stream.of(
                Arguments.of("42", 42L),
                Arguments.of("9223372036854775807", 9223372036854775807L),
                Arguments.of("-9223372036854775808", -9223372036854775808L),
                Arguments.of("decimal '42'", 42L),
                Arguments.of("decimal '9223372036854775807'", 9223372036854775807L),
                Arguments.of("decimal '9223372036854775807.1'", 9223372036854775807L),
                Arguments.of("decimal '9223372036854775807.9'", 9223372036854775807L),
                Arguments.of("decimal '-9223372036854775808'", -9223372036854775808L),
                Arguments.of("decimal '-9223372036854775808.1'", -9223372036854775808L),
                Arguments.of("decimal '-9223372036854775808.9'", -9223372036854775808L),
                Arguments.of("42.0", 42L),
                Arguments.of("9223372036854775807.0", 9223372036854775807L),
                Arguments.of("9223372036854775807.1", 9223372036854775807L),
                Arguments.of("9223372036854775807.9", 9223372036854775807L),
                Arguments.of("-9223372036854775808.0", -9223372036854775808L),
                Arguments.of("-9223372036854775808.1", -9223372036854775808L),
                Arguments.of("-9223372036854775808.9", -9223372036854775808L),
                Arguments.of("'42'", 42L),
                Arguments.of("'9223372036854775807'", 9223372036854775807L),
                Arguments.of("'9223372036854775807.1'", 9223372036854775807L),
                Arguments.of("'9223372036854775807.9'", 9223372036854775807L),
                Arguments.of("'-9223372036854775808'", -9223372036854775808L),
                Arguments.of("'-9223372036854775808.1'", -9223372036854775808L),
                Arguments.of("'-9223372036854775808.9'", -9223372036854775808L)
        );
    }

    private static Stream<Arguments> valuesWithExpectedResult() {
        return Stream.of(
                Arguments.of((byte) 42, 42L),
                Arguments.of((byte) 127, 127L),
                Arguments.of((byte) -128, -128L),
                Arguments.of((short) 42, 42L),
                Arguments.of((short) 32767, 32767L),
                Arguments.of((short) -32768, -32768L),
                Arguments.of(42, 42L),
                Arguments.of(2147483647, 2147483647L),
                Arguments.of(-2147483648, -2147483648L),
                Arguments.of(new BigDecimal("42"), 42L),
                Arguments.of(new BigDecimal("9223372036854775807"), 9223372036854775807L),
                Arguments.of(new BigDecimal("9223372036854775807.1"), 9223372036854775807L),
                Arguments.of(new BigDecimal("9223372036854775807.9"), 9223372036854775807L),
                Arguments.of(new BigDecimal("-9223372036854775808"), -9223372036854775808L),
                Arguments.of(new BigDecimal("-9223372036854775808.1"), -9223372036854775808L),
                Arguments.of(new BigDecimal("-9223372036854775808.9"), -9223372036854775808L),
                Arguments.of(42.0D, 42L),
                Arguments.of(9.223372036854775E18D, 9223372036854774784L),
                Arguments.of(-9.223372036854775E18D, -9223372036854774784L),
                Arguments.of(42.0F, 42L),
                Arguments.of(9.223371E18F, 9223370937343148032L),
                Arguments.of(-9.223371E18F, -9223370937343148032L),
                Arguments.of("42", 42L),
                Arguments.of("9223372036854775807", 9223372036854775807L),
                Arguments.of("9223372036854775807.1", 9223372036854775807L),
                Arguments.of("9223372036854775807.9", 9223372036854775807L),
                Arguments.of("-9223372036854775808", -9223372036854775808L),
                Arguments.of("-9223372036854775808.1", -9223372036854775808L),
                Arguments.of("-9223372036854775808.9", -9223372036854775808L)
        );
    }

    private static Stream<Arguments> literalsWithOverflow() {
        return Stream.of(
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
                9.223372E18F, -9.223372E18F, 9.223372036854776E18D, -9.223372036854776E18D,
                new BigDecimal("9223372036854775808"), new BigDecimal("9223372036854775808.1"),
                new BigDecimal("-9223372036854775809"), new BigDecimal("-9223372036854775809.1"),
                "9223372036854775808", "9223372036854775808.0", "9223372036854775808.1",
                "-9223372036854775809", "-9223372036854775809.1"
        ).map(Arguments::of);
    }
}
