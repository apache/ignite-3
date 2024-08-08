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
 * Set of tests to ensure correctness of CAST expression to SMALLINT for
 * type pairs supported by cast specification.
 */
@WithSystemProperty(key = "IMPLICIT_PK_ENABLED", value = "true")
public class ItCastToSmallintTest extends BaseSqlIntegrationTest {
    @BeforeAll
    static void createTable() {
        sql("CREATE TABLE test (val SMALLINT)");
        sql("CREATE TABLE src (id INT PRIMARY KEY, ti TINYINT, i INT, bi BIGINT," 
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
                "SMALLINT out of range",
                () -> sql(format("INSERT INTO test VALUES ({})", literal))
        );
    }

    @ParameterizedTest
    @MethodSource("literalsWithExpectedResult")
    void explicitCastOfLiteralsOnInsert(String literal, Object expectedResult) {
        assertQuery(format("INSERT INTO test VALUES (CAST({} as SMALLINT))", literal)).check();

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
                "SMALLINT out of range",
                () -> sql(format("INSERT INTO test VALUES (CAST({} as SMALLINT))", literal))
        );
    }

    @ParameterizedTest
    @MethodSource("literalsWithExpectedResult")
    void explicitCastOfLiteralsOnSelect(String literal, Object expectedResult) {
        assertQuery(format("SELECT CAST({} as SMALLINT)", literal))
                .returns(expectedResult)
                .check();
    }

    @SuppressWarnings("ThrowableNotThrown")
    @ParameterizedTest
    @MethodSource("literalsWithOverflow")
    void explicitCastOfLiteralsOnSelectWithOverflow(String literal) {
        SqlTestUtils.assertThrowsSqlException(
                Sql.RUNTIME_ERR,
                "SMALLINT out of range",
                () -> sql(format("SELECT CAST({} as SMALLINT)", literal))
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

            builder.append("(CAST(").append(literal).append(" as SMALLINT))");

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
                "SMALLINT out of range",
                () -> sql("INSERT INTO test VALUES (?)", param)
        );
    }

    @ParameterizedTest
    @MethodSource("valuesWithExpectedResult")
    void explicitCastOfDynParamsOnInsert(Object param, Object expectedResult) {
        assertQuery("INSERT INTO test VALUES (CAST(? as SMALLINT))")
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
                "SMALLINT out of range",
                () -> sql("INSERT INTO test VALUES (CAST(? as SMALLINT))", param)
        );
    }

    @ParameterizedTest
    @MethodSource("valuesWithExpectedResult")
    void explicitCastOfDynParamsOnSelect(Object param, Object expectedResult) {
        assertQuery("SELECT CAST(? as SMALLINT)")
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
                "SMALLINT out of range",
                () -> sql("SELECT CAST(? as SMALLINT)", param)
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

            builder.append("(CAST(? as SMALLINT))");

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
                + "(2, 127, 32767, 32767, 32767, 32767, 32767, '32767'),"
                + "(3, 127.1, 32767.1, 32767.1, 32767.1, 32767.1, 32767.1, '32767.1'),"
                + "(4, 127.9, 32767.9, 32767.9, 32767.9, 32767.9, 32767.9, '32767.9'),"
                + "(5, -128, -32768, -32768, -32768, -32768, -32768, '-32768'),"
                + "(6, -128.1, -32768.1, -32768.1, -32768.1, -32768.1, -32768.1, '-32768.1'),"
                + "(7, -128.9, -32768.9, -32768.9, -32768.9, -32768.9, -32768.9, '-32768.9')"
        );

        assertQuery("INSERT INTO test SELECT ti FROM src").check();
        assertQuery("INSERT INTO test SELECT i FROM src").check();
        assertQuery("INSERT INTO test SELECT bi FROM src").check();
        assertQuery("INSERT INTO test SELECT dec FROM src").check();
        assertQuery("INSERT INTO test SELECT r FROM src").check();
        assertQuery("INSERT INTO test SELECT d FROM src").check();
        assertQuery("INSERT INTO test SELECT s FROM src").check();

        assertQuery("SELECT * FROM test")
                .returns(NULL_AS_VARARG)
                .returns((short) 42)
                .returns((short) 127)
                .returns((short) 127)
                .returns((short) 127)
                .returns((short) -128)
                .returns((short) -128)
                .returns((short) -128)
                .returns(NULL_AS_VARARG)
                .returns((short) 42)
                .returns((short) 32767)
                .returns((short) 32767)
                .returns((short) 32767)
                .returns((short) -32768)
                .returns((short) -32768)
                .returns((short) -32768)
                .returns(NULL_AS_VARARG)
                .returns((short) 42)
                .returns((short) 32767)
                .returns((short) 32767)
                .returns((short) 32767)
                .returns((short) -32768)
                .returns((short) -32768)
                .returns((short) -32768)
                .returns(NULL_AS_VARARG)
                .returns((short) 42)
                .returns((short) 32767)
                .returns((short) 32767)
                .returns((short) 32767)
                .returns((short) -32768)
                .returns((short) -32768)
                .returns((short) -32768)
                .returns(NULL_AS_VARARG)
                .returns((short) 42)
                .returns((short) 32767)
                .returns((short) 32767)
                .returns((short) 32767)
                .returns((short) -32768)
                .returns((short) -32768)
                .returns((short) -32768)
                .returns(NULL_AS_VARARG)
                .returns((short) 42)
                .returns((short) 32767)
                .returns((short) 32767)
                .returns((short) 32767)
                .returns((short) -32768)
                .returns((short) -32768)
                .returns((short) -32768)
                .returns(NULL_AS_VARARG)
                .returns((short) 42)
                .returns((short) 32767)
                .returns((short) 32767)
                .returns((short) 32767)
                .returns((short) -32768)
                .returns((short) -32768)
                .returns((short) -32768)
                .check();
    }

    @SuppressWarnings("ThrowableNotThrown")
    @Test
    void implicitCastOfSourceTableOnInsertWithOverflow() {
        sql("INSERT INTO src VALUES "
                + "(1, 127, 32768, 32768, 32768, 32768, 32768, '32768'),"
                + "(2, 127.0, 32768.0, 32768.0, 32768.0, 32768.0, 32768.0, '32768.0'),"
                + "(3, 127.1, 32768.1, 32768.1, 32768.1, 32768.1, 32768.1, '32768.1'),"
                + "(4, -128, -32769, -32769, -32769, -32769, -32769, '-32769'),"
                + "(5, -128.0, -32769.0, -32769.0, -32769.0, -32769.0, -32769.0, '-32769.0'),"
                + "(6, -128.1, -32769.1, -32769.1, -32769.1, -32769.1, -32769.1, '-32769.1')"
        );

        for (int idx : new int[] {1, 2, 3, 4, 5, 6}) {
            for (String column : new String[] {"i", "bi", "dec", "r", "d", "s"}) {
                SqlTestUtils.assertThrowsSqlException(
                        Sql.RUNTIME_ERR,
                        "SMALLINT out of range",
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
                + "(2, 127, 32767, 32767, 32767, 32767, 32767, '32767'),"
                + "(3, 127.1, 32767.1, 32767.1, 32767.1, 32767.1, 32767.1, '32767.1'),"
                + "(4, 127.9, 32767.9, 32767.9, 32767.9, 32767.9, 32767.9, '32767.9'),"
                + "(5, -128, -32768, -32768, -32768, -32768, -32768, '-32768'),"
                + "(6, -128.1, -32768.1, -32768.1, -32768.1, -32768.1, -32768.1, '-32768.1'),"
                + "(7, -128.9, -32768.9, -32768.9, -32768.9, -32768.9, -32768.9, '-32768.9')"
        );

        assertQuery("INSERT INTO test SELECT CAST(ti as SMALLINT) FROM src").check();
        assertQuery("INSERT INTO test SELECT CAST(i as SMALLINT) FROM src").check();
        assertQuery("INSERT INTO test SELECT CAST(bi as SMALLINT) FROM src").check();
        assertQuery("INSERT INTO test SELECT CAST(dec as SMALLINT) FROM src").check();
        assertQuery("INSERT INTO test SELECT CAST(r as SMALLINT) FROM src").check();
        assertQuery("INSERT INTO test SELECT CAST(d as SMALLINT) FROM src").check();
        assertQuery("INSERT INTO test SELECT CAST(s as SMALLINT) FROM src").check();

        assertQuery("SELECT * FROM test")
                .returns(NULL_AS_VARARG)
                .returns((short) 42)
                .returns((short) 127)
                .returns((short) 127)
                .returns((short) 127)
                .returns((short) -128)
                .returns((short) -128)
                .returns((short) -128)
                .returns(NULL_AS_VARARG)
                .returns((short) 42)
                .returns((short) 32767)
                .returns((short) 32767)
                .returns((short) 32767)
                .returns((short) -32768)
                .returns((short) -32768)
                .returns((short) -32768)
                .returns(NULL_AS_VARARG)
                .returns((short) 42)
                .returns((short) 32767)
                .returns((short) 32767)
                .returns((short) 32767)
                .returns((short) -32768)
                .returns((short) -32768)
                .returns((short) -32768)
                .returns(NULL_AS_VARARG)
                .returns((short) 42)
                .returns((short) 32767)
                .returns((short) 32767)
                .returns((short) 32767)
                .returns((short) -32768)
                .returns((short) -32768)
                .returns((short) -32768)
                .returns(NULL_AS_VARARG)
                .returns((short) 42)
                .returns((short) 32767)
                .returns((short) 32767)
                .returns((short) 32767)
                .returns((short) -32768)
                .returns((short) -32768)
                .returns((short) -32768)
                .returns(NULL_AS_VARARG)
                .returns((short) 42)
                .returns((short) 32767)
                .returns((short) 32767)
                .returns((short) 32767)
                .returns((short) -32768)
                .returns((short) -32768)
                .returns((short) -32768)
                .returns(NULL_AS_VARARG)
                .returns((short) 42)
                .returns((short) 32767)
                .returns((short) 32767)
                .returns((short) 32767)
                .returns((short) -32768)
                .returns((short) -32768)
                .returns((short) -32768)
                .check();
    }

    @SuppressWarnings("ThrowableNotThrown")
    @Test
    void explicitCastOfSourceTableOnInsertWithOverflow() {
        sql("INSERT INTO src VALUES "
                + "(1, 127, 32768, 32768, 32768, 32768, 32768, '32768'),"
                + "(2, 127.0, 32768.0, 32768.0, 32768.0, 32768.0, 32768.0, '32768.0'),"
                + "(3, 127.1, 32768.1, 32768.1, 32768.1, 32768.1, 32768.1, '32768.1'),"
                + "(4, -128, -32769, -32769, -32769, -32769, -32769, '-32769'),"
                + "(5, -128.0, -32769.0, -32769.0, -32769.0, -32769.0, -32769.0, '-32769.0'),"
                + "(6, -128.1, -32769.1, -32769.1, -32769.1, -32769.1, -32769.1, '-32769.1')"
        );

        for (int idx : new int[] {1, 2, 3, 4, 5, 6}) {
            for (String column : new String[] {"i", "bi", "dec", "r", "d", "s"}) {
                SqlTestUtils.assertThrowsSqlException(
                        Sql.RUNTIME_ERR,
                        "SMALLINT out of range",
                        () -> sql(format("INSERT INTO test SELECT CAST({} as SMALLINT) FROM src WHERE id = {}", column, idx))
                );
            }
        }
    }

    @Test
    void explicitCastOfSourceTableOnSelect() {
        sql("INSERT INTO src VALUES "
                + "(0, NULL, NULL, NULL, NULL, NULL, NULL, NULL),"
                + "(1, 42, 42, 42, 42, 42, 42, '42'),"
                + "(2, 127, 32767, 32767, 32767, 32767, 32767, '32767'),"
                + "(3, 127.1, 32767.1, 32767.1, 32767.1, 32767.1, 32767.1, '32767.1'),"
                + "(4, 127.9, 32767.9, 32767.9, 32767.9, 32767.9, 32767.9, '32767.9'),"
                + "(5, -128, -32768, -32768, -32768, -32768, -32768, '-32768'),"
                + "(6, -128.1, -32768.1, -32768.1, -32768.1, -32768.1, -32768.1, '-32768.1'),"
                + "(7, -128.9, -32768.9, -32768.9, -32768.9, -32768.9, -32768.9, '-32768.9')"
        );

        assertQuery("SELECT CAST(ti as SMALLINT) FROM src")
                .returns(NULL_AS_VARARG)
                .returns((short) 42)
                .returns((short) 127)
                .returns((short) 127)
                .returns((short) 127)
                .returns((short) -128)
                .returns((short) -128)
                .returns((short) -128)
                .check();
        assertQuery("SELECT CAST(i as SMALLINT) FROM src")
                .returns(NULL_AS_VARARG)
                .returns((short) 42)
                .returns((short) 32767)
                .returns((short) 32767)
                .returns((short) 32767)
                .returns((short) -32768)
                .returns((short) -32768)
                .returns((short) -32768)
                .check();
        assertQuery("SELECT CAST(bi as SMALLINT) FROM src")
                .returns(NULL_AS_VARARG)
                .returns((short) 42)
                .returns((short) 32767)
                .returns((short) 32767)
                .returns((short) 32767)
                .returns((short) -32768)
                .returns((short) -32768)
                .returns((short) -32768)
                .check();
        assertQuery("SELECT CAST(dec as SMALLINT) FROM src")
                .returns(NULL_AS_VARARG)
                .returns((short) 42)
                .returns((short) 32767)
                .returns((short) 32767)
                .returns((short) 32767)
                .returns((short) -32768)
                .returns((short) -32768)
                .returns((short) -32768)
                .check();
        assertQuery("SELECT CAST(r as SMALLINT) FROM src")
                .returns(NULL_AS_VARARG)
                .returns((short) 42)
                .returns((short) 32767)
                .returns((short) 32767)
                .returns((short) 32767)
                .returns((short) -32768)
                .returns((short) -32768)
                .returns((short) -32768)
                .check();
        assertQuery("SELECT CAST(d as SMALLINT) FROM src")
                .returns(NULL_AS_VARARG)
                .returns((short) 42)
                .returns((short) 32767)
                .returns((short) 32767)
                .returns((short) 32767)
                .returns((short) -32768)
                .returns((short) -32768)
                .returns((short) -32768)
                .check();
        assertQuery("SELECT CAST(s as SMALLINT) FROM src")
                .returns(NULL_AS_VARARG)
                .returns((short) 42)
                .returns((short) 32767)
                .returns((short) 32767)
                .returns((short) 32767)
                .returns((short) -32768)
                .returns((short) -32768)
                .returns((short) -32768)
                .check();
    }

    @SuppressWarnings("ThrowableNotThrown")
    @Test
    void explicitCastOfSourceTableOnSelectWithOverflow() {
        sql("INSERT INTO src VALUES "
                + "(1, 127, 32768, 32768, 32768, 32768, 32768, '32768'),"
                + "(2, 127.0, 32768.0, 32768.0, 32768.0, 32768.0, 32768.0, '32768.0'),"
                + "(3, 127.1, 32768.1, 32768.1, 32768.1, 32768.1, 32768.1, '32768.1'),"
                + "(4, -128, -32769, -32769, -32769, -32769, -32769, '-32769'),"
                + "(5, -128.0, -32769.0, -32769.0, -32769.0, -32769.0, -32769.0, '-32769.0'),"
                + "(6, -128.1, -32769.1, -32769.1, -32769.1, -32769.1, -32769.1, '-32769.1')"
        );

        for (int idx : new int[] {1, 2, 3, 4, 5, 6}) {
            for (String column : new String[] {"i", "bi", "dec", "r", "d", "s"}) {
                SqlTestUtils.assertThrowsSqlException(
                        Sql.RUNTIME_ERR,
                        "SMALLINT out of range",
                        () -> sql(format("SELECT CAST({} as SMALLINT) FROM src WHERE id = {}", column, idx))
                );
            }
        }
    }

    private static Stream<Arguments> literalsWithExpectedResult() {
        return Stream.of(
                Arguments.of("NULL", null),
                Arguments.of("42", (short) 42),
                Arguments.of("32767", (short) 32767),
                Arguments.of("-32768", (short) -32768),
                Arguments.of("decimal '42'", (short) 42),
                Arguments.of("decimal '32767'", (short) 32767),
                Arguments.of("decimal '32767.1'", (short) 32767),
                Arguments.of("decimal '32767.9'", (short) 32767),
                Arguments.of("decimal '-32768'", (short) -32768),
                Arguments.of("decimal '-32768.1'", (short) -32768),
                Arguments.of("decimal '-32768.9'", (short) -32768),
                Arguments.of("42.0", (short) 42),
                Arguments.of("32767.0", (short) 32767),
                Arguments.of("32767.1", (short) 32767),
                Arguments.of("32767.9", (short) 32767),
                Arguments.of("-32768.0", (short) -32768),
                Arguments.of("-32768.1", (short) -32768),
                Arguments.of("-32768.9", (short) -32768),
                Arguments.of("'42'", (short) 42),
                Arguments.of("'32767'", (short) 32767),
                Arguments.of("'32767.1'", (short) 32767),
                Arguments.of("'32767.9'", (short) 32767),
                Arguments.of("'-32768'", (short) -32768),
                Arguments.of("'-32768.1'", (short) -32768),
                Arguments.of("'-32768.9'", (short) -32768)
        );
    }

    private static Stream<Arguments> valuesWithExpectedResult() {
        return Stream.of(
                Arguments.of(null, null),
                Arguments.of((byte) 42, (short) 42),
                Arguments.of((byte) 127, (short) 127),
                Arguments.of((byte) -128, (short) -128),
                Arguments.of(42, (short) 42),
                Arguments.of(32767, (short) 32767),
                Arguments.of(-32768, (short) -32768),
                Arguments.of((long) 42, (short) 42),
                Arguments.of((long) 32767, (short) (long) 32767),
                Arguments.of((long) -32768, (short) -32768),
                Arguments.of(new BigDecimal("42"), (short) 42),
                Arguments.of(new BigDecimal("32767"), (short) 32767),
                Arguments.of(new BigDecimal("32767.1"), (short) 32767),
                Arguments.of(new BigDecimal("32767.9"), (short) 32767),
                Arguments.of(new BigDecimal("-32768"), (short) -32768),
                Arguments.of(new BigDecimal("-32768.1"), (short) -32768),
                Arguments.of(new BigDecimal("-32768.9"), (short) -32768),
                Arguments.of(42.0D, (short) 42),
                Arguments.of(32767.0D, (short) 32767),
                Arguments.of(32767.1D, (short) 32767),
                Arguments.of(32767.9D, (short) 32767),
                Arguments.of(-32768.0D, (short) -32768),
                Arguments.of(-32768.1D, (short) -32768),
                Arguments.of(-32768.9D, (short) -32768),
                Arguments.of(42.0F, (short) 42),
                Arguments.of(32767.0F, (short) 32767),
                Arguments.of(32767.1F, (short) 32767),
                Arguments.of(32767.9F, (short) 32767),
                Arguments.of(-32768.0F, (short) -32768),
                Arguments.of(-32768.1F, (short) -32768),
                Arguments.of(-32768.9F, (short) -32768),
                Arguments.of("42", (short) 42),
                Arguments.of("32767", (short) 32767),
                Arguments.of("32767.1", (short) 32767),
                Arguments.of("32767.9", (short) 32767),
                Arguments.of("-32768", (short) -32768),
                Arguments.of("-32768.1", (short) -32768),
                Arguments.of("-32768.9", (short) -32768)
        );
    }

    private static Stream<Arguments> literalsWithOverflow() {
        return Stream.of(
                "32768", "32768.0", "32768.1", "-32769", "-32769.0", "-32769.1", "decimal '32768'", "decimal '32768.1'",
                "decimal '-32769'", "decimal '-32769.1'", "'32768'", "'32768.0'", "'32768.1'", "'-32769'", "'-32769.1'"
        ).map(Arguments::of);
    }

    private static Stream<Arguments> valuesWithOverflow() {
        return Stream.of(
                32768, 32768, -32769, -32769L, 32768.0F, 32768.0D,
                32768.1F, 32768.1D, -32769.0F, -32769.0D, -32769.1F, -32769.1D, new BigDecimal("32768"),
                new BigDecimal("32768.1"), new BigDecimal("-32769"), new BigDecimal("-32769.1"),
                "32768", "32768.0", "32768.1", "-32769", "-32769.1"
        ).map(Arguments::of);
    }
}
