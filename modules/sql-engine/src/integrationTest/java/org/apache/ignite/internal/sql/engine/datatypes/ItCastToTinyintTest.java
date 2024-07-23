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
 * Set of tests to ensure correctness of CAST expression to TINYINT for
 * type pairs supported by cast specification.
 */
@WithSystemProperty(key = "IMPLICIT_PK_ENABLED", value = "true")
public class ItCastToTinyintTest extends BaseSqlIntegrationTest {
    @BeforeAll
    static void createTable() {
        sql("CREATE TABLE test (val TINYINT)");
        sql("CREATE TABLE src (id INT PRIMARY KEY, si SMALLINT, i INT, bi BIGINT," 
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
                "TINYINT out of range",
                () -> sql(format("INSERT INTO test VALUES ({})", literal))
        );
    }

    @ParameterizedTest
    @MethodSource("literalsWithExpectedResult")
    void explicitCastOfLiteralsOnInsert(String literal, Object expectedResult) {
        assertQuery(format("INSERT INTO test VALUES (CAST({} as TINYINT))", literal)).check();

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
                "TINYINT out of range",
                () -> sql(format("INSERT INTO test VALUES (CAST({} as TINYINT))", literal))
        );
    }

    @ParameterizedTest
    @MethodSource("literalsWithExpectedResult")
    void explicitCastOfLiteralsOnSelect(String literal, Object expectedResult) {
        assertQuery(format("SELECT CAST({} as TINYINT)", literal))
                .returns(expectedResult)
                .check();
    }

    @SuppressWarnings("ThrowableNotThrown")
    @ParameterizedTest
    @MethodSource("literalsWithOverflow")
    void explicitCastOfLiteralsOnSelectWithOverflow(String literal) {
        SqlTestUtils.assertThrowsSqlException(
                Sql.RUNTIME_ERR,
                "TINYINT out of range",
                () -> sql(format("SELECT CAST({} as TINYINT)", literal))
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

            builder.append("(CAST(").append(literal).append(" as TINYINT))");

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
                "TINYINT out of range",
                () -> sql("INSERT INTO test VALUES (?)", param)
        );
    }

    @ParameterizedTest
    @MethodSource("valuesWithExpectedResult")
    void explicitCastOfDynParamsOnInsert(Object param, Object expectedResult) {
        assertQuery("INSERT INTO test VALUES (CAST(? as TINYINT))")
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
                "TINYINT out of range",
                () -> sql("INSERT INTO test VALUES (CAST(? as TINYINT))", param)
        );
    }

    @ParameterizedTest
    @MethodSource("valuesWithExpectedResult")
    void explicitCastOfDynParamsOnSelect(Object param, Object expectedResult) {
        assertQuery("SELECT CAST(? as TINYINT)")
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
                "TINYINT out of range",
                () -> sql("SELECT CAST(? as TINYINT)", param)
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

            builder.append("(CAST(? as TINYINT))");

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
                + "(2, 127, 127, 127, 127, 127, 127, '127'),"
                + "(3, 127.1, 127.1, 127.1, 127.1, 127.1, 127.1, '127.1'),"
                + "(4, 127.9, 127.9, 127.9, 127.9, 127.9, 127.9, '127.9'),"
                + "(5, -128, -128, -128, -128, -128, -128, '-128'),"
                + "(6, -128.1, -128.1, -128.1, -128.1, -128.1, -128.1, '-128.1'),"
                + "(7, -128.9, -128.9, -128.9, -128.9, -128.9, -128.9, '-128.9')"
        );

        assertQuery("INSERT INTO test SELECT si FROM src").check();
        assertQuery("INSERT INTO test SELECT i FROM src").check();
        assertQuery("INSERT INTO test SELECT bi FROM src").check();
        assertQuery("INSERT INTO test SELECT dec FROM src").check();
        assertQuery("INSERT INTO test SELECT r FROM src").check();
        assertQuery("INSERT INTO test SELECT d FROM src").check();
        assertQuery("INSERT INTO test SELECT s FROM src").check();

        assertQuery("SELECT * FROM test")
                .returns((byte) 42)
                .returns((byte) 127)
                .returns((byte) 127)
                .returns((byte) 127)
                .returns((byte) -128)
                .returns((byte) -128)
                .returns((byte) -128)
                .returns((byte) 42)
                .returns((byte) 127)
                .returns((byte) 127)
                .returns((byte) 127)
                .returns((byte) -128)
                .returns((byte) -128)
                .returns((byte) -128)
                .returns((byte) 42)
                .returns((byte) 127)
                .returns((byte) 127)
                .returns((byte) 127)
                .returns((byte) -128)
                .returns((byte) -128)
                .returns((byte) -128)
                .returns((byte) 42)
                .returns((byte) 127)
                .returns((byte) 127)
                .returns((byte) 127)
                .returns((byte) -128)
                .returns((byte) -128)
                .returns((byte) -128)
                .returns((byte) 42)
                .returns((byte) 127)
                .returns((byte) 127)
                .returns((byte) 127)
                .returns((byte) -128)
                .returns((byte) -128)
                .returns((byte) -128)
                .returns((byte) 42)
                .returns((byte) 127)
                .returns((byte) 127)
                .returns((byte) 127)
                .returns((byte) -128)
                .returns((byte) -128)
                .returns((byte) -128)
                .returns((byte) 42)
                .returns((byte) 127)
                .returns((byte) 127)
                .returns((byte) 127)
                .returns((byte) -128)
                .returns((byte) -128)
                .returns((byte) -128)
                .check();
    }

    @SuppressWarnings("ThrowableNotThrown")
    @Test
    void implicitCastOfSourceTableOnInsertWithOverflow() {
        sql("INSERT INTO src VALUES "
                + "(1, 128, 128, 128, 128, 128, 128, '128'),"
                + "(2, 128.0, 128.0, 128.0, 128.0, 128.0, 128.0, '128.0'),"
                + "(3, 128.1, 128.1, 128.1, 128.1, 128.1, 128.1, '128.1'),"
                + "(4, -129, -129, -129, -129, -129, -129, '-129'),"
                + "(5, -129.0, -129.0, -129.0, -129.0, -129.0, -129.0, '-129.0'),"
                + "(6, -129.1, -129.1, -129.1, -129.1, -129.1, -129.1, '-129.1')"
        );

        for (int idx : new int[] {1, 2, 3, 4, 5, 6}) {
            for (String column : new String[] {"si", "i", "bi", "dec", "r", "d", "s"}) {
                SqlTestUtils.assertThrowsSqlException(
                        Sql.RUNTIME_ERR,
                        "TINYINT out of range",
                        () -> sql(format("INSERT INTO test SELECT {} FROM src WHERE id = {}", column, idx))
                );
            }
        }
    }

    @Test
    void explicitCastOfSourceTableOnInsert() {
        sql("INSERT INTO src VALUES "
                + "(1, 42, 42, 42, 42, 42, 42, '42'),"
                + "(2, 127, 127, 127, 127, 127, 127, '127'),"
                + "(3, 127.1, 127.1, 127.1, 127.1, 127.1, 127.1, '127.1'),"
                + "(4, 127.9, 127.9, 127.9, 127.9, 127.9, 127.9, '127.9'),"
                + "(5, -128, -128, -128, -128, -128, -128, '-128'),"
                + "(6, -128.1, -128.1, -128.1, -128.1, -128.1, -128.1, '-128.1'),"
                + "(7, -128.9, -128.9, -128.9, -128.9, -128.9, -128.9, '-128.9')"
        );

        assertQuery("INSERT INTO test SELECT CAST(si as TINYINT) FROM src").check();
        assertQuery("INSERT INTO test SELECT CAST(i as TINYINT) FROM src").check();
        assertQuery("INSERT INTO test SELECT CAST(bi as TINYINT) FROM src").check();
        assertQuery("INSERT INTO test SELECT CAST(dec as TINYINT) FROM src").check();
        assertQuery("INSERT INTO test SELECT CAST(r as TINYINT) FROM src").check();
        assertQuery("INSERT INTO test SELECT CAST(d as TINYINT) FROM src").check();
        assertQuery("INSERT INTO test SELECT CAST(s as TINYINT) FROM src").check();

        assertQuery("SELECT * FROM test")
                .returns((byte) 42)
                .returns((byte) 127)
                .returns((byte) 127)
                .returns((byte) 127)
                .returns((byte) -128)
                .returns((byte) -128)
                .returns((byte) -128)
                .returns((byte) 42)
                .returns((byte) 127)
                .returns((byte) 127)
                .returns((byte) 127)
                .returns((byte) -128)
                .returns((byte) -128)
                .returns((byte) -128)
                .returns((byte) 42)
                .returns((byte) 127)
                .returns((byte) 127)
                .returns((byte) 127)
                .returns((byte) -128)
                .returns((byte) -128)
                .returns((byte) -128)
                .returns((byte) 42)
                .returns((byte) 127)
                .returns((byte) 127)
                .returns((byte) 127)
                .returns((byte) -128)
                .returns((byte) -128)
                .returns((byte) -128)
                .returns((byte) 42)
                .returns((byte) 127)
                .returns((byte) 127)
                .returns((byte) 127)
                .returns((byte) -128)
                .returns((byte) -128)
                .returns((byte) -128)
                .returns((byte) 42)
                .returns((byte) 127)
                .returns((byte) 127)
                .returns((byte) 127)
                .returns((byte) -128)
                .returns((byte) -128)
                .returns((byte) -128)
                .returns((byte) 42)
                .returns((byte) 127)
                .returns((byte) 127)
                .returns((byte) 127)
                .returns((byte) -128)
                .returns((byte) -128)
                .returns((byte) -128)
                .check();
    }

    @SuppressWarnings("ThrowableNotThrown")
    @Test
    void explicitCastOfSourceTableOnInsertWithOverflow() {
        sql("INSERT INTO src VALUES "
                + "(1, 128, 128, 128, 128, 128, 128, '128'),"
                + "(2, 128.0, 128.0, 128.0, 128.0, 128.0, 128.0, '128.0'),"
                + "(3, 128.1, 128.1, 128.1, 128.1, 128.1, 128.1, '128.1'),"
                + "(4, -129, -129, -129, -129, -129, -129, '-129'),"
                + "(5, -129.0, -129.0, -129.0, -129.0, -129.0, -129.0, '-129.0'),"
                + "(6, -129.1, -129.1, -129.1, -129.1, -129.1, -129.1, '-129.1')"
        );

        for (int idx : new int[] {1, 2, 3, 4, 5, 6}) {
            for (String column : new String[] {"si", "i", "bi", "dec", "r", "d", "s"}) {
                SqlTestUtils.assertThrowsSqlException(
                        Sql.RUNTIME_ERR,
                        "TINYINT out of range",
                        () -> sql(format("INSERT INTO test SELECT CAST({} as TINYINT) FROM src WHERE id = {}", column, idx))
                );
            }
        }
    }

    @Test
    void explicitCastOfSourceTableOnSelect() {
        sql("INSERT INTO src VALUES "
                + "(1, 42, 42, 42, 42, 42, 42, '42'),"
                + "(2, 127, 127, 127, 127, 127, 127, '127'),"
                + "(3, 127.1, 127.1, 127.1, 127.1, 127.1, 127.1, '127.1'),"
                + "(4, 127.9, 127.9, 127.9, 127.9, 127.9, 127.9, '127.9'),"
                + "(5, -128, -128, -128, -128, -128, -128, '-128'),"
                + "(6, -128.1, -128.1, -128.1, -128.1, -128.1, -128.1, '-128.1'),"
                + "(7, -128.9, -128.9, -128.9, -128.9, -128.9, -128.9, '-128.9')"
        );

        assertQuery("SELECT CAST(si as TINYINT) FROM src")
                .returns((byte) 42)
                .returns((byte) 127)
                .returns((byte) 127)
                .returns((byte) 127)
                .returns((byte) -128)
                .returns((byte) -128)
                .returns((byte) -128)
                .check();
        assertQuery("SELECT CAST(i as TINYINT) FROM src")
                .returns((byte) 42)
                .returns((byte) 127)
                .returns((byte) 127)
                .returns((byte) 127)
                .returns((byte) -128)
                .returns((byte) -128)
                .returns((byte) -128)
                .check();
        assertQuery("SELECT CAST(bi as TINYINT) FROM src")
                .returns((byte) 42)
                .returns((byte) 127)
                .returns((byte) 127)
                .returns((byte) 127)
                .returns((byte) -128)
                .returns((byte) -128)
                .returns((byte) -128)
                .check();
        assertQuery("SELECT CAST(dec as TINYINT) FROM src")
                .returns((byte) 42)
                .returns((byte) 127)
                .returns((byte) 127)
                .returns((byte) 127)
                .returns((byte) -128)
                .returns((byte) -128)
                .returns((byte) -128)
                .check();
        assertQuery("SELECT CAST(r as TINYINT) FROM src")
                .returns((byte) 42)
                .returns((byte) 127)
                .returns((byte) 127)
                .returns((byte) 127)
                .returns((byte) -128)
                .returns((byte) -128)
                .returns((byte) -128)
                .check();
        assertQuery("SELECT CAST(d as TINYINT) FROM src")
                .returns((byte) 42)
                .returns((byte) 127)
                .returns((byte) 127)
                .returns((byte) 127)
                .returns((byte) -128)
                .returns((byte) -128)
                .returns((byte) -128)
                .check();
        assertQuery("SELECT CAST(s as TINYINT) FROM src")
                .returns((byte) 42)
                .returns((byte) 127)
                .returns((byte) 127)
                .returns((byte) 127)
                .returns((byte) -128)
                .returns((byte) -128)
                .returns((byte) -128)
                .check();
    }

    @SuppressWarnings("ThrowableNotThrown")
    @Test
    void explicitCastOfSourceTableOnSelectWithOverflow() {
        sql("INSERT INTO src VALUES "
                + "(1, 128, 128, 128, 128, 128, 128, '128'),"
                + "(2, 128.0, 128.0, 128.0, 128.0, 128.0, 128.0, '128.0'),"
                + "(3, 128.1, 128.1, 128.1, 128.1, 128.1, 128.1, '128.1'),"
                + "(4, -129, -129, -129, -129, -129, -129, '-129'),"
                + "(5, -129.0, -129.0, -129.0, -129.0, -129.0, -129.0, '-129.0'),"
                + "(6, -129.1, -129.1, -129.1, -129.1, -129.1, -129.1, '-129.1')"
        );

        for (int idx : new int[] {1, 2, 3, 4, 5, 6}) {
            for (String column : new String[] {"si", "i", "bi", "dec", "r", "d", "s"}) {
                SqlTestUtils.assertThrowsSqlException(
                        Sql.RUNTIME_ERR,
                        "TINYINT out of range",
                        () -> sql(format("SELECT CAST({} as TINYINT) FROM src WHERE id = {}", column, idx))
                );
            }
        }
    }

    private static Stream<Arguments> literalsWithExpectedResult() {
        return Stream.of(
                Arguments.of("42", (byte) 42),
                Arguments.of("127", (byte) 127),
                Arguments.of("-128", (byte) -128),
                Arguments.of("decimal '42'", (byte) 42),
                Arguments.of("decimal '127'", (byte) 127),
                Arguments.of("decimal '127.1'", (byte) 127),
                Arguments.of("decimal '127.9'", (byte) 127),
                Arguments.of("decimal '-128'", (byte) -128),
                Arguments.of("decimal '-128.1'", (byte) -128),
                Arguments.of("decimal '-128.9'", (byte) -128),
                Arguments.of("42.0", (byte) 42),
                Arguments.of("127.0", (byte) 127),
                Arguments.of("127.1", (byte) 127),
                Arguments.of("127.9", (byte) 127),
                Arguments.of("-128.0", (byte) -128),
                Arguments.of("-128.1", (byte) -128),
                Arguments.of("-128.9", (byte) -128),
                Arguments.of("'42'", (byte) 42),
                Arguments.of("'127'", (byte) 127),
                Arguments.of("'127.1'", (byte) 127),
                Arguments.of("'127.9'", (byte) 127),
                Arguments.of("'-128'", (byte) -128),
                Arguments.of("'-128.1'", (byte) -128),
                Arguments.of("'-128.9'", (byte) -128)
        );
    }

    private static Stream<Arguments> valuesWithExpectedResult() {
        return Stream.of(
                Arguments.of((short) 42, (byte) 42),
                Arguments.of((short) 127, (byte) 127),
                Arguments.of((short) -128, (byte) -128),
                Arguments.of(42, (byte) 42),
                Arguments.of(127, (byte) 127),
                Arguments.of(-128, (byte) -128),
                Arguments.of((long) 42, (byte) 42),
                Arguments.of((long) 127, (byte) (long) 127),
                Arguments.of((long) -128, (byte) -128),
                Arguments.of(new BigDecimal("42"), (byte) 42),
                Arguments.of(new BigDecimal("127"), (byte) 127),
                Arguments.of(new BigDecimal("127.1"), (byte) 127),
                Arguments.of(new BigDecimal("127.9"), (byte) 127),
                Arguments.of(new BigDecimal("-128"), (byte) -128),
                Arguments.of(new BigDecimal("-128.1"), (byte) -128),
                Arguments.of(new BigDecimal("-128.9"), (byte) -128),
                Arguments.of(42.0D, (byte) 42),
                Arguments.of(127.0D, (byte) 127),
                Arguments.of(127.1D, (byte) 127),
                Arguments.of(127.9D, (byte) 127),
                Arguments.of(-128.0D, (byte) -128),
                Arguments.of(-128.1D, (byte) -128),
                Arguments.of(-128.9D, (byte) -128),
                Arguments.of(42.0F, (byte) 42),
                Arguments.of(127.0F, (byte) 127),
                Arguments.of(127.1F, (byte) 127),
                Arguments.of(127.9F, (byte) 127),
                Arguments.of(-128.0F, (byte) -128),
                Arguments.of(-128.1F, (byte) -128),
                Arguments.of(-128.9F, (byte) -128),
                Arguments.of("42", (byte) 42),
                Arguments.of("127", (byte) 127),
                Arguments.of("127.1", (byte) 127),
                Arguments.of("127.9", (byte) 127),
                Arguments.of("-128", (byte) -128),
                Arguments.of("-128.1", (byte) -128),
                Arguments.of("-128.9", (byte) -128)
        );
    }

    private static Stream<Arguments> literalsWithOverflow() {
        return Stream.of(
                "128", "128.0", "128.1", "-129", "-129.0", "-129.1", "decimal '128'", "decimal '128.1'",
                "decimal '-129'", "decimal '-129.1'", "'128'", "'128.0'", "'128.1'", "'-129'", "'-129.1'"
        ).map(Arguments::of);
    }

    private static Stream<Arguments> valuesWithOverflow() {
        return Stream.of(
                (short) 128, 128, 128L, (short) -129, -129, -129L, 128.0F, 128.0D,
                128.1F, 128.1D, -129.0F, -129.0D, -129.1F, -129.1D, new BigDecimal("128"),
                new BigDecimal("128.1"), new BigDecimal("-129"), new BigDecimal("-129.1"),
                "128", "128.0", "128.1", "-129", "-129.1"
        ).map(Arguments::of);
    }
}
