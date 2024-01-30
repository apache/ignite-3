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

package org.apache.ignite.internal.sql.engine.sql;

import static org.apache.ignite.internal.sql.engine.util.SqlTestUtils.assertThrowsSqlException;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.util.Arrays;
import java.util.List;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.ignite.lang.ErrorGroups.Sql;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Tests for sql parser.
 */
public class IgniteSqlParserTest {
    @Test
    public void testStatementMode() {
        StatementParseResult result = IgniteSqlParser.parse("SELECT 1 + ?", StatementParseResult.MODE);

        assertEquals(1, result.dynamicParamsCount());
        assertNotNull(result.statement());
    }

    @ParameterizedTest
    @MethodSource("multiStatementQueries")
    public void testScriptMode(String query, int[] expectedParamsCountPerStatement) {
        ScriptParseResult scriptParseResult = IgniteSqlParser.parse(query, ScriptParseResult.MODE);
        int expectedTotalParams = Arrays.stream(expectedParamsCountPerStatement).sum();
        int expectedStatementsCount = expectedParamsCountPerStatement.length;

        assertEquals(expectedTotalParams, scriptParseResult.dynamicParamsCount());
        assertEquals(expectedStatementsCount, scriptParseResult.results().size());

        for (int i = 0; i < scriptParseResult.results().size(); i++) {
            StatementParseResult res = scriptParseResult.results().get(i);

            assertNotNull(res.statement());
            assertEquals(expectedParamsCountPerStatement[i], res.dynamicParamsCount());
        }
    }

    /**
     * {@link StatementParseResult#MODE} does not allow input that contains multiple statements.
     */
    @Test
    public void testStatementModeRejectMultipleStatements() {
        assertThrowsSqlException(
                Sql.STMT_VALIDATION_ERR,
                "Multiple statements are not allowed",
                () -> IgniteSqlParser.parse("SELECT 1; SELECT 2", StatementParseResult.MODE));
    }

    @Test
    public void testGrammarViolation() {
        assertThrowsSqlException(
                Sql.STMT_PARSE_ERR,
                "Failed to parse query: Encountered \"TABEL\" at line 1, column 7",
                () -> IgniteSqlParser.parse("ALTER TABEL foo ADD COLUMN bar INT", StatementParseResult.MODE));
    }

    @Test
    public void testGrammarViolationButExceptionIsReplacedInsideParser() {
        assertThrowsSqlException(
                Sql.STMT_PARSE_ERR,
                "Failed to parse query: Incorrect syntax near the keyword 'FROM' at line 1, column 12",
                () -> IgniteSqlParser.parse("SELECT ALL FROM foo", StatementParseResult.MODE));
    }

    @Test
    public void testEmptyString() {
        assertThrowsSqlException(Sql.STMT_PARSE_ERR,
                "Failed to parse query: Encountered \"<EOF>\" at line 0, column 0",
                () -> IgniteSqlParser.parse("", StatementParseResult.MODE));
    }

    @Test
    public void testEmptyStatements() {
        assertThrowsSqlException(Sql.STMT_PARSE_ERR,
                "Failed to parse query: Encountered \";\" at line 1, column 1",
                () -> IgniteSqlParser.parse(";", ScriptParseResult.MODE));

        assertThrowsSqlException(Sql.STMT_PARSE_ERR,
                "Failed to parse query: Encountered \";\" at line 2, column 1",
                () -> IgniteSqlParser.parse("--- comment\n;", ScriptParseResult.MODE));

        assertThrowsSqlException(Sql.STMT_PARSE_ERR,
                "Failed to parse query: Encountered \"<EOF>\" at line 1, column 11",
                () -> IgniteSqlParser.parse("--- comment", ScriptParseResult.MODE));
    }

    @Test
    public void testCommentedQuery() {
        assertThrowsSqlException(
                Sql.STMT_PARSE_ERR,
                "Failed to parse query: Encountered \"<EOF>\" at line 1, column 11",
                () -> IgniteSqlParser.parse("-- SELECT 1", StatementParseResult.MODE));
    }

    @Test
    public void testTokenizationError() {
        assertThrowsSqlException(
                Sql.STMT_PARSE_ERR,
                "Failed to parse query: Lexical error at line 1, column 11.  Encountered: \"#\" (35), after : \"\"",
                () -> IgniteSqlParser.parse("SELECT foo#bar", StatementParseResult.MODE));
    }

    @Test
    public void testExpressionInsteadOfStatement() {
        assertThrowsSqlException(
                Sql.STMT_PARSE_ERR,
                "Failed to parse query: Non-query expression encountered in illegal context. At line 1, column 1",
                () -> IgniteSqlParser.parse("a = 2", StatementParseResult.MODE));
    }

    @Test
    public void testExpressionInsteadOfSubStatement() {
        assertThrowsSqlException(
                Sql.STMT_PARSE_ERR,
                "Failed to parse query: Non-query expression encountered in illegal context. At line 1, column 21",
                () -> IgniteSqlParser.parse("INSERT INTO t (c1) (a = 2)", StatementParseResult.MODE));
    }

    @Test
    public void testInvalidDecimalLiteral() {
        assertThrowsSqlException(
                Sql.STMT_PARSE_ERR,
                "Literal '2a' can not be parsed to type 'DECIMAL'",
                () -> IgniteSqlParser.parse("SELECT decimal '2a'", StatementParseResult.MODE));
    }

    @ParameterizedTest
    @MethodSource("dmlStatements")
    public void testDmlStatementsClone(String stmt) {
        StatementParseResult single = IgniteSqlParser.parse(stmt, StatementParseResult.MODE);

        SqlNode statement = single.statement();
        SqlParserPos pos = statement.getParserPosition();

        assertEquals(statement.clone(pos).toString(), statement.toString());
    }

    @ParameterizedTest
    @MethodSource("dmlStatements")
    public void testScriptDmlStatementsClone(String stmt) {
        ScriptParseResult script = IgniteSqlParser.parse(stmt, ScriptParseResult.MODE);

        StatementParseResult single = script.results().get(0);

        SqlNode statement = single.statement();
        SqlParserPos pos = statement.getParserPosition();

        assertEquals(statement.clone(pos).toString(), statement.toString());
    }

    private static List<Arguments> dmlStatements() {
        return List.of(
                Arguments.of("UPDATE t SET x = 1"),
                Arguments.of("UPDATE t SET x = 1 WHERE y = 2"),

                Arguments.of("DELETE FROM t"),
                Arguments.of("DELETE FROM t WHERE y = 2"),

                Arguments.of("MERGE INTO T2 dst USING t1 src ON dst.c1 = src.c1"
                        + " WHEN MATCHED THEN UPDATE SET c2 = 1"),

                Arguments.of("MERGE INTO T2 dst USING t1 src ON dst.c1 = src.c1 "
                        + "WHEN NOT MATCHED THEN INSERT (c1, c2, c3) VALUES (src.c1, src.c2, 1)"),

                Arguments.of("MERGE INTO T2 dst USING t1 src ON dst.c1 = src.c1 "
                        + "WHEN MATCHED THEN UPDATE SET c2 = 1 "
                        + "WHEN NOT MATCHED THEN INSERT (c1, c2, c3) VALUES (src.c1, src.c2, 1)")

        );
    }

    private static List<Arguments> multiStatementQueries() {
        return List.of(
                Arguments.of(
                        "-- insert\n"
                        + "INSERT INTO TEST VALUES(1, 1);;\n"
                        + "--- select\n"
                        + ";;;SELECT 1 + 2",
                        new int[]{0, 0}
                ),
                Arguments.of("SELECT 1 + ? - ?", new int[]{2}),
                Arguments.of("SELECT 1; INSERT INTO TEST VALUES(?, ?, ?)", new int[] {0, 3}),
                Arguments.of("INSERT INTO TEST VALUES(?, ?); SELECT 1; SELECT 2 + ?", new int[] {2, 0, 1})
        );
    }
}
