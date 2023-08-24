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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;
import org.apache.ignite.sql.SqlException;
import org.junit.jupiter.api.Test;

/**
 * Tests for sql parser.
 */
public class IgniteSqlParserTest {
    @Test
    public void testStatementMode() {
        StatementParseResult result = IgniteSqlParser.parse("SELECT 1 + ?", StatementParseResult.MODE);

        assertEquals(1, result.dynamicParamsCount());
        assertNotNull(result.statement());
        assertEquals(List.of(result.statement()), result.statements());
    }

    @Test
    public void testScriptMode() {
        ScriptParseResult result = IgniteSqlParser.parse("SELECT 1 + ?; SELECT 1; SELECT 2 + ?", ScriptParseResult.MODE);

        assertEquals(2, result.dynamicParamsCount());
        assertEquals(3, result.statements().size());
    }

    /**
     * {@link StatementParseResult#MODE} does not allow input that contains multiple statements.
     */
    @Test
    public void testStatementModeRejectMultipleStatements() {
        SqlException t = assertThrows(SqlException.class,
                () -> IgniteSqlParser.parse("SELECT 1; SELECT 2", StatementParseResult.MODE));
        assertThat(t.getMessage(), containsString("Multiple statements are not allowed"));

    }

    @Test
    public void testGrammarViolation() {
        SqlException t = assertThrows(SqlException.class,
                () -> IgniteSqlParser.parse("ALTER TABEL foo ADD COLUMN bar INT", StatementParseResult.MODE));
        assertThat(t.getMessage(), equalTo("Failed to parse query: Encountered \"TABEL\" at line 1, column 7"));
    }

    @Test
    public void testGrammarViolationButExceptionIsReplacedInsideParser() {
        SqlException t = assertThrows(SqlException.class,
                () -> IgniteSqlParser.parse("SELECT ALL FROM foo", StatementParseResult.MODE));
        assertThat(t.getMessage(), equalTo("Failed to parse query: Incorrect syntax near the keyword 'FROM' at line 1, column 12"));
    }

    @Test
    public void testEmptyString() {
        SqlException t = assertThrows(SqlException.class,
                () -> IgniteSqlParser.parse("", StatementParseResult.MODE));
        assertThat(t.getMessage(), equalTo("Failed to parse query: Encountered \"<EOF>\" at line 0, column 0"));
    }

    @Test
    public void testCommentedQuery() {
        SqlException t = assertThrows(SqlException.class,
                () -> IgniteSqlParser.parse("-- SELECT 1", StatementParseResult.MODE));
        assertThat(t.getMessage(), equalTo("Failed to parse query: Encountered \"<EOF>\" at line 1, column 11"));
    }

    @Test
    public void testTokenizationError() {
        SqlException t = assertThrows(SqlException.class,
                () -> IgniteSqlParser.parse("SELECT foo#bar", StatementParseResult.MODE));
        assertThat(t.getMessage(), equalTo("Failed to parse query: Lexical error at line 1, column 11. "
                + " Encountered: \"#\" (35), after : \"\""));
    }

    @Test
    public void testExpressionInsteadOfStatement() {
        SqlException t = assertThrows(SqlException.class,
                () -> IgniteSqlParser.parse("a = 2", StatementParseResult.MODE));
        assertThat(t.getMessage(), equalTo("Failed to parse query: Non-query expression encountered "
                + "in illegal context. At line 1, column 1"));
    }

    @Test
    public void testExpressionInsteadOfSubStatement() {
        SqlException t = assertThrows(SqlException.class,
                () -> IgniteSqlParser.parse("INSERT INTO t (c1) (a = 2)", StatementParseResult.MODE));
        assertThat(t.getMessage(), equalTo("Failed to parse query: Non-query expression encountered "
                + "in illegal context. At line 1, column 21"));
    }

    @Test
    public void testInvalidDecimalLiteral() {
        SqlException t = assertThrows(SqlException.class,
                () -> IgniteSqlParser.parse("SELECT decimal '2a'", StatementParseResult.MODE));
        assertThat(t.getMessage(), equalTo("Failed to parse query: Invalid decimal literal. At line 1, column 16"));
    }
}
