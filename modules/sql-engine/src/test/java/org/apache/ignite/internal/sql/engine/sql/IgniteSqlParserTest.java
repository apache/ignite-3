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
}
