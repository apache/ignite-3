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
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

import org.apache.calcite.sql.SqlNode;
import org.apache.ignite.lang.ErrorGroups.Sql;
import org.junit.jupiter.api.Test;

/**
 * Tests to verify parsing of the EXPLAIN " commands.
 */
public class ExplainPlanForParserTest extends AbstractParserTest {
    @Test
    public void explainPlanFull() {
        IgniteSqlExplain explain = parseExplain("EXPLAIN PLAN FOR select 1");

        assertEquals(0, explain.getDynamicParamCount());

        expectUnparsed(explain, "EXPLAIN PLAN FOR" + System.lineSeparator() + "SELECT 1");
    }

    @Test
    public void explainPlanWithMapping() {
        IgniteSqlExplain explain = parseExplain("EXPLAIN MAPPING FOR select 1");

        assertEquals(0, explain.getDynamicParamCount());

        expectUnparsed(explain, "EXPLAIN MAPPING FOR" + System.lineSeparator() + "SELECT 1");

        assertThrowsSqlException(Sql.STMT_PARSE_ERR, "Non-query expression encountered in illegal context. At line 1, column 9",
                () -> parse("explain MAPPINGGG FOR plan select 1"));
    }

    @Test
    public void explainPlanFullWithDynParam() {
        IgniteSqlExplain explain = parseExplain("EXPLAIN PLAN FOR select ?");

        assertEquals(1, explain.getDynamicParamCount());

        expectUnparsed(explain, "EXPLAIN PLAN FOR" + System.lineSeparator() + "SELECT ?");
    }

    @Test
    public void explainPlanShort() {
        IgniteSqlExplain explain = parseExplain("EXPLAIN  select 1");

        assertEquals(0, explain.getDynamicParamCount());

        expectUnparsed(explain, "EXPLAIN PLAN FOR" + System.lineSeparator() + "SELECT 1");
    }

    @Test
    public void explainPlanShortWithDynPar() {
        IgniteSqlExplain explain = parseExplain("EXPLAIN  select ?");

        assertEquals(1, explain.getDynamicParamCount());

        expectUnparsed(explain, "EXPLAIN PLAN FOR" + System.lineSeparator() + "SELECT ?");
    }

    @Test
    public void explainInvalidSyntax() {
        assertThrowsSqlException(Sql.STMT_PARSE_ERR, "Incorrect syntax near the keyword 'FOR'", () -> parse("explain FOR plan select 1"));
    }

    private static IgniteSqlExplain parseExplain(String stmt) {
        SqlNode node = parse(stmt);

        return assertInstanceOf(IgniteSqlExplain.class, node);
    }
}
