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

import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;
import static org.apache.ignite.internal.sql.engine.util.SqlTestUtils.assertThrowsSqlException;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlNode;
import org.apache.ignite.internal.sql.engine.planner.AbstractPlannerTest;
import org.apache.ignite.internal.sql.engine.rel.IgniteRel;
import org.apache.ignite.internal.sql.engine.schema.IgniteSchema;
import org.apache.ignite.lang.ErrorGroups.Sql;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Tests for decimal literals, like: DECIMAL '99.999'.
 */
public class DecimalLiteralTest extends AbstractPlannerTest {
    /**
     * Type of numeric decimal literal and type of decimal literal should match.
     */
    @ParameterizedTest
    @CsvSource({
            "-0.01",
            "-0.1",
            "-10.0",
            "-10.122",
            "0.0",
            "0.1",
            "0.01",
            "10.0",
            "10.122",
    })
    public void testLiteralTypeMatch(String val) throws Exception {
        String query = format("SELECT {}, DECIMAL '{}'", val, val);

        IgniteRel rel = physicalPlan(query, new IgniteSchema(DEFAULT_SCHEMA, 1, List.of()));

        RelDataType numericLitType = rel.getRowType().getFieldList().get(0).getType();
        RelDataType decimalLitType = rel.getRowType().getFieldList().get(1).getType();

        assertEquals(numericLitType, decimalLitType);
    }

    /**
     * Test cases for invalid literal values.
     */
    @ParameterizedTest
    @ValueSource(strings = {
            "DECIMAL 'NAN'",
            "DECIMAL '10a'",
            "DECIMAL 'a10'",
            "DECIMAL 'f1'",
            "DECIMAL '1\n1000'",
    })
    public void testParserRejectsInvalidValues(String value) {
        var query = format("SELECT {}", value);
        assertThrowsSqlException(Sql.STMT_PARSE_ERR, "can not be parsed to type", () -> parseQuery(query));
    }

    /**
     * Test cases for invalid literal expressions.
     */
    @ParameterizedTest
    @ValueSource(strings = {
            "DECIMAL x'00'",
            "DECIMAL N\"10\"",
            "DECIMAL 'a10'",
            "DECIMAL '10",
            "DECIMAL 10'",
    })
    public void testParserRejectInvalidForms(String value) {
        var query = format("SELECT {}", value);

        assertThrowsSqlException(Sql.STMT_PARSE_ERR, "Failed to parse query:", () -> parseQuery(query));
    }

    private static SqlNode parseQuery(String qry) {
        StatementParseResult parseResult = IgniteSqlParser.parse(qry, StatementParseResult.MODE);
        return parseResult.statement();
    }
}
