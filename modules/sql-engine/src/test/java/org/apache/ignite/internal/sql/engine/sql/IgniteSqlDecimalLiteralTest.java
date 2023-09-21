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
import static org.apache.ignite.lang.IgniteStringFormatter.format;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.util.List;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.pretty.SqlPrettyWriter;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Litmus;
import org.apache.ignite.internal.sql.engine.planner.AbstractPlannerTest;
import org.apache.ignite.internal.sql.engine.rel.IgniteRel;
import org.apache.ignite.internal.sql.engine.schema.IgniteSchema;
import org.apache.ignite.internal.sql.engine.util.Commons;
import org.apache.ignite.lang.ErrorGroups.Sql;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Tests for {@link IgniteSqlDecimalLiteral}.
 */
public class IgniteSqlDecimalLiteralTest extends AbstractPlannerTest {

    /**
     * Tests literal type type.
     */
    @Test
    public void testValueAndType() {
        var input = new BigDecimal("100.20");
        var literal = IgniteSqlDecimalLiteral.create(input, SqlParserPos.ZERO);
        assertTrue(literal.isExact(), "decimal is always exact");

        assertEquals(5, literal.getPrec(), "precision");
        assertEquals(2, literal.getScale(), "scale");

        assertEquals(input, literal.getValue(), "value");

        var typeFactory = Commons.typeFactory();
        var actualType = literal.createSqlType(typeFactory);

        var expectedType = typeFactory.createSqlType(SqlTypeName.DECIMAL, input.precision(), input.scale());
        assertEquals(expectedType, actualType, "type");
    }

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
     * Tests {@link IgniteSqlDecimalLiteral#unparse(SqlWriter, int, int)}.
     */
    @Test
    public void testToSql() {
        var input = new BigDecimal("100.20");
        var literal = IgniteSqlDecimalLiteral.create(input, SqlParserPos.ZERO);

        var w = new SqlPrettyWriter();
        literal.unparse(w, 0, 0);

        assertEquals(format("DECIMAL '{}'", input), w.toString(), "SQL string");
    }

    /**
     * Tests {@link IgniteSqlDecimalLiteral#clone(SqlParserPos)}.
     */
    @Test
    public void testClone() {
        var literal = IgniteSqlDecimalLiteral.create(BigDecimal.ONE, SqlParserPos.ZERO);

        var newPos = new SqlParserPos(1, 2);
        var literalAtPos = literal.clone(newPos);

        assertEquals(IgniteSqlDecimalLiteral.create(BigDecimal.ONE, newPos), literalAtPos, "clone with position");
    }

    /**
     * Tests {@link IgniteSqlDecimalLiteral#equalsDeep(SqlNode, Litmus)}.
     */
    @Test
    public void testEquality() {
        var decimal = IgniteSqlDecimalLiteral.create(BigDecimal.ONE, SqlParserPos.ZERO);
        var exactNumeric = SqlLiteral.createExactNumeric("1", SqlParserPos.ZERO);

        boolean equal = decimal.equalsDeep(exactNumeric, Litmus.IGNORE);
        assertFalse(equal, "decimal literal != exact numeric literal");

        var decimal2 = IgniteSqlDecimalLiteral.create(BigDecimal.ONE, SqlParserPos.ZERO);
        assertEquals(decimal, decimal2);
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
        assertThrowsSqlException(Sql.STMT_PARSE_ERR, "Invalid decimal literal", () -> parseQuery(query));
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
