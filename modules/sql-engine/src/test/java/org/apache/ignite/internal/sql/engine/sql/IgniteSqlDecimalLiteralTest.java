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

import static org.apache.ignite.lang.IgniteStringFormatter.format;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.util.stream.Stream;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.pretty.SqlPrettyWriter;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Litmus;
import org.apache.ignite.internal.sql.engine.planner.AbstractPlannerTest;
import org.apache.ignite.internal.sql.engine.type.IgniteTypeFactory;
import org.apache.ignite.internal.sql.engine.util.Commons;
import org.apache.ignite.sql.SqlException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Tests for {@link IgniteSqlDecimalLiteral}.
 */
public class IgniteSqlDecimalLiteralTest extends AbstractPlannerTest {

    /**
     * Test cases for different precision and scale values.
     */
    @ParameterizedTest
    @CsvSource({
            "-0, 1, 0",
            "-0.001, 1, 3",
            "-1, 1, 0",
            "-10.001, 5, 3",
            "-20, 2, 0",
            "-20.0, 3, 1",
            "-20.001, 5, 3",

            "0, 1, 0",

            "0.0, 1, 1",
            "0.001, 1, 3",
            "1, 1, 0",
            "10.001, 5, 3",
            "20, 2, 0",
            "20.0, 3, 1",
            "20.001, 5, 3",
    })
    public void testValueAndType(String val, int precision, int scale) {
        var literal = IgniteSqlDecimalLiteral.create(val, SqlParserPos.ZERO);
        assertTrue(literal.isExact(), "decimal is always exact");

        assertEquals(Integer.valueOf(precision), literal.getPrec(), "precision");
        assertEquals(Integer.valueOf(scale), literal.getScale(), "scale");

        var decimalValue = new BigDecimal(val);
        assertEquals(decimalValue, literal.getValue(), "value");

        var typeFactory = new IgniteTypeFactory();
        var actualType = literal.createSqlType(typeFactory);

        var expectedType = typeFactory.createSqlType(SqlTypeName.DECIMAL, decimalValue.precision(), decimalValue.scale());
        assertEquals(expectedType, actualType, "type");
    }

    /**
     * Tests {@link IgniteSqlDecimalLiteral#unparse(SqlWriter, int, int)}.
     */
    @ParameterizedTest
    @MethodSource("decimalLiterals")
    public void testToSql(String input, String expected) {
        var literal = IgniteSqlDecimalLiteral.create(input, SqlParserPos.ZERO);

        var w = new SqlPrettyWriter();
        literal.unparse(w, 0, 0);

        assertEquals(format("DECIMAL '{}'", expected), w.toString(), "SQL string");
    }

    /**
     * Tests {@link IgniteSqlDecimalLiteral#clone(SqlParserPos)}.
     */
    @Test
    public void testClone() {
        var literal = IgniteSqlDecimalLiteral.create("1", SqlParserPos.ZERO);

        var newPos = new SqlParserPos(1, 2);
        var literalAtPos = literal.clone(newPos);

        assertEquals(IgniteSqlDecimalLiteral.create("1", newPos), literalAtPos, "clone with position");
    }

    /**
     * Tests {@link IgniteSqlDecimalLiteral#equalsDeep(SqlNode, Litmus)}.
     */
    @Test
    public void testEquality() {
        var decimal = IgniteSqlDecimalLiteral.create("1", SqlParserPos.ZERO);
        var exactNumeric = SqlLiteral.createExactNumeric("1", SqlParserPos.ZERO);

        boolean equal = decimal.equalsDeep(exactNumeric, Litmus.IGNORE);
        assertFalse(equal, "decimal literal != exact numeric literal");

        var decimal2 = IgniteSqlDecimalLiteral.create("1", SqlParserPos.ZERO);
        assertEquals(decimal, decimal2);
    }

    /**
     * Test cases for invalid inputs.
     */
    @ParameterizedTest
    @ValueSource(strings = {
            "DECIMAL 'trash'",
            "DECIMAL '10a'",
            "DECIMAL 'a10'",
            "DECIMAL x'1'",
            "DECIMAL '1\n1000'",
    })
    public void testParserRejectsInvalidValues(String value) {
        var query = format("SELECT {}", value);
        assertThrows(SqlException.class, () -> Commons.parse(query, Commons.PARSER_CONFIG));
    }

    private static Stream<Arguments> decimalLiterals() {
        return Stream.of(
                Arguments.of("-10.0", "-10.0"),
                Arguments.of("-10.123", "-10.123"),
                Arguments.of("-10.000", "-10.000"),

                Arguments.of("-0", "0"),
                Arguments.of("0", "0"),
                Arguments.of("+0", "0"),
                Arguments.of(".0", "0.0"),
                Arguments.of("000.0", "0.0"),

                Arguments.of("1", "1"),
                Arguments.of("10.0", "10.0"),
                Arguments.of("10.123", "10.123"),
                Arguments.of("10.000", "10.000")
        );
    }
}
