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

package org.apache.ignite.internal.sql.engine.planner;

import static org.junit.jupiter.params.provider.Arguments.arguments;

import java.util.function.Predicate;
import java.util.function.UnaryOperator;
import java.util.stream.Stream;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.ignite.internal.sql.engine.framework.TestBuilders.TableBuilder;
import org.apache.ignite.internal.sql.engine.rel.IgniteValues;
import org.apache.ignite.internal.sql.engine.rel.ProjectableFilterableTableScan;
import org.apache.ignite.internal.sql.engine.schema.IgniteSchema;
import org.apache.ignite.internal.sql.engine.trait.IgniteDistributions;
import org.apache.ignite.internal.sql.engine.type.IgniteTypeFactory;
import org.apache.ignite.internal.type.NativeTypes;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/** Test to validate behaviour of constant reduction in binary comparison operation. */
public class OutOfRangeLiteralsInComparisonReductionPlannerTest extends AbstractPlannerTest {
    /**
     * Index bound checks - search key lies out of value range.
     */
    @ParameterizedTest
    @MethodSource("args")
    void testBoundsTypeLimits(RelDataType type, String expression, String expectedExpression) throws Exception {
        IgniteSchema schema = createSchemaFrom(table("TEST", "C2", type));

        Predicate<RelNode> matcher = expression(expectedExpression);

        assertPlan("SELECT * FROM test WHERE C2 " + expression, schema, matcher);
    }

    private static Stream<Arguments> args() {
        return Stream.of(
                // Column type, expr to use in condition, expected expression.

                // TINYINT

                arguments(sqlType(SqlTypeName.TINYINT), "= -129", "false"),
                arguments(sqlType(SqlTypeName.TINYINT), "= -128.1", "false"),
                arguments(sqlType(SqlTypeName.TINYINT), "= -128", "=($t1, -128)"),
                arguments(sqlType(SqlTypeName.TINYINT), "= -127.1", "false"),
                arguments(sqlType(SqlTypeName.TINYINT), "= 128", "false"),
                arguments(sqlType(SqlTypeName.TINYINT), "= 127.1", "false"),
                arguments(sqlType(SqlTypeName.TINYINT), "= 127", "=($t1, 127)"),
                arguments(sqlType(SqlTypeName.TINYINT), "= 126.1", "false"),

                arguments(sqlType(SqlTypeName.TINYINT), "IN (-129, -128.1)", "false"),
                arguments(sqlType(SqlTypeName.TINYINT), "IN (-128.1, -127.1)", "false"),
                arguments(sqlType(SqlTypeName.TINYINT), "IN (-128.1, -128, -127.1)", "=($t1, -128)"),
                arguments(sqlType(SqlTypeName.TINYINT), "IN (-128.1, -128, -127.1, -127)",
                        "SEARCH($t1, Sarg[-128:TINYINT, -127:TINYINT]:TINYINT)"),
                arguments(sqlType(SqlTypeName.TINYINT), "IN (128, 127.1)", "false"),
                arguments(sqlType(SqlTypeName.TINYINT), "IN (127.1, 126.1)", "false"),
                arguments(sqlType(SqlTypeName.TINYINT), "IN (127.1, 127, 126.1)", "=($t1, 127)"),
                arguments(sqlType(SqlTypeName.TINYINT), "IN (127.1, 127, 126.1, 126)",
                        "SEARCH($t1, Sarg[126:TINYINT, 127:TINYINT]:TINYINT)"),

                arguments(sqlType(SqlTypeName.TINYINT), ">= -129", "IS NOT NULL($t1)"),
                arguments(sqlType(SqlTypeName.TINYINT), ">= -128.1", "IS NOT NULL($t1)"),
                arguments(sqlType(SqlTypeName.TINYINT), ">= -128", ">=($t1, -128)"),
                arguments(sqlType(SqlTypeName.TINYINT), ">= -127.1", ">=($t1, -127)"),
                arguments(sqlType(SqlTypeName.TINYINT), ">= 128", "false"),
                arguments(sqlType(SqlTypeName.TINYINT), ">= 127.1", "false"),
                arguments(sqlType(SqlTypeName.TINYINT), ">= 127", ">=($t1, 127)"),
                arguments(sqlType(SqlTypeName.TINYINT), ">= 126.1", ">($t1, 126)"),

                arguments(sqlType(SqlTypeName.TINYINT), "> -129", "IS NOT NULL($t1)"),
                arguments(sqlType(SqlTypeName.TINYINT), "> -128.1", "IS NOT NULL($t1)"),
                arguments(sqlType(SqlTypeName.TINYINT), "> -128", ">($t1, -128)"),
                arguments(sqlType(SqlTypeName.TINYINT), "> -127.1", ">=($t1, -127)"),
                arguments(sqlType(SqlTypeName.TINYINT), "> 128", "false"),
                arguments(sqlType(SqlTypeName.TINYINT), "> 127.1", "false"),
                arguments(sqlType(SqlTypeName.TINYINT), "> 127", ">($t1, 127)"),
                arguments(sqlType(SqlTypeName.TINYINT), "> 126.1", ">($t1, 126)"),

                arguments(sqlType(SqlTypeName.TINYINT), "< -129", "false"),
                arguments(sqlType(SqlTypeName.TINYINT), "< -128.1", "false"),
                arguments(sqlType(SqlTypeName.TINYINT), "< -128", "<($t1, -128)"),
                arguments(sqlType(SqlTypeName.TINYINT), "< -127.1", "<($t1, -127)"),
                arguments(sqlType(SqlTypeName.TINYINT), "< 128", "IS NOT NULL($t1)"),
                arguments(sqlType(SqlTypeName.TINYINT), "< 127.1", "IS NOT NULL($t1)"),
                arguments(sqlType(SqlTypeName.TINYINT), "< 127", "<($t1, 127)"),
                arguments(sqlType(SqlTypeName.TINYINT), "< 126.1", "<=($t1, 126)"),

                arguments(sqlType(SqlTypeName.TINYINT), "<= -129", "false"),
                arguments(sqlType(SqlTypeName.TINYINT), "<= -128.1", "false"),
                arguments(sqlType(SqlTypeName.TINYINT), "<= -128", "<=($t1, -128)"),
                arguments(sqlType(SqlTypeName.TINYINT), "<= -127.1", "<($t1, -127)"),
                arguments(sqlType(SqlTypeName.TINYINT), "<= 128", "IS NOT NULL($t1)"),
                arguments(sqlType(SqlTypeName.TINYINT), "<= 127.1", "IS NOT NULL($t1)"),
                arguments(sqlType(SqlTypeName.TINYINT), "<= 127", "<=($t1, 127)"),
                arguments(sqlType(SqlTypeName.TINYINT), "<= 126.1", "<=($t1, 126)"),

                // SMALLINT

                arguments(sqlType(SqlTypeName.SMALLINT), "= -32769", "false"),
                arguments(sqlType(SqlTypeName.SMALLINT), "= -32768.1", "false"),
                arguments(sqlType(SqlTypeName.SMALLINT), "= -32768", "=($t1, -32768)"),
                arguments(sqlType(SqlTypeName.SMALLINT), "= -32767.1", "false"),
                arguments(sqlType(SqlTypeName.SMALLINT), "= 32768", "false"),
                arguments(sqlType(SqlTypeName.SMALLINT), "= 32767.1", "false"),
                arguments(sqlType(SqlTypeName.SMALLINT), "= 32767", "=($t1, 32767)"),
                arguments(sqlType(SqlTypeName.SMALLINT), "= 32766.1", "false"),

                arguments(sqlType(SqlTypeName.SMALLINT), "IN (-32769, -32768.1)", "false"),
                arguments(sqlType(SqlTypeName.SMALLINT), "IN (-32768.1, -32767.1)", "false"),
                arguments(sqlType(SqlTypeName.SMALLINT), "IN (-32768.1, -32768, -32767.1)", "=($t1, -32768)"),
                arguments(sqlType(SqlTypeName.SMALLINT), "IN (-32768.1, -32768, -32767.1, -32767)",
                        "SEARCH($t1, Sarg[-32768:SMALLINT, -32767:SMALLINT]:SMALLINT)"),
                arguments(sqlType(SqlTypeName.SMALLINT), "IN (32768, 32767.1)", "false"),
                arguments(sqlType(SqlTypeName.SMALLINT), "IN (32767.1, 32766.1)", "false"),
                arguments(sqlType(SqlTypeName.SMALLINT), "IN (32767.1, 32767, 32766.1)", "=($t1, 32767)"),
                arguments(sqlType(SqlTypeName.SMALLINT), "IN (32767.1, 32767, 32766.1, 32766)",
                        "SEARCH($t1, Sarg[32766:SMALLINT, 32767:SMALLINT]:SMALLINT)"),

                arguments(sqlType(SqlTypeName.SMALLINT), ">= -32769", "IS NOT NULL($t1)"),
                arguments(sqlType(SqlTypeName.SMALLINT), ">= -32768.1", "IS NOT NULL($t1)"),
                arguments(sqlType(SqlTypeName.SMALLINT), ">= -32768", ">=($t1, -32768)"),
                arguments(sqlType(SqlTypeName.SMALLINT), ">= -32767.1", ">=($t1, -32767)"),
                arguments(sqlType(SqlTypeName.SMALLINT), ">= 32768", "false"),
                arguments(sqlType(SqlTypeName.SMALLINT), ">= 32767.1", "false"),
                arguments(sqlType(SqlTypeName.SMALLINT), ">= 32767", ">=($t1, 32767)"),
                arguments(sqlType(SqlTypeName.SMALLINT), ">= 32766.1", ">($t1, 32766)"),

                arguments(sqlType(SqlTypeName.SMALLINT), "> -32769", "IS NOT NULL($t1)"),
                arguments(sqlType(SqlTypeName.SMALLINT), "> -32768.1", "IS NOT NULL($t1)"),
                arguments(sqlType(SqlTypeName.SMALLINT), "> -32768", ">($t1, -32768)"),
                arguments(sqlType(SqlTypeName.SMALLINT), "> -32767.1", ">=($t1, -32767)"),
                arguments(sqlType(SqlTypeName.SMALLINT), "> 32768", "false"),
                arguments(sqlType(SqlTypeName.SMALLINT), "> 32767.1", "false"),
                arguments(sqlType(SqlTypeName.SMALLINT), "> 32767", ">($t1, 32767)"),
                arguments(sqlType(SqlTypeName.SMALLINT), "> 32766.1", ">($t1, 32766)"),

                arguments(sqlType(SqlTypeName.SMALLINT), "< -32769", "false"),
                arguments(sqlType(SqlTypeName.SMALLINT), "< -32768.1", "false"),
                arguments(sqlType(SqlTypeName.SMALLINT), "< -32768", "<($t1, -32768)"),
                arguments(sqlType(SqlTypeName.SMALLINT), "< -32767.1", "<($t1, -32767)"),
                arguments(sqlType(SqlTypeName.SMALLINT), "< 32768", "IS NOT NULL($t1)"),
                arguments(sqlType(SqlTypeName.SMALLINT), "< 32767.1", "IS NOT NULL($t1)"),
                arguments(sqlType(SqlTypeName.SMALLINT), "< 32767", "<($t1, 32767)"),
                arguments(sqlType(SqlTypeName.SMALLINT), "< 32766.1", "<=($t1, 32766)"),

                arguments(sqlType(SqlTypeName.SMALLINT), "<= -32769", "false"),
                arguments(sqlType(SqlTypeName.SMALLINT), "<= -32768.1", "false"),
                arguments(sqlType(SqlTypeName.SMALLINT), "<= -32768", "<=($t1, -32768)"),
                arguments(sqlType(SqlTypeName.SMALLINT), "<= -32767.1", "<($t1, -32767)"),
                arguments(sqlType(SqlTypeName.SMALLINT), "<= 32768", "IS NOT NULL($t1)"),
                arguments(sqlType(SqlTypeName.SMALLINT), "<= 32767.1", "IS NOT NULL($t1)"),
                arguments(sqlType(SqlTypeName.SMALLINT), "<= 32767", "<=($t1, 32767)"),
                arguments(sqlType(SqlTypeName.SMALLINT), "<= 32766.1", "<=($t1, 32766)"),

                // INTEGER

                arguments(sqlType(SqlTypeName.INTEGER), "= -2147483649", "false"),
                arguments(sqlType(SqlTypeName.INTEGER), "= -2147483648.1", "false"),
                arguments(sqlType(SqlTypeName.INTEGER), "= -2147483648", "=($t1, -2147483648)"),
                arguments(sqlType(SqlTypeName.INTEGER), "= -2147483647.1", "false"),
                arguments(sqlType(SqlTypeName.INTEGER), "= 2147483648", "false"),
                arguments(sqlType(SqlTypeName.INTEGER), "= 2147483647.1", "false"),
                arguments(sqlType(SqlTypeName.INTEGER), "= 2147483647", "=($t1, 2147483647)"),
                arguments(sqlType(SqlTypeName.INTEGER), "= 2147483646.1", "false"),

                arguments(sqlType(SqlTypeName.INTEGER), "IN (-2147483649, -2147483648.1)", "false"),
                arguments(sqlType(SqlTypeName.INTEGER), "IN (-2147483648.1, -2147483647.1)", "false"),
                arguments(sqlType(SqlTypeName.INTEGER), "IN (-2147483648.1, -2147483648, -2147483647.1)", "=($t1, -2147483648)"),
                arguments(sqlType(SqlTypeName.INTEGER), "IN (-2147483648.1, -2147483648, -2147483647.1, -2147483647)",
                        "SEARCH($t1, Sarg[-2147483648, -2147483647])"),
                arguments(sqlType(SqlTypeName.INTEGER), "IN (2147483648, 2147483647.1)", "false"),
                arguments(sqlType(SqlTypeName.INTEGER), "IN (2147483647.1, 2147483646.1)", "false"),
                arguments(sqlType(SqlTypeName.INTEGER), "IN (2147483647.1, 2147483647, 2147483646.1)", "=($t1, 2147483647)"),
                arguments(sqlType(SqlTypeName.INTEGER), "IN (2147483647.1, 2147483647, 2147483646.1, 2147483646)",
                        "SEARCH($t1, Sarg[2147483646, 2147483647])"),

                arguments(sqlType(SqlTypeName.INTEGER), ">= -2147483649", "IS NOT NULL($t1)"),
                arguments(sqlType(SqlTypeName.INTEGER), ">= -2147483648.1", "IS NOT NULL($t1)"),
                arguments(sqlType(SqlTypeName.INTEGER), ">= -2147483648", ">=($t1, -2147483648)"),
                arguments(sqlType(SqlTypeName.INTEGER), ">= -2147483647.1", ">=($t1, -2147483647)"),
                arguments(sqlType(SqlTypeName.INTEGER), ">= 2147483648", "false"),
                arguments(sqlType(SqlTypeName.INTEGER), ">= 2147483647.1", "false"),
                arguments(sqlType(SqlTypeName.INTEGER), ">= 2147483647", ">=($t1, 2147483647)"),
                arguments(sqlType(SqlTypeName.INTEGER), ">= 2147483646.1", ">($t1, 2147483646)"),

                arguments(sqlType(SqlTypeName.INTEGER), "> -2147483649", "IS NOT NULL($t1)"),
                arguments(sqlType(SqlTypeName.INTEGER), "> -2147483648.1", "IS NOT NULL($t1)"),
                arguments(sqlType(SqlTypeName.INTEGER), "> -2147483648", ">($t1, -2147483648)"),
                arguments(sqlType(SqlTypeName.INTEGER), "> -2147483647.1", ">=($t1, -2147483647)"),
                arguments(sqlType(SqlTypeName.INTEGER), "> 2147483648", "false"),
                arguments(sqlType(SqlTypeName.INTEGER), "> 2147483647.1", "false"),
                arguments(sqlType(SqlTypeName.INTEGER), "> 2147483647", ">($t1, 2147483647)"),
                arguments(sqlType(SqlTypeName.INTEGER), "> 2147483646.1", ">($t1, 2147483646)"),

                arguments(sqlType(SqlTypeName.INTEGER), "< -2147483649", "false"),
                arguments(sqlType(SqlTypeName.INTEGER), "< -2147483648.1", "false"),
                arguments(sqlType(SqlTypeName.INTEGER), "< -2147483648", "<($t1, -2147483648)"),
                arguments(sqlType(SqlTypeName.INTEGER), "< -2147483647.1", "<($t1, -2147483647)"),
                arguments(sqlType(SqlTypeName.INTEGER), "< 2147483648", "IS NOT NULL($t1)"),
                arguments(sqlType(SqlTypeName.INTEGER), "< 2147483647.1", "IS NOT NULL($t1)"),
                arguments(sqlType(SqlTypeName.INTEGER), "< 2147483647", "<($t1, 2147483647)"),
                arguments(sqlType(SqlTypeName.INTEGER), "< 2147483646.1", "<=($t1, 2147483646)"),

                arguments(sqlType(SqlTypeName.INTEGER), "<= -2147483649", "false"),
                arguments(sqlType(SqlTypeName.INTEGER), "<= -2147483648.1", "false"),
                arguments(sqlType(SqlTypeName.INTEGER), "<= -2147483648", "<=($t1, -2147483648)"),
                arguments(sqlType(SqlTypeName.INTEGER), "<= -2147483647.1", "<($t1, -2147483647)"),
                arguments(sqlType(SqlTypeName.INTEGER), "<= 2147483648", "IS NOT NULL($t1)"),
                arguments(sqlType(SqlTypeName.INTEGER), "<= 2147483647.1", "IS NOT NULL($t1)"),
                arguments(sqlType(SqlTypeName.INTEGER), "<= 2147483647", "<=($t1, 2147483647)"),
                arguments(sqlType(SqlTypeName.INTEGER), "<= 2147483646.1", "<=($t1, 2147483646)"),

                // BIGINT

                arguments(sqlType(SqlTypeName.BIGINT), "= -9223372036854775809", "false"),
                arguments(sqlType(SqlTypeName.BIGINT), "= -9223372036854775808.1", "false"),
                arguments(sqlType(SqlTypeName.BIGINT), "= -9223372036854775808", "=($t1, -9223372036854775808)"),
                arguments(sqlType(SqlTypeName.BIGINT), "= -9223372036854775807.1", "false"),
                arguments(sqlType(SqlTypeName.BIGINT), "= 9223372036854775808", "false"),
                arguments(sqlType(SqlTypeName.BIGINT), "= 9223372036854775807.1", "false"),
                arguments(sqlType(SqlTypeName.BIGINT), "= 9223372036854775807", "=($t1, 9223372036854775807)"),
                arguments(sqlType(SqlTypeName.BIGINT), "= 9223372036854775806.1", "false"),

                arguments(sqlType(SqlTypeName.BIGINT), "IN (-9223372036854775809, -9223372036854775808.1)", "false"),
                arguments(sqlType(SqlTypeName.BIGINT), "IN (-9223372036854775808.1, -9223372036854775807.1)", "false"),
                arguments(sqlType(SqlTypeName.BIGINT), "IN (-9223372036854775808.1, -9223372036854775808, -9223372036854775807.1)",
                        "=($t1, -9223372036854775808)"),
                arguments(sqlType(SqlTypeName.BIGINT),
                        "IN (-9223372036854775808.1, -9223372036854775808, -9223372036854775807.1, -9223372036854775807)",
                        "SEARCH($t1, Sarg[-9223372036854775808L:BIGINT, -9223372036854775807L:BIGINT]:BIGINT)"),
                arguments(sqlType(SqlTypeName.BIGINT), "IN (9223372036854775808, 9223372036854775807.1)", "false"),
                arguments(sqlType(SqlTypeName.BIGINT), "IN (9223372036854775807.1, 9223372036854775806.1)", "false"),
                arguments(sqlType(SqlTypeName.BIGINT), "IN (9223372036854775807.1, 9223372036854775807, 9223372036854775806.1)",
                        "=($t1, 9223372036854775807)"),
                arguments(sqlType(SqlTypeName.BIGINT),
                        "IN (9223372036854775807.1, 9223372036854775807, 9223372036854775806.1, 9223372036854775806)",
                        "SEARCH($t1, Sarg[9223372036854775806L:BIGINT, 9223372036854775807L:BIGINT]:BIGINT)"),

                arguments(sqlType(SqlTypeName.BIGINT), ">= -9223372036854775809", "IS NOT NULL($t1)"),
                arguments(sqlType(SqlTypeName.BIGINT), ">= -9223372036854775808.1", "IS NOT NULL($t1)"),
                arguments(sqlType(SqlTypeName.BIGINT), ">= -9223372036854775808", ">=($t1, -9223372036854775808)"),
                arguments(sqlType(SqlTypeName.BIGINT), ">= -9223372036854775807.1", ">=($t1, -9223372036854775807)"),
                arguments(sqlType(SqlTypeName.BIGINT), ">= 9223372036854775808", "false"),
                arguments(sqlType(SqlTypeName.BIGINT), ">= 9223372036854775807.1", "false"),
                arguments(sqlType(SqlTypeName.BIGINT), ">= 9223372036854775807", ">=($t1, 9223372036854775807)"),
                arguments(sqlType(SqlTypeName.BIGINT), ">= 9223372036854775806.1", ">($t1, 9223372036854775806)"),

                arguments(sqlType(SqlTypeName.BIGINT), "> -9223372036854775809", "IS NOT NULL($t1)"),
                arguments(sqlType(SqlTypeName.BIGINT), "> -9223372036854775808.1", "IS NOT NULL($t1)"),
                arguments(sqlType(SqlTypeName.BIGINT), "> -9223372036854775808", ">($t1, -9223372036854775808)"),
                arguments(sqlType(SqlTypeName.BIGINT), "> -9223372036854775807.1", ">=($t1, -9223372036854775807)"),
                arguments(sqlType(SqlTypeName.BIGINT), "> 9223372036854775808", "false"),
                arguments(sqlType(SqlTypeName.BIGINT), "> 9223372036854775807.1", "false"),
                arguments(sqlType(SqlTypeName.BIGINT), "> 9223372036854775807", ">($t1, 9223372036854775807)"),
                arguments(sqlType(SqlTypeName.BIGINT), "> 9223372036854775806.1", ">($t1, 9223372036854775806)"),

                arguments(sqlType(SqlTypeName.BIGINT), "< -9223372036854775809", "false"),
                arguments(sqlType(SqlTypeName.BIGINT), "< -9223372036854775808.1", "false"),
                arguments(sqlType(SqlTypeName.BIGINT), "< -9223372036854775808", "<($t1, -9223372036854775808)"),
                arguments(sqlType(SqlTypeName.BIGINT), "< -9223372036854775807.1", "<($t1, -9223372036854775807)"),
                arguments(sqlType(SqlTypeName.BIGINT), "< 9223372036854775808", "IS NOT NULL($t1)"),
                arguments(sqlType(SqlTypeName.BIGINT), "< 9223372036854775807.1", "IS NOT NULL($t1)"),
                arguments(sqlType(SqlTypeName.BIGINT), "< 9223372036854775807", "<($t1, 9223372036854775807)"),
                arguments(sqlType(SqlTypeName.BIGINT), "< 9223372036854775806.1", "<=($t1, 9223372036854775806)"),

                arguments(sqlType(SqlTypeName.BIGINT), "<= -9223372036854775809", "false"),
                arguments(sqlType(SqlTypeName.BIGINT), "<= -9223372036854775808.1", "false"),
                arguments(sqlType(SqlTypeName.BIGINT), "<= -9223372036854775808", "<=($t1, -9223372036854775808)"),
                arguments(sqlType(SqlTypeName.BIGINT), "<= -9223372036854775807.1", "<($t1, -9223372036854775807)"),
                arguments(sqlType(SqlTypeName.BIGINT), "<= 9223372036854775808", "IS NOT NULL($t1)"),
                arguments(sqlType(SqlTypeName.BIGINT), "<= 9223372036854775807.1", "IS NOT NULL($t1)"),
                arguments(sqlType(SqlTypeName.BIGINT), "<= 9223372036854775807", "<=($t1, 9223372036854775807)"),
                arguments(sqlType(SqlTypeName.BIGINT), "<= 9223372036854775806.1", "<=($t1, 9223372036854775806)"),

                // DECIMAL(1,0)

                arguments(sqlType(SqlTypeName.DECIMAL, 1), "= -10", "false"),
                arguments(sqlType(SqlTypeName.DECIMAL, 1), "= -9.1", "false"),
                arguments(sqlType(SqlTypeName.DECIMAL, 1), "= -9.0", "=($t1, -9)"),
                arguments(sqlType(SqlTypeName.DECIMAL, 1), "= -9", "=($t1, -9)"),
                arguments(sqlType(SqlTypeName.DECIMAL, 1), "= -8.1", "false"),
                arguments(sqlType(SqlTypeName.DECIMAL, 1), "= 10", "false"),
                arguments(sqlType(SqlTypeName.DECIMAL, 1), "= 9.1", "false"),
                arguments(sqlType(SqlTypeName.DECIMAL, 1), "= 9.0", "=($t1, 9)"),
                arguments(sqlType(SqlTypeName.DECIMAL, 1), "= 9", "=($t1, 9)"),
                arguments(sqlType(SqlTypeName.DECIMAL, 1), "= 8.1", "false"),

                arguments(sqlType(SqlTypeName.DECIMAL, 1), "IN (-10, -9.1)", "false"),
                arguments(sqlType(SqlTypeName.DECIMAL, 1), "IN (-9.1, -8.1)", "false"),
                arguments(sqlType(SqlTypeName.DECIMAL, 1), "IN (-9.1, -9, -8.1)", "=($t1, -9)"),
                arguments(sqlType(SqlTypeName.DECIMAL, 1), "IN (-9.1, -9, -8.1, -8)",
                        "SEARCH($t1, Sarg[-9:DECIMAL(1, 0), -8:DECIMAL(1, 0)]:DECIMAL(1, 0))"),
                arguments(sqlType(SqlTypeName.DECIMAL, 1), "IN (10, 9.1)", "false"),
                arguments(sqlType(SqlTypeName.DECIMAL, 1), "IN (9.1, 8.1)", "false"),
                arguments(sqlType(SqlTypeName.DECIMAL, 1), "IN (9.1, 9, 8.1)", "=($t1, 9)"),
                arguments(sqlType(SqlTypeName.DECIMAL, 1), "IN (9.1, 9, 8.1, 8)",
                        "SEARCH($t1, Sarg[8:DECIMAL(1, 0), 9:DECIMAL(1, 0)]:DECIMAL(1, 0))"),

                arguments(sqlType(SqlTypeName.DECIMAL, 1), ">= -10", "IS NOT NULL($t1)"),
                arguments(sqlType(SqlTypeName.DECIMAL, 1), ">= -9.1", "IS NOT NULL($t1)"),
                arguments(sqlType(SqlTypeName.DECIMAL, 1), ">= -9.0", ">=($t1, -9)"),
                arguments(sqlType(SqlTypeName.DECIMAL, 1), ">= -9", ">=($t1, -9)"),
                arguments(sqlType(SqlTypeName.DECIMAL, 1), ">= -8.1", ">=($t1, -8)"),
                arguments(sqlType(SqlTypeName.DECIMAL, 1), ">= 10", "false"),
                arguments(sqlType(SqlTypeName.DECIMAL, 1), ">= 9.1", "false"),
                arguments(sqlType(SqlTypeName.DECIMAL, 1), ">= 9.0", ">=($t1, 9)"),
                arguments(sqlType(SqlTypeName.DECIMAL, 1), ">= 9", ">=($t1, 9)"),
                arguments(sqlType(SqlTypeName.DECIMAL, 1), ">= 8.1", ">($t1, 8)"),

                arguments(sqlType(SqlTypeName.DECIMAL, 1), "> -10", "IS NOT NULL($t1)"),
                arguments(sqlType(SqlTypeName.DECIMAL, 1), "> -9.1", "IS NOT NULL($t1)"),
                arguments(sqlType(SqlTypeName.DECIMAL, 1), "> -9.0", ">($t1, -9)"),
                arguments(sqlType(SqlTypeName.DECIMAL, 1), "> -9", ">($t1, -9)"),
                arguments(sqlType(SqlTypeName.DECIMAL, 1), "> -8.1", ">=($t1, -8)"),
                arguments(sqlType(SqlTypeName.DECIMAL, 1), "> 10", "false"),
                arguments(sqlType(SqlTypeName.DECIMAL, 1), "> 9.1", "false"),
                arguments(sqlType(SqlTypeName.DECIMAL, 1), "> 9.0", ">($t1, 9)"),
                arguments(sqlType(SqlTypeName.DECIMAL, 1), "> 9", ">($t1, 9)"),
                arguments(sqlType(SqlTypeName.DECIMAL, 1), "> 8.1", ">($t1, 8)"),

                arguments(sqlType(SqlTypeName.DECIMAL, 1), "< -10", "false"),
                arguments(sqlType(SqlTypeName.DECIMAL, 1), "< -9.1", "false"),
                arguments(sqlType(SqlTypeName.DECIMAL, 1), "< -9.0", "<($t1, -9)"),
                arguments(sqlType(SqlTypeName.DECIMAL, 1), "< -9", "<($t1, -9)"),
                arguments(sqlType(SqlTypeName.DECIMAL, 1), "< -8.1", "<($t1, -8)"),
                arguments(sqlType(SqlTypeName.DECIMAL, 1), "< 10", "IS NOT NULL($t1)"),
                arguments(sqlType(SqlTypeName.DECIMAL, 1), "< 9.1", "IS NOT NULL($t1)"),
                arguments(sqlType(SqlTypeName.DECIMAL, 1), "< 9.0", "<($t1, 9)"),
                arguments(sqlType(SqlTypeName.DECIMAL, 1), "< 9", "<($t1, 9)"),
                arguments(sqlType(SqlTypeName.DECIMAL, 1), "< 8.1", "<=($t1, 8)"),

                arguments(sqlType(SqlTypeName.DECIMAL, 1), "<= -10", "false"),
                arguments(sqlType(SqlTypeName.DECIMAL, 1), "<= -9.1", "false"),
                arguments(sqlType(SqlTypeName.DECIMAL, 1), "<= -9.0", "<=($t1, -9)"),
                arguments(sqlType(SqlTypeName.DECIMAL, 1), "<= -9", "<=($t1, -9)"),
                arguments(sqlType(SqlTypeName.DECIMAL, 1), "<= -8.1", "<($t1, -8)"),
                arguments(sqlType(SqlTypeName.DECIMAL, 1), "<= 10", "IS NOT NULL($t1)"),
                arguments(sqlType(SqlTypeName.DECIMAL, 1), "<= 9.1", "IS NOT NULL($t1)"),
                arguments(sqlType(SqlTypeName.DECIMAL, 1), "<= 9.0", "<=($t1, 9)"),
                arguments(sqlType(SqlTypeName.DECIMAL, 1), "<= 9", "<=($t1, 9)"),
                arguments(sqlType(SqlTypeName.DECIMAL, 1), "<= 8.1", "<=($t1, 8)"),

                // DECIMAL(2,1)

                arguments(sqlType(SqlTypeName.DECIMAL, 2, 1), "= -10", "false"),
                arguments(sqlType(SqlTypeName.DECIMAL, 2, 1), "= -9.91", "false"),
                arguments(sqlType(SqlTypeName.DECIMAL, 2, 1), "= -9.90", "=($t1, -9.9)"),
                arguments(sqlType(SqlTypeName.DECIMAL, 2, 1), "= -9.9", "=($t1, -9.9)"),
                arguments(sqlType(SqlTypeName.DECIMAL, 2, 1), "= -8.91", "false"),
                arguments(sqlType(SqlTypeName.DECIMAL, 2, 1), "= 10", "false"),
                arguments(sqlType(SqlTypeName.DECIMAL, 2, 1), "= 9.91", "false"),
                arguments(sqlType(SqlTypeName.DECIMAL, 2, 1), "= 9.90", "=($t1, 9.9)"),
                arguments(sqlType(SqlTypeName.DECIMAL, 2, 1), "= 9.9", "=($t1, 9.9)"),
                arguments(sqlType(SqlTypeName.DECIMAL, 2, 1), "= 8.91", "false"),

                arguments(sqlType(SqlTypeName.DECIMAL, 2, 1), "IN (-10, -9.91)", "false"),
                arguments(sqlType(SqlTypeName.DECIMAL, 2, 1), "IN (-9.91, -8.91)", "false"),
                arguments(sqlType(SqlTypeName.DECIMAL, 2, 1), "IN (-9.91, -9.9, -8.91)", "=($t1, -9.9)"),
                arguments(sqlType(SqlTypeName.DECIMAL, 2, 1), "IN (-9.91, -9.9, -8.91, -8.9)",
                        "SEARCH($t1, Sarg[-9.9:DECIMAL(2, 1), -8.9:DECIMAL(2, 1)]:DECIMAL(2, 1))"),
                arguments(sqlType(SqlTypeName.DECIMAL, 2, 1), "IN (10, 9.91)", "false"),
                arguments(sqlType(SqlTypeName.DECIMAL, 2, 1), "IN (9.91, 8.91)", "false"),
                arguments(sqlType(SqlTypeName.DECIMAL, 2, 1), "IN (9.91, 9.9, 8.91)", "=($t1, 9.9)"),
                arguments(sqlType(SqlTypeName.DECIMAL, 2, 1), "IN (9.91, 9.9, 8.91, 8.9)",
                        "SEARCH($t1, Sarg[8.9:DECIMAL(2, 1), 9.9:DECIMAL(2, 1)]:DECIMAL(2, 1))"),

                arguments(sqlType(SqlTypeName.DECIMAL, 2, 1), ">= -10", "IS NOT NULL($t1)"),
                arguments(sqlType(SqlTypeName.DECIMAL, 2, 1), ">= -9.91", "IS NOT NULL($t1)"),
                arguments(sqlType(SqlTypeName.DECIMAL, 2, 1), ">= -9.90", ">=($t1, -9.9)"),
                arguments(sqlType(SqlTypeName.DECIMAL, 2, 1), ">= -9.9", ">=($t1, -9.9)"),
                arguments(sqlType(SqlTypeName.DECIMAL, 2, 1), ">= -8.91", ">=($t1, -8.9)"),
                arguments(sqlType(SqlTypeName.DECIMAL, 2, 1), ">= 10", "false"),
                arguments(sqlType(SqlTypeName.DECIMAL, 2, 1), ">= 9.91", "false"),
                arguments(sqlType(SqlTypeName.DECIMAL, 2, 1), ">= 9.90", ">=($t1, 9.9)"),
                arguments(sqlType(SqlTypeName.DECIMAL, 2, 1), ">= 9.9", ">=($t1, 9.9)"),
                arguments(sqlType(SqlTypeName.DECIMAL, 2, 1), ">= 8.91", ">($t1, 8.9)"),

                arguments(sqlType(SqlTypeName.DECIMAL, 2, 1), "> -10", "IS NOT NULL($t1)"),
                arguments(sqlType(SqlTypeName.DECIMAL, 2, 1), "> -9.91", "IS NOT NULL($t1)"),
                arguments(sqlType(SqlTypeName.DECIMAL, 2, 1), "> -9.90", ">($t1, -9.9)"),
                arguments(sqlType(SqlTypeName.DECIMAL, 2, 1), "> -9.9", ">($t1, -9.9)"),
                arguments(sqlType(SqlTypeName.DECIMAL, 2, 1), "> -8.91", ">=($t1, -8.9)"),
                arguments(sqlType(SqlTypeName.DECIMAL, 2, 1), "> 10", "false"),
                arguments(sqlType(SqlTypeName.DECIMAL, 2, 1), "> 9.91", "false"),
                arguments(sqlType(SqlTypeName.DECIMAL, 2, 1), "> 9.90", ">($t1, 9.9)"),
                arguments(sqlType(SqlTypeName.DECIMAL, 2, 1), "> 9.9", ">($t1, 9.9)"),
                arguments(sqlType(SqlTypeName.DECIMAL, 2, 1), "> 8.91", ">($t1, 8.9)"),

                arguments(sqlType(SqlTypeName.DECIMAL, 2, 1), "< -10", "false"),
                arguments(sqlType(SqlTypeName.DECIMAL, 2, 1), "< -9.91", "false"),
                arguments(sqlType(SqlTypeName.DECIMAL, 2, 1), "< -9.90", "<($t1, -9.9)"),
                arguments(sqlType(SqlTypeName.DECIMAL, 2, 1), "< -9.9", "<($t1, -9.9)"),
                arguments(sqlType(SqlTypeName.DECIMAL, 2, 1), "< -8.91", "<($t1, -8.9)"),
                arguments(sqlType(SqlTypeName.DECIMAL, 2, 1), "< 10", "IS NOT NULL($t1)"),
                arguments(sqlType(SqlTypeName.DECIMAL, 2, 1), "< 9.91", "IS NOT NULL($t1)"),
                arguments(sqlType(SqlTypeName.DECIMAL, 2, 1), "< 9.90", "<($t1, 9.9)"),
                arguments(sqlType(SqlTypeName.DECIMAL, 2, 1), "< 9.9", "<($t1, 9.9)"),
                arguments(sqlType(SqlTypeName.DECIMAL, 2, 1), "< 8.91", "<=($t1, 8.9)"),

                arguments(sqlType(SqlTypeName.DECIMAL, 2, 1), "<= -10", "false"),
                arguments(sqlType(SqlTypeName.DECIMAL, 2, 1), "<= -9.91", "false"),
                arguments(sqlType(SqlTypeName.DECIMAL, 2, 1), "<= -9.90", "<=($t1, -9.9)"),
                arguments(sqlType(SqlTypeName.DECIMAL, 2, 1), "<= -9.9", "<=($t1, -9.9)"),
                arguments(sqlType(SqlTypeName.DECIMAL, 2, 1), "<= -8.91", "<($t1, -8.9)"),
                arguments(sqlType(SqlTypeName.DECIMAL, 2, 1), "<= 10", "IS NOT NULL($t1)"),
                arguments(sqlType(SqlTypeName.DECIMAL, 2, 1), "<= 9.91", "IS NOT NULL($t1)"),
                arguments(sqlType(SqlTypeName.DECIMAL, 2, 1), "<= 9.90", "<=($t1, 9.9)"),
                arguments(sqlType(SqlTypeName.DECIMAL, 2, 1), "<= 9.9", "<=($t1, 9.9)"),
                arguments(sqlType(SqlTypeName.DECIMAL, 2, 1), "<= 8.91", "<=($t1, 8.9)")
        );
    }

    private static RelDataType sqlType(SqlTypeName typeName) {
        return TYPE_FACTORY.createSqlType(typeName);
    }

    private static RelDataType sqlType(SqlTypeName typeName, int precision) {
        return TYPE_FACTORY.createSqlType(typeName, precision);
    }

    private static RelDataType sqlType(SqlTypeName typeName, int precision, int scale) {
        return TYPE_FACTORY.createSqlType(typeName, precision, scale);
    }

    private static Predicate<RelNode> emptyValuesNode() {
        return byClass(IgniteValues.class).and(node -> ((IgniteValues) node).tuples.isEmpty());
    }

    private static Predicate<RelNode> expression(String expected) {
        return byClass(ProjectableFilterableTableScan.class)
                .and(node -> {
                    var tableScan = (ProjectableFilterableTableScan) node;
                    RexNode condition = tableScan.condition();
                    return condition != null && condition.toString().equals(expected);

                });
    }

    private static UnaryOperator<TableBuilder> table(String tableName, String column, RelDataType type) {
        return tableBuilder -> tableBuilder
                .name(tableName)
                .addColumn("C1", NativeTypes.INT32)
                .addColumn(column, IgniteTypeFactory.relDataTypeToNative(type))
                .size(400)
                .distribution(IgniteDistributions.single());
    }
}
