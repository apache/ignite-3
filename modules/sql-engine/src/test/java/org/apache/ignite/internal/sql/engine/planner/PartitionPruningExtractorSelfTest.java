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

import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;
import static org.junit.jupiter.api.Assertions.assertEquals;

import it.unimi.dsi.fastutil.ints.IntImmutableList;
import it.unimi.dsi.fastutil.ints.IntList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.ignite.internal.sql.engine.prepare.pruning.PartitionPruningColumns;
import org.apache.ignite.internal.sql.engine.prepare.pruning.PartitionPruningMetadataExtractor;
import org.apache.ignite.internal.sql.engine.util.Commons;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/**
 * Tests basic operations that extract partition pruning metadata using non-optimized expressions. For tests against optimized expressions
 * see {@link PartitionPruningMetadataTest}.
 */
public class PartitionPruningExtractorSelfTest extends BaseIgniteAbstractTest {

    private static final RelDataTypeFactory TYPE_FACTORY = Commons.typeFactory();

    private static final RexBuilder rexBuilder = new RexBuilder(TYPE_FACTORY);

    enum TestCase {
        SIMPLE_1a(
                // col0 = 1 => [0=1] colo keys = [0]
                eq(intCol(0), lit(1)),
                List.of(0), List.of("[0=1]")
        ),
        SIMPLE_1b(
                // 1 = col0 same result as col1 = 1
                eq(lit(1), intCol(0)),
                List.of(0), List.of("[0=1]")
        ),
        SIMPLE_2c(
                // another colo key.
                eq(intCol(0), lit(1)),
                List.of(1), List.of()
        ),
        SIMPLE_2d(
                // col1 key is missing
                eq(intCol(0), lit(1)),
                List.of(0, 1), List.of()
        ),

        // OR

        OR_1a(
                // col0 = 1 OR col0 = 2 => two alternatives
                or(
                        eq(intCol(0), lit(1)),
                        eq(intCol(0), lit(2))
                ),
                List.of(0),
                List.of("[0=1]", "[0=2]")
        ),

        OR_1b(
                // col0 = 1 OR col1 = 1 => no result both as both cols are colo keys.
                or(
                        eq(intCol(0), lit(1)),
                        eq(intCol(1), lit(2))
                ),
                List.of(0, 1),
                List.of()
        ),

        OR_1c(
                // col0 = 1 OR col1 = 2 => no result col1 is not a colo key
                or(
                        eq(intCol(0), lit(1)),
                        eq(intCol(1), lit(2))
                ),
                List.of(0),
                List.of()
        ),

        AND_1a(
                // col0 = 1 AND col1 = 2 => [0=1, 1=2]
                and(
                        eq(intCol(0), lit(1)),
                        eq(intCol(1), lit(2))
                ),
                List.of(0, 1),
                List.of("[0=1, 1=2]")
        ),
        AND_1b(
                // col0 = 1 AND col0 = 2 => contradiction => no result
                and(
                        eq(intCol(0), lit(1)),
                        eq(intCol(0), lit(2))
                ),
                List.of(0),
                List.of()
        ),
        AND_1c(
                // col0 = 1 AND col1 = 2 => [0=1] since col1 is not a colo key
                and(
                        eq(intCol(0), lit(1)),
                        eq(intCol(1), lit(2))
                ),
                List.of(0),
                List.of("[0=1]")
        ),
        AND_1d(
                // col0 = 1 AND col1 = 2 AND col0 = 1 => [0=1, 1=2]
                and(
                        eq(intCol(0), lit(1)),
                        eq(intCol(1), lit(2)), eq(intCol(0), lit(1))
                ),
                List.of(0, 1),
                List.of("[0=1, 1=2]")
        ),

        DYNAMIC_PARAM_1a(
                // col0 = ?
                eq(intCol(0), intDynParam(0)),
                List.of(0),
                List.of("[0=?0]")
        ),

        DYNAMIC_PARAM_1b(
                // col0 = 1 OR col0 = ?
                or(
                        eq(intCol(0), lit(1)),
                        eq(intCol(0), intDynParam(0))
                ),
                List.of(0),
                List.of("[0=1]", "[0=?0]")
        ),

        DYNAMIC_PARAM_1c(
                // col0 = 1 AND col0 = ? => contradiction.
                and(
                        eq(intCol(0), lit(1)),
                        eq(intCol(0), intDynParam(0))
                ),
                List.of(0),
                List.of()
        ),

        COMPOUND_1a(
                // col0 = 1 AND col2 = 2 OR col0=3 AND col2=4
                or(
                        and(
                                eq(intCol(0), lit(1)),
                                eq(intCol(1), lit(2))
                        ),
                        and(
                                eq(intCol(0), lit(3)),
                                eq(intCol(1), lit(4))
                        )
                ),
                List.of(0, 1), List.of("[0=1, 1=2]", "[0=3, 1=4]")
        ),
        COMPOUND_1b(
                // col0 = 1 AND col2 = 2 OR col0=3 => []
                or(
                        and(
                                eq(intCol(0), lit(1)),
                                eq(intCol(1), lit(2))
                        ),
                        eq(intCol(0), lit(3))
                ),
                List.of(0, 1),
                List.of()
        ),
        COMPOUND_1c(
                // col0 = 1 AND col2 = 2 OR col0=3 => [col=1], [col0=3] since col0 is the only colo key.
                or(
                        and(
                                eq(intCol(0), lit(1)),
                                eq(intCol(1), lit(2))
                        ),
                        eq(intCol(0), lit(3))
                ),
                List.of(0),
                List.of("[0=1]", "[0=3]")
        ),

        // NEGATE

        NEGATE_1a(
                // NOT(col0 = 1) => col0 != 1 => []
                not(eq(intCol(0), lit(1))),
                List.of(0),
                List.of()
        ),
        NEGATE_1b(
                // NOT(col0 != 1) => col0 = 1 => [0=1]
                not(notEq(intCol(0), lit(1))),
                List.of(0),
                List.of("[0=1]")
        ),
        NEGATE_1c(
                // IS_NOT_TRUE(col0 = 1) => col0 != 1 => []
                call(SqlStdOperatorTable.IS_NOT_TRUE, eq(intCol(0), lit(1))),
                List.of(0),
                List.of()
        ),
        NEGATE_1d(
                // IS_NOT_TRUE(col0 != 1) => col0 = 1 => [0=1]
                call(SqlStdOperatorTable.IS_NOT_TRUE, notEq(intCol(0), lit(1))),
                List.of(0),
                List.of("[0=1]")
        ),

        // bools
        NEGATE_2a(
                // NOT(bool col0) => [col0=false]
                not(boolCol(0)), List.of(0), List.of("[0=false]")
        ),
        NEGATE_2b(
                // NOT(NOT(bool col0)) => [col0=true]
                not(not(boolCol(0))),
                List.of(0), List.of("[0=true]")
        ),
        NEGATE_2c(
                // NOT(IS_TRUE(bool col0)) => [col0=false]
                not(call(SqlStdOperatorTable.IS_TRUE, boolCol(0))),
                List.of(0), List.of("[0=false]")
        ),
        NEGATE_1e(
                // NOT(IS_FALSE(bool col0)) => [col0=true]
                not(call(SqlStdOperatorTable.IS_FALSE, boolCol(0))),
                List.of(0), List.of("[0=true]")
        ),
        NEGATE_2f(
                // IS_NOT_TRUE(bool col0) => [col0=false]
                call(SqlStdOperatorTable.IS_NOT_TRUE, boolCol(0)),
                List.of(0), List.of("[0=false]")
        ),
        NEGATE_2g(
                // IS_NOT_FALSE(bool col0) => [col0=true]
                call(SqlStdOperatorTable.IS_NOT_FALSE, boolCol(0)),
                List.of(0), List.of("[0=true]")
        ),
        NEGATE_2h(
                // NOT(IS_NOT_TRUE(bool col0)) => [col0=true]
                not(call(SqlStdOperatorTable.IS_NOT_TRUE, boolCol(0))),
                List.of(0), List.of("[0=true]")
        ),
        NEGATE_2i(
                // NOT(IS_NOT_FALSE(bool col0)) => [col0=false]
                not(call(SqlStdOperatorTable.IS_NOT_FALSE, boolCol(0))),
                List.of(0), List.of("[0=false]")
        ),

        IS_NULL_1a(
                // col0 IS NULL
                call(SqlStdOperatorTable.IS_NULL, intCol(0)),
                List.of(0), List.of()
        ),
        IS_NULL_1b(
                // col0 IS NOT NULL
                call(SqlStdOperatorTable.IS_NULL, intCol(0)),
                List.of(0), List.of()
        )

        ;

        final RexNode condition;

        final List<Integer> keys;

        final List<String> expected;

        TestCase(RexNode condition, List<Integer> keys, List<String> expected) {
            this.condition = condition;
            this.keys = keys;
            this.expected = expected;
        }

        @Override
        public String toString() {
            return condition + " " + keys + " -> " + expected;
        }
    }

    @ParameterizedTest
    @EnumSource(TestCase.class)
    public void test(TestCase test) {
        expectMetadata(test.condition, test.keys, test.expected);
    }

    private void expectMetadata(RexNode condition, List<Integer> keys, List<String> expectedMetadata) {
        log.info("Condition: {}", condition);
        log.info("Keys: {}", keys);
        log.info("Expected metadata: {}", expectedMetadata);

        IntList keyList = IntImmutableList.toList(keys.stream().mapToInt(Integer::intValue));
        PartitionPruningColumns actualMetadata = PartitionPruningMetadataExtractor.extractMetadata(keyList, condition, rexBuilder);

        List<String> actual;

        if (actualMetadata == null) {
            actual = Collections.emptyList();
        } else {
            // [1 = RexExpr1, 2 = RexExpr2] => [1 = "expr1-as-string", 2 = "expr2-as-string" ]
            actual = PartitionPruningColumns.canonicalForm(actualMetadata).stream()
                    .map(String::valueOf)
                    .collect(Collectors.toList());
        }

        log.info("Actual metadata: {}", actual);

        assertEquals(expectedMetadata, actual);
    }

    private static RexNode intCol(int i) {
        return rexBuilder.makeLocalRef(TYPE_FACTORY.createSqlType(SqlTypeName.INTEGER), i);
    }

    private static RexNode boolCol(int i) {
        return rexBuilder.makeLocalRef(TYPE_FACTORY.createSqlType(SqlTypeName.INTEGER), i);
    }

    private static RexNode lit(Object value) {
        if (value instanceof Boolean) {
            return rexBuilder.makeLiteral(value, type(SqlTypeName.BOOLEAN));
        } else if (value instanceof Integer) {
            return rexBuilder.makeLiteral(value, type(SqlTypeName.INTEGER));
        } else if (value == null) {
            return rexBuilder.makeLiteral(null, type(SqlTypeName.NULL));
        } else {
            throw new IllegalArgumentException(format("Unexpected literal: <{}> of type: {}", value, value.getClass()));
        }
    }

    private static RexNode intDynParam(int i) {
        return rexBuilder.makeDynamicParam(TYPE_FACTORY.createSqlType(SqlTypeName.INTEGER), i);
    }

    private static RexNode or(RexNode... exprs) {
        return rexBuilder.makeCall(SqlStdOperatorTable.OR, exprs);
    }

    private static RexNode and(RexNode... exprs) {
        return rexBuilder.makeCall(SqlStdOperatorTable.AND, exprs);
    }

    private static RexNode eq(RexNode lhs, RexNode rhs) {
        return rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, lhs, rhs);
    }

    private static RexNode not(RexNode expr) {
        return rexBuilder.makeCall(SqlStdOperatorTable.NOT, expr);
    }

    private static RexNode notEq(RexNode lhs, RexNode rhs) {
        return rexBuilder.makeCall(SqlStdOperatorTable.NOT_EQUALS, lhs, rhs);
    }

    private static RexNode call(SqlOperator op, RexNode... exprs) {
        return rexBuilder.makeCall(op, exprs);
    }

    private static RelDataType type(SqlTypeName typeName) {
        return TYPE_FACTORY.createSqlType(typeName);
    }
}
